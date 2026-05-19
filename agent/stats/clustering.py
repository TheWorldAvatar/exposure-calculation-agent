from collections import defaultdict
from itertools import product
from agent.calculation.shared_utils import get_iri_to_point_dict_4326
from agent.interactor.trigger_calculation import get_dataset_iri
from agent.objects.exposure_dataset import ExposureDataset
from agent.utils.postgis_client import postgis_client
from agent.interactor.csv_export import _get_calculations, _get_subject_to_result_dict_calc_iri_sql
from agent.objects.exposure_result_set import ExposureResultSet
import numpy as np
from sklearn.cluster import KMeans
from twa import agentlogging

logger = agentlogging.get_logger('dev')


def run_clustering(inputs: dict):
    from agent.utils.kg_client import kg_client

    clustering_data = inputs['clustering_data']
    subject_query_file = inputs['subject_query_file']
    subjects = kg_client.get_subjects_via_file(
        subject_query_file=subject_query_file)
    iri_to_point_dict = get_iri_to_point_dict_4326(subjects)
    list_of_set_list = []  # len = number of datasets, each element is a list of exposure set

    datasets_for_output = []

    for dataset in clustering_data:
        exposure_set_list_for_this_dataset = []
        exposure_table = dataset['exposure_table']
        datasets_for_output.append(exposure_table)

        exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)
        exposure_dataset = ExposureDataset(
            iri=exposure_dataset_iri, table_name=exposure_table)
        rdf_type = dataset['rdf_type']
        distance = None
        if 'distance' in dataset:
            distance = dataset['distance']

        dataset_filters = []
        if 'dataset_filter_values' in dataset:
            dataset_filter_values = dataset['dataset_filter_values']
            dataset_filters = [
                dict(zip(dataset_filter_values.keys(), combo))
                for combo in product(*dataset_filter_values.values())
            ]
            first_keys = set(dataset_filters[0].keys())
            if not all(set(d.keys()) == first_keys for d in dataset_filters):
                raise Exception(
                    'Provided dataset filters should have the same keys')

        calculation_metadata_list = _get_calculations(
            rdf_type=rdf_type, dataset_filters=dataset_filters)

        filtered_calc_metadata = [
            x for x in calculation_metadata_list if x.distance == distance]

        with postgis_client.connect() as conn:
            for calculation in filtered_calc_metadata:
                logger.info(f"Querying results for <{calculation.iri}>")

                _, subject_to_percentile_dict = _get_subject_to_result_dict_calc_iri_sql(
                    exposure=exposure_dataset_iri, calculation_iri=calculation.iri, subject=subjects, conn=conn)

                if not subject_to_percentile_dict:
                    continue

                exposure_set_list_for_this_dataset.append(ExposureResultSet(exposure_dataset=exposure_dataset,
                                                                            calculation_metadata=calculation, subject_to_percentile_dict=subject_to_percentile_dict))

        list_of_set_list.append(exposure_set_list_for_this_dataset)

    # create cross product between exposure sets
    combinations_for_clustering = list(product(*list_of_set_list))

    result_for_output = {}
    result_for_output['dataset'] = datasets_for_output
    result_for_output['subject_to_point_dict'] = iri_to_point_dict
    result_for_output['combinations'] = []

    for combination in combinations_for_clustering:
        subject_to_exposures = defaultdict(list)
        calculations_for_output = []
        for result_set in combination:
            calculations_for_output.append(
                result_set.calculation_metadata.get_string())
            for subject, percentile in result_set.subject_to_percentile_dict.items():
                subject_to_exposures[subject].append(percentile)
        subject_to_cluster, cluster_centre = clustering(n_clusters=inputs['n_clusters'],
                                                        subject_to_exposures=subject_to_exposures)

        cluster_info = get_cluster_info(
            subject_to_cluster, subject_to_exposures, cluster_centre)

        combination_for_output = {}
        combination_for_output['calculations'] = calculations_for_output
        combination_for_output['subject_to_cluster'] = subject_to_cluster
        combination_for_output['cluster_info'] = cluster_info
        result_for_output['combinations'].append(combination_for_output)

    return result_for_output


def clustering(n_clusters: int, subject_to_exposures: defaultdict[list]):
    # Keep keys separately
    keys = list(subject_to_exposures.keys())

    # Convert values to feature matrix
    X = np.array(list(subject_to_exposures.values()))

    # Run clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    kmeans.fit_predict(X)
    # Map results back to keys
    subject_to_cluster = dict(zip(keys, kmeans.labels_.tolist()))

    cluster_centers = kmeans.cluster_centers_.tolist()

    return subject_to_cluster, cluster_centers


def get_cluster_info(subject_to_cluster, subject_to_exposures, cluster_centre):
    # subject_to_cluster = {'http://something', 1}
    # subject_to_exposures = {'http://something', [1,2,3, ...]}
    cluster_info_dict = defaultdict(dict)
    cluster_to_count_dict = defaultdict(int)

    for subject in subject_to_cluster:
        cluster = subject_to_cluster[subject]
        exposures = subject_to_exposures[subject]

        if cluster not in cluster_to_count_dict:
            cluster_to_count_dict[cluster] = 1
        else:
            cluster_to_count_dict[cluster] += 1

        if cluster not in cluster_info_dict:
            cluster_info_dict[cluster]["min"] = [
                exposure for exposure in exposures]
            cluster_info_dict[cluster]["max"] = [
                exposure for exposure in exposures]
        else:
            for i, exposure in enumerate(exposures):
                if exposure < cluster_info_dict[cluster]["min"][i]:
                    cluster_info_dict[cluster]["min"][i] = exposure
                if exposure > cluster_info_dict[cluster]["max"][i]:
                    cluster_info_dict[cluster]["max"][i] = exposure

    for i, centre in enumerate(cluster_centre):
        cluster_info_dict[i]["centre"] = centre
        cluster_info_dict[i]["count"] = cluster_to_count_dict[i]

    return cluster_info_dict
