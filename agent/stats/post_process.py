from collections import defaultdict

from agent.objects.calculation_metadata import get_calculation_metadata
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.objects.exposure_result_set import ExposureResultSet
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')


def correlation_post_process():
    with postgis_client.connect() as conn:
        # returns something like {id: ExposureResultSet object}
        id_to_exposure_result_set, calc_iri_to_metadata = _get_exposure_result_set(
            conn)

        # returns something like {(exposure_result1, exposure_result2): 0.5}
        # exposure_result1 is an ExposureResult object
        result_set_pair_to_correlation = _get_all_correlations(
            id_to_exposure_result_set, conn)

    # group exposure result set according to exposure dataset
    dataset_pair_dict = defaultdict(dict)

    for pair, corr in result_set_pair_to_correlation.items():
        # to group results by pair of exposure dataset, e.g. ndvi vs lst, ndvi vs income (sorted makes lst vs ndvi the same as ndvi vs lst)
        iri_to_exposure_dataset = {}
        iri_to_exposure_dataset[pair[0].exposure_dataset.iri] = pair[0].exposure_dataset
        iri_to_exposure_dataset[pair[1].exposure_dataset.iri] = pair[1].exposure_dataset

        iri_tuple = (
            sorted((pair[0].exposure_dataset.iri, pair[1].exposure_dataset.iri)))
        key = (iri_to_exposure_dataset[iri_tuple[0]],
               iri_to_exposure_dataset[iri_tuple[1]])

        # if the exposure dataset pair is swapped from sorting, swap the result set pair as well
        if pair[0].exposure_dataset.iri == iri_tuple[0]:
            result_set_pair_key = pair
        else:
            result_set_pair_key = (pair[1], pair[0])

        dataset_pair_dict[key][result_set_pair_key] = corr

    sorted_data = {
        k: dict(sorted(v.items(), key=lambda item: abs(item[1]), reverse=True))
        for k, v in dataset_pair_dict.items()
    }

    same_datasets = defaultdict(dict)
    different_datasets = defaultdict(dict)
    response_string = []

    for dataset_pair, set_pair_dict in sorted_data.items():
        response_string.append(
            f"{dataset_pair[0].table_name}, {dataset_pair[1].table_name}")

        # only print the highest
        highest_corr = next(iter(set_pair_dict.values()))
        response_string.append(f"Highest correlation: {highest_corr}")
        response_string.append("-------")
        if dataset_pair[0] == dataset_pair[1]:
            # get buffer distance
            for set, corr in set_pair_dict.items():
                metadata1 = set[0].calculation_metadata
                metadata2 = set[1].calculation_metadata
                same_datasets[dataset_pair[0]][(metadata1, metadata2)] = corr

        else:
            # get buffer distance or column name
            for set, corr in set_pair_dict.items():
                metadata1 = set[0].calculation_metadata
                metadata2 = set[1].calculation_metadata
                different_datasets[dataset_pair][(metadata1, metadata2)] = corr

    for dataset, pair_to_corr_dict in same_datasets.items():
        response_string.append(dataset.table_name)
        for pair, corr in pair_to_corr_dict.items():
            response_string.append(pair[0].get_string())
            response_string.append(pair[1].get_string())
            response_string.append(str(corr))
        response_string.append("-------")

    for dataset_pair, pair_to_corr_dict in different_datasets.items():
        response_string.append(
            f"{dataset_pair[0].table_name}, {dataset_pair[1].table_name}")
        for pair, corr in pair_to_corr_dict.items():
            response_string.append(pair[0].get_string())
            response_string.append(pair[1].get_string())
            response_string.append(str(corr))
        response_string.append("-------")

    return "\n".join(response_string)


def _get_exposure_result_set(conn):
    query = """
    SELECT e.id, e.exposure, e.calculation
    FROM exposure_result_set e
    JOIN (
        SELECT set_id1 AS id FROM exposure_correlation
        UNION
        SELECT set_id2 AS id FROM exposure_correlation
    ) t
    ON e.id = t.id
    """
    id_to_exposure_set = {}
    exposure_set = set()
    calculation_set = set()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        if cur.description:
            query_result = cur.fetchall()
            for row in query_result:
                id_to_exposure_set[row['id']] = (
                    row['exposure'], row['calculation'])

    for id in id_to_exposure_set:
        exposure_set.add(id_to_exposure_set[id][0])
        calculation_set.add(id_to_exposure_set[id][1])

    exposure_iri_to_dataset = {}
    calculation_iri_to_metadata = {}
    for exposure in exposure_set:
        exposure_iri_to_dataset[exposure] = get_exposure_dataset(exposure)

    for calculation in calculation_set:
        calculation_iri_to_metadata[calculation] = get_calculation_metadata(
            calculation)

    for id in id_to_exposure_set:
        exposure_dataset = exposure_iri_to_dataset[id_to_exposure_set[id][0]]
        calculation_metadata = calculation_iri_to_metadata[id_to_exposure_set[id][1]]
        exposure_result_set = ExposureResultSet(
            exposure_dataset=exposure_dataset, calculation_metadata=calculation_metadata)

        # overwrite existing dict with object
        id_to_exposure_set[id] = exposure_result_set

    return id_to_exposure_set, calculation_iri_to_metadata


def _get_all_correlations(id_to_exposure_result_set, conn):
    query = """
    SELECT set_id1, set_id2, value
    FROM exposure_correlation
    """
    result_set_pair_to_correlation = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        if cur.description:
            query_result = cur.fetchall()
            for row in query_result:
                pair = (id_to_exposure_result_set[row['set_id1']],
                        id_to_exposure_result_set[row['set_id2']])
                result_set_pair_to_correlation[pair] = row['value']

    return result_set_pair_to_correlation
