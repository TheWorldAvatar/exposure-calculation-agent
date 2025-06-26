from flask import Blueprint, Response, request
from twa import agentlogging
from agent.interactor.trigger_calculation import get_dataset_iri
import agent.utils.constants as constants
from pathlib import Path
from rdflib.plugins.sparql.parser import parseQuery
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL
from agent.objects.calculation_metadata import CalculationMetadata
import csv
import io

logger = agentlogging.get_logger('dev')

generate_results_bp = Blueprint(
    'generate_results', __name__, url_prefix='/generate_results')


@generate_results_bp.route('/', methods=['GET'])
def non_trajectory():
    from agent.utils.kg_client import kg_client
    # rdf type of calculation type
    rdf_type = request.args.get('rdf_type')

    # metadata for calculation instance
    distance = request.args.get('distance')

    # IRI(s) of subject to calculate
    subject = request.args.get('subject')
    exposure_table = request.args.get('exposure_table')

    # for trajectory time series
    upperbound = request.args.get('upperbound')
    lowerbound = request.args.get('lowerbound')

    # query to obtain subject IRIs
    subject_query_file = request.args.get('subject_query_file')

    # query for user facing label of subject IRI, e.g. postcode value
    subject_label_query_file = request.args.get('subject_label_query_file')

    if subject is not None and subject_query_file is not None:
        raise Exception('Provide subject or subject_query_file, but not both')

    # do SPARQL query to obtain a list of subject IRIs
    if subject_query_file is not None:
        subject_list = _get_subjects(subject_query_file=subject_query_file)

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    calculate_metadata = CalculationMetadata(
        rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound)

    var = 'var'
    query_result = kg_client.remote_store_client.executeQuery(
        calculate_metadata.get_query(var))
    if query_result.length() != 1:
        raise Exception(
            'Queried size for calculation instances: ' + str(query_result.length()))

    calculation_iri = query_result.getJSONObject(0).getString(var)

    # returns IRI to result
    logger.info('Querying results')
    subject_to_result_dict = _get_subject_to_result_dict(
        subject if subject is not None else subject_list, exposure=exposure_dataset_iri, calculation_iri=calculation_iri)

    # returns IRI to label
    logger.info('Querying label')
    subject_to_label_dict = _get_subject_to_label_dict(
        subject_label_query_file=subject_label_query_file, subjects=subject if subject is not None else subject_list)

    logger.info('Generating CSV file')
    csv = _create_csv(subject_to_label_dict=subject_to_label_dict,
                      subject_to_result_dict=subject_to_result_dict)

    response = Response(csv.getvalue(), mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    return response


@generate_results_bp.route('/trajectory', methods=['GET'])
def trajectory():
    return ''


def _get_subject_to_result_dict(subject, exposure, calculation_iri):
    from agent.utils.kg_client import kg_client
    subject_to_result_dict = {}

    for chunk in _chunk_list(subject):
        values = " ".join(f"<{s}>" for s in chunk)
        query = f"""
        SELECT ?subject ?value
        WHERE {{
            VALUES ?subject {{{values}}}
            ?derivation <{constants.IS_DERIVED_FROM}> ?subject;
                <{constants.IS_DERIVED_FROM}> <{exposure}>;
                <{constants.IS_DERIVED_USING}> <{calculation_iri}>.
            ?result <{constants.BELONGS_TO}> ?derivation;
                <{constants.HAS_VALUE}> ?value
        }}
        """

        query_result = kg_client.remote_store_client.executeQuery(query)

        for i in range(query_result.length()):
            iri = query_result.getJSONObject(i).getString('subject')
            result = query_result.getJSONObject(i).getDouble('value')
            subject_to_result_dict[iri] = result

    return subject_to_result_dict


def _get_select_var(sparql_query):
    parsed = parseQuery(sparql_query)

    if len(parsed[1]['projection']) != 1:
        raise Exception(
            'Provided query needs to have exactly one select variable')

    return str(parsed[1]['projection'][0]['var'])


def _get_subjects(subject_query_file):
    from agent.utils.kg_client import kg_client
    with open(Path(constants.BIND_MOUNT_PATH)/subject_query_file, "r") as f:
        subject_query = f.read()

    select_var = _get_select_var(subject_query)

    logger.info(
        'Querying subject IRIs with provided SPARQL query template')
    query_result = kg_client.remote_store_client.executeFederatedQuery(
        [ONTOP_URL, BLAZEGRAPH_URL], subject_query)

    logger.info('Received ' + str(query_result.length()) + ' IRIs')

    subject_list = []
    for i in range(query_result.length()):
        subject_list.append(
            query_result.getJSONObject(i).getString(select_var))

    return subject_list


def _get_subject_to_label_dict(subject_label_query_file, subjects):
    from agent.utils.kg_client import kg_client
    with open(Path(constants.BIND_MOUNT_PATH)/subject_label_query_file, "r") as f:
        query_template = f.read()
    subject_to_label_dict = {}

    for chunk in _chunk_list(subjects):
        query = _insert_values_clause(
            sparql_query=query_template, varname='Feature', uris=chunk)

        query_result = kg_client.remote_store_client.executeFederatedQuery(
            [ONTOP_URL, BLAZEGRAPH_URL], query)

        for i in range(query_result.length()):
            subject_iri = query_result.getJSONObject(i).getString('Feature')
            subject_label = query_result.getJSONObject(i).getString('Label')
            subject_to_label_dict[subject_iri] = subject_label

    return subject_to_label_dict


def _insert_values_clause(sparql_query, varname, uris):
    """ 
    inserts "VALUES ?Feature {..}" right after "WHERE {"
    """
    values = " ".join(f"<{uri}>" for uri in uris)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    where_index = sparql_query.lower().find("where")
    brace_index = sparql_query.find("{", where_index)
    return sparql_query[:brace_index + 1] + "\n  " + values_clause + "\n" + sparql_query[brace_index + 1:]


def _create_csv(subject_to_result_dict, subject_to_label_dict):
    data = []

    for subject in subject_to_result_dict.keys():
        label = subject_to_label_dict[subject]
        value = subject_to_result_dict[subject]

        data.append({'label': label, 'value': value})

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    return output


def _chunk_list(values, chunk_size=10000):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]
