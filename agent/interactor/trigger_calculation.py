from flask import Blueprint, request, Response
import requests
from twa import agentlogging
from agent.calculation.api import CALCULATE_ROUTE
from agent.interactor.initialise_calculation import initialise_calculation
from agent.objects.calculation_metadata import CalculationMetadata
import agent.utils.constants as constants
from pathlib import Path
from rdflib.plugins.sparql.parser import parseQuery
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL

logger = agentlogging.get_logger('dev')

trigger_calculation_bp = Blueprint(
    'trigger_calculation', __name__, url_prefix='/trigger_calculation')


@trigger_calculation_bp.route('/', methods=['POST'])
def trigger_calculation():
    from agent.utils.kg_client import kg_client
    logger.info('Received request to trigger calculation')

    rdf_type = request.args.get('rdf_type')
    distance = request.args.get('distance')

    # IRI(s) of subject to calculate
    subject = request.args.get('subject')
    exposure_table = request.args.get('exposure_table')

    # for trajectory time series
    upperbound = request.args.get('upperbound')
    lowerbound = request.args.get('lowerbound')

    # query to obtain subject IRIs
    subject_query_file = request.args.get('subject_query_file')

    if subject is not None and subject_query_file is not None:
        raise Exception('Provide subject or subject_query_file, but not both')

    # do SPARQL query to obtain a list of subject IRIs
    if subject_query_file is not None:
        with open(Path(constants.BIND_MOUNT_PATH)/subject_query_file, "r") as f:
            query = f.read()

        parsed = parseQuery(query)

        if len(parsed[1]['projection']) != 1:
            raise Exception(
                'Provided query needs to have exactly one select variable')

        select_var = str(parsed[1]['projection'][0]['var'])

        logger.info(
            'Querying subject IRIs with provided SPARQL query template')
        query_result = kg_client.remote_store_client.executeFederatedQuery(
            [ONTOP_URL, BLAZEGRAPH_URL], query)

        logger.info('Received ' + str(query_result.length()) + ' IRIs')

        subject_list = []
        for i in range(query_result.length()):
            subject_list.append(
                query_result.getJSONObject(i).getString(select_var))

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound))

    logger.info('Calling core calculation agent')

    # call core calculation agent
    agent_response = requests.post('http://localhost:5000' + CALCULATE_ROUTE, json={
                                   "calculation": calculation_iri, "subject": subject if subject is not None else subject_list, "exposure": exposure_dataset_iri})

    return Response(response=agent_response.content, status=agent_response.status_code, content_type=agent_response.headers.get('Content-Type'))


def get_dataset_iri(table_name):
    from agent.utils.kg_client import kg_client
    var_name = 'dataset'
    query = f"""
    SELECT ?{var_name}
    WHERE {{
        ?{var_name} a <{constants.DCAT_DATASET}>;
            <{constants.DCTERM_TITLE}> "{table_name}".
    }}
    """
    query_results = kg_client.remote_store_client.executeQuery(query)

    if query_results.length() != 1:
        raise Exception('Failed to get dataset IRI.')
    else:
        return query_results.getJSONObject(0).getString(var_name)
