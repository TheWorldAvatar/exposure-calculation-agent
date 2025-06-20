from flask import Blueprint, request, Response
import requests
from twa import agentlogging
from agent.interactor.initialise_calculation import initialise_calculation
from agent.objects.calculation_metadata import CalculationMetadata
import agent.utils.constants as constants
from agent.calculation.api import CALCULATE_ROUTE
from pathlib import Path
from rdflib.plugins.sparql.parser import parseQuery
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL

logger = agentlogging.get_logger('dev')

trigger_calculation_bp = Blueprint(
    'trigger_calculation', __name__, url_prefix='/trigger_calculation')


@trigger_calculation_bp.route('/trajectory_count', methods=['POST'])
def trajectory_count():
    """
    A slightly more user friendly API to trigger trajectory count, instead of calling agent directly with IRIs
    Inputs as HTTP parameters
    distance: buffer distance
    subject: IRI that contains point time series for trajectory
    exposure_table: name of table for exposure dataset
    """
    logger.info('Received request to trigger calculation for trajectory count')

    distance = request.args.get('distance')
    subject = request.args.get('subject')
    exposure_table = request.args.get('exposure_table')
    upperbound = request.args.get('upperbound')
    lowerbound = request.args.get('lowerbound')

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=constants.TRAJECTORY_COUNT, distance=distance, upperbound=upperbound, lowerbound=lowerbound))

    logger.info('Calling core calculation agent')

    # call core calculation agent
    agent_response = requests.post('http://localhost:5000/' + CALCULATE_ROUTE, json={
                                   "calculation": calculation_iri, "subject": subject, "exposure": exposure_dataset_iri})
    return Response(response=agent_response.content, status=agent_response.status_code, content_type=agent_response.headers.get('Content-Type'))


@trigger_calculation_bp.route('/simple_count', methods=['POST'])
def simple_count():
    from agent.utils.kg_client import kg_client
    """
    API route to trigger simple counting for fixed geometries
    Inputs as HTTP parameters:
    - distance: distance to use in st_dwithin
    - exposure_table: name of table to calculate against
    - query_file: name of file containing sparql query to get subject IRIs in bind mount folder
    """

    logger.info(
        'Received request to trigger calculations to count features around fixed geometries')

    distance = request.args.get('distance')
    exposure_table = request.args.get('exposure_table')
    query_file = request.args.get('query_file')

    if query_file is not None:
        with open(Path(constants.BIND_MOUNT_PATH)/query_file, "r") as f:
            query = f.read()

    parsed = parseQuery(query)

    if len(parsed[1]['projection']) != 1:
        raise Exception(
            'Provided query needs to have exactly one select variable')

    select_var = str(parsed[1]['projection'][0]['var'])

    logger.info('Querying subject IRIs with provided SPARQL query template')
    query_result = kg_client.remote_store_client.executeFederatedQuery(
        [ONTOP_URL, BLAZEGRAPH_URL], query)

    logger.info('Received ' + str(query_result.length()) + ' IRIs')
    subject_list = []
    for i in range(query_result.length()):
        subject_list.append(
            query_result.getJSONObject(i).getString(select_var))

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # initialise if does not exist
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=constants.SIMPLE_COUNT, distance=distance))

    logger.info('Calling core calculation agent')
    agent_response = requests.post('http://localhost:5000/' + CALCULATE_ROUTE, json={
                                   "calculation": calculation_iri, "subject": subject_list, "exposure": exposure_dataset_iri})

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
        return None
    else:
        return query_results.getJSONObject(0).getString(var_name)
