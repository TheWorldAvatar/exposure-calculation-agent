from flask import Blueprint, request
from itertools import product
from twa import agentlogging
from agent.calculation.api import do_calculation
from agent.interactor.initialise_calculation import initialise_calculation
from agent.objects.calculation_metadata import CalculationMetadata
import agent.utils.constants as constants
from pathlib import Path
from rdflib.plugins.sparql.parser import parseQuery
from agent.utils.ts_client import TimeSeriesClient
import json

logger = agentlogging.get_logger('dev')

trigger_calculation_bp = Blueprint(
    'trigger_calculation', __name__, url_prefix='/trigger_calculation')


@trigger_calculation_bp.route('/delete_time_series', methods=['DELETE'])
def delete_time_series():
    data_iri = request.args.get('data_iri')
    ts_client = TimeSeriesClient(data_iri)
    ts_client.delete_data(data_iri)
    return 'Deleted data'


@trigger_calculation_bp.route('/bulk', methods=['POST'])
def bulk_trigger_calculation():
    # example input of JSON request body
    # {
    #     "exposure_table": "ndvi_raster",
    #     "rdf_types": [
    #         "https://www.theworldavatar.com/kg/ontoexposure/AreaWeightedSum"
    #     ],
    #     "distances": [
    #         400,
    #         800,
    #         1000
    #     ],
    #     "dataset_filter_values": {
    #         "year": [
    #         2016
    #         ]
    #     },
    #     // provide either subject_query_file or subject, not both
    #     "subject_query_file": "subject_query.sparql",
    #     "subject": "http://subject"
    # }

    # a cross product between distances and the provided dataset filters is done to find all parameter combinations,
    # then a calculation is initiated for each of the combinations for each rdf_type provided

    from agent.utils.kg_client import kg_client
    inputs = request.json
    exposure_table = inputs['exposure_table']
    rdf_types = inputs['rdf_types']
    distances = inputs['distances']

    upperbound = None
    if 'upperbound' in inputs:
        upperbound = inputs['upperbound']

    lowerbound = None
    if 'lowerbound' in inputs:
        lowerbound = inputs['lowerbound']

    dataset_filters = [{}]
    if 'dataset_filter_values' in inputs:
        dataset_filter_values = inputs['dataset_filter_values']
        # produces a cartesian product between the dataset_filter_values
        dataset_filters = [
            dict(zip(dataset_filter_values.keys(), combo))
            for combo in product(*dataset_filter_values.values())
        ]

    # IRI(s) of subject to calculate
    subject = None
    # query to obtain subject IRIs
    subject_query_file = None

    if 'subject' in inputs:
        subject = inputs['subject']

    if 'subject_query_file' in inputs:
        subject_query_file = inputs['subject_query_file']

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
        query_result = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        logger.info('Received ' + str(len(query_result)) + ' IRIs')

        if len(query_result) == 0:
            logger.warning('There are no subject IRIs')
            return

        subject_list = []
        for i in query_result:
            subject_list.append(i[select_var])

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    for rdf_type in rdf_types:
        for distance in distances:
            for dataset_filter in dataset_filters:
                # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
                calculation_iri = initialise_calculation(CalculationMetadata(
                    rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound, dataset_filter=dataset_filter))

                logger.info('Calling core calculation agent')

                # call core calculation agent
                do_calculation(subject=subject if subject is not None else subject_list,
                               calculation=calculation_iri, exposure=exposure_dataset_iri)

                logger.info(
                    f"""
                        Completed calculation for: rdf_type={rdf_type}, distance={distance}, upperbound={upperbound}, 
                        lowerbound={lowerbound}, dataset_filter={dataset_filter}
                    """)

    return f"Finished all calculations for request: {inputs}"


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

    dataset_filter = request.args.get('dataset_filter')
    if dataset_filter:
        dataset_filter = json.loads(dataset_filter)

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
        query_result = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        logger.info('Received ' + str(len(query_result)) + ' IRIs')

        if len(query_result) == 0:
            logger.warning('There are no subject IRIs')
            return

        subject_list = []
        for i in query_result:
            subject_list.append(i[select_var])

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound, dataset_filter=dataset_filter))

    logger.info('Calling core calculation agent')

    # call core calculation agent
    agent_response = do_calculation(subject=subject if subject is not None else subject_list,
                                    calculation=calculation_iri, exposure=exposure_dataset_iri)

    return agent_response


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
