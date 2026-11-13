from flask import Blueprint, request
from itertools import product
from twa import agentlogging
from agent.calculation.api import do_calculation
from agent.interactor.initialise_calculation import initialise_calculation
from agent.objects.calculation_metadata import CalculationMetadata
import agent.utils.constants as constants
from agent.utils.ts_client import TimeSeriesClient
from agent.jobs.store import jobs
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
    inputs = request.get_json()
    if not isinstance(inputs, dict):
        return {'error': 'Provide a JSON object'}, 400
    if not inputs.get('exposure_table') or not inputs.get('rdf_types'):
        return {'error': 'exposure_table and rdf_types are required'}, 400
    return enqueue_calculation('trigger_bulk', inputs)


def enqueue_calculation(kind, inputs):
    if (inputs.get('subject') is None) == (inputs.get('subject_query_file') is None):
        return {'error': 'Provide either subject or subject_query_file'}, 400
    job_id = jobs.enqueue({'kind': kind, 'inputs': inputs})
    # Relative to the application's base URL, including the blueprint prefix.
    status_url = 'trigger_calculation/jobs/' + job_id
    return {'job_id': job_id, 'status': 'queued', 'status_url': status_url}, 202, {'Location': status_url}


@trigger_calculation_bp.route('/jobs/<uuid:job_id>', methods=['GET'])
def calculation_job_status(job_id):
    job = jobs.get(job_id)
    if job is None or job['owner_id'] is not None:
        return {'error': 'Job not found'}, 404
    public = {key: value for key, value in job.items() if key != 'owner_id'}
    for key in ('created_at', 'started_at', 'finished_at', 'heartbeat_at'):
        if public.get(key) is not None:
            public[key] = public[key].isoformat()
    return public


def run_bulk_calculation(inputs):
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
    #     "column_names": ["column1"], // only applicable for Areal calculation
    #     // provide either subject_query_file or subject, not both
    #     "subject_query_file": "subject_query.sparql",
    #     "subject": "http://subject"
    # }

    # a cross product between distances and the provided dataset filters is done to find all parameter combinations,
    # then a calculation is initiated for each of the combinations for each rdf_type provided

    from agent.utils.kg_client import kg_client
    exposure_table = inputs['exposure_table']
    rdf_types = inputs['rdf_types']
    if 'distances' in inputs:
        distances = inputs['distances']
    else:
        distances = [None]

    if 'column_names' in inputs:
        column_names = inputs['column_names']
    else:
        column_names = [None]

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
        subject_list = kg_client.get_subjects_via_file(
            subject_query_file=subject_query_file)

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    for rdf_type in rdf_types:
        for distance in distances:
            for dataset_filter in dataset_filters:
                for column_name in column_names:
                    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
                    calculation_iri = initialise_calculation(CalculationMetadata(
                        rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound, dataset_filter=dataset_filter, column_name=column_name))

                    logger.info('Calling core calculation agent')

                    # call core calculation agent
                    check_calculation_response(do_calculation(
                        subject=subject if subject is not None else subject_list,
                        calculation=calculation_iri, exposure=exposure_dataset_iri))

                    logger.info(
                        f"""
                            Completed calculation for: rdf_type={rdf_type}, distance={distance}, upperbound={upperbound}, 
                            lowerbound={lowerbound}, dataset_filter={dataset_filter}, exposure={exposure_table}, column_name={column_name}
                        """)

    return f"Finished all calculations for request: {inputs}"


@trigger_calculation_bp.route('/', methods=['POST'])
def trigger_calculation():
    inputs = request.args.to_dict()
    if not inputs.get('rdf_type') or not inputs.get('exposure_table'):
        return {'error': 'rdf_type and exposure_table are required'}, 400
    if inputs.get('dataset_filter'):
        try:
            inputs['dataset_filter'] = json.loads(inputs['dataset_filter'])
        except ValueError:
            return {'error': 'dataset_filter must be valid JSON'}, 400
        if not isinstance(inputs['dataset_filter'], dict):
            return {'error': 'dataset_filter must be a JSON object'}, 400
    return enqueue_calculation('trigger_single', inputs)


def run_calculation(inputs):
    from agent.utils.kg_client import kg_client
    logger.info('Received request to trigger calculation')

    rdf_type = inputs.get('rdf_type')
    distance = inputs.get('distance')

    # IRI(s) of subject to calculate
    subject = inputs.get('subject')
    exposure_table = inputs.get('exposure_table')

    # for trajectory time series
    upperbound = inputs.get('upperbound')
    lowerbound = inputs.get('lowerbound')

    # query to obtain subject IRIs
    subject_query_file = inputs.get('subject_query_file')

    dataset_filter = inputs.get('dataset_filter')

    if subject is not None and subject_query_file is not None:
        raise Exception('Provide subject or subject_query_file, but not both')

    # do SPARQL query to obtain a list of subject IRIs
    if subject_query_file is not None:
        subject_list = kg_client.get_subjects_via_file(
            subject_query_file=subject_query_file)

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound, dataset_filter=dataset_filter))

    logger.info('Calling core calculation agent')

    # call core calculation agent
    agent_response = do_calculation(subject=subject if subject is not None else subject_list,
                                    calculation=calculation_iri, exposure=exposure_dataset_iri)

    return check_calculation_response(agent_response)


def check_calculation_response(response):
    if isinstance(response, tuple):
        message, status = response[:2]
        if not 200 <= status < 300:
            raise ValueError(f'Calculation returned HTTP {status}: {message}')
        return message
    return response


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
