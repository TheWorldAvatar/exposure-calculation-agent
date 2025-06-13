from flask import Blueprint, request
import requests
from twa import agentlogging
from agent.interactor.initialise_calculation import initialise_calculation
from agent.objects.calculation_metadata import CalculationMetadata
from agent.utils.constants import TRAJECTORY_COUNT
from agent.calculation.api import CALCULATE_ROUTE
from agent.utils.kg_client import kg_client


logger = agentlogging.get_logger('dev')

trigger_calculation_bp = Blueprint(
    'trigger_calculation', __name__, url_prefix='/trigger_calculation')


@trigger_calculation_bp.route('/trajectory_count', methods=['POST'])
def trajectory_count():
    """
    A slightly more user friendly API to trigger trajectory count, instead of calling agent directly with IRIs
    Inputs as HTTP parameter
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
    exposure_dataset_iri = kg_client.get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist and return the instantiated iri, or return an existing iri
    calculation_iri = initialise_calculation(CalculationMetadata(
        rdf_type=TRAJECTORY_COUNT, distance=distance, upperbound=upperbound, lowerbound=lowerbound))

    logger.info('Calling core calculation agent')
    # call core calculation agent
    requests.post('http://localhost:5000/' + CALCULATE_ROUTE,
                  json={"calculation": calculation_iri, "subject": subject, "exposure": exposure_dataset_iri})
    return ''
