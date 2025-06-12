from flask import Blueprint, request
from twa import agentlogging
from utils.constants import CALCULATION_TYPES
from agent.objects.calculation_metadata import CalculationMetadata
from agent.utils.kg_client import KgClient

logger = agentlogging.get_logger('dev')

INIT_ROUTE = '/initialise_calculation'

initialise_calculation_bp = Blueprint('initialise_calculation_bp', __name__)

kg_client = KgClient()  # for getting data from KG


@initialise_calculation_bp.route(INIT_ROUTE, methods=['POST'])
def api():
    """
    JSON input
    {
        "rdf_type": "http://class"
        "distance": 10
    }
    """
    logger.info('Received request to instantiate a calculation instance')
    metadata = request.get_json()

    calculation_metadata = CalculationMetadata(metadata)

    if calculation_metadata.get_rdf_type() not in CALCULATION_TYPES:
        msg = 'Unsupported calculation type: ' + calculation_metadata.get_rdf_type()
        logger.error(msg)
        logger.error('Supported types: ' + ', '.join(CALCULATION_TYPES))
        return msg

    calculation_iri = kg_client.get_calculation_iri(calculation_metadata)

    if calculation_iri is None:
        logger.info(
            'Existing calculation instance not found, instantiating a new instance')
        calculation_iri = kg_client.instantiate_calculation(
            calculation_metadata)
        return calculation_iri
    else:
        logger.info('Found existing calculation instance')
        return calculation_iri
