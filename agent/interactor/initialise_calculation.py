from agent.objects.calculation_metadata import CalculationMetadata
from utils.constants import CALCULATION_TYPES
from twa import agentlogging
import uuid
import agent.utils.constants as constants

logger = agentlogging.get_logger('dev')


def initialise_calculation(calculation_metadata: CalculationMetadata):
    """
    Instantiates a calculation instance if it does not exist and returns the IRI
    """
    if calculation_metadata.rdf_type not in CALCULATION_TYPES:
        msg = 'Unsupported calculation type: ' + calculation_metadata.rdf_type
        logger.error(msg)
        logger.error('Supported types: ' + ', '.join(CALCULATION_TYPES))
        return msg

    calculation_iri = _get_calculation_iri(calculation_metadata)

    if calculation_iri is None:
        logger.info(
            'Existing calculation instance not found, instantiating a new instance')
        calculation_iri = _instantiate_calculation(calculation_metadata)
        return calculation_iri
    else:
        logger.info('Found existing calculation instance')
        return calculation_iri


def _get_calculation_iri(calculation_metadata: CalculationMetadata):
    from agent.utils.kg_client import kg_client
    var = 'calc'
    query_results = kg_client.remote_store_client.executeQuery(
        calculation_metadata.get_query(var))

    if not query_results.isEmpty():
        return query_results.getJSONObject(0).getString(var)
    else:
        return None


def _instantiate_calculation(calculation_metadata: CalculationMetadata):
    from agent.utils.kg_client import kg_client
    calculation_iri = constants.PREFIX_EXPOSURE + \
        'calculation/' + str(uuid.uuid4())
    query = calculation_metadata.get_insert_query(
        calculation_iri=calculation_iri)
    kg_client.remote_store_client.executeUpdate(query)

    return calculation_iri
