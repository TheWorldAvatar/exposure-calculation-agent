from agent.utils.kg_client import kg_client
from agent.objects.calculation_metadata import CalculationMetadata
from utils.constants import CALCULATION_TYPES
from twa import agentlogging

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
