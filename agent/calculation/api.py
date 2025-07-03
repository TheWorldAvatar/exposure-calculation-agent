from flask import Blueprint, request
from twa import agentlogging
from agent.calculation.simple_area import simple_area
from agent.calculation.trajectory_count import trajectory_count
from agent.calculation.simple_count import simple_count
from agent.calculation.raster_sum import raster_sum
from agent.objects.calculation_metadata import get_calculation_metadata
from agent.utils.constants import TRAJECTORY_COUNT, SIMPLE_COUNT, RASTER_SUM, SIMPLE_AREA
from agent.calculation.calculation_input import CalculationInput

logger = agentlogging.get_logger('dev')
calculation_blueprint = Blueprint('calculation_blueprint', __name__)

# map of RDF type of calculation to the function to call
function_map = {
    TRAJECTORY_COUNT: trajectory_count,
    SIMPLE_COUNT: simple_count,
    RASTER_SUM: raster_sum,
    SIMPLE_AREA: simple_area
}

CALCULATE_ROUTE = '/calculate_exposure'


@calculation_blueprint.route(CALCULATE_ROUTE, methods=['POST'])
# core agent, takes IRIs of calculation, subject, and exposure as inputs
def api():
    """
    This is the core agent to process calculations, typically triggered by APIs in 
    agent.interactor.trigger_calculation 
    Input should be a JSON payload with the following keys, with IRI(s) as their values
    1) subject (value can be an IRI array or a single IRI)
    2) exposure (single IRI)
    3) calculation (single IRI)
    """
    logger.info('Core calculation agent request received')
    request_json = request.get_json()
    calculation_iri = request_json['calculation']
    subject = request_json['subject']
    exposure = request_json['exposure']

    # gives a CalculationMetadata object
    calculation_metadata = get_calculation_metadata(calculation_iri)

    calculation_input = CalculationInput(
        subject=subject, exposure=exposure, calculation_metadata=calculation_metadata)

    # calls the appropriate function according to calculation rdf_type
    return function_map[calculation_metadata.rdf_type](calculation_input)
