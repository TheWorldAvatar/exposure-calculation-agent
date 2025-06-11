from flask import Blueprint, request
from twa import agentlogging
from agent.calculation.trajectory_count import trajectory_count
from agent.utils.constants import TRAJECTORY_COUNT
from agent.calculation.kg_client import KgClient
from agent.calculation.calculation_input import CalculationInput

logger = agentlogging.get_logger('dev')
calculation_blueprint = Blueprint('calculation_blueprint', __name__)

# map of RDF type of calculation to the function to call
function_map = {
    TRAJECTORY_COUNT: trajectory_count
}

CALCULATE_ROUTE = '/calculate_exposure'
kg_client = KgClient()


@calculation_blueprint.route(CALCULATE_ROUTE, methods=['POST'])
# core agent, takes IRIs of calculation, subject, and exposure as inputs
def api():
    """
    This is the core API to process calculations, typically triggered by APIs in 
    agent.interactor.trigger_calculation 
    """
    request_json = request.get_json()
    calculation_iri = request_json['calculation']
    subject = request_json['subject']
    exposure = request_json['exposure']

    # gives a CalculationMetadata object
    calculation_metadata = kg_client.get_calculation_metadata(calculation_iri)

    calculation_input = CalculationInput(
        subject=subject, exposure=exposure, calculation_metadata=calculation_metadata)

    # calls the appropriate function according to calculation
    response = function_map[calculation_metadata.get_rdf_type()](
        calculation_input)
