from flask import Blueprint
from twa import agentlogging

from agent.correlation.correlation import calculate_correlation

logger = agentlogging.get_logger('dev')

correlation_blueprint = Blueprint(
    'correlation', __name__, url_prefix='/correlation')


@correlation_blueprint.route('/', methods=['POST'])
def api():
    calculate_correlation()
    return 'calculated correlation'
