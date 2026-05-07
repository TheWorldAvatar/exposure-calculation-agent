from flask import Blueprint
from twa import agentlogging

from agent.correlation.correlation import calculate_correlation
from agent.correlation.post_process import post_process_correlation

logger = agentlogging.get_logger('dev')

correlation_blueprint = Blueprint(
    'correlation', __name__, url_prefix='/correlation')


@correlation_blueprint.route('/', methods=['POST'])
def calculate():
    calculate_correlation()
    return 'calculated correlation'


@correlation_blueprint.route('/post_process', methods=['POST'])
def post_process():
    return post_process_correlation()
