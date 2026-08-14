from flask import Blueprint, request
from twa import agentlogging
from agent.interactor.trigger_calculation import get_dataset_iri
from agent.stats.clustering import run_clustering
from agent.stats.correlation import calculate_correlation
from agent.stats.post_process import correlation_post_process

logger = agentlogging.get_logger('dev')

stats_blueprint = Blueprint('stats', __name__, url_prefix='/stats')


@stats_blueprint.route('/correlation', methods=['POST'])
def calculate():
    logger.info('Calculating correlations')
    exposure_tables = request.args.getlist('exposure_table')
    if exposure_tables:
        logger.info(f"Only running calculations for {exposure_tables}")
    dataset_iri_list = [get_dataset_iri(exposure_table)
                        for exposure_table in exposure_tables]
    calculate_correlation(dataset_iri_list)
    return 'calculated correlation'


@stats_blueprint.route('/post_process_correlation', methods=['POST'])
def post_process_correlation():
    return correlation_post_process()


@stats_blueprint.route('clustering', methods=['GET'])
def clustering():
    return run_clustering(request.json)
