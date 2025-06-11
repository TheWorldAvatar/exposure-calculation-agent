from flask import Blueprint, request
import requests
from twa import agentlogging
from agent.interactor.initialise_calculation import INIT_ROUTE
from agent.utils.constants import TRAJECTORY_COUNT
from agent.calculation.api import CALCULATE_ROUTE
from agent.interactor.kg_client import KgClient

logger = agentlogging.get_logger('dev')
kg_client = KgClient()  # for getting data from KG, initialise once

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
    distance = request.args.get('distance')
    subject = request.args.get('subject')
    exposure_table = request.args.get(
        'exposure_table')  # this is the table name

    exposure_dataset_iri = kg_client.get_dataset_iri(table_name=exposure_table)

    # this will initialise a calculation if it does not exist, or return an existing iri
    calculation_response = requests.post('http://localhost:5000/' +
                                         INIT_ROUTE, json={"rdf_type": TRAJECTORY_COUNT, "distance": distance})

    requests.post('http://localhost:5000/' + CALCULATE_ROUTE,
                  json={"calculation": calculation_response.text, "subject": subject, "exposure": exposure_dataset_iri})
    return ''
