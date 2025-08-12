from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_point_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')


def simple_area(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/temp_table_vector.sql", "r") as f:
        temp_table_sql = f.read()

    with open("agent/calculation/resources/area.sql", "r") as f:
        area_sql = f.read()

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            # create temp table for efficiency
            temp_table = 'temp_table'
            temp_table_sql = temp_table_sql.format(
                TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name)
            cur.execute(temp_table_sql)

            area_sql = area_sql.format(TEMP_TABLE=temp_table)
            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(area_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    if query_result[0][0] is None:
                        subject_to_result_dict[iri] = 0
                    else:
                        subject_to_result_dict[iri] = query_result[0][0]

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    logger.info('Completed instantiation')
    return 'Completed calculation for simple area'
