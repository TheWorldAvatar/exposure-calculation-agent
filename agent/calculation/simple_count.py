from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_point_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from tqdm import tqdm
import sys
from agent.objects.exposure_value import ExposureValue

logger = agentlogging.get_logger('dev')


def simple_count(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/count.sql", "r") as f:
        count_sql = f.read()

    with open("agent/calculation/resources/temp_table_vector.sql", "r") as f:
        temp_table_sql = f.read()

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            # create temp table for efficiency
            temp_table = 'temp_table'
            temp_table_sql = temp_table_sql.format(
                TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name, GEOMETRY_COLUMN=exposure_dataset.geometry_column)
            cur.execute(temp_table_sql)

            count_sql = count_sql.format(TEMP_TABLE=temp_table)

            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(count_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    subject_to_result_dict[iri] = ExposureValue(
                        value=query_result[0][0])

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    logger.info('Completed instantiation')
    return 'Calculation complete', 200
