from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_point_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils import constants
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from tqdm import tqdm
import sys
from agent.objects.exposure_value import ExposureValue

logger = agentlogging.get_logger('dev')


def closest_distance(calculation_input: CalculationInput):
    # designed to calculate closest distance of subject to the edge of a polygon in the exposure dataset
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/closest_distance.sql", "r") as f:
        closest_distance_sql = f.read()

    with open("agent/calculation/resources/temp_table_vector.sql", "r") as f:
        temp_table_sql = f.read()

    # handle dataset filters
    where_clause = " AND ".join(
        f"{k} = %({k})s" for k in calculation_input.calculation_metadata.dataset_filter)
    if where_clause:
        where_clause = f"WHERE {where_clause}"

    params = {}
    for key, value in calculation_input.calculation_metadata.dataset_filter.items():
        params[key] = value

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            # create temp table for efficiency
            temp_table = 'temp_table'

            if exposure_dataset.geometry_column is not None:
                geometry_column = exposure_dataset.geometry_column
            else:
                geometry_column = constants.VECTOR_GEOMETRY_COLUMN

            temp_table_sql = temp_table_sql.format(
                TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name, GEOMETRY_COLUMN=geometry_column, DATASET_FILTERS=where_clause)
            cur.execute(temp_table_sql, params)

            closest_distance_sql = closest_distance_sql.format(
                TEMP_TABLE=temp_table)

            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt
                }
                cur.execute(closest_distance_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    subject_to_result_dict[iri] = ExposureValue(
                        value=query_result[0][0], unit='m')

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    complete_message = 'Completed calculation for closest distance'
    logger.info(complete_message)
    return complete_message, 200
