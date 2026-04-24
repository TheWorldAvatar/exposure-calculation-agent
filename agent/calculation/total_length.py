from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_point_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils import constants
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from tqdm import tqdm
import sys
from agent.objects.exposure_value import ExposureValue
from agent.utils.constants import METRE

logger = agentlogging.get_logger('dev')


def total_length(calculation_input: CalculationInput):
    # sums up length of lines within buffer
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/temp_table_vector.sql", "r") as f:
        temp_table_sql = f.read()

    with open("agent/calculation/resources/total_length.sql", "r") as f:
        total_length_sql = f.read()

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

            total_length_sql = total_length_sql.format(TEMP_TABLE=temp_table)
            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(total_length_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    if query_result[0][0] is None:
                        subject_to_result_dict[iri] = ExposureValue(
                            value=0, unit=METRE)
                    else:
                        subject_to_result_dict[iri] = ExposureValue(
                            value=query_result[0][0], unit=METRE)

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    complete_message = 'Completed calculation for total length'
    logger.info(complete_message)

    return complete_message
