from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_buffer_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from tqdm import tqdm
import sys
from agent.objects.exposure_value import ExposureValue
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')


def raster_count(calculation_input: CalculationInput):
    # simply count number of pixels
    iri_to_buffer_dict = get_iri_to_buffer_dict(
        subject=calculation_input.subject, distance=calculation_input.calculation_metadata.distance)
    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/raster_count.sql", "r") as f:
        raster_count_sql = f.read()

    where_clauses = []
    params = {}
    for key, value in calculation_input.calculation_metadata.dataset_filter.items():
        where_clauses.append(f"AND r.{key} = %({key})s")
        params[key] = value

    raster_count_sql = raster_count_sql.format(
        EXPOSURE_DATASET=exposure_dataset.table_name, GEOMETRY_COLUMN=exposure_dataset.geometry_column,
        DATASET_FILTERS="\n".join(where_clauses))

    logger.info('Submitting SQL queries for calculations')
    subject_to_result_dict = {}
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # get clipped pixels
            for iri, buffer in tqdm(iri_to_buffer_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                params['GEOMETRY_PLACEHOLDER'] = buffer.wkt

                cur.execute(raster_count_sql, params)
                if cur.description:
                    query_result = cur.fetchall()
                    subject_to_result_dict[iri] = ExposureValue(
                        value=query_result[0]['result'])
                else:
                    raise Exception('Something wrong?')

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    complete_message = 'Completed calculation for area weighted sum'
    logger.info(complete_message)

    return complete_message
