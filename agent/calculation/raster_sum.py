from agent.calculation.calculation_input import CalculationInput
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.calculation.simple_count import get_iri_to_point_dict, instantiate_result
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')


def raster_sum(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/templates/raster_summary.sql", "r") as f:
        sql = f.read()

    subject_to_result_dict = {}

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            temp_table = 'temp_table'

            create_temp_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT ST_Transform(rast, 3857) AS rast
            FROM {exposure_dataset.table_name}
            """

            cur.execute(create_temp_sql)

            sql = sql.format(TEMP_TABLE=temp_table)

            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    if query_result[0]['count'] > 0:
                        subject_to_result_dict[iri] = query_result[0]['sum']
                    else:
                        # does not intersect any pixels
                        subject_to_result_dict[iri] = 0

    logger.info('Instantiating results')
    instantiate_result(subject_to_result_dict, calculation_input)

    return 'Completed raster calculations'
