from agent.calculation.calculation_input import CalculationInput
from agent.calculation.shared_utils import get_iri_to_point_dict, instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')


def raster_sum(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/resources/raster_summary.sql", "r") as f:
        raster_sql = f.read()

    with open("agent/calculation/resources/temp_table_raster.sql", "r") as f:
        temp_table_sql = f.read()

    subject_to_result_dict = {}

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # temp table for efficiency
            temp_table = 'temp_table'
            temp_table_sql = temp_table_sql.format(
                TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name)
            cur.execute(temp_table_sql)

            raster_sql = raster_sql.format(TEMP_TABLE=temp_table)

            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                subject_to_result_dict[iri] = 0
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(raster_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    for row in query_result:
                        if row['count'] > 0:
                            subject_to_result_dict[iri] += row['sum']

    logger.info('Instantiating results')
    instantiate_result_ontop(subject_to_result_dict, calculation_input)

    logger.info('Instantiated results')
    return 'Completed raster calculations'
