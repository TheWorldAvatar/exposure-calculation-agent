from agent.calculation.calculation_input import CalculationInput
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.calculation.simple_count import get_iri_to_point_dict, instantiate_result
from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')


def raster_sum(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/templates/raster_summary.sql", "r") as f:
        sql = f.read()

    sql = sql.format(TABLE_PLACEHOLDER=exposure_dataset.table_name)

    subject_to_result_dict = {}

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for iri, point in iri_to_point_dict.items():
                replacements = {
                    'SRID_PLACEHOLDER': 4326,
                    'GEOMETRY_PLACEHOLDER': point,
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
