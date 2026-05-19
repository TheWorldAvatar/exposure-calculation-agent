from collections import defaultdict
from itertools import product

from flask import Blueprint, request
from shapely import Point
from twa import agentlogging
from shapely.ops import transform
from pyproj import Transformer
from agent.interactor.csv_export import _get_calculations
from agent.interactor.trigger_calculation import get_dataset_iri
from agent.stats.clustering import run_clustering
from agent.stats.correlation import calculate_correlation
from agent.stats.post_process import correlation_post_process
from agent.utils.postgis_client import postgis_client
from psycopg2.extras import RealDictCursor

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


@stats_blueprint.route('/check_punggol', methods=['GET'])
def check_punggol():
    # adhoc code that will be removed
    transformer = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True)

    transformer_back = Transformer.from_crs(
        "EPSG:3857", "EPSG:4326", always_xy=True)

    lat = request.args['lat']
    lng = request.args['lng']
    buffer = request.args['buffer']
    point = Point(lng, lat)

    projected_geom = transform(transformer.transform, point)
    buffered_geom = projected_geom.buffer(float(buffer))
    buffer_in_4326 = transform(
        transformer_back.transform, buffered_geom)

    hours = range(0, 24)
    months = [5, 6, 7]

    query = """
        WITH buffer AS (
            SELECT ST_GeomFromText(%(GEOMETRY_PLACEHOLDER)s, 4326) AS geom
        ),
        clipped_raster AS (
            SELECT ST_Clip(r.rast, b.geom) AS clipped
            FROM utci_raster_hourly r
            CROSS JOIN buffer b
            WHERE ST_Intersects(b.geom, r.rast)
            AND month = %(MONTH_PLACEHOLDER)s
            AND hour = %(HOUR_PLACEHOLDER)s
        )
        SELECT COALESCE(
            SUM((stats).sum) / NULLIF(SUM((stats).count), 0),
            0
        ) AS result
        FROM (
            SELECT ST_SummaryStats(clipped) AS stats
            FROM clipped_raster
        ) t;
    """
    results = defaultdict(list)
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for month in months:
                for hour in hours:
                    replacements = {
                        "GEOMETRY_PLACEHOLDER": buffer_in_4326.wkt,
                        "MONTH_PLACEHOLDER": month, "HOUR_PLACEHOLDER": hour}

                    cur.execute(query, replacements)

                    if cur.description:
                        query_result = cur.fetchall()
                        for row in query_result:
                            results[month].append(row['result'])

    return results
