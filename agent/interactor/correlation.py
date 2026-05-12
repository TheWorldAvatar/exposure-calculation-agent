from collections import defaultdict

from flask import Blueprint, request
from shapely import Point
from twa import agentlogging
from shapely.ops import transform
from pyproj import Transformer
from agent.correlation.correlation import calculate_correlation
from agent.correlation.post_process import post_process_correlation
from agent.utils.postgis_client import postgis_client
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')

correlation_blueprint = Blueprint(
    'correlation', __name__, url_prefix='/correlation')


@correlation_blueprint.route('/', methods=['POST'])
def calculate():
    calculate_correlation()
    return 'calculated correlation'


@correlation_blueprint.route('/post_process', methods=['POST'])
def post_process():
    return post_process_correlation()


@correlation_blueprint.route('/check_punggol', methods=['GET'])
def check_punggol():
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
