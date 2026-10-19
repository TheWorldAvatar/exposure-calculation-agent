"""Calculate trip exposure using raster tiles in their original CRS."""
from pathlib import Path
import sys

from psycopg2.extras import RealDictCursor
from tqdm import tqdm

from agent.calculation.trajectory import _prepare_trajectory, _persist_results, logger
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils import constants
from agent.utils.postgis_client import postgis_client


def _quote_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def trajectory_raster(calculation_input, *, timeline=False):
    rdf_type = calculation_input.calculation_metadata.rdf_type
    if rdf_type not in constants.TRAJECTORY_RASTER_TYPES:
        raise ValueError('Unsupported raster trajectory calculation')
    prepared = _prepare_trajectory(calculation_input, timeline=timeline)
    if prepared is None:
        return 'Trajectory time series is empty', 404
    _, proj4text, trips, native_times, sources = prepared
    dataset = get_exposure_dataset(calculation_input.exposure)

    filters = []
    filter_params = {}
    for index, (column, value) in enumerate(calculation_input.calculation_metadata.dataset_filter.items()):
        parameter = f'dataset_filter_{index}'
        filters.append(f'AND r.{_quote_identifier(column)} = %({parameter})s')
        filter_params[parameter] = value

    resources = Path(__file__).with_name('resources')
    replacements = dict(
        EXPOSURE_DATASET=_quote_identifier(dataset.table_name),
        GEOMETRY_COLUMN=_quote_identifier(dataset.geometry_column or constants.RASTER_GEOMETRY_COLUMN),
        AREA_COLUMN=_quote_identifier(dataset.area_column) if rdf_type != constants.TRAJECTORY_RASTER_AVERAGE else '',
        DATASET_FILTERS='\n'.join(filters))

    sql_paths = {
        constants.TRAJECTORY_AREA_WEIGHTED_SUM: 'area_weighted_sum_trajectory.sql',
        constants.TRAJECTORY_RASTER_AREA: 'raster_area_trajectory.sql',
        constants.TRAJECTORY_RASTER_AVERAGE: 'raster_average_trajectory.sql',
    }
    calculation_sql = (resources / sql_paths[rdf_type]).read_text().format(**replacements)
    srid_sql = (
        'SELECT DISTINCT ST_SRID(r.{GEOMETRY_COLUMN}) AS srid '
        'FROM {EXPOSURE_DATASET} r '
        'WHERE r.{GEOMETRY_COLUMN} IS NOT NULL {DATASET_FILTERS}'
    ).format(**replacements)
    params = {**filter_params, 'PROJ4_TEXT': proj4text,
              'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance}

    logger.info('Reading the SRID of filtered raster tiles')
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(srid_sql, filter_params)
            srids = [row['srid'] for row in cur.fetchall()]
            if srids:
                if len(srids) != 1 or srids[0] is None or srids[0] <= 0:
                    raise ValueError('Filtered raster tiles must share one known SRID')
                params['RASTER_SRID'] = srids[0]
                logger.info('Transforming AEQD trip buffers to raster SRID %s', srids[0])
            for trip in tqdm(trips, mininterval=60, ncols=80, file=sys.stdout):
                if not srids:
                    trip.set_exposure_result(0)
                    continue
                params['GEOMETRY_PLACEHOLDER'] = trip.trajectory.wkt
                cur.execute(calculation_sql, params)
                trip.set_exposure_result(cur.fetchone()['exposure_result'])

    _persist_results(calculation_input, trips, sources, native_times)
    logger.info('Trajectory calculation complete')
    return 'Trajectory calculation complete', 200
