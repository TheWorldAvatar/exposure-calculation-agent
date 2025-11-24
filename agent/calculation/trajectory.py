from agent.calculation.shared_utils import instantiate_result_ontop
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils import constants
from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from shapely.geometry import LineString
from twa import agentlogging
from agent.utils.stack_gateway import stack_clients_view
from agent.utils.postgis_client import postgis_client
from agent.objects.trip import Trip
from pyproj import Transformer, CRS
import agent.utils.constants as constants
import json
from shapely import wkt
from datetime import datetime
from py4j.java_gateway import JavaObject
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')

rdf_type_to_sql_path = {
    constants.TRAJECTORY_COUNT: "agent/calculation/resources/count_trajectory.sql",
    constants.TRAJECTORY_AREA: "agent/calculation/resources/area_trajectory.sql",
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: "agent/calculation/resources/area_weighted_sum_trajectory.sql"
}

rdf_type_to_ts_class = {
    constants.TRAJECTORY_COUNT: stack_clients_view.java.lang.Integer.TYPE,
    constants.TRAJECTORY_AREA: stack_clients_view.java.lang.Double.TYPE,
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: stack_clients_view.java.lang.Double.TYPE
}


def trajectory(calculation_input: CalculationInput):
    from agent.utils.kg_client import kg_client
    # subject must be a time series
    ts_client = TimeSeriesClient(calculation_input.subject)
    lowerbound = calculation_input.calculation_metadata.lowerbound
    upperbound = calculation_input.calculation_metadata.upperbound

    # check if there is a trip instance attached to this trajectory
    trip_iri = _get_trip(calculation_input.subject)

    data_iri_list = [calculation_input.subject]
    if trip_iri is not None:
        data_iri_list.append(trip_iri)

    logger.info('Querying time series')

    points, trip_list, time_list = _get_time_series_sparql(
        calculation_input.subject, trip_iri, lowerbound, upperbound)

    if len(points) == 0:
        logger.info('Trajectory time series is empty')
        return None

    # create temporary centroid for AEQD projection
    centroid = LineString(points).envelope.centroid
    proj4text = f"+proj=aeqd +lat_0={centroid.y} +lon_0={centroid.x} +units=m +datum=WGS84 +no_defs"

    transformer = Transformer.from_crs(
        "EPSG:4326", CRS.from_proj4(proj4text), always_xy=True)
    points = [transformer.transform(p.x, p.y) for p in points]

    logger.info('Processing trips')
    if trip_iri is not None:
        # split trajectory into trips
        trips = _process_trip(trip_list, points)
    else:
        # entire trajectory considered as a single trip
        trips = [Trip(trajectory=LineString(points), lower_index=0,
                      upper_index=len(points) - 1)]

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open(rdf_type_to_sql_path[calculation_input.calculation_metadata.rdf_type], "r") as f:
        calculation_sql = f.read()

    # create temp table for efficiency
    temp_table = 'temp_table'
    if calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_AREA_WEIGHTED_SUM:
        with open("agent/calculation/resources/temp_table_area_weighted_trajectory.sql", "r") as f:
            temp_table_sql = f.read()
        temp_table_sql = temp_table_sql.format(
            TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name, PROJ4_TEXT=proj4text, GEOMETRY_COLUMN=exposure_dataset.geometry_column, VALUE_COLUMN=exposure_dataset.value_column, AREA_COLUMN=exposure_dataset.area_column)
    else:
        with open("agent/calculation/resources/temp_table_trajectory.sql", "r") as f:
            temp_table_sql = f.read()
        temp_table_sql = temp_table_sql.format(
            TEMP_TABLE=temp_table, EXPOSURE_DATASET=exposure_dataset.table_name, PROJ4_TEXT=proj4text, GEOMETRY_COLUMN=exposure_dataset.geometry_column)

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:

            cur.execute(temp_table_sql)

            calculation_sql = calculation_sql.format(TEMP_TABLE=temp_table)

            for trip in tqdm(trips, mininterval=60, ncols=80, file=sys.stdout):
                # two types of replacement, table name via python, variables via psycopg2,
                # supposed to be more secure against sql injection like this
                replacements = {
                    'GEOMETRY_PLACEHOLDER': trip.trajectory.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }

                cur.execute(calculation_sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    if query_result[0][0] is None:
                        trip.set_exposure_result(0)
                    else:
                        trip.set_exposure_result(query_result[0][0])

    # check if an existing result time series exists
    result_iri = _get_exposure_result(calculation_input)

    # create a new column sharing the same time series with trajectory if it does not exist
    if result_iri is None:
        instantiate_result_ontop(calculation_input=calculation_input)
        result_iri = _get_exposure_result(calculation_input)

    if kg_client.get_time_series(result_iri) is None:
        # add a column that shares the same time series with trajectory
        time_series_iri = kg_client.get_time_series(calculation_input.subject)
        ts_client.add_columns(time_series_iri=time_series_iri, data_iri=[result_iri], class_list=[
                              rdf_type_to_ts_class[calculation_input.calculation_metadata.rdf_type]])

    # create a Java time series object to upload to database
    result_time_series = _create_result_time_series(
        trips, result_iri=result_iri, time_list=time_list, ts_client=ts_client)

    # uploads data to database
    ts_client.add_time_series(result_time_series)

    return 'Trajectory count complete', 200


def _process_trip(trip_index_array, points):
    """
    returns a list of Trip objects
   """
    trips = []
    current_trip_index = trip_index_array[0]  # index given by trip calculation
    lowerbound_index = 0  # position in the trajectory array

    # records a new tuple every time the trip index changes
    for i in range(1, len(trip_index_array)):
        if trip_index_array[i] != current_trip_index:
            trips.append(
                Trip(upper_index=i-1, lower_index=lowerbound_index, trajectory=LineString(points[lowerbound_index:i-1])))
            lowerbound_index = i
            current_trip_index = trip_index_array[i]
        elif i == len(trip_index_array) - 1:
            trips.append(Trip(upper_index=i, lower_index=lowerbound_index,
                         trajectory=LineString(points[lowerbound_index:i])))

    return trips


def _create_result_time_series(trips: list[Trip], result_iri: str, time_list, ts_client: TimeSeriesClient):
    result_list = []

    for trip in trips:
        # repeat the same value for each portion of the trip in each time row
        size = trip.upper_index - trip.lower_index + 1
        temp_list = [trip.exposure_result] * size
        result_list.extend(temp_list)

    return ts_client.create_time_series(times=time_list, data_iri_list=[result_iri], values=[result_list])


def _get_trip(point_iri: str):
    from agent.utils.kg_client import kg_client
    query = f"""
    SELECT ?trip
    WHERE {{
        <{point_iri}> <{constants.HAS_TIME_SERIES}> ?time_series.
        ?trip <{constants.HAS_TIME_SERIES}> ?time_series;
            a <{constants.TRIP}>.
    }}
    """
    query_results = kg_client.remote_store_client.executeQuery(query)

    if query_results.isEmpty():
        return None
    elif query_results.length() > 1:
        raise Exception('More than 1 trip instance detected?')
    else:
        return query_results.getJSONObject(0).getString('trip')


def _get_exposure_result(calculation_input: CalculationInput):
    from agent.utils.kg_client import kg_client
    query = f"""
    SELECT ?result
    WHERE {{
        ?derivation <{constants.IS_DERIVED_FROM}> <{calculation_input.subject}>;
            <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>.
        ?result a <{constants.EXPOSURE_RESULT}>;
            <{constants.BELONGS_TO}> ?derivation;
            <{constants.HAS_CALCULATION_METHOD}> <{calculation_input.calculation_metadata.iri}>.
    }}
    """
    query_result = kg_client.remote_store_client.executeQuery(query)

    if query_result.isEmpty():
        return None
    elif query_result.length() == 1:
        return query_result.getJSONObject(0).getString('result')
    else:
        raise Exception('Unexpected query result size ' +
                        query_result.toString())


def _get_time_series_sparql(subject: str, trip: str, lowerbound, upperbound):
    from agent.utils.kg_client import kg_client
    values_list = [subject]
    if trip is not None:
        values_list.append(trip)
    time_series = kg_client.get_time_series_data(
        values_list, lowerbound, upperbound)

    points = [wkt.loads(s) for s in time_series.get_value_list(subject)]

    if trip is not None:
        trip_list = time_series.get_value_list(trip)
    else:
        trip_list = []

    return points, trip_list, time_series.get_timestamp_java(subject)
