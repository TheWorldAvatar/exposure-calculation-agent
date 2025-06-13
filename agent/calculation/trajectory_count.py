from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from agent.utils.kg_client import kg_client
from shapely.geometry import LineString
from twa import agentlogging
from agent.utils.baselib_gateway import baselib_view, jpsBaseLibGW
from agent.utils.postgis_client import postgis_client
from agent.objects.trip import Trip

logger = agentlogging.get_logger('dev')


def trajectory_count(calculation_input: CalculationInput):
    # subject must be a time series
    ts_client = TimeSeriesClient(calculation_input.subject)
    lowerbound = calculation_input.calculation_metadata.lowerbound
    upperbound = calculation_input.calculation_metadata.upperbound

    # convert upperbound and lowerbound into the correct types from string
    if upperbound is not None:
        upperbound = convert_input_time_for_timeseries(
            upperbound, calculation_input.subject)

    if lowerbound is not None:
        lowerbound = convert_input_time_for_timeseries(
            lowerbound, calculation_input.subject)

    # check if there is a trip instance attached to this trajectory
    trip_iri = kg_client.get_trip(calculation_input.subject)

    data_iri_list = [calculation_input.subject]
    if trip_iri is not None:
        data_iri_list.append(trip_iri)

    # get time series data from database
    logger.info('Querying time series from database')
    trajectory_time_series = ts_client.get_time_series(
        data_iri_list=data_iri_list, lowerbound=lowerbound, upperbound=upperbound)

    if trajectory_time_series.getTimes().isEmpty():
        logger.info('Trajectory time series is empty')
        return None

    postgis_point_list = trajectory_time_series.getValuesAsPoint(
        calculation_input.subject)

    points = [(p.getX(), p.getY()) for p in postgis_point_list]

    trips = process_trip(
        trajectory_time_series.getValuesAsInteger(trip_iri), points)

    exposure_dataset = kg_client.get_exposure_dataset(
        calculation_input.exposure)

    with open("agent/calculation/templates/trajectory_count.sql", "r") as f:
        sql = f.read()

    sql = sql.format(TABLE_PLACEHOLDER=exposure_dataset.table_name)

    srid = postgis_point_list[0].getSrid()

    with postgis_client.connect() as conn:
        for trip in trips:
            line = trip.trajectory

            # two types of replacement, table name via python, variables via psycopg2,
            # supposed to be more secure against sql injection like this
            replacements = {
                'SRID_PLACEHOLDER': str(srid),
                'LINE_PLACEHOLDER': line.wkt,
                'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
            }
            query_result = postgis_client.execute_query(
                conn=conn, query=sql, params=replacements)
            trip.set_exposure_result(query_result[0][0])

    return ''


def process_trip(trip_index_array, points):
    """
    returns an array Trip objects
   """
    trips = []
    current_trip_index = trip_index_array[0]
    lowerbound_index = 0

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


def convert_input_time_for_timeseries(time, point_iri: str):
    # assumes time is in seconds or milliseconds, if an exception is thrown,
    # queries the time class from KG (e.g. java.time.Instant) and use the
    # parse method to parse time into the correct Java object
    try:
        # assume epoch seconds
        return int(time)
    except (ValueError, TypeError):
        class_name = kg_client.get_java_time_class(point_iri)
        time_clazz = baselib_view.java.lang.Class.forName(class_name)

        char_class = baselib_view.java.lang.Class.forName(
            "java.lang.CharSequence")
        param_types = jpsBaseLibGW.gateway.new_array(
            baselib_view.java.lang.Class, 1)
        param_types[0] = char_class

        java_string = baselib_view.java.lang.String(time)
        object_class = baselib_view.java.lang.Object
        args_array = jpsBaseLibGW.gateway.new_array(object_class, 1)
        args_array[0] = java_string

        return time_clazz.getMethod("parse", param_types).invoke(None, args_array)
