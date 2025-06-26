import uuid
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils import constants
from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from shapely.geometry import LineString
from twa import agentlogging
from agent.utils.baselib_gateway import baselib_view, jpsBaseLibGW
from agent.utils.postgis_client import postgis_client
from agent.objects.trip import Trip
from pyproj import Transformer

logger = agentlogging.get_logger('dev')


def trajectory_count(calculation_input: CalculationInput):
    from agent.utils.kg_client import kg_client
    # subject must be a time series
    ts_client = TimeSeriesClient(calculation_input.subject)
    lowerbound = calculation_input.calculation_metadata.lowerbound
    upperbound = calculation_input.calculation_metadata.upperbound

    # convert upperbound and lowerbound into the correct types from string
    if upperbound is not None:
        upperbound = _convert_input_time_for_timeseries(
            upperbound, calculation_input.subject)

    if lowerbound is not None:
        lowerbound = _convert_input_time_for_timeseries(
            lowerbound, calculation_input.subject)

    # check if there is a trip instance attached to this trajectory
    trip_iri = _get_trip(calculation_input.subject)

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

    # Java object list
    postgis_point_list = trajectory_time_series.getValuesAsPoint(
        calculation_input.subject)

    srid = postgis_point_list[0].getSrid()

    # transform to EPSG 3857 to use metres later
    transformer = Transformer.from_crs(
        "EPSG:" + str(srid), "EPSG:3857", always_xy=True)
    points = [transformer.transform(p.getX(), p.getY())
              for p in postgis_point_list]

    logger.info('Processing trips')
    if trip_iri is not None:
        # split trajectory into trips
        trips = _process_trip(
            trajectory_time_series.getValuesAsInteger(trip_iri), points)
    else:
        # entire trajectory considered as a single trip
        trips = [Trip(trajectory=LineString(points))]

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/templates/count.sql", "r") as f:
        sql = f.read()

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            # create temp table for efficiency
            temp_table = 'temp_table'

            create_temp_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT ST_Transform(wkb_geometry, 3857) AS wkb_geometry
            FROM {exposure_dataset.table_name}
            """

            cur.execute(create_temp_sql)

            sql = sql.format(TEMP_TABLE=temp_table)

            for trip in trips:
                # two types of replacement, table name via python, variables via psycopg2,
                # supposed to be more secure against sql injection like this
                replacements = {
                    'GEOMETRY_PLACEHOLDER': trip.trajectory.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }

                cur.execute(sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    trip.set_exposure_result(query_result[0][0])

    # check if an existing result time series exists
    result_iri = _get_exposure_result(calculation_input)

    # create a new column sharing the same time series with trajectory if it does not exist
    if result_iri is None:
        result_iri = _instantiate_result(calculation_input)
        time_series_iri = kg_client.get_time_series(calculation_input.subject)
        ts_client.add_columns(time_series_iri=time_series_iri, data_iri=[
                              result_iri], class_list=[baselib_view.java.lang.Integer.TYPE])

    # create a Java time series object to upload to database
    result_time_series = _create_result_time_series(
        trips, result_iri=result_iri, time_list=trajectory_time_series.getTimes(), ts_client=ts_client)

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


def _convert_input_time_for_timeseries(time, point_iri: str):
    from agent.utils.kg_client import kg_client
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
            <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
        ?result a <{constants.EXPOSURE_RESULT}>;
            <{constants.BELONGS_TO}> ?derivation.
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


def _instantiate_result(calculation_input: CalculationInput):
    from agent.utils.kg_client import kg_client
    result_iri = constants.PREFIX_EXPOSURE + 'result/' + str(uuid.uuid4())
    derivation_iri = constants.PREFIX_DERIVATION + str(uuid.uuid4())
    query = f"""
    INSERT DATA{{
        <{result_iri}> a <{constants.EXPOSURE_RESULT}>;
            <{constants.BELONGS_TO}> <{derivation_iri}>.
        <{derivation_iri}> a <{constants.DERIVATION}>;
            <{constants.IS_DERIVED_FROM}> <{calculation_input.subject}>;
            <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>;
            <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
    }}
    """
    kg_client.remote_store_client.executeUpdate(query)
    return result_iri
