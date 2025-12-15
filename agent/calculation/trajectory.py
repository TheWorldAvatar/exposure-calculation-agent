from zoneinfo import ZoneInfo
from agent.calculation.shared_utils import instantiate_result_ontop
from agent.objects.business_establishment import BusinessEstablishment, Schedule
from agent.objects.exposure_dataset import ExposureDataset, get_exposure_dataset
from agent.utils import constants
from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from shapely.geometry import LineString, Point
from twa import agentlogging
from agent.utils.stack_gateway import stack_clients_view
from agent.utils.postgis_client import postgis_client
from agent.objects.trip import Trip
from pyproj import Transformer, CRS
import agent.utils.constants as constants
from shapely import wkt
from tqdm import tqdm
import sys
import json
from datetime import datetime, date, time
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')

rdf_type_to_sql_path = {
    constants.TRAJECTORY_COUNT: "agent/calculation/resources/count_trajectory.sql",
    constants.TRAJECTORY_AREA: "agent/calculation/resources/area_trajectory.sql",
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: "agent/calculation/resources/area_weighted_sum_trajectory.sql",
    constants.TRAJECTORY_TIME_FILTER_COUNT: "agent/calculation/resources/trajectory_iri.sql"
}

rdf_type_to_ts_class = {
    constants.TRAJECTORY_COUNT: stack_clients_view.java.lang.Integer.TYPE,
    constants.TRAJECTORY_AREA: stack_clients_view.java.lang.Double.TYPE,
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: stack_clients_view.java.lang.Double.TYPE,
    constants.TRAJECTORY_TIME_FILTER_COUNT: stack_clients_view.java.lang.Integer.TYPE
}

# Important assumptions


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

    points, trip_list, java_time_list, timestamp_list = _get_time_series_sparql(
        calculation_input.subject, trip_iri, lowerbound, upperbound)

    if len(points) == 0:
        logger.info('Trajectory time series is empty')
        return None

    # create temporary centroid for AEQD projection
    centroid = LineString(points).envelope.centroid
    proj4text = f"+proj=aeqd +lat_0={centroid.y} +lon_0={centroid.x} +units=m +datum=WGS84 +no_defs"

    transformer = Transformer.from_crs(
        "EPSG:4326", CRS.from_proj4(proj4text), always_xy=True)
    points = [Point(transformer.transform(p.x, p.y)) for p in points]

    logger.info('Processing trips')
    if trip_iri is not None:
        # split trajectory into trips
        trips = _process_trip(trip_list, points, timestamp_list)
    else:
        # entire trajectory considered as a single trip
        trips = [Trip(full_points_list=points,
                      lower_index=0,
                      upper_index=len(points) - 1,
                      full_time_list=timestamp_list)]

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open(rdf_type_to_sql_path[calculation_input.calculation_metadata.rdf_type], "r") as f:
        calculation_sql = f.read()

    # create temp table for efficiency
    temp_table = 'temp_table'
    columns = ["""ST_Transform(ST_Transform({GEOMETRY_COLUMN}, 4326), '{PROJ4_TEXT}') AS wkb_geometry""".format(
        GEOMETRY_COLUMN=exposure_dataset.geometry_column, PROJ4_TEXT=proj4text)]

    # different calculation types require different additional columns
    # area weighted sum requires area and associated value of each pixel
    # time filter needs the iri of the feature for time filtering later, where data are stored as triples
    if calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_AREA_WEIGHTED_SUM:
        columns.append(exposure_dataset.area_column + ' AS area')
        columns.append(exposure_dataset.value_columhn + ' AS val')
    elif calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_TIME_FILTER_COUNT:
        columns.append(exposure_dataset.iri_column + ' AS iri')

    select_clause = ",\n       ".join(columns)

    with open("agent/calculation/resources/temp_table_trajectory.sql", "r") as f:
        temp_table_sql = f.read()
    temp_table_sql = temp_table_sql.format(
        TEMP_TABLE=temp_table, SELECT_CLAUSE=select_clause, EXPOSURE_DATASET=exposure_dataset.table_name)

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
                    if calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_TIME_FILTER_COUNT:
                        iri_list = [row['iri'] for row in query_result]
                        trip.set_iri_list(iri_list)
                    else:
                        if query_result[0]['exposure_result'] is None:
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

    if calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_TIME_FILTER_COUNT:
        timezone = _get_time_zone(centroid)
        _process_time_filter(trips, timezone, exposure_dataset)

    # create a Java time series object to upload to database
    result_time_series = _create_result_time_series(
        trips, result_iri=result_iri, time_list=java_time_list, ts_client=ts_client)

    # uploads data to database
    ts_client.add_time_series(result_time_series)

    return 'Trajectory count complete', 200


def _process_trip(trip_index_array, points: list[Point], timestamp_list):
    """
    returns a list of Trip objects
   """
    trips = []
    current_trip_index = trip_index_array[0]  # index given by trip calculation
    lowerbound_index = 0  # position in the trajectory array

    # records a new trip every time the trip index changes
    for i in range(1, len(trip_index_array)):
        if trip_index_array[i] != current_trip_index:
            upperbound_index = i - 1

            trips.append(
                Trip(upper_index=upperbound_index,
                     lower_index=lowerbound_index,
                     full_points_list=points,
                     full_time_list=timestamp_list))

            lowerbound_index = i
            current_trip_index = trip_index_array[i]
        elif i == len(trip_index_array) - 1:
            upperbound_index = i

            trips.append(Trip(upper_index=upperbound_index,
                              lower_index=lowerbound_index,
                         full_points_list=points,
                         full_time_list=timestamp_list))

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

    return points, trip_list, time_series.get_timestamp_java(subject), time_series.get_timestamp(subject)


def _get_time_zone(centroid: Point):
    from agent.utils.kg_client import kg_client

    query = f"""
    PREFIX exposure: <https://www.theworldavatar.com/kg/ontoexposure/>
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    SELECT ?tzid
    WHERE {{
        ?x a exposure:TimeZone; geo:asWKT ?timezone_wkt; exposure:tzid ?tzid.
        FILTER(geof:sfWithin("{centroid.wkt}"^^geo:wktLiteral, ?timezone_wkt))
    }}
    """

    query_result = json.loads(
        kg_client.remote_store_client.executeQuery(query).toString())

    if len(query_result) == 1:
        return query_result[0]['tzid']
    else:
        raise Exception('Unexpected query size while getting time zone')


def _process_time_filter(trips: list[Trip], timezone: str, exposure_dataset: ExposureDataset):
    # check if trips fall into range of datasets
    tz = ZoneInfo(timezone)
    trips_to_consider = []
    if None not in (exposure_dataset.start_date, exposure_dataset.end_date):
        for trip in trips:
            if exposure_dataset.start_date <= trip.lowerbound_time.astimezone(tz).date() <= trip.upperbound_time.astimezone(tz).date() <= exposure_dataset.end_date:
                trips_to_consider.append(trip)
    else:
        logger.warning(
            'Dataset start and end dates are not instantiated, hence ignored')
        trips_to_consider = trip

    # there are no valid trips
    if not trips_to_consider:
        return

    # collect a flattened iri list of all intersected establishments
    iri_list = []
    for trip in trips_to_consider:
        iri_list += trip.iri_list

    # there are no features to consider
    if not iri_list:
        return

    # remove duplicates
    iri_set = set(iri_list)
    business_establishments = {
        iri: BusinessEstablishment(iri) for iri in iri_set}

    _set_business_start_end(business_establishments)
    _set_schedules(business_establishments)
    _calculate_with_time_filter(
        trips_to_consider, tz, business_establishments)


def _set_business_start_end(business_establishments: dict[str, BusinessEstablishment]):
    from agent.utils.kg_client import kg_client
    varname = 'feature'
    values = " ".join(f"<{iri}>" for iri in business_establishments)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    with open("agent/calculation/resources/business_start_end.sparql", "r") as f:
        business_start_end_sparql = f.read().format(
            VALUES_CLAUSE=values_clause, VARNAME=varname)

    query_result = json.loads(kg_client.remote_store_client.executeQuery(
        business_start_end_sparql).toString())

    # please refer to the template for the variable names
    for entry in query_result:
        # support both date and timestamp
        try:
            start = date.fromisoformat(entry['start_time'])
            end = date.fromisoformat(entry['end_time'])
        except Exception():
            start = datetime.fromisoformat(entry['start_time'])
            end = datetime.fromisoformat(entry['end_time'])
        business_establishments[entry[varname]].add_business_start_and_end(
            business_start=start, business_end=end)


def _set_schedules(business_establishments: dict[str, BusinessEstablishment]):
    from agent.utils.kg_client import kg_client
    varname = 'feature'
    values = " ".join(f"<{iri}>" for iri in business_establishments)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    with open("agent/calculation/resources/opening_hours.sparql", "r") as f:
        opening_hours_sparql = f.read().format(
            VALUES_CLAUSE=values_clause, VARNAME=varname)

    query_result = json.loads(kg_client.remote_store_client.executeQuery(
        opening_hours_sparql).toString())

    feature_to_schedule_dict = {}
    schedule_to_type_dict = {}
    schedule_days_dict = {}
    schedule_start_date_dict = {}
    schedule_end_date_dict = {}
    schedule_to_end_time_dict = {}
    schedule_to_start_time_dict = {}

    # one schedule can repeat over multiple days, but can only have one time range
    # please refer to the template for the variable names
    for entry in query_result:
        feature = entry['feature']
        schedule = entry['schedule']
        day = entry['reccurent_day']
        schedule_type = entry['schedule_type']
        try:
            schedule_start_date = date.fromisoformat(
                entry['schedule_start_date'])
            schedule_end_date = date.fromisoformat(entry['schedule_end_date'])
            start_time = time.fromisoformat(entry['start_time'])
            end_time = time.fromisoformat(entry['end_time'])
        except Exception as e:
            logger.error(e)
            logger.error(entry)
            continue

        if feature in feature_to_schedule_dict:
            if schedule not in feature_to_schedule_dict[feature]:
                feature_to_schedule_dict[feature].append(schedule)
        else:
            feature_to_schedule_dict[feature] = [schedule]

        if schedule in schedule_days_dict:
            schedule_days_dict[schedule].append(day)
        else:
            schedule_days_dict[schedule] = [day]

        schedule_start_date_dict[schedule] = schedule_start_date
        schedule_end_date_dict[schedule] = schedule_end_date

        schedule_to_start_time_dict[schedule] = start_time
        schedule_to_end_time_dict[schedule] = end_time

        schedule_to_type_dict[schedule] = schedule_type

    for feature in feature_to_schedule_dict:
        schedules = feature_to_schedule_dict[feature]

        for schedule in schedules:
            days = schedule_days_dict[schedule]
            start_date = schedule_start_date_dict[schedule]
            end_date = schedule_end_date_dict[schedule]
            start_time = schedule_to_start_time_dict[schedule]
            end_time = schedule_to_end_time_dict[schedule]
            schedule_type = schedule_to_type_dict[schedule]

            be_schedule = Schedule(days=days, start_date=start_date, end_date=end_date,
                                   start_time=start_time, end_time=end_time, schedule_type=schedule_type)

            business_establishments[feature].add_schedule(be_schedule)


def _calculate_with_time_filter(trips: list[Trip], timezone: ZoneInfo, business_establishments: dict[str, BusinessEstablishment]):
    for trip in trips:
        number = 0
        for iri in trip.iri_list:
            business_establishment = business_establishments[iri]

            if business_establishment.business_exists(lowerbound_time=trip.lowerbound_time, upperbound_time=trip.upperbound_time) and business_establishment.is_open_full_containment(
                    lowerbound_time=trip.lowerbound_time,
                    upperbound_time=trip.upperbound_time,
                    timezone=timezone):
                number += 1

        trip.set_exposure_result(number)
