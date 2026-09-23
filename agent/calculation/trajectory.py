from zoneinfo import ZoneInfo
from dataclasses import replace
from agent.calculation.shared_utils import instantiate_result_ontop
from agent.objects.business_establishment import BusinessEstablishment
from agent.objects.exposure_dataset import ExposureDataset, get_exposure_dataset
from agent.objects.schedule import AdHocSchedule, RegularSchedule, SchedulePeriod
from agent.utils import constants
from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from shapely.geometry import MultiPoint, Point
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
from datetime import datetime, date, time, timedelta
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')

rdf_type_to_sql_path = {
    constants.TRAJECTORY_COUNT: "agent/calculation/resources/count_trajectory.sql",
    constants.TRAJECTORY_AREA: "agent/calculation/resources/area_trajectory.sql",
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: "agent/calculation/resources/area_weighted_sum_trajectory.sql",
    constants.TRAJECTORY_TIME_FILTER_COUNT: "agent/calculation/resources/trajectory_iri.sql",
    constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED: "agent/calculation/resources/trajectory_iri.sql"
}

rdf_type_to_ts_class = {
    constants.TRAJECTORY_COUNT: stack_clients_view.java.lang.Integer.TYPE,
    constants.TRAJECTORY_AREA: stack_clients_view.java.lang.Double.TYPE,
    constants.TRAJECTORY_AREA_WEIGHTED_SUM: stack_clients_view.java.lang.Double.TYPE,
    constants.TRAJECTORY_TIME_FILTER_COUNT: stack_clients_view.java.lang.Integer.TYPE,
    constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED: stack_clients_view.java.lang.Integer.TYPE
}


def trajectory(calculation_input: CalculationInput):
    lowerbound = calculation_input.calculation_metadata.lowerbound
    upperbound = calculation_input.calculation_metadata.upperbound

    logger.info('Querying time series')
    points, trip_list, java_time_list, timestamp_list, sources = _load_trajectories(
        calculation_input.subject, lowerbound, upperbound)

    if len(points) == 0:
        logger.info('Trajectory time series is empty')
        return 'Trajectory time series is empty', 404

    # create temporary centroid for AEQD projection
    centroid = MultiPoint(points).envelope.centroid
    proj4text = f"+proj=aeqd +lat_0={centroid.y} +lon_0={centroid.x} +units=m +datum=WGS84 +no_defs"

    transformer = Transformer.from_crs(
        "EPSG:4326", CRS.from_proj4(proj4text), always_xy=True)
    points = [Point(transformer.transform(p.x, p.y)) for p in points]

    logger.info('Processing trips')
    if trip_list:
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
    if exposure_dataset.geometry_column is not None:
        geometry_column = exposure_dataset.geometry_column
    else:
        geometry_column = constants.VECTOR_GEOMETRY_COLUMN

    columns = ["""ST_Transform(ST_Transform({GEOMETRY_COLUMN}, 4326), '{PROJ4_TEXT}') AS wkb_geometry""".format(
        GEOMETRY_COLUMN=geometry_column, PROJ4_TEXT=proj4text)]

    # different calculation types require different additional columns
    # area weighted sum requires area and associated value of each pixel
    # time filter needs the iri of the feature for time filtering later, where data are stored as triples
    if calculation_input.calculation_metadata.rdf_type == constants.TRAJECTORY_AREA_WEIGHTED_SUM:
        columns.append(exposure_dataset.area_column + ' AS area')
    elif calculation_input.calculation_metadata.rdf_type in [constants.TRAJECTORY_TIME_FILTER_COUNT, constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED]:
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
                    if calculation_input.calculation_metadata.rdf_type in [constants.TRAJECTORY_TIME_FILTER_COUNT, constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED]:
                        iri_wkt_dict = {row['iri']: row['wkt']
                                        for row in query_result}
                        trip.set_iri_wkt_dict(iri_wkt_dict)
                    else:
                        # 1 trip is expected to have one row of result
                        if query_result[0]['exposure_result'] is None:
                            trip.set_exposure_result(0)
                        else:
                            trip.set_exposure_result(
                                query_result[0]['exposure_result'])

    if calculation_input.calculation_metadata.rdf_type in [constants.TRAJECTORY_TIME_FILTER_COUNT, constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED]:
        timezone = _get_time_zone(centroid)
        _process_time_filter(trips=trips, timezone=timezone,
                             exposure_dataset=exposure_dataset, calculation_type=calculation_input.calculation_metadata.rdf_type)

    _persist_results(calculation_input, trips, sources, java_time_list)

    complete_message = 'Trajectory calculation complete'
    logger.info(complete_message)

    return complete_message, 200


def _process_trip(trip_index_array, points: list[Point], timestamp_list):
    """
    returns a list of Trip objects
   """
    trips = []
    start = 0
    for end in range(1, len(trip_index_array) + 1):
        if end == len(trip_index_array) or trip_index_array[end] != trip_index_array[start]:
            trips.append(Trip(lower_index=start, upper_index=end - 1,
                              full_points_list=points, full_time_list=timestamp_list))
            start = end
    return trips


def _load_trajectories(subject, lowerbound, upperbound):
    """Merge observations chronologically, preserving source and native time.

    A list denotes jointly processed trajectories belonging to one person.
    Ownership must be resolved by the caller at the authentication boundary.
    """
    combined = isinstance(subject, list)
    subjects = list(dict.fromkeys(subject)) if combined else [subject]
    if not subjects or any(not isinstance(s, str) or not s for s in subjects):
        raise ValueError('Provide at least one trajectory point IRI')
    rows = []
    for point_iri in sorted(subjects):
        trip_iri = _get_trip(point_iri)
        points, labels, native_times, timestamps = _get_time_series_sparql(
            point_iri, trip_iri, lowerbound, upperbound)
        if not points:
            continue
        if combined and trip_iri is None:
            raise ValueError('Run joint trip detection for all devices before calculating exposure')
        if len(points) != len(timestamps) or len(points) != len(native_times):
            raise ValueError('Trajectory values and timestamps are not aligned')
        if trip_iri is not None and (len(labels) != len(points) or any(v is None for v in labels)):
            raise ValueError('Every trajectory observation must have a trip label')
        if trip_iri is not None:
            labels = [int(label) for label in labels]
        for i, point in enumerate(points):
            if timestamps[i].utcoffset() is None:
                raise ValueError('Trajectory timestamps must include a timezone')
            rows.append((timestamps[i], point_iri, i, point,
                         labels[i] if trip_iri is not None else None, native_times[i]))
    rows.sort(key=lambda row: (row[0], row[1], row[2]))
    if combined:
        seen = set()
        previous = None
        for i, row in enumerate(rows):
            label = row[4]
            if i and row[0] == rows[i - 1][0] and label != previous:
                raise ValueError('Conflicting trip indices at the same timestamp')
            if i == 0 or label != previous:
                if label != 0 and label in seen:
                    raise ValueError('Nonzero trip index occurs in separate groups; rerun joint trip detection')
                seen.add(label)
            previous = label
    return ([r[3] for r in rows],
            [r[4] for r in rows] if rows and rows[0][4] is not None else [],
            [r[5] for r in rows], [r[0] for r in rows], [r[1] for r in rows])


def _persist_results(calculation_input, trips, sources, native_times):
    """Broadcast combined trip results back to each source time series."""
    from agent.utils.kg_client import kg_client
    values = [trip.exposure_result for trip in trips
              for _ in range(trip.upper_index - trip.lower_index + 1)]
    if len(values) != len(sources) or len(values) != len(native_times):
        raise ValueError('Exposure results and source observations are not aligned')
    for subject in dict.fromkeys(sources):
        source_input = replace(calculation_input, subject=subject)
        result_iri = _get_exposure_result(source_input)
        if result_iri is None:
            instantiate_result_ontop(calculation_input=source_input)
            result_iri = _get_exposure_result(source_input)
            if result_iri is None:
                raise RuntimeError('Failed to obtain new result IRI')
        ts_client = TimeSeriesClient(subject)
        if kg_client.get_time_series(result_iri) is None:
            ts_client.add_columns(
                time_series_iri=kg_client.get_time_series(subject), data_iri=[result_iri],
                class_list=[rdf_type_to_ts_class[calculation_input.calculation_metadata.rdf_type]])
        indices = [i for i, source in enumerate(sources) if source == subject]
        result = ts_client.create_time_series(
            times=[native_times[i] for i in indices], data_iri_list=[result_iri],
            values=[[values[i] for i in indices]])
        ts_client.add_time_series(result)


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
    SELECT DISTINCT ?trip
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

    try:
        query_result = kg_client.remote_store_client.executeQuery(query)
    except:
        logger.warning('Federated query failed, trying direct ontop query')
        query_result = kg_client.ontop_client.executeQuery(query)

    if query_result.isEmpty():
        logger.info(query)
        return None
    elif query_result.length() == 1:
        return query_result.getJSONObject(0).getString('result')
    else:
        logger.info(query)
        raise Exception('Found more than one exposure result: ' +
                        query_result.toString())


def _get_time_series_sparql(subject: str, trip: str, lowerbound, upperbound):
    from agent.utils.kg_client import kg_client

    # check time class, exception will be thrown if checks fail
    kg_client.check_time_class(subject)

    values_list = [subject]
    if trip is not None:
        values_list.append(trip)
    time_series = kg_client.get_time_series_data(
        values_list, lowerbound, upperbound)

    if not time_series.get_value_list(subject):
        return [], [], [], []

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


def _process_time_filter(trips: list[Trip], timezone: str, exposure_dataset: ExposureDataset, calculation_type: str):
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
        trips_to_consider = trips

    # there are no valid trips
    if not trips_to_consider:
        return

    # collect a flattened iri list of all intersected establishments
    iri_list = []
    for trip in trips_to_consider:
        iri_list += trip.get_iri_list()

    # there are no features to consider
    if not iri_list:
        return

    # combine dicts holding wkt values, some features may appear in multiple trips
    combined_iri_wkt_dict = {}
    for trip in trips_to_consider:
        combined_iri_wkt_dict.update(trip.iri_wkt_dict)

    # remove duplicates
    iri_set = set(iri_list)
    business_establishments = {
        iri: BusinessEstablishment(iri=iri, wkt_string=combined_iri_wkt_dict[iri]) for iri in iri_set}

    _set_business_start_end(business_establishments)
    _set_regular_schedules(business_establishments)
    _set_adhoc_schedules(business_establishments)
    if calculation_type == constants.TRAJECTORY_TIME_FILTER_COUNT:
        _opening_hours_filter_full_containment(
            trips_to_consider, tz, business_establishments)
    elif calculation_type == constants.TRAJECTORY_TIME_FILTER_COUNT_DETAILED:
        _opening_hours_filter_closest_point(
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


def _set_adhoc_schedules(business_establishments: dict[str, BusinessEstablishment]):
    from agent.utils.kg_client import kg_client
    varname = 'feature'
    values = " ".join(f"<{iri}>" for iri in business_establishments)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    with open("agent/calculation/resources/adhoc_opening_hours.sparql", "r") as f:
        opening_hours_sparql = f.read().format(
            VALUES_CLAUSE=values_clause, VARNAME=varname)

    query_result = json.loads(kg_client.remote_store_client.executeQuery(
        opening_hours_sparql).toString())

    feature_to_schedule_dict = {}  # multiple schedules allowed
    schedule_start_date_dict = {}  # single value only, optional
    schedule_end_date_dict = {}  # single value only, optional
    schedule_to_period_dict = {}  # multiple periods allowed, e.g. 1000-1200, 1300-1700
    period_to_start_time_dict = {}  # single value only, optional
    period_to_end_time_dict = {}  # single value only, optional
    schedule_to_entries_dict = {}  # multiple entries allowed

    for entry in query_result:
        # compulsory variables
        feature = entry['feature']
        schedule = entry['schedule']
        entry_date = entry['entry_date']

        if feature not in feature_to_schedule_dict:
            feature_to_schedule_dict[feature] = set()

        feature_to_schedule_dict[feature].add(schedule)

        if schedule not in schedule_to_entries_dict:
            schedule_to_entries_dict[schedule] = set()

        if schedule not in schedule_to_period_dict:
            schedule_to_period_dict[schedule] = set()

        schedule_to_entries_dict[schedule].add(date.fromisoformat(entry_date))

        if 'schedule_start_date' in entry:
            schedule_start_date_dict[schedule] = date.fromisoformat(
                entry['schedule_start_date'])

        if 'schedule_end_date' in entry:
            schedule_end_date_dict[schedule] = date.fromisoformat(
                entry['schedule_end_date'])

        if 'timeperiod' in entry:
            # if period does not exist it is assumed to be closed for the day
            period = entry['timeperiod']
            schedule_to_period_dict[schedule].add(period)
            period_to_start_time_dict[period] = time.fromisoformat(
                entry['start_time'])
            period_to_end_time_dict[period] = time.fromisoformat(
                entry['end_time'])

    for feature in feature_to_schedule_dict:
        schedules = feature_to_schedule_dict[feature]

        for schedule in schedules:
            entry_dates = schedule_to_entries_dict[schedule]

            schedule_periods = [SchedulePeriod(start_time=period_to_start_time_dict[period],
                                               end_time=period_to_end_time_dict[period]) for period in schedule_to_period_dict[schedule]]

            ad_hoc_schedule = AdHocSchedule(
                iri=schedule, entry_dates=entry_dates)

            if schedule in schedule_start_date_dict and schedule in schedule_end_date_dict:
                ad_hoc_schedule.set_start_date(
                    schedule_start_date_dict[schedule])
                ad_hoc_schedule.set_end_date(schedule_end_date_dict[schedule])

            for schedule_period in schedule_periods:
                ad_hoc_schedule.add_period(schedule_period)

            business_establishments[feature].add_ad_hoc_schedule(
                ad_hoc_schedule)


def _set_regular_schedules(business_establishments: dict[str, BusinessEstablishment]):
    from agent.utils.kg_client import kg_client
    varname = 'feature'
    values = " ".join(f"<{iri}>" for iri in business_establishments)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    with open("agent/calculation/resources/regular_opening_hours.sparql", "r") as f:
        opening_hours_sparql = f.read().format(
            VALUES_CLAUSE=values_clause, VARNAME=varname)

    query_result = json.loads(kg_client.remote_store_client.executeQuery(
        opening_hours_sparql).toString())

    feature_to_schedule_dict = {}  # multiple schedules allowed
    schedule_days_dict = {}  # multiple days allowed
    schedule_start_date_dict = {}  # single value only
    schedule_end_date_dict = {}  # single value only
    schedule_to_period_dict = {}  # multiple periods allowed, e.g. 1000-1200, 1300-1700
    period_to_start_time_dict = {}  # single value only
    period_to_end_time_dict = {}  # single value only

    # one schedule can repeat over multiple days, but can only have one time range
    # please refer to the template for the variable names
    for entry in query_result:
        # compulsory variables
        feature = entry['feature']
        schedule = entry['schedule']
        day = entry['reccurent_day']

        if feature not in feature_to_schedule_dict:
            feature_to_schedule_dict[feature] = set()

        feature_to_schedule_dict[feature].add(schedule)

        if schedule not in schedule_days_dict:
            schedule_days_dict[schedule] = set()

        schedule_days_dict[schedule].add(day)

        if schedule not in schedule_to_period_dict:
            schedule_to_period_dict[schedule] = set()

        if 'schedule_start_date' in entry:
            schedule_start_date_dict[schedule] = date.fromisoformat(
                entry['schedule_start_date'])

        if 'schedule_end_date' in entry:
            schedule_end_date_dict[schedule] = date.fromisoformat(
                entry['schedule_end_date'])

        if 'timeperiod' in entry:
            # if period does not exist it is assumed to be closed for the day
            period = entry['timeperiod']
            schedule_to_period_dict[schedule].add(period)
            period_to_start_time_dict[period] = time.fromisoformat(
                entry['start_time'])
            period_to_end_time_dict[period] = time.fromisoformat(
                entry['end_time'])

    for feature in feature_to_schedule_dict:
        schedules = feature_to_schedule_dict[feature]

        for schedule in schedules:
            days = schedule_days_dict[schedule]

            schedule_periods = [SchedulePeriod(start_time=period_to_start_time_dict[period],
                                               end_time=period_to_end_time_dict[period]) for period in schedule_to_period_dict[schedule]]

            regular_schedule = RegularSchedule(iri=schedule, days=days)

            if schedule in schedule_start_date_dict and schedule in schedule_end_date_dict:
                regular_schedule.set_start_date(
                    schedule_start_date_dict[schedule])
                regular_schedule.set_end_date(schedule_end_date_dict[schedule])

            for schedule_period in schedule_periods:
                regular_schedule.add_period(schedule_period)

            business_establishments[feature].add_regular_schedule(
                regular_schedule)


def _opening_hours_filter_full_containment(trips: list[Trip], timezone: ZoneInfo, business_establishments: dict[str, BusinessEstablishment]):
    for trip in trips:
        number = 0
        iri_list = []

        trip_lb = trip.lowerbound_time.astimezone(timezone)
        trip_ub = trip.upperbound_time.astimezone(timezone)

        for iri in trip.get_iri_list():
            business_establishment = business_establishments[iri]

            if business_establishment.business_exists(lowerbound_time=trip_lb, upperbound_time=trip_ub) \
                    and _is_open_trip_full_containment(trip_lb=trip_lb, trip_ub=trip_ub, business_establishment=business_establishment):
                number += 1
                iri_list.append(iri)

        trip.set_exposure_result(number)


def _is_open_trip_full_containment(trip_lb: datetime, trip_ub: datetime, business_establishment: BusinessEstablishment):
    datetime_ranges = _split_by_day(start=trip_lb, end=trip_ub)

    # each portion of the trip needs to be within the opening hours
    all_open = all(
        business_establishment.is_open_full_containment(
            lowerbound_time=dt_start,
            upperbound_time=dt_end
        )
        for dt_start, dt_end in datetime_ranges
    )

    return all_open


def _opening_hours_filter_closest_point(trips: list[Trip], timezone: ZoneInfo, business_establishments: dict[str, BusinessEstablishment]):
    for trip in trips:
        number = 0
        iri_list = []
        trip_lb = trip.lowerbound_time.astimezone(timezone)
        trip_ub = trip.upperbound_time.astimezone(timezone)

        for iri in trip.get_iri_list():
            business_establishment = business_establishments[iri]

            if business_establishment.business_exists(lowerbound_time=trip_lb, upperbound_time=trip_ub) \
                    and _is_open_trip_partial_overlap(trip_lb=trip_lb, trip_ub=trip_ub, business_establishment=business_establishment) \
                    and business_establishment.is_open_closest_point(trip=trip):
                number += 1
                iri_list.append(iri)

        trip.set_exposure_result(number)


def _is_open_trip_partial_overlap(trip_lb: datetime, trip_ub: datetime, business_establishment: BusinessEstablishment):
    datetime_ranges = _split_by_day(start=trip_lb, end=trip_ub)

    any_overlap = any(
        business_establishment.is_open_partial_overlap(
            lowerbound_time=dt_start,
            upperbound_time=dt_end
        )
        for dt_start, dt_end in datetime_ranges
    )

    return any_overlap


def _split_by_day(start: datetime, end: datetime):
    if start > end:
        raise ValueError("start must be <= end")

    result = []
    current = start

    while current.date() < end.date():
        day_end = datetime.combine(
            current.date(),
            time(23, 59),
            tzinfo=current.tzinfo,
        )
        result.append((current, day_end))
        current = day_end + timedelta(minutes=1)

    # last segment
    result.append((current, end))
    return result
