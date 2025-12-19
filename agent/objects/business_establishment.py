# class to store business start/end dates and opening hours
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
from twa import agentlogging
from enum import StrEnum
from shapely import wkt
from shapely.strtree import STRtree

from agent.objects.schedule import Schedule
from agent.objects.trip import Trip

logger = agentlogging.get_logger('dev')


class ScheduleType(StrEnum):
    REGULAR = "regular"
    AD_HOC = "ad hoc"


class BusinessEstablishment():
    def __init__(self, iri, wkt_string):
        self.iri = iri
        self.start_and_end = []
        self.regular_schedules: list[Schedule] = []

        # key is isoweekday, each day can have multiple schedules but the schedules should not overlap
        self.regular_schedule_dict: dict[int, list[Schedule]] = {}
        self.geom = wkt.loads(wkt_string)

    def add_business_start_and_end(self, business_start, business_end):
        if (business_start, business_end) in self.start_and_end:
            raise Exception('Duplicate business_start and business_end pair')
        self.start_and_end.append((business_start, business_end))

    def add_schedule(self, schedule: Schedule):
        # check if new schedule overlaps with any existing schedules
        if schedule.schedule_type == ScheduleType.REGULAR:
            for s in self.regular_schedules:
                if schedule.start_date <= s.end_date and s.start_date <= schedule.end_date:
                    if set(s.days) & set(schedule.days):
                        raise Exception('Overlapping schedule detected')

            self.regular_schedules.append(schedule)
            for day in schedule.days:
                if day in self.regular_schedule_dict:
                    self.regular_schedule_dict[day].append(schedule)
                else:
                    self.regular_schedule_dict[day] = [schedule]
        else:
            raise Exception('Only supporting regular schedules at the moment')

    def business_exists(self, lowerbound_time: datetime, upperbound_time: datetime):
        if not self.start_and_end:
            logger.info(
                f"<{self.iri}> has no business start/end, assumed to exist")
            return True
        for start, end in self.start_and_end:
            if isinstance(start, datetime) and isinstance(end, datetime):
                return start <= lowerbound_time and upperbound_time <= end
            elif isinstance(start, date) and isinstance(end, date):
                return start <= lowerbound_time.date() and upperbound_time.date() <= end
            else:
                raise Exception('Unsupported type for business start and end')

    def is_open_full_containment(self, lowerbound_time: datetime, upperbound_time: datetime):
        # upper and lowerbound times are completely within opening hours
        if not self.regular_schedules:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True

        if lowerbound_time.isoweekday() not in self.regular_schedule_dict.keys():
            # there is no schedule for the day
            return False

        for schedule in self.regular_schedule_dict[lowerbound_time.isoweekday()]:
            # check if trip spans across multiple dates
            trip_span_days = (upperbound_time.date() -
                              lowerbound_time.date()).days

            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(lowerbound_time.date()):
                for period in schedule.periods:
                    if period.start_time <= period.end_time and trip_span_days == 0:
                        # same day range, e.g. 10:00 - 22:00
                        return period.start_time <= lowerbound_time.time() <= upperbound_time.time() <= period.end_time
                    else:
                        # one of the ranges crosses midnight
                        opening_seconds = _to_seconds(period.start_time)
                        closing_seconds = _to_seconds(period.end_time)

                        if closing_seconds <= opening_seconds:
                            closing_seconds += 24*60*60

                        start_seconds = _to_seconds(lowerbound_time.time())
                        end_seconds = _to_seconds(upperbound_time.time())

                        # adjust for trip crossing midnight
                        if trip_span_days > 1:
                            raise Exception(
                                'Trips spanning more than 2 days are not supported')
                        if trip_span_days == 1:
                            end_seconds += 24*60*60

                        return opening_seconds <= start_seconds <= end_seconds <= closing_seconds

        return False

    def is_open_partial_overlap(self, lowerbound_time: datetime, upperbound_time: datetime):
        if not self.regular_schedules:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True

        if lowerbound_time.isoweekday() not in self.regular_schedule_dict.keys():
            # there is no schedule for the day
            return False

        for schedule in self.regular_schedule_dict[lowerbound_time.isoweekday()]:
            # check if trip spans across multiple dates
            trip_span_days = (upperbound_time.date() -
                              lowerbound_time.date()).days

            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(lowerbound_time.date()):
                for period in schedule.periods:
                    if period.start_time <= period.end_time and trip_span_days == 0:
                        # same day range, e.g. 10:00 - 22:00
                        return period.start_time <= lowerbound_time.time() <= period.end_time or period.start_time <= upperbound_time.time() <= period.end_time
                    else:
                        # one of the ranges crosses midnight
                        opening_seconds = _to_seconds(period.start_time)
                        closing_seconds = _to_seconds(period.end_time)

                        if closing_seconds <= opening_seconds:
                            closing_seconds += 24*60*60

                        start_seconds = _to_seconds(lowerbound_time.time())
                        end_seconds = _to_seconds(upperbound_time.time())

                        # adjust for trip crossing midnight
                        if trip_span_days > 1:
                            raise Exception(
                                'Trips spanning more than 2 days are not supported')
                        if trip_span_days == 1:
                            end_seconds += 24*60*60

                        return opening_seconds <= start_seconds <= closing_seconds or opening_seconds <= end_seconds <= closing_seconds

        return False

    def is_open_closest_point(self, timezone: ZoneInfo, trip: Trip):
        # upper and lowerbound times are completely within opening hours
        if not self.regular_schedules:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True

        # finds the closest sensor point within the given trip
        tree = STRtree(trip.points_list)
        closest_point = trip.points_list[tree.nearest(self.geom)]

        # handle duplicate coordinates if they exist
        matching_indices = [i for i, p in enumerate(
            trip.points_list) if p == closest_point]
        matched_time_list = [trip.time_list[i] for i in matching_indices]

        # check if any time value falls within any opening hours
        exposed = False
        for matched_time in matched_time_list:
            if exposed:
                return True  # terminate loop if there is already one exposed point
            matched_time_converted = matched_time.astimezone(timezone)

            if matched_time_converted.isoweekday() not in self.regular_schedule_dict.keys():
                # there is no schedule for the day
                continue

            for schedule in self.regular_schedule_dict[matched_time_converted.isoweekday()]:
                # check if trip is within validity of schedule, then obtain opening hours of that specific day
                if schedule.is_valid_for_date(matched_time_converted.date()):
                    for period in schedule.periods:
                        if period.start_time <= period.end_time:
                            # same day range, e.g. 10:00 - 22:00
                            exposed = period.start_time <= matched_time_converted.time() <= period.end_time
                            # there is only one valid regular schedule per day so the loop will not continue
                        else:
                            # opening hours crosses midnight
                            opening_seconds = _to_seconds(period.start_time)
                            closing_seconds = _to_seconds(
                                period.end_time) + 24*60*60

                            # if this happens right after midnight there will be an error, will be supported in the next iteration
                            matched_time_seconds = _to_seconds(
                                matched_time_converted.time())

                            exposed = opening_seconds <= matched_time_seconds <= closing_seconds
                            # there is only one valid regular schedule per day so the loop will not continue

        return exposed


def _to_seconds(t: time):
    return t.hour * 3600 + t.minute * 60 + t.second
