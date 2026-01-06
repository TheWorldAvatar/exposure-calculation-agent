# class to store business start/end dates and opening hours
from datetime import date, datetime
from zoneinfo import ZoneInfo
from twa import agentlogging
from shapely import wkt
from shapely.strtree import STRtree

from agent.objects.schedule import RegularSchedule, AdHocSchedule
from agent.objects.trip import Trip

logger = agentlogging.get_logger('dev')


class BusinessEstablishment():
    def __init__(self, iri, wkt_string):
        self.iri = iri
        self.start_and_end = []
        self.ad_hoc_schedule_dict: dict[date, AdHocSchedule] = {}
        self.regular_schedules: list[RegularSchedule] = []

        # key is isoweekday, each day can have multiple schedules but the schedules should not overlap
        self.regular_schedule_dict: dict[int, list[RegularSchedule]] = {}
        self.geom = wkt.loads(wkt_string)

    def add_business_start_and_end(self, business_start, business_end):
        if (business_start, business_end) in self.start_and_end:
            raise Exception('Duplicate business_start and business_end pair')
        self.start_and_end.append((business_start, business_end))

    def add_regular_schedule(self, schedule: RegularSchedule):
        # check if new schedule overlaps with any existing schedules
        for s in self.regular_schedules:
            if (
                schedule.start_date
                and schedule.end_date
                and s.start_date
                and s.end_date
                and schedule.start_date <= s.end_date and s.start_date <= schedule.end_date
                and set(s.days) & set(schedule.days)
            ):
                raise Exception('Overlapping schedule detected')

        self.regular_schedules.append(schedule)

        for day in schedule.days:
            if day in self.regular_schedule_dict:
                self.regular_schedule_dict[day].append(schedule)
            else:
                self.regular_schedule_dict[day] = [schedule]

    def add_ad_hoc_schedule(self, schedule: AdHocSchedule):
        # check if new schedule overlaps with any existing schedules
        for s in self.ad_hoc_schedule_dict.values():
            if (
                schedule.start_date
                and schedule.end_date
                and s.start_date
                and s.end_date
                and schedule.start_date <= s.end_date and s.start_date <= schedule.end_date
                and set(s.entry_dates) & set(schedule.entry_dates)
            ):
                raise Exception('Overlapping schedule detected')

        for entry_date in schedule.entry_dates:
            self.ad_hoc_schedule_dict[entry_date] = schedule

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
        # if ad hoc schedule exists, it overwrites the regular schedules
        if lowerbound_time.date() in self.ad_hoc_schedule_dict:
            schedules = [self.ad_hoc_schedule_dict[lowerbound_time.date()]]
        elif not self.regular_schedule_dict:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True
        elif lowerbound_time.isoweekday() not in self.regular_schedule_dict:
            # there is no schedule for the day
            return False
        else:
            schedules = self.regular_schedule_dict[lowerbound_time.isoweekday(
            )]

        for schedule in schedules:
            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(lowerbound_time.date()):
                return any(period.start_time <= lowerbound_time.time() <= upperbound_time.time() <= period.end_time for period in schedule.periods)

        return False

    def is_open_partial_overlap(self, lowerbound_time: datetime, upperbound_time: datetime):
        # if ad hoc schedule exists, it overwrites the regular schedules
        if lowerbound_time.date() in self.ad_hoc_schedule_dict:
            schedules = [self.ad_hoc_schedule_dict[lowerbound_time.date()]]
        elif not self.regular_schedule_dict:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True
        elif lowerbound_time.isoweekday() not in self.regular_schedule_dict:
            # there is no schedule for the day
            return False
        else:
            schedules = self.regular_schedule_dict[lowerbound_time.isoweekday(
            )]

        for schedule in schedules:
            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(lowerbound_time.date()):
                return any(period.start_time <= lowerbound_time.time() <= period.end_time or period.start_time <= upperbound_time.time() <= period.end_time for period in schedule.periods)

        return False

    def is_open_closest_point(self, trip: Trip):
        # finds the closest point within the trip to this business establishment
        tree = STRtree(trip.points_list)
        closest_point = trip.points_list[tree.nearest(self.geom)]

        # handle duplicate coordinates if they exist
        matching_indices = [i for i, p in enumerate(
            trip.points_list) if p == closest_point]
        matched_time_list = [trip.time_list[i] for i in matching_indices]

        # check if any time value falls within any opening hours
        exposed = False

        if all(matched_time.date() not in self.ad_hoc_schedule_dict for matched_time in matched_time_list) and not self.regular_schedule_dict:
            logger.info(
                f"<{self.iri}> has no regular schedules and matching ad hoc schedules, assumed to be open at all times")
            exposed = True

        for matched_time in matched_time_list:
            if exposed:
                return True  # terminate loop if there is already one exposed point

            # if ad hoc schedule exists, it overwrites the regular schedules
            if matched_time.date() in self.ad_hoc_schedule_dict:
                schedules = [self.ad_hoc_schedule_dict[matched_time.date()]]
            elif matched_time.isoweekday() not in self.regular_schedule_dict:
                # there is no schedule for the day
                continue
            else:
                schedules = self.regular_schedule_dict[matched_time.isoweekday(
                )]

            for schedule in schedules:
                # check if trip is within validity of schedule, then obtain opening hours of that specific day
                if schedule.is_valid_for_date(matched_time.date()):
                    exposed = any(period.start_time <= matched_time.time(
                    ) <= period.end_time for period in schedule.periods)
        return exposed
