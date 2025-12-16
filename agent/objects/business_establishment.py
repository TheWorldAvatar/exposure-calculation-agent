# class to store business start/end dates and opening hours
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
from twa import agentlogging
from enum import StrEnum

logger = agentlogging.get_logger('dev')


class ScheduleType(StrEnum):
    REGULAR = "regular"
    AD_HOC = "ad hoc"


class Schedule():
    # accepted recurrent schedules
    _IRI_TO_ISO_WEEKDAY_DICT = {
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Monday': 1,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Tuesday': 2,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Wednesday': 3,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Thursday': 4,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Friday': 5,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Saturday': 6,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Sunday': 7}

    _IRI_TO_SCHEDULE_TYPE = {
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/RegularSchedule': ScheduleType.REGULAR,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/AdHocSchedule': ScheduleType.AD_HOC}

    def __init__(self, days, start_time: time, end_time: time, start_date: date, end_date: date, schedule_type: str):
        if set(days).issubset(self._IRI_TO_ISO_WEEKDAY_DICT.keys()):
            self.days = [self._IRI_TO_ISO_WEEKDAY_DICT[day] for day in days]
        else:
            raise Exception(
                f"Unsupported recurring days, accepted ones are: {self._IRI_TO_ISO_WEEKDAY_DICT.keys()}, given: {days}")

        if schedule_type not in self._IRI_TO_SCHEDULE_TYPE.keys():
            raise Exception(
                f"Unsupported schedule type, accepted types: {self._IRI_TO_SCHEDULE_TYPE.keys()}")

        self.end_time = end_time
        self.start_time = start_time
        self.start_date = start_date
        self.end_date = end_date
        self.schedule_type = self._IRI_TO_SCHEDULE_TYPE[schedule_type]

    def is_valid_for_date(self, d: date):
        return self.start_date <= d <= self.end_date


class BusinessEstablishment():
    def __init__(self, iri):
        self.iri = iri
        self.start_and_end = []
        self.regular_schedules: list[Schedule] = []

        # key is isoweekday, each day can have multiple schedules but the schedules should not overlap
        self.regular_schedule_dict: dict[int, list[Schedule]] = {}

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

    def is_open_full_containment(self, lowerbound_time: datetime, upperbound_time: datetime, timezone: ZoneInfo):
        # upper and lowerbound times are completely within opening hours
        if not self.regular_schedules:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True

        start_timestamp = lowerbound_time.astimezone(timezone)
        end_timestamp = upperbound_time.astimezone(timezone)

        if start_timestamp.isoweekday() not in self.regular_schedule_dict.keys():
            # there is no schedule for the day
            return False

        for schedule in self.regular_schedule_dict[start_timestamp.isoweekday()]:
            # check if trip spans across multiple dates
            trip_span_days = (end_timestamp.date() -
                              start_timestamp.date()).days

            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(start_timestamp.date()):
                if schedule.start_time <= schedule.end_time and trip_span_days == 0:
                    # same day range, e.g. 10:00 - 22:00
                    return schedule.start_time <= start_timestamp.time() <= end_timestamp.time() <= schedule.end_time
                else:
                    # one of the ranges crosses midnight
                    opening_seconds = _to_seconds(schedule.start_time)
                    closing_seconds = _to_seconds(schedule.end_time)

                    if closing_seconds <= opening_seconds:
                        closing_seconds += 24*60*60

                    start_seconds = _to_seconds(start_timestamp.time())
                    end_seconds = _to_seconds(end_timestamp.time())

                    # adjust for trip crossing midnight
                    if trip_span_days > 1:
                        raise Exception(
                            'Trips spanning more than 2 days are not supported')
                    if trip_span_days == 1:
                        end_seconds += 24*60*60

                    return opening_seconds <= start_seconds <= end_seconds <= closing_seconds

        return False

    def is_open_partial_overlap(self, lowerbound_time: datetime, upperbound_time: datetime, timezone: ZoneInfo):
        if not self.regular_schedules:
            logger.info(
                f"<{self.iri}> has no regular schedules, assumed to be open at all times")
            return True

        start_timestamp = lowerbound_time.astimezone(timezone)
        end_timestamp = upperbound_time.astimezone(timezone)

        if start_timestamp.isoweekday() not in self.regular_schedule_dict.keys():
            # there is no schedule for the day
            return False

        for schedule in self.regular_schedule_dict[start_timestamp.isoweekday()]:
            # check if trip spans across multiple dates
            trip_span_days = (end_timestamp.date() -
                              start_timestamp.date()).days

            # check if trip is within validity of schedule, then obtain opening hours of that specific day
            if schedule.is_valid_for_date(start_timestamp.date()):
                if schedule.start_time <= schedule.end_time and trip_span_days == 0:
                    # same day range, e.g. 10:00 - 22:00
                    return schedule.start_time <= start_timestamp.time() <= schedule.end_time or schedule.start_time <= end_timestamp.time() <= schedule.end_time
                else:
                    # one of the ranges crosses midnight
                    opening_seconds = _to_seconds(schedule.start_time)
                    closing_seconds = _to_seconds(schedule.end_time)

                    if closing_seconds <= opening_seconds:
                        closing_seconds += 24*60*60

                    start_seconds = _to_seconds(start_timestamp.time())
                    end_seconds = _to_seconds(end_timestamp.time())

                    # adjust for trip crossing midnight
                    if trip_span_days > 1:
                        raise Exception(
                            'Trips spanning more than 2 days are not supported')
                    if trip_span_days == 1:
                        end_seconds += 24*60*60

                    return opening_seconds <= start_seconds <= closing_seconds or opening_seconds <= end_seconds <= closing_seconds

        return False


def _to_seconds(t: time):
    return t.hour * 3600 + t.minute * 60 + t.second
