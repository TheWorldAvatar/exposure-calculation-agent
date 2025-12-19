from dataclasses import dataclass
from enum import StrEnum
from twa import agentlogging
from datetime import date, time

logger = agentlogging.get_logger('dev')


class ScheduleType(StrEnum):
    REGULAR = "regular"
    AD_HOC = "ad hoc"


@dataclass
class SchedulePeriod():
    start_time: time
    end_time: time

    def __post_init__(self):
        if self.start_time > self.end_time:
            logger.warning("start_time > end_time, will be unsupported soon")
            # raise ValueError(
            #     f"Invalid period, start_time must be less than end_time start_time: {self.start_time}, end_time: {self.end_time}")


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

    def __init__(self, days, start_date: date, end_date: date, schedule_type: str):
        if set(days).issubset(self._IRI_TO_ISO_WEEKDAY_DICT.keys()):
            self.days = [self._IRI_TO_ISO_WEEKDAY_DICT[day] for day in days]
        else:
            raise Exception(
                f"Unsupported recurring days, accepted ones are: {self._IRI_TO_ISO_WEEKDAY_DICT.keys()}, given: {days}")

        if schedule_type not in self._IRI_TO_SCHEDULE_TYPE.keys():
            raise Exception(
                f"Unsupported schedule type, accepted types: {self._IRI_TO_SCHEDULE_TYPE.keys()}")

        self.periods: list[SchedulePeriod] = []
        self.start_date = start_date
        self.end_date = end_date
        self.schedule_type = self._IRI_TO_SCHEDULE_TYPE[schedule_type]

    def is_valid_for_date(self, d: date):
        return self.start_date <= d <= self.end_date

    def add_period(self, schedule_period: SchedulePeriod):
        if schedule_period not in self.periods:
            self.periods.append(schedule_period)
        else:
            logger.warning("Duplicate period detected, skipping")
