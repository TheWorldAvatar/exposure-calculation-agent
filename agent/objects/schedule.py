from dataclasses import dataclass
from twa import agentlogging
from datetime import date, time

logger = agentlogging.get_logger('dev')


@dataclass
class SchedulePeriod():
    start_time: time
    end_time: time

    def __post_init__(self):
        if self.start_time > self.end_time:
            logger.warning(
                f"start_time > end_time, opening hours crossing midnight? start_time = {self.start_time}, end_time = {self.end_time}")
            logger.warning(
                "You need to instantiate this differently, setting end_time to 23:59")
            self.end_time = time(23, 59)


class Schedule():
    # super class
    def __init__(self, iri: str):
        self.iri = iri
        self.start_date = None
        self.end_date = None
        self.periods: list[SchedulePeriod] = []

    def set_start_date(self, start_date: date):
        self.start_date = start_date

    def set_end_date(self, end_date: date):
        self.end_date = end_date

    def is_valid_for_date(self, d: date):
        if self.start_date is not None and self.end_date is not None:
            return self.start_date <= d <= self.end_date
        else:
            logger.info(
                f"No start and end dates for schedule <{self.iri}>, assumed to be valid at all times")
            return True

    def add_period(self, schedule_period: SchedulePeriod):
        if schedule_period not in self.periods:
            self.periods.append(schedule_period)
        else:
            logger.warning("Duplicate period detected, skipping")


class RegularSchedule(Schedule):
    # accepted recurrent schedules
    _IRI_TO_ISO_WEEKDAY_DICT = {
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Monday': 1,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Tuesday': 2,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Wednesday': 3,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Thursday': 4,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Friday': 5,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Saturday': 6,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Sunday': 7}

    def __init__(self, iri: str, days: list[str]):
        super().__init__(iri)
        if set(days).issubset(self._IRI_TO_ISO_WEEKDAY_DICT.keys()):
            self.days = [self._IRI_TO_ISO_WEEKDAY_DICT[day] for day in days]
        else:
            raise Exception(
                f"Unsupported recurring days, accepted ones are: {self._IRI_TO_ISO_WEEKDAY_DICT.keys()}, given: {days}")


class AdHocSchedule(Schedule):
    # ad hoc schedule overwrites regular schedules on the entry dates
    def __init__(self, iri: str, entry_dates: list[date]):
        super().__init__(iri)
        self.entry_dates = entry_dates

    def get_entry_dates(self):
        return self.get_entry_dates
