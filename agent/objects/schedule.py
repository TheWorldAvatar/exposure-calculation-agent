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


class RegularSchedule():
    # accepted recurrent schedules
    _IRI_TO_ISO_WEEKDAY_DICT = {
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Monday': 1,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Tuesday': 2,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Wednesday': 3,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Thursday': 4,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Friday': 5,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Saturday': 6,
        'https://spec.edmcouncil.org/fibo/ontology/FND/DatesAndTimes/FinancialDates/Sunday': 7}

    def __init__(self, days, start_date: date, end_date: date):
        if set(days).issubset(self._IRI_TO_ISO_WEEKDAY_DICT.keys()):
            self.days = [self._IRI_TO_ISO_WEEKDAY_DICT[day] for day in days]
        else:
            raise Exception(
                f"Unsupported recurring days, accepted ones are: {self._IRI_TO_ISO_WEEKDAY_DICT.keys()}, given: {days}")

        self.periods: list[SchedulePeriod] = []
        self.start_date = start_date
        self.end_date = end_date

    def is_valid_for_date(self, d: date):
        return self.start_date <= d <= self.end_date

    def add_period(self, schedule_period: SchedulePeriod):
        if schedule_period not in self.periods:
            self.periods.append(schedule_period)
        else:
            logger.warning("Duplicate period detected, skipping")
