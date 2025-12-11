# class to store business start/end dates and opening hours
from datetime import date, datetime
from zoneinfo import ZoneInfo


class BusinessEstablishment():
    def __init__(self, iri):
        self.iri = iri
        self.start_and_end = []
        self.schedules: list[Schedule] = []

    def add_business_start_and_end(self, business_start, business_end):
        if (business_start, business_end) in self.start_and_end:
            raise Exception('Duplicate business_start and business_end pair')
        self.start_and_end.append((business_start, business_end))

    def add_schedule(self, schedule):
        # check if new schedule overlaps with any existing schedules
        for s in self.schedules:
            if schedule.start_date <= s.end_date and s.start_date <= schedule.end_date:
                if set(s.days) & set(schedule.days):
                    raise Exception('Overlapping schedule detected')

        self.schedules.append(schedule)

    def business_exists(self, lowerbound_time: datetime, upperbound_time: datetime):
        for start, end in self.start_and_end:
            if isinstance(start, datetime) and isinstance(end, datetime):
                return start <= lowerbound_time and upperbound_time <= end
            elif isinstance(start, date) and isinstance(end, date):
                return start <= lowerbound_time.date and upperbound_time.date <= end
            else:
                raise Exception('Unsupported type for business start and end')

    def business_is_open(self, lowerbound_time: datetime, upperbound_time: datetime, timezone: str):
        tz = ZoneInfo(timezone)
        start_timestamp = lowerbound_time.astimezone(tz)
        end_timestamp = upperbound_time.astimezone(tz)

        for schedule in self.schedules:
            # check if trip is within validity of schedule, then obtain opening hours of that specific day

            if schedule.is_valid_for_date(start_timestamp.date()) and start_timestamp.isoweekday() in schedule.days:

                if schedule.start_time <= schedule.end_time:
                    # same day range, e.g. 10:00 - 22:00
                    return schedule.start_time <= start_timestamp.time() <= end_timestamp.time() <= schedule.end_time
                else:
                    # opening hour crosses midnight, e.g. 22:00 - 02:00
                    return (schedule.start_time <= start_timestamp.time() or start_timestamp.time() <= schedule.end_time) and \
                        (schedule.start_time <= end_timestamp.time()
                         or end_timestamp.time() <= schedule.end_time)

        return False


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

    def __init__(self, days, start_time: datetime, end_time: datetime, start_date: date, end_date: date):
        if set(days).issubset(self._IRI_TO_ISO_WEEKDAY_DICT.keys()):
            self.days = [self._IRI_TO_ISO_WEEKDAY_DICT[day] for day in days]
        else:
            raise Exception(
                f"Unsupported recurring days, accepted ones are: {self._IRI_TO_ISO_WEEKDAY_DICT.keys()}, given: {days}")

        self.end_time = end_time
        self.start_time = start_time
        self.start_date = start_date
        self.end_date = end_date

    def is_valid_for_date(self, d: date):
        return self.start_date <= d <= self.end_date
