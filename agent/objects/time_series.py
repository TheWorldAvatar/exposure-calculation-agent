class TimeSeries:
    def __init__(self):
        self.timestamp_java_list: dict[str, list] = {}
        self.values: dict[str, list] = {}
        self.time_number: dict[str, list] = {}
        self.timestamp: dict[str, list] = {}

    def add_timestamp_java(self, measure, timestamp_java_list):
        self.timestamp_java_list[measure] = timestamp_java_list

    def add_time_number(self, measure, time_number_list):
        self.time_number[measure] = time_number_list

    def add_timestamp(self, measure, timestamp_list):
        self.timestamp[measure] = timestamp_list

    def add_value(self, measure, value_list):
        self.values[measure] = value_list

    def get_timestamp_java(self, measure):
        if measure in self.timestamp_java_list.keys():
            return self.timestamp_java_list[measure]
        else:
            return self.time_number[measure]

    def get_value_list(self, measure):
        return self.values[measure]

    def get_measures(self):
        return self.values.keys()
