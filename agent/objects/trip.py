class Trip:
    def __init__(self, upper_index=None, lower_index=None, trajectory=None, lowerbound_time=None, upperbound_time=None):
        # positions in the trajectory point array
        self.upper_index = upper_index
        self.lower_index = lower_index

        # vector (line or point) geometry
        self.trajectory = trajectory

        # timebounds
        self.lowerbound_time = lowerbound_time
        self.upperbound_time = upperbound_time

    def set_exposure_result(self, exposure_result):
        self.exposure_result = exposure_result

    def set_iri_list(self, iri_list: list):
        self.iri_list = iri_list

    def set_time_bounds(self, time_list):
        self.lowerbound = time_list[self.lower_index]
        self.upperbound = time_list[self.upper_index]
