from datetime import datetime
from shapely.geometry import Point, LineString


class Trip:
    exposure_result = 0

    def __init__(self, upper_index=None, lower_index=None, full_time_list: list[datetime] = None, lowerbound_time: datetime = None, upperbound_time: datetime = None, full_points_list: list[Point] = None):
        # positions in the trajectory point array
        self.upper_index = upper_index
        self.lower_index = lower_index

        if upper_index == lower_index:  # LineString requires at least two points
            self.points_list = [full_points_list[lower_index]]
            self.trajectory = full_points_list[lower_index]
        else:
            # python slicing does not include the end index
            self.points_list = full_points_list[lower_index:upper_index+1]
            self.trajectory = LineString(self.points_list)

        # timebounds
        self.lowerbound_time = full_time_list[lower_index]
        self.upperbound_time = full_time_list[upper_index]

        self.time_list = full_time_list[lower_index:upper_index+1]

    def set_exposure_result(self, exposure_result):
        self.exposure_result = exposure_result

    def set_iri_list(self, iri_list: list[str]):
        self.iri_list = iri_list
