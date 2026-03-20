from datetime import datetime
from shapely.geometry import Point, LineString


class Trip:
    exposure_result = 0

    def __init__(self, upper_index=None, lower_index=None, full_time_list: list[datetime] = None, full_points_list: list[Point] = None):
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
        self.lowerbound_time: datetime = full_time_list[lower_index]
        self.upperbound_time: datetime = full_time_list[upper_index]

        self.time_list = full_time_list[lower_index:upper_index+1]

    def set_exposure_result(self, exposure_result):
        self.exposure_result = exposure_result

    def set_iri_wkt_dict(self, iri_wkt_dict: dict[str, str]):
        # should be a property in the BusinesEstablishment class naturally, but placing it here due to convenience
        self.iri_wkt_dict = iri_wkt_dict

    def get_iri_list(self):
        return self.iri_wkt_dict.keys()
