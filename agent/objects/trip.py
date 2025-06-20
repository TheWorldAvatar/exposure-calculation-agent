from agent.utils import constants


class Trip:
    def __init__(self, upper_index=None, lower_index=None, trajectory=None):
        # positions in the trajectory point array
        self.upper_index = upper_index
        self.lower_index = lower_index

        # line geometry
        self.trajectory = trajectory

    def set_exposure_result(self, exposure_result):
        self.exposure_result = exposure_result
