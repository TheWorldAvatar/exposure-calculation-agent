class Trip:
    def __init__(self, upper_index, lower_index, trajectory):
        self.upper_index = upper_index
        self.lower_index = lower_index
        self.trajectory = trajectory

    def set_exposure_result(self, exposure_result):
        self.exposure_result = exposure_result
