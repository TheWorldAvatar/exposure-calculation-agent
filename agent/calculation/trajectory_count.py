from agent.utils.ts_client import TimeSeriesClient
from agent.calculation.calculation_input import CalculationInput
from agent.utils.kg_client import KgClient

kg_client = KgClient()


def trajectory_count(calculation_input: CalculationInput):
    # subject must be a time series
    ts_client = TimeSeriesClient(calculation_input.subject)

    trajectory_time_series = ts_client.get_time_series(
        data_iri_list=[calculation_input.subject])

    return ''
