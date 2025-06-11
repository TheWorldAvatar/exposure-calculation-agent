from agent.utils.baselib_gateway import baselib_view
from agent.utils.stack_configs import BLAZEGRAPH_URL
from agent.objects.calculation_metadata import CalculationMetadata
from agent.utils.constants import HAS_DISTANCE
from twa import agentlogging
import json

logger = agentlogging.get_logger('dev')


class KgClient():
    def __init__(self):
        self.remote_store_client = baselib_view.RemoteStoreClient(
            BLAZEGRAPH_URL, BLAZEGRAPH_URL)

    def get_calculation_metadata(self, iri: str) -> CalculationMetadata:
        query = f"""
        SELECT ?rdf_type ?distance
        WHERE
        {{
            <{iri}> a ?rdf_type.
            OPTIONAL{{<{iri}> <{HAS_DISTANCE}> ?distance.}}
        }}
        """

        query_results = self.remote_store_client.executeQuery(query)

        # convert to python dict
        if query_results.length() != 1:
            raise Exception(
                'Expected one set of results in get_calculation_metadata')

        metadata = json.loads(query_results.getJSONObject(0).toString())

        return CalculationMetadata(metadata)

    def get_trip(self, point_iri: str):
        query = f"""
        SELECT ?trip
        WHERE {{
            <{point_iri}>
        }}
        """
