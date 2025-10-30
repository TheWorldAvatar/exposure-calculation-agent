from agent.utils.stack_gateway import stack_clients_view
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL, BLAZEGRAPH_DEFAULT_URL, STACK_OUTGOING
import agent.utils.constants as constants
from twa import agentlogging

logger = agentlogging.get_logger('dev')


class KgClientException(Exception):
    """Raise in case of exception when using the KgClient."""


class KgClient():
    def __init__(self):
        self.remote_store_client = stack_clients_view.RemoteStoreClient(
            BLAZEGRAPH_URL, BLAZEGRAPH_URL)
        self.remote_store_client_kb = stack_clients_view.RemoteStoreClient(
            BLAZEGRAPH_DEFAULT_URL, BLAZEGRAPH_DEFAULT_URL)
        self.ontop_client = stack_clients_view.RemoteStoreClient(ONTOP_URL)
        self.federate_client = stack_clients_view.RemoteStoreClient(
            STACK_OUTGOING)

    def get_time_series(self, iri: str):
        query = f"""
        SELECT ?time_series
        WHERE {{
            <{iri}> <{constants.HAS_TIME_SERIES}> ?time_series.
        }}
        """
        query_result = self.remote_store_client.executeQuery(query)

        if query_result.isEmpty():
            return None
        else:
            return query_result.getJSONObject(0).getString('time_series')

    def get_java_time_class(self, point_iri: str):
        query = f"""
        SELECT ?time_class
        WHERE {{
            <{point_iri}> <{constants.HAS_TIME_SERIES}>/<{constants.HAS_TIME_CLASS}> ?time_class.
        }}
        """
        query_results = self.remote_store_client.executeQuery(query)
        return query_results.getJSONObject(0).getString('time_class')


kg_client = KgClient()  # global object shared between modules
