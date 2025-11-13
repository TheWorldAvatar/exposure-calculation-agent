from agent.utils.stack_gateway import stack_clients_view
from agent.utils.stack_configs import BLAZEGRAPH_URL, STACK_OUTGOING, ONTOP_URL
import agent.utils.constants as constants
from twa import agentlogging
import time

logger = agentlogging.get_logger('dev')


class KgClientException(Exception):
    """Raise in case of exception when using the KgClient."""


class KgClient():
    def __init__(self):
        self.remote_store_client = RetryRemoteStoreClient(stack_clients_view.RemoteStoreClient(
            STACK_OUTGOING, BLAZEGRAPH_URL))
        self.federate_client = RetryRemoteStoreClient(
            stack_clients_view.RemoteStoreClient(STACK_OUTGOING))
        self.ontop_client = stack_clients_view.RemoteStoreClient(ONTOP_URL)

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


class RetryRemoteStoreClient:
    def __init__(self, java_client, max_retries=3, delay=10):
        self.java_client = java_client
        self.max_retries = max_retries
        self.delay = delay

    def executeQuery(self, query):
        for attempt in range(1, self.max_retries + 1):
            try:
                return self.java_client.executeQuery(query)
            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay)

    # forward other methods if needed
    def __getattr__(self, name):
        return getattr(self.java_client, name)


kg_client = KgClient()  # global object shared between modules
