from agent.utils.baselib_gateway import baselib_view
from agent.utils.stack_configs import BLAZEGRAPH_URL
from agent.objects.calculation_metadata import CalculationMetadata
from agent.utils.constants import HAS_DISTANCE, HAS_TIME_SERIES, TRIP, PREFIX_EXPOSURE, HAS_DISTANCE, DCAT_DATASET, DCTERM_TITLE
from twa import agentlogging
import json
import uuid

logger = agentlogging.get_logger('dev')


class KgClientException(Exception):
    """Raise in case of exception when using the KgClient."""


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
            raise KgClientException(
                'Expected one set of results in get_calculation_metadata')

        metadata = json.loads(query_results.getJSONObject(0).toString())

        return CalculationMetadata(metadata)

    def get_trip(self, point_iri: str):
        query = f"""
        SELECT ?trip
        WHERE {{
            <{point_iri}> <{HAS_TIME_SERIES}> ?time_series.
            ?trip <{HAS_TIME_SERIES}> ?time_series;
                a <{TRIP}>.
        }}
        """
        query_results = self.remote_store_client.executeQuery(query)

        if query_results.isEmpty():
            return None
        elif query_results.length() > 1:
            raise KgClientException('More than 1 trip instance detected?')
        else:
            return query_results.getJSONObject(0).getString('trip')

    def get_calculation_iri(self, calculation_metadata: CalculationMetadata):
        var = 'calc'
        query_results = self.remote_store_client.executeQuery(
            calculation_metadata.get_query(var))

        if not query_results.isEmpty():
            return query_results.getJSONObject(0).getString(var)
        else:
            return None

    def instantiate_calculation(self, calculation_metadata: CalculationMetadata):
        calculation_iri = PREFIX_EXPOSURE + str(uuid.uuid4())
        query = f"""
        INSERT DATA {{
            <{calculation_iri}> a <{calculation_metadata.get_rdf_type()}>;
                <{HAS_DISTANCE}> {calculation_metadata.get_distance()}.
        }}
        """
        self.remote_store_client.executeUpdate(query)

        return calculation_iri

    def get_dataset_iri(self, table_name):
        var_name = 'dataset'
        query = f"""
        SELECT ?{var_name}
        WHERE {{
            ?{var_name} a <{DCAT_DATASET}>;
                <{DCTERM_TITLE}> "{table_name}".
        }}
        """
        query_results = self.remote_store_client.executeQuery(query)

        if query_results.length() != 1:
            return None
        else:
            return query_results.getJSONObject(0).getString(var_name)
