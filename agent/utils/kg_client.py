from agent.utils.baselib_gateway import baselib_view
from agent.utils.stack_configs import BLAZEGRAPH_URL
from agent.objects.calculation_metadata import CalculationMetadata
from agent.objects.exposure_dataset import ExposureDataset
import agent.utils.constants as constants
from twa import agentlogging
import json
import uuid
from agent.utils.env_configs import STACK_NAME

logger = agentlogging.get_logger('dev')


class KgClientException(Exception):
    """Raise in case of exception when using the KgClient."""


class KgClient():
    def __init__(self):
        self.remote_store_client = baselib_view.RemoteStoreClient(
            BLAZEGRAPH_URL, BLAZEGRAPH_URL)

    def get_calculation_metadata(self, iri: str) -> CalculationMetadata:
        query = f"""
        SELECT ?rdf_type ?distance ?upperbound ?lowerbound
        WHERE
        {{
            <{iri}> a ?rdf_type.
            OPTIONAL{{<{iri}> <{constants.HAS_DISTANCE}> ?distance.}}
            OPTIONAL{{<{iri}> <{constants.HAS_UPPERBOUND}> ?upperbound.}}
            OPTIONAL{{<{iri}> <{constants.HAS_LOWERBOUND}> ?lowerbound.}}
        }}
        """

        query_results = self.remote_store_client.executeQuery(query)

        # convert to python dict
        if query_results.length() != 1:
            raise KgClientException(
                'Expected one set of results in get_calculation_metadata')

        metadata = json.loads(query_results.getJSONObject(0).toString())
        rdf_type = metadata['rdf_type']
        distance = metadata['distance']

        if 'upperbound' in metadata:
            upperbound = metadata['upperbound']
        else:
            upperbound = None

        if 'lowerbound' in metadata:
            lowerbound = metadata['lowerbound']
        else:
            lowerbound = None

        return CalculationMetadata(rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound)

    def get_trip(self, point_iri: str):
        query = f"""
        SELECT ?trip
        WHERE {{
            <{point_iri}> <{constants.HAS_TIME_SERIES}> ?time_series.
            ?trip <{constants.HAS_TIME_SERIES}> ?time_series;
                a <{constants.TRIP}>.
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
        calculation_iri = constants.PREFIX_EXPOSURE + str(uuid.uuid4())
        query = calculation_metadata.get_insert_query(
            calculation_iri=calculation_iri)
        self.remote_store_client.executeUpdate(query)

        return calculation_iri

    def get_dataset_iri(self, table_name):
        var_name = 'dataset'
        query = f"""
        SELECT ?{var_name}
        WHERE {{
            ?{var_name} a <{constants.DCAT_DATASET}>;
                <{constants.DCTERM_TITLE}> "{table_name}".
        }}
        """
        query_results = self.remote_store_client.executeQuery(query)

        if query_results.length() != 1:
            return None
        else:
            return query_results.getJSONObject(0).getString(var_name)

    def get_exposure_dataset(self, dataset_iri):
        query = f"""
        SELECT ?url ?table_name
        WHERE {{
            ?catalog <{constants.DATASET_PREDICATE}> <{dataset_iri}>.
            <{dataset_iri}> <{constants.DCTERM_TITLE}> ?table_name.
            ?postgis a <{constants.POSTGIS_SERVICE}>;
                <{constants.SERVES_DATASET}> ?catalog;
                <{constants.ENDPOINT_URL}> ?url.
        }}
        """
        query_result = self.remote_store_client.executeQuery(query)
        url = query_result.getJSONObject(0).getString('url')
        table_name = query_result.getJSONObject(0).getString('table_name')

        if STACK_NAME not in url:
            raise KgClientException(
                'Dataset must be located within the same stack')

        return ExposureDataset(url=url, table_name=table_name)


kg_client = KgClient()  # global object shared between modules
