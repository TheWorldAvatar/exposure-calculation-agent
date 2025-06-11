from agent.utils.baselib_gateway import baselib_view
from agent.utils.stack_configs import BLAZEGRAPH_URL
from agent.objects.calculation_metadata import CalculationMetadata
from agent.utils.constants import PREFIX, HAS_DISTANCE, DCAT_DATASET, DCTERM_TITLE
import uuid
from twa import agentlogging

logger = agentlogging.get_logger('dev')


class KgClient():
    def __init__(self):
        self.remote_store_client = baselib_view.RemoteStoreClient(
            BLAZEGRAPH_URL, BLAZEGRAPH_URL)

    def get_calculation_iri(self, calculation_metadata: CalculationMetadata):
        var = 'calc'
        query_results = self.remote_store_client.executeQuery(
            calculation_metadata.get_query(var))

        if not query_results.isEmpty():
            return query_results.getJSONObject(0).getString(var)
        else:
            return None

    def instantiate_calculation(self, calculation_metadata: CalculationMetadata):
        calculation_iri = PREFIX + str(uuid.uuid4())
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
