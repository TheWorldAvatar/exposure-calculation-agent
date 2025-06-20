from dataclasses import dataclass
from agent.utils import constants
from agent.utils.env_configs import STACK_NAME


@dataclass
class ExposureDataset:
    table_name: str
    url: str


def get_exposure_dataset(dataset_iri):
    from agent.utils.kg_client import kg_client
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
    query_result = kg_client.remote_store_client.executeQuery(query)
    url = query_result.getJSONObject(0).getString('url')
    table_name = query_result.getJSONObject(0).getString('table_name')

    if STACK_NAME not in url:
        raise Exception('Dataset must be located within the same stack')

    return ExposureDataset(url=url, table_name=table_name)
