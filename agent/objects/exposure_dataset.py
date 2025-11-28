from dataclasses import dataclass
from typing import Optional
from agent.utils import constants
from agent.utils.env_configs import STACK_NAME
import json


@dataclass
class ExposureDataset:
    table_name: str
    url: str
    geometry_column: str = constants.VECTOR_GEOMETRY_COLUMN
    # this property is used for area weighted calculation, it is the value associated with a polygon
    value_column: Optional[str] = None
    # used for area weighted calculation, pre-calculated area of a polygon (converted from pixel)
    area_column: Optional[str] = None


def get_exposure_dataset(dataset_iri):
    from agent.utils.kg_client import kg_client
    query = f"""
    SELECT ?url ?table_name ?geometry_column ?value_column ?area_column
    WHERE {{
        ?catalog <{constants.DATASET_PREDICATE}> <{dataset_iri}>.
        <{dataset_iri}> <{constants.DCTERM_TITLE}> ?table_name.
        OPTIONAL {{
            <{dataset_iri}> <{constants.HAS_GEOMETRY_COLUMN}> ?geometry_column.
        }}
        OPTIONAL {{
            <{dataset_iri}> <{constants.HAS_VALUE_COLUMN}> ?value_column.
        }}
        OPTIONAL {{
            <{dataset_iri}> <{constants.HAS_AREA_COLUMN}> ?area_column.
        }}
        ?postgis a <{constants.POSTGIS_SERVICE}>;
            <{constants.SERVES_DATASET}> ?catalog;
            <{constants.ENDPOINT_URL}> ?url.
    }}
    """
    query_result = json.loads(
        kg_client.remote_store_client.executeQuery(query).toString())

    if len(query_result) != 1:
        raise Exception('Unexpected query result size')

    url = query_result[0]['url']
    table_name = query_result[0]['table_name']

    if STACK_NAME not in url:
        raise Exception('Dataset must be located within the same stack')

    exposure_dataset = ExposureDataset(url=url, table_name=table_name)

    if 'geometry_column' in query_result[0]:
        exposure_dataset.geometry_column = query_result[0]['geometry_column']
    else:
        exposure_dataset.geometry_column = 'wkb_geometry'

    if 'value_column' in query_result[0]:
        exposure_dataset.value_column = query_result[0]['value_column']

    if 'area_column' in query_result[0]:
        exposure_dataset.area_column = query_result[0]['area_column']

    return exposure_dataset
