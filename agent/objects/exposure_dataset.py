from dataclasses import dataclass
from typing import Optional
from agent.utils import constants
from agent.utils.env_configs import STACK_NAME
import json
from datetime import date


@dataclass
class ExposureDataset:
    table_name: str
    url: str
    geometry_column: str = constants.VECTOR_GEOMETRY_COLUMN
    # this property is used for area weighted calculation, it is the value associated with a polygon
    value_column: Optional[str] = None
    # used for area weighted calculation, pre-calculated area of a polygon (converted from pixel)
    area_column: Optional[str] = None
    # used for time filtering
    iri_column: Optional[str] = None
    # used to determine if this feature exists
    start_date: date = None
    end_date: date = None


def get_exposure_dataset(dataset_iri):
    from agent.utils.kg_client import kg_client
    query = f"""
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>

    SELECT ?url ?table_name ?geometry_column ?value_column ?area_column ?iri_column ?start_date ?end_date
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
        OPTIONAL {{
            <{dataset_iri}> <{constants.HAS_IRI_COLUMN}> ?iri_column.
        }}
        OPTIONAL {{
            <{dataset_iri}> dcterms:temporal/dcat:startDate ?start_date;
                dcterms:temporal/dcat:endDate ?end_date.
        }}
        ?postgis a <{constants.POSTGIS_SERVICE}>;
            <{constants.SERVES_DATASET}> ?catalog;
            <{constants.ENDPOINT_URL}> ?url.
    }}
    """
    query_result = json.loads(
        kg_client.remote_store_client.executeQuery(query).toString())

    if len(query_result) != 1:
        raise Exception('Unexpected query result size for exposure dataset')

    url = query_result[0]['url']
    table_name = query_result[0]['table_name']

    if STACK_NAME not in url:
        raise Exception('Dataset must be located within the same stack')

    exposure_dataset = ExposureDataset(url=url, table_name=table_name)

    if 'geometry_column' in query_result[0]:
        exposure_dataset.geometry_column = query_result[0]['geometry_column']
    else:
        # default name provided by gdal
        exposure_dataset.geometry_column = 'wkb_geometry'

    if 'value_column' in query_result[0]:
        exposure_dataset.value_column = query_result[0]['value_column']
    else:
        exposure_dataset.value_column = 'val'

    if 'area_column' in query_result[0]:
        exposure_dataset.area_column = query_result[0]['area_column']
    else:
        exposure_dataset.area_column = 'area'

    if 'iri_column' in query_result[0]:
        exposure_dataset.iri_column = query_result[0]['iri_column']
    else:
        exposure_dataset.iri_column = 'iri'

    if 'start_date' in query_result[0]:
        exposure_dataset.start_date = date.fromisoformat(
            query_result[0]['start_date'])
        exposure_dataset.end_date = date.fromisoformat(
            query_result[0]['end_date'])

    return exposure_dataset
