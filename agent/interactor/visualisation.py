import json
from twa import agentlogging
from flask import Blueprint, request
from itertools import product
from agent.interactor.trigger_calculation import get_dataset_iri
from agent.objects.calculation_metadata import CalculationMetadata, get_dataset_filter_where_clauses
from agent.utils import constants
from agent.utils.postgis_client import postgis_client
from agent.utils.env_configs import VIS_DATA_JSON
from agent.utils.stack_configs import BLAZEGRAPH_URL
from pathlib import Path
from agent.utils.stack_gateway import stack_clients_view
from urllib.parse import urlencode
import matplotlib.pyplot as plt
from matplotlib.colors import to_hex
from rdflib.plugins.sparql.parser import parseQuery
from psycopg2.extras import RealDictCursor

logger = agentlogging.get_logger('dev')

visualisation_bp = Blueprint(
    'visualisation', __name__, url_prefix='/visualisation')


@visualisation_bp.route('/create_2d_layer', methods=['POST'])
def create_2d_layer():
    inputs = request.json

    subject_query_file = inputs['subject_query_file']
    subjects = _get_subjects(subject_query_file)

    geoserver_layer_name = inputs['geoserver_layer_name']
    geoserver_workspace = inputs['geoserver_workspace']
    subject_table = inputs['subject_table']
    host = inputs['host']
    layer_group_name = inputs['layer_group_name']
    colour_palette = inputs['colour_palette']

    if 'other_paint_properties' in inputs:
        other_paint_properties = inputs['other_paint_properties']
    else:
        other_paint_properties = None

    dataset1 = inputs['dataset1']
    # get properties of dataset1
    dataset_filters = [{}]
    if 'dataset_filter_values' in dataset1:
        dataset_filter_values = dataset1['dataset_filter_values']
        # produces a cartesian product between the dataset_filter_values
        dataset_filters = [
            dict(zip(dataset_filter_values.keys(), combo))
            for combo in product(*dataset_filter_values.values())
        ]

    if len(dataset_filters) > 1:
        raise Exception('Only one dataset filter allowed')

    calculation1 = _get_calculation(
        rdf_type=dataset1['rdf_type'], distance=dataset1['distance'], dataset_filter=dataset_filters[0])
    exposure_dataset_iri1 = get_dataset_iri(
        table_name=dataset1['exposure_table'])
    set_id1 = _get_set_id(exposure=exposure_dataset_iri1,
                          calculation=calculation1, subjects=subjects)

    print(f"Calculation1 = <{calculation1}>")
    print(f"Exposure1 = <{exposure_dataset_iri1}>")
    print(f"set_id1 = {set_id1}")

    dataset2 = inputs['dataset2']
    # get properties of dataset2
    dataset_filters = [{}]
    if 'dataset_filter_values' in dataset2:
        dataset_filter_values = dataset2['dataset_filter_values']
        # produces a cartesian product between the dataset_filter_values
        dataset_filters = [
            dict(zip(dataset_filter_values.keys(), combo))
            for combo in product(*dataset_filter_values.values())
        ]

    if len(dataset_filters) > 1:
        raise Exception('Only one dataset filter allowed')

    calculation2 = _get_calculation(
        rdf_type=dataset2['rdf_type'], distance=dataset2['distance'], dataset_filter=dataset_filters[0])
    exposure_dataset_iri2 = get_dataset_iri(
        table_name=dataset2['exposure_table'])
    set_id2 = _get_set_id(exposure=exposure_dataset_iri2,
                          calculation=calculation2, subjects=subjects)

    print(f"Calculation2 = <{calculation2}>")
    print(f"Exposure2 = <{exposure_dataset_iri2}>")
    print(f"set_id2 = {set_id2}")

    _create_geoserver_layer_2d(
        geoserver_layer_name, geoserver_workspace, subject_table)

    _update_data_json_2d(host=host, layer_group_name=layer_group_name,
                         exposure1=exposure_dataset_iri1, calculation1=calculation1, set_id1=set_id1,
                         exposure2=exposure_dataset_iri2, calculation2=calculation2, set_id2=set_id2,
                         geoserver_layer_name=geoserver_layer_name, geoserver_workspace=geoserver_workspace,
                         other_paint_properties=other_paint_properties, colour_palette=colour_palette)
    return 'done'


@visualisation_bp.route('/create_layer', methods=['POST'])
def create_layer():
    # create the layer in geoserver and edit visualisation data.json
    inputs = request.json
    exposure_table = inputs['exposure_table']
    rdf_type = inputs['rdf_type']
    distance = inputs['distance']
    host = inputs['host']
    layer_group_name = inputs['layer_group_name']
    geoserver_layer_name = inputs['geoserver_layer_name']
    geoserver_workspace = inputs['geoserver_workspace']
    subject_table = inputs['subject_table']
    subject_query_file = inputs['subject_query_file']

    if 'column_name' in inputs:
        column_name = inputs['column_name']
    else:
        column_name = None

    if 'other_paint_properties' in inputs:
        other_paint_properties = inputs['other_paint_properties']
    else:
        other_paint_properties = None

    subjects = _get_subjects(subject_query_file)
    if 'num_bin' in inputs:
        num_bin = inputs['num_bin']
    else:
        num_bin = 3

    if 'colour_scheme' in inputs:
        colour_scheme = inputs['colour_scheme']
    else:
        colour_scheme = 'jet'

    dataset_filters = [{}]
    if 'dataset_filter_values' in inputs:
        dataset_filter_values = inputs['dataset_filter_values']
        # produces a cartesian product between the dataset_filter_values
        dataset_filters = [
            dict(zip(dataset_filter_values.keys(), combo))
            for combo in product(*dataset_filter_values.values())
        ]

    if len(dataset_filters) > 1:
        raise Exception('Only one dataset filter allowed')

    calculation = _get_calculation(rdf_type=rdf_type, distance=distance,
                                   dataset_filter=dataset_filters[0], column_name=column_name)
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)
    set_id = _get_set_id(exposure=exposure_dataset_iri,
                         calculation=calculation, subjects=subjects)

    print(f"Calculation = <{calculation}>")
    print(f"Exposure = <{exposure_dataset_iri}>")
    print(f"set_id = {set_id}")

    # create layer in geoserver that is going to be reused
    _create_geoserver_layer(geoserver_layer_name,
                            geoserver_workspace, subject_table)

    # read in data json and add layer
    _update_data_json(host=host, layer_group_name=layer_group_name,
                      exposure=exposure_dataset_iri, calculation=calculation, set_id=set_id,
                      geoserver_layer_name=geoserver_layer_name, geoserver_workspace=geoserver_workspace, num_bin=num_bin, colour_scheme=colour_scheme,
                      other_paint_properties=other_paint_properties)

    return 'done'


def _get_subjects(subject_query_file):
    from agent.utils.kg_client import kg_client
    with open(Path(constants.BIND_MOUNT_PATH)/subject_query_file, "r") as f:
        query = f.read()

        parsed = parseQuery(query)

        if len(parsed[1]['projection']) != 1:
            raise Exception(
                'Provided query needs to have exactly one select variable')

        select_var = str(parsed[1]['projection'][0]['var'])

        logger.info(
            'Querying subject IRIs with provided SPARQL query template')
        query_result = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        logger.info('Received ' + str(len(query_result)) + ' IRIs')

        if len(query_result) == 0:
            logger.warning('There are no subject IRIs')
            return

        subject_list = []
        for i in query_result:
            subject_list.append(i[select_var])

        return subject_list


def _get_set_id(exposure, calculation, subjects):
    query = """
        SELECT DISTINCT set_id
        FROM exposure_result
        WHERE exposure = %(EXPOSURE_PLACEHOLDER)s
        AND calculation = %(CALCULATION_PLACEHOLDER)s
        AND subject = ANY(%(SUBJECT_PLACEHOLDER)s)
    """
    replacements = {
        'EXPOSURE_PLACEHOLDER': exposure,
        'CALCULATION_PLACEHOLDER': calculation,
        'SUBJECT_PLACEHOLDER': subjects
    }
    with postgis_client.connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, replacements)

            if cur.description:
                query_result = cur.fetchall()
                if len(query_result) > 1:
                    raise Exception('more than one set_id')
                else:
                    set_id = query_result[0]['set_id']
    return set_id


def _get_calculation(rdf_type: str, distance: float, dataset_filter: dict, column_name: str = None) -> CalculationMetadata:
    from agent.utils.kg_client import kg_client

    query_template = """
    SELECT ?calculation
    WHERE {{
        SERVICE<{blazegraph_url}> {{
            {where_clauses}
            {dataset_filter_clauses}
        }}
    }}
    """
    where_clauses = []
    var = 'calculation'
    where_clauses.append(
        f"?{var} a <{rdf_type}>; <{constants.HAS_DISTANCE}> {distance}.")

    if column_name is not None:
        where_clauses.append(
            f"?{var} <{constants.HAS_COLUMN_NAME}> \"{column_name}\".")

    if len(dataset_filter) > 0:  # this one counts the number of key-value pairs
        dataset_filter_where_clauses = get_dataset_filter_where_clauses(
            calc_var='calculation', dataset_filter=dataset_filter)

        query = query_template.format(where_clauses="\n".join(where_clauses),
                                      dataset_filter_clauses="\n".join(dataset_filter_where_clauses), blazegraph_url=BLAZEGRAPH_URL)

        query_results = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        if len(query_results) == 0:
            raise Exception(f"No results for {dataset_filter}")

        if len(query_results) == 1:
            return query_results[0]['calculation']
        else:
            raise Exception('More than 1 calculation queried?')

    if len(dataset_filter) == 0:
        filter_not_exists = f"FILTER NOT EXISTS {{?calculation <{constants.HAS_DATASET_FILTER}> ?filter}}"
        query = query_template.format(where_clauses="\n".join(
            where_clauses), dataset_filter_clauses=filter_not_exists, blazegraph_url=BLAZEGRAPH_URL)

        query_results = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        if len(query_results) == 0:
            logger.warning(
                f"No results for calculation query without dataset filters")
            return

        if len(query_results) == 1:
            return query_results[0]['calculation']
        else:
            raise Exception('More than 1 calculation queried')


def _create_geoserver_layer(geoserver_layer_name, geoserver_workspace, subject_table):
    with open("agent/interactor/resources/geoserver_layer.sql", "r") as f:
        geoserver_layer = f.read()

    geoserver_layer = geoserver_layer.format(SUBJECT_TABLE=subject_table)

    geoServerClient = stack_clients_view.GeoServerClient.getInstance()
    geoServerClient.createWorkspace(geoserver_workspace)

    virtualTable = stack_clients_view.UpdatedGSVirtualTableEncoder()
    virtualTable.setSql(geoserver_layer)
    virtualTable.setEscapeSql(True)
    virtualTable.setName(geoserver_layer_name)
    virtualTable.addVirtualTableParameter("exposure", "", ".*")
    virtualTable.addVirtualTableParameter("calculation", "", ".*")
    virtualTable.addVirtualTableParameter("set_id", "1", "\\d+")
    virtualTable.addVirtualTableGeometry("wkb_geometry", "Geometry", "4326")

    geoServerVectorSettings = stack_clients_view.GeoServerVectorSettings()
    geoServerVectorSettings.setVirtualTable(virtualTable)
    geoServerClient.createPostGISDataStore(
        geoserver_workspace, "exposure", "postgres", "public")
    geoServerClient.createPostGISLayer(
        geoserver_workspace, "postgres", "public", geoserver_layer_name, geoServerVectorSettings)


def _update_data_json(host, layer_group_name, exposure, calculation, set_id, geoserver_layer_name, geoserver_workspace, num_bin, colour_scheme, other_paint_properties):
    path = Path(VIS_DATA_JSON)
    with path.open("r", encoding="utf-8") as f:
        data_json = json.load(f)

    wms_path = "/geoserver/twa/wms?service=WMS&version=1.1.0&request=GetMap&bbox=%7Bbbox-epsg-3857%7D&width=256&height=256&srs=EPSG:3857&format=application/vnd.mapbox-vector-tile"

    params = {"layers": f"{geoserver_workspace}:{geoserver_layer_name}",
              "viewparams": f"exposure:{exposure};calculation:{calculation};set_id:{set_id}"}

    base_url = host + wms_path

    wms_url = f"{base_url}&{urlencode(params)}"

    group = {}
    group["name"] = layer_group_name
    group["expanded"] = False
    group["stack"] = host + "/exposure-feature-info-agent/"

    source = {}
    source["type"] = "vector"
    source["tiles"] = [wms_url]
    source["id"] = "exposure-source"
    group["sources"] = [source]

    layers = []
    layout = {}
    layout["visibility"] = "none"
    group["layers"] = layers

    cmap = plt.get_cmap(colour_scheme, num_bin)
    colors = [to_hex(cmap(i)) for i in range(cmap.N)]

    percentile_range = {}
    percentile_colour = {}
    step = 100 / num_bin
    for i in range(num_bin):
        percentile_range[f"{i * step}-{(i + 1) * step}"] = (i *
                                                            step, (i + 1) * step)
        percentile_colour[f"{i * step}-{(i + 1) * step}"] = colors[i]

    for percentile in percentile_range:
        layer = {}
        layer["id"] = f"{percentile}-id"
        layer["name"] = percentile
        layer["source"] = source["id"]
        layer["source-layer"] = geoserver_layer_name
        layer["type"] = "circle"
        layer["layout"] = layout
        layer["filter"] = ["all",
                           [">", ["get", "percentile"],
                               percentile_range[percentile][0]],
                           ["<=", ["get", "percentile"],
                               percentile_range[percentile][1]]
                           ]
        circle_color = {"circle-color": percentile_colour[percentile]}
        if other_paint_properties is not None:
            layer["paint"] = circle_color | other_paint_properties
        else:
            layer["paint"] = circle_color

        layers.append(layer)

    groups = data_json["groups"]
    replaced = False
    for i, d in enumerate(groups):
        if d["name"] == layer_group_name:
            groups[i] = group
            replaced = True
            break

    if not replaced:
        groups.append(group)

    with open(VIS_DATA_JSON, "w") as f:
        json.dump(data_json, f, indent=2)


def _create_geoserver_layer_2d(geoserver_layer_name, geoserver_workspace, subject_table):
    with open("agent/interactor/resources/geoserver_layer_2d.sql", "r") as f:
        geoserver_layer = f.read()

    geoserver_layer = geoserver_layer.format(SUBJECT_TABLE=subject_table)

    geoServerClient = stack_clients_view.GeoServerClient.getInstance()
    geoServerClient.createWorkspace(geoserver_workspace)

    virtualTable = stack_clients_view.UpdatedGSVirtualTableEncoder()
    virtualTable.setSql(geoserver_layer)
    virtualTable.setEscapeSql(True)
    virtualTable.setName(geoserver_layer_name)
    virtualTable.addVirtualTableParameter("exposure1", "", ".*")
    virtualTable.addVirtualTableParameter("calculation1", "", ".*")
    virtualTable.addVirtualTableParameter("set_id1", "1", "\\d+")
    virtualTable.addVirtualTableParameter("exposure2", "", ".*")
    virtualTable.addVirtualTableParameter("calculation2", "", ".*")
    virtualTable.addVirtualTableParameter("set_id2", "1", "\\d+")
    virtualTable.addVirtualTableGeometry("wkb_geometry", "Geometry", "4326")

    geoServerVectorSettings = stack_clients_view.GeoServerVectorSettings()
    geoServerVectorSettings.setVirtualTable(virtualTable)
    geoServerClient.createPostGISDataStore(
        geoserver_workspace, "exposure", "postgres", "public")
    geoServerClient.createPostGISLayer(
        geoserver_workspace, "postgres", "public", geoserver_layer_name, geoServerVectorSettings)


def _update_data_json_2d(host, layer_group_name, exposure1, calculation1, set_id1, exposure2, calculation2, set_id2, geoserver_layer_name, geoserver_workspace, other_paint_properties, colour_palette):
    path = Path(VIS_DATA_JSON)
    with path.open("r", encoding="utf-8") as f:
        data_json = json.load(f)

    wms_path = "/geoserver/twa/wms?service=WMS&version=1.1.0&request=GetMap&bbox=%7Bbbox-epsg-3857%7D&width=256&height=256&srs=EPSG:3857&format=application/vnd.mapbox-vector-tile"

    params = {"layers": f"{geoserver_workspace}:{geoserver_layer_name}",
              "viewparams": f"exposure1:{exposure1};calculation1:{calculation1};set_id1:{set_id1};exposure2:{exposure2};calculation2:{calculation2};set_id2:{set_id2}"}

    base_url = host + wms_path

    wms_url = f"{base_url}&{urlencode(params)}"

    group = {}
    group["name"] = layer_group_name
    group["expanded"] = False
    group["stack"] = host + "/exposure-feature-info-agent/"

    source = {}
    source["type"] = "vector"
    source["tiles"] = [wms_url]
    source["id"] = "exposure-source"
    group["sources"] = [source]

    layers = []
    layout = {}
    layout["visibility"] = "none"
    group["layers"] = layers

    percentile_range = {}
    percentile_colour = {}
    step1 = 100 / len(colour_palette)
    step2 = 100 / len(colour_palette[0])

    for i in range(len(colour_palette)):
        for j in range(len(colour_palette[0])):
            percentile_range[f"{i * step1}-{(i + 1) * step1}-{j * step2}-{(j + 1) * step2}"] = [(i *
                                                                                                 step1, (i + 1) * step1), (j * step2, (j + 1) * step2)]
            percentile_colour[f"{i * step1}-{(i + 1) * step1}-{j * step2}-{(j + 1) * step2}"] = colour_palette[i][j]

    for percentile in percentile_range:
        layer = {}
        layer["id"] = f"{percentile}-id"
        layer["name"] = percentile
        layer["source"] = source["id"]
        layer["source-layer"] = geoserver_layer_name
        layer["type"] = "circle"
        layer["layout"] = layout
        layer["filter"] = ["all",
                           [">", ["get", "percentile1"],
                               percentile_range[percentile][0][0]],
                           [">", ["get", "percentile2"],
                               percentile_range[percentile][1][0]],
                           ["<=", ["get", "percentile1"],
                               percentile_range[percentile][0][1]],
                           ["<=", ["get", "percentile2"],
                            percentile_range[percentile][1][1]]
                           ]
        circle_color = {"circle-color": percentile_colour[percentile]}
        if other_paint_properties is not None:
            layer["paint"] = circle_color | other_paint_properties
        else:
            layer["paint"] = circle_color

        layers.append(layer)

    groups = data_json["groups"]
    replaced = False
    for i, d in enumerate(groups):
        if d["name"] == layer_group_name:
            groups[i] = group
            replaced = True
            break

    if not replaced:
        groups.append(group)

    with open(VIS_DATA_JSON, "w") as f:
        json.dump(data_json, f, indent=2)
