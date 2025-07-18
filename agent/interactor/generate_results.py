import re
from flask import Blueprint, Response, request
from twa import agentlogging
from agent.interactor.trigger_calculation import get_dataset_iri
import agent.utils.constants as constants
from pathlib import Path
from rdflib.plugins.sparql.parser import parseQuery
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL
import csv
import io
from shapely import wkt
from collections import defaultdict
import json

logger = agentlogging.get_logger('dev')

generate_results_bp = Blueprint(
    'generate_results', __name__, url_prefix='/generate_results')


@generate_results_bp.route('/', methods=['GET'])
def non_trajectory():
    from agent.utils.kg_client import kg_client
    # IRI(s) of subject to calculate
    subject = request.args.get('subject')
    exposure_table = request.args.get('exposure_table')

    # query to obtain subject IRIs
    subject_query_file = request.args.get('subject_query_file')

    # query for user facing label of subject IRI, e.g. postcode value
    subject_label_query_file = request.args.get('subject_label_query_file')

    # rdf type of calculation type
    rdf_type = request.args.get('rdf_type')

    if subject is not None and subject_query_file is not None:
        raise Exception('Provide subject or subject_query_file, but not both')

    # do SPARQL query to obtain a list of subject IRIs
    if subject_query_file is not None:
        subject = _get_subjects(subject_query_file=subject_query_file)

    # get dataset iri to pass the core calculation agent
    exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)

    # returns IRI to result
    logger.info('Querying results')
    subject_to_result_dict = _get_subject_to_result_dict(
        subject=subject, exposure=exposure_dataset_iri, calculation_type=rdf_type)

    # returns IRI to label
    logger.info('Querying label')
    subject_to_label_dict = _get_subject_to_label_dict(
        subject_label_query_file=subject_label_query_file, subjects=subject)

    logger.info('Getting subject coordinates')
    subject_to_point_dict = _get_subject_to_point_dict(subject=subject)

    logger.info('Generating CSV file')
    csv = _create_csv(subject_to_label_dict=subject_to_label_dict,
                      subject_to_result_dict=subject_to_result_dict, subject_to_point_dict=subject_to_point_dict)

    response = Response(csv.getvalue(), mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    return response


@generate_results_bp.route('/trajectory', methods=['GET'])
def trajectory():
    from agent.utils.kg_client import kg_client
    # rdf type of calculation type
    rdf_type = request.args.get('rdf_type')

    # metadata for calculation instance
    distance = request.args.get('distance')

    # IRI(s) of subject to calculate
    subject = request.args.get('subject')
    exposure_table = request.args.get('exposure_table')

    # for trajectory time series
    upperbound = request.args.get('upperbound')
    lowerbound = request.args.get('lowerbound')

    # not general, assumes epoch seconds
    if upperbound is not None:
        upperbound = int(upperbound)

    if lowerbound is not None:
        lowerbound = int(lowerbound)

    return ''


def _get_subject_to_result_dict(subject, exposure, calculation_type):
    from agent.utils.kg_client import kg_client
    subject_to_result_dict = defaultdict(lambda: defaultdict(dict))

    for chunk in _chunk_list(subject):
        values = " ".join(f"<{s}>" for s in chunk)
        query = f"""
        SELECT ?subject ?value ?distance
        WHERE {{
            ?calculation a <{calculation_type}>;
                <{constants.HAS_DISTANCE}> ?distance.
            SERVICE <{ONTOP_URL}> {{VALUES ?subject {{{values}}}
                ?derivation <{constants.IS_DERIVED_FROM}> ?subject;
                    <{constants.IS_DERIVED_FROM}> <{exposure}>;
                    <{constants.IS_DERIVED_USING}> ?calculation.
                ?result <{constants.BELONGS_TO}> ?derivation;
                    <{constants.HAS_VALUE}> ?value
            }}
        }}
        """

        # remote store client gives a Java JSONArray
        query_result = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        for item in query_result:
            iri = item['subject']
            distance = item['distance']
            subject_to_result_dict[iri][distance] = item['value']

    return subject_to_result_dict


def _get_select_var(sparql_query):
    parsed = parseQuery(sparql_query)

    if len(parsed[1]['projection']) != 1:
        raise Exception(
            'Provided query needs to have exactly one select variable')

    return str(parsed[1]['projection'][0]['var'])


def _get_subjects(subject_query_file):
    from agent.utils.kg_client import kg_client
    with open(Path(constants.BIND_MOUNT_PATH)/subject_query_file, "r") as f:
        subject_query = f.read()

    select_var = _get_select_var(subject_query)

    logger.info(
        'Querying subject IRIs with provided SPARQL query template')
    query_result = kg_client.ontop_client.executeQuery(subject_query)

    logger.info('Received ' + str(query_result.length()) + ' IRIs')

    subject_list = []
    for i in range(query_result.length()):
        subject_list.append(
            query_result.getJSONObject(i).getString(select_var))

    return subject_list


def _get_subject_to_label_dict(subject_label_query_file, subjects):
    from agent.utils.kg_client import kg_client
    with open(Path(constants.BIND_MOUNT_PATH)/subject_label_query_file, "r") as f:
        query_template = f.read()
    subject_to_label_dict = {}

    for chunk in _chunk_list(subjects):
        query = _insert_values_clause(
            sparql_query=query_template, varname='Feature', uris=chunk)

        query_result = kg_client.remote_store_client.executeFederatedQuery(
            [ONTOP_URL, BLAZEGRAPH_URL], query)

        for i in range(query_result.length()):
            subject_iri = query_result.getJSONObject(i).getString('Feature')
            subject_label = query_result.getJSONObject(i).getString('Label')
            subject_to_label_dict[subject_iri] = subject_label

    return subject_to_label_dict


def _insert_values_clause(sparql_query, varname, uris):
    """ 
    inserts "VALUES ?Feature {..}" right after "WHERE {"
    """
    values = " ".join(f"<{uri}>" for uri in uris)
    values_clause = f"VALUES ?{varname} {{ {values} }}"

    where_index = sparql_query.lower().find("where")
    brace_index = sparql_query.find("{", where_index)
    return sparql_query[:brace_index + 1] + "\n  " + values_clause + "\n" + sparql_query[brace_index + 1:]


def _create_csv(subject_to_result_dict, subject_to_label_dict, subject_to_point_dict):
    data = []

    for subject in subject_to_result_dict.keys():
        label = subject_to_label_dict[subject]
        value = subject_to_result_dict[subject]
        lat = subject_to_point_dict[subject].y
        lng = subject_to_point_dict[subject].x

        data.append({'postal_code': label, 'lat': lat, 'lng': lng} | value)

    output = io.StringIO()
    if data:
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
    else:
        writer = csv.DictWriter(
            output, fieldnames=['postal_code', 'lat', 'lng'])
    writer.writeheader()
    writer.writerows(data)

    return output


def _get_subject_to_point_dict(subject):
    """
    Identical to the one use by core agent but does not convert lat lon..
    """
    from agent.utils.kg_client import kg_client

    iri_to_point_dict = {}

    query_template = """
    SELECT ?subject ?wkt
    WHERE {{
        VALUES ?subject {{{values}}}.
        ?subject <http://www.opengis.net/ont/geosparql#asWKT> ?wkt.
    }}
    """

    logger.info(
        'Querying geometries of subjects, number of subjects: ' + str(len(subject)))

    query_list = []
    # submit queries in batches to avoid crashing ontop
    for chunk in _chunk_list(subject):
        values = " ".join(f"<{s}>" for s in chunk)
        query = query_template.format(values=values)
        query_list.append(query)

    for query in query_list:
        query_result = kg_client.ontop_client.executeQuery(query)

        for i in range(query_result.length()):
            sub = query_result.getJSONObject(i).getString('subject')
            wkt_literal = query_result.getJSONObject(i).getString('wkt')

            # strip RDF literal IRI, i.e. ^^<http://www.opengis.net/ont/geosparql#wktLiteral>
            match = re.match(r'^"(.+)"\^\^<.+>$', wkt_literal)
            if match:
                geom = wkt.loads(match.group(1))
            else:
                geom = wkt.loads(wkt_literal)

            iri_to_point_dict[sub] = geom

    return iri_to_point_dict


def _chunk_list(values, chunk_size=1000):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]
