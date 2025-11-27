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
from tqdm import tqdm
import sys

from agent.utils.ts_client import TimeSeriesClient

logger = agentlogging.get_logger('dev')

csv_export_bp = Blueprint(
    'csv_export', __name__, url_prefix='/csv_export')


@csv_export_bp.route('/greenspace', methods=['GET'])
def greenspace():
    # IRI(s) of subject to calculate
    subject = request.args.get('subject')

    if subject is not None:
        subject = [subject]

    exposure_table_list = request.args.getlist('exposure_table')
    rdf_type_list = request.args.getlist('rdf_type')

    # query to obtain subject IRIs
    subject_query_file = request.args.get('subject_query_file')

    # query for user facing label of subject IRI, e.g. postcode value
    subject_label_query_file = request.args.get('subject_label_query_file')

    if subject is not None and subject_query_file is not None:
        raise Exception('Provide subject or subject_query_file, but not both')

    # do SPARQL query to obtain a list of subject IRIs
    if subject_query_file is not None:
        subject = _get_subjects(subject_query_file=subject_query_file)

    # returns IRI to label
    logger.info('Querying label')
    subject_to_label_dict = _get_subject_to_label_dict(
        subject_label_query_file=subject_label_query_file, subjects=subject)

    logger.info('Getting subject coordinates')
    subject_to_point_dict = _get_subject_to_point_dict(subject=subject)

    # dictionary hierarchy [dataset_year][calculation][subject][distance]
    overall_result = defaultdict(lambda: defaultdict(dict))
    for exposure_table in exposure_table_list:
        exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)
        dataset_year = _get_dataset_year(exposure_dataset_iri)
        for calculation in rdf_type_list:
            logger.info(
                f"""Querying results for calculation: <{calculation}>, dataset: {exposure_table}""")
            subject_to_result_dict = _get_subject_to_result_dict(
                subject=subject, exposure=exposure_dataset_iri, calculation_type=calculation)
            overall_result[dataset_year][calculation] = subject_to_result_dict

    logger.info('Generating CSV file')
    csv = _create_csv(overall_result=overall_result, subject_to_label_dict=subject_to_label_dict,
                      subject_to_point_dict=subject_to_point_dict)

    response = Response(csv.getvalue(), mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    return response


@csv_export_bp.route('/trajectory', methods=['GET'])
def trajectory():
    from agent.utils.kg_client import kg_client
    logger.info('Received request to generate CSV file for trajectory')

    # IRI(s) of subject to calculate
    subject = request.args.get('subject')

    exposure_table_list = request.args.getlist('exposure_table')
    rdf_type_list = request.args.getlist('rdf_type')

    trip_iri = _get_trip(subject)

    # for trajectory time series
    upperbound = request.args.get('upperbound')
    lowerbound = request.args.get('lowerbound')

    data_iri_list_to_query = [subject]
    if trip_iri is not None:
        data_iri_list_to_query.append(trip_iri)

    # dictionary hierarchy [dataset_year][calculation][distance] = result_iri
    overall_result = defaultdict(lambda: defaultdict(dict))
    for exposure_table in exposure_table_list:
        exposure_dataset_iri = get_dataset_iri(table_name=exposure_table)
        dataset_year = _get_dataset_year(exposure_dataset_iri)
        for calculation in rdf_type_list:
            logger.info(
                f"""Querying results for calculation: <{calculation}>, dataset: {exposure_table}""")
            distance_to_result_dict = _get_distance_to_result_dict(
                subject=subject, exposure=exposure_dataset_iri, calculation_type=calculation)
            overall_result[dataset_year][calculation] = distance_to_result_dict
            data_iri_list_to_query.extend(distance_to_result_dict.values())

    logger.info('Querying time series')
    time_series = kg_client.get_time_series_data(
        data_iri_list_to_query, lowerbound, upperbound)
    points = [wkt.loads(s) for s in time_series.get_value_list(subject)]

    lat_list = []
    lng_list = []

    for point in points:
        lat_list.append(point.y)
        lng_list.append(point.x)

    time_list = time_series.get_timestamp_java(subject)

    data_to_write = [time_list, lat_list, lng_list]
    headers = ['time', 'lat', 'lng']

    if trip_iri is not None:
        data_to_write.append(time_series.get_value_list(trip_iri))
        headers.append('trip index')

    for year in overall_result.keys():
        for calculation in overall_result[year].keys():
            for distance in overall_result[year][calculation].keys():
                result_iri = overall_result[year][calculation][distance]
                result_list = time_series.get_value_list(result_iri)
                data_to_write.append(result_list)
                headers.append(
                    str(year) + '_' + re.findall(r'[^/]+$', calculation)[0] + '_' + str(distance))

    rows = zip(*data_to_write)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"

    return response


def _get_subject_to_result_dict(subject, exposure, calculation_type):
    from agent.utils.kg_client import kg_client
    subject_to_result_dict = defaultdict(lambda: defaultdict(dict))

    for chunk in tqdm(_chunk_list(subject), mininterval=60, ncols=80, file=sys.stdout):
        values = " ".join(f"<{s}>" for s in chunk)
        # SERVICE is used here to speed up the queries..
        query = f"""
        SELECT ?subject ?value ?distance
        WHERE {{
            SERVICE <{BLAZEGRAPH_URL}> {{?calculation a <{calculation_type}>;
                <{constants.HAS_DISTANCE}> ?distance.}}
            SERVICE <{ONTOP_URL}> {{VALUES ?subject {{{values}}}
            ?derivation <{constants.IS_DERIVED_FROM}> ?subject;
                <{constants.IS_DERIVED_FROM}> <{exposure}>.
            ?result <{constants.BELONGS_TO}> ?derivation;
                <{constants.EXP_HAS_VALUE}> ?value;
                <{constants.HAS_CALCULATION_METHOD}> ?calculation.}}
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


def _get_distance_to_result_dict(subject, exposure, calculation_type):
    from agent.utils.kg_client import kg_client
    distance_to_result_dict = {}

    query = f"""
    SELECT ?result ?distance
    WHERE {{
        ?calculation a <{calculation_type}>;
            <{constants.HAS_DISTANCE}> ?distance.
        ?derivation <{constants.IS_DERIVED_FROM}> <{subject}>;
            <{constants.IS_DERIVED_FROM}> <{exposure}>.
        ?result <{constants.BELONGS_TO}> ?derivation;
            <{constants.HAS_CALCULATION_METHOD}> ?calculation.
    }}
    """

    query_result = json.loads(
        kg_client.remote_store_client.executeQuery(query).toString())
    for row in query_result:
        distance_to_result_dict[row['distance']] = row['result']

    return distance_to_result_dict


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
    query_result = kg_client.remote_store_client.executeQuery(subject_query)

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

        query_result = kg_client.remote_store_client.executeQuery(query)

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
        query_result = json.loads(
            kg_client.remote_store_client.executeQuery(query).toString())

        for row in query_result:
            sub = row['subject']
            wkt_literal = row['wkt']

            # strip RDF literal IRI, i.e. ^^<http://www.opengis.net/ont/geosparql#wktLiteral>
            match = re.match(r'^"(.+)"\^\^<.+>$', wkt_literal)
            if match:
                geom = wkt.loads(match.group(1))
            else:
                geom = wkt.loads(wkt_literal)

            iri_to_point_dict[sub] = geom

    return iri_to_point_dict


def _get_trip(point_iri: str):
    from agent.utils.kg_client import kg_client
    query = f"""
    SELECT ?trip
    WHERE {{
        <{point_iri}> <{constants.HAS_TIME_SERIES}> ?time_series.
        ?trip <{constants.HAS_TIME_SERIES}> ?time_series;
            a <{constants.TRIP}> .
    }}
    """
    query_results = kg_client.remote_store_client.executeQuery(query)

    if query_results.isEmpty():
        return None
    elif query_results.length() > 1:
        raise Exception('More than 1 trip instance detected?')
    else:
        return query_results.getJSONObject(0).getString('trip')


def _create_csv(overall_result, subject_to_label_dict, subject_to_point_dict):

    subject_to_header = defaultdict(lambda: defaultdict(dict))

    for year in overall_result.keys():
        calculation_to_subject = overall_result[year]

        for calculation in calculation_to_subject.keys():
            subject_to_distance = calculation_to_subject[calculation]

            for subject in subject_to_distance.keys():
                distance_to_value = subject_to_distance[subject]

                for distance in distance_to_value.keys():
                    value = distance_to_value[distance]

                    header = 'greenspace_' + \
                        re.findall(r'[^/]+$', calculation)[0] + \
                        '_' + distance + 'm_' + year
                    header = header.lower()
                    subject_to_header[subject][header] = value
    data = []

    for subject in subject_to_header.keys():
        label = subject_to_label_dict[subject]
        value = subject_to_header[subject]
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


def _get_dataset_year(dataset_iri):
    from agent.utils.kg_client import kg_client
    # distinct is a fudge due to bug in stack data uploader that uploads duplicates
    query = f"""
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dcat: <http://www.w3.org/ns/dcat#>

    SELECT DISTINCT (YEAR(?startdate) AS ?year)
    WHERE {{
        <{dataset_iri}> dcterms:temporal/dcat:startDate ?startdate.
    }}
    """

    query_results = kg_client.remote_store_client.executeQuery(query)

    return query_results.getJSONObject(0).getString('year')


def _chunk_list(values, chunk_size=1000):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]
