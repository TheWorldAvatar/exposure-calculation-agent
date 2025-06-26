import uuid
from agent.calculation.calculation_input import CalculationInput
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL
from twa import agentlogging
import agent.utils.constants as constants
import re
from shapely import wkt
from shapely.ops import transform
from pyproj import Transformer
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')


def simple_count(calculation_input: CalculationInput):
    iri_to_point_dict = get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/templates/count.sql", "r") as f:
        sql = f.read()

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            # create temp table for efficiency
            temp_table = 'temp_table'

            create_temp_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT ST_Transform(wkb_geometry, 3857) AS wkb_geometry
            FROM {exposure_dataset.table_name}
            """

            cur.execute(create_temp_sql)

            sql = sql.format(TEMP_TABLE=temp_table)

            for iri, point in tqdm(iri_to_point_dict.items(), mininterval=60, ncols=80, file=sys.stdout):
                replacements = {
                    'GEOMETRY_PLACEHOLDER': point.wkt,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    subject_to_result_dict[iri] = query_result[0][0]

    logger.info('Instantiating results')
    instantiate_result(subject_to_result_dict, calculation_input)

    logger.info('Completed instantiation')
    return 'Calculation complete', 200


def get_iri_to_point_dict(subject):
    from agent.utils.kg_client import kg_client

    iri_to_point_dict = {}
    transformer = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True)

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
        query_result = kg_client.remote_store_client.executeFederatedQuery(
            [BLAZEGRAPH_URL, ONTOP_URL], query)

        for i in range(query_result.length()):
            sub = query_result.getJSONObject(i).getString('subject')
            wkt_literal = query_result.getJSONObject(i).getString('wkt')

            # strip RDF literal IRI, i.e. ^^<http://www.opengis.net/ont/geosparql#wktLiteral>
            match = re.match(r'^"(.+)"\^\^<.+>$', wkt_literal)
            if match:
                geom = wkt.loads(match.group(1))
            else:
                geom = wkt.loads(wkt_literal)

            projected_geom = transform(transformer.transform, geom)
            iri_to_point_dict[sub] = projected_geom

    return iri_to_point_dict


def instantiate_result(subject_to_value_dict: dict, calculation_input: CalculationInput):
    """
    Overwrites existing result if not already instantiated
    """
    from agent.utils.kg_client import kg_client
    # first check which instances already have a previous result instantiated
    values1 = " ".join(f"<{s}>" for s in subject_to_value_dict.keys())

    query = f"""
    SELECT ?subject ?result
    WHERE {{
        VALUES ?subject {{{values1}}}
        ?derivation <{constants.IS_DERIVED_FROM}> ?subject;
            <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>;
            <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
        ?result <{constants.BELONGS_TO}> ?derivation.
    }}
    """

    query_result = kg_client.remote_store_client.executeQuery(query)

    subjects_with_existing_result = []
    subject_to_result_dict = {}
    for i in range(query_result.length()):
        sub = query_result.getJSONObject(i).getString('subject')
        result = query_result.getJSONObject(i).getString('result')
        subjects_with_existing_result.append(sub)
        subject_to_result_dict[sub] = result

    subjects_without_existing_result = list(subject_to_value_dict.keys())
    for s in subjects_with_existing_result:
        subjects_without_existing_result.remove(s)

    # overwrite existing result for instances with existing result
    if len(subjects_with_existing_result) > 0:
        values2 = " ".join(f"<{s}>" for s in subjects_with_existing_result)
        insert_triples = "\n".join(
            f"<{subject_to_result_dict[subject]}> <{constants.HAS_VALUE}> {subject_to_value_dict[subject]} ."
            for subject in subjects_with_existing_result
        )

        update_query = f"""
        DELETE
        {{
            ?result <{constants.HAS_VALUE}> ?value
        }}
        INSERT
        {{
            {insert_triples}
        }}
        WHERE
        {{
            VALUES ?subject {{{values2}}}
            ?derivation <{constants.IS_DERIVED_FROM}> ?subject;
                <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>;
                <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
            ?result <{constants.BELONGS_TO}> ?derivation;
                <{constants.HAS_VALUE}> ?value.
        }}
        """
        kg_client.remote_store_client.executeUpdate(update_query)

    if len(subjects_without_existing_result) > 0:
        insert_triples = []
        for subject in subjects_without_existing_result:
            result_iri = constants.PREFIX_EXPOSURE + \
                'result/' + str(uuid.uuid4())
            derivation_iri = constants.PREFIX_DERIVATION + str(uuid.uuid4())

            insert_triple = f"""
            <{derivation_iri}> a <{constants.DERIVATION}>;
                <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>;
                <{constants.IS_DERIVED_FROM}> <{subject}>;
                <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
            <{result_iri}> a <{constants.EXPOSURE_RESULT}>;
                <{constants.BELONGS_TO}> <{derivation_iri}>;
                <{constants.HAS_VALUE}> {subject_to_value_dict[subject]}.
            """

            insert_triples.append(insert_triple)

        insert_query = f"""
        INSERT DATA {{{"".join(insert_triples)}}}
        """

        kg_client.remote_store_client.executeUpdate(insert_query)


def _chunk_list(values, chunk_size=10000):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]
