import uuid
from agent.calculation.calculation_input import CalculationInput
from agent.objects.exposure_dataset import get_exposure_dataset
from agent.utils.postgis_client import postgis_client
from agent.utils.stack_configs import BLAZEGRAPH_URL, ONTOP_URL
from twa import agentlogging
import agent.utils.constants as constants
import re

logger = agentlogging.get_logger('dev')


def simple_count(calculation_input: CalculationInput):
    iri_to_point_dict = _get_iri_to_point_dict(calculation_input.subject)
    subject_to_result_dict = {}

    exposure_dataset = get_exposure_dataset(calculation_input.exposure)

    with open("agent/calculation/templates/count.sql", "r") as f:
        sql = f.read()

    sql = sql.format(TABLE_PLACEHOLDER=exposure_dataset.table_name)

    logger.info('Submitting SQL queries for calculations')
    with postgis_client.connect() as conn:
        with conn.cursor() as cur:
            for iri, point in iri_to_point_dict.items():
                replacements = {
                    'SRID_PLACEHOLDER': 4326,
                    'GEOMETRY_PLACEHOLDER': point,
                    'DISTANCE_PLACEHOLDER': calculation_input.calculation_metadata.distance
                }
                cur.execute(sql, replacements)
                if cur.description:
                    query_result = cur.fetchall()
                    subject_to_result_dict[iri] = query_result[0][0]

    logger.info('Instantiating results')
    _instantiate_result(subject_to_result_dict, calculation_input)
    return 'Calculation complete', 200


def _get_iri_to_point_dict(subject):
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
    if isinstance(subject, list):
        # submitting results in chunks because querying more than 100k of geometries causes Ontop to crash
        for chunk in _chunk_list(subject):
            values = " ".join(f"<{s}>" for s in chunk)
            query = query_template.format(values=values)
            query_result = kg_client.remote_store_client.executeFederatedQuery(
                [BLAZEGRAPH_URL, ONTOP_URL], query)

            for i in range(query_result.length()):
                sub = query_result.getJSONObject(i).getString('subject')
                wkt_literal = query_result.getJSONObject(i).getString('wkt')

                # strip RDF literal IRI, i.e. ^^<http://www.opengis.net/ont/geosparql#wktLiteral>
                match = re.match(r'^"(.+)"\^\^<.+>$', wkt_literal)
                if match:
                    wkt_str = match.group(1)
                    iri_to_point_dict[sub] = wkt_str
                else:
                    logger.error("Invalid WKT literal format: " + wkt_literal)

    else:
        values = f"<{subject}>"
        query = query_template.format(values=values)
        query_result = kg_client.remote_store_client.executeFederatedQuery(
            [BLAZEGRAPH_URL, ONTOP_URL], query)

        for i in range(query_result.length()):
            sub = query_result.getJSONObject(i).getString('subject')
            wkt = query_result.getJSONObject(i).getString('wkt')
            iri_to_point_dict[sub] = wkt

    return iri_to_point_dict


def _instantiate_result(subject_to_value_dict: dict, calculation_input: CalculationInput):
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
            <{derivation_iri}> <{constants.IS_DERIVED_FROM}> <{calculation_input.exposure}>;
                <{constants.IS_DERIVED_FROM}> <{subject}>;
                <{constants.IS_DERIVED_USING}> <{calculation_input.calculation_metadata.iri}>.
            <{result_iri}> a <{constants.EXPOSURE_RESULT}>;
                <{constants.BELONGS_TO}> <{derivation_iri}>;
                <{constants.HAS_VALUE}> {subject_to_value_dict[subject]}.
            """

            insert_triples.append(insert_triple)

        insert_query = f"""
        INSERT DATA {{{"\n".join(insert_triples)}}}
        """

        kg_client.remote_store_client.executeUpdate(insert_query)


def _chunk_list(values, chunk_size=10000):
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]
