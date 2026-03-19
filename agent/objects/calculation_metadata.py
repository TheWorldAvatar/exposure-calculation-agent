import json
import uuid
from agent.utils import constants
from dateutil.parser import parse

from agent.utils.stack_configs import BLAZEGRAPH_URL


class CalculationMetadataException(Exception):
    """Exception for CalculationMetadata"""


class CalculationMetadata():
    def __init__(self, rdf_type, distance: float, upperbound=None, lowerbound=None, iri=None, dataset_filter: dict = {}):
        self.rdf_type = rdf_type
        self.distance = distance
        self.upperbound = upperbound
        self.lowerbound = lowerbound
        self.iri = iri
        self.dataset_filter = dataset_filter

    def get_query(self, var: str) -> str:
        query = f"""
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT DISTINCT ?{var}
        WHERE {{
            SERVICE<{BLAZEGRAPH_URL}> {{
                ?{var} a <{self.rdf_type}>.
                {self.__get_where_clauses(var)}
            }}
        }}
        """
        return query

    def get_insert_query(self, calculation_iri: str) -> str:
        insert_triples = [f"<{calculation_iri}> a <{self.rdf_type}>."]
        insert_triples.append(
            f"<{calculation_iri}> <{constants.HAS_DISTANCE}> {self.distance}.")

        if self.upperbound is not None and is_integer(self.upperbound):
            insert_triples.append(
                f"<{calculation_iri}> <{constants.HAS_UPPERBOUND}> {self.upperbound}.")
        elif self.upperbound is not None and is_datetime(self.upperbound):
            insert_triples.append(
                f"<{calculation_iri}> <{constants.HAS_UPPERBOUND}> \"{self.upperbound}\"^^xsd:dateTime.")

        if self.lowerbound is not None and is_integer(self.lowerbound):
            insert_triples.append(
                f"<{calculation_iri}> <{constants.HAS_LOWERBOUND}> {self.lowerbound}.")
        elif self.lowerbound is not None and is_datetime(self.lowerbound):
            insert_triples.append(
                f"<{calculation_iri}> <{constants.HAS_LOWERBOUND}> \"{self.lowerbound}\"^^xsd:dateTime.")

        if self.dataset_filter:
            for key, value in self.dataset_filter.items():
                dataset_filter_iri = constants.PREFIX_EXPOSURE + \
                    'datasetfilter/' + str(uuid.uuid4())
                insert_triples.append(
                    f"<{dataset_filter_iri}> a <{constants.DATASET_FITLER}>.")
                insert_triples.append(
                    f"<{calculation_iri}> <{constants.HAS_DATASET_FILTER}> <{dataset_filter_iri}>.")
                insert_triples.append(
                    f"<{dataset_filter_iri}> <{constants.HAS_FILTER_COLUMN}> '{key}'.")
                insert_triples.append(
                    f"<{dataset_filter_iri}> <{constants.HAS_FILTER_VALUE}> {format_rdf_literal(value)}.")

        query = f"""
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        INSERT DATA {{
            {"\n".join(insert_triples)}
        }}
        """
        return query

    def __get_where_clauses(self, var: str) -> str:
        where_clauses = [f"?{var} <{constants.HAS_DISTANCE}> {self.distance}."]

        if self.upperbound is not None and is_integer(self.upperbound):
            where_clauses.append(
                f"?{var} <{constants.HAS_UPPERBOUND}> {self.upperbound}.")
        elif self.upperbound is not None and is_datetime(self.upperbound):
            where_clauses.append(
                f"?{var} <{constants.HAS_UPPERBOUND}> \"{self.upperbound}\"^^xsd:dateTime.")
        elif self.upperbound is not None:
            raise CalculationMetadataException(
                'Unsupported format of upperbound')

        if self.lowerbound is not None and is_integer(self.lowerbound):
            where_clauses.append(
                f"?{var} <{constants.HAS_LOWERBOUND}> {self.lowerbound}.")
        elif self.lowerbound is not None and is_datetime(self.lowerbound):
            where_clauses.append(
                f"?{var} <{constants.HAS_LOWERBOUND}> \"{self.lowerbound}\"xsd:dateTime.")
        elif self.lowerbound is not None:
            raise CalculationMetadataException(
                'Unsupported format of lowerbound')

        if self.dataset_filter:
            where_clauses.extend(get_dataset_filter_where_clauses(
                calc_var=var, dataset_filter=self.dataset_filter))

        return "\n".join(where_clauses)


def get_dataset_filter_where_clauses(calc_var: str, dataset_filter: dict):
    where_clauses = []
    i = 0
    for key, value in dataset_filter.items():
        where_clauses.append(
            f"?{calc_var} <{constants.HAS_DATASET_FILTER}> ?filter{i}.")
        where_clauses.append(
            f"?filter{i} <{constants.HAS_FILTER_COLUMN}> '{key}'; <{constants.HAS_FILTER_VALUE}> {format_rdf_literal(value)}.")
        i += 1

    # no extra pairs allowed
    values_block = "\n".join(
        f'("{k}" {format_rdf_literal(v)})'
        for k, v in dataset_filter.items()
    )
    where_clauses.append(f"""
        FILTER NOT EXISTS {{
            ?{calc_var} <{constants.HAS_DATASET_FILTER}> ?fExtra .
            ?fExtra <{constants.HAS_FILTER_COLUMN}> ?c ;
                    <{constants.HAS_FILTER_VALUE}>  ?v .

            FILTER NOT EXISTS {{
                VALUES (?col ?val) {{
                    {values_block}
                }}
            FILTER (?c = ?col && ?v = ?val)
            }}
        }}          
    """)

    return where_clauses


def get_calculation_metadata(iri: str) -> CalculationMetadata:
    from agent.utils.kg_client import kg_client
    query = f"""
    SELECT DISTINCT ?rdf_type ?distance ?upperbound ?lowerbound ?filter_column ?filter_value
    WHERE
    {{
        <{iri}> a ?rdf_type.
        OPTIONAL{{<{iri}> <{constants.HAS_DISTANCE}> ?distance.}}
        OPTIONAL{{<{iri}> <{constants.HAS_UPPERBOUND}> ?upperbound.}}
        OPTIONAL{{<{iri}> <{constants.HAS_LOWERBOUND}> ?lowerbound.}}
        OPTIONAL{{<{iri}> <{constants.HAS_DATASET_FILTER}> ?dataset_filter.
            ?dataset_filter <{constants.HAS_FILTER_COLUMN}> ?filter_column;
            <{constants.HAS_FILTER_VALUE}> ?filter_value.}}
    }}
    """

    query_results = json.loads(
        kg_client.remote_store_client.executeQuery(query).toString())

    rdf_type = None
    distance = None
    upperbound = None
    lowerbound = None
    dataset_filter = {}
    for row in query_results:
        if rdf_type is not None and rdf_type != row['rdf_type']:
            raise Exception('more than 1 rdf type')
        rdf_type = row['rdf_type']

        if distance is not None and distance != float(row['distance']):
            raise Exception('unexpected distance value')
        distance = float(row['distance'])

        if 'upperbound' in row:
            if upperbound is not None and upperbound != row['upperbound']:
                raise Exception('unexpected upperbound value')
            else:
                upperbound = row['upperbound']

        if 'lowerbound' in row:
            if lowerbound is not None and lowerbound != row['lowerbound']:
                raise Exception('unexpected lowerbound value')
            else:
                lowerbound = row['lowerbound']

        if 'filter_column' in row:
            dataset_filter[row['filter_column']
                           ] = parse_value(row['filter_value'])

    return CalculationMetadata(rdf_type=rdf_type, distance=distance, upperbound=upperbound, lowerbound=lowerbound, iri=iri, dataset_filter=dataset_filter)


def is_integer(s: str) -> bool:
    try:
        int(s)
        return True
    except (ValueError, TypeError):
        return False


def is_datetime(s: str) -> bool:
    try:
        parse(s)
        return True
    except (ValueError, TypeError):
        return False


def parse_value(s: str):
    # parse result from sparql query into native python type
    s_clean = s.strip()
    s_lower = s_clean.lower()

    # 1. Boolean first
    if s_lower == 'true':
        return True
    if s_lower == 'false':
        return False

    # 2. Integer check
    try:
        # Only allow pure integers
        if '.' not in s_clean:
            return int(s_clean)
    except ValueError:
        pass

    # 3. Float check
    try:
        return float(s_clean)
    except ValueError:
        pass

    # 4. Fallback: string
    return s_clean


def format_rdf_literal(value):
    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        if value:
            return "true"
        else:
            return "false"
    else:
        return value
