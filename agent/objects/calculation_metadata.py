from agent.utils.constants import HAS_DISTANCE, HAS_LOWERBOUND, HAS_UPPERBOUND
from dateutil.parser import parse


class CalculationMetadataException(Exception):
    """Exception for CalculationMetadata"""


class CalculationMetadata():
    def __init__(self, rdf_type, distance, upperbound=None, lowerbound=None, iri=None):
        self.rdf_type = rdf_type
        self.distance = distance
        self.upperbound = upperbound
        self.lowerbound = lowerbound
        self.iri = iri

    def get_query(self, var: str) -> str:
        query = f"""
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?{var}
        WHERE {{
            ?{var} a <{self.rdf_type}>.
            {self.__get_where_clauses(var)}
        }}
        """
        return query

    def get_insert_query(self, calculation_iri: str) -> str:
        insert_triples = [f"<{calculation_iri}> a <{self.rdf_type}>."]
        insert_triples.append(
            f"<{calculation_iri}> <{HAS_DISTANCE}> {self.distance}.")

        if self.upperbound is not None and is_integer(self.upperbound):
            insert_triples.append(
                f"<{calculation_iri}> <{HAS_UPPERBOUND}> {self.upperbound}.")
        elif self.upperbound is not None and is_datetime(self.upperbound):
            insert_triples.append(
                f"<{calculation_iri}> <{HAS_UPPERBOUND}> \"{self.upperbound}\"^^xsd:dateTime.")

        if self.lowerbound is not None and is_integer(self.lowerbound):
            insert_triples.append(
                f"<{calculation_iri}> <{HAS_LOWERBOUND}> {self.lowerbound}.")
        elif self.lowerbound is not None and is_datetime(self.lowerbound):
            insert_triples.append(
                f"<{calculation_iri}> <{HAS_LOWERBOUND}> \"{self.lowerbound}\"^^xsd:dateTime.")

        query = f"""
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        INSERT DATA {{
            {"\n".join(insert_triples)}
        }}
        """
        return query

    def __get_where_clauses(self, var: str) -> str:
        where_clauses = [f"?{var} <{HAS_DISTANCE}> {self.distance}."]

        if self.upperbound is not None and is_integer(self.upperbound):
            where_clauses.append(
                f"?{var} <{HAS_UPPERBOUND}> {self.upperbound}.")
        elif self.upperbound is not None and is_datetime(self.upperbound):
            where_clauses.append(
                f"?{var} <{HAS_UPPERBOUND}> \"{self.upperbound}\"^^xsd:dateTime.")
        elif self.upperbound is not None:
            raise CalculationMetadataException(
                'Unsupported format of upperbound')

        if self.lowerbound is not None and is_integer(self.lowerbound):
            where_clauses.append(
                f"?{var} <{HAS_LOWERBOUND}> {self.lowerbound}.")
        elif self.lowerbound is not None and is_datetime(self.lowerbound):
            where_clauses.append(
                f"?{var} <{HAS_LOWERBOUND}> \"{self.lowerbound}\"xsd:dateTime.")
        elif self.lowerbound is not None:
            raise CalculationMetadataException(
                'Unsupported format of lowerbound')

        return "\n".join(where_clauses)


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
