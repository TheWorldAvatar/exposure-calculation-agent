from agent.utils.constants import HAS_DISTANCE


class CalculationMetadata():
    def __init__(self, metadata):
        self.rdf_type = metadata.get('rdf_type')
        self.distance = metadata.get('distance')
        self.upperbound = metadata.get('upperbound')
        self.lowerbound = metadata.get('lowerbound')

    def get_distance(self) -> float:
        return self.distance

    def get_rdf_type(self) -> str:
        return self.rdf_type

    def get_upperbound(self):
        return self.upperbound

    def get_lowerbound(self):
        return self.lowerbound

    def get_query(self, var: str) -> str:
        query = f"""
        SELECT ?{var}
        WHERE {{
            ?{var} a <{self.rdf_type}>.
            {self.__get_where_clauses(var)}
        }}
        """
        return query

    def __get_where_clauses(self, var: str) -> str:
        where_clause = f"""
        {{?{var} <{HAS_DISTANCE}> {self.distance}}}
        """
        return where_clause
