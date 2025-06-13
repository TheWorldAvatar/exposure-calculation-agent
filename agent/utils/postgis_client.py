import psycopg2
from urllib.parse import urlparse
from agent.utils.stack_configs import RDB_URL, RDB_PASSWORD, RDB_USER


class PostGISClient:
    def __init__(self):
        jdbc_url = RDB_URL
        jdbc_url = jdbc_url[5:]
        parsed = urlparse(jdbc_url)

        self.dbname = parsed.path.lstrip('/')
        self.host = parsed.hostname
        self.port = parsed.port

    def connect(self):
        return psycopg2.connect(dbname=self.dbname, user=RDB_USER, password=RDB_PASSWORD, host=self.host, port=self.port)

    def execute_query(self, conn, query, params=None):
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
            return None


postgis_client = PostGISClient()
