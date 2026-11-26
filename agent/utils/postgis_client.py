import psycopg2
from contextlib import contextmanager
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

    @contextmanager
    def connect(self):
        """Own a transaction and close its connection on every exit path.

        Callers must use ``with client.connect() as conn``. Psycopg2's own
        context manages commit/rollback; the finally block ends the session,
        releasing temporary tables as well as the database connection.
        """
        conn = psycopg2.connect(dbname=self.dbname, user=RDB_USER,
                                password=RDB_PASSWORD, host=self.host, port=self.port)
        try:
            with conn:
                yield conn
        finally:
            conn.close()


postgis_client = PostGISClient()
