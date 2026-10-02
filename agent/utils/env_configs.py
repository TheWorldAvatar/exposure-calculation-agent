import os

KEYCLOAK_SERVER = os.getenv('KEYCLOAK_SERVER', '')
KEYCLOAK_REALM = os.getenv('KEYCLOAK_REALM', '')


def retrieve_default_settings():
    global NAMESPACE, DATABASE, STACK_NAME, VIS_DATA_JSON

    NAMESPACE = os.getenv("NAMESPACE")
    if NAMESPACE is None:
        NAMESPACE = 'kb'

    DATABASE = os.getenv('DATABASE')
    if DATABASE is None:
        DATABASE = 'postgres'

    STACK_NAME = os.getenv('STACK_NAME')

    VIS_DATA_JSON = os.getenv('VIS_DATA_JSON')

# run when module is imported
retrieve_default_settings()
