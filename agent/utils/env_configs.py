import os


def retrieve_default_settings():
    global NAMESPACE, DATABASE, STACK_NAME

    NAMESPACE = os.getenv("NAMESPACE")
    if NAMESPACE is None:
        NAMESPACE = 'kb'

    DATABASE = os.getenv('DATABASE')
    if DATABASE is None:
        DATABASE = 'postgres'

    STACK_NAME = os.getenv('STACK_NAME')


# run when module is imported
retrieve_default_settings()
