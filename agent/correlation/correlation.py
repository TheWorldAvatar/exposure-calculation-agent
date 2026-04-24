from agent.utils.postgis_client import postgis_client
from twa import agentlogging
from psycopg2.extras import RealDictCursor, execute_values
from scipy.stats import spearmanr
from itertools import combinations
from tqdm import tqdm
import sys

logger = agentlogging.get_logger('dev')


def calculate_correlation():
    with postgis_client.connect() as conn:
        _create_table(conn)

        set_ids = _get_set_ids(conn)
        pairs = list(combinations(set_ids, 2))
        for pair in tqdm(pairs, mininterval=60, ncols=80, file=sys.stdout):
            set_id1 = pair[0]
            set_id2 = pair[1]

            subject_to_value_dict1 = _get_subject_to_value(set_id1, conn)
            subject_to_value_dict2 = _get_subject_to_value(set_id2, conn)

            subjects1 = set(subject_to_value_dict1.keys())
            subjects2 = set(subject_to_value_dict2.keys())

            if subjects1 != subjects2:
                raise Exception('Result sets must share the same subjects')

            # remove null values
            for subject in subjects1:
                if subject_to_value_dict1[subject] is None or subject_to_value_dict2[subject] is None:
                    del subject_to_value_dict1[subject]
                    del subject_to_value_dict2[subject]

            value_list1 = []
            value_list2 = []

            for subject in subject_to_value_dict1:
                value_list1.append(subject_to_value_dict1[subject])
                value_list2.append(subject_to_value_dict2[subject])

            correlation_value, _ = spearmanr(value_list1, value_list2)

            _update_table(set_id1=set_id1, set_id2=set_id2,
                          correlation_value=correlation_value, conn=conn)


def _get_set_ids(conn):
    query = """
    SELECT id from exposure_result_set
    """
    set_ids = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        if cur.description:
            query_result = cur.fetchall()
            for row in query_result:
                set_ids.append(row['id'])
    return set_ids


def _get_subject_to_value(set_id: int, conn):
    # hardcoded! be sure to match ontop.obda
    query = """
        SELECT subject, value
        FROM exposure_result
        WHERE set_id = %(SET_ID_PLACEHOLDER)s
    """

    subject_to_value_dict = {}
    replacements = {"SET_ID_PLACEHOLDER": set_id}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, replacements)
        if cur.description:
            query_result = cur.fetchall()
            for row in query_result:
                subject_to_value_dict[row['subject']] = row['value']

    return subject_to_value_dict


def _create_table(conn):
    query = """
        CREATE TABLE IF NOT EXISTS exposure_correlation (
            id BIGSERIAL PRIMARY KEY,
            set_id1 INT REFERENCES exposure_result_set(id) ON DELETE CASCADE,
            set_id2 INT REFERENCES exposure_result_set(id) ON DELETE CASCADE,
            value NUMERIC,
            CHECK (set_id1 <= set_id2),
            UNIQUE (set_id1, set_id2)
        );
    """

    with conn.cursor() as cur:
        cur.execute(query)


def _update_table(set_id1, set_id2, correlation_value, conn):
    x = 1
    query = """
        INSERT INTO exposure_correlation (set_id1, set_id2, value)
        VALUES %s
        ON CONFLICT (set_id1, set_id2)
        DO UPDATE SET value = EXCLUDED.value;
    """

    set_ids = [set_id1, set_id2]
    set_ids.sort()

    data = [(set_ids[0], set_ids[1], correlation_value.item())]

    with conn.cursor() as cur:
        execute_values(cur, query, data)
