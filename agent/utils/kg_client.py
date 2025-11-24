from agent.objects.time_series import TimeSeries
from agent.utils.stack_gateway import stack_clients_view
from agent.utils.stack_configs import BLAZEGRAPH_URL, STACK_OUTGOING, ONTOP_URL
from agent.utils.env_configs import NAMESPACE
import agent.utils.constants as constants
from twa import agentlogging
import time
import requests
from urllib.parse import urlsplit
from py4j.java_gateway import JavaObject
import json
from datetime import datetime

logger = agentlogging.get_logger('dev')


class KgClientException(Exception):
    """Raise in case of exception when using the KgClient."""


class KgClient():
    def __init__(self):
        self.remote_store_client = RetryRemoteStoreClient(stack_clients_view.RemoteStoreClient(
            STACK_OUTGOING, BLAZEGRAPH_URL))
        self.ontop_client = stack_clients_view.RemoteStoreClient(ONTOP_URL)

        # check if namespace exists, if not initialise
        r = requests.head(BLAZEGRAPH_URL)

        if r.status_code != 200:
            # get the front part of the url
            parsed_url = urlsplit(BLAZEGRAPH_URL)
            url = f"{parsed_url.scheme}://{parsed_url.netloc}" + \
                '/blazegraph/namespace'

            props = (
                f"com.bigdata.rdf.sail.namespace={NAMESPACE}\n"
                f"com.bigdata.rdf.store.AbstractTripleStore.quads=false\n"
                f"com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms\n"
            )

            r = requests.post(url, data=props, headers={
                              "Content-Type": "text/plain"})

            if r.status_code not in (200, 201):
                raise RuntimeError(
                    f"Failed to create namespace '{NAMESPACE}': {r.status_code} {r.text}")

    def get_time_series(self, iri: str):
        query = f"""
        SELECT ?time_series
        WHERE {{
            <{iri}> <{constants.HAS_TIME_SERIES}> ?time_series.
        }}
        """
        query_result = self.remote_store_client.executeQuery(query)

        if query_result.isEmpty():
            return None
        else:
            return query_result.getJSONObject(0).getString('time_series')

    def get_java_time_class(self, point_iri: str):
        query = f"""
        SELECT ?time_class
        WHERE {{
            <{point_iri}> <{constants.HAS_TIME_SERIES}>/<{constants.HAS_TIME_CLASS}> ?time_class.
        }}
        """
        query_results = self.remote_store_client.executeQuery(query)
        return query_results.getJSONObject(0).getString('time_class')

    def get_time_series_data(self, measures, lowerbound, upperbound):
        values_clause = " ".join(f"<{s}>" for s in measures)
        conditions = []

        if lowerbound is not None:
            lowerbound = self.convert_input_time_for_timeseries(
                measures[0], lowerbound)
            if isinstance(lowerbound, JavaObject):
                condition = f""" ?timestamp >= "{lowerbound[0].toString()}"^^xsd:dateTime"""
            else:
                condition = f"""?time_number >= {lowerbound}"""
            conditions.append(condition)

        if upperbound is not None:
            upperbound = self.convert_input_time_for_timeseries(
                measures[0], upperbound)
            if isinstance(upperbound, JavaObject):
                condition = f"""?timestamp <= "{upperbound[0].toString()}"^^xsd:dateTime"""
            else:
                condition = f"""?time_number <= {upperbound}"""
            conditions.append(condition)

        filter_clause = ''
        if conditions:
            filter_clause = f"FILTER ({' && '.join(conditions)})"

        query = f"""
        PREFIX time: <http://www.w3.org/2006/time#>

        SELECT ?timestamp ?time_number ?val ?measure
        WHERE {{
            VALUES ?measure {{{values_clause}}}.
            ?obs <{constants.OBSERVATION_OF}> ?measure;
                <{constants.HAS_RESULT}>/<{constants.TS_HAS_VALUE}> ?val.
            OPTIONAL {{?obs time:hasTime/time:inXSDDateTime ?timestamp.}}
            OPTIONAL {{?obs time:hasTime/time:inTimePosition/time:numericPosition ?time_number.}}
            {filter_clause}
        }}
        ORDER BY ?timestamp ?time_number
        """

        query_results = kg_client.remote_store_client.executeQuery(query)
        query_results_parsed = json.loads(query_results.toString())

        time_series = TimeSeries()
        time_number_dict = {}
        timestamp_dict = {}
        value_dict = {}

        for entry in query_results_parsed:
            measure = entry['measure']

            if measure in value_dict.keys():
                value_dict[measure].append(entry['val'])
            else:
                value_dict[measure] = [entry['val']]

            if 'timestamp' in entry:
                if measure in timestamp_dict.keys():
                    timestamp_dict[measure].append(entry['timestamp'])
                else:
                    timestamp_dict[measure] = [entry['timestamp']]

            if 'time_number' in entry:
                if measure in time_number_dict.keys():
                    time_number_dict[measure].append(entry['time_number'])
                else:
                    time_number_dict[measure] = [entry['time_number']]

        timestamp_list_list = []
        time_number_list_list = []
        for measure in measures:
            if measure in timestamp_dict.keys():
                timestamp_list = [datetime.fromisoformat(
                    s) for s in timestamp_dict[measure]]
                timestamp_list_list.append(timestamp_list)
                time_series.add_timestamp_java(measure, self.convert_input_time_for_timeseries(
                    timestamp_dict[measure], measure))
                time_series.add_timestamp(measure, timestamp_list)

            if measure in time_number_dict.keys():
                time_number_list = [float(s)
                                    for s in time_number_dict[measure]]
                time_number_list_list.append(time_number_list)
                time_series.add_time_number(measure, time_number_list)

            time_series.add_value(measure, value_dict[measure])

        if timestamp_list_list:
            # check all time lists are equal
            if not all(inner == timestamp_list_list[0] for inner in timestamp_list_list):
                raise Exception('Unexpected timestamp list size')

        if time_number_list_list:
            # check all time lists are the same
            if not all(inner == time_number_list_list[0] for inner in time_number_list_list):
                raise Exception('Unexpected time number list size')

        return time_series

    def convert_input_time_for_timeseries(self, time, iri: str):
        # assumes time is in seconds or milliseconds, if an exception is thrown,
        # queries the time class from KG (e.g. java.time.Instant) and use the
        # parse method to parse time into the correct Java object
        try:
            # assume epoch seconds
            if isinstance(time, list):
                return [float(t) for t in time]
            else:
                return float(time)
        except (ValueError, TypeError):
            class_name = self.get_java_time_class(iri)
            return stack_clients_view.TimeSeriesClientFactory.timestampFactory(class_name, time)


class RetryRemoteStoreClient:
    def __init__(self, java_client, max_retries=3, delay=10):
        self.java_client = java_client
        self.max_retries = max_retries
        self.delay = delay

    def executeQuery(self, query):
        for attempt in range(1, self.max_retries + 1):
            try:
                return self.java_client.executeQuery(query)
            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(self.delay)

    # forward other methods if needed
    def __getattr__(self, name):
        return getattr(self.java_client, name)


kg_client = KgClient()  # global object shared between modules
