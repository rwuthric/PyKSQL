import json
import httpx
import pandas as pd
from urllib.parse import urljoin
from typing import Dict, List

from .models import Server, Stream, Table, Topic

KSQL_HEADERS = {"Accept": "application/vnd.ksql.v1+json"}


class KSQL:
    """
    Pyhton client to ksqlDB
    """

    def __init__(self, ksqlDB_server):
        self.ksqlDB_server = ksqlDB_server

    def _get_query(self, resource):
        url = urljoin(self.ksqlDB_server, resource)
        return httpx.get(url, headers=KSQL_HEADERS)

    def _statement_query(self, statement):
        url = urljoin(self.ksqlDB_server, "/ksql")
        response = httpx.post(
            url,
            json={"ksql": statement},
            headers=KSQL_HEADERS
        )
        return response

    def info(self):
        """
        Returns info about ksqlDB
        """
        response = self._get_query("info")
        response.raise_for_status()

        def json_to_server(obj, server):
            if "KsqlServerInfo" in obj:
                return obj["KsqlServerInfo"]
            if "version" in obj:
                return Server(
                    id=obj["kafkaClusterId"],
                    service_id=obj["ksqlServiceId"],
                    status=obj["serverStatus"],
                    version=obj["version"],
                    server=server,
                )
            return obj

        return response.json(object_hook=lambda d: json_to_server(d, self.ksqlDB_server))

    def streams(self):
        """
        Returns data streams defined in ksqlDB
        """
        response = self._statement_query("LIST STREAMS;")
        response.raise_for_status()

        def json_to_stream(obj):
            if "streams" in obj:
                return obj["streams"]
            if "name" in obj:
                return Stream(
                    name=obj["name"],
                    topic=obj["topic"],
                    key_format=obj["keyFormat"],
                    value_format=obj["valueFormat"],
                )
            return obj

        return response.json(object_hook=json_to_stream)[0]

    def tables(self):
        """
        Returns tables defined in ksqlDB
        """
        response = self._statement_query("LIST TABLES;")
        response.raise_for_status()

        def json_to_table(obj):
            if "tables" in obj:
                return obj["tables"]
            if "name" in obj:
                return Table(
                    name=obj["name"],
                    topic=obj["topic"],
                    key_format=obj["keyFormat"],
                    value_format=obj["valueFormat"],
                )
            return obj

        return response.json(object_hook=json_to_table)[0]

    def topics(self):
        """
        Returns kafka topics
        """
        response = self._statement_query("LIST TOPICS;")
        response.raise_for_status()

        def json_to_topic(obj):
            if "topics" in obj:
                return obj["topics"]
            if "name" in obj:
                return Topic(
                    name=obj["name"],
                )
            return obj

        return response.json(object_hook=json_to_topic)[0]

    def insert_into_stream(self, stream_name, rows):
        """
        Insert rows into a data stream
        """
        url = urljoin(self.ksqlDB_server, "/inserts-stream")
        data = json.dumps({"target": stream_name}) + "\n"
        for row in rows:
            data += f"{json.dumps(row)}\n"

        client = httpx.Client(http1=False, http2=True)
        with client.stream(method="POST", url=url, content=data,
                           headers={"Content-Type": "application/vnd.ksql.v1+json"}) as r:
            response_data = [json.loads(x) for x in r.iter_lines()]
        return response_data

    def close_query(self, id):
        """
        Closes a query
        """
        url = urljoin(self.ksqlDB_server, "/close-query")
        data = {"queryId": id}
        httpx.post(url, json=data, headers=KSQL_HEADERS)

    async def query(
        self,
        query,
        earliest=False,
        on_init=lambda data: None,
        on_new_row=lambda row: None,
        on_close=lambda: None,
        on_error=lambda code, content: None,
    ):
        """
        Runs a query in ksqlDB
        """
        url = urljoin(self.ksqlDB_server, "/query-stream")
        data = {
            "sql": query,
            "properties": {"auto.offset.reset": "earliest"} if earliest else {"auto.offset.reset": "latest"},
        }

        async with httpx.AsyncClient(http2=True, timeout=3600) as client:
            async with client.stream(method="POST", url=url, json=data) as stream:
                async for chunk in stream.aiter_lines():
                    if chunk:
                        try:
                            results = json.loads(chunk)
                        except ValueError:
                            print("ERROR in decoding ", chunk)

                        if stream.status_code != 200:
                            on_error(stream.status_code, chunk)
                            break

                        if isinstance(results, Dict):
                            on_init(results)
                        elif isinstance(results, List):
                            on_new_row(results)

        on_close()

    async def query_to_dataframe(
        self,
        query,
        earliest=False,
    ):
        """
        Runs a query in ksqlDB
        """
        url = urljoin(self.ksqlDB_server, "/query-stream")
        data = {
            "sql": query,
            "properties": {"auto.offset.reset": "earliest"} if earliest else {},
        }

        rows = []
        columns_name = []

        async with httpx.AsyncClient(http2=True, timeout=3600) as client:
            async with client.stream(method="POST", url=url, json=data) as stream:
                async for chunk in stream.aiter_lines():
                    if chunk:
                        try:
                            results = json.loads(chunk)
                        except ValueError:
                            print("ERROR in decoding ", chunk)

                        if stream.status_code != 200:
                            print('ERROR', stream.status_code, chunk)
                            break

                        if isinstance(results, Dict):
                            columns_name = results['columnNames']
                        elif isinstance(results, List):
                            rows.append(results)

        return pd.DataFrame(rows, columns=columns_name)
