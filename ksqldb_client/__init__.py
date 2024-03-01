import warnings
from typing import Any, ClassVar

import httpx

from ksqldb_client.models.status_response import StatusResponse

from .decorators import ParseResponse
from .models.info_response import InfoResponse
from .models.ksql_response import KSqlResponse
from .models.query_response import QueryResponse


class KSqlDBClient:
    _headers: ClassVar = {
        "Accept": "application/vnd.ksql.v1+json",
        "Content-Type": "application/json",
    }

    def __init__(
        self,
        url: str,
        api_key: str | None = None,
        secret: str | None = None,
        timeout_in_seconds: int = 15,
    ) -> None:
        self.url = url
        self.api_key = api_key
        self.secret = secret
        self.timeout_in_seconds = timeout_in_seconds

        self._client = httpx.Client(http2=True, timeout=self.timeout_in_seconds)

    @ParseResponse(KSqlResponse)
    def ksql(
        self,
        ksql: str,
        *,
        stream_properties: dict | None = None,
        session_variables: dict | None = None,
        command_sequence_number: int | None = None,
    ) -> httpx.Response:
        """Run a sequence of SQL statements.

        Args:
        ----
            ksql (str): A semicolon-delimited sequence of SQL statements to run.
            stream_properties (dict, optional): Property overrides to run the statements with.
                Refer to the Configuration Parameter Reference for details on properties that can be set.
                Each property-name should be a string, and the corresponding value should also be a string.
            session_variables (dict, optional): A map of string variable names and values of any type as initial
                variable substitution values. See ksqlDB Variable Substitution for more information on variable substitution.
            command_sequence_number (int, optional): If specified, the statements will not be run until all existing
                commands up to and including the specified sequence number have completed. If unspecified, the statements
                are run immediately. When a command is processed, the result object contains its sequence number.

        Returns:
        -------
            tuple: Array of result objects

        """
        body: dict[Any, Any] = {
            "ksql": ksql,
            "streamsProperties": stream_properties or {},
        }

        if session_variables is not None:
            body.update({"sessionVariables": session_variables})

        if command_sequence_number is not None:
            body.update({"commandSequenceNumber": command_sequence_number})

        return self._client.post(
            f"{self.url}/ksql",
            json=body,
        )

    @ParseResponse(QueryResponse)
    def query(
        self,
        ksql: str,
        *,
        stream_properties: dict | None = None,
    ) -> httpx.Response:
        """Run a SELECT statement and stream back the results.

        Args:
        ----
            ksql (str): The SELECT statement to run.
            stream_properties (dict, optional): Property overrides to run the statements with.
                Refer to the Config Reference for details on properties that can be set.
                Each property-name should be a string, and the corresponding value should also be a string.

        Returns:
        -------
            dict: The result of the executed SELECT statement. Each response chunk is a JSON object with the following format:
                - header (object): Information about the result.
                    - header.queryId: (string): the unique id of the query.
                    - header.schema: (string): the list of columns being returned.
                - row (object): A single row being returned. This will be null if an error is being returned.
                    - row.columns (array): The values of the columns requested.
                    - row.tombstone (boolean): Whether the row is a deletion of a previous row.
                - finalMessage (string): If this field is non-null, it contains a final message from the server.
                - errorMessage (string): If this field is non-null, an error has been encountered while running the statement.

        """
        warnings.warn(  # noqa: B028
            (
                """
This endpoint was proposed to be deprecated as part of KLIP-15 in favor of the new HTTP/2 /query-stream.
"""
            ),
            DeprecationWarning,
        )

        return self._client.post(
            f"{self.url}/query",
            json={
                "ksql": ksql,
                "streamsProperties": stream_properties or {},
            },
            headers=self._headers,
        )

    @ParseResponse(QueryResponse)
    def query_stream(
        self,
        sql: str,
        *,
        properties: dict | None = None,
        session_variables: dict | None = None,
    ) -> httpx.Response:
        """Run a SELECT statement and stream back the results.

        Args:
        ----
            sql (str): The SELECT statement to run.
            properties (dict, optional): Property overrides to run the statements with.
                Refer to the Config Reference for details on properties that can be set.
                Each property-name should be a string, and the corresponding value should also be a string.
            session_variables (dict, optional): A map of string variable names and values of any type to substitute into the sql statement.

        Returns:
        -------
            dict: The result of the executed SELECT statement. Each response chunk is a JSON object with the following format:
                - header (object): Information about the result.
                    - header.queryId: (string): the unique id of the query.
                    - header.schema: (string): the list of columns being returned.
                - row (object): A single row being returned. This will be null if an error is being returned.
                    - row.columns (array): The values of the columns requested.
                    - row.tombstone (boolean): Whether the row is a deletion of a previous row.
                - finalMessage (string): If this field is non-null, it contains a final message from the server.
                - errorMessage (string): If this field is non-null, an error has been encountered while running the statement.

        """
        body: dict[Any, Any] = {
            "sql": sql,
            "properties": properties or {},
        }

        if session_variables is not None:
            body.update({"sessionVariables": session_variables})

        return self._client.post(
            f"{self.url}/query-stream",
            json=body,
        )

    @ParseResponse(None)
    def close_query(
        self,
        *,
        query_id: str,
    ) -> httpx.Response:
        """Terminate a running query."""
        return self._client.post(
            f"{self.url}/close-query",
            json={"queryId": query_id},
        )

    @ParseResponse(StatusResponse)
    def status(self, command_id: str) -> httpx.Response:
        """Get the current command status for a CREATE, DROP, or TERMINATE statement.

        Args:
        ----
            command_id (str): The command ID of the statement. This ID is returned by the /ksql endpoint.

        Returns:
        -------
            dict: The current status of the command. The response is a JSON object with the following format:
                - status (string): One of QUEUED, PARSING, EXECUTING, TERMINATED, SUCCESS, or ERROR.
                - message (string): Detailed message regarding the status of the execution statement.

        Note:
        ----
            CREATE, DROP, and TERMINATE statements return an object that indicates the current state of statement execution.
            A statement can be in one of the following states:
                - QUEUED, PARSING, EXECUTING: The statement was accepted by the server and is being processed.
                - SUCCESS: The statement was successfully processed.
                - ERROR: There was an error processing the statement. The statement was not executed.
                - TERMINATED: The query started by the statement was terminated. Only returned for CREATE STREAM|TABLE AS SELECT.

        """
        return self._client.get(f"{self.url}/status{f'/{command_id}' if command_id is not None else ''}")

    @ParseResponse(InfoResponse)
    def info(self) -> httpx.Response:
        """Get information about the status of a ksqlDB Server.

        This can be useful for health checks and troubleshooting.

        """
        return self._client.get(f"{self.url}/info")

    @ParseResponse(None)
    def cluster_status(self) -> httpx.Response:
        """Get information about the status of all ksqlDB servers in a ksqlDB cluster.

        This can be useful for troubleshooting. Enable this endpoint by setting ksql.heartbeat.enable to true.
        Optionally, you can also set ksql.lag.reporting.enable to true to have your ksqlDB servers report state store lag,
        which will then also be returned with the response from the /clusterStatus endpoint.

        Returns:
        -------
            dict: The status information of the ksqlDB cluster. The response object contains a clusterStatus field with the following
            information for each ksqlDB server (represented as host:port):
                - hostAlive (boolean): whether the server is alive, as determined by heartbeats received by the queried server
                - lastStatusUpdateMs (long): epoch timestamp, in milliseconds, for when the last status update was received for this server,
                by the queried server
                - activeStandbyPerQuery (object): for each query ID, a collection of active and standby partitions and state stores
                on this server
                - hostStoreLags (object): state store lag information. Empty unless ksql.lag.reporting.enable is set to true.
                - hostStoreLags.stateStoreLags (object): partition-level lag breakdown for each state store.
                - hostStoreLags.updateTimeMs (long): epoch timestamp, in milliseconds, for when the last lag update was received
                for this server, by the queried server

        Note:
        ----
            ksqlDB servers in a cluster discover each other through persistent queries. If you have no persistent queries running,
            then the /clusterStatus endpoint contains info for the particular server that was queried, rather than all servers
            in the cluster.

        """
        raise NotImplementedError
