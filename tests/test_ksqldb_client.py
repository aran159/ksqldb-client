import pytest

from ksqldb_client import KSqlDBClient
from ksqldb_client.exceptions import APIError
from ksqldb_client.models.info_response import InfoResponse, KSQLServerInfo
from ksqldb_client.models.ksql_response import CommandStatus, KSqlResponse, KSqlResponseItem
from ksqldb_client.models.query_response import FinalMessageItem, Header, HeaderItem, QueryResponse

TESTING_KSQLDB_SERVER_URL = "http://localhost:8088"


def test_query() -> None:
    client = KSqlDBClient(TESTING_KSQLDB_SERVER_URL)

    with pytest.warns(match="This endpoint was proposed to be deprecated as part of KLIP-15 in favor of the new HTTP/2 /query-stream."):
        response = client.query(ksql="select * from ksql_processing_log;")

    expected_response = QueryResponse(
        root=(
            HeaderItem(
                header=Header(
                    query_id=response[0].header.query_id,  # type: ignore[union-attr]
                    schema_="`LOGGER` STRING, `LEVEL` STRING, `TIME` BIGINT, `MESSAGE` STRUCT<`TYPE` INTEGER, `DESERIALIZATIONERROR` STRUCT<`TARGET` STRING, `ERRORMESSAGE` STRING, `RECORDB64` STRING, `CAUSE` ARRAY<STRING>, `topic` STRING>, `RECORDPROCESSINGERROR` STRUCT<`ERRORMESSAGE` STRING, `RECORD` STRING, `CAUSE` ARRAY<STRING>>, `PRODUCTIONERROR` STRUCT<`ERRORMESSAGE` STRING>, `SERIALIZATIONERROR` STRUCT<`TARGET` STRING, `ERRORMESSAGE` STRING, `RECORD` STRING, `CAUSE` ARRAY<STRING>, `topic` STRING>, `KAFKASTREAMSTHREADERROR` STRUCT<`ERRORMESSAGE` STRING, `THREADNAME` STRING, `CAUSE` ARRAY<STRING>>>",  # noqa: E501
                ),
            ),
            FinalMessageItem(final_message="Query Completed"),
        ),
    )

    assert response == expected_response


def test_ksql_commands() -> None:
    client = KSqlDBClient(TESTING_KSQLDB_SERVER_URL)

    client.ksql(ksql="drop stream if exists my_stream;")

    response = client.ksql(ksql="create stream my_stream as select * from ksql_processing_log emit changes;")

    expected_response = KSqlResponse(
        root=(
            KSqlResponseItem(
                type="currentStatus",
                statement_text="CREATE STREAM MY_STREAM WITH (KAFKA_TOPIC='MY_STREAM', PARTITIONS=1, REPLICAS=1) AS SELECT *\nFROM KSQL_PROCESSING_LOG KSQL_PROCESSING_LOG\nEMIT CHANGES;",  # noqa: E501
                warnings=(),
                streams=None,
                queries=None,
                command_id="stream/`MY_STREAM`/create",
                command_status=CommandStatus(
                    status="SUCCESS",
                    message=response[0].command_status.message,  # type: ignore[union-attr]
                    query_id=response[0].command_status.query_id,  # type: ignore[union-attr]
                ),
                command_sequence_number=response[0].command_sequence_number,
            ),
        ),
    )

    assert response == expected_response

    client.ksql(ksql="drop stream my_stream;")


def test_status() -> None:
    client = KSqlDBClient(TESTING_KSQLDB_SERVER_URL)
    client.status(command_id="stream/`MY_STREAM`/create")


def test_info() -> None:
    client = KSqlDBClient(TESTING_KSQLDB_SERVER_URL)

    response = client.info()

    expected_response = InfoResponse(
        ksql_server_info=KSQLServerInfo(
            version="0.28.2",
            kafka_cluster_id=response.ksql_server_info.kafka_cluster_id,  # type: ignore[union-attr]
            ksql_service_id="default_",
            server_status="RUNNING",
        ),
    )

    assert response == expected_response


def test_close_query_raises_api_error_with_a_non_existent_query_id() -> None:
    client = KSqlDBClient(TESTING_KSQLDB_SERVER_URL)

    with pytest.raises(APIError):
        client.close_query(query_id="non-existent-query-id")
