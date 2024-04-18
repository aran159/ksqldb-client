import re
from typing import Any

import pandas as pd

from ksqldb_client.models.query_response import HeaderItem, QueryResponse, RowItem


def extract_rows_from_query_response(query_response: QueryResponse) -> tuple[RowItem, ...]:
    return tuple(row for row in query_response if isinstance(row, RowItem))


def extract_header_from_query_response(query_response: QueryResponse) -> HeaderItem:
    items = tuple(row for row in query_response if isinstance(row, HeaderItem))

    if len(items) == 0:
        raise ValueError
    if len(items) > 1:
        raise ValueError

    return items[0]


def extract_columns_from_ksql_schema(schema: str) -> dict[str, str]:
    parts = re.split(r",(?![^<]*>)", schema)

    result: dict[str, str] = {}
    for part in parts:
        part_stripped = part.strip()
        if re.search(r"(STRUCT|ARRAY)<.*>", part_stripped):
            if match := re.search(r"`([^`]+)`\s+(STRUCT|ARRAY)<", part_stripped):
                result.update({match.group(1): match.group(2)})
        elif match := re.search(r"`([^`]+)`\s+([A-Z]+)", part_stripped):
            result.update({match.group(1): match.group(2)})

    return result


def extract_redshift_jsonpaths_from_ksql_schema(schema: str) -> dict[str, list[str]]:
    columns = extract_columns_from_ksql_schema(schema)

    return {
        "jsonpaths": [f"$['{column_name}']" for column_name in columns],
    }


KSQL_TO_PANDAS_TYPES = {
    "STRING": pd.StringDtype(),
    "BOOLEAN": pd.BooleanDtype(),
    "INTEGER": pd.Int64Dtype(),
    "BIGINT": pd.Int64Dtype(),
    "DOUBLE": pd.Float64Dtype(),
    "DECIMAL": pd.Float64Dtype(),
    "DATE": pd.DatetimeTZDtype(tz="utc"),
    "TIMESTAMP": pd.DatetimeTZDtype(tz="utc"),
    "ARRAY": "object",
    "MAP": "object",
    "STRUCT": "object",
}


def query_response_to_pandas(query_response: QueryResponse) -> pd.DataFrame:
    """Convert a ksql query response to a pandas DataFrame.

    Note: Column names are converted to lowercase.
    """
    header = extract_header_from_query_response(query_response)
    rows = extract_rows_from_query_response(query_response)



    data = tuple(row.row.columns for row in rows)

    column_name_to_ksqldb_type_dict = extract_columns_from_ksql_schema(header.header.schema_)
    column_names = tuple(column_name.lower() for column_name in column_name_to_ksqldb_type_dict)
    pandas_column_types = tuple(KSQL_TO_PANDAS_TYPES[ksql_column_type] for ksql_column_type in column_name_to_ksqldb_type_dict.values())

    return pd.DataFrame(
        dictionary_keys_to_lowercase_recursively(data),
        columns=column_names,
    ).astype(
        dict(
            zip(
                column_names,
                pandas_column_types,
                strict=True,
            ),
        ),
    )


def dictionary_keys_to_lowercase_recursively(dict_: dict[str, Any] | tuple[Any, ...]) -> dict[str, Any] | tuple[Any, ...]:
    """Convert all dictionary keys to lowercase, including nested dictionaries and lists of dictionaries."""
    if isinstance(dict_, dict):
        return {k.lower(): dictionary_keys_to_lowercase_recursively(v) for k, v in dict_.items()}
    if isinstance(dict_, tuple):
        return tuple(dictionary_keys_to_lowercase_recursively(v) for v in dict_)

    return dict_


KSQLDB_TO_REDSHIFT_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE PRECISION",
    "VARCHAR": "VARCHAR",
    "STRING": "VARCHAR",
    "ARRAY": "SUPER",
    "MAP": "SUPER",
    "STRUCT": "SUPER",
    "DECIMAL": "DECIMAL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
}


def redshift_create_table_statement_from_ksql_schema(
    table_name: str,
    schema: str,
) -> str:
    columns = extract_columns_from_ksql_schema(schema)

    return f"""create table {table_name}(
    {
        ", ".join([
            f'{column_name} {KSQLDB_TO_REDSHIFT_TYPES[column_type]}'
            for column_name, column_type in columns.items()
        ])
    }
)"""
