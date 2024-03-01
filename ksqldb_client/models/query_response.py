from typing import Any

from pydantic import Field, RootModel

from ksqldb_client.models.base import BaseModelWithConfig


class Header(BaseModelWithConfig):
    query_id: str = Field(..., alias="queryId")
    schema_: str = Field(..., alias="schema")

class Row(BaseModelWithConfig):
    columns: tuple[Any, ...]
    tombstone: bool | None = None


class HeaderItem(BaseModelWithConfig):
    header: Header


class RowItem(BaseModelWithConfig):
    row: Row


class FinalMessageItem(BaseModelWithConfig):
    final_message: str = Field(..., alias="finalMessage")


class ErrorMessageItem(BaseModelWithConfig):
    error_message: str = Field(..., alias="errorMessage")

class QueryResponse(RootModel):
    root: tuple[HeaderItem | RowItem | FinalMessageItem | ErrorMessageItem, ...]

    def __iter__(self):  # noqa: D105
        return iter(self.root)

    def __getitem__(self, item: int) -> HeaderItem | RowItem | FinalMessageItem | ErrorMessageItem:  # noqa: D105
        return self.root[item]
