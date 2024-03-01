from pydantic import Field, RootModel

from .base import BaseModelWithConfig


class Stream(BaseModelWithConfig):
    type: str
    name: str
    topic: str
    key_format: str = Field(..., alias="keyFormat")
    value_format: str = Field(..., alias="valueFormat")
    is_windowed: bool = Field(..., alias="isWindowed")


class _Table(BaseModelWithConfig):
    pass


class _Query(BaseModelWithConfig):
    query_string: str = Field(..., alias="queryString")
    sinks: tuple[str]
    id: str


class CommandStatus(BaseModelWithConfig):
    status: str
    message: str
    query_id: str | None = Field(None, alias="queryId")


class _Property(BaseModelWithConfig):
    pass


class _SourceDescription(BaseModelWithConfig):
    pass


class _QueryDescription(BaseModelWithConfig):
    pass


class _Warning(BaseModelWithConfig):
    message: str


class KSqlResponseItem(BaseModelWithConfig):

    """The result object contents depend on the statement that it is returning results for."""

    type: str = Field(..., alias="@type")
    statement_text: str = Field(..., alias="statementText")
    warnings: tuple[_Warning, ...]
    streams: tuple[Stream, ...] | None = None
    queries: tuple[_Query, ...] | None = None

    command_id: str | None = Field(None, alias="commandId")
    command_status: CommandStatus | None = Field(None, alias="commandStatus")
    command_sequence_number: int | None = Field(None, alias="commandSequenceNumber")


class KSqlResponse(RootModel):
    root: tuple[KSqlResponseItem, ...]

    def __iter__(self):  # noqa: D105
        return iter(self.root)

    def __getitem__(self, item: int) -> KSqlResponseItem:  # noqa: D105
        return self.root[item]
