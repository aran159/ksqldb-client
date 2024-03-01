from pydantic import Field

from ksqldb_client.models.base import BaseModelWithConfig


class StatusResponse(BaseModelWithConfig):
    status: str
    message: str
    query_id: str = Field(..., alias="queryId")
