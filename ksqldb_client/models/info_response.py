from pydantic import Field

from .base import BaseModelWithConfig


class KSQLServerInfo(BaseModelWithConfig):
    version: str
    kafka_cluster_id: str = Field(..., alias="kafkaClusterId")
    ksql_service_id: str = Field(..., alias="ksqlServiceId")
    server_status: str = Field(..., alias="serverStatus")

class InfoResponse(BaseModelWithConfig):
    ksql_server_info: KSQLServerInfo = Field(..., alias="KsqlServerInfo")
