from pydantic import BaseModel, ConfigDict


class BaseModelWithConfig(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
    )
