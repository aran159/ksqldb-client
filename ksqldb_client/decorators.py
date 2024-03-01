from functools import wraps
from typing import Callable, Generic, TypeVar

import httpx
from pydantic import BaseModel

from .exceptions import APIError

T = TypeVar("T", bound=BaseModel)

class ParseResponse(Generic[T]):
    def __init__(self, model: type[T] | None):
        self.model = model

    def __call__(self, function: Callable[..., httpx.Response]) -> Callable[..., T]:
        """Decorate a function to parse the httpx.Response of the ksqlDB API and return the corresponding Pydantic model."""

        @wraps(function)
        def wrapper(*args: list, **kwargs: dict) -> T:
            result = function(*args, **kwargs)

            self._raise_api_error_if_not_success(result)

            if self.model is None:
                return result.json()

            return self.model.model_validate(result.json())

        return wrapper

    @staticmethod
    def _raise_api_error_if_not_success(result: httpx.Response) -> None:
        if result.status_code != 200:  # noqa: PLR2004
            if result.headers.get("content-type") is None:
                raise APIError(result.json())

            if "application/json" in result.headers.get("content-type"):
                raise APIError(result.json())

            raise APIError({"error_code": result.status_code, "message": result.text})
