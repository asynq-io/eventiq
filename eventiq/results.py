from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Union

from pydantic import BaseModel, RootModel

from .broker import Broker, Message, R
from .middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Consumer, Service

    from .models import CloudEvent


class Ok(BaseModel):
    data: Any


class Error(BaseModel):
    type: str
    detail: str


class Result(RootModel[Union[Ok, Error]]):
    pass


class ResultBackend(Broker[Message, R], ABC):
    def __init__(
        self,
        store_results: bool = False,
        store_exceptions: bool = False,
        **extra: Any,
    ) -> None:
        super().__init__(**extra)
        self.store_results = store_results
        self.store_exceptions = store_exceptions

    @abstractmethod
    async def store_result(self, key: str, result: Ok | Error) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_result(self, key: str) -> Result | None:
        raise NotImplementedError


class ResultBackendMiddleware(Middleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        self.result_backend: ResultBackend | None = None

        if isinstance(service.broker, ResultBackend) and service.broker.store_results:
            self.result_backend = service.broker

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEvent,
        result: Any | None = None,
        exc: Exception | None = None,
    ) -> None:
        if not consumer.options.get("store_results") or not self.result_backend:
            return
        if exc is None:
            await self.result_backend.store_result(
                f"{self.service.name}:{message.id}",
                Ok(data=result),
            )
        elif self.result_backend:
            await self.result_backend.store_result(
                f"{self.service.name}:{message.id}",
                Error(type=type(exc).__name__, detail=str(exc)),
            )
