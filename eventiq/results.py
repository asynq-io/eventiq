from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic import RootModel

from .broker import Broker, Message, R
from .middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Consumer, Service

    from .models import CloudEvent


class AnyModel(RootModel[Any]):
    pass


class ResultBackend(Broker[Message, R], ABC):
    async def init_storage(self) -> None:
        pass

    @abstractmethod
    async def store_result(self, key: str, result: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_result(self, key: str) -> bytes | None:
        raise NotImplementedError


class ResultBackendMiddleware(Middleware):
    def __init__(self, service: Service) -> None:
        super().__init__(service)
        if not isinstance(service.broker, ResultBackend):
            err = f"Broker type must be ResultBackend. Got {type(service.broker).__name__}"
            raise TypeError(err)

        self.result_backend: ResultBackend = service.broker

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEvent,
        result: Any | None = None,
        exc: Exception | None = None,
    ) -> None:
        if not all([result, exc is None, consumer.store_results]):
            return
        encoder = consumer.encoder or self.service.encoder
        data = encoder.encode(AnyModel(result))
        await self.result_backend.store_result(
            f"{self.service.name}:{message.id}",
            data,
        )
