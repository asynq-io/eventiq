from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from .consumer import Consumer
from .exceptions import Fail, Retry, Skip
from .logging import LoggerMixin
from .types import CloudEventType

if TYPE_CHECKING:
    from .service import Service


class _Sentinel(Exception):
    pass


class Middleware(Generic[CloudEventType], LoggerMixin):
    """Base class for middlewares"""

    throws: type[Exception] | tuple[type[Exception], ...] = _Sentinel
    requires: type[CloudEventType] | None = None

    async def before_broker_connect(self, *, service: Service) -> None:
        """Called before broker connects"""

    async def after_broker_connect(self, *, service: Service) -> None:
        """Called after broker connects"""

    async def before_broker_disconnect(self, *, service: Service) -> None:
        """Called before broker disconnects"""

    async def after_broker_disconnect(self, *, service: Service) -> None:
        """Called after broker disconnects"""

    async def before_consumer_start(
        self, *, service: Service, consumer: Consumer
    ) -> None:
        """Called before consumer is started"""

    async def after_consumer_start(
        self, *, service: Service, consumer: Consumer
    ) -> None:
        """Called after consumer is started"""

    async def before_ack(
        self,
        *,
        service: Service,
        consumer: Consumer,
        raw_message: Any,
    ) -> None:
        """Called before message is acknowledged"""

    async def after_ack(
        self,
        *,
        service: Service,
        consumer: Consumer,
        raw_message: Any,
    ) -> None:
        """Called after message is acknowledged"""

    async def before_nack(
        self, *, service: Service, consumer: Consumer, raw_message: Any
    ) -> None:
        """Called before message is rejected"""

    async def after_nack(
        self, *, service: Service, consumer: Consumer, raw_message: Any
    ) -> None:
        """Called after message is rejected"""

    async def before_publish(
        self, *, service: Service, message: CloudEventType, **kwargs: Any
    ) -> None:
        """Called before message is published"""

    async def after_publish(
        self, *, service: Service, message: CloudEventType, **kwargs: Any
    ) -> None:
        """Called after message is published"""

    async def after_skip_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEventType,
        exc: Skip,
    ) -> None:
        """Called after message is skipped by the middleware"""

    async def after_fail_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEventType,
        exc: Fail,
    ) -> None:
        """Called after message is failed by the middleware"""

    async def after_retry_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEventType,
        exc: Retry,
    ) -> None:
        """Called after message is retried by the middleware"""

    async def before_process_message(
        self, *, service: Service, consumer: Consumer, message: CloudEventType
    ) -> None:
        """Called before message is processed"""

    async def after_process_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEventType,
        result: Any = None,
        exc: Exception | None = None,
    ) -> None:
        """Called after message is processed (but not acknowledged/rejected yet)"""
