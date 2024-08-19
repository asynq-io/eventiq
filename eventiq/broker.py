from __future__ import annotations

import inspect
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeVar
from urllib.parse import urlparse

import anyio

from eventiq.exceptions import BrokerConnectionError, BrokerError

from .imports import import_from_string
from .logging import LoggerMixin
from .settings import BrokerSettings, UrlBrokerSettings
from .types import ID, DecodedMessage, DefaultAction, Message, Timeout
from .utils import format_topic, to_float

if TYPE_CHECKING:
    from datetime import datetime

    from anyio.streams.memory import MemoryObjectSendStream
    from pydantic import AnyUrl

    from eventiq import Consumer


R = TypeVar("R", bound=Any)


class BulkMessage(NamedTuple):
    topic: str
    body: bytes
    headers: dict[str, str]
    kwargs: dict[str, Any]


class Broker(Generic[Message, R], LoggerMixin, ABC):
    """Base broker class
    :param description: Broker (Server) Description
    :param encoder: Encoder (Serializer) class
    :param decoder: Decoder (Deserializer) class.
    """

    protocol: str
    protocol_version: str = ""

    Settings: type[BrokerSettings] = BrokerSettings

    WILDCARD_ONE: str
    WILDCARD_MANY: str

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not inspect.isabstract(cls):
            protocol = getattr(cls, "protocol", None)
            if protocol is None:
                msg = f"Broker subclass {cls} must define a protocol"
                raise ValueError(msg)

    def __init__(
        self,
        *,
        name: str = "default",
        description: str | None = None,
        default_on_exc: DefaultAction = "nack",
        default_consumer_timeout: Timeout = 300,
        tags: list[str] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        validate_error_delay: int = 3600 * 12,
    ) -> None:
        self.name = name
        self.description = description or type(self).__name__
        self.default_on_exc = default_on_exc
        self.tags = tags
        self.async_api_extra = asyncapi_extra or {}
        self.default_consumer_timeout = to_float(default_consumer_timeout)
        self.validate_error_delay = validate_error_delay

    def __repr__(self) -> str:
        return type(self).__name__

    def should_nack(self, raw_message: Message) -> bool:
        return False

    def format_topic(self, topic: str) -> str:
        return format_topic(topic, self.WILDCARD_ONE, self.WILDCARD_MANY)

    @abstractmethod
    def get_info(self) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def decode_message(raw_message: Message) -> DecodedMessage:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return broker connection status."""
        raise NotImplementedError

    @abstractmethod
    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: ID,
        message_type: str,
        message_content_type: str,
        message_time: datetime,
        message_source: str,
        **kwargs: Any,
    ) -> R:
        raise NotImplementedError

    async def bulk_publish(
        self,
        messages: list[BulkMessage],
        topic: str | None = None,
    ) -> None:
        """
        Default implementation of bulk publish uses task groups to publish messages concurrently.
        """
        async with anyio.create_task_group() as tg:
            for msg_topic, body, headers, kwargs in messages:
                message_topic = topic or msg_topic
                tg.start_soon(self._publish_task, message_topic, body, headers, kwargs)

    async def _publish_task(
        self, topic: str, body: bytes, headers: dict[str, str], kwargs: dict[str, Any]
    ) -> None:
        await self.publish(topic, body, headers=headers, **kwargs)

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def ack(self, raw_message: Message) -> None:
        raise NotImplementedError

    @abstractmethod
    async def nack(self, raw_message: Message, delay: int | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[Message],
    ) -> None:
        raise NotImplementedError

    def get_num_delivered(self, raw_message: Message) -> int | None:
        return getattr(raw_message, "num_delivered", None)

    @classmethod
    def from_settings(
        cls, settings: BrokerSettings | None = None, **kwargs: Any
    ) -> Broker:
        if settings is None:
            settings = cls.Settings()
        kw = settings.model_dump(exclude_none=True)
        kw.update(kwargs)
        return cls(**kw)

    @classmethod
    def from_env(
        cls,
        **kwargs: Any,
    ) -> Broker:
        if cls == Broker:
            try:
                type_name = os.environ["BROKER_CLASS"]
            except KeyError:
                msg = "BROKER_CLASS evironment variable not set"
                raise BrokerError(msg) from None
            broker_cls = import_from_string(type_name)
        else:
            broker_cls = cls
        return broker_cls.from_settings(**kwargs)


class UrlBroker(Broker[Message, R], ABC):
    settings: type[UrlBrokerSettings]
    error_msg = "Broker not connected"

    def __init__(
        self,
        *,
        url: AnyUrl,
        connection_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.url = str(url)
        self.connection_options = connection_options or {}

    @property
    def connection_error(self) -> BrokerConnectionError:
        return BrokerConnectionError(self.error_msg)

    def get_info(self) -> dict[str, Any]:
        parsed = urlparse(self.url)
        return {
            "host": parsed.hostname,
            "pathname": parsed.path,
        }
