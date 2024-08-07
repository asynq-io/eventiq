from __future__ import annotations

import inspect
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from urllib.parse import urlparse

from .decoder import DEFAULT_DECODER
from .encoder import DEFAULT_ENCODER
from .imports import import_from_string
from .logging import LoggerMixin
from .settings import BrokerSettings, UrlBrokerSettings
from .types import DecodedMessage, Decoder, DefaultAction, Encoder, Message, Timeout
from .utils import format_topic, to_float

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from pydantic import AnyUrl

    from eventiq import Consumer

    from .models import CloudEvent


R = TypeVar("R", bound=Any)


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
        encoder: Encoder = DEFAULT_ENCODER,
        decoder: Decoder = DEFAULT_DECODER,
        default_on_exc: DefaultAction = "nack",
        default_consumer_timeout: Timeout = 300,
        tags: list[str] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        validate_error_delay: int = 3600 * 12,
    ) -> None:
        self.name = name
        self.encoder = encoder
        self.decoder = decoder
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

    def _encode_message(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
    ) -> bytes:
        encoder = encoder or self.encoder
        message.content_type = encoder.CONTENT_TYPE
        return encoder.encode(message)

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
        message: CloudEvent,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> R:
        raise NotImplementedError

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
    def from_settings(cls, settings: BrokerSettings, **kwargs: Any) -> Broker:
        return cls(**settings.model_dump(), **kwargs)

    @classmethod
    def _from_env(cls, **kwargs: Any) -> Broker:
        return cls.from_settings(cls.Settings(**kwargs))

    @classmethod
    def from_env(
        cls,
        **kwargs: Any,
    ) -> Broker:
        if cls == Broker:
            type_name = os.getenv("BROKER_CLASS", "eventiq.backends.stub:StubBroker")
            cls = import_from_string(type_name)
        return cls._from_env(**kwargs)


class UrlBroker(Broker[Message, R], ABC):
    settings: type[UrlBrokerSettings]

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

    def get_info(self) -> dict[str, Any]:
        parsed = urlparse(self.url)
        return {
            "host": parsed.hostname,
            "pathname": parsed.path,
        }
