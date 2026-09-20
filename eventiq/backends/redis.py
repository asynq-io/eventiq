from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Annotated, Any, TypedDict, TypeVar, cast

from pydantic import AnyUrl, NonNegativeFloat, UrlConstraints
from redis.asyncio import Redis

from eventiq.broker import UrlBroker
from eventiq.settings import UrlBrokerSettings

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer
    from eventiq.types import DecodedMessage

RedisUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["redis", "rediss"])]

DEFAULT_POLL_TIMEOUT = 2


class RMessage(TypedDict):
    type: bytes | str
    pattern: bytes | str | None
    channel: bytes | str
    data: bytes


RedisRawMessage = TypeVar("RedisRawMessage", bound=RMessage)


class RedisSettings(UrlBrokerSettings[RedisUrl]):
    poll_timeout: int = DEFAULT_POLL_TIMEOUT
    poll_interval: NonNegativeFloat = 0.0


class RedisBroker(UrlBroker[RedisRawMessage, None]):
    """
    Broker implementation based on redis PUB/SUB and aioredis package
    :param poll_interval: delay (in seconds) between consecutive get_message calls
        in the sender loop.
    :param kwargs: base class arguments
    """

    Settings = RedisSettings
    protocol = "redis"

    WILDCARD_ONE = "*"
    WILDCARD_MANY = "*"

    def __init__(
        self,
        *,
        poll_timeout: int = DEFAULT_POLL_TIMEOUT,
        poll_interval: float = 0.0,
        redis: Redis | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval
        self._redis: Redis | None = redis

    @staticmethod
    def decode_message(raw_message: RedisRawMessage) -> DecodedMessage:
        return raw_message["data"], {}

    @property
    def is_connected(self) -> bool:
        # A pooled client keeps `.connection` as None even while healthy, and the
        # `redis` property raises before connect(): report on the client instead.
        return self._redis is not None

    async def check_health(self) -> bool:
        if self._redis is None:
            return False
        # `ping` is typed for both the sync and async clients.
        return bool(await cast("Awaitable[bool]", self._redis.ping()))

    @property
    def redis(self) -> Redis:
        if self._redis is None:
            raise self.connection_error
        return self._redis

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        _ = group  # not supported
        poll_interval = consumer.options.get("poll_interval", self.poll_interval)
        async with self.redis.pubsub() as sub:
            await sub.psubscribe(self.format_topic(consumer.topic))
            async with send_stream:
                while True:
                    message = await sub.get_message(
                        ignore_subscribe_messages=True, timeout=self.poll_timeout
                    )
                    if message:
                        if message["type"] == "pong":
                            self.logger.debug("Received pong from pubsub %s", message)
                            continue
                        await send_stream.send(message)
                    else:
                        await sub.ping()
                    await asyncio.sleep(poll_interval)

    async def disconnect(self) -> None:
        if self._redis is None:
            return
        await self._redis.aclose()
        self._redis = None

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, **self.connection_options)
        # `ping` is typed for both the sync and async clients.
        await cast("Awaitable[bool]", self._redis.ping())

    async def publish(
        self,
        topic: str,
        body: bytes,
        **_: Any,
    ) -> None:
        await self.redis.publish(topic, body)

    async def ack(self, raw_message: RedisRawMessage) -> None:
        pass

    async def nack(
        self,
        raw_message: RedisRawMessage,
        delay: int | None = None,
    ) -> None:
        if delay is not None:
            self.logger.warning("delay is not supported expected None got %d", delay)
        await self.redis.publish(raw_message["channel"], raw_message["data"])
