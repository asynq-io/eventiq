from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, TypedDict, TypeVar

from pydantic import AnyUrl, UrlConstraints
from redis.asyncio import Redis

from eventiq.broker import UrlBroker, UrlBrokerSettings
from eventiq.results import ResultBackend

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer
    from eventiq.types import DecodedMessage

RedisUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["redis", "rediss"])]


class RMessage(TypedDict):
    type: bytes | str
    pattern: bytes | str | None
    channel: bytes | str
    data: bytes


RedisRawMessage = TypeVar("RedisRawMessage", bound=RMessage)


class RedisBroker(
    UrlBroker[RedisRawMessage, None], ResultBackend[RedisRawMessage, None]
):
    """
    Broker implementation based on redis PUB/SUB and aioredis package
    :param kwargs: base class arguments
    """

    Settings = UrlBrokerSettings[RedisUrl]
    protocol = "redis"

    WILDCARD_ONE = "*"
    WILDCARD_MANY = "*"

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._redis: Redis | None = None

    @staticmethod
    def decode_message(raw_message: RedisRawMessage) -> DecodedMessage:
        return raw_message["data"], None

    @property
    def is_connected(self) -> bool:
        if self.redis.connection:
            return self.redis.connection.is_connected
        return False

    @property
    def redis(self) -> Redis:
        if self._redis is None:
            raise self.connection_error
        return self._redis

    async def sender(
        self, group: str, consumer: Consumer, send_stream: MemoryObjectSendStream
    ) -> None:
        async with self.redis.pubsub() as sub:
            await sub.psubscribe(consumer.topic)
            async with send_stream:
                while True:
                    message = await sub.get_message(ignore_subscribe_messages=True)
                    if message:
                        await send_stream.send(message)

    async def disconnect(self) -> None:
        await self.redis.close()

    async def connect(self) -> None:
        self._redis = Redis.from_url(self.url, **self.connection_options)

    async def publish(
        self,
        topic: str,
        body: bytes,
        **kwargs: Any,
    ) -> None:
        await self.redis.publish(topic, body)

    async def store_result(self, key: str, result: bytes) -> None:
        await self.redis.set(key, result)

    async def get_result(self, key: str) -> Any | None:
        return await self.redis.get(key)

    async def ack(self, raw_message: RedisRawMessage) -> None:
        pass

    async def nack(
        self, raw_message: RedisRawMessage, delay: int | None = None
    ) -> None:
        await self.redis.publish(raw_message["channel"], raw_message["data"])
