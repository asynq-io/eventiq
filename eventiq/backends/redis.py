from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, TypedDict, TypeVar

from anyio.streams.memory import MemoryObjectSendStream
from pydantic import AnyUrl, UrlConstraints
from redis.asyncio import Redis

from eventiq.broker import UrlBroker, UrlBrokerSettings
from eventiq.exceptions import BrokerError
from eventiq.results import Error, Ok, Result, ResultBackend
from eventiq.types import Encoder

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer

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
    def get_message_data(raw_message: RedisRawMessage) -> bytes:
        return raw_message["data"]

    @property
    def is_connected(self) -> bool:
        if self.redis.connection:
            return self.redis.connection.is_connected
        return False

    @property
    def redis(self) -> Redis:
        if self._redis is None:
            raise BrokerError("Not connected")
        return self._redis

    async def sender(
        self, group: str, consumer: Consumer, send_stream: MemoryObjectSendStream
    ):
        async with self.redis.pubsub() as sub:
            await sub.psubscribe(consumer.topic)
            async with send_stream:
                while True:
                    # TODO: timeout?
                    message = await sub.get_message(ignore_subscribe_messages=True)
                    if message:
                        await send_stream.send(message)

    async def disconnect(self) -> None:
        await self.redis.close()

    async def connect(self) -> None:
        self._redis = Redis.from_url(self.url, **self.connection_options)

    async def publish(
        self, message: CloudEvent, encoder: Encoder | None = None, **kwargs
    ) -> None:
        data = self._encode_message(message, encoder)
        await self.redis.publish(message.topic, data)

    async def store_result(self, key: str, result: Ok | Error) -> None:
        data = self.encoder.encode(result)
        await self.redis.set(key, data)

    async def get_result(self, key: str) -> Result | None:
        result = await self.redis.get(key)
        if result:
            return self.decoder.decode(result, Result)

    async def ack(self, raw_message: RedisRawMessage):
        pass

    async def nack(self, raw_message: RedisRawMessage, delay: int | None = None):
        await self.redis.publish(raw_message["channel"], raw_message["data"])
