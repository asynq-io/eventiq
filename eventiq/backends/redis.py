from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, TypedDict, TypeVar

from pydantic import AnyUrl, UrlConstraints
from redis.asyncio import Redis

from eventiq.broker import UrlBroker, UrlBrokerSettings
from eventiq.exceptions import BrokerError
from eventiq.results import AnyModel, ResultBackend

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import CloudEvent, Consumer
    from eventiq.types import Encoder

RedisUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["redis", "rediss"])]


class RMessage(TypedDict):
    type: bytes | str
    pattern: bytes | str | None
    channel: bytes | str
    data: bytes


RedisRawMessage = TypeVar("RedisRawMessage", bound=RMessage)


class RedisBroker(
    UrlBroker[RedisRawMessage, None],
    ResultBackend[RedisRawMessage, None],
):
    """Broker implementation based on redis PUB/SUB and aioredis package
    :param kwargs: base class arguments.
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
            err = "Not connected"
            raise BrokerError(err)
        return self._redis

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        async with self.redis.pubsub() as sub:
            await sub.psubscribe(consumer.topic)
            async with send_stream:
                while True:
                    message = await sub.get_message(ignore_subscribe_messages=True)
                    if message:
                        await send_stream.send(message)

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.close()

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(self.url, **self.connection_options)

    async def publish(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> None:
        data = self._encode_message(message, encoder)
        await self.redis.publish(message.topic, data)

    async def store_result(self, key: str, result: AnyModel) -> None:
        data = self.encoder.encode(result)
        await self.redis.set(key, data)

    async def get_result(self, key: str) -> Any:
        result = await self.redis.get(key)
        if result:
            return self.decoder.decode(result)
        return None

    async def ack(self, raw_message: RedisRawMessage) -> None:
        pass

    async def nack(
        self, raw_message: RedisRawMessage, delay: int | None = None
    ) -> None:
        await self.redis.publish(raw_message["channel"], raw_message["data"])
