from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from eventiq.broker import Broker
from eventiq.settings import BrokerSettings

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer
    from eventiq.types import DecodedMessage


@dataclass
class StubMessage:
    data: bytes
    queue: asyncio.Queue
    event: asyncio.Event
    headers: dict[str, str] = field(default_factory=dict)


class StubSettings(BrokerSettings):
    wait_on_publish: bool = True


class StubBroker(Broker[StubMessage, dict[str, asyncio.Event]]):
    """This is in-memory implementation of a broker class, mainly designed for testing."""

    Settings = StubSettings

    protocol = ""

    WILDCARD_ONE = r"\w+"
    WILDCARD_MANY = "*"

    def __init__(
        self,
        *,
        wait_on_publish: bool = True,
        queue_max_size: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_max_size = queue_max_size
        self.topics: dict[str, asyncio.Queue[StubMessage]] = defaultdict(
            self.queue_factory,
        )
        self._connected = False
        self.wait_on_publish = wait_on_publish

    def queue_factory(self) -> asyncio.Queue[StubMessage]:
        return asyncio.Queue(maxsize=self.queue_max_size)

    def get_info(self) -> dict[str, str]:
        return {"host": "localhost"}

    @staticmethod
    def decode_message(raw_message: StubMessage) -> DecodedMessage:
        return raw_message.data, raw_message.headers

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[StubMessage],
    ) -> None:
        queue = self.topics[self.format_topic(consumer.topic)]
        async with send_stream:
            while self._connected:
                message = await queue.get()
                await send_stream.send(message)

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        self.topics.clear()

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        **kwargs: Any,
    ) -> dict[str, asyncio.Event]:
        response = {}
        for target_topic, queue in self.topics.items():
            if re.fullmatch(topic, target_topic):
                event = asyncio.Event()
                msg = StubMessage(data=body, queue=queue, event=event, headers=headers)
                await queue.put(msg)
                response[topic] = event
                if self.wait_on_publish:
                    await event.wait()
        return response

    async def ack(self, raw_message: StubMessage) -> None:
        raw_message.queue.task_done()
        raw_message.event.set()

    async def nack(self, raw_message: StubMessage, delay: int | None = None) -> None:
        if delay:
            await asyncio.sleep(delay)
        await raw_message.queue.put(raw_message)

    @property
    def is_connected(self) -> bool:
        return self._connected
