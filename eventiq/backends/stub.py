from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from anyio.streams.memory import MemoryObjectSendStream

from eventiq.broker import Broker
from eventiq.settings import BrokerSettings
from eventiq.types import Encoder

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer


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
            self.queue_factory
        )
        self._connected = False
        self.wait_on_publish = wait_on_publish

    def queue_factory(self):
        return asyncio.Queue(maxsize=self.queue_max_size)

    def get_info(self) -> dict[str, str]:
        return {"host": "localhost"}

    @staticmethod
    def get_message_data(raw_message: StubMessage) -> bytes:
        return raw_message.data

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[StubMessage],
    ):
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
        self, message: CloudEvent, encoder: Encoder | None = None, **kwargs
    ) -> dict[str, asyncio.Event]:
        data = self._encode_message(message, encoder)
        headers = kwargs.get("headers", {})
        response = {}
        message_topic = self.format_topic(message.topic)
        for topic, queue in self.topics.items():
            if re.fullmatch(message_topic, topic):
                event = asyncio.Event()
                msg = StubMessage(data=data, queue=queue, event=event, headers=headers)
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
