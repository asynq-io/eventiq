from __future__ import annotations

import asyncio
import time
from abc import ABC
from typing import TYPE_CHECKING, Annotated, Any

import anyio
from nats.aio.client import Client
from nats.aio.msg import Msg as NatsMsg
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext, api
from nats.js.api import ConsumerConfig
from nats.js.errors import APIError, KeyNotFoundError, NotFoundError
from pydantic import AnyUrl, Field, NonNegativeFloat, UrlConstraints

from eventiq.broker import R, UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.results import ResultBackend
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import to_float

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from eventiq.types import ID, DecodedMessage

NatsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["nats"])]


class NatsSettings(UrlBrokerSettings[NatsUrl]):
    auto_flush: bool = True


class JetStreamSettings(NatsSettings):
    jetstream_options: dict[str, Any] = Field({})
    kv_options: dict[str, Any] = Field({})
    poll_interval: NonNegativeFloat = 0.0
    heartbeat: NonNegativeFloat = 0.1


if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from nats.js.kv import KeyValue

    from eventiq import Consumer


class AbstractNatsBroker(UrlBroker[NatsMsg, R], ABC):
    """:param auto_flush: auto flush messages on publish
    :param auto_flush: auto flush on publish
    :param kwargs: options for base class
    """

    protocol = "nats"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = ">"

    def __init__(
        self,
        *,
        auto_flush: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.client = Client()
        self._auto_flush = auto_flush
        self.connection_options.setdefault("max_reconnect_attempts", 10)
        for k in ("error", "closed", "reconnected", "disconnected"):
            self.connection_options.setdefault(f"{k}_cb", self._default_cb(k))

    def _default_cb(
        self, message: str
    ) -> Callable[[Exception | None], Awaitable[None]]:
        async def wrapped(error: Exception | None = None) -> None:
            self.logger.warning(message)
            if error:
                self.logger.error(error)

        return wrapped

    @staticmethod
    def decode_message(raw_message: NatsMsg) -> DecodedMessage:
        return raw_message.data, raw_message.headers

    @staticmethod
    def get_message_metadata(raw_message: NatsMsg) -> dict[str, str]:
        try:
            return {
                "messaging.nats.sequence.consumer": str(
                    raw_message.metadata.sequence.consumer,
                ),
                "messaging.nats.sequence.stream": str(
                    raw_message.metadata.sequence.stream,
                ),
                "messaging.nats.num_delivered": str(raw_message.metadata.num_delivered),
            }
        except Exception:
            return {}

    async def connect(self) -> None:
        if not self.client.is_connected:
            await self.client.connect(self.url, **self.connection_options)

    async def disconnect(self) -> None:
        if self.client.is_connected:
            await self.client.drain()
            await self.client.close()

    async def flush(self) -> None:
        await self.client.flush()

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected

    async def ack(self, raw_message: NatsMsg) -> None:
        await raw_message.ack()

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        await raw_message.nak(delay=delay)


class NatsBroker(AbstractNatsBroker[None]):
    Settings = NatsSettings

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        queue = "" if consumer.dynamic else f"{group}:{consumer.name}"
        subscription = await self.client.subscribe(
            subject=self.format_topic(consumer.topic),
            queue=queue,
        )
        try:
            async with send_stream:
                async for message in subscription.messages:
                    await send_stream.send(message)
        finally:
            with anyio.move_on_after(1, shield=True):
                if consumer.dynamic:
                    await subscription.unsubscribe()
            self.logger.info("Sender finished for %s", consumer.name)

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        reply: str = "",
        flush: bool = False,
        **kwargs: Any,
    ) -> None:
        await self.client.publish(topic, body, headers=headers, reply=reply)
        if self._auto_flush or flush:
            await self.flush()


class JetStreamBroker(
    AbstractNatsBroker[api.PubAck],
    ResultBackend[NatsMsg, api.PubAck],
):
    """NatsBroker with JetStream enabled
    :param jetstream_options: additional options passed to nc.jetstream(...)
    :param kv_options: options for nats KV initialization.
    :param poll_interval: delay (in seconds) between consecutive pull-subscription
        fetch calls in the sender loop.
    :param heartbeat: heartbeat interval (in seconds) for pull-subscription fetch
        calls in the sender loop.
    :param kwargs: all other options for base classes NatsBroker, Broker.
    """

    Settings = JetStreamSettings
    kv_error = "KeyVal not initialized"

    def __init__(
        self,
        *,
        jetstream_options: dict[str, Any] | None = None,
        kv_options: dict[str, Any] | None = None,
        poll_interval: float = 0.0,
        heartbeat: float = 0.1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.jetstream_options = jetstream_options or {}
        self.js = JetStreamContext(self.client, **self.jetstream_options)
        self.kv_options = kv_options or {}
        self.poll_interval = poll_interval
        self.heartbeat = heartbeat
        self._kv: KeyValue | None = None
        self._held_by_reply: dict[str, dict[str, NatsMsg]] = {}

    async def init_storage(self) -> None:
        self._kv = await self.js.create_key_value(**self.kv_options)

    @property
    def kv(self) -> KeyValue:
        if self._kv is None:
            raise BrokerError(self.kv_error)
        return self._kv

    async def get_result(self, key: str) -> bytes | None:
        try:
            data = await self.kv.get(key)
            return data.value  # noqa: TRY300
        except KeyNotFoundError:
            self.logger.warning("Key %s not found", key)

    async def store_result(self, key: str, result: bytes) -> None:
        await self.kv.put(key, result)

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: ID,
        timeout: float | None = None,
        stream: str | None = None,
        **kwargs: Any,
    ) -> api.PubAck:
        if "Nats-Msg-Id" not in headers:
            headers["Nats-Msg-Id"] = str(message_id)
        response = await self.js.publish(
            topic, payload=body, timeout=timeout, stream=stream, headers=headers
        )
        if self._auto_flush:
            await self.flush()
        return response

    async def ack(self, raw_message: NatsMsg) -> None:
        try:
            if not raw_message.is_acked:
                await raw_message.ack()
        finally:
            self._release(raw_message)

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        try:
            if not raw_message.is_acked:
                await raw_message.nak(delay=delay)
        finally:
            self._release(raw_message)

    async def on_receive(self, raw_message: NatsMsg) -> None:
        if raw_message.is_acked:
            return
        try:
            await raw_message.in_progress()
        except Exception:
            self.logger.exception("In-progress on receive failed")

    def _hold(self, held: dict[str, NatsMsg], raw_message: NatsMsg) -> None:
        held[raw_message.reply] = raw_message
        self._held_by_reply[raw_message.reply] = held

    def _release(self, raw_message: NatsMsg) -> None:
        held = self._held_by_reply.pop(raw_message.reply, None)
        if held is not None:
            held.pop(raw_message.reply, None)

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        timeout = to_float(consumer.timeout) or self.default_consumer_timeout
        config_kwargs: dict[str, Any] = {
            "name": f"{group}:{consumer.name}",
            "ack_wait": timeout + 30,
            "max_ack_pending": 10_000,
        }
        explicit_fields = {"ack_wait", "max_ack_pending"}
        for key in ConsumerConfig.__dataclass_fields__:
            if key in consumer.options:
                config_kwargs[key] = consumer.options[key]
                explicit_fields.add(key)
        config = ConsumerConfig(**config_kwargs)
        in_progress_interval = self._get_in_progress_interval(consumer, timeout)
        fetch_timeout = consumer.options.get("fetch_timeout", 10)
        heartbeat = consumer.options.get("heartbeat", self.heartbeat)
        poll_interval = consumer.options.get("poll_interval", self.poll_interval)
        subject = self.format_topic(consumer.topic)
        durable = None if consumer.dynamic else f"{group}:{consumer.name}"
        stream = None
        if durable:
            stream = await self.js.find_stream_name_by_subject(subject)
            await self._sync_consumer_config(
                stream=stream,
                durable=durable,
                config=config,
                fields=explicit_fields - {"name", "durable_name"},
            )
        subscription = await self.js.pull_subscribe(
            subject=subject,
            durable=durable,
            stream=stream,
            config=config,
        )
        held: dict[str, NatsMsg] = {}
        try:
            async with anyio.create_task_group() as tg:
                if in_progress_interval is not None:
                    tg.start_soon(
                        self._send_in_progress,
                        consumer.name,
                        held,
                        in_progress_interval,
                    )
                async with send_stream:
                    while True:
                        try:
                            stats = send_stream.statistics()
                            batch = stats.tasks_waiting_receive + (
                                stats.max_buffer_size - stats.current_buffer_used
                            )
                            if batch <= 0:
                                await asyncio.sleep(0.1)
                                continue
                            self.logger.debug("Fetching %d messages", batch)
                            messages = await subscription.fetch(
                                batch=int(batch),
                                timeout=fetch_timeout,
                                heartbeat=heartbeat,
                            )
                            for message in messages:
                                self._hold(held, message)
                                await send_stream.send(message)
                        except NatsTimeoutError:
                            self.logger.debug("Suppressing nats timeout error")
                        await asyncio.sleep(poll_interval)
        finally:
            buffered_count = send_stream.statistics().current_buffer_used
            buffered = list(held.values())[len(held) - buffered_count :]
            with anyio.move_on_after(5, shield=True):
                await self._nack_buffered(consumer.name, buffered)
                if consumer.dynamic:
                    await subscription.unsubscribe()
            self.logger.info("Stopped sender for consumer: %s", consumer.name)

    @staticmethod
    def _get_in_progress_interval(consumer: Consumer, timeout: float) -> float | None:
        ack_wait = to_float(consumer.options.get("ack_wait"))
        interval = to_float(consumer.options.get("in_progress_interval"))
        if ack_wait is None:
            if interval is not None:
                msg = (
                    f"Consumer {consumer.name}: in_progress_interval requires ack_wait"
                )
                raise ValueError(msg)
            return None
        if ack_wait >= timeout:
            msg = (
                f"Consumer {consumer.name}: ack_wait ({ack_wait}) must be lower "
                f"than timeout ({timeout})"
            )
            raise ValueError(msg)
        if interval is None:
            interval = ack_wait / 2
        if not 0 < interval < ack_wait:
            msg = (
                f"Consumer {consumer.name}: in_progress_interval must be greater "
                f"than 0 and lower than ack_wait ({ack_wait})"
            )
            raise ValueError(msg)
        return interval

    async def _sync_consumer_config(
        self, *, stream: str, durable: str, config: ConsumerConfig, fields: set[str]
    ) -> None:
        try:
            info = await self.js.consumer_info(stream, durable)
        except NotFoundError:
            return
        changes = self._get_config_changes(
            current=info.config, desired=config, fields=fields
        )
        if not changes:
            return
        try:
            await self.js.add_consumer(stream, info.config.evolve(**changes))
        except APIError:
            self.logger.warning(
                "Could not update consumer %s with %s, keeping its current config",
                durable,
                changes,
                exc_info=True,
            )
            return
        self.logger.info("Updated consumer %s with %s", durable, changes)

    @staticmethod
    def _get_config_changes(
        *, current: ConsumerConfig, desired: ConsumerConfig, fields: set[str]
    ) -> dict[str, Any]:
        return {
            field: value
            for field in fields
            if (value := getattr(desired, field)) != getattr(current, field)
        }

    async def _send_in_progress(
        self, consumer_name: str, held: dict[str, NatsMsg], interval: float
    ) -> None:
        last_beat = time.monotonic()
        while True:
            await anyio.sleep(interval)
            now = time.monotonic()
            delay = now - last_beat - interval
            if delay > interval:
                self.logger.warning(
                    "In-progress for consumer %s sent %.1f seconds late, "
                    "the event loop was blocked",
                    consumer_name,
                    delay,
                )
            last_beat = now
            for message in list(held.values()):
                if message.is_acked:
                    continue
                try:
                    await message.in_progress()
                except Exception:
                    self.logger.exception(
                        "In-progress failed for consumer %s", consumer_name
                    )

    async def _nack_buffered(self, consumer_name: str, buffered: list[NatsMsg]) -> None:
        for message in buffered:
            try:
                await self.nack(message)
            except Exception:  # noqa: PERF203
                self.logger.exception(
                    "Nack on shutdown failed for consumer %s", consumer_name
                )

    def should_nack(self, raw_message: NatsMsg) -> bool:
        return raw_message.metadata.num_delivered <= 3

    def get_num_delivered(self, raw_message: NatsMsg) -> int | None:
        return raw_message.metadata.num_delivered
