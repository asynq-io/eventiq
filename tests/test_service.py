from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eventiq import CloudEvent, Consumer, Service
from eventiq.backends.stub import StubBroker
from eventiq.exceptions import ConsumerCancelledError


def test_service(service: Service):
    assert isinstance(service, Service)
    assert isinstance(service.broker, StubBroker)
    assert service.name == "test_service"


async def test_consumer_called(
    running_service: Service, ce: CloudEvent, mock_consumer: Any
):
    await running_service.publish(ce)
    mock_consumer.assert_called_once_with(ce)


async def test_bulk_publish(
    running_service: Service, ce: CloudEvent, mock_consumer: MagicMock
):
    messages = [ce] * 10
    for message in messages:
        assert "Content-Type" not in message.headers
        assert message.source is None
    await running_service.bulk_publish(messages)
    assert mock_consumer.call_count == 10
    for message in messages:
        assert message.headers["Content-Type"] == "application/json"
        assert message.source == running_service.name


async def test_broker_notified_on_receive(
    running_service: Service, ce: CloudEvent, mock_consumer: MagicMock
) -> None:
    on_receive = AsyncMock()
    running_service.broker.on_receive = on_receive  # type: ignore[method-assign]

    await running_service.publish(ce)

    on_receive.assert_awaited_once()
    mock_consumer.assert_called_once_with(ce)


async def test_nack_dispatches_before_and_after_once(
    service: Service, test_consumer: Consumer
) -> None:
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]
    service.dispatch_before = AsyncMock()  # type: ignore[method-assign]
    service.dispatch_after = AsyncMock()  # type: ignore[method-assign]
    raw_message = MagicMock()

    await service.nack(test_consumer, raw_message, delay=5)

    service.dispatch_before.assert_awaited_once_with(
        "nack", consumer=test_consumer, raw_message=raw_message
    )
    service.dispatch_after.assert_awaited_once_with(
        "nack", consumer=test_consumer, raw_message=raw_message
    )
    service.broker.nack.assert_awaited_once_with(raw_message, delay=5)


async def test_cancelled_message_nacked_without_retry(
    service: Service, test_consumer: Consumer, ce: CloudEvent
) -> None:
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]
    ce.set_context(service, MagicMock(), {})

    await service._handle_message_finalization(
        test_consumer, ce, None, ConsumerCancelledError("Consumer cancelled")
    )

    service.broker.nack.assert_awaited_once_with(ce.raw, delay=None)


@pytest.mark.parametrize("buffer_size", [-1, 1.5, 3])
def test_invalid_buffer_size_rejected(
    service: Service, handler: Any, buffer_size: float
) -> None:
    with pytest.raises(ValueError, match="Buffer size"):
        service.subscribe(
            handler,
            topic="test_topic",
            name="buffered",
            concurrency=2,
            buffer_size=buffer_size,
        )
