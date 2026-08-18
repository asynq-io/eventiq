# Eventiq

<p align="center">
<img src="https://raw.githubusercontent.com/asynq-io/eventiq/main/assets/logo.svg" style="width: 250px">

</p>
<p align="center">
<em>Asyncio native pub/sub framework for Python</em>
</p>

[![Tests](https://github.com/asynq-io/eventiq/workflows/Tests/badge.svg)](https://github.com/asynq-io/eventiq/actions)
[![License](https://img.shields.io/github/license/asynq-io/eventiq)](https://github.com/asynq-io/eventiq/blob/main/LICENSE)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://docs.pydantic.dev/)
![Python](https://img.shields.io/pypi/pyversions/eventiq)
![PyPi](https://img.shields.io/pypi/v/eventiq)

**Eventiq** is a lightweight, fully type-annotated pub/sub framework built on top of `asyncio` and `pydantic`. It uses the [CloudEvents](https://cloudevents.io/) specification as the base message structure and supports [AsyncAPI](https://www.asyncapi.com/) documentation generation.

---

## Features

- Modern `asyncio`-based Python 3.8+ syntax
- Fully type-annotated
- Minimal external dependencies (`anyio`, `pydantic`, `typer`)
- Automatic message parsing from type annotations via pydantic
- Highly scalable — each service processes messages concurrently; all instances load-balance by default
- Resilient — at-least-once delivery for all messages by default
- Pluggable message encoder/decoder (JSON by default, MessagePack optional)
- Multiple broker backends: NATS JetStream, Kafka, RabbitMQ, Redis, and an in-memory stub for testing
- [CloudEvents](https://cloudevents.io/) standard message structure
- [AsyncAPI 3.0](https://www.asyncapi.com/) documentation generation from code
- Lifespan protocol support (similar to ASGI)
- Lightweight dependency injection based on type annotations
- Twelve-factor app friendly: stdout logging, environment-variable configuration
- Easily extensible via [Middlewares](usage.md#middlewares)

---

## Installation

```shell
pip install eventiq
```

Install an optional broker backend:

```shell
pip install 'eventiq[nats]'
pip install 'eventiq[rabbitmq]'
pip install 'eventiq[kafka]'
pip install 'eventiq[redis]'
```

---

## Quick Example

```python
from eventiq import Service, CloudEvent, GenericConsumer
from eventiq.backends.nats import JetStreamBroker

broker = JetStreamBroker(url="nats://localhost:4222")

service = Service(
    name="example-service",
    broker=broker,
)


@service.subscribe(topic="test.topic")
async def handle_message(message: CloudEvent) -> None:
    print(f"Received {message.id}: {message.data}")


@service.subscribe(topic="test.topic2")
class MyConsumer(GenericConsumer[CloudEvent]):
    async def process(self, message: CloudEvent) -> None:
        print(f"Received {message.id}: {message.data}")
        await self.publish(CloudEvent(topic="test.topic", data={"response": "ok"}))
```

Run with:

```shell
eventiq run app:service --log-level=info
```

---

## Navigation

- [Usage](usage.md) — step-by-step guides for common patterns
- [Reference](reference/service.md) — full API reference
