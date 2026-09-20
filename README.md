<p align="center">
<img src="https://raw.githubusercontent.com/asynq-io/eventiq/main/assets/logo.svg" style="width: 250px">

</p>
<p align="center">
<em>Asyncio native pub/sub framework for Python</em>
</p>

![Tests](https://github.com/asynq-io/eventiq/workflows/Tests/badge.svg)
![Build](https://github.com/asynq-io/eventiq/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/asynq-io/eventiq)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://docs.pydantic.dev/latest/contributing/#badges)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
![Python](https://img.shields.io/pypi/pyversions/eventiq)
![Format](https://img.shields.io/pypi/format/eventiq)
![PyPi](https://img.shields.io/pypi/v/eventiq)

## Installation
```shell
pip install eventiq
```
or
```shell
poetry add eventiq
```

### Installing optional dependencies

```shell
pip install 'eventiq[broker]'
```

### Available brokers

- `nats`
- `rabbitmq`
- `kafka`
- `redis`

## Features

- Modern, `asyncio` based python 3.8+ syntax
- Fully type annotated
- Minimal external dependencies (`anyio`, `pydantic`, `typer`)
- Automatic message parsing based on type annotations using `pydantic`
- Code hot-reload
- Highly scalable: each service can process hundreds of tasks concurrently,
    all messages are load balanced between all instances by default
- Resilient - at least once delivery for all messages by default (except for Redis*)
- Customizable & pluggable message encoder/decoder (`json` as default)
- Multiple broker support
    - Memory (for testing)
    - Nats
    - Kafka
    - Rabbitmq
    - Redis
- Result Backend implementation for Nats & Redis
- Lifespan protocol support
- Lightweight (and completely optional) dependency injection system based on type annotations
- Easy and lightweight (~3k lines of code including types definitions and brokers implementations)
- [Cloud Events](https://cloudevents.io/) standard as base message structure (no more python specific `*args` and `**kwargs` in messages)
- [AsyncAPI](https://www.asyncapi.com/en) documentation generation from code
- Twelve factor app approach - stdout logging, configuration through environment variables
- Easily extensible via Middlewares
- Multiple extensions and integrations including:

### Prometheus metrics exporter

Source: https://github.com/asynq-io/eventiq-exporter

Installation:
```shell
uv add eventiq-exporter
# or
pip install eventiq-exporter
```

### OpenTelemetry instrumentation
Source: https://github.com/asynq-io/opentelemetry-instrumentation-eventiq

Installation:
```shell
uv add opentelemetry-instrumentation-eventiq
# or
pip install opentelemetry-instrumentation-eventiq
```

### FastAPI integration
 Source: https://github.com/asynq-io/eventiq-fastapi
Installation
```shell
pip install eventiq-fastapi
# or
pip install eventiq-fastapi
```

## Basic Usage

```Python
import asyncio
from eventiq import Service, Middleware, CloudEvent, GenericConsumer
from eventiq.backends.nats import JetStreamBroker

class SendMessageMiddleware(Middleware):
    async def after_broker_connect(self):
        print(f"After service start, running with {service.broker}")
        await asyncio.sleep(10)
        for i in range(100):
            message = CloudEvent(topic="test.topic", data={"counter": i})
            await service.publish(message)
        print("Published messages(s)")

broker = JetStreamBroker(url="nats://localhost:4222")

service = Service(
    name="example-service",
    broker=broker,
)
service.add_middleware(SendMessageMiddleware)

@service.subscribe(topic="test.topic")
async def example_run(message: CloudEvent):
    print(f"Received Message {message.id} with data: {message.data}")

@service.subscribe(topic="test.topic2")
class MyConsumer(GenericConsumer[CloudEvent]):
    async def process(self, message: CloudEvent):
        print(f"Received Message {message.id} with data: {message.data}")
        await self.publish(CloudEvent(topic="test.topic", data={"response": "ok"})

```

Run with

```shell
eventiq run app:service --log-level=info
```

## Watching for changes

```shell
eventiq run app:service --log-level=info --reload=.
```

## Testing

`StubBroker` class is provided as in memory replacement for running unit tests

```python
import os


def get_broker(**kwargs):
    if os.getenv('ENV') == 'TEST':
        from eventiq.backends.stub import StubBroker
        return StubBroker()
    else:
        from eventiq.backends.rabbitmq import RabbitmqBroker
        return RabbitmqBroker(**kwargs)

broker = get_broker()

```

Furthermore, subscribers are just regular python coroutines, so it's possible to test them simply by invocation

```python

# main.py
@service.subscribe(topic="test.topic")
async def my_subscriber(message: CloudEvent):
    return 42

# tests.py
from main import my_subscriber

async def test_my_subscriber():
    result = await my_subscriber(None)
    assert result == 42

```

## Running tests

Install the dev dependencies (includes `pytest`, `pytest-xdist`, `testcontainers`, the backend clients and the linters):

```shell
uv sync --group dev
```

Run the unit test suite:

```shell
uv run pytest
```

Run the tests in parallel across multiple workers with [pytest-xdist](https://pytest-xdist.readthedocs.io):

```shell
uv run pytest -n auto
```

The end-to-end suite (`tests/e2e`) runs the whole publish/subscribe + middleware stack against real brokers started as testcontainers Docker containers. It is collected and skipped by a normal `pytest` run, so no container is started unless you opt in:

```shell
# e2e only (requires a running Docker daemon)
uv run pytest --e2e ./tests/e2e

# unit suite + e2e
uv run pytest --e2e

# a single backend
uv run pytest --e2e --e2e-backends=redis
```

See [`tests/e2e/README.md`](tests/e2e/README.md) for details.

## CLI

Getting help:
```shell
eventiq --help
```

Installing shell autocompletion:
```shell
eventiq --install-completion [bash|zsh|fish|powershell|pwsh]
```

Basic commands

- `run` - run service
- `docs` - generate AsyncAPI docs
- `send` - send message to broker
