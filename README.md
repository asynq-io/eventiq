![Tests](https://github.com/asynq-io/eventiq/workflows/Tests/badge.svg)
![Build](https://github.com/asynq-io/eventiq/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/asynq-io/eventiq)
![Python](https://img.shields.io/pypi/pyversions/eventiq)
![Format](https://img.shields.io/pypi/format/eventiq)
![PyPi](https://img.shields.io/pypi/v/eventiq)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v1.json)](https://github.com/charliermarsh/ruff)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)

# eventiq

Publish/Subscribe asyncio framework for Python

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
- Minimal external dependencies
- Automatic message parsing based on type annotations using `pydantic`
- Code hot-reload
- Highly scalable: each service can process hundreds of tasks concurrently,
    all messages are load balanced between all instances by default
- Resilient - at least once delivery for all messages by default
- Customizable & pluggable message encoders (json by default)
- Multiple broker support
    - Nats
    - Kafka
    - Rabbitmq
    - Redis
- Easily extensible via Middlewares
- Cloud Events standard as base message structure (no more python specific `*args` and `**kwargs` in messages)
- AsyncAPI documentation generation from code
- Twelve factor app approach - stdout logging, configuration through environment variables
- Available extensions for integrating with Prometheus (metrics) and OpenTelemetry (tracing, metrics)

## Basic Usage

```Python
import asyncio
from eventiq import Service, Middleware, CloudEvent
from eventiq.backends.nats import JetStreamBroker

class SendMessageMiddleware(Middleware):
    async def after_broker_connect(self, *, service: Service):
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
    middlewares=[SendMessageMiddleware()]
)


@service.subscribe(topic="test.topic")
async def example_run(message: CloudEvent):
    print(f"Received Message {message.id} with data: {message.data}")
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

## CLI

Getting help:
```shell
eventiq --help
```

Installing shell autocompletion:
```shell
eventiq --install-completion [bash|zsh|fish|powershell|pwsh]
```

### Basic commands

- `run` - run service
- `docs` - generate AsyncAPI docs
- `send` - send message to broker
