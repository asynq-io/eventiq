# Usage

## Installation

```shell
pip install eventiq
```

Install a broker backend:

```shell
pip install 'eventiq[nats]'
pip install 'eventiq[rabbitmq]'
pip install 'eventiq[kafka]'
pip install 'eventiq[redis]'
```

---

## Defining a service

```python
from eventiq import Service
from eventiq.backends.nats import JetStreamBroker

broker = JetStreamBroker(url="nats://localhost:4222")

service = Service(
    name="my-service",
    broker=broker,
    version="1.0.0",
    description="Example eventiq service",
)
```

---

## Defining messages

Subclass `CloudEvent` and declare a default topic via the class keyword argument:

```python
from pydantic import BaseModel
from eventiq import CloudEvent

class OrderPayload(BaseModel):
    order_id: int
    amount: float

class OrderCreated(CloudEvent[OrderPayload], topic="orders.created"):
    pass
```

The generic type parameter is the type of the `data` field and can be any pydantic-compatible type.

### Event type

The `type` attribute defaults to the class name. Per the CloudEvents spec it is a producer
defined string, often a reverse-DNS name carrying a version, so it can be declared
explicitly with the `type` class keyword argument:

```python
class OrderCreated(
    CloudEvent[OrderPayload],
    topic="orders.created",
    type="com.example.order.created.v1",
):
    pass
```

The declared value becomes the field default and the only accepted value, so messages of
another type fail validation when decoded into this class.

The same can be expressed by annotating the field with a `Literal`, which is useful when
the type is part of an explicitly declared schema:

```python
class OrderCreated(CloudEvent[OrderPayload], topic="orders.created"):
    type: Literal["com.example.order.created.v1"]
```

A single valued `Literal` is used as the field default, so `type` does not have to be passed
when creating a message. Type checkers cannot see that default, so assign it as well
(`type: Literal["..."] = "..."`) if you construct the event without passing `type`.
Annotating multiple values keeps the field required, restricted to those values.

---

## Subscribing

### Function consumer

```python
@service.subscribe(topic="orders.created")
async def handle_order(message: OrderCreated) -> None:
    print(f"Order {message.data.order_id} received")
```

The event type is inferred from the type annotation. Options like `concurrency` and `timeout` can be set as decorator arguments:

```python
@service.subscribe(topic="orders.created", concurrency=5, timeout=30)
async def handle_order(message: OrderCreated) -> None:
    ...
```

### Class consumer

Subclass `GenericConsumer` for stateful consumers or when you need to publish from within the handler:

```python
from eventiq import CloudEvent, GenericConsumer

@service.subscribe(topic="orders.created")
class OrderConsumer(GenericConsumer[OrderCreated]):
    async def process(self, message: OrderCreated) -> None:
        result = await some_service(message.data)
        await self.publish(
            CloudEvent(topic="orders.processed", data=result)
        )
```

---

## Publishing messages

### From a service instance

```python
event = OrderCreated(data=OrderPayload(order_id=1, amount=9.99))
await service.publish(event)
```

Use `send` as a shorthand when you don't need a typed event subclass:

```python
await service.send(data={"order_id": 1}, type=OrderCreated)
```

### From inside a handler function

Use `CloudEvent.create()` to construct and immediately publish a message from anywhere a service is running:

```python
@service.subscribe(topic="orders.created")
async def handle_order(message: OrderCreated) -> None:
    await CloudEvent.create(
        data={"processed": True},
        topic="orders.processed",
    )
```

### From inside a GenericConsumer

```python
@service.subscribe(topic="orders.created")
class OrderConsumer(GenericConsumer[OrderCreated]):
    async def process(self, message: OrderCreated) -> None:
        await self.publish(
            CloudEvent(topic="orders.processed", data={"ok": True})
        )
```

### Bulk publishing

```python
events = [OrderCreated(data=OrderPayload(order_id=i, amount=1.0)) for i in range(10)]
await service.bulk_publish(events)
```

---

## Wildcard topics

Topics support single-level and multi-level wildcards. The patterns are broker-agnostic — eventiq translates them to the underlying broker's native syntax automatically:

```python
@service.subscribe(topic="orders.*")
async def handle_any_order(message: CloudEvent) -> None:
    ...

```

---

## Lifespan

Pass an async context manager as `lifespan` to run startup/shutdown logic. An optional state dict yielded from the lifespan is merged into `service.state`:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(svc):
    pool = await create_db_pool()
    yield {"db": pool}
    await pool.close()

service = Service(name="my-service", broker=broker, lifespan=lifespan)
```

---

## Dependency injection

Handler parameters other than `message` are injected by annotation. Anything held
in `service.state` — populated at construction, from the lifespan, or directly — is
matched by its type:

```python
service = Service(name="my-service", broker=broker, state={Settings: settings})

@service.subscribe(topic="orders.created")
async def handle_order(message: OrderCreated, settings: Settings) -> None: ...
```

Resolution itself is pluggable: `Service(dependency_resolver=...)` takes anything
with a `resolve(message, annotation)` coroutine returning the value — or
`UNRESOLVED` to leave the parameter to its default — and a `close()` coroutine,
awaited once the handler has returned. `DefaultDependencyResolver` is the one
described above; the modern-di integration installs its own.

### modern-di

For scoped dependencies, install the extra and hand a container to `setup_di`:

```shell
pip install eventiq[modern-di]
```

```python
from typing import Annotated

from modern_di import Container, Group, Scope, providers

from eventiq import CloudEvent, Service
from eventiq.integrations.modern_di import FromDI, setup_di


class AppGroup(Group):
    settings = providers.Factory(Settings, scope=Scope.APP, cache=True)
    report = providers.Factory(Report, scope=Scope.REQUEST)


service = Service(name="reports", broker=broker)
container = setup_di(service, Container(groups=[AppGroup]))
container.validate()


@service.subscribe(topic="reports.requested")
async def handle(
    message: CloudEvent,
    report: Annotated[Report, FromDI(AppGroup.report)],
    settings: Settings,
) -> None: ...
```

No decorator is needed on the handler: `setup_di` installs a middleware which opens
the root container when the broker connects and closes it on disconnect, and replaces
`service.dependency_resolver` with one which builds a `Scope.REQUEST` child container
for the message being processed and closes it — running its finalizers — once the
handler has returned. Parameters are resolved from `service.state` first, then from
that child container, either through an explicit `FromDI` marker or by their bare
type; a handler needing nothing from the container never opens one.

The message being processed is registered as a `Scope.REQUEST` context value, so
providers may depend on `CloudEvent` and receive it, and a `Container` parameter
resolves to the request container.

Two ordering rules: call `setup_di` after `add_middleware`, so the container outlives
the other middlewares' hooks, and call `container.validate()` after `setup_di`, which
is what registers the message's context provider.

---

## Running the service

### CLI

```shell
eventiq run myapp:service --log-level=info
```

Watch for code changes and reload automatically:

```shell
eventiq run myapp:service --reload=.
```

Generate AsyncAPI docs:

```shell
eventiq docs myapp:service --out=asyncapi.json --fmt=yaml
```

Send a one-off message:

```shell
eventiq send myapp:service orders.created '{"order_id": 1}'
```

The `SERVICE_PATH` argument uses the `module:attribute` format, e.g. `mypackage.app:service`.

---

## Middlewares

Middlewares hook into the service lifecycle. Implement only the hooks you need:

```python
from eventiq import Middleware

class LoggingMiddleware(Middleware):
    async def before_process_message(self, *, consumer, message) -> None:
        self.logger.info("Processing %s on %s", message.id, consumer.name)

    async def after_process_message(self, *, consumer, message, result=None, exc=None) -> None:
        if exc:
            self.logger.error("Failed: %s", exc)

service.add_middleware(LoggingMiddleware)
```

See the [Middleware reference](reference/middleware.md) for the full list of available hooks and built-in middleware classes.

### Controlling message fate

Raise one of the following exceptions inside a consumer to explicitly control what happens to the message:

| Exception | Effect |
|-----------|--------|
| `Skip` | Acknowledges the message without retrying. |
| `Fail` | Acknowledges and marks as permanently failed. Triggers `after_fail_message`. |
| `Retry` | Nacks with an optional delay. Triggers `after_retry_message`. |

```python
from eventiq.exceptions import Skip, Fail, Retry

@service.subscribe(topic="orders.created")
async def handle_order(message: OrderCreated) -> None:
    if duplicate(message):
        raise Skip("duplicate")
    if not valid(message.data):
        raise Fail("invalid payload")
    if not downstream_available():
        raise Retry(delay=60)
```

---

## Testing

Use `StubBroker` — an in-memory broker — to write tests without any external infrastructure:

```python
from eventiq import Service
from eventiq.backends.stub import StubBroker

broker = StubBroker(wait_on_publish=True)
service = Service(name="test-service", broker=broker)
```

Because consumers are plain async functions, the simplest tests call them directly:

```python
from eventiq import CloudEvent

async def test_my_handler():
    message = CloudEvent(topic="test.topic", data={})
    result = await my_handler(message)
    assert result == 42
```

For full pipeline tests use `service.context()`:

```python
received = []

@service.subscribe(topic="test.topic")
async def handler(message: CloudEvent) -> None:
    received.append(message.data)

async def test_pipeline():
    async with service.context():
        await service.publish(CloudEvent(topic="test.topic", data={"hello": "world"}))
    assert received == [{"hello": "world"}]
```

`wait_on_publish=True` (default) blocks `publish` until the consumer acks the message, making assertions straightforward. Set it to `False` when you only need fire-and-forget behaviour.

---

## Breaking changes in 2.0

### `CloudEvent.model_dump()` no longer overrides pydantic's defaults

Up to 1.x, `CloudEvent` overrode `model_dump` to default `by_alias=True` and
`exclude_none=True`. That override is gone: `model_dump()` and `model_dump_json()`
now behave exactly like on any other pydantic model.

```python
event = OrderCreated(data=OrderPayload(order_id=1, amount=9.99))

# 2.0: field names, and unset optional attributes are included
event.model_dump()
# {"topic": "orders.created", "content_type": None, "dataschema": None, ...}

# pass the flags explicitly to get CloudEvents-spec keys, as 1.x did implicitly
event.model_dump(by_alias=True, exclude_none=True)
# {"subject": "orders.created", "datacontenttype": "application/json", ...}
```

The keys that change are the aliased ones: `topic` → `subject` and
`content_type` → `datacontenttype`.

The wire format is unaffected — encoders are configured with the right flags
(the default `JsonEncoder(by_alias=True)`) — so only code that persisted,
logged or forwarded the result of `model_dump()` itself needs updating.
