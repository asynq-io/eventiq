from pydantic import BaseModel

from eventiq import CloudEvent, Publishes, Service
from eventiq.backends.nats import JetStreamBroker
from eventiq.models import Command

broker = JetStreamBroker(url="nats://nats:password@localhost:4222")


class MyData(BaseModel):
    """Main data for service"""

    counter: int
    info: str


class MyEvent(CloudEvent[MyData]):
    """Some custom event"""


class MyCommand(Command[int], topic="commands.run"):
    """Command representing current number of items"""


service = Service(
    name="example-service",
    version="1.0",
    broker=broker,
    publishes=[
        Publishes(
            type=MyEvent,
            topic="test.topic.{param}.*",
            tags=["tag2"],
            summary="Publishes when X happens",
            parameters={
                "param": {
                    "enum": ["x", "y", "z"],
                    "description": "Description for param",
                },
            },
        ),
    ],
    tags_metadata=[{"name": "tag1", "description": "Some tag 1"}],
)


@service.subscribe(
    topic="test.topic.{param}.*",
    tags=["tag1"],
    parameters={
        "param": {
            "enum": ["a", "b", "c"],
            "description": "Some description",
            "examples": ["a", "b"],
        },
    },
)
async def example_handler(message: MyEvent):
    """Consumer for processing MyEvent(s)"""
    print(f"Received Message {message.id} with data: {message.data}")


@service.subscribe
async def example_run(message: MyCommand):
    """Consumer for processing MyCommands(s)"""
    print(f"Received Message {message.id} with data: {message.data}")
