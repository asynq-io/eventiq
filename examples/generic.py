from eventiq import CloudEvent, GenericConsumer, Service
from eventiq.backends.stub import StubBroker

broker = StubBroker()

service = Service(name="example-service", broker=broker)


@service.subscribe(topic="example.topic")
class MyConsumer(GenericConsumer[CloudEvent]):
    # optionally replace `CloudEvent` with more specific class
    name = "example_consumer"

    async def process(self, message: CloudEvent):
        print(f"Received Message {message.id} with data: {message.data}")
        await self.publish(
            message.copy(update={"topic": "example.topic2", "data": "new data"})
        )
