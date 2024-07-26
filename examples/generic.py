from eventiq import CloudEvent, GenericConsumer, Service
from eventiq.backends.stub import StubBroker

broker = StubBroker()

service = Service(name="example-service", broker=broker)


@service.subscribe(topic="example.topic")
class MyConsumer(GenericConsumer[CloudEvent]):
    # optionally replace `CloudEvent` with more specific class
    name = "example_consumer"

    async def process(self, message: CloudEvent):
        print(message)
