import asyncio

from eventiq import CloudEvent, Middleware, Service
from eventiq.backends.nats import JetStreamBroker


class SendMessageMiddleware(Middleware):
    async def after_broker_connect(self):
        print(f"After service start, running with {self.service.broker}")
        for i in range(1, 10):
            message = CloudEvent.new({"counter": i}, topic="test.topic")
            await self.service.publish(message)
        print("Published messages(s)")


broker = JetStreamBroker(url="nats://localhost:4222")

service = Service(
    name="example-service",
    broker=broker,
)
service.add_middleware(SendMessageMiddleware)


@service.subscribe(topic="test.topic", concurrency=2)
async def example_run(message: CloudEvent):
    print("Received Message", message.id, "with data:", message.data)
    await asyncio.sleep(5)
