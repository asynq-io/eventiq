import asyncio

from eventiq import CloudEvent, Middleware, Service
from eventiq.backends.nats import JetStreamBroker


class SendMessageMiddleware(Middleware):
    async def after_broker_connect(self, *, service: Service):
        print(f"After service start, running with {service.broker}")
        await asyncio.sleep(10)
        for i in range(100):
            message = CloudEvent.new({"counter": i}, topic="test.topic")
            await service.publish(message)
        print("Published messages(s)")


broker = JetStreamBroker(url="nats://localhost:4222")

service = Service(
    name="example-service", broker=broker, middlewares=[SendMessageMiddleware()]
)


@service.subscribe(topic="test.topic", concurrency=10)
async def example_run(message: CloudEvent):
    print(f"Received Message {message.id} with data: {message.data}")
