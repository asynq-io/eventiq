from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol

from eventiq import CloudEvent, Service
from eventiq.backends.nats import JetStreamBroker
from eventiq.types import State

broker = JetStreamBroker(url="nats://localhost:4222")


class UserRepository(Protocol):
    async def get_usernames(self) -> list[str]: ...


class FakeUserRepository:
    def __init__(self, data) -> None:
        self.data = data

    async def get_usernames(self) -> list[str]:
        return self.data


@asynccontextmanager
async def lifespan(_service) -> AsyncIterator[State]:
    print("setup dependencies, connect to db, etc")
    yield {UserRepository: FakeUserRepository(["user1", "user2"])}
    print("cleanup")


service = Service(name="example-service", broker=broker, lifespan=lifespan)


@service.subscribe(topic="test.topic", concurrency=10)
async def example_run(message: CloudEvent, *, repository: UserRepository):
    print(f"Received Message {message.id} with data: {message.data}")
    assert isinstance(repository, FakeUserRepository)
    usernames = await repository.get_usernames()
    assert usernames == ["user1", "user2"]
