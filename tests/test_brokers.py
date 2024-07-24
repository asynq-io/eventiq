import os

import pytest

from eventiq.backends.kafka import KafkaBroker
from eventiq.backends.nats import JetStreamBroker, NatsBroker
from eventiq.backends.rabbitmq import RabbitmqBroker
from eventiq.backends.redis import RedisBroker
from eventiq.broker import Broker

backends = [
    (NatsBroker, "nats://loca;host:4444"),
    (JetStreamBroker, "nats://localhost:4444"),
    (KafkaBroker, "kafka://localhost:3333"),
    (RabbitmqBroker, "amqp://localhost:9191"),
    (RedisBroker, "redis://localhost:6379"),
]


@pytest.mark.parametrize("broker,url", backends)
def test_is_subclass(broker, url):
    assert issubclass(broker, Broker)


@pytest.mark.parametrize("broker,url", backends)
def test_from_env(broker, url):
    os.environ["BROKER_URL"] = url
    broker.from_env()
