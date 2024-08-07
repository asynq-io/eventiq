import os

import pytest

from eventiq.backends.kafka import KafkaBroker
from eventiq.backends.nats import JetStreamBroker, NatsBroker
from eventiq.backends.rabbitmq import RabbitmqBroker
from eventiq.backends.redis import RedisBroker
from eventiq.broker import Broker

backends = (NatsBroker, JetStreamBroker, KafkaBroker, RabbitmqBroker, RedisBroker)


@pytest.mark.parametrize("broker", backends)
def test_is_subclass(broker):
    assert issubclass(broker, Broker)


@pytest.mark.parametrize("broker", backends)
def test_from_env(broker):
    os.environ["BROKER_URL"] = f"{broker.protocol}://localhost:1111"
    broker.from_env()
