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


@pytest.mark.parametrize("broker", [JetStreamBroker, RedisBroker])
def test_poll_interval_default(broker):
    os.environ["BROKER_URL"] = f"{broker.protocol}://localhost:1111"
    os.environ.pop("BROKER_POLL_INTERVAL", None)
    instance = broker.from_env()
    assert instance.poll_interval == 0.0


@pytest.mark.parametrize("broker", [JetStreamBroker, RedisBroker])
def test_poll_interval_from_env(broker):
    os.environ["BROKER_URL"] = f"{broker.protocol}://localhost:1111"
    os.environ["BROKER_POLL_INTERVAL"] = "1.5"
    instance = broker.from_env()
    assert instance.poll_interval == 1.5
    del os.environ["BROKER_POLL_INTERVAL"]


def test_heartbeat_default():
    os.environ["BROKER_URL"] = f"{JetStreamBroker.protocol}://localhost:1111"
    os.environ.pop("BROKER_HEARTBEAT", None)
    instance = JetStreamBroker.from_env()
    assert instance.heartbeat == 0.1


def test_heartbeat_from_env():
    os.environ["BROKER_URL"] = f"{JetStreamBroker.protocol}://localhost:1111"
    os.environ["BROKER_HEARTBEAT"] = "0.5"
    instance = JetStreamBroker.from_env()
    assert instance.heartbeat == 0.5
    del os.environ["BROKER_HEARTBEAT"]
