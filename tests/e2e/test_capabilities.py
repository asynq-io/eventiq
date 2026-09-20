"""Pin what the Redis backend does and does not provide.

Redis pub/sub is the most minimal broker eventiq supports, so most of these
gaps are *expected*; the goal is to make them explicit so that adding a richer
backend (NATS, RabbitMQ, Kafka) documents the contrast. A change here should
force a conscious update of both this file and ``backends.py``.
"""

from __future__ import annotations

from .backends import BACKENDS


def test_every_selected_backend_is_declared(backend):
    assert backend.name in BACKENDS
    assert BACKENDS[backend.name] is backend


def test_redis_does_not_relay_headers(backend):
    if backend.name == "redis":
        assert backend.headers is False


def test_redis_nack_has_no_delay(backend):
    if backend.name == "redis":
        assert backend.nack_delay is False


def test_redis_nack_redelivers(backend):
    if backend.name == "redis":
        assert backend.redelivery is True


def test_redis_reports_no_delivery_count(backend):
    if backend.name == "redis":
        assert backend.num_delivered is False
