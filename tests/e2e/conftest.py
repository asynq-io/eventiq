"""Fixtures binding the e2e suite to one real broker per backend.

Every test runs once per selected backend. A container is started once per
session and shared, while the broker is per test: the broker owns a client
bound to the event loop that opened it, and pytest gives each test a fresh
loop.

Only Redis is wired up today, but the parametrisation over ``backend`` is
generic: adding a broker to :data:`BACKENDS` in ``backends.py`` runs the whole
suite against it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from eventiq import Service

from .backends import Backend, parse_backends

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from eventiq import Broker


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Run every e2e test once per selected backend.

    The ``--e2e`` and ``--e2e-backends`` options are registered in the top-level
    ``tests/conftest.py`` so ``pytest --e2e`` works without a path argument.
    """
    if "backend" not in metafunc.fixturenames:
        return
    backends = parse_backends(metafunc.config.getoption("e2e_backends"))
    metafunc.parametrize(
        "backend",
        backends,
        ids=[backend.name for backend in backends],
        indirect=True,
        scope="session",
    )


@pytest.fixture(scope="session")
def backend(request: pytest.FixtureRequest) -> Backend:
    selected: Backend = request.param
    # Importing the broker module also imports its client library (e.g. `redis`),
    # so this doubles as a check that the driver is installed.
    pytest.importorskip(selected.broker_class.__module__)
    return selected


@pytest.fixture(scope="session")
def broker_url(backend: Backend) -> Generator[str]:
    """Spin the backend up, yield its URL and tear it down after the session."""
    with backend.url() as url:
        yield url


@pytest.fixture
def topic(backend: Backend) -> str:
    """A topic unique to this test, so tests never interfere over pub/sub."""
    return f"e2e.{backend.name}.{uuid4().hex[:8]}"


@pytest.fixture
async def broker(backend: Backend, broker_url: str) -> AsyncGenerator[Broker]:
    """A fresh, initially disconnected broker for the test.

    Connecting is left to the test or to ``Service.context()`` (which calls
    ``broker.connect``/``disconnect`` itself), so the lifecycle tests can assert
    the pre-connect state. ``disconnect`` is idempotent for every backend, so
    this teardown is a safe no-op when the test never connected the broker.
    """
    # ``from_settings(url=...)`` would build the broker's own ``Settings`` first,
    # which for a URL broker requires ``url`` and so would fail; build the
    # settings with the url and pass it in instead.
    settings_cls = cast("type[Any]", backend.broker_class.Settings)
    settings = settings_cls(url=broker_url)
    instance: Broker = backend.broker_class.from_settings(settings)
    try:
        yield instance
    finally:
        await instance.disconnect()


@pytest.fixture
def service(broker: Broker) -> Service:
    return Service(name=f"e2e-{uuid4().hex[:8]}", broker=broker)
