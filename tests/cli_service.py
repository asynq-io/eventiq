"""A module-level service for the CLI tests to import by path."""

from eventiq import CloudEvent, Service
from eventiq.backends.stub import StubBroker


class CliEvent(CloudEvent[dict], topic="cli.event"):
    """Event used by the CLI test service."""


service = Service(name="cli_test_service", broker=StubBroker(wait_on_publish=False))


@service.subscribe(topic="cli.event")
async def handle_cli_event(message: CliEvent) -> None:
    pass
