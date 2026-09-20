import logging.config
import shlex
import sys
from enum import Enum
from pathlib import Path

import anyio
import typer

from .imports import import_from_string
from .logging import get_logger
from .models import CloudEvent
from .service import Service

cli = typer.Typer()

logger = get_logger(__name__, "cli")


class DocsFormat(str, Enum):
    """Output formats supported by the `docs` command."""

    json = "json"
    yaml = "yaml"


def import_service(path: str) -> Service:
    """Import a `Service` instance from a `"module:attribute"` path."""
    # The working directory is added here rather than at import time, so merely
    # importing this module does not mutate interpreter state.
    if "." not in sys.path:
        sys.path.insert(0, ".")
    return import_from_string(path)


def _build_target_from_opts(
    service: str,
    log_level: str | None,
    log_config: str | None,
    *,
    use_uvloop: bool | None,
    debug: bool | None,
) -> str:
    # Quoted: the reload child is spawned as a shell command, so a service path or
    # log config containing spaces would otherwise be split into separate arguments.
    cmd = ["eventiq", "run", shlex.quote(service)]
    if log_level:
        cmd.append(f"--log-level={shlex.quote(log_level)}")
    if log_config:
        cmd.append(f"--log-config={shlex.quote(log_config)}")
    if use_uvloop is not None:
        cmd.append("--use-uvloop" if use_uvloop else "--no-use-uvloop")
    if debug:
        cmd.append("--debug")
    return " ".join(cmd)


@cli.command(help="Run service")
def run(
    service: str,
    *,
    log_level: str | None = typer.Option(
        None,
        help="Logger level, accepted values are: debug, info, warning, error, critical",
    ),
    log_config: str | None = typer.Option(
        None,
        help="Logging file configuration path.",
    ),
    use_uvloop: bool | None = typer.Option(None, help="Enable uvloop"),
    debug: bool = typer.Option(default=False, help="Enable debug"),
    reload: str | None = typer.Option(None, help="Hot-reload on provided path"),
) -> None:
    # Configured before anything else, so the messages below are actually emitted.
    logging.basicConfig(level=(log_level or "info").upper())
    if log_config:
        logging.config.fileConfig(log_config)

    if reload:
        try:
            from watchfiles import run_process
        except ImportError:
            logger.exception(
                "--reload option requires 'watchfiles' installed. Please run 'pip install watchfiles'.",
            )
            return
        logger.info("Watching for changes in: %s", reload)
        target = _build_target_from_opts(
            service,
            log_level,
            log_config,
            use_uvloop=use_uvloop,
            debug=debug,
        )
        run_process(
            reload,
            target=target,
            target_type="command",
            callback=logger.info,
            sigint_timeout=30,
            sigkill_timeout=30,
        )
        # run_process returns when the watcher is interrupted; without this the
        # service would then start a second time in the foreground.
        return

    instance = import_service(service)
    logger.info("Running service: %s", service)
    anyio.run(
        instance.run,
        backend="asyncio",
        backend_options={"use_uvloop": use_uvloop, "debug": debug},
    )


@cli.command(help="Send message via cli")
def send(
    service: str = typer.Argument(
        ...,
        help="Global service object to import in format {package}.{module}:{service_object}",
    ),
    topic: str = typer.Argument(
        ...,
        help="Topic name",
    ),
    data: str = typer.Argument(
        ...,
        help="Data to send",
    ),
    type: str = typer.Option(
        "CloudEvent",
        help="Message type",
    ),
) -> None:
    svc = import_service(service)

    async def connect_and_send(message: CloudEvent) -> None:
        await svc.broker.connect()
        try:
            await svc.publish(message)
        finally:
            await svc.broker.disconnect()

    message_data = svc.decoder.decode(data)
    message = CloudEvent.new(message_data, type=type, topic=topic)
    anyio.run(connect_and_send, message)


@cli.command(help="Generate AsyncAPI documentation from service")
def docs(
    service: str = typer.Argument(
        ...,
        help="Global service object to import in format {package}.{module}:{service_object}",
    ),
    out: Path = typer.Option("./asyncapi.json", help="Output file path"),
    format: DocsFormat = typer.Option(
        DocsFormat.json,
        help="Output format. Valid options are 'yaml' and 'json'(default)",
    ),
) -> None:
    from eventiq.asyncapi import (
        get_async_api_spec,
        save_async_api_to_file,
    )

    svc = import_service(service)
    spec = get_async_api_spec(svc)
    save_async_api_to_file(spec, out, format.value)
    typer.secho(f"Docs saved successfully to {out}", fg="green")
