from __future__ import annotations

import logging.config
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import typer

from .imports import import_from_string
from .logging import get_logger

if TYPE_CHECKING:
    from .service import Service

cli = typer.Typer()

logger = get_logger(__name__, "cli")

if "." not in sys.path:
    sys.path.insert(0, ".")


def _build_target_from_opts(
    service: str,
    log_level: str | None,
    log_config: str | None,
    use_uvloop: bool | None,
    debug: bool | None,
) -> str:
    cmd = [f"eventiq run {service}"]
    if log_level:
        cmd.append(f"--log-level={log_level}")
    if log_config:
        cmd.append(f"--log-config={log_config}")
    if use_uvloop:
        cmd.append("--use-uvloop=true")
    if debug:
        cmd.append("--debug=true")
    return " ".join(cmd)


@cli.command(help="Run service")
def run(
    service: str,
    log_level: str | None = typer.Option(
        None,
        help="Logger level, accepted values are: debug, info, warning, error, critical",
    ),
    log_config: str | None = typer.Option(
        None, help="Logging file configuration path."
    ),
    use_uvloop: bool | None = typer.Option(None, help="Enable uvloop"),
    debug: bool = typer.Option(False, help="Enable debug"),
    reload: str | None = typer.Option(None, help="Hot-reload on provided path"),
) -> None:
    if reload:
        try:
            from watchfiles import run_process
        except ImportError:
            logger.error(
                "--reload option requires 'watchfiles' installed. Please run 'pip install watchfiles'."
            )
            return
        logger.info(f"Watching [{service}]...")
        target = _build_target_from_opts(
            service, log_level, log_config, use_uvloop, debug
        )
        run_process(
            reload,
            target=target,
            target_type="command",
            callback=logger.info,
            sigint_timeout=30,
            sigkill_timeout=30,
        )

    if log_level:
        logging.basicConfig(level=log_level.upper())
    if log_config:
        logging.config.fileConfig(log_config)

    logger.info(f"Running [{service}]...")
    instance: Service = import_from_string(service)
    anyio.run(
        instance.run,
        True,
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
):
    svc: Service = import_from_string(service)

    async def connect_and_send(message_data):
        await svc.broker.connect()
        try:
            await svc.send(topic, type, message_data)
        finally:
            await svc.broker.disconnect()

    message_data = svc.broker.decoder.decode(data)
    anyio.run(connect_and_send, message_data)


@cli.command(help="Generate AsyncAPI documentation from service")
def docs(
    service: str = typer.Argument(
        ...,
        help="Global service object to import in format {package}.{module}:{service_object}",
    ),
    out: Path = typer.Option("./asyncapi.json", help="Output file path"),
    format: str = typer.Option(
        "json", help="Output format. Valid options are 'yaml' and 'json'(default)"
    ),
):
    from eventiq.asyncapi import get_async_api_spec, save_async_api_to_file

    svc = import_from_string(service)
    spec = get_async_api_spec(svc)
    save_async_api_to_file(spec, out, format)
    typer.secho(f"Docs saved successfully to {out}", fg="green")