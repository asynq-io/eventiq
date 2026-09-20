import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from eventiq.cli import DocsFormat, cli, import_service

runner = CliRunner()

SERVICE_PATH = "tests.cli_service:service"


# --- import_service ---


def test_import_service_returns_service():
    from eventiq import Service

    assert isinstance(import_service(SERVICE_PATH), Service)


# --- run --reload ---


def test_run_with_reload_does_not_also_run_service():
    """run_process returns on Ctrl+C; the service must not then start again."""
    watchfiles = MagicMock()
    with (
        patch.dict("sys.modules", {"watchfiles": watchfiles}),
        patch("eventiq.cli.import_service") as import_mock,
        patch("eventiq.cli.anyio.run") as anyio_run,
    ):
        result = runner.invoke(cli, ["run", SERVICE_PATH, "--reload", "."])

    assert result.exit_code == 0
    watchfiles.run_process.assert_called_once()
    import_mock.assert_not_called()
    anyio_run.assert_not_called()


def test_run_reload_target_includes_options():
    watchfiles = MagicMock()
    with (
        patch.dict("sys.modules", {"watchfiles": watchfiles}),
        patch("eventiq.cli.anyio.run"),
    ):
        runner.invoke(
            cli,
            ["run", SERVICE_PATH, "--reload", ".", "--log-level", "debug", "--debug"],
        )

    target = watchfiles.run_process.call_args.kwargs["target"]
    assert SERVICE_PATH in target
    assert "--log-level=debug" in target
    assert "--debug" in target


def test_run_without_reload_starts_service():
    with (
        patch("eventiq.cli.import_runner") as import_mock,
        patch("eventiq.cli.anyio.run") as anyio_run,
    ):
        result = runner.invoke(cli, ["run", SERVICE_PATH])

    assert result.exit_code == 0
    import_mock.assert_called_once_with(SERVICE_PATH)
    anyio_run.assert_called_once()


# --- docs ---


def test_docs_writes_json(tmp_path: Path):
    out = tmp_path / "asyncapi.json"
    result = runner.invoke(cli, ["docs", SERVICE_PATH, "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert json.loads(out.read_text())["asyncapi"] == "3.0.0"


def test_docs_rejects_unknown_format(tmp_path: Path):
    """An unknown --format must fail loudly instead of silently writing JSON."""
    out = tmp_path / "asyncapi.yml"
    result = runner.invoke(
        cli, ["docs", SERVICE_PATH, "--out", str(out), "--format", "yml"]
    )

    assert result.exit_code != 0
    assert not out.exists()


def test_docs_format_enum_values():
    assert {f.value for f in DocsFormat} == {"json", "yaml"}
