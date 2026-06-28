"""Tests for the read-only Herdr doctor diagnostics."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_cli
from tendwire.backends.herdr_cli import diagnose_herdr, fetch_herdr_state
from tendwire.cli import main
from tendwire.config import Config, load_config


FIXTURES = Path(__file__).parent / "fixtures" / "herdr"

_FORBIDDEN_TEXT = (
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "backend_target",
)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _completed(args: Sequence[str], stdout: str = "", stderr: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", *args],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _doctor_outcomes(payload: dict[str, Any]) -> dict[str, str]:
    return {str(check["name"]): str(check["outcome"]) for check in payload["checks"]}


def test_doctor_reports_missing_herdr_binary(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="missing-herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: None)

    payload = diagnose_herdr(config)

    assert payload["status"] == "unavailable"
    assert _doctor_outcomes(payload)["workspace_list"] == "missing_binary"
    assert all(check["ok"] is False for check in payload["checks"])


def test_doctor_reports_timeout_and_skips_remaining_checks(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr", herdr_timeout_seconds=0.25)
    calls: list[tuple[str, ...]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args[1:]))
        assert kwargs["timeout"] == 0.25
        raise subprocess.TimeoutExpired(cmd=args, timeout=kwargs["timeout"])

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    payload = diagnose_herdr(config)
    outcomes = _doctor_outcomes(payload)

    assert payload["status"] == "timeout"
    assert calls == [("workspace", "list")]
    assert outcomes["workspace_list"] == "timeout"
    assert outcomes["agent_list"] == "skipped_after_timeout"


def test_doctor_distinguishes_nonzero_malformed_empty_and_nonempty(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed(
            ["workspace", "list"],
            stderr=_fixture("nonzero_stderr.txt"),
            returncode=2,
        ),
        ("workspace", "list", "--json"): _completed(
            ["workspace", "list", "--json"],
            stdout=_fixture("workspace_list_json_empty.json"),
        ),
        ("agent", "list"): _completed(
            ["agent", "list"],
            stdout=_fixture("malformed.txt"),
        ),
        ("agent", "list", "--json"): _completed(
            ["agent", "list", "--json"],
            stdout=_fixture("agent_list_no_flag_nonempty.json"),
        ),
        ("pane", "list"): _completed(
            ["pane", "list"],
            stdout=_fixture("pane_list_empty.json"),
        ),
    }

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return responses[tuple(args[1:])]

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    payload = diagnose_herdr(config)
    outcomes = _doctor_outcomes(payload)

    assert payload["status"] == "degraded"
    assert outcomes["workspace_list"] == "nonzero"
    assert outcomes["workspace_list_json"] == "empty_healthy"
    assert outcomes["agent_list"] == "malformed_json"
    assert outcomes["agent_list_json"] == "healthy_non_empty"
    assert outcomes["pane_list"] == "empty_healthy"
    assert "herdr fixture error" in json.dumps(payload)


def test_doctor_cli_outputs_json_only_and_sanitizes_samples(capsys, monkeypatch) -> None:
    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return _completed(args[1:], stdout="not json token=must-not-leak", returncode=0)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    code = main(["--host-id", "doctor-host", "--herdr-bin", "herdr", "doctor", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["command"] == "doctor"
    serialized = json.dumps(payload).lower()
    assert not any(forbidden in serialized for forbidden in _FORBIDDEN_TEXT)


def test_cli_herdr_timeout_knob_is_used_by_doctor(capsys, monkeypatch) -> None:
    timeouts: list[float] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        timeouts.append(float(kwargs["timeout"]))
        return _completed(args[1:], stdout=_fixture("pane_list_empty.json"))

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli.subprocess, "run", fake_run)

    code = main(["--herdr-timeout", "0.75", "--herdr-bin", "herdr", "doctor", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert timeouts == [0.75, 0.75, 0.75]
    assert payload["timeout_seconds"] == 0.75


def test_tilde_path_expansion_for_configured_paths(monkeypatch) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", "~/tendwire-data")
    monkeypatch.setenv("TENDWIRE_DB_PATH", "~/tendwire-data/tendwire.db")
    monkeypatch.setenv("TENDWIRE_HERDR_BIN", "~/bin/herdr")
    monkeypatch.setenv("TENDWIRE_HERDR_TIMEOUT_SECONDS", "0.5")

    config = load_config()

    assert str(config.data_dir).startswith(str(Path.home()))
    assert str(config.db_path).startswith(str(Path.home()))
    assert config.herdr_bin.startswith(str(Path.home()))
    assert config.herdr_timeout_seconds == 0.5


def test_fixture_outputs_parse_through_snapshot_fail_soft_path(monkeypatch) -> None:
    config = Config(host_id="fixture-host", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed(
            ["workspace", "list"],
            stdout=_fixture("workspace_list_no_flag_nonempty.json"),
        ),
        ("agent", "list"): _completed(
            ["agent", "list"],
            stdout=_fixture("agent_list_no_flag_nonempty.json"),
        ),
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(args))
        return responses.get(tuple(args), _completed(args, stdout="", returncode=1))

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-fixture"
    assert len(workers) == 1
    assert workers[0].id == "Fixture Agent"
