"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from tendwire.backends import herdr_cli
from tendwire.cli import main
from tendwire.store.sqlite import latest_snapshot


def test_cli_snapshot_json_prints_contract_json_only(capsys) -> None:
    code = main(
        [
            "--host-id",
            "cli-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-host"
    assert len(payload["content_fingerprint"]) == 24
    assert {"updated_at", "spaces", "workers", "attention"} <= set(payload)


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


def test_cli_snapshot_store_persists_printed_snapshot(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    code = main(
        [
            "--host-id",
            "cli-store",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    payload = json.loads(captured.out)
    assert captured.err == ""
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "cli-store"
    assert restored.content_fingerprint == payload["content_fingerprint"]


def test_cli_module_invocation() -> None:
    """python -m tendwire.cli snapshot --json works."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tendwire.cli",
            "--host-id",
            "module-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "module-host"
    assert len(payload["content_fingerprint"]) == 24


def test_cli_snapshot_with_live_shaped_herdr_fixtures(capsys, monkeypatch) -> None:
    """CLI emits schema v2 JSON with non-empty spaces and workers from Herdr fixtures."""

    def _fake_run_herdr(args, cfg):
        if tuple(args) == ("workspace", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "workspaces": [
                            {
                                "workspace_id": "ws-cli",
                                "label": "CLI Space",
                                "agent_status": "working",
                                "focused": True,
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("agent", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "agents": [
                            {
                                "agent_session": {"value": "sess-cli"},
                                "agent": "CLI Agent",
                                "workspace_id": "ws-cli",
                                "agent_status": "executing",
                                "cwd": "/tmp",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-live", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-live"
    assert len(payload["spaces"]) == 1
    assert payload["spaces"][0]["id"] == "ws-cli"
    assert payload["spaces"][0]["status"] == "active"
    assert len(payload["workers"]) == 1
    assert payload["workers"][0]["id"] == "sess-cli"
    assert payload["workers"][0]["status"] == "active"
