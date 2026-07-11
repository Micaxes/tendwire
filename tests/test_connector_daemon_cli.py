"""Daemon and CLI coverage for connector JSON boundary."""

from __future__ import annotations

from contextlib import closing
import io
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tendwire.cli import main
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.config import Config
from tendwire.core.models import Snapshot
from tendwire.daemon import TendwireDaemon
from tendwire.daemon_api import TendwireDaemonAPI
from tendwire.store.sqlite import init_store



@pytest.fixture(autouse=True)
def _isolate_cli_state(tmp_path: Path, monkeypatch) -> None:
    private_home = tmp_path / "isolated-home"
    private_home.mkdir(mode=0o700)
    data_dir = tmp_path / "isolated-data"
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(data_dir / "tendwire.db"))


def _enqueue(db_path: Path, *, host_id: str = "host-a", key: str = "job-1") -> None:
    init_store(db_path)
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                host_id,
                "attention",
                key,
                "queued",
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_type": "attention_escalated",
                        "safe": "kept",
                        "transport": "telegram",
                        "chat_id": "must-strip",
                        "nested": {"backend_value": "herdres", "safe": "nested"},
                    }
                ),
                json.dumps({"message_id": "private"}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )


def _assert_json_only_and_safe(payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True).lower()
    for forbidden in (
        "private_state_json",
        "backend_target",
        "chat_id",
        "topic_id",
        "message_id",
        "bot_token",
        "telegram",
        "herdres",
        "pane_id",
        "session_id",
        "terminal_id",
        "shell",
        "argv",
        "connector",
        "delivery",
    ):
        assert forbidden not in encoded


def test_daemon_api_routes_connector_methods_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-connector.db"
    _enqueue(db_path, host_id="daemon-host")
    config = Config(host_id="daemon-host", db_path=db_path)
    daemon = TendwireDaemon(config)
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok", "host_id": "daemon-host"},
        submit_command=daemon.submit_command,
        connector_call=daemon.connector_call,
    )

    poll = api.dispatch({"method": "connector.poll", "params": {"name": "attention"}})
    ref = poll["result"]["items"][0]["ref"]
    ack = api.dispatch(
        {
            "method": "connector.ack",
            "params": {
                "name": "attention",
                "ref": ref,
                "response": {"safe": "kept", "message_id": "must-strip"},
            },
        }
    )
    after = api.dispatch({"method": "connector.poll", "params": {"name": "attention"}})

    assert poll["ok"] is True
    assert poll["result"]["ok"] is True
    assert ack["result"]["status"] == "acknowledged"
    assert after["result"]["items"] == []
    _assert_json_only_and_safe(poll)
    _assert_json_only_and_safe(ack)


def test_cli_connector_poll_and_ack_print_json_only(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli-connector.db"
    _enqueue(db_path)
    poll_code = main(
        [
            "--host-id",
            "host-a",
            "connector",
            "poll",
            "--name",
            "attention",
            "--db-path",
            str(db_path),
            "--lease-seconds",
            "60",
        ]
    )
    poll_captured = capsys.readouterr()
    poll_payload = json.loads(poll_captured.out)
    ref = poll_payload["items"][0]["ref"]

    ack_code = main(
        [
            "--host-id",
            "host-a",
            "connector",
            "ack",
            "--name",
            "attention",
            "--ref",
            ref,
            "--response-json",
            json.dumps({"safe": "kept", "chat_id": "must-strip", "provider": "telegram"}),
            "--db-path",
            str(db_path),
        ]
    )
    ack_captured = capsys.readouterr()
    ack_payload = json.loads(ack_captured.out)

    assert poll_code == 0
    assert ack_code == 0
    assert poll_captured.err == ""
    assert ack_captured.err == ""
    assert poll_payload["items"][0]["payload"]["safe"] == "kept"
    assert ack_payload["status"] == "acknowledged"
    _assert_json_only_and_safe(poll_payload)
    _assert_json_only_and_safe(ack_payload)


def test_cli_connector_prepare_reads_bounded_action_from_stdin(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    private_home = tmp_path / "home"
    private_home.mkdir(mode=0o700)
    data_dir = tmp_path / "tendwire-data"
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(data_dir / "tendwire.db"))
    calls: list[tuple[str, dict[str, Any]]] = []
    action = {
        "schema_version": 1,
        "action": "part",
        "plan_token": "twplan1.public",
        "ordinal": 0,
        "spans": [
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": 42,
            }
        ],
    }

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, dict(params or {})))
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "name": "turn-final",
                    "plan_token": "twplan1.public",
                    "ordinal": 0,
                    "accepted_parts": 1,
                },
            }

    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(tmp_path / "data" / "tendwire.db"))
    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(action)))
    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "prepare",
            "--name",
            "turn-final",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["plan_token"] == "twplan1.public"
    assert calls == [
        (
            "connector.prepare",
            {
                **action,
                "name": "turn-final",
            },
        )
    ]


def test_cli_daemon_connector_result_is_sanitized_before_printing(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    class FakeDaemonAPIClient:
        def __init__(self, socket_path: Any, *, timeout_seconds: float, max_response_bytes: int = 1024 * 1024):
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "connector.poll"
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "host_id": "host-a",
                    "name": "attention",
                    "backend_target": "sentinel-private-target",
                    "items": [
                        {
                            "ref": "twref1.publicSafeRef",
                            "payload": {
                                "safe": "kept",
                                "chat_id": "sentinel-private-chat",
                                "raw_payload": "sentinel-private-raw",
                            },
                            "pane_id": "sentinel-private-pane",
                        }
                    ],
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)

    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "poll",
            "--name",
            "attention",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert code == 0
    assert captured.err == ""
    assert payload["items"][0]["payload"] == {"safe": "kept"}
    assert "sentinel-private" not in encoded
    assert "raw_payload" not in encoded
    _assert_json_only_and_safe(payload)


def test_connector_api_store_unavailable_returns_safe_error() -> None:
    payload = ConnectorOutboxAPI(None, "host-a").poll({"name": "attention"})

    assert payload["ok"] is False
    assert payload["status"] == "store_unavailable"
    _assert_json_only_and_safe(payload)


def test_recover_rpc_is_forwarded_and_printed_with_exact_frozen_contract(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    failed_token = "twplan1.failedPublic"
    recovered_token = "twplan1.recoveredPublic"
    params = {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": failed_token,
        "request_id": "recover-request-42",
    }
    result = {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": failed_token,
        "plan_token": recovered_token,
        "generation": 2,
        "content_revision": "twrev1.publicRevision",
        "state": "active",
        "acknowledged_prefix_count": 1,
        "executable_job_count": 2,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 3,
        "idempotent_replay": False,
    }
    daemon_calls: list[tuple[str, dict[str, Any]]] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="host-a"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda method, call_params: (
            daemon_calls.append((method, dict(call_params))) or result
        ),
    )
    envelope = api.dispatch({"method": "connector.prepare", "params": params})
    assert envelope["result"] == result
    assert daemon_calls == [("connector.prepare", params)]

    cli_calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            cli_calls.append((method, dict(params or {})))
            return {"ok": True, "result": result}

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(params)))
    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "prepare",
            "--name",
            "turn-final",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == result
    assert cli_calls == [("connector.prepare", params)]
