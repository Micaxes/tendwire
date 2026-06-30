"""Daemon and CLI coverage for connector JSON boundary."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from tendwire.cli import main
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.config import Config
from tendwire.core.models import Snapshot
from tendwire.daemon import TendwireDaemon
from tendwire.daemon_api import TendwireDaemonAPI
from tendwire.store.sqlite import init_store


def _enqueue(db_path: Path, *, host_id: str = "host-a", key: str = "job-1") -> None:
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
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
                        "chat_id": "must-strip",
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
            json.dumps({"safe": "kept", "chat_id": "must-strip"}),
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


def test_connector_api_store_unavailable_returns_safe_error() -> None:
    payload = ConnectorOutboxAPI(None, "host-a").poll({"name": "attention"})

    assert payload["ok"] is False
    assert payload["status"] == "store_unavailable"
    _assert_json_only_and_safe(payload)
