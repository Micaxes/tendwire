"""Tests for the neutral connector outbox boundary."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from tendwire.connectors import ConnectorOutboxAPI
from tendwire.store.sqlite import init_store, reclaim_expired_connector_leases


FORBIDDEN = {
    "private_state_json",
    "backend_target",
    "pane_id",
    "session_id",
    "terminal_id",
    "chat_id",
    "topic_id",
    "message_id",
    "bot_token",
    "telegram",
    "herdr",
    "herdres",
    "shell",
    "argv",
    "connector",
    "delivery",
}


def _assert_no_forbidden(value: Any) -> None:
    encoded = json.dumps(value, sort_keys=True).lower()
    for forbidden in FORBIDDEN:
        assert forbidden not in encoded


def _enqueue(db_path: Path, *, key: str = "job-1", status: str = "queued") -> None:
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "host-a",
                "attention",
                key,
                status,
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_type": "attention_created",
                        "safe": "kept",
                        "chat_id": "must-strip",
                        "nested": {"message_id": "must-strip", "safe": "nested"},
                    }
                ),
                json.dumps({"route": "private", "token": "secret"}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                None,
            ),
        )


def _delivery_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT d.status, d.response_json, o.status, o.next_attempt_at, d.attempt
            FROM connector_deliveries d
            JOIN connector_outbox o ON o.id = d.outbox_id
            ORDER BY d.id
            """
        ).fetchall()


def test_poll_leases_sanitized_item_and_skips_duplicate_live_lease(tmp_path: Path) -> None:
    db_path = tmp_path / "connector.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")

    first = api.poll({"name": "attention", "limit": 10, "lease_seconds": 60})
    second = api.poll({"name": "attention", "limit": 10})

    assert first["ok"] is True
    assert first["items"][0]["key"] == "job-1"
    assert first["items"][0]["attempt"] == 1
    assert first["items"][0]["payload"]["safe"] == "kept"
    assert first["items"][0]["payload"]["nested"]["safe"] == "nested"
    assert first["items"][0]["ref"].startswith("twref1.")
    assert second["items"] == []
    _assert_no_forbidden(first)


def test_reclaim_allows_fresh_ref_and_rejects_stale_ref(tmp_path: Path) -> None:
    db_path = tmp_path / "reclaim.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    old = api.poll({"name": "attention", "lease_seconds": 60})["items"][0]["ref"]

    reclaim = reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "attention",
        now="9999-01-01T00:00:00+00:00",
    )
    fresh = api.poll({"name": "attention", "lease_seconds": 60})["items"][0]
    stale_ack = api.ack({"name": "attention", "ref": old, "response": {"safe": "stale"}})

    assert reclaim["reclaimed"] == 1
    assert fresh["attempt"] == 2
    assert fresh["ref"] != old
    assert stale_ack["ok"] is False
    assert stale_ack["status"] in {"stale_ref", "expired_ref", "invalid_ref"}
    assert _delivery_rows(db_path)[-1][0] == "leased"


def test_ack_delivers_sanitized_response_and_blocks_future_poll(tmp_path: Path) -> None:
    db_path = tmp_path / "ack.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    ref = api.poll({"name": "attention"})["items"][0]["ref"]

    ack = api.ack(
        {
            "name": "attention",
            "ref": ref,
            "response": {
                "ok": True,
                "ref": "opaque-provider-ref",
                "message_id": "must-strip",
                "nested": {"bot_token": "must-strip", "safe": "kept"},
            },
        }
    )
    again = api.poll({"name": "attention"})

    assert ack["ok"] is True
    assert ack["status"] == "acknowledged"
    assert again["items"] == []
    rows = _delivery_rows(db_path)
    assert rows[0][0] == "delivered"
    assert rows[0][2] == "delivered"
    stored = json.loads(rows[0][1])
    assert stored["response"]["ref"] == "opaque-provider-ref"
    assert stored["response"]["nested"]["safe"] == "kept"
    _assert_no_forbidden(stored)


def test_fail_and_defer_schedule_future_availability(tmp_path: Path) -> None:
    db_path = tmp_path / "schedule.db"
    _enqueue(db_path, key="fail-job")
    api = ConnectorOutboxAPI(db_path, "host-a")
    first_ref = api.poll({"name": "attention"})["items"][0]["ref"]

    failed = api.fail(
        {
            "name": "attention",
            "ref": first_ref,
            "reason": "temporary",
            "available_at": "9999-01-01T00:00:00+00:00",
            "response": {"safe": "kept", "chat_id": "must-strip"},
        }
    )
    blocked_retry = api.poll({"name": "attention"})
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE connector_outbox SET next_attempt_at = ? WHERE delivery_key = ?",
            ("2000-01-01T00:00:00+00:00", "fail-job"),
        )
    retry = api.poll({"name": "attention"})["items"][0]

    deferred = api.defer(
        {
            "name": "attention",
            "ref": retry["ref"],
            "available_at": "9999-01-01T00:00:00+00:00",
            "reason": "later",
        }
    )
    blocked_defer = api.poll({"name": "attention"})

    assert failed["status"] == "retry_scheduled"
    assert blocked_retry["items"] == []
    assert retry["attempt"] == 2
    assert deferred["status"] == "deferred"
    assert blocked_defer["items"] == []
    _assert_no_forbidden(failed)
    _assert_no_forbidden(deferred)


def test_invalid_wrong_host_and_wrong_name_refs_do_not_mutate(tmp_path: Path) -> None:
    db_path = tmp_path / "invalid.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")
    ref = api.poll({"name": "attention"})["items"][0]["ref"]

    wrong_host = ConnectorOutboxAPI(db_path, "other-host").ack({"name": "attention", "ref": ref})
    wrong_name = api.fail({"name": "other-name", "ref": ref, "reason": "nope"})
    malformed = api.defer({"name": "attention", "ref": "not-a-ref"})

    assert wrong_host["ok"] is False
    assert wrong_name["ok"] is False
    assert malformed["ok"] is False
    assert _delivery_rows(db_path)[0][0] == "leased"
