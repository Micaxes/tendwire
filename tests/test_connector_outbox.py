"""Tests for the neutral connector outbox boundary."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Barrier
from typing import Any
import pytest

from tendwire.store import sqlite as store_sqlite

from tendwire.core.models import AttentionSignal, Snapshot
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    init_store,
    poll_connector_outbox,
    reclaim_expired_connector_leases,
    save_snapshot,
)


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
    "backend.target",
    "pane.id",
    "message.id",
    "bot.token",
    "backend target",
    "pane id",
    "session id",
    "terminal id",
    "chat id",
    "topic id",
    "message id",
    "bot token",
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
                        "transport": "telegram",
                        "backend_name": "herdres",
                        "chat_id": "must-strip",
                        "nested": {
                            "message_id": "must-strip",
                            "safe": "nested",
                            "backend_value": "herdr",
                            "list": ["ok", "telegram", "bot.token"],
                            "dot_value": "message.id",
                        },
                        "dot_private": "backend.target",
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
    assert "host-a" not in first["items"][0]["ref"]
    assert "attention" not in first["items"][0]["ref"]
    assert "job-1" not in first["items"][0]["ref"]
    assert "lease" not in first["items"][0]["ref"].lower()
    assert "transport" not in first["items"][0]["payload"]
    assert "backend_name" not in first["items"][0]["payload"]
    assert "backend_value" not in first["items"][0]["payload"]["nested"]
    assert first["items"][0]["payload"]["nested"]["list"] == ["ok"]
    assert second["items"] == []
    _assert_no_forbidden(first)


def test_poll_uses_configured_default_lease_and_explicit_lease_wins(tmp_path: Path) -> None:
    db_path = tmp_path / "lease-default.db"
    _enqueue(db_path, key="default-lease")
    default_api = ConnectorOutboxAPI(db_path, "host-a", default_lease_seconds=3600)
    default_item = default_api.poll({"name": "attention"})["items"][0]
    default_delta = (
        datetime.fromisoformat(default_item["leased_until"])
        - datetime.fromisoformat(default_item["available_at"])
    ).total_seconds()

    reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "attention",
        now="9999-01-01T00:00:00+00:00",
    )
    explicit_item = default_api.poll({"name": "attention", "lease_seconds": 5})["items"][0]
    explicit_delta = (
        datetime.fromisoformat(explicit_item["leased_until"])
        - datetime.fromisoformat(explicit_item["available_at"])
    ).total_seconds()

    assert default_delta == 3600
    assert explicit_delta == 5


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
                "provider": "telegram",
                "message_id": "must-strip",
                "dot_value": "message.id",
                "space_value": "message id must strip",
                "nested": {
                    "bot_token": "must-strip",
                    "safe": "kept",
                    "transport": "herdres",
                    "dot_list": ["safe", "bot.token"],
                    "space_list": ["safe", "bot token"],
                },
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
    assert "ref" not in stored["response"]
    assert stored["response"]["nested"]["safe"] == "kept"
    assert stored["response"]["nested"]["dot_list"] == ["safe"]
    assert stored["response"]["nested"]["space_list"] == ["safe"]
    assert "provider" not in stored["response"]
    assert "space_value" not in stored["response"]
    assert "transport" not in stored["response"]["nested"]
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
            "reason": "backend target chat id bot token",
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
            "reason": "pane id terminal id session id message id",
        }
    )
    blocked_defer = api.poll({"name": "attention"})
    rows = _delivery_rows(db_path)
    failed_payload = json.loads(rows[0][1])
    deferred_payload = json.loads(rows[1][1])

    assert failed["status"] == "retry_scheduled"
    assert blocked_retry["items"] == []
    assert retry["attempt"] == 2
    assert deferred["status"] == "deferred"
    assert blocked_defer["items"] == []
    assert failed_payload["reason"] == ""
    assert deferred_payload["reason"] == ""
    _assert_no_forbidden(failed_payload)
    _assert_no_forbidden(deferred_payload)
    _assert_no_forbidden(failed)
    _assert_no_forbidden(deferred)


def test_max_outbox_attempts_dead_letters_exhausted_failures(tmp_path: Path) -> None:
    db_path = tmp_path / "attempts.db"
    _enqueue(db_path, key="attempt-job")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=2)
    first_ref = api.poll({"name": "attention"})["items"][0]["ref"]
    first_fail = api.fail({"name": "attention", "ref": first_ref, "delay_seconds": 0})

    second = api.poll({"name": "attention"})["items"][0]
    exhausted = api.fail({"name": "attention", "ref": second["ref"], "delay_seconds": 0})
    after = api.poll({"name": "attention"})

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status, next_attempt_at = conn.execute(
            "SELECT status, next_attempt_at FROM connector_outbox WHERE delivery_key = ?",
            ("attempt-job",),
        ).fetchone()

    assert first_fail["status"] == "retry_scheduled"
    assert second["attempt"] == 2
    assert exhausted["status"] == "attempts_exhausted"
    assert "available_at" not in exhausted
    assert outbox_status == "dead_letter"
    assert next_attempt_at is None
    assert after["items"] == []
    _assert_no_forbidden(exhausted)


def test_poll_dead_letters_expired_lease_at_max_attempts_before_repoll(tmp_path: Path) -> None:
    db_path = tmp_path / "expired-max-attempt.db"
    _enqueue(db_path, key="expired-max")
    first = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    fail_connector_delivery(
        db_path,
        host_id="host-a",
        name="attention",
        ref=first["ref"],
        delay_seconds=0,
        max_attempts=2,
        now="2026-01-01T00:00:01+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]

    after = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("expired-max",),
        ).fetchone()[0]
        attempts = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(attempt), 0)
            FROM connector_deliveries
            WHERE delivery_key = ?
            """,
            ("expired-max",),
        ).fetchone()

    assert first["attempt"] == 1
    assert second["attempt"] == 2
    assert after["items"] == []
    assert outbox_status == "dead_letter"
    assert attempts == (2, 2)


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


def test_connector_api_rejects_non_neutral_public_names_without_echoing_them(tmp_path: Path) -> None:
    db_path = tmp_path / "connector-name.db"
    _enqueue(db_path)
    api = ConnectorOutboxAPI(db_path, "host-a")

    payloads = [
        api.poll({"name": "telegram"}),
        api.reclaim({"name": "herdres"}),
        api.ack({"name": "attention/chat", "ref": "twref1.publicSafeRef"}),
        api.fail({"name": "backend_target", "ref": "twref1.publicSafeRef"}),
        api.defer({"name": "attention delivery", "ref": "twref1.publicSafeRef"}),
        api.poll({"name": "backend.target"}),
        api.reclaim({"name": "pane.id"}),
        api.ack({"name": "message.id", "ref": "twref1.publicSafeRef"}),
        api.fail({"name": "bot.token", "ref": "twref1.publicSafeRef"}),
    ]
    unsafe_ref_payloads = [
        api.ack({"name": "attention", "ref": "telegram-message-id"}),
        api.fail({"name": "attention", "ref": "herdres-route"}),
        api.defer({"name": "attention", "ref": "backend_target"}),
    ]
    still_pollable = api.poll({"name": "attention"})

    for payload in payloads:
        assert payload["ok"] is False
        assert payload["status"] == "invalid_params"
        assert payload["name"] == ""
        _assert_no_forbidden(payload)
    for payload in unsafe_ref_payloads:
        assert payload["ok"] is False
        assert payload["status"] == "invalid_ref"
        assert "ref" not in payload
        _assert_no_forbidden(payload)
    assert still_pollable["items"][0]["key"] == "job-1"


def test_lifecycle_delivery_key_survives_fail_defer_reclaim_and_ack(tmp_path: Path) -> None:
    db_path = tmp_path / "lifecycle-delivery.db"
    host_id = "host-lifecycle"
    observed_at = "2026-01-01T00:00:00+00:00"
    snapshot = Snapshot(
        host_id=host_id,
        updated_at=observed_at,
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Review the worker",
                source="worker:worker-1",
                updated_at=observed_at,
                meta={"worker_id": "worker-1", "needs_human": True},
                host_id=host_id,
            )
        ],
    )
    observation = SnapshotObservationContext(authority="complete", observed_at=observed_at)
    save_snapshot(db_path, snapshot, observation=observation)

    with sqlite3.connect(str(db_path)) as conn:
        generated = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert len(generated) == 1
    stable_key = generated[0][0]
    assert stable_key.startswith("attention:attention_created:")
    assert generated[0][1] == "queued"

    first = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:01+00:00",
    )["items"][0]
    assert first["key"] == stable_key
    assert first["attempt"] == 1
    assert first["payload"]["event_type"] == "attention_created"

    save_snapshot(db_path, snapshot, observation=observation)
    with sqlite3.connect(str(db_path)) as conn:
        leased_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
    assert leased_rows == [(stable_key, "leased")]

    failed = fail_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=first["ref"],
        delay_seconds=1,
        now="2026-01-01T00:00:02+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:03+00:00",
    )["items"][0]
    deferred = defer_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=second["ref"],
        available_at="2026-01-01T00:00:05+00:00",
        now="2026-01-01T00:00:04+00:00",
    )
    third = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:05+00:00",
    )["items"][0]

    not_expired = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:14+00:00",
    )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:15+00:00",
    )
    fourth = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=10,
        now="2026-01-01T00:00:15+00:00",
    )["items"][0]
    acknowledged = ack_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=fourth["ref"],
        response={"safe": "delivered"},
        now="2026-01-01T00:00:16+00:00",
    )
    after_ack = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:17+00:00",
    )

    keyed_responses = (first, failed, second, deferred, third, fourth, acknowledged)
    assert {response["key"] for response in keyed_responses} == {stable_key}
    assert [first["attempt"], second["attempt"], third["attempt"], fourth["attempt"]] == [1, 2, 3, 4]
    assert len({first["ref"], second["ref"], third["ref"], fourth["ref"]}) == 4
    assert failed["status"] == "retry_scheduled"
    assert deferred["status"] == "deferred"
    assert not_expired["reclaimed"] == 0
    assert reclaimed["reclaimed"] == 1
    assert acknowledged["status"] == "acknowledged"
    assert after_ack["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        outbox_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        delivery_rows = conn.execute(
            """
            SELECT delivery_key, attempt, status
            FROM connector_deliveries
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert outbox_rows == [(stable_key, "delivered")]
    assert [row[0] for row in delivery_rows] == [stable_key] * 4
    assert [row[1] for row in delivery_rows] == [1, 2, 3, 4]
    assert [row[2] for row in delivery_rows] == ["failed", "deferred", "expired", "delivered"]


def test_migration_terminalizes_noncanonical_live_leases_through_public_helpers(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "leased-duplicate-migration.db"
    host_id = "host-migration-leases"
    observed_at = "2026-01-01T00:00:00+00:00"
    snapshot = Snapshot(
        host_id=host_id,
        updated_at=observed_at,
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Review the worker",
                source="worker:worker-1",
                updated_at=observed_at,
                host_id=host_id,
            )
        ],
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=observed_at,
        ),
    )

    with sqlite3.connect(str(db_path)) as conn:
        for suffix in range(1, 4):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                )
                SELECT
                    host_id, connector, ?, 'queued', payload_json,
                    '{}', created_at, updated_at, NULL
                FROM connector_outbox
                WHERE host_id = ? AND connector = 'attention'
                ORDER BY id
                LIMIT 1
                """,
                (f"legacy-duplicate-{suffix}", host_id),
            )

    canonical = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:01+00:00",
    )["items"][0]
    failed_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]
    deferred_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=100,
        now="2026-01-01T00:00:03+00:00",
    )["items"][0]
    expiring_lease = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        lease_seconds=5,
        now="2026-01-01T00:00:04+00:00",
    )["items"][0]

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)

    failed = fail_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=failed_lease["ref"],
        delay_seconds=0,
        now="2026-01-01T00:00:05+00:00",
    )
    deferred = defer_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=deferred_lease["ref"],
        delay_seconds=0,
        now="2026-01-01T00:00:06+00:00",
    )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:09+00:00",
    )
    acknowledged = ack_connector_delivery(
        db_path,
        host_id=host_id,
        name="attention",
        ref=canonical["ref"],
        now="2026-01-01T00:00:10+00:00",
    )
    after = poll_connector_outbox(
        db_path,
        host_id,
        "attention",
        now="2026-01-01T00:00:11+00:00",
    )

    assert failed["status"] == "superseded"
    assert failed["key"] == failed_lease["key"]
    assert deferred["status"] == "superseded"
    assert deferred["key"] == deferred_lease["key"]
    assert reclaimed["reclaimed"] == 1
    assert acknowledged["status"] == "acknowledged"
    assert acknowledged["key"] == canonical["key"]
    assert after["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        outbox_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        delivery_rows = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_deliveries
            WHERE host_id = ? AND connector = 'attention'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()

    assert outbox_rows == [
        (canonical["key"], "delivered"),
        (failed_lease["key"], "superseded"),
        (deferred_lease["key"], "superseded"),
        (expiring_lease["key"], "superseded"),
    ]
    assert delivery_rows == [
        (canonical["key"], "delivered"),
        (failed_lease["key"], "failed"),
        (deferred_lease["key"], "deferred"),
        (expiring_lease["key"], "expired"),
    ]


def _canonical_turn(
    db_path: Path,
    *,
    host_id: str = "host-a",
    worker_id: str = "worker-prepare",
    source_turn_id: str = "source-prepare",
    final_text: str,
    user_text: str | None = None,
) -> tuple[str, str]:
    init_store(db_path)
    turn_id = f"turn-{worker_id}-{source_turn_id}"
    user_state = "complete" if user_text is not None else "absent"
    revision = store_sqlite.content_revision(
        turn_id,
        user_text,
        final_text,
        user_state,
        "complete",
    )
    created_at = "2026-01-01T00:00:00+00:00"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO turns (
                host_id, turn_id, worker_id, worker_fingerprint, space_id,
                status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json
            ) VALUES (?, ?, ?, NULL, NULL, 'complete', 'turn', ?, ?, ?, ?, ?)
            """,
            (
                host_id,
                turn_id,
                worker_id,
                created_at,
                f"fingerprint-{worker_id}",
                f"snapshot-{worker_id}",
                created_at,
                json.dumps({"source_turn_id": source_turn_id, "complete": True}),
            ),
        )
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 0, superseded_at = ?
            WHERE host_id = ? AND turn_id = ?
              AND content_revision != ? AND is_current = 1
            """,
            (created_at, host_id, turn_id, revision),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO turn_content_revisions (
                host_id, turn_id, content_revision,
                user_text, assistant_final_text, user_state, final_state,
                user_char_length, user_byte_length,
                final_char_length, final_byte_length,
                user_page_count, final_page_count,
                is_current, created_at, superseded_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'complete', ?, ?, ?, ?, ?, ?, 1, ?, NULL)
            """,
            (
                host_id,
                turn_id,
                revision,
                user_text,
                final_text,
                user_state,
                len(user_text or ""),
                len((user_text or "").encode("utf-8")),
                len(final_text),
                len(final_text.encode("utf-8")),
                (
                    len(store_sqlite.segment_canonical_text(user_text))
                    if user_text is not None
                    else 0
                ),
                len(store_sqlite.segment_canonical_text(final_text)),
                created_at,
            ),
        )
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 1, superseded_at = NULL
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, turn_id, revision),
        )
    return turn_id, revision


def _begin_plan(
    api: ConnectorOutboxAPI,
    *,
    turn_id: str,
    revision: str,
    part_count: int,
    version: str = "turn-present-v27",
) -> dict[str, Any]:
    return api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": turn_id,
            "content_revision": revision,
            "presentation_version": version,
            "part_count": part_count,
        }
    )


def _put_final_part(
    api: ConnectorOutboxAPI,
    *,
    plan_token: str,
    ordinal: int,
    start: int,
    end: int,
) -> dict[str, Any]:
    return api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": "turn-final",
            "plan_token": plan_token,
            "ordinal": ordinal,
            "spans": [
                {
                    "field": "assistant_final_text",
                    "start_char": start,
                    "end_char": end,
                }
            ],
        }
    )


def _commit_plan(api: ConnectorOutboxAPI, plan_token: str) -> dict[str, Any]:
    return api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": "turn-final",
            "plan_token": plan_token,
        }
    )


def _stage_final_plan(
    api: ConnectorOutboxAPI,
    *,
    turn_id: str,
    revision: str,
    ranges: list[tuple[int, int]],
    version: str = "turn-present-v27",
) -> dict[str, Any]:
    begun = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=len(ranges),
        version=version,
    )
    assert begun["ok"] is True
    token = begun["plan_token"]
    for ordinal, (start, end) in enumerate(ranges):
        assert _put_final_part(
            api,
            plan_token=token,
            ordinal=ordinal,
            start=start,
            end=end,
        )["ok"] is True
    return _commit_plan(api, token)


def _drain_turn_final(api: ConnectorOutboxAPI) -> list[str]:
    keys: list[str] = []
    while True:
        items = api.poll({"name": "turn-final", "limit": 100})["items"]
        if not items:
            return keys
        assert len(items) == 1
        item = items[0]
        keys.append(item["key"])
        assert api.ack({"name": "turn-final", "ref": item["ref"]})["ok"] is True


def _downgrade_presentation_schema_to_v6(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(
            """
            CREATE TABLE turn_presentation_plans_v6 (
                id INTEGER PRIMARY KEY,
                host_id TEXT NOT NULL,
                name TEXT NOT NULL,
                plan_token TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                content_revision TEXT NOT NULL,
                presentation_version TEXT NOT NULL,
                part_count INTEGER NOT NULL CHECK (part_count > 0),
                state TEXT NOT NULL,
                replaces_plan_token TEXT,
                created_at TEXT NOT NULL,
                activated_at TEXT,
                completed_at TEXT,
                UNIQUE (host_id, name, plan_token),
                UNIQUE (
                    host_id, name, turn_id, content_revision, presentation_version
                ),
                FOREIGN KEY (host_id, turn_id, content_revision)
                    REFERENCES turn_content_revisions(
                        host_id, turn_id, content_revision
                    ) ON DELETE RESTRICT
            );
            INSERT INTO turn_presentation_plans_v6 (
                id, host_id, name, plan_token, turn_id, content_revision,
                presentation_version, part_count, state, replaces_plan_token,
                created_at, activated_at, completed_at
            )
            SELECT
                id, host_id, name, plan_token, turn_id, content_revision,
                presentation_version, part_count, state, replaces_plan_token,
                created_at, activated_at, completed_at
            FROM turn_presentation_plans;

            CREATE TABLE turn_presentation_jobs_v6 (
                id INTEGER PRIMARY KEY,
                plan_id INTEGER NOT NULL,
                sequence_index INTEGER NOT NULL CHECK (sequence_index >= 0),
                operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retire')),
                part_ordinal INTEGER NOT NULL CHECK (part_ordinal >= 0),
                spans_json TEXT NOT NULL,
                outbox_id INTEGER UNIQUE,
                created_at TEXT NOT NULL,
                UNIQUE (plan_id, sequence_index),
                UNIQUE (plan_id, operation, part_ordinal),
                FOREIGN KEY (plan_id)
                    REFERENCES turn_presentation_plans_v6(id) ON DELETE CASCADE,
                FOREIGN KEY (outbox_id)
                    REFERENCES connector_outbox(id) ON DELETE RESTRICT
            );
            INSERT INTO turn_presentation_jobs_v6 (
                id, plan_id, sequence_index, operation, part_ordinal,
                spans_json, outbox_id, created_at
            )
            SELECT
                id, plan_id, sequence_index, operation, part_ordinal,
                spans_json, outbox_id, created_at
            FROM turn_presentation_jobs;

            DROP TABLE turn_presentation_recoveries;
            DROP TABLE turn_presentation_jobs;
            DROP TABLE turn_presentation_plans;
            ALTER TABLE turn_presentation_plans_v6
                RENAME TO turn_presentation_plans;
            ALTER TABLE turn_presentation_jobs_v6
                RENAME TO turn_presentation_jobs;
            PRAGMA user_version = 6;
            """
        )


def test_v6_to_v7_plan_migration_is_bounded_atomic_and_preserves_jobs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "presentation-v6.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a")
    plan = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8)],
    )
    _downgrade_presentation_schema_to_v6(db_path)

    init_store(db_path)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        plan_row = conn.execute(
            """
            SELECT plan_token, generation, recovers_plan_token, state
            FROM turn_presentation_plans
            """
        ).fetchone()
        job_count = conn.execute(
            "SELECT COUNT(*) FROM turn_presentation_jobs"
        ).fetchone()[0]
        outbox_count = conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0]
        audit_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_presentation_recoveries)"
            ).fetchall()
        }
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert version == store_sqlite.STORE_SCHEMA_VERSION == 7
    assert plan_row == (plan["plan_token"], 1, None, "active")
    assert job_count == outbox_count == 2
    assert {
        "request_id",
        "failed_plan_id",
        "recovered_plan_id",
        "generation",
        "delivered_prefix_count",
        "fresh_job_count",
        "retained_failed_job_count",
        "prior_attempt_count",
    } <= audit_columns
    assert foreign_keys == []

    _downgrade_presentation_schema_to_v6(db_path)

    def fail_rebuild(_conn: sqlite3.Connection) -> None:
        raise RuntimeError("controlled v7 migration failure")

    monkeypatch.setattr(
        store_sqlite,
        "_rebuild_v6_presentation_plans_conn",
        fail_rebuild,
    )
    with pytest.raises(RuntimeError, match="controlled v7 migration failure"):
        init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
        assert "generation" not in {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_presentation_plans)"
            ).fetchall()
        }
        assert conn.execute(
            "SELECT COUNT(*) FROM turn_presentation_jobs"
        ).fetchone()[0] == 2


def test_prepare_stages_idempotently_and_rejects_conflicts_or_incomplete_coverage(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-idempotent.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefghij")
    api = ConnectorOutboxAPI(db_path, "host-a")

    first = _begin_plan(api, turn_id=turn_id, revision=revision, part_count=2)
    repeated = _begin_plan(api, turn_id=turn_id, revision=revision, part_count=2)
    conflicting_header = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=3,
    )
    token = first["plan_token"]
    part = _put_final_part(api, plan_token=token, ordinal=0, start=0, end=5)
    repeated_part = _put_final_part(
        api,
        plan_token=token,
        ordinal=0,
        start=0,
        end=5,
    )
    conflicting_part = _put_final_part(
        api,
        plan_token=token,
        ordinal=0,
        start=0,
        end=4,
    )
    incomplete = _commit_plan(api, token)

    assert first == repeated
    assert first["state"] == "preparing"
    assert conflicting_header["status"] == "plan_conflict"
    assert part["accepted_parts"] == repeated_part["accepted_parts"] == 1
    assert conflicting_part["status"] == "plan_conflict"
    assert incomplete["status"] == "plan_incomplete"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == 0

    _put_final_part(api, plan_token=token, ordinal=1, start=5, end=10)
    committed = _commit_plan(api, token)
    repeated_commit = _commit_plan(api, token)
    assert committed["state"] == "active"
    assert committed["job_count"] == 2
    assert repeated_commit == committed
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == 2


def test_prepare_validation_is_strict_and_backend_neutral(tmp_path: Path) -> None:
    db_path = tmp_path / "prepare-validation.db"
    turn_id, revision = _canonical_turn(db_path, final_text="complete")
    api = ConnectorOutboxAPI(db_path, "host-a")
    base = {
        "schema_version": 1,
        "action": "begin",
        "name": "turn-final",
        "turn_id": turn_id,
        "content_revision": revision,
        "presentation_version": "turn-present-v27",
        "part_count": 1,
    }

    invalid = [
        api.prepare({**base, "schema_version": True}),
        api.prepare({**base, "part_count": 0}),
        api.prepare({**base, "part_count": 10_001}),
        api.prepare({**base, "presentation_version": "telegram-rich-v1"}),
        api.prepare({**base, "text": "must never enter staging"}),
        api.prepare({**base, "name": "attention"}),
    ]
    for result in invalid:
        assert result["ok"] is False
        assert result["status"] == "invalid_params"
        _assert_no_forbidden(result)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_presentation_plans").fetchone()[0] == 0


def test_prepare_commit_rolls_back_all_materialization_on_injected_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "prepare-rollback.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a")
    begun = _begin_plan(api, turn_id=turn_id, revision=revision, part_count=2)
    token = begun["plan_token"]
    _put_final_part(api, plan_token=token, ordinal=0, start=0, end=4)
    _put_final_part(api, plan_token=token, ordinal=1, start=4, end=8)

    original = store_sqlite._materialize_connector_plan_job_conn
    calls = 0

    def fail_second(*args: Any, **kwargs: Any) -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("controlled materialization failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_materialize_connector_plan_job_conn",
        fail_second,
    )
    with pytest.raises(RuntimeError, match="controlled materialization failure"):
        _commit_plan(api, token)

    with sqlite3.connect(str(db_path)) as conn:
        state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (token,),
        ).fetchone()[0]
        outbox_count = conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0]
        linked_count = conn.execute(
            "SELECT COUNT(*) FROM turn_presentation_jobs WHERE outbox_id IS NOT NULL"
        ).fetchone()[0]
    assert state == "preparing"
    assert outbox_count == linked_count == 0


def test_prepare_commit_rechecks_current_revision_and_creates_no_jobs_on_conflict(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-revision-conflict.db"
    turn_id, revision = _canonical_turn(db_path, final_text="original")
    api = ConnectorOutboxAPI(db_path, "host-a")
    begun = _begin_plan(api, turn_id=turn_id, revision=revision, part_count=1)
    token = begun["plan_token"]
    _put_final_part(api, plan_token=token, ordinal=0, start=0, end=8)

    replacement_turn, replacement_revision = _canonical_turn(
        db_path,
        final_text="replacement",
    )
    assert replacement_turn == turn_id
    assert replacement_revision != revision
    conflict = _commit_plan(api, token)
    stale_begin = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=1,
        version="turn-present-v28",
    )

    assert conflict["status"] == "revision_conflict"
    assert stale_begin["status"] == "revision_conflict"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == 0


def test_ordered_jobs_gate_siblings_and_retry_only_current_sequence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-ordering.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefghijkl")
    api = ConnectorOutboxAPI(db_path, "host-a")
    committed = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8), (8, 12)],
    )
    token = committed["plan_token"]
    _enqueue(db_path, key="attention-still-independent")

    first_poll = api.poll({"name": "turn-final", "limit": 10})
    first = first_poll["items"][0]
    assert len(first_poll["items"]) == 1
    assert first["key"] == f"turn-final:{token}:000000"
    assert first["payload"]["plan_token"] == token
    assert first["payload"]["replaces_plan_token"] is None
    assert api.poll({"name": "attention", "limit": 10})["items"][0]["key"] == (
        "attention-still-independent"
    )

    failed = api.fail(
        {"name": "turn-final", "ref": first["ref"], "delay_seconds": 0}
    )
    retry = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert failed["status"] == "retry_scheduled"
    assert retry["key"] == first["key"]
    assert retry["ref"] != first["ref"]
    assert retry["attempt"] == 2
    deferred = api.defer(
        {"name": "turn-final", "ref": retry["ref"], "delay_seconds": 0}
    )
    after_defer = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert deferred["status"] == "deferred"
    assert after_defer["key"] == first["key"]

    reclaimed = reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "turn-final",
        now="9999-01-01T00:00:00+00:00",
    )
    after_reclaim = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert reclaimed["reclaimed"] == 1
    assert after_reclaim["key"] == first["key"]
    assert api.ack({"name": "turn-final", "ref": after_reclaim["ref"]})["ok"] is True

    second_poll = api.poll({"name": "turn-final", "limit": 10})
    assert len(second_poll["items"]) == 1
    assert second_poll["items"][0]["key"] == f"turn-final:{token}:000001"
    api.ack({"name": "turn-final", "ref": second_poll["items"][0]["ref"]})
    third_poll = api.poll({"name": "turn-final", "limit": 10})
    assert len(third_poll["items"]) == 1
    assert third_poll["items"][0]["key"] == f"turn-final:{token}:000002"
    api.ack({"name": "turn-final", "ref": third_poll["items"][0]["ref"]})
    assert api.poll({"name": "turn-final", "limit": 10})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (token,),
        ).fetchone()[0] == "completed"


def test_exhaustion_fails_plan_and_never_unlocks_successor(tmp_path: Path) -> None:
    db_path = tmp_path / "prepare-exhaustion.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    committed = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8)],
    )
    first = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    exhausted = api.fail(
        {"name": "turn-final", "ref": first["ref"], "delay_seconds": 0}
    )

    assert exhausted["status"] == "attempts_exhausted"
    assert api.poll({"name": "turn-final", "limit": 10})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        plan_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (committed["plan_token"],),
        ).fetchone()[0]
        statuses = conn.execute(
            """
            SELECT outbox.status
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (committed["plan_token"],),
        ).fetchall()
    assert plan_state == "failed"
    assert statuses == [("dead_letter",), ("queued",)]


def test_explicit_failed_plan_recovery_preserves_prefix_and_audits_generation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-explicit-recovery.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefghijkl")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    failed_plan = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8), (8, 12)],
    )
    first = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert api.ack({"name": "turn-final", "ref": first["ref"]})["ok"] is True
    second = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert api.fail(
        {"name": "turn-final", "ref": second["ref"], "delay_seconds": 0}
    )["status"] == "attempts_exhausted"

    request = {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": failed_plan["plan_token"],
        "request_id": "recover-request-1",
    }
    recovered = api.prepare(request)
    replay = api.prepare(request)
    expected_keys = {
        "schema_version",
        "ok",
        "status",
        "failed_plan_token",
        "plan_token",
        "generation",
        "content_revision",
        "state",
        "acknowledged_prefix_count",
        "executable_job_count",
        "retained_failed_job_count",
        "prior_attempt_count",
        "idempotent_replay",
    }
    assert set(recovered) == expected_keys
    _assert_no_forbidden(recovered)
    assert recovered == {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": failed_plan["plan_token"],
        "plan_token": recovered["plan_token"],
        "generation": 2,
        "content_revision": revision,
        "state": "active",
        "acknowledged_prefix_count": 1,
        "executable_job_count": 2,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 2,
        "idempotent_replay": False,
    }
    assert replay == {**recovered, "idempotent_replay": True}
    assert recovered["plan_token"] != failed_plan["plan_token"]

    recovered_second = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert recovered_second["payload"]["sequence_index"] == 1
    assert recovered_second["payload"]["plan_token"] == recovered["plan_token"]
    assert recovered_second["payload"]["predecessor_job_key"] == first["key"]
    assert api.ack(
        {"name": "turn-final", "ref": recovered_second["ref"]}
    )["ok"] is True
    recovered_third = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert recovered_third["payload"]["sequence_index"] == 2
    assert api.ack(
        {"name": "turn-final", "ref": recovered_third["ref"]}
    )["ok"] is True
    assert api.poll({"name": "turn-final", "limit": 10})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        source_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (failed_plan["plan_token"],),
        ).fetchone()[0]
        source_statuses = conn.execute(
            """
            SELECT outbox.status
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (failed_plan["plan_token"],),
        ).fetchall()
        recovered_state = conn.execute(
            """
            SELECT state, generation, recovers_plan_token
            FROM turn_presentation_plans
            WHERE plan_token = ?
            """,
            (recovered["plan_token"],),
        ).fetchone()
        audit = conn.execute(
            """
            SELECT
                request_id, failed_plan_token, recovered_plan_token,
                generation, source_job_count, delivered_prefix_count,
                fresh_job_count, retained_failed_job_count,
                prior_attempt_count, outcome
            FROM turn_presentation_recoveries
            """
        ).fetchone()
    assert source_state == "failed"
    assert source_statuses == [("delivered",), ("dead_letter",), ("queued",)]
    assert recovered_state == ("completed", 2, failed_plan["plan_token"])
    assert audit == (
        "recover-request-1",
        failed_plan["plan_token"],
        recovered["plan_token"],
        2,
        3,
        1,
        2,
        1,
        2,
        "recovered",
    )

    conflict = api.prepare({**request, "request_id": "recover-request-2"})
    assert conflict["ok"] is False
    assert conflict["status"] == "plan_conflict"


def test_recovery_can_advance_another_bounded_failed_generation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-recovery-generations.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefghijkl")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    generation_one = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8), (8, 12)],
    )
    first = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    api.ack({"name": "turn-final", "ref": first["ref"]})
    failed_second = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    api.fail({"name": "turn-final", "ref": failed_second["ref"], "delay_seconds": 0})
    generation_two = api.prepare(
        {
            "schema_version": 1,
            "action": "recover",
            "name": "turn-final",
            "failed_plan_token": generation_one["plan_token"],
            "request_id": "recover-generation-2",
        }
    )
    failed_again = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    api.fail({"name": "turn-final", "ref": failed_again["ref"], "delay_seconds": 0})
    generation_three = api.prepare(
        {
            "schema_version": 1,
            "action": "recover",
            "name": "turn-final",
            "failed_plan_token": generation_two["plan_token"],
            "request_id": "recover-generation-3",
        }
    )

    assert generation_three["generation"] == 3
    assert generation_three["content_revision"] == revision
    assert generation_three["acknowledged_prefix_count"] == 1
    assert generation_three["executable_job_count"] == 2
    assert generation_three["retained_failed_job_count"] == 2
    assert generation_three["prior_attempt_count"] == 3
    resumed = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert resumed["payload"]["sequence_index"] == 1
    assert resumed["payload"]["predecessor_job_key"] == first["key"]


def test_concurrent_recovery_request_creates_one_generation_and_one_audit(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-recovery-concurrency.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    failed = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8)],
    )
    leased = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    api.fail({"name": "turn-final", "ref": leased["ref"], "delay_seconds": 0})
    request = {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": failed["plan_token"],
        "request_id": "recover-concurrently",
    }
    barrier = Barrier(4)

    def recover_once() -> dict[str, Any]:
        barrier.wait()
        return ConnectorOutboxAPI(
            db_path,
            "host-a",
            max_attempts=1,
        ).prepare(request)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _index: recover_once(), range(4)))

    assert {result["plan_token"] for result in results} == {
        results[0]["plan_token"]
    }
    assert sum(not result["idempotent_replay"] for result in results) == 1
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM turn_presentation_recoveries"
        ).fetchone()[0] == 1
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_presentation_plans
            WHERE content_revision = ? AND generation = 2
            """,
            (revision,),
        ).fetchone()[0] == 1


def test_unrelated_plans_poll_concurrently_but_never_colease_siblings(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-unrelated.db"
    first_turn, first_revision = _canonical_turn(
        db_path,
        worker_id="worker-one",
        source_turn_id="source-one",
        final_text="abcdefgh",
    )
    second_turn, second_revision = _canonical_turn(
        db_path,
        worker_id="worker-two",
        source_turn_id="source-two",
        final_text="ijklmnop",
    )
    api = ConnectorOutboxAPI(db_path, "host-a")
    first_plan = _stage_final_plan(
        api,
        turn_id=first_turn,
        revision=first_revision,
        ranges=[(0, 4), (4, 8)],
    )
    second_plan = _stage_final_plan(
        api,
        turn_id=second_turn,
        revision=second_revision,
        ranges=[(0, 4), (4, 8)],
    )

    items = api.poll({"name": "turn-final", "limit": 10})["items"]
    assert len(items) == 2
    assert {item["payload"]["plan_token"] for item in items} == {
        first_plan["plan_token"],
        second_plan["plan_token"],
    }
    assert {item["payload"]["sequence_index"] for item in items} == {0}


def test_replacement_waits_for_old_lease_then_activates_without_requeue(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-replacement-barrier.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a")
    old = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v26",
    )
    leased_old = api.poll({"name": "turn-final", "limit": 10})["items"][0]

    replacement_turn, replacement_revision = _canonical_turn(
        db_path,
        final_text="ABCDEFGH",
    )
    assert replacement_turn == turn_id
    new = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=replacement_revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v27",
    )
    assert new["state"] == "waiting_predecessor"
    assert api.poll({"name": "turn-final", "limit": 10})["items"] == []

    terminalized = api.fail(
        {"name": "turn-final", "ref": leased_old["ref"], "delay_seconds": 0}
    )
    assert terminalized["status"] == "superseded"
    activated = api.poll({"name": "turn-final", "limit": 10})["items"]
    assert len(activated) == 1
    assert activated[0]["payload"]["plan_token"] == new["plan_token"]
    assert activated[0]["payload"]["replaces_plan_token"] == old["plan_token"]
    with sqlite3.connect(str(db_path)) as conn:
        old_statuses = conn.execute(
            """
            SELECT outbox.status
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (old["plan_token"],),
        ).fetchall()
        new_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (new["plan_token"],),
        ).fetchone()[0]
    assert old_statuses == [("superseded",), ("superseded",)]
    assert new_state == "active"


def test_commit_requires_exact_contiguous_ordered_coverage_of_selected_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-coverage.db"
    turn_id, revision = _canonical_turn(
        db_path,
        user_text="prompt",
        final_text="abcdefghij",
    )
    api = ConnectorOutboxAPI(db_path, "host-a")

    gap = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=2,
        version="turn-present-gap-v1",
    )
    _put_final_part(
        api,
        plan_token=gap["plan_token"],
        ordinal=0,
        start=0,
        end=4,
    )
    _put_final_part(
        api,
        plan_token=gap["plan_token"],
        ordinal=1,
        start=5,
        end=10,
    )
    assert _commit_plan(api, gap["plan_token"])["status"] == "plan_incomplete"

    overlap = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=2,
        version="turn-present-overlap-v1",
    )
    _put_final_part(
        api,
        plan_token=overlap["plan_token"],
        ordinal=0,
        start=0,
        end=6,
    )
    _put_final_part(
        api,
        plan_token=overlap["plan_token"],
        ordinal=1,
        start=5,
        end=10,
    )
    assert _commit_plan(api, overlap["plan_token"])["status"] == "plan_incomplete"

    complete = _begin_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        part_count=2,
        version="turn-present-complete-v1",
    )
    first = api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": "turn-final",
            "plan_token": complete["plan_token"],
            "ordinal": 0,
            "spans": [
                {"field": "user_text", "start_char": 0, "end_char": 6},
                {
                    "field": "assistant_final_text",
                    "start_char": 0,
                    "end_char": 4,
                },
            ],
        }
    )
    second = _put_final_part(
        api,
        plan_token=complete["plan_token"],
        ordinal=1,
        start=4,
        end=10,
    )
    committed = _commit_plan(api, complete["plan_token"])
    assert first["ok"] is second["ok"] is committed["ok"] is True
    assert committed["state"] == "active"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == 2


def test_recovery_after_partial_failed_shrink_retains_applied_tail_footprint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-failed-shrink-recovery.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefghijklmnop")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    original = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8), (8, 12), (12, 16)],
        version="turn-present-v23",
    )
    assert len(_drain_turn_final(api)) == 4

    _, shrink_revision = _canonical_turn(db_path, final_text="ABCDEFGH")
    failed_shrink = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=shrink_revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v24",
    )
    first = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    api.ack({"name": "turn-final", "ref": first["ref"]})
    second = api.poll({"name": "turn-final", "limit": 10})["items"][0]
    assert api.fail(
        {"name": "turn-final", "ref": second["ref"], "delay_seconds": 0}
    )["status"] == "attempts_exhausted"

    _, recovery_revision = _canonical_turn(db_path, final_text="12345678")
    recovery = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=recovery_revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v25",
    )
    with sqlite3.connect(str(db_path)) as conn:
        recovery_rows = conn.execute(
            """
            SELECT jobs.sequence_index, jobs.operation, jobs.part_ordinal
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (recovery["plan_token"],),
        ).fetchall()
        states = dict(
            conn.execute(
                """
                SELECT plan_token, state
                FROM turn_presentation_plans
                WHERE plan_token IN (?, ?, ?)
                """,
                (
                    original["plan_token"],
                    failed_shrink["plan_token"],
                    recovery["plan_token"],
                ),
            ).fetchall()
        )
    assert recovery["job_count"] == 4
    assert recovery["state"] == "active"
    assert recovery_rows == [
        (0, "upsert", 0),
        (1, "upsert", 1),
        (2, "retire", 3),
        (3, "retire", 2),
    ]
    assert states[failed_shrink["plan_token"]] == "failed"
    assert states[recovery["plan_token"]] == "active"


def test_same_grow_shrink_materialization_and_range_only_storage(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-size-changes.db"
    sentinel = "CANONICAL-CONTENT-MUST-NOT-ENTER-PLAN-JOBS"
    turn_id, revision = _canonical_turn(db_path, final_text=sentinel)
    api = ConnectorOutboxAPI(db_path, "host-a")
    old = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 10), (10, 20), (20, 30), (30, len(sentinel))],
        version="turn-present-v24",
    )
    assert len(_drain_turn_final(api)) == 4

    _, shrink_revision = _canonical_turn(db_path, final_text="0123456789ABCDEF")
    shrink = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=shrink_revision,
        ranges=[(0, 8), (8, 16)],
        version="turn-present-v25",
    )
    with sqlite3.connect(str(db_path)) as conn:
        shrink_rows = conn.execute(
            """
            SELECT jobs.sequence_index, jobs.operation, jobs.part_ordinal
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (shrink["plan_token"],),
        ).fetchall()
    assert shrink_rows == [
        (0, "upsert", 0),
        (1, "upsert", 1),
        (2, "retire", 3),
        (3, "retire", 2),
    ]
    assert len(_drain_turn_final(api)) == 4

    _, grow_revision = _canonical_turn(db_path, final_text="abcdefghijklmnop")
    grow = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=grow_revision,
        ranges=[(0, 4), (4, 8), (8, 12), (12, 16)],
        version="turn-present-v26",
    )
    assert grow["job_count"] == 4
    assert len(_drain_turn_final(api)) == 4

    _, same_revision = _canonical_turn(db_path, final_text="ABCDEFGHIJKLMNOP")
    same = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=same_revision,
        ranges=[(0, 4), (4, 8), (8, 12), (12, 16)],
        version="turn-present-v27",
    )
    assert same["job_count"] == 4
    with sqlite3.connect(str(db_path)) as conn:
        same_rows = conn.execute(
            """
            SELECT jobs.sequence_index, jobs.operation, jobs.part_ordinal
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (same["plan_token"],),
        ).fetchall()
        stored_metadata = "\n".join(
            str(value)
            for row in conn.execute(
                """
                SELECT plan_token, presentation_version, replaces_plan_token
                FROM turn_presentation_plans
                UNION ALL
                SELECT operation, spans_json, ''
                FROM turn_presentation_jobs
                UNION ALL
                SELECT delivery_key, payload_json, private_state_json
                FROM connector_outbox
                WHERE connector = 'turn-final'
                """
            ).fetchall()
            for value in row
        )
    assert same_rows == [
        (0, "upsert", 0),
        (1, "upsert", 1),
        (2, "upsert", 2),
        (3, "upsert", 3),
    ]
    assert sentinel not in stored_metadata
    assert "user_text" not in stored_metadata
    assert "assistant_final_text" in stored_metadata


def test_lost_commit_response_retries_after_supersede_or_failure_without_duplicates(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prepare-lost-commit-response.db"
    turn_id, revision = _canonical_turn(db_path, final_text="abcdefgh")
    api = ConnectorOutboxAPI(db_path, "host-a", max_attempts=1)
    superseded_plan = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v30",
    )

    _, replacement_revision = _canonical_turn(db_path, final_text="ABCDEFGH")
    failed_plan = _stage_final_plan(
        api,
        turn_id=turn_id,
        revision=replacement_revision,
        ranges=[(0, 4), (4, 8)],
        version="turn-present-v31",
    )
    with sqlite3.connect(str(db_path)) as conn:
        before_retry_count = conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0]

    superseded_retry = _commit_plan(api, superseded_plan["plan_token"])
    assert superseded_retry["ok"] is True
    assert superseded_retry["state"] == "superseded"
    assert superseded_retry["job_count"] == 2
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == before_retry_count

    first_replacement_job = api.poll(
        {"name": "turn-final", "limit": 10}
    )["items"][0]
    assert api.fail(
        {
            "name": "turn-final",
            "ref": first_replacement_job["ref"],
            "delay_seconds": 0,
        }
    )["status"] == "attempts_exhausted"
    failed_retry = _commit_plan(api, failed_plan["plan_token"])

    assert failed_retry["ok"] is True
    assert failed_retry["state"] == "failed"
    assert failed_retry["job_count"] == 2
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'turn-final'"
        ).fetchone()[0] == before_retry_count
