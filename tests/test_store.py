"""Tests for the sqlite store contract."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from tendwire.core.commands import STATUS_ACCEPTED
from tendwire.config import Config
from tendwire.core.models import WorkerBinding
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    append_event,
    attention_payload_from_store,
    cleanup_event_retention,
    exhaust_connector_retries,
    expire_stale_worker_bindings,
    expire_worker_bindings,
    fail_connector_delivery,
    get_command_receipt,
    init_store,
    latest_snapshot,
    list_attention_items,
    list_hosts,
    list_worker_bindings,
    poll_connector_outbox,
    reserve_command_receipt,
    resolve_worker_binding,
    run_store_maintenance,
    save_command_receipt,
    save_snapshot,
    store_status,
    tail_event_metadata,
    merge_turn_content,
    turns_payload_from_store,
    upsert_worker_bindings,
)


_PR6_TABLES = {
    "events",
    "spaces",
    "workers",
    "worker_bindings",
    "turns",
    "pending_interactions",
    "attention_items",
    "commands",
    "command_receipts",
    "connector_outbox",
    "connector_deliveries",
    "backend_health",
}


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _indexed_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    columns: set[str] = set()
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        index_name = row[1]
        for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall():
            columns.add(index_row[2])
    return columns


def _unique_index_columns(conn: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if int(row[2]) != 1:
            continue
        index_name = str(row[1])
        columns = tuple(
            str(index_row[2])
            for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
        indexes[index_name] = columns
    return indexes


def test_store_initializes_v4_pr6_schema_with_attention_lifecycle_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
        assert _user_version(conn) == 4
        assert {"host_id", "created_at", "payload", "content_fingerprint"} <= columns
        indexed = _indexed_columns(conn, "snapshots")
        assert "host_id" in indexed
        assert "content_fingerprint" in indexed
        binding_columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_bindings)")}
        assert {
            "host_id",
            "worker_id",
            "worker_fingerprint",
            "backend",
            "target_kind",
            "target_value",
            "turn_target_kind",
            "turn_target_value",
            "sendable",
            "reason",
            "observed_at",
            "expires_at",
            "private_fingerprint",
        } <= binding_columns
        binding_indexed = _indexed_columns(conn, "worker_bindings")
        assert {
            "worker_id",
            "worker_fingerprint",
            "private_fingerprint",
            "target_kind",
            "target_value",
            "expires_at",
        } <= binding_indexed
        command_columns = {row[1] for row in conn.execute("PRAGMA table_info(commands)")}
        assert {
            "host_id",
            "request_id",
            "action",
            "payload_fingerprint",
            "status",
            "result_json",
            "uncertain",
        } <= command_columns
        attention_columns = {row[1] for row in conn.execute("PRAGMA table_info(attention_items)")}
        assert {
            "attention_id",
            "fingerprint",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "resolved_at",
            "lifecycle_status",
            "resolved_reason",
            "signal_count",
        } <= attention_columns
        attention_indexed = _indexed_columns(conn, "attention_items")
        assert {"lifecycle_status", "last_seen_at", "fingerprint"} <= attention_indexed


def test_store_connections_apply_wal_busy_timeout_and_foreign_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "pragmas.db"

    init_store(db_path)

    with store_sqlite._connect(db_path) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1


def test_store_command_receipts_have_unique_logical_key_index(tmp_path: Path) -> None:
    db_path = tmp_path / "receipts.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        indexes = _unique_index_columns(conn, "command_receipts")

    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


def test_store_status_tail_and_retention_cleanup_are_host_scoped_and_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "maintenance.db"
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "Worker One"}])
    save_snapshot(db_path, snapshot)
    append_event(
        db_path,
        "storehost",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "storehost",
        "public.event",
        {"safe": "kept"},
        observed_at="2026-01-09T00:00:00+00:00",
    )
    append_event(
        db_path,
        "otherhost",
        "other.event",
        {"safe": "kept"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-1",
                "queued",
                '{"safe":"kept"}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    before = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=2)
    dry_run = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
    )
    after_dry_run_count = store_status(db_path, "storehost")["counts"]["events"]
    cleanup = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        host_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("storehost",)).fetchone()[0]
        other_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("otherhost",)).fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(*) FROM snapshots WHERE host_id = ?", ("storehost",)).fetchone()[0]
        outbox_rows = conn.execute("SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?", ("storehost",)).fetchone()[0]

    assert before["ok"] is True
    assert before["outbox"]["pending"] == 1
    assert len(tail["events"]) == 2
    assert "payload_json" not in json.dumps(tail)
    assert "sentinel-private" not in json.dumps(tail)
    assert dry_run["deleted"] == 1
    assert after_dry_run_count == before["counts"]["events"]
    assert cleanup["deleted"] == 1
    assert host_events == before["counts"]["events"] - 1
    assert other_events == 1
    assert snapshots == 1
    assert outbox_rows == 1


def test_store_operational_metadata_buckets_unsafe_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-metadata.db"
    init_store(db_path)
    append_event(
        db_path,
        "storehost",
        "telegram.delivery",
        {"safe": "kept"},
        aggregate_type="raw_payload",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-unsafe",
                "telegram_delivery",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-queued",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    status = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=10)
    encoded = json.dumps({"status": status, "tail": tail}, sort_keys=True).lower()

    assert status["outbox"]["pending"] == 1
    assert status["outbox"]["by_status"]["queued"] == 1
    assert status["outbox"]["by_status"]["unknown"] == 1
    assert "telegram_delivery" not in status["outbox"]["by_status"]
    assert tail["events"][0]["event_type"] == "unknown"
    assert tail["events"][0]["aggregate_type"] == "unknown"
    assert "telegram" not in encoded
    assert "raw_payload" not in encoded
    assert "delivery" not in encoded


def test_attention_payload_from_store_buckets_unsafe_row_text(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-attention.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO attention_items (
                host_id, attention_id, source, kind, severity, status,
                updated_at, fingerprint, snapshot_content_fingerprint, observed_at,
                first_seen_at, last_seen_at, last_changed_at, lifecycle_status,
                resolved_reason, signal_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attn-unsafe",
                "telegram:chat",
                "herdres_delivery",
                "warning",
                "blocked",
                "2026-01-01T00:00:00+00:00",
                "fp-unsafe",
                "snapshot-fp",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "open",
                "telegram delivery resolved",
                1,
                json.dumps(
                    {
                        "reason": "telegram delivery token",
                        "meta": {"kept": "visible", "unsafe": "herdres route"},
                        "suggested_actions": [
                            {"label": "notify telegram", "tendwire_action": "noop"}
                        ],
                    }
                ),
            ),
        )

    payload = attention_payload_from_store(db_path, "storehost")
    assert payload is not None
    item = payload["attention"][0]
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert item["source"] == "unknown"
    assert item["kind"] == "unknown"
    assert item["reason"] == ""
    assert item["meta"] == {"kept": "visible"}
    assert "resolved_reason" not in item
    assert "telegram" not in encoded
    assert "herdres" not in encoded
    assert "delivery" not in encoded
    assert "token" not in encoded
    assert "route" not in encoded


def test_store_maintenance_dry_run_and_exhausted_outbox_status(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox-maintenance.db"
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
                "storehost",
                "attention",
                "job-1",
                "retry",
                '{"safe":"kept"}',
                '{}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "storehost",
                "attention",
                "job-1",
                3,
                "failed",
                '{}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:01:00+00:00",
            ),
        )

    dry_run = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_status = conn.execute("SELECT status FROM connector_outbox").fetchone()[0]
    real = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
    )
    with sqlite3.connect(str(db_path)) as conn:
        real_status, private_state = conn.execute(
            "SELECT status, private_state_json FROM connector_outbox"
        ).fetchone()

    assert dry_run["outbox"]["updated"] == 1
    assert dry_status == "retry"
    assert real["outbox"]["updated"] == 1
    assert real_status == "dead_letter"
    assert json.loads(private_state) == {}


def test_exhaust_connector_retries_reclaims_expired_leases_before_dead_letter(tmp_path: Path) -> None:
    db_path = tmp_path / "expired-maintenance.db"
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
                "storehost",
                "attention",
                "leased-job",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    first = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    fail_connector_delivery(
        db_path,
        host_id="storehost",
        name="attention",
        ref=first["ref"],
        delay_seconds=0,
        max_attempts=2,
        now="2026-01-01T00:00:01+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]

    dry_run = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_run_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
    result = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
        attempt_count, max_attempt = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(attempt), 0)
            FROM connector_deliveries
            WHERE delivery_key = ?
            """,
            ("leased-job",),
        ).fetchone()

    assert first["attempt"] == 1
    assert second["attempt"] == 2
    assert dry_run["updated"] == 1
    assert dry_run_status == "leased"
    assert result["updated"] == 1
    assert outbox_status == "dead_letter"
    assert (attempt_count, max_attempt) == (2, 2)


def test_store_migrates_v1_schema_and_persists_content_fingerprint(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            PRAGMA user_version = 1;
            """
        )

    init_store(db_path)
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )
    save_snapshot(db_path, snapshot)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == 4
        row = conn.execute(
            "SELECT host_id, content_fingerprint, payload FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row[0] == "storehost"
    assert row[1] == snapshot.content_fingerprint
    assert json.loads(row[2]) == json.loads(snapshot.to_json())
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "storehost"
    assert restored.content_fingerprint == snapshot.content_fingerprint


def test_store_migrates_partial_v3_db_with_legacy_data_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "partial-v3.db"
    snapshot = project_empty(Config(host_id="legacy-host", db_path=db_path))
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE worker_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_fingerprint TEXT NOT NULL,
                backend TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_value TEXT NOT NULL,
                turn_target_kind TEXT,
                turn_target_value TEXT,
                sendable INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                observed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                private_fingerprint TEXT NOT NULL
            );
            PRAGMA user_version = 3;
            """
        )
        conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.host_id,
                snapshot.updated_at,
                snapshot.content_fingerprint,
                snapshot.to_json(),
            ),
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "legacy-req",
                "send_instruction",
                "legacy-fp",
                STATUS_ACCEPTED,
                '{"status":"accepted"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                0,
            ),
        )
        conn.execute(
            """
            INSERT INTO worker_bindings (
                host_id, worker_id, worker_fingerprint, backend, target_kind,
                target_value, sendable, reason, observed_at, expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "worker-legacy",
                "worker-fp",
                "herdr",
                "agent_id",
                "agent-private",
                1,
                None,
                "2026-01-01T00:00:00+00:00",
                "9999-12-31T23:59:59+00:00",
                "legacy-private",
            ),
        )

    init_store(db_path)
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        assert _user_version(conn) == 4
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM worker_bindings").fetchone()[0] == 1
        command = conn.execute(
            """
            SELECT status, payload_fingerprint, result_json
            FROM commands
            WHERE host_id = 'legacy-host'
              AND request_id = 'legacy-req'
              AND action = 'send_instruction'
            """
        ).fetchone()

    assert command == (STATUS_ACCEPTED, "legacy-fp", '{"status":"accepted"}')


def _snapshot_with_worker_status(
    config: Config,
    *,
    status: str | None,
    observed_at: str,
    health_status: str = "healthy",
    outcome: str = "healthy_non_empty",
) -> Any:
    workers = []
    if status is not None:
        workers = [
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": status,
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "terminalId": "sentinel-private-terminal",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ]
    return project_from_raw(
        config,
        workers=workers,
        backend_health=[
            {
                "name": "herdr",
                "status": health_status,
                "outcome": outcome,
                "observed_at": observed_at,
                "counts": {"workers": len(workers)},
            }
        ],
        timestamp=datetime.fromisoformat(observed_at),
    )


def _connector_outbox_rows(db_path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT connector, delivery_key, payload_json
            FROM connector_outbox
            ORDER BY id
            """
        ).fetchall()


def test_store_migrates_legacy_attention_rows_with_lifecycle_backfill(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-attention.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE attention_items (
                host_id TEXT NOT NULL,
                attention_id TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT,
                fingerprint TEXT NOT NULL,
                snapshot_content_fingerprint TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (host_id, attention_id)
            );
            INSERT INTO attention_items (
                host_id, attention_id, source, kind, severity, status,
                updated_at, fingerprint, snapshot_content_fingerprint,
                observed_at, payload_json
            ) VALUES (
                'legacy-host', 'attn-legacy', 'worker:legacy', 'worker_status',
                'warning', 'blocked', NULL, 'fp-legacy', 'snapshot-fp',
                '2026-01-01T00:00:00+00:00',
                '{"id":"attn-legacy","kind":"worker_status","severity":"warning","status":"blocked","source":"worker:legacy","fingerprint":"fp-legacy"}'
            );
            PRAGMA user_version = 3;
            """
        )

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(attention_items)")}
        row = conn.execute(
            """
            SELECT first_seen_at, last_seen_at, last_changed_at,
                   resolved_at, lifecycle_status, resolved_reason, signal_count,
                   payload_json
            FROM attention_items
            WHERE host_id = 'legacy-host' AND attention_id = 'attn-legacy'
            """
        ).fetchone()

    assert {
        "first_seen_at",
        "last_seen_at",
        "last_changed_at",
        "resolved_at",
        "lifecycle_status",
        "resolved_reason",
        "signal_count",
    } <= columns
    assert row[:7] == (
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:00:00+00:00",
        None,
        "open",
        None,
        1,
    )
    assert json.loads(row[7])["id"] == "attn-legacy"


def test_store_attention_lifecycle_repeats_update_seen_without_duplicate_rows_or_outbox(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-repeat.db"
    config = Config(host_id="attention-host", db_path=db_path)
    first = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    repeated = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at="2026-01-01T00:05:00+00:00",
    )

    save_snapshot(db_path, first)
    first_rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    save_snapshot(db_path, repeated)
    rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    payload = attention_payload_from_store(db_path, "attention-host")
    outbox_rows = _connector_outbox_rows(db_path)

    assert len(first_rows) == 1
    assert len(rows) == 1
    assert rows[0]["id"] == first_rows[0]["id"]
    assert rows[0]["fingerprint"] == first_rows[0]["fingerprint"]
    assert rows[0]["lifecycle_status"] == "open"
    assert rows[0]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert rows[0]["last_seen_at"] == "2026-01-01T00:05:00+00:00"
    assert rows[0]["last_changed_at"] == "2026-01-01T00:00:00+00:00"
    assert rows[0]["resolved_at"] is None
    assert rows[0]["signal_count"] == 2
    assert payload is not None
    assert payload["attention"][0]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert len(outbox_rows) == 1
    assert outbox_rows[0][0] == "attention"
    assert "attention_created" in outbox_rows[0][1]
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    assert "sentinel-private" not in outbox_rows[0][2]


def test_store_attention_payload_synthesizes_snapshot_attention_without_lifecycle_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-snapshot-only.db"
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM attention_items WHERE host_id = ?", ("attention-host",))

    rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    payload = attention_payload_from_store(db_path, "attention-host")

    assert rows == []
    assert payload is not None
    assert len(payload["attention"]) == 1
    item = payload["attention"][0]
    assert item["id"] == snapshot.attention[0].id
    assert item["fingerprint"] == snapshot.attention[0].fingerprint
    assert item["status"] == "blocked"
    assert item["lifecycle_status"] == "open"
    assert item["first_seen_at"] == snapshot.updated_at
    assert item["last_seen_at"] == snapshot.updated_at
    assert item["last_changed_at"] == snapshot.updated_at
    assert item["signal_count"] == 1
    assert item["resolved_at"] is None
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)


def test_store_attention_escalation_resolves_old_signal_and_dedups_repeats(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-escalation.db"
    config = Config(host_id="attention-host", db_path=db_path)

    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="blocked",
            observed_at="2026-01-01T00:00:00+00:00",
        ),
    )
    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="failed",
            observed_at="2026-01-01T00:10:00+00:00",
        ),
    )
    rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    current = list_attention_items(db_path, "attention-host")
    outbox_rows = _connector_outbox_rows(db_path)

    assert len(rows) == 2
    assert len(current) == 1
    assert current[0]["severity"] == "critical"
    assert current[0]["status"] == "failed"
    assert current[0]["first_seen_at"] == "2026-01-01T00:10:00+00:00"
    assert current[0]["last_changed_at"] == "2026-01-01T00:10:00+00:00"
    resolved = [row for row in rows if row["lifecycle_status"] == "resolved"]
    assert len(resolved) == 1
    assert resolved[0]["severity"] == "warning"
    assert resolved[0]["status"] == "blocked"
    assert resolved[0]["resolved_at"] == "2026-01-01T00:10:00+00:00"
    assert resolved[0]["resolved_reason"] == "gone"
    assert len(outbox_rows) == 2
    assert "attention_created" in outbox_rows[0][1]
    assert "attention_escalated" in outbox_rows[1][1]

    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="failed",
            observed_at="2026-01-01T00:20:00+00:00",
        ),
    )
    repeated_current = list_attention_items(db_path, "attention-host")
    assert repeated_current[0]["signal_count"] == 2
    assert repeated_current[0]["last_seen_at"] == "2026-01-01T00:20:00+00:00"
    assert repeated_current[0]["last_changed_at"] == "2026-01-01T00:10:00+00:00"
    assert len(_connector_outbox_rows(db_path)) == 2


def test_store_attention_disappearance_resolves_only_from_authoritative_healthy_snapshot(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-disappearance.db"
    config = Config(host_id="attention-host", db_path=db_path)

    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="blocked",
            observed_at="2026-01-01T00:00:00+00:00",
        ),
    )
    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status=None,
            observed_at="2026-01-01T00:05:00+00:00",
            health_status="degraded",
            outcome="malformed_json",
        ),
    )
    degraded_rows = list_attention_items(db_path, "attention-host", include_resolved=True)

    assert len(degraded_rows) == 1
    assert degraded_rows[0]["lifecycle_status"] == "open"
    assert degraded_rows[0]["last_changed_at"] == "2026-01-01T00:00:00+00:00"
    assert degraded_rows[0]["resolved_at"] is None

    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status=None,
            observed_at="2026-01-01T00:10:00+00:00",
            outcome="empty_healthy",
        ),
    )
    resolved_rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    current_rows = list_attention_items(db_path, "attention-host")

    assert current_rows == []
    assert len(resolved_rows) == 1
    assert resolved_rows[0]["lifecycle_status"] == "resolved"
    assert resolved_rows[0]["resolved_at"] == "2026-01-01T00:10:00+00:00"
    assert resolved_rows[0]["resolved_reason"] == "gone"
    assert resolved_rows[0]["last_changed_at"] == "2026-01-01T00:10:00+00:00"


def test_store_attention_recreate_enqueues_new_lifecycle_delivery(tmp_path: Path) -> None:
    db_path = tmp_path / "attention-recreate.db"
    config = Config(host_id="attention-host", db_path=db_path)

    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="blocked",
            observed_at="2026-01-01T00:00:00+00:00",
        ),
    )
    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status=None,
            observed_at="2026-01-01T00:05:00+00:00",
            outcome="empty_healthy",
        ),
    )
    save_snapshot(
        db_path,
        _snapshot_with_worker_status(
            config,
            status="blocked",
            observed_at="2026-01-01T00:10:00+00:00",
        ),
    )

    rows = list_attention_items(db_path, "attention-host", include_resolved=True)
    outbox_rows = _connector_outbox_rows(db_path)
    delivery_keys = [row[1] for row in outbox_rows]

    assert len(rows) == 1
    assert rows[0]["lifecycle_status"] == "open"
    assert rows[0]["last_changed_at"] == "2026-01-01T00:10:00+00:00"
    assert len(outbox_rows) == 2
    assert len(set(delivery_keys)) == 2
    assert all("attention_created" in key for key in delivery_keys)
    assert all("sentinel-private" not in row[2] for row in outbox_rows)


def _worker_binding(
    *,
    worker_id: str = "worker-1",
    worker_fingerprint: str = "fp-1",
    target_kind: str = "pane_id",
    target_value: str = "pane-1",
    private_fingerprint: str = "priv-1",
    sendable: bool = True,
    reason: str | None = None,
    observed_at: str = "2026-01-01T00:00:00+00:00",
    expires_at: str = "2026-01-02T00:00:00+00:00",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker_id,
        worker_fingerprint=worker_fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=None,
        turn_target_value=None,
        sendable=sendable,
        reason=reason,
        observed_at=observed_at,
        expires_at=expires_at,
        private_fingerprint=private_fingerprint,
    )


def test_store_worker_binding_upsert_list_resolve_and_expire(tmp_path: Path) -> None:
    db_path = tmp_path / "bindings.db"
    first = _worker_binding()
    moved = _worker_binding(
        target_value="pane-2",
        observed_at="2026-01-01T00:10:00+00:00",
    )

    init_store(db_path)
    assert upsert_worker_bindings(db_path, [first]) == 1
    assert upsert_worker_bindings(db_path, [moved]) == 1

    current = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert len(current) == 1
    assert current[0].target_value == "pane-2"
    assert current[0].worker_id == "worker-1"
    resolved = resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        worker_fingerprint="fp-1",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert resolved is not None
    assert resolved.target_value == "pane-2"

    expired_count = expire_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        private_fingerprints=["priv-1"],
        now="2026-01-01T00:45:00+00:00",
        reason="stale_target",
    )
    assert expired_count == 1
    assert list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:46:00+00:00") == []
    history = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        include_expired=True,
        now="2026-01-01T00:46:00+00:00",
    )
    assert len(history) == 1
    assert history[0].sendable is False
    assert history[0].reason == "stale_target"
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        backend="herdr",
        now="2026-01-01T00:46:00+00:00",
    ) is None


def test_store_worker_bindings_allow_duplicate_targets_and_expire_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        private_fingerprint="priv-a",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        private_fingerprint="priv-b",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    upsert_worker_bindings(db_path, [binding_a, binding_b])

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.target_value for binding in current} == {"same-pane"}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None

    expired_count = expire_stale_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        current_private_fingerprints=["priv-a"],
        now="2026-01-01T00:40:00+00:00",
        reason="stale_observation",
    )
    assert expired_count == 1
    remaining = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:41:00+00:00")
    assert [binding.private_fingerprint for binding in remaining] == ["priv-a"]


def test_store_upsert_separates_colliding_duplicate_private_fingerprints(tmp_path: Path) -> None:
    db_path = tmp_path / "colliding-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )

    assert upsert_worker_bindings(db_path, [binding_a, binding_b]) == 2

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.worker_id for binding in current} == {"worker-a", "worker-b"}
    assert {binding.sendable for binding in current} == {False}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert "colliding-private" not in {binding.private_fingerprint for binding in current}
    assert len({binding.private_fingerprint for binding in current}) == 2
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None


def test_store_snapshot_payload_does_not_contain_private_worker_bindings(tmp_path: Path) -> None:
    db_path = tmp_path / "payload-clean.db"
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    binding = _worker_binding(target_value="pane-secret", private_fingerprint="priv-secret")

    save_snapshot(db_path, snapshot)
    upsert_worker_bindings(db_path, [binding])

    with sqlite3.connect(str(db_path)) as conn:
        payload = conn.execute("SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        target_value = conn.execute("SELECT target_value FROM worker_bindings LIMIT 1").fetchone()[0]

    assert target_value == "pane-secret"
    assert "pane-secret" not in payload
    assert "priv-secret" not in payload
    assert "target_kind" not in payload


def test_store_save_snapshot_updates_pr6_projections_and_prunes_by_host(tmp_path: Path) -> None:
    db_path = tmp_path / "projections.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    snapshot_a_old = project_from_raw(
        config_a,
        spaces=[{"id": "space-old", "name": "Old", "status": "active"}],
        workers=[
            {
                "id": "worker-old",
                "name": "Old Worker",
                "status": "active",
                "space_id": "space-old",
                "summary": "old",
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    snapshot_b = project_from_raw(
        config_b,
        spaces=[{"id": "space-b", "name": "B", "status": "active"}],
        workers=[{"id": "worker-b", "name": "Worker B", "status": "active"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        spaces=[{"id": "space-new", "name": "New", "status": "warning"}],
        workers=[
            {
                "id": "worker-new",
                "name": "New Worker",
                "status": "pending",
                "space_id": "space-new",
                "summary": "human approval required before continuing",
                "meta": {
                    "needs_human": True,
                    "safe": "kept",
                    "connectorId": "sentinel-connector-id",
                    "delivery": "sentinel-delivery",
                },
                "backend_target": {"value": "sentinel-private-target"},
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "degraded",
                "outcome": "malformed_json",
                "observed_at": "2026-01-01T00:01:00+00:00",
                "message": "Herdr command returned malformed JSON",
                "counts": {"workers": 1},
            }
        ],
    )

    save_snapshot(db_path, snapshot_a_old)
    save_snapshot(db_path, snapshot_b)
    save_snapshot(db_path, snapshot_a_new)

    with sqlite3.connect(str(db_path)) as conn:
        host_a_workers = conn.execute(
            "SELECT worker_id, status, payload_json FROM workers WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_b_workers = conn.execute(
            "SELECT worker_id FROM workers WHERE host_id = ?",
            ("host-b",),
        ).fetchall()
        host_a_spaces = conn.execute(
            "SELECT space_id FROM spaces WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_turns = conn.execute(
            "SELECT worker_id FROM turns WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_pending_count = conn.execute(
            "SELECT COUNT(*) FROM pending_interactions WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_attention_count = conn.execute(
            "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_health = conn.execute(
            "SELECT backend_name, status, outcome FROM backend_health WHERE host_id = ?",
            ("host-a",),
        ).fetchone()
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert [(row[0], row[1]) for row in host_a_workers] == [("worker-new", "waiting")]
    assert host_b_workers == [("worker-b",)]
    assert host_a_spaces == [("space-new",)]
    assert host_a_turns == [("worker-new",)]
    assert host_a_pending_count == 1
    assert host_a_attention_count == 1
    assert host_a_health == ("herdr", "degraded", "malformed_json")
    assert event_count == 3
    assert "sentinel-" not in host_a_workers[0][2]


def test_store_merges_public_turn_content_without_private_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please explain Telegram delivery.",
            "assistant_final_text": "Done without leaking pane_id pane-private or terminal_id term-private.",
            "assistant_stream_text": "Working...",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)

    assert updated == 1
    turn = payload["turns"][0]
    assert turn["worker_id"] == "worker-1"
    assert turn["user_text"] == "Please explain Telegram delivery."
    assert "Done without leaking" in turn["assistant_final_text"]
    assert "pane-private" not in json.dumps(payload)
    assert "term-private" not in json.dumps(payload)
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False


def test_store_merges_turn_content_into_matching_command_row_only(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-command-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    command_turn = store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="Please answer from Telegram.",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    assert command_turn is not None

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please answer from Telegram.",
            "assistant_final_text": "Telegram answer delivered.",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    command_rows = [
        turn for turn in payload["turns"] if turn.get("origin_command_id") == "req-1"
    ]
    snapshot_rows = [
        turn for turn in payload["turns"] if turn.get("id") != command_turn["id"]
    ]

    assert updated == 1
    assert len(command_rows) == 1
    assert command_rows[0]["assistant_final_text"] == "Telegram answer delivered."
    assert command_rows[0]["complete"] is True
    assert snapshot_rows
    assert all(not (turn.get("assistant_final_text") or "") for turn in snapshot_rows)


def test_store_save_latest_host_scope_and_list_hosts(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)

    init_store(db_path)
    assert latest_snapshot(db_path) is None

    snapshot_a_old = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-old", "name": "Host A Old", "status": "active"}],
    )
    snapshot_b = project_from_raw(
        config_b,
        workers=[{"id": "worker-b", "name": "Host B", "status": "idle"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-new", "name": "Host A New", "status": "waiting"}],
    )

    save_snapshot(db_path, snapshot_a_old)
    save_snapshot(db_path, snapshot_b)
    save_snapshot(db_path, snapshot_a_new)

    global_restored = latest_snapshot(db_path)
    assert global_restored is not None
    assert global_restored.host_id == "host-a"
    assert global_restored.content_fingerprint == snapshot_a_new.content_fingerprint

    restored_a = latest_snapshot(db_path, "host-a")
    assert restored_a is not None
    assert restored_a.host_id == "host-a"
    assert restored_a.content_fingerprint == snapshot_a_new.content_fingerprint
    assert restored_a.workers[0].id == "worker-a-new"

    restored_b = latest_snapshot(db_path, "host-b")
    assert restored_b is not None
    assert restored_b.host_id == "host-b"
    assert restored_b.content_fingerprint == snapshot_b.content_fingerprint
    assert restored_b.workers[0].id == "worker-b"

    assert latest_snapshot(db_path, "missing-host") is None
    assert list_hosts(db_path) == ["host-a", "host-b"]


def test_store_command_receipts_track_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)

    assert get_command_receipt(db_path, "host-a", "req-1", "send_instruction") is None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-1",
        action="send_instruction",
        payload_fingerprint="fp-1",
        status="backend_unsupported",
        result_json='{"ok": false}',
    )

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-1"
    assert receipt["status"] == "backend_unsupported"
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-2",
        action="send_instruction",
        payload_fingerprint="fp-2",
        status="request_state_uncertain",
        result_json='{"ok": false}',
        uncertain=True,
    )

    uncertain = get_command_receipt(db_path, "host-a", "req-2", "send_instruction")
    assert uncertain is not None
    assert uncertain["uncertain"] is True
    assert uncertain["completed_at"] is None


def test_store_migrates_legacy_duplicate_command_receipts_by_latest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-receipts.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES
                ('host-a', 'req-1', 'send_instruction', 'fp-old', 'backend_failed',
                 '{"status":"backend_failed"}', '2026-01-01T00:00:00+00:00',
                 '2026-01-01T00:00:01+00:00', 0),
                ('host-a', 'req-1', 'send_instruction', 'fp-new', 'accepted',
                 '{"status":"accepted"}', '2026-01-01T00:00:02+00:00',
                 '2026-01-01T00:00:03+00:00', 0);
            PRAGMA user_version = 2;
            """
        )

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
        indexes = _unique_index_columns(conn, "command_receipts")

    receipt = get_command_receipt(db_path, "host-a", "req-1", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["payload_fingerprint"] == "fp-new"
    assert receipt["status"] == STATUS_ACCEPTED
    assert indexes["ux_command_receipts_host_request_action"] == (
        "host_id",
        "request_id",
        "action",
    )


def test_store_completion_updates_reserved_receipt_row(tmp_path: Path) -> None:
    db_path = tmp_path / "completion.db"
    init_store(db_path)

    reservation = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )
    assert reservation["reserved"] is True

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="req-update",
        action="send_instruction",
        payload_fingerprint="fp-update",
        status=STATUS_ACCEPTED,
        result_json='{"ok": true, "status": "accepted"}',
    )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]

    receipt = get_command_receipt(db_path, "host-a", "req-update", "send_instruction")
    assert count == 1
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None


def test_store_command_audit_tracks_one_row_per_receipt_key(tmp_path: Path) -> None:
    db_path = tmp_path / "command-audit.db"
    init_store(db_path)

    reservation = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )
    duplicate = reserve_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
    )

    assert reservation["reserved"] is True
    assert duplicate["reserved"] is False
    with sqlite3.connect(str(db_path)) as conn:
        pending_rows = conn.execute(
            """
            SELECT status, payload_fingerprint, uncertain, completed_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchall()
    assert pending_rows == [("pending", "audit-fp", 1, None)]

    save_command_receipt(
        db_path,
        host_id="host-a",
        request_id="audit-req",
        action="send_instruction",
        payload_fingerprint="audit-fp",
        status=STATUS_ACCEPTED,
        result_json='{"ok": true, "status": "accepted"}',
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT status, payload_fingerprint, uncertain, completed_at, result_json, updated_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchall()
        receipt_count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]

    assert receipt_count == 1
    assert len(rows) == 1
    assert rows[0][0:3] == (STATUS_ACCEPTED, "audit-fp", 0)
    assert rows[0][3] is not None
    assert rows[0][4] == '{"ok": true, "status": "accepted"}'
    updated_at = rows[0][5]

    init_store(db_path)
    get_command_receipt(db_path, "host-a", "audit-req", "send_instruction")

    with sqlite3.connect(str(db_path)) as conn:
        stable_updated_at = conn.execute(
            """
            SELECT updated_at
            FROM commands
            WHERE host_id = 'host-a'
              AND request_id = 'audit-req'
              AND action = 'send_instruction'
            """
        ).fetchone()[0]

    assert stable_updated_at == updated_at


def test_store_command_receipt_reservation_allows_one_concurrent_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "race.db"
    init_store(db_path)
    barrier = threading.Barrier(2)
    mutations: list[str] = []
    results: list[dict[str, object]] = []
    lock = threading.Lock()

    def attempt(label: str) -> None:
        barrier.wait(timeout=5)
        reservation = reserve_command_receipt(
            db_path,
            host_id="host-a",
            request_id="req-race",
            action="send_instruction",
            payload_fingerprint="same-fp",
            pending_result_json='{"ok": false, "status": "request_state_uncertain"}',
        )
        with lock:
            results.append(reservation)
            if reservation["reserved"]:
                mutations.append(label)
        if reservation["reserved"]:
            save_command_receipt(
                db_path,
                host_id="host-a",
                request_id="req-race",
                action="send_instruction",
                payload_fingerprint="same-fp",
                status=STATUS_ACCEPTED,
                result_json='{"ok": true, "status": "accepted"}',
            )

    threads = [threading.Thread(target=attempt, args=(label,)) for label in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 2
    assert sum(1 for result in results if result["reserved"]) == 1
    assert len(mutations) == 1
    receipt = get_command_receipt(db_path, "host-a", "req-race", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]
    assert count == 1


def test_distinct_source_turns_mint_distinct_public_turn_ids(tmp_path: Path) -> None:
    db_path = tmp_path / "source-turns.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    for index, (prompt, reply) in enumerate(
        [("first question", "first answer"), ("second question", "second answer")],
        start=1,
    ):
        merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "user_text": prompt,
                "assistant_final_text": reply,
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        )

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    content_turns = [t for t in payload["turns"] if t.get("assistant_final_text")]
    assert len(content_turns) == 2
    assert len({t["id"] for t in content_turns}) == 2
    # Newest first per worker; the worker's base row must not carry stale text.
    assert content_turns[0]["assistant_final_text"] == "second answer"
    base_rows = [t for t in payload["turns"] if not t.get("source_turn_id")]
    assert all(not t.get("assistant_final_text") and not t.get("user_text") for t in base_rows)

    # Same source turn observed again updates its row, keeping the id stable.
    merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {"assistant_final_text": "second answer, revised", "complete": True, "source_turn_id": "uuid-2"},
    )
    payload2 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    revised = [t for t in payload2["turns"] if t.get("assistant_final_text") == "second answer, revised"]
    assert len(revised) == 1
    assert revised[0]["id"] in {t["id"] for t in content_turns}

    # Snapshot rewrites must not prune per-source-turn rows.
    save_snapshot(db_path, snapshot)
    payload3 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    assert len([t for t in payload3["turns"] if t.get("source_turn_id")]) == 2


def test_source_turn_history_is_capped(tmp_path: Path) -> None:
    db_path = tmp_path / "source-turn-cap.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(10):
        merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:{index:02d}:00+00:00",
        )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [t for t in payload["turns"] if t.get("source_turn_id")]
    assert len(source_rows) == 6
    assert source_rows[0]["assistant_final_text"] == "answer 9"
