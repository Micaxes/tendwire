"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from collections.abc import Collection, Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..core.commands import CommandEnvelope
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    WorkerBinding,
    normalize_severity,
    separate_duplicate_worker_bindings,
    sanitize_forbidden_fields,
    stable_fingerprint,
    stable_json_dumps,
    utc_timestamp,
)
from ..core.turns import (
    Turn,
    is_internal_automation_turn_payload,
    pending_from_snapshot,
    turns_from_snapshot,
    turns_payload_from_snapshot,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS
STORE_SCHEMA_VERSION = 4
ATTENTION_LIFECYCLE_OPEN = "open"
ATTENTION_LIFECYCLE_RESOLVED = "resolved"
ATTENTION_RESOLVED_REASON_GONE = "gone"
ATTENTION_OUTBOX_CONNECTOR = "attention"
_ATTENTION_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}

CREATE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);
"""

CREATE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_snapshots_host_id ON snapshots(host_id)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at)",
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_content_fingerprint "
        "ON snapshots(content_fingerprint)"
    ),
)

CREATE_COMMAND_RECEIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS command_receipts (
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
"""

CREATE_COMMAND_RECEIPT_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)",
)
CREATE_COMMAND_RECEIPT_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_command_receipts_host_request_action "
    "ON command_receipts(host_id, request_id, action)"
)

CREATE_WORKER_BINDINGS_TABLE = """
CREATE TABLE IF NOT EXISTS worker_bindings (
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
"""

CREATE_WORKER_BINDING_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_id "
    "ON worker_bindings(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_fingerprint "
    "ON worker_bindings(host_id, worker_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_private_fingerprint "
    "ON worker_bindings(host_id, backend, private_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_backend_target "
    "ON worker_bindings(host_id, backend, target_kind, target_value)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_expires_at "
    "ON worker_bindings(host_id, expires_at)",
)
CREATE_WORKER_BINDING_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_worker_bindings_host_backend_private "
    "ON worker_bindings(host_id, backend, private_fingerprint)"
)

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL DEFAULT '',
    aggregate_id TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
"""

CREATE_SPACES_TABLE = """
CREATE TABLE IF NOT EXISTS spaces (
    host_id TEXT NOT NULL,
    space_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, space_id)
);
"""

CREATE_WORKERS_TABLE = """
CREATE TABLE IF NOT EXISTS workers (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    space_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    last_seen_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, worker_id)
);
"""

CREATE_TURNS_TABLE = """
CREATE TABLE IF NOT EXISTS turns (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    status TEXT NOT NULL,
    kind TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, turn_id)
);
"""

CREATE_PENDING_INTERACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS pending_interactions (
    host_id TEXT NOT NULL,
    pending_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, pending_id)
);
"""

CREATE_ATTENTION_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS attention_items (
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
    first_seen_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT '',
    last_changed_at TEXT NOT NULL DEFAULT '',
    resolved_at TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'open',
    resolved_reason TEXT,
    signal_count INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, attention_id)
);
"""

CREATE_COMMANDS_TABLE = """
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    dry_run INTEGER NOT NULL DEFAULT 0,
    uncertain INTEGER NOT NULL DEFAULT 0,
    request_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    reserved_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);
"""

CREATE_CONNECTOR_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS connector_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    next_attempt_at TEXT
);
"""

CREATE_CONNECTOR_DELIVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS connector_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id INTEGER,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL DEFAULT '{}',
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE SET NULL
);
"""

CREATE_BACKEND_HEALTH_TABLE = """
CREATE TABLE IF NOT EXISTS backend_health (
    host_id TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    observed_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, backend_name)
);
"""

CREATE_PR6_TABLES = (
    CREATE_EVENTS_TABLE,
    CREATE_SPACES_TABLE,
    CREATE_WORKERS_TABLE,
    CREATE_TURNS_TABLE,
    CREATE_PENDING_INTERACTIONS_TABLE,
    CREATE_ATTENTION_ITEMS_TABLE,
    CREATE_COMMANDS_TABLE,
    CREATE_CONNECTOR_OUTBOX_TABLE,
    CREATE_CONNECTOR_DELIVERIES_TABLE,
    CREATE_BACKEND_HEALTH_TABLE,
)

CREATE_PR6_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_host_observed_at ON events(host_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_host_type ON events(host_id, event_type)",
    (
        "CREATE INDEX IF NOT EXISTS idx_events_host_aggregate "
        "ON events(host_id, aggregate_type, aggregate_id)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_spaces_host_status ON spaces(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_status ON workers(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_space ON workers(host_id, space_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_worker ON turns(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_status ON turns(host_id, status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_worker "
        "ON pending_interactions(host_id, worker_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_status "
        "ON pending_interactions(host_id, status)"
    ),
    (
        "CREATE TABLE IF NOT EXISTS backend_pending ("
        "host_id TEXT NOT NULL, "
        "worker_id TEXT NOT NULL, "
        "payload_json TEXT NOT NULL, "
        "observed_at TEXT NOT NULL, "
        "PRIMARY KEY (host_id, worker_id))"
    ),
    (
        # A REAL structured decision (AskUserQuestion/ExitPlanMode) read from a pending-decision file
        # and joined to a worker. Kept SEPARATE from backend_pending so the exact file-based decision
        # is never clobbered/pruned by the screen-scraped backend_pending writer; get_pending prefers
        # it over both backend_pending and the synthetic row.
        "CREATE TABLE IF NOT EXISTS backend_decision ("
        "host_id TEXT NOT NULL, "
        "worker_id TEXT NOT NULL, "
        "payload_json TEXT NOT NULL, "
        "observed_at TEXT NOT NULL, "
        "PRIMARY KEY (host_id, worker_id))"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_source "
        "ON attention_items(host_id, source)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_status "
        "ON attention_items(host_id, status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_lifecycle_status "
        "ON attention_items(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_last_seen "
        "ON attention_items(host_id, last_seen_at)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_fingerprint "
        "ON attention_items(host_id, fingerprint)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_commands_host_request_action "
        "ON commands(host_id, request_id, action)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_commands_host_status ON commands(host_id, status)",
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_connector_outbox_host_connector_key "
        "ON connector_outbox(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_connector_outbox_status ON connector_outbox(status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_outbox "
        "ON connector_deliveries(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_host_connector "
        "ON connector_deliveries(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_backend_health_host_status ON backend_health(host_id, status)",
)


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _is_memory_db(db_path: Path) -> bool:
    raw = str(db_path)
    return raw == ":memory:" or (raw.startswith("file:") and "mode=memory" in raw)


def _apply_connection_pragmas(conn: sqlite3.Connection, db_path: Path) -> None:
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    if not _is_memory_db(db_path):
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class _ClosingConnection(sqlite3.Connection):
    """sqlite3's ``with conn:`` commits/rolls back but does
    NOT close the connection, so every ``with _connect(...) as conn:`` site leaked
    its db + WAL file descriptors. Under the long-running daemon (polled every few
    seconds) these piled up until the process hit its open-file limit and crashed
    on ``accept()`` (EMFILE), which surfaced as "request_state_uncertain" sends.
    Closing on __exit__ bounds the FD count."""

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def _connect(
    db_path: Path,
    *,
    isolation_level: str | None = "",
) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=30.0,
        isolation_level=isolation_level,
        factory=_ClosingConnection,
    )
    _apply_connection_pragmas(conn, db_path)
    return conn


def _canonical_json(data: Any) -> str:
    return stable_json_dumps(data)

_CONNECTOR_LEASE_STATUS = "leased"
_CONNECTOR_POLLABLE_STATUSES = frozenset({"queued", "deferred", "retry"})
_CONNECTOR_TERMINAL_OUTBOX_STATUS = "delivered"
_CONNECTOR_EXHAUSTED_OUTBOX_STATUS = "dead_letter"
_CONNECTOR_PUBLIC_OUTBOX_STATUSES = frozenset(
    {
        _CONNECTOR_LEASE_STATUS,
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        *_CONNECTOR_POLLABLE_STATUSES,
    }
)
_CONNECTOR_REF_PREFIX = "twref1."
_CONNECTOR_PUBLIC_DROP = object()
_STORE_PUBLIC_DROP = object()
_CONNECTOR_FORBIDDEN_PUBLIC_TEXT = (
    "telegram",
    "herdr",
    "herdres",
    "backend_target",
    "pane_id",
    "session_id",
    "terminal_id",
    "chat_id",
    "topic_id",
    "message_id",
    "bot_token",
    "shell",
    "argv",
    "connector",
    "delivery",
)
_STORE_METADATA_FORBIDDEN_PUBLIC_TEXT = (
    *_CONNECTOR_FORBIDDEN_PUBLIC_TEXT,
    "private",
    "raw",
)


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _connector_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _connector_iso(value: str | datetime) -> str:
    parsed = value if isinstance(value, datetime) else _connector_datetime(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _connector_now(value: str | None = None) -> str:
    return _connector_iso(value or utc_timestamp())


def _connector_add_seconds(now: str, seconds: int) -> str:
    return _connector_iso(_connector_datetime(now) + timedelta(seconds=max(0, int(seconds))))


def _utc_cutoff(*, retention_days: int, now: str | None = None) -> str:
    current = _connector_datetime(now or utc_timestamp())
    cutoff = current - timedelta(days=max(1, int(retention_days)))
    return _connector_iso(cutoff)


def _connector_public_ref() -> str:
    return f"{_CONNECTOR_REF_PREFIX}{secrets.token_hex(32)}"


def _compact_public_text(value: str) -> str:
    return "".join(char for char in value.lower() if char.isalnum())


def _connector_contains_forbidden_public_text(value: str) -> bool:
    lowered = value.lower()
    compact = _compact_public_text(lowered)
    return any(
        token in lowered or token.replace("_", "") in compact
        for token in _CONNECTOR_FORBIDDEN_PUBLIC_TEXT
    )


def _connector_public_reason(value: Any) -> str:
    text = str(value or "").strip()
    if not text or _connector_contains_forbidden_public_text(text):
        return ""
    return text


def _store_public_label(value: Any, *, allowed: Collection[str] | None = None) -> str:
    lowered = str(value or "").strip().lower().replace("-", "_")
    clean = "".join(
        char if char.isalnum() or char in {"_", "."} else "_"
        for char in lowered
    )
    clean = "_".join(part for part in clean.split("_") if part).strip("._")[:64]
    if not clean:
        return "unknown"
    compact = clean.replace("_", "").replace(".", "")
    if any(
        token in clean or token.replace("_", "") in compact
        for token in _STORE_METADATA_FORBIDDEN_PUBLIC_TEXT
    ):
        return "unknown"
    if allowed is not None and clean not in allowed:
        return "unknown"
    return clean


def _store_contains_forbidden_public_text(value: str) -> bool:
    lowered = value.lower()
    compact = _compact_public_text(lowered)
    return any(
        token in lowered or token.replace("_", "") in compact
        for token in _STORE_METADATA_FORBIDDEN_PUBLIC_TEXT
    )


def _store_public_text(value: Any, *, default: str = "") -> str:
    text = str(value or "").strip()
    if not text or _store_contains_forbidden_public_text(text):
        return default
    return text


def _store_sanitize_public_value(value: Any) -> Any:
    clean = sanitize_forbidden_fields(value)
    if isinstance(clean, Mapping):
        result: dict[str, Any] = {}
        for key, item in clean.items():
            sanitized = _store_sanitize_public_value(item)
            if sanitized is not _STORE_PUBLIC_DROP:
                result[str(key)] = sanitized
        return result
    if isinstance(clean, list):
        result_list: list[Any] = []
        for item in clean:
            sanitized = _store_sanitize_public_value(item)
            if sanitized is not _STORE_PUBLIC_DROP:
                result_list.append(sanitized)
        return result_list
    if isinstance(clean, str) and _store_contains_forbidden_public_text(clean):
        return _STORE_PUBLIC_DROP
    return clean


def _store_sanitize_public_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    clean = _store_sanitize_public_value(dict(value))
    return dict(clean) if isinstance(clean, Mapping) else {}


def _connector_sanitize_public_value(value: Any) -> Any:
    clean = sanitize_forbidden_fields(value)
    if isinstance(clean, Mapping):
        result: dict[str, Any] = {}
        for key, item in clean.items():
            sanitized = _connector_sanitize_public_value(item)
            if sanitized is not _CONNECTOR_PUBLIC_DROP:
                result[str(key)] = sanitized
        return result
    if isinstance(clean, list):
        result_list: list[Any] = []
        for item in clean:
            sanitized = _connector_sanitize_public_value(item)
            if sanitized is not _CONNECTOR_PUBLIC_DROP:
                result_list.append(sanitized)
        return result_list
    if isinstance(clean, str) and _connector_contains_forbidden_public_text(clean):
        return _CONNECTOR_PUBLIC_DROP
    return clean


def _connector_sanitize_public_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    clean = _connector_sanitize_public_value(dict(value))
    return dict(clean) if isinstance(clean, Mapping) else {}


def _connector_sanitize_payload(raw: Any) -> dict[str, Any]:
    return _connector_sanitize_public_mapping(_json_object(raw))


def _connector_private_with_lease(
    raw: Any,
    *,
    delivery_id: int | None,
    attempt: int,
    lease_token: str,
    lease_expires_at: str,
    public_ref: str,
) -> str:
    state = _json_object(raw)
    state["current_delivery_id"] = delivery_id
    state["current_attempt"] = int(attempt)
    state["lease_token"] = str(lease_token)
    state["lease_expires_at"] = str(lease_expires_at)
    state["public_ref"] = str(public_ref)
    return _canonical_json(state)


def _connector_private_clear_current(raw: Any) -> str:
    state = _json_object(raw)
    for key in ("current_delivery_id", "current_attempt", "lease_token", "lease_expires_at", "public_ref"):
        state.pop(key, None)
    return _canonical_json(state)


def _connector_response(
    *,
    ok: bool,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
    key: str | None = None,
    attempt: int | None = None,
    available_at: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ok": bool(ok),
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
    }
    if ref is not None:
        payload["ref"] = str(ref)
    if key is not None:
        payload["key"] = str(key)
    if attempt is not None:
        payload["attempt"] = int(attempt)
    if available_at is not None:
        payload["available_at"] = str(available_at)
    return sanitize_forbidden_fields(payload)


def _connector_error_response(
    *,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
) -> dict[str, Any]:
    payload = _connector_response(ok=False, status=status, host_id=host_id, name=name, ref=ref)
    payload["error"] = {
        "code": str(status),
        "message": "reference is not valid for the requested operation",
    }
    return sanitize_forbidden_fields(payload)


def _connector_reclaim_expired_leases_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> int:
    clauses = ["d.status = ?"]
    params: list[Any] = [_CONNECTOR_LEASE_STATUS]
    if host_id:
        clauses.append("d.host_id = ?")
        params.append(str(host_id))
    if name:
        clauses.append("d.connector = ?")
        params.append(str(name))
    rows = conn.execute(
        f"""
        SELECT
            d.id,
            d.outbox_id,
            d.private_state_json,
            o.status,
            o.private_state_json
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE {" AND ".join(clauses)}
        """,
        params,
    ).fetchall()
    reclaimed = 0
    now_dt = _connector_datetime(now)
    for delivery_id, outbox_id, delivery_private, outbox_status, outbox_private in rows:
        state = _json_object(delivery_private)
        lease_expires_at = state.get("lease_expires_at")
        if not lease_expires_at or _connector_datetime(str(lease_expires_at)) > now_dt:
            continue
        conn.execute(
            """
            UPDATE connector_deliveries
            SET status = ?, response_json = ?, delivered_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                "expired",
                _canonical_json({"schema_version": 1, "status": "expired"}),
                now,
                int(delivery_id),
                _CONNECTOR_LEASE_STATUS,
            ),
        )
        outbox_state = _json_object(outbox_private)
        current_delivery_id = outbox_state.get("current_delivery_id")
        if int(outbox_id or 0) > 0 and (
            current_delivery_id is None or int(current_delivery_id or 0) == int(delivery_id)
        ) and str(outbox_status or "") == _CONNECTOR_LEASE_STATUS:
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, updated_at = ?, private_state_json = ?
                WHERE id = ? AND status = ?
                """,
                (
                    "queued",
                    now,
                    _connector_private_clear_current(outbox_private),
                    int(outbox_id),
                    _CONNECTOR_LEASE_STATUS,
                ),
            )
        reclaimed += 1
    return reclaimed


def _connector_exhaust_retryable_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None = None,
    max_attempts: int,
    now: str,
    dry_run: bool = False,
) -> int:
    clauses = [
        "host_id = ?",
        "status IN ('queued', 'deferred', 'retry')",
        """
        (
            SELECT COALESCE(MAX(d.attempt), 0)
            FROM connector_deliveries d
            WHERE d.outbox_id = connector_outbox.id
        ) >= ?
        """,
    ]
    params: list[Any] = [str(host_id), max(1, int(max_attempts))]
    if name is not None:
        clauses.insert(1, "connector = ?")
        params.insert(1, str(name))
    where_sql = " AND ".join(clauses)
    if dry_run:
        row = conn.execute(
            f"SELECT COUNT(*) FROM connector_outbox WHERE {where_sql}",
            params,
        ).fetchone()
        return int(row[0] or 0)

    cursor = conn.execute(
        f"""
        UPDATE connector_outbox
        SET status = ?,
            next_attempt_at = NULL,
            updated_at = ?,
            private_state_json = ?
        WHERE {where_sql}
        """,
        [
            _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
            now,
            "{}",
            *params,
        ],
    )
    return int(cursor.rowcount or 0)


def reclaim_expired_connector_leases(
    db_path: Path,
    host_id: str,
    name: str | None = None,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Expire stale connector leases and return their outbox rows to polling."""
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name or ""),
            "reclaimed": 0,
        }
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            reclaimed = _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name) if name is not None else None,
                now=current_time,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "name": str(name or ""),
            "reclaimed": int(reclaimed),
        }
    )


def exhaust_connector_retries(
    db_path: Path,
    host_id: str,
    *,
    max_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Move host-scoped retryable outbox rows beyond max attempts to a neutral terminal state."""
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "updated": 0,
        }
    current_time = _connector_now(now)
    attempt_limit = max(1, int(max_attempts))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if dry_run:
                conn.execute("SAVEPOINT dry_run_exhaust_connector_retries")
                try:
                    _connector_reclaim_expired_leases_conn(
                        conn,
                        host_id=str(host_id),
                        name=None,
                        now=current_time,
                    )
                    updated = _connector_exhaust_retryable_conn(
                        conn,
                        host_id=str(host_id),
                        max_attempts=attempt_limit,
                        now=current_time,
                    )
                finally:
                    conn.execute("ROLLBACK TO dry_run_exhaust_connector_retries")
                    conn.execute("RELEASE dry_run_exhaust_connector_retries")
            else:
                _connector_reclaim_expired_leases_conn(
                    conn,
                    host_id=str(host_id),
                    name=None,
                    now=current_time,
                )
                updated = _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    max_attempts=attempt_limit,
                    now=current_time,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "max_attempts": attempt_limit,
            "updated": int(updated),
        }
    )


def poll_connector_outbox(
    db_path: Path,
    host_id: str,
    name: str,
    *,
    limit: int = 1,
    lease_seconds: int = 60,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically lease due connector outbox rows for one neutral queue name."""
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name),
            "items": [],
        }
    current_time = _connector_now(now)
    lease_expires_at = _connector_add_seconds(current_time, max(1, int(lease_seconds)))
    row_limit = max(1, min(int(limit), 100))
    items: list[dict[str, Any]] = []
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            if max_attempts is not None:
                _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    max_attempts=max_attempts,
                    now=current_time,
                )
            rows = conn.execute(
                """
                SELECT
                    id,
                    delivery_key,
                    payload_json,
                    private_state_json
                FROM connector_outbox
                WHERE host_id = ?
                  AND connector = ?
                  AND status IN ('queued', 'deferred', 'retry')
                  AND (next_attempt_at IS NULL OR next_attempt_at = '' OR next_attempt_at <= ?)
                ORDER BY id
                LIMIT ?
                """,
                (str(host_id), str(name), current_time, row_limit),
            ).fetchall()
            for row in rows:
                outbox_id = int(row[0])
                attempt_row = conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0)
                    FROM connector_deliveries
                    WHERE outbox_id = ?
                    """,
                    (outbox_id,),
                ).fetchone()
                attempt = int(attempt_row[0] or 0) + 1
                lease_token = secrets.token_urlsafe(24)
                public_ref = _connector_public_ref()
                cursor = conn.execute(
                    """
                    INSERT INTO connector_deliveries (
                        outbox_id,
                        host_id,
                        connector,
                        delivery_key,
                        attempt,
                        status,
                        response_json,
                        private_state_json,
                        created_at,
                        delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        outbox_id,
                        str(host_id),
                        str(name),
                        str(row[1]),
                        attempt,
                        _CONNECTOR_LEASE_STATUS,
                        "{}",
                        _connector_private_with_lease(
                            {},
                            delivery_id=None,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        current_time,
                        None,
                    ),
                )
                delivery_id = int(cursor.lastrowid)
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _connector_private_with_lease(
                            {},
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        delivery_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, updated_at = ?, private_state_json = ?
                    WHERE id = ? AND status IN ('queued', 'deferred', 'retry')
                    """,
                    (
                        _CONNECTOR_LEASE_STATUS,
                        current_time,
                        _connector_private_with_lease(
                            row[3],
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        outbox_id,
                    ),
                )
                items.append(
                    {
                        "outbox_id": outbox_id,
                        "delivery_id": delivery_id,
                        "host_id": str(host_id),
                        "name": str(name),
                        "key": str(row[1]),
                        "attempt": attempt,
                        "lease_token": lease_token,
                        "leased_until": lease_expires_at,
                        "ref": public_ref,
                        "available_at": current_time,
                        "payload": _connector_sanitize_payload(row[2]),
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name),
        "items": items,
    }


def _connector_validate_live_ref_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    ref: str,
    now: str,
) -> tuple[Any | None, str | None]:
    rows = conn.execute(
        """
        SELECT
            d.id,
            d.outbox_id,
            d.host_id,
            d.connector,
            d.delivery_key,
            d.attempt,
            d.status,
            d.private_state_json,
            o.status,
            o.private_state_json
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE d.host_id = ? AND d.connector = ? AND d.status = ?
        ORDER BY d.id DESC
        """,
        (str(host_id), str(name), _CONNECTOR_LEASE_STATUS),
    ).fetchall()
    for row in rows:
        delivery_state = _json_object(row[7])
        if str(delivery_state.get("public_ref") or "") != str(ref):
            continue
        if str(row[6] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        outbox_state = _json_object(row[9])
        if int(outbox_state.get("current_delivery_id") or 0) != int(row[0]):
            return row, "stale_ref"
        if str(row[8] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        lease_expires_at = str(delivery_state.get("lease_expires_at") or "")
        if not lease_expires_at or _connector_datetime(lease_expires_at) <= _connector_datetime(now):
            return row, "expired_ref"
        return row, None
    return None, "invalid_ref"


def _connector_update_ref(
    db_path: Path,
    *,
    action: str,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    reason: str | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    if not db_path.exists():
        return _connector_error_response(status="store_unavailable", host_id=host_id, name=name, ref=ref)
    current_time = _connector_now(now)
    sanitized_response = _connector_sanitize_public_mapping(response or {})
    sanitized_reason = _connector_public_reason(reason)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            row, error = _connector_validate_live_ref_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                ref=str(ref),
                now=current_time,
            )
            if error is not None or row is None:
                conn.rollback()
                return _connector_error_response(status=error or "invalid_ref", host_id=host_id, name=name, ref=ref)

            delivery_id = int(row[0])
            outbox_id = int(row[1] or 0)
            delivery_key = str(row[4])
            attempt = int(row[5] or 0)
            if action == "ack":
                response_json = _canonical_json(
                    sanitize_forbidden_fields(
                        {
                            "schema_version": 1,
                            "status": "acknowledged",
                            "response": dict(sanitized_response),
                        }
                    )
                )
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = ?, response_json = ?, delivered_at = ?
                    WHERE id = ?
                    """,
                    ("delivered", response_json, current_time, int(delivery_id)),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, next_attempt_at = NULL, updated_at = ?, private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
                        current_time,
                        _connector_private_clear_current(row[9]),
                        int(outbox_id),
                    ),
                )
                conn.commit()
                return _connector_response(
                    ok=True,
                    status="acknowledged",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                    key=delivery_key,
                    attempt=attempt,
                )

            if available_at is None:
                available_at = _connector_add_seconds(
                    current_time,
                    60 if delay_seconds is None else int(delay_seconds),
                )
            else:
                available_at = _connector_iso(available_at)
            attempt_limit = max(1, int(max_attempts)) if max_attempts is not None else None
            exhausted = action == "fail" and attempt_limit is not None and attempt >= attempt_limit
            result_status = "attempts_exhausted" if exhausted else ("retry_scheduled" if action == "fail" else "deferred")
            delivery_status = "failed" if action == "fail" else "deferred"
            outbox_status = (
                _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
                if exhausted
                else ("retry" if action == "fail" else "deferred")
            )
            response_json = _canonical_json(
                sanitize_forbidden_fields(
                    {
                        "schema_version": 1,
                        "status": result_status,
                        "reason": sanitized_reason,
                        "available_at": available_at,
                        "response": dict(sanitized_response),
                    }
                )
            )
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = ?, response_json = ?, delivered_at = ?
                WHERE id = ?
                """,
                (delivery_status, response_json, current_time, int(delivery_id)),
            )
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = ?, updated_at = ?, private_state_json = ?
                WHERE id = ?
                """,
                (
                    outbox_status,
                    None if exhausted else available_at,
                    current_time,
                    _connector_private_clear_current(row[9]),
                    int(outbox_id),
                ),
            )
            conn.commit()
            return _connector_response(
                ok=True,
                status=result_status,
                host_id=host_id,
                name=name,
                ref=ref,
                key=delivery_key,
                attempt=attempt,
                available_at=None if exhausted else available_at,
            )
        except Exception:
            conn.rollback()
            raise


def ack_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Acknowledge a live connector lease and make the outbox item terminal."""
    return _connector_update_ref(
        db_path,
        action="ack",
        host_id=host_id,
        name=name,
        ref=ref,
        response=response,
        now=now,
    )


def fail_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector failure and schedule the outbox item for retry."""
    return _connector_update_ref(
        db_path,
        action="fail",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        max_attempts=max_attempts,
        now=now,
    )


def defer_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector deferral and make the outbox item available later."""
    return _connector_update_ref(
        db_path,
        action="defer",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        now=now,
    )


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    if hasattr(snapshot, "to_dict"):
        data = snapshot.to_dict()
    else:
        data = json.loads(snapshot.to_json())
    return dict(data)


def _sort_observations(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    if not all(isinstance(item, Mapping) for item in value):
        return value
    return sorted(
        (dict(item) for item in value),
        key=lambda item: (
            str(item.get("id") or item.get("fingerprint") or ""),
            str(item.get("fingerprint") or ""),
            _canonical_json(item),
        ),
    )


def _strip_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in {"updated_at", "observed_at", "content_fingerprint"}
        }
    if isinstance(value, list | tuple):
        return [_strip_content_volatile(item) for item in value]
    return value


def _fingerprint_input(data: Mapping[str, Any]) -> dict[str, Any]:
    fingerprint_data = dict(_strip_content_volatile(data))
    for collection in ("spaces", "workers", "attention"):
        if collection in fingerprint_data:
            fingerprint_data[collection] = _sort_observations(
                fingerprint_data[collection]
            )
    return fingerprint_data


def _content_fingerprint(data: Mapping[str, Any]) -> str:
    raw = data.get("content_fingerprint")
    if isinstance(raw, str) and raw:
        return raw
    return stable_fingerprint(
        _fingerprint_input(data),
        length=FINGERPRINT_HEX_LENGTH,
    )


def _command_receipt_from_row(row: Any) -> dict[str, Any]:
    return {
        "host_id": row[0],
        "request_id": row[1],
        "action": row[2],
        "payload_fingerprint": row[3],
        "status": row[4],
        "result_json": row[5],
        "created_at": row[6],
        "completed_at": row[7],
        "uncertain": bool(row[8]),
    }


def _worker_binding_from_row(row: Any) -> WorkerBinding:
    return WorkerBinding(
        host_id=row[0],
        worker_id=row[1],
        worker_fingerprint=row[2],
        backend=row[3],
        target_kind=row[4],
        target_value=row[5],
        turn_target_kind=row[6],
        turn_target_value=row[7],
        sendable=bool(row[8]),
        reason=row[9],
        observed_at=row[10],
        expires_at=row[11],
        private_fingerprint=row[12],
    )


def _dedupe_command_receipts(conn: sqlite3.Connection) -> None:
    """Keep the latest legacy receipt per logical command key before uniquing."""
    rows = conn.execute(
        """
        SELECT
            id,
            host_id,
            request_id,
            action,
            created_at,
            completed_at
        FROM command_receipts
        ORDER BY id
        """
    ).fetchall()
    keep_by_key: dict[tuple[str, str, str], tuple[str, str, int]] = {}
    for row in rows:
        row_id = int(row[0])
        key = (str(row[1]), str(row[2]), str(row[3]))
        created_at = str(row[4] or "")
        completed_at = str(row[5] or "")
        sort_key = (completed_at or created_at, created_at, row_id)
        if key not in keep_by_key or sort_key > keep_by_key[key]:
            keep_by_key[key] = sort_key

    keep_ids = {item[2] for item in keep_by_key.values()}
    delete_ids = [int(row[0]) for row in rows if int(row[0]) not in keep_ids]
    if not delete_ids:
        return
    placeholders = ",".join("?" for _ in delete_ids)
    conn.execute(
        f"DELETE FROM command_receipts WHERE id IN ({placeholders})",
        delete_ids,
    )


def _ensure_command_receipt_unique_index(conn: sqlite3.Connection) -> None:
    for row in conn.execute("PRAGMA index_list(command_receipts)").fetchall():
        index_name = str(row[1])
        is_unique = int(row[2]) == 1
        if index_name == "ux_command_receipts_host_request_action" and not is_unique:
            conn.execute("DROP INDEX ux_command_receipts_host_request_action")
            break
    conn.execute(CREATE_COMMAND_RECEIPT_UNIQUE_INDEX)


def _latest_command_receipt_row(
    conn: sqlite3.Connection,
    host_id: str,
    request_id: str,
    action: str,
) -> Any:
    return conn.execute(
        """
        SELECT
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            result_json,
            created_at,
            completed_at,
            uncertain
        FROM command_receipts
        WHERE host_id = ? AND request_id = ? AND action = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()


def _snapshot_payload(data: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload_data = dict(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
    raw = payload_data.get("content_fingerprint")
    if not isinstance(raw, str) or not raw:
        payload_data["content_fingerprint"] = fingerprint
    return payload_data, fingerprint


def _table_columns(conn: sqlite3.Connection, table: str = "snapshots") -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: Mapping[str, str],
) -> None:
    existing = _table_columns(conn, table)
    for column, definition in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _backfill_content_fingerprints(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, payload
        FROM snapshots
        WHERE content_fingerprint IS NULL OR content_fingerprint = ''
        """
    ).fetchall()
    for row_id, payload in rows:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            fingerprint = _content_fingerprint({"payload": payload})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        if not isinstance(data, Mapping):
            fingerprint = _content_fingerprint({"payload": data})
            conn.execute(
                "UPDATE snapshots SET content_fingerprint = ? WHERE id = ?",
                (fingerprint, row_id),
            )
            continue
        payload_data, fingerprint = _snapshot_payload(
            Snapshot.from_dict(data).to_dict()
        )
        conn.execute(
            """
            UPDATE snapshots
            SET content_fingerprint = ?, payload = ?
            WHERE id = ?
            """,
            (fingerprint, _canonical_json(payload_data), row_id),
        )


def _ensure_command_receipt_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "command_receipts",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "request_id": "TEXT NOT NULL DEFAULT ''",
            "action": "TEXT NOT NULL DEFAULT ''",
            "payload_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "completed_at": "TEXT",
            "uncertain": "INTEGER NOT NULL DEFAULT 0",
        },
    )


def _ensure_worker_binding_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "worker_bindings",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "backend": "TEXT NOT NULL DEFAULT ''",
            "target_kind": "TEXT NOT NULL DEFAULT ''",
            "target_value": "TEXT NOT NULL DEFAULT ''",
            "turn_target_kind": "TEXT",
            "turn_target_value": "TEXT",
            "sendable": "INTEGER NOT NULL DEFAULT 0",
            "reason": "TEXT",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "expires_at": "TEXT NOT NULL DEFAULT '9999-12-31T23:59:59+00:00'",
            "private_fingerprint": "TEXT NOT NULL DEFAULT ''",
        },
    )


def _ensure_pr6_columns(conn: sqlite3.Connection) -> None:
    _ensure_columns(
        conn,
        "events",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "event_type": "TEXT NOT NULL DEFAULT ''",
            "aggregate_type": "TEXT NOT NULL DEFAULT ''",
            "aggregate_id": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "spaces",
        {
            "name": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "workers",
        {
            "worker_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "space_id": "TEXT",
            "name": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "last_seen_at": "TEXT",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "turns",
        {
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT",
            "space_id": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "pending_interactions",
        {
            "worker_id": "TEXT NOT NULL DEFAULT ''",
            "worker_fingerprint": "TEXT",
            "space_id": "TEXT",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "attention_items",
        {
            "source": "TEXT NOT NULL DEFAULT ''",
            "kind": "TEXT NOT NULL DEFAULT 'unknown'",
            "severity": "TEXT NOT NULL DEFAULT 'info'",
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "updated_at": "TEXT",
            "fingerprint": "TEXT NOT NULL DEFAULT ''",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "observed_at": "TEXT NOT NULL DEFAULT ''",
            "first_seen_at": "TEXT NOT NULL DEFAULT ''",
            "last_seen_at": "TEXT NOT NULL DEFAULT ''",
            "last_changed_at": "TEXT NOT NULL DEFAULT ''",
            "resolved_at": "TEXT",
            "lifecycle_status": "TEXT NOT NULL DEFAULT 'open'",
            "resolved_reason": "TEXT",
            "signal_count": "INTEGER NOT NULL DEFAULT 1",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )
    _ensure_columns(
        conn,
        "commands",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "request_id": "TEXT NOT NULL DEFAULT ''",
            "action": "TEXT NOT NULL DEFAULT ''",
            "payload_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "dry_run": "INTEGER NOT NULL DEFAULT 0",
            "uncertain": "INTEGER NOT NULL DEFAULT 0",
            "request_json": "TEXT NOT NULL DEFAULT '{}'",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "reserved_at": "TEXT",
            "completed_at": "TEXT",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
        },
    )
    _ensure_columns(
        conn,
        "connector_outbox",
        {
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "connector": "TEXT NOT NULL DEFAULT ''",
            "delivery_key": "TEXT NOT NULL DEFAULT ''",
            "status": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
            "private_state_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TEXT NOT NULL DEFAULT ''",
            "next_attempt_at": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "connector_deliveries",
        {
            "outbox_id": "INTEGER",
            "host_id": "TEXT NOT NULL DEFAULT ''",
            "connector": "TEXT NOT NULL DEFAULT ''",
            "delivery_key": "TEXT NOT NULL DEFAULT ''",
            "attempt": "INTEGER NOT NULL DEFAULT 0",
            "status": "TEXT NOT NULL DEFAULT ''",
            "response_json": "TEXT NOT NULL DEFAULT '{}'",
            "private_state_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TEXT NOT NULL DEFAULT ''",
            "delivered_at": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "backend_health",
        {
            "status": "TEXT NOT NULL DEFAULT 'unknown'",
            "outcome": "TEXT NOT NULL DEFAULT 'unknown'",
            "observed_at": "TEXT",
            "snapshot_content_fingerprint": "TEXT NOT NULL DEFAULT ''",
            "payload_json": "TEXT NOT NULL DEFAULT '{}'",
        },
    )


def _append_event_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    aggregate_type: str = "",
    aggregate_id: str = "",
    observed_at: str | None = None,
    content_fingerprint: str | None = None,
) -> int:
    payload_json = _canonical_json(payload)
    fingerprint = content_fingerprint or stable_fingerprint(
        {"event_type": event_type, "payload": payload}
    )
    cursor = conn.execute(
        """
        INSERT INTO events (
            host_id,
            event_type,
            aggregate_type,
            aggregate_id,
            observed_at,
            content_fingerprint,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(host_id),
            str(event_type),
            str(aggregate_type),
            str(aggregate_id),
            observed_at or utc_timestamp(),
            str(fingerprint),
            payload_json,
        ),
    )
    return int(cursor.lastrowid)


def append_event(
    db_path: Path,
    host_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    *,
    aggregate_type: str = "",
    aggregate_id: str = "",
    observed_at: str | None = None,
    content_fingerprint: str | None = None,
) -> int:
    """Append a private store event and return its row id."""
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        return _append_event_conn(
            conn,
            host_id=host_id,
            event_type=event_type,
            payload=payload,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            observed_at=observed_at,
            content_fingerprint=content_fingerprint,
        )


def _prune_host_projection(
    conn: sqlite3.Connection,
    table: str,
    key_column: str,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM {table} WHERE host_id = ? AND {key_column} NOT IN ({placeholders})",
            [str(host_id), *ids],
        )
    else:
        conn.execute(f"DELETE FROM {table} WHERE host_id = ?", (str(host_id),))


def _turn_payload_is_prune_protected(payload_json: Any) -> bool:
    """Rows tied to a command or a concrete backend turn outlive snapshot rewrites."""
    try:
        payload = json.loads(str(payload_json or "{}"))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, Mapping):
        return False
    return bool(
        str(payload.get("origin_command_id") or "").strip()
        or str(payload.get("source_turn_id") or "").strip()
    )


def _prune_turn_projection(
    conn: sqlite3.Connection,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id NOT IN ({placeholders})
            """,
            [str(host_id), *ids],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (str(host_id),),
        ).fetchall()
    for turn_id, payload_json in rows:
        if _turn_payload_is_prune_protected(payload_json):
            continue
        conn.execute(
            "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
            (str(host_id), str(turn_id)),
        )


def _attention_id_from_item(item: Mapping[str, Any]) -> str:
    return str(item.get("id") or item.get("fingerprint") or "unknown")


def _attention_lifecycle_payload(
    item: Mapping[str, Any],
    *,
    attention_id: str,
    observed_at: str,
    first_seen_at: str,
    last_seen_at: str,
    last_changed_at: str,
    lifecycle_status: str,
    signal_count: int,
    resolved_at: str | None = None,
    resolved_reason: str | None = None,
) -> dict[str, Any]:
    payload = dict(item)
    payload.setdefault("id", attention_id)
    payload.setdefault("source", "")
    payload.setdefault("kind", "unknown")
    payload.setdefault("severity", "info")
    payload.setdefault("status", "unknown")
    payload.setdefault("fingerprint", "")
    payload["observed_at"] = observed_at
    payload["first_seen_at"] = first_seen_at
    payload["last_seen_at"] = last_seen_at
    payload["last_changed_at"] = last_changed_at
    payload["lifecycle_status"] = lifecycle_status
    payload["resolved_at"] = resolved_at
    if resolved_reason is not None:
        payload["resolved_reason"] = resolved_reason
    payload["signal_count"] = max(1, int(signal_count))
    return sanitize_forbidden_fields(payload)


def _snapshot_attention_is_authoritative_healthy(payload_data: Mapping[str, Any]) -> bool:
    health_items = payload_data.get("backend_health", [])
    if not isinstance(health_items, list) or not health_items:
        return False
    for item in health_items:
        if not isinstance(item, Mapping):
            return False
        if str(item.get("status") or "unknown") != "healthy":
            return False
    return True


def _attention_severity_rank(value: Any) -> int:
    return _ATTENTION_SEVERITY_RANK.get(normalize_severity(value), 0)


def _attention_transition_event_type(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    attention_id: str,
    source: str,
    kind: str,
    severity: str,
) -> str:
    rows = conn.execute(
        """
        SELECT severity
        FROM attention_items
        WHERE host_id = ?
          AND source = ?
          AND kind = ?
          AND attention_id != ?
          AND lifecycle_status = ?
        """,
        (
            str(host_id),
            str(source),
            str(kind),
            str(attention_id),
            ATTENTION_LIFECYCLE_OPEN,
        ),
    ).fetchall()
    new_rank = _attention_severity_rank(severity)
    if any(new_rank > _attention_severity_rank(row[0]) for row in rows):
        return "attention_escalated"
    return "attention_created"


def _enqueue_attention_lifecycle_job_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    event_type: str,
    attention_id: str,
    attention_payload: Mapping[str, Any],
    transition_at: str,
) -> None:
    lifecycle_key = stable_fingerprint(
        {
            "event_type": event_type,
            "attention_id": attention_id,
            "transition_at": transition_at,
        }
    )[:16]
    delivery_key = f"attention:{event_type}:{attention_id}:{lifecycle_key}"
    payload = sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": str(host_id),
            "attention": dict(attention_payload),
            "transition_at": transition_at,
        }
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id,
            connector,
            delivery_key,
            status,
            payload_json,
            private_state_json,
            created_at,
            updated_at,
            next_attempt_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
        """,
        (
            str(host_id),
            ATTENTION_OUTBOX_CONNECTOR,
            delivery_key,
            "queued",
            _canonical_json(payload),
            "{}",
            transition_at,
            transition_at,
            None,
        ),
    )


def _upsert_attention_item_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    item: Mapping[str, Any],
    content_fingerprint: str,
    observed_at: str,
) -> str:
    attention_id = _attention_id_from_item(item)
    source = str(item.get("source") or "")
    kind = str(item.get("kind") or "unknown")
    severity = str(item.get("severity") or "info")
    signal_status = str(item.get("status") or "unknown")
    updated_at = item.get("updated_at")
    fingerprint = str(item.get("fingerprint") or "")
    payload_json = _canonical_json(dict(item))
    existing = conn.execute(
        """
        SELECT
            fingerprint,
            lifecycle_status,
            signal_count,
            last_changed_at,
            severity,
            status
        FROM attention_items
        WHERE host_id = ? AND attention_id = ?
        """,
        (str(host_id), attention_id),
    ).fetchone()

    if existing is None:
        transition_event = _attention_transition_event_type(
            conn,
            host_id=host_id,
            attention_id=attention_id,
            source=source,
            kind=kind,
            severity=severity,
        )
        conn.execute(
            """
            INSERT INTO attention_items (
                host_id,
                attention_id,
                source,
                kind,
                severity,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                first_seen_at,
                last_seen_at,
                last_changed_at,
                resolved_at,
                lifecycle_status,
                resolved_reason,
                signal_count,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(host_id),
                attention_id,
                source,
                kind,
                severity,
                signal_status,
                updated_at,
                fingerprint,
                str(content_fingerprint),
                observed_at,
                observed_at,
                observed_at,
                observed_at,
                None,
                ATTENTION_LIFECYCLE_OPEN,
                None,
                1,
                payload_json,
            ),
        )
        _enqueue_attention_lifecycle_job_conn(
            conn,
            host_id=host_id,
            event_type=transition_event,
            attention_id=attention_id,
            attention_payload=_attention_lifecycle_payload(
                item,
                attention_id=attention_id,
                observed_at=observed_at,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                last_changed_at=observed_at,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                signal_count=1,
            ),
            transition_at=observed_at,
        )
        return attention_id

    previous_fingerprint = str(existing[0] or "")
    previous_lifecycle_status = str(existing[1] or ATTENTION_LIFECYCLE_OPEN)
    previous_signal_count = int(existing[2] or 0)
    previous_last_changed_at = str(existing[3] or observed_at)
    previous_severity = str(existing[4] or "info")
    previous_signal_status = str(existing[5] or "unknown")
    changed = (
        previous_lifecycle_status != ATTENTION_LIFECYCLE_OPEN
        or previous_fingerprint != fingerprint
        or previous_severity != severity
        or previous_signal_status != signal_status
    )
    last_changed_at = observed_at if changed else previous_last_changed_at
    signal_count = max(0, previous_signal_count) + 1
    conn.execute(
        """
        UPDATE attention_items
        SET
            source = ?,
            kind = ?,
            severity = ?,
            status = ?,
            updated_at = ?,
            fingerprint = ?,
            snapshot_content_fingerprint = ?,
            observed_at = ?,
            last_seen_at = ?,
            last_changed_at = ?,
            resolved_at = NULL,
            lifecycle_status = ?,
            resolved_reason = NULL,
            signal_count = ?,
            payload_json = ?
        WHERE host_id = ? AND attention_id = ?
        """,
        (
            source,
            kind,
            severity,
            signal_status,
            updated_at,
            fingerprint,
            str(content_fingerprint),
            observed_at,
            observed_at,
            last_changed_at,
            ATTENTION_LIFECYCLE_OPEN,
            signal_count,
            payload_json,
            str(host_id),
            attention_id,
        ),
    )
    if previous_lifecycle_status != ATTENTION_LIFECYCLE_OPEN:
        _enqueue_attention_lifecycle_job_conn(
            conn,
            host_id=host_id,
            event_type="attention_created",
            attention_id=attention_id,
            attention_payload=_attention_lifecycle_payload(
                item,
                attention_id=attention_id,
                observed_at=observed_at,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                last_changed_at=last_changed_at,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                signal_count=signal_count,
            ),
            transition_at=observed_at,
        )
    return attention_id


def _resolve_missing_attention_items_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    current_attention_ids: Iterable[str],
    content_fingerprint: str,
    observed_at: str,
    authoritative: bool,
) -> int:
    if not authoritative:
        return 0
    current = sorted({str(value) for value in current_attention_ids})
    if current:
        placeholders = ",".join("?" for _ in current)
        cursor = conn.execute(
            f"""
            UPDATE attention_items
            SET
                lifecycle_status = ?,
                resolved_at = ?,
                resolved_reason = ?,
                last_changed_at = ?,
                snapshot_content_fingerprint = ?
            WHERE host_id = ?
              AND lifecycle_status = ?
              AND attention_id NOT IN ({placeholders})
            """,
            [
                ATTENTION_LIFECYCLE_RESOLVED,
                observed_at,
                ATTENTION_RESOLVED_REASON_GONE,
                observed_at,
                str(content_fingerprint),
                str(host_id),
                ATTENTION_LIFECYCLE_OPEN,
                *current,
            ],
        )
    else:
        cursor = conn.execute(
            """
            UPDATE attention_items
            SET
                lifecycle_status = ?,
                resolved_at = ?,
                resolved_reason = ?,
                last_changed_at = ?,
                snapshot_content_fingerprint = ?
            WHERE host_id = ?
              AND lifecycle_status = ?
            """,
            (
                ATTENTION_LIFECYCLE_RESOLVED,
                observed_at,
                ATTENTION_RESOLVED_REASON_GONE,
                observed_at,
                str(content_fingerprint),
                str(host_id),
                ATTENTION_LIFECYCLE_OPEN,
            ),
        )
    return int(cursor.rowcount or 0)


def _upsert_snapshot_projections(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    *,
    snapshot_id: int,
    content_fingerprint: str,
) -> None:
    host_id = str(snapshot.host_id)
    observed_at = str(snapshot.updated_at)

    _append_event_conn(
        conn,
        host_id=host_id,
        event_type="snapshot.saved",
        aggregate_type="snapshot",
        aggregate_id=str(content_fingerprint),
        observed_at=observed_at,
        content_fingerprint=str(content_fingerprint),
        payload={
            "snapshot_id": int(snapshot_id),
            "content_fingerprint": str(content_fingerprint),
            "snapshot": dict(payload_data),
        },
    )

    space_ids: set[str] = set()
    for item in payload_data.get("spaces", []):
        if not isinstance(item, Mapping):
            continue
        space_id = str(item.get("id") or "unknown")
        space_ids.add(space_id)
        conn.execute(
            """
            INSERT INTO spaces (
                host_id,
                space_id,
                name,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, space_id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                space_id,
                str(item.get("name") or space_id),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "spaces", "space_id", host_id, space_ids)

    worker_ids: set[str] = set()
    for item in payload_data.get("workers", []):
        if not isinstance(item, Mapping):
            continue
        worker_id = str(item.get("id") or "unknown")
        worker_ids.add(worker_id)
        conn.execute(
            """
            INSERT INTO workers (
                host_id,
                worker_id,
                worker_fingerprint,
                space_id,
                name,
                status,
                last_seen_at,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                name = excluded.name,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                worker_id,
                str(item.get("fingerprint") or ""),
                item.get("space_id"),
                str(item.get("name") or worker_id),
                str(item.get("status") or "unknown"),
                item.get("last_seen_at"),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "workers", "worker_id", host_id, worker_ids)

    attention_ids: set[str] = set()
    for item in payload_data.get("attention", []):
        if not isinstance(item, Mapping):
            continue
        attention_id = _upsert_attention_item_conn(
            conn,
            host_id=host_id,
            item=item,
            content_fingerprint=content_fingerprint,
            observed_at=observed_at,
        )
        attention_ids.add(attention_id)
    _resolve_missing_attention_items_conn(
        conn,
        host_id=host_id,
        current_attention_ids=attention_ids,
        content_fingerprint=content_fingerprint,
        observed_at=observed_at,
        authoritative=_snapshot_attention_is_authoritative_healthy(payload_data),
    )

    turn_ids: set[str] = set()
    for turn in turns_from_snapshot(snapshot):
        item = turn.to_dict()
        turn_id = str(item.get("id") or "unknown")
        turn_ids.add(turn_id)
        conn.execute(
            """
            INSERT INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                turn_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
            ),
        )
    _prune_turn_projection(conn, host_id, turn_ids)

    pending_ids: set[str] = set()
    for pending in pending_from_snapshot(snapshot):
        item = pending.to_dict()
        pending_id = str(item.get("id") or "unknown")
        pending_ids.add(pending_id)
        conn.execute(
            """
            INSERT INTO pending_interactions (
                host_id,
                pending_id,
                worker_id,
                worker_fingerprint,
                space_id,
                kind,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, pending_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                kind = excluded.kind,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                pending_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("kind") or "unknown"),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
            ),
        )
    _prune_host_projection(
        conn,
        "pending_interactions",
        "pending_id",
        host_id,
        pending_ids,
    )

    backend_names: set[str] = set()
    for item in payload_data.get("backend_health", []):
        if not isinstance(item, Mapping):
            continue
        backend_name = str(item.get("name") or "unknown")
        backend_names.add(backend_name)
        conn.execute(
            """
            INSERT INTO backend_health (
                host_id,
                backend_name,
                status,
                outcome,
                observed_at,
                snapshot_content_fingerprint,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend_name) DO UPDATE SET
                status = excluded.status,
                outcome = excluded.outcome,
                observed_at = excluded.observed_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                backend_name,
                str(item.get("status") or "unknown"),
                str(item.get("outcome") or "unknown"),
                item.get("observed_at"),
                str(content_fingerprint),
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(
        conn,
        "backend_health",
        "backend_name",
        host_id,
        backend_names,
    )


def _upsert_command_audit(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    created_at: str | None = None,
    reserved_at: str | None = None,
    completed_at: str | None = None,
    uncertain: bool = False,
    dry_run: bool = False,
    request_json: str = "{}",
    updated_at: str | None = None,
) -> None:
    if not str(request_id):
        return
    now = utc_timestamp()
    created = created_at or now
    updated = updated_at or now
    conn.execute(
        """
        INSERT INTO commands (
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            dry_run,
            uncertain,
            request_json,
            result_json,
            created_at,
            reserved_at,
            completed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, request_id, action) DO UPDATE SET
            payload_fingerprint = excluded.payload_fingerprint,
            status = excluded.status,
            uncertain = excluded.uncertain,
            result_json = excluded.result_json,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        """,
        (
            str(host_id),
            str(request_id),
            str(action),
            str(payload_fingerprint),
            str(status),
            int(dry_run),
            int(uncertain),
            str(request_json),
            str(result_json),
            created,
            reserved_at,
            completed_at,
            updated,
        ),
    )


def _command_audit_exists(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM commands
        WHERE host_id = ? AND request_id = ? AND action = ?
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()
    return row is not None


def _upsert_command_audit_from_receipt_row(
    conn: sqlite3.Connection,
    row: Any,
) -> None:
    if _command_audit_exists(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
    ):
        return
    created_at = str(row[6] or utc_timestamp())
    completed_at = row[7]
    _upsert_command_audit(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
        payload_fingerprint=str(row[3]),
        status=str(row[4]),
        result_json=str(row[5]),
        created_at=created_at,
        reserved_at=created_at,
        completed_at=completed_at,
        uncertain=bool(row[8]),
        updated_at=str(completed_at or created_at),
    )


def find_recent_matching_command_submission(
    db_path: Path,
    host_id: str,
    *,
    action: str,
    worker_id: str,
    worker_fingerprint: str = "",
    instruction_text: str,
    since: str,
    exclude_request_id: str = "",
) -> dict[str, Any] | None:
    """Return a recent same-worker/same-text accepted command, if one exists."""
    if not db_path.exists() or not str(worker_id).strip() or not str(instruction_text):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT request_id, status, request_json, created_at, updated_at
            FROM commands
            WHERE host_id = ?
              AND action = ?
              AND request_id != ?
              AND status = 'accepted'
              AND updated_at >= ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 200
            """,
            (str(host_id), str(action), str(exclude_request_id), str(since)),
        ).fetchall()
    for row in rows:
        try:
            request = json.loads(str(row[2] or "{}"))
        except json.JSONDecodeError:
            continue
        if not isinstance(request, dict):
            continue
        target = request.get("target")
        instruction = request.get("instruction")
        if not isinstance(target, dict) or not isinstance(instruction, dict):
            continue
        if str(target.get("worker_id") or "").strip() != str(worker_id).strip():
            continue
        previous_fingerprint = str(target.get("worker_fingerprint") or "").strip()
        current_fingerprint = str(worker_fingerprint or "").strip()
        if previous_fingerprint and current_fingerprint and previous_fingerprint != current_fingerprint:
            continue
        if instruction.get("text") != instruction_text:
            continue
        return sanitize_forbidden_fields(
            {
                "request_id": str(row[0] or ""),
                "status": str(row[1] or ""),
                "created_at": str(row[3] or ""),
                "updated_at": str(row[4] or ""),
            }
        )
    return None


def _backfill_command_audit(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            result_json,
            created_at,
            completed_at,
            uncertain
        FROM command_receipts
        WHERE request_id != ''
        ORDER BY id
        """
    ).fetchall()
    for row in rows:
        _upsert_command_audit_from_receipt_row(conn, row)


def _backfill_attention_lifecycle(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE attention_items
        SET
            first_seen_at = CASE
                WHEN first_seen_at IS NULL OR first_seen_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE first_seen_at
            END,
            last_seen_at = CASE
                WHEN last_seen_at IS NULL OR last_seen_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE last_seen_at
            END,
            last_changed_at = CASE
                WHEN last_changed_at IS NULL OR last_changed_at = ''
                THEN COALESCE(NULLIF(observed_at, ''), updated_at, '')
                ELSE last_changed_at
            END,
            lifecycle_status = CASE
                WHEN lifecycle_status IS NULL OR lifecycle_status = ''
                THEN 'open'
                ELSE lifecycle_status
            END,
            signal_count = CASE
                WHEN signal_count IS NULL OR signal_count < 1
                THEN 1
                ELSE signal_count
            END
        """
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_SNAPSHOTS_TABLE)
    columns = _table_columns(conn)
    if "content_fingerprint" not in columns:
        conn.execute(
            "ALTER TABLE snapshots ADD COLUMN "
            "content_fingerprint TEXT NOT NULL DEFAULT ''"
        )
    _backfill_content_fingerprints(conn)
    for statement in CREATE_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_COMMAND_RECEIPTS_TABLE)
    _ensure_command_receipt_columns(conn)
    _dedupe_command_receipts(conn)
    for statement in CREATE_COMMAND_RECEIPT_INDEXES:
        conn.execute(statement)
    _ensure_command_receipt_unique_index(conn)
    conn.execute(CREATE_WORKER_BINDINGS_TABLE)
    _ensure_worker_binding_columns(conn)
    for statement in CREATE_WORKER_BINDING_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_WORKER_BINDING_UNIQUE_INDEX)
    for statement in CREATE_PR6_TABLES:
        conn.execute(statement)
    _ensure_pr6_columns(conn)
    _backfill_attention_lifecycle(conn)
    for statement in CREATE_PR6_INDEXES:
        conn.execute(statement)
    _backfill_command_audit(conn)
    conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")


def init_store(db_path: Path) -> None:
    """Initialize or migrate the sqlite store to the current schema."""
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)


def store_status(db_path: Path, host_id: str) -> dict[str, Any]:
    """Return bounded public-safe host-scoped store and outbox counts."""
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "counts": {},
            "outbox": {"pending": 0, "leased": 0, "terminal": 0, "by_status": {}},
        }
    tables = (
        "snapshots",
        "events",
        "spaces",
        "workers",
        "turns",
        "pending_interactions",
        "attention_items",
        "commands",
        "command_receipts",
        "backend_health",
    )
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        counts = {
            table: int(
                conn.execute(f"SELECT COUNT(*) FROM {table} WHERE host_id = ?", (str(host_id),)).fetchone()[0]
            )
            for table in tables
        }
        last_event_row = conn.execute(
            """
            SELECT observed_at
            FROM events
            WHERE host_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT 1
            """,
            (str(host_id),),
        ).fetchone()
        last_snapshot_row = conn.execute(
            """
            SELECT created_at
            FROM snapshots
            WHERE host_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(host_id),),
        ).fetchone()
        outbox_rows = conn.execute(
            """
            SELECT status, COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
            GROUP BY status
            """,
            (str(host_id),),
        ).fetchall()
    by_status: dict[str, int] = {}
    for row in outbox_rows:
        status = _store_public_label(row[0], allowed=_CONNECTOR_PUBLIC_OUTBOX_STATUSES)
        by_status[status] = by_status.get(status, 0) + int(row[1] or 0)
    pending_statuses = _CONNECTOR_POLLABLE_STATUSES
    terminal_statuses = {_CONNECTOR_TERMINAL_OUTBOX_STATUS, _CONNECTOR_EXHAUSTED_OUTBOX_STATUS}
    outbox = {
        "pending": sum(count for status, count in by_status.items() if status in pending_statuses),
        "leased": int(by_status.get(_CONNECTOR_LEASE_STATUS, 0)),
        "terminal": sum(count for status, count in by_status.items() if status in terminal_statuses),
        "by_status": by_status,
    }
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "counts": counts,
            "outbox": outbox,
            "last_event_at": last_event_row[0] if last_event_row is not None else None,
            "last_snapshot_at": last_snapshot_row[0] if last_snapshot_row is not None else None,
        }
    )


def tail_event_metadata(
    db_path: Path,
    host_id: str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Return bounded event/history metadata without raw payloads."""
    row_limit = max(1, min(int(limit), 100))
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "limit": row_limit,
            "events": [],
        }
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, event_type, aggregate_type, observed_at, content_fingerprint
            FROM events
            WHERE host_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT ?
            """,
            (str(host_id), row_limit),
        ).fetchall()
    events = [
        {
            "row_id": int(row[0]),
            "event_type": _store_public_label(row[1]),
            "aggregate_type": _store_public_label(row[2]),
            "observed_at": str(row[3] or ""),
            "content_fingerprint": str(row[4] or ""),
        }
        for row in rows
    ]
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "limit": row_limit,
            "events": events,
        }
    )


def cleanup_event_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete only host-scoped old rows from the events/history table."""
    days = max(1, int(retention_days))
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    if not db_path.exists():
        return {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "deleted": 0,
        }
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM events
            WHERE host_id = ? AND observed_at < ?
            """,
            (str(host_id), cutoff_at),
        ).fetchone()
        deleted = int(row[0] or 0)
        if deleted and not dry_run:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    DELETE FROM events
                    WHERE host_id = ? AND observed_at < ?
                    """,
                    (str(host_id), cutoff_at),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "deleted": deleted,
        }
    )


def run_store_maintenance(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    max_outbox_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run bounded host-scoped store maintenance and return public-safe counts."""
    retention = cleanup_event_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
    )
    outbox = exhaust_connector_retries(
        db_path,
        host_id,
        max_attempts=max_outbox_attempts,
        now=now,
        dry_run=dry_run,
    )
    ok = bool(retention.get("ok")) and bool(outbox.get("ok"))
    return sanitize_forbidden_fields(
        {
            "schema_version": 1,
            "ok": ok,
            "status": "ok" if ok else "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention": {
                "retention_days": int(retention.get("retention_days") or retention_days),
                "cutoff_at": retention.get("cutoff_at"),
                "deleted": int(retention.get("deleted") or 0),
            },
            "outbox": {
                "max_attempts": int(outbox.get("max_attempts") or max_outbox_attempts),
                "updated": int(outbox.get("updated") or 0),
            },
        }
    )


_TURN_CONTENT_FIELDS = frozenset(
    {
        "user_text",
        "assistant_final_text",
        "assistant_stream_text",
        "model",
        "complete",
        "has_open_turn",
        "source_turn_id",
    }
)

_SOURCE_TURN_HISTORY_LIMIT = 6

_TURN_IDENTITY_SEED_FIELDS = (
    "schema_version",
    "host_id",
    "worker_id",
    "worker_fingerprint",
    "space_id",
    "status",
    "kind",
    "source",
    "origin_command_id",
    "title",
    "summary",
)


def _turn_merge_match_text(value: Any) -> str:
    return "\n".join(" ".join(line.split()) for line in str(value or "").splitlines()).strip()


def _turn_merge_score(payload: Mapping[str, Any], content: Mapping[str, Any]) -> tuple[int, str, str]:
    incoming_user = _turn_merge_match_text(content.get("user_text"))
    existing_user = _turn_merge_match_text(payload.get("user_text"))
    source = str(payload.get("source") or "")
    has_origin = bool(str(payload.get("origin_command_id") or "").strip())
    open_turn = payload.get("has_open_turn") is True or payload.get("complete") is False
    has_existing_content = bool(
        existing_user
        or str(payload.get("assistant_final_text") or "").strip()
        or str(payload.get("assistant_stream_text") or "").strip()
    )
    score = 0
    if incoming_user and existing_user == incoming_user:
        score += 1000
    elif incoming_user and has_origin and existing_user:
        score -= 500
    if has_origin and incoming_user and existing_user == incoming_user:
        score += 250
    elif has_origin:
        score -= 40
    if open_turn:
        score += 80
    if source == "command":
        score += 40 if incoming_user and existing_user == incoming_user else -20
    elif source == "snapshot":
        score += 10
    if not has_existing_content:
        score += 5
    return (
        score,
        str(payload.get("updated_at") or payload.get("observed_at") or ""),
        str(payload.get("id") or payload.get("turn_id") or ""),
    )


def _turn_content_matches_origin(payload: Mapping[str, Any], content: Mapping[str, Any]) -> bool:
    incoming_user = _turn_merge_match_text(content.get("user_text"))
    if not incoming_user:
        return False
    return incoming_user == _turn_merge_match_text(payload.get("user_text"))
def merge_backend_pending(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any] | None,
) -> bool:
    """Presence-sync one worker's backend-provided pending prompt (a REAL pane prompt with choices,
    read through the turn adapter). `pending=None` prunes the row (the prompt was answered)."""
    if not Path(db_path).exists():
        return False
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if pending is None:
            cur = conn.execute(
                "DELETE FROM backend_pending WHERE host_id = ? AND worker_id = ?",
                (host_id, worker_id),
            )
            return cur.rowcount > 0
        payload = stable_json_dumps(dict(pending))
        row = conn.execute(
            "SELECT payload_json FROM backend_pending WHERE host_id = ? AND worker_id = ?",
            (host_id, worker_id),
        ).fetchone()
        if row and row[0] == payload:
            return False
        conn.execute(
            "INSERT INTO backend_pending (host_id, worker_id, payload_json, observed_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(host_id, worker_id) DO UPDATE SET payload_json = excluded.payload_json, "
            "observed_at = excluded.observed_at",
            (host_id, worker_id, payload, utc_timestamp()),
        )
        return True


def list_backend_pending(db_path: Path | str, host_id: str) -> dict[str, dict[str, Any]]:
    """worker_id -> normalized pending dict for every live backend-provided prompt."""
    out: dict[str, dict[str, Any]] = {}
    if not Path(db_path).exists():
        return out
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        for worker_id, payload_json in conn.execute(
            "SELECT worker_id, payload_json FROM backend_pending WHERE host_id = ?",
            (host_id,),
        ).fetchall():
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                out[str(worker_id)] = payload
    return out


def prune_backend_pending(db_path: Path | str, host_id: str, live_worker_ids: Iterable[str]) -> int:
    """Delete backend_pending rows whose worker no longer has a live binding. Presence-sync only
    prunes workers still being polled; this reaps rows orphaned when a worker/pane disappears with
    a prompt still open (otherwise get_pending would surface a phantom prompt for a dead worker)."""
    if not Path(db_path).exists():
        return 0
    live = {str(worker_id) for worker_id in live_worker_ids}
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        stored = [
            str(row[0])
            for row in conn.execute(
                "SELECT worker_id FROM backend_pending WHERE host_id = ?",
                (host_id,),
            ).fetchall()
        ]
        stale = [worker_id for worker_id in stored if worker_id not in live]
        for worker_id in stale:
            conn.execute(
                "DELETE FROM backend_pending WHERE host_id = ? AND worker_id = ?",
                (host_id, worker_id),
            )
        return len(stale)


def merge_backend_decision(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    decision: Mapping[str, Any] | None,
) -> bool:
    """Presence-sync one worker's structured decision (an AskUserQuestion/ExitPlanMode read from a
    pending-decision file). ``decision=None`` prunes the row (the prompt was answered / the file is
    gone). Separate table from backend_pending so the exact file-based decision is authoritative."""
    if not Path(db_path).exists():
        return False
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if decision is None:
            cur = conn.execute(
                "DELETE FROM backend_decision WHERE host_id = ? AND worker_id = ?",
                (host_id, worker_id),
            )
            return cur.rowcount > 0
        payload = stable_json_dumps(dict(decision))
        row = conn.execute(
            "SELECT payload_json FROM backend_decision WHERE host_id = ? AND worker_id = ?",
            (host_id, worker_id),
        ).fetchone()
        if row and row[0] == payload:
            return False
        conn.execute(
            "INSERT INTO backend_decision (host_id, worker_id, payload_json, observed_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(host_id, worker_id) DO UPDATE SET payload_json = excluded.payload_json, "
            "observed_at = excluded.observed_at",
            (host_id, worker_id, payload, utc_timestamp()),
        )
        return True


def list_backend_decision(db_path: Path | str, host_id: str) -> dict[str, dict[str, Any]]:
    """worker_id -> normalized decision dict for every live file-based decision."""
    out: dict[str, dict[str, Any]] = {}
    if not Path(db_path).exists():
        return out
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        for worker_id, payload_json in conn.execute(
            "SELECT worker_id, payload_json FROM backend_decision WHERE host_id = ?",
            (host_id,),
        ).fetchall():
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                out[str(worker_id)] = payload
    return out


def prune_backend_decision(db_path: Path | str, host_id: str, live_worker_ids: Iterable[str]) -> int:
    """Delete backend_decision rows whose worker no longer has a live binding (pane closed / worker
    gone with a decision still open)."""
    if not Path(db_path).exists():
        return 0
    live = {str(worker_id) for worker_id in live_worker_ids}
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        stored = [
            str(row[0])
            for row in conn.execute(
                "SELECT worker_id FROM backend_decision WHERE host_id = ?",
                (host_id,),
            ).fetchall()
        ]
        stale = [worker_id for worker_id in stored if worker_id not in live]
        for worker_id in stale:
            conn.execute(
                "DELETE FROM backend_decision WHERE host_id = ? AND worker_id = ?",
                (host_id, worker_id),
            )
        return len(stale)


def merge_turn_content(
    db_path: Path,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> int:
    """Merge bounded public structured-turn content into host-scoped turn rows."""
    if not db_path.exists():
        return 0
    clean_content = {key: content.get(key) for key in _TURN_CONTENT_FIELDS if key in content}
    if not clean_content:
        return 0
    if is_internal_automation_turn_payload(clean_content):
        return 0
    current_time = observed_at or utc_timestamp()
    updated = 0
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND worker_id = ?
            """,
            (str(host_id), str(worker_id)),
        ).fetchall()
        if not rows:
            return 0
        decoded_rows: list[tuple[Any, dict[str, Any]]] = []
        for turn_id, payload_json in rows:
            try:
                payload = json.loads(str(payload_json or "{}"))
            except json.JSONDecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            decoded_rows.append((turn_id, payload))
        incoming_source_turn = str(clean_content.get("source_turn_id") or "").strip()
        exact_source_rows = [
            (row_turn_id, row_payload)
            for row_turn_id, row_payload in decoded_rows
            if incoming_source_turn
            and str(row_payload.get("source_turn_id") or "").strip() == incoming_source_turn
        ]
        base_turn_id, base_payload = max(
            decoded_rows,
            key=lambda row: _turn_merge_score(row[1], clean_content),
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            if exact_source_rows:
                # Same backend turn observed again: update its dedicated row.
                turn_id, payload = exact_source_rows[0]
                payload.update(clean_content)
                _update_turn_row(conn, host_id, turn_id, payload, current_time)
                updated += 1
            elif incoming_source_turn:
                # A new backend turn: mint a dedicated row with its own public
                # identity instead of overwriting the worker's evolving row —
                # otherwise every conversation turn shares one public turn id
                # and downstream deliveries silently edit one old message.
                seed = {
                    key: base_payload.get(key)
                    for key in _TURN_IDENTITY_SEED_FIELDS
                    if base_payload.get(key) is not None
                }
                if seed.get("origin_command_id") and not _turn_content_matches_origin(
                    base_payload,
                    clean_content,
                ):
                    seed.pop("origin_command_id", None)
                    if str(seed.get("source") or "") == "command":
                        seed["source"] = "snapshot"
                seed.update(clean_content)
                item = Turn.from_dict(seed).to_dict()
                conn.execute(
                    """
                    INSERT INTO turns (
                        host_id, turn_id, worker_id, worker_fingerprint, space_id,
                        status, kind, updated_at, fingerprint,
                        snapshot_content_fingerprint, observed_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(host_id, turn_id) DO UPDATE SET
                        status = excluded.status,
                        kind = excluded.kind,
                        updated_at = excluded.updated_at,
                        fingerprint = excluded.fingerprint,
                        observed_at = excluded.observed_at,
                        payload_json = excluded.payload_json
                    """,
                    (
                        str(host_id),
                        str(item.get("id") or "unknown"),
                        str(item.get("worker_id") or worker_id),
                        item.get("worker_fingerprint"),
                        item.get("space_id"),
                        str(item.get("status") or "unknown"),
                        str(item.get("kind") or "unknown"),
                        current_time,
                        str(item.get("fingerprint") or ""),
                        "",
                        current_time,
                        _canonical_json(item),
                    ),
                )
                if not str(base_payload.get("source_turn_id") or "").strip():
                    # The worker's evolving row donated its (now superseded)
                    # text to per-turn rows; clear it so stale content cannot
                    # resurface as a deliverable turn.
                    base_changed = False
                    for key in ("user_text", "assistant_final_text", "assistant_stream_text"):
                        if base_payload.get(key):
                            base_payload[key] = None
                            base_changed = True
                    if base_changed:
                        _update_turn_row(conn, host_id, base_turn_id, base_payload, current_time)
                _prune_source_turn_history(conn, host_id, worker_id)
                updated += 1
            else:
                turn_id, payload = base_turn_id, base_payload
                payload.update(clean_content)
                _update_turn_row(conn, host_id, turn_id, payload, current_time)
                updated += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return updated


def _update_turn_row(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: Any,
    payload: dict[str, Any],
    current_time: str,
) -> None:
    item = Turn.from_dict(payload).to_dict()
    conn.execute(
        """
        UPDATE turns
        SET status = ?,
            kind = ?,
            updated_at = ?,
            fingerprint = ?,
            observed_at = ?,
            payload_json = ?
        WHERE host_id = ? AND turn_id = ?
        """,
        (
            str(item.get("status") or "unknown"),
            str(item.get("kind") or "unknown"),
            item.get("updated_at") or current_time,
            str(item.get("fingerprint") or ""),
            current_time,
            _canonical_json(item),
            str(host_id),
            str(turn_id),
        ),
    )


def _prune_source_turn_history(conn: sqlite3.Connection, host_id: str, worker_id: str) -> None:
    rows = conn.execute(
        """
        SELECT turn_id, payload_json
        FROM turns
        WHERE host_id = ? AND worker_id = ?
        ORDER BY COALESCE(updated_at, observed_at, '') DESC
        """,
        (str(host_id), str(worker_id)),
    ).fetchall()
    kept = 0
    for turn_id, payload_json in rows:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, Mapping) or not str(payload.get("source_turn_id") or "").strip():
            continue
        kept += 1
        if kept > _SOURCE_TURN_HISTORY_LIMIT:
            conn.execute(
                "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
                (str(host_id), str(turn_id)),
            )


def upsert_command_pending_turn(
    db_path: Path,
    host_id: str,
    worker: Any,
    *,
    request_id: str,
    instruction_text: str,
    observed_at: str | None = None,
) -> dict[str, Any] | None:
    """Upsert a public pending turn for an accepted command submission."""
    clean_request_id = str(request_id or "").strip()
    clean_text = str(instruction_text or "").strip()
    if not clean_request_id or not clean_text:
        return None
    current_time = observed_at or utc_timestamp()
    worker_id = str(getattr(worker, "id", "") or "").strip()
    if not worker_id and isinstance(worker, Mapping):
        worker_id = str(worker.get("id") or "").strip()
    if not worker_id:
        return None
    item = Turn(
        host_id=str(host_id),
        worker_id=worker_id,
        worker_fingerprint=str(getattr(worker, "fingerprint", "") or ""),
        space_id=getattr(worker, "space_id", None),
        status="active",
        kind="task",
        source="command",
        user_text=clean_text,
        assistant_final_text="",
        assistant_stream_text="",
        complete=False,
        has_open_turn=True,
        started_at=current_time,
        updated_at=current_time,
        origin_command_id=clean_request_id,
    ).to_dict()
    turn_id = str(item.get("id") or "")
    if not turn_id:
        return None
    content_fingerprint = stable_fingerprint(
        {
            "source": "command",
            "host_id": str(host_id),
            "worker_id": worker_id,
            "request_id": clean_request_id,
            "turn_fingerprint": item.get("fingerprint"),
        }
    )
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                str(host_id),
                turn_id,
                worker_id,
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                content_fingerprint,
                current_time,
                _canonical_json(item),
            ),
        )
    return item


def turns_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    snapshot: Snapshot | None = None,
) -> dict[str, Any]:
    """Return public turns from the store, preserving structured-turn content."""
    if not db_path.exists():
        if snapshot is not None:
            return turns_payload_from_snapshot(snapshot)
        return {
            "schema_version": 1,
            "host_id": str(host_id),
            "updated_at": None,
            "content_fingerprint": stable_fingerprint({"host_id": str(host_id), "turns": []}),
            "turns": [],
            "backend_health": [],
        }
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT payload_json, observed_at
            FROM turns
            WHERE host_id = ?
            ORDER BY worker_id, COALESCE(updated_at, observed_at, '') DESC, turn_id
            """,
            (str(host_id),),
        ).fetchall()
    if not rows:
        if snapshot is not None:
            return turns_payload_from_snapshot(snapshot)
        turns: list[dict[str, Any]] = []
        updated_at = None
    else:
        turns = []
        observed_values: list[str] = []
        for payload_json, observed_at in rows:
            try:
                payload = json.loads(str(payload_json or "{}"))
            except json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict) and not is_internal_automation_turn_payload(payload):
                turns.append(Turn.from_dict(payload).to_dict())
            if observed_at:
                observed_values.append(str(observed_at))
        updated_at = max(observed_values) if observed_values else None
    backend_health = [health.to_dict() for health in snapshot.backend_health] if snapshot is not None else []
    payload = {
        "schema_version": 1,
        "host_id": str(host_id),
        "updated_at": updated_at or (snapshot.updated_at if snapshot is not None else None),
        "turns": turns,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "turns": turns,
            "backend_health": backend_health,
        }
    )
    return sanitize_forbidden_fields(payload)


def save_snapshot(db_path: Path, snapshot: Snapshot) -> None:
    """Persist a canonical snapshot JSON blob in the sqlite store."""
    data, fingerprint = _snapshot_payload(_snapshot_dict(snapshot))
    payload = _canonical_json(data)
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (snapshot.host_id, snapshot.updated_at, fingerprint, payload),
        )
        _upsert_snapshot_projections(
            conn,
            snapshot,
            data,
            snapshot_id=int(cursor.lastrowid),
            content_fingerprint=fingerprint,
        )


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not db_path.exists():
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if host_id is None:
            row = conn.execute(
                "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (host_id,),
            ).fetchone()
    if row is None:
        return None
    return Snapshot.from_json(row[0])


def _attention_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[Any]:
    clauses = ["host_id = ?"]
    params: list[Any] = [str(host_id)]
    if not include_resolved:
        clauses.append("lifecycle_status = ?")
        params.append(ATTENTION_LIFECYCLE_OPEN)
    where = " AND ".join(clauses)
    return conn.execute(
        f"""
        SELECT
            attention_id,
            source,
            kind,
            severity,
            status,
            updated_at,
            fingerprint,
            snapshot_content_fingerprint,
            observed_at,
            payload_json,
            first_seen_at,
            last_seen_at,
            last_changed_at,
            resolved_at,
            lifecycle_status,
            resolved_reason,
            signal_count
        FROM attention_items
        WHERE {where}
        ORDER BY
            CASE lifecycle_status WHEN 'open' THEN 0 ELSE 1 END,
            last_changed_at DESC,
            attention_id
        """,
        params,
    ).fetchall()


def _attention_item_from_row(row: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(row[9] or "{}")
    except (TypeError, json.JSONDecodeError):
        parsed = {}
    payload = _store_sanitize_public_mapping(parsed)
    payload.update(
        {
            "id": str(row[0] or ""),
            "source": _store_public_text(row[1], default="unknown"),
            "kind": _store_public_label(row[2]),
            "severity": str(row[3] or "info"),
            "status": str(row[4] or "unknown"),
            "updated_at": row[5],
            "fingerprint": str(row[6] or ""),
        }
    )
    payload["reason"] = _store_public_text(payload.get("reason"), default="")
    return _attention_lifecycle_payload(
        payload,
        attention_id=str(row[0] or ""),
        observed_at=str(row[8] or row[11] or ""),
        first_seen_at=str(row[10] or row[8] or ""),
        last_seen_at=str(row[11] or row[8] or ""),
        last_changed_at=str(row[12] or row[8] or ""),
        resolved_at=row[13],
        lifecycle_status=str(row[14] or ATTENTION_LIFECYCLE_OPEN),
        resolved_reason=_store_public_text(row[15], default="") or None,
        signal_count=int(row[16] or 1),
    )


def list_attention_items(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[dict[str, Any]]:
    """Return public-safe persisted attention items for a host."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
    return [_attention_item_from_row(row) for row in rows]


def attention_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> dict[str, Any] | None:
    """Return a public attention.list payload from lifecycle rows or snapshot fallback."""
    if not db_path.exists():
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
        snapshot_row = conn.execute(
            """
            SELECT payload
            FROM snapshots
            WHERE host_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(host_id),),
        ).fetchone()
        attention_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )

    if snapshot_row is None and not rows:
        return None

    snapshot: Snapshot | None = None
    if snapshot_row is not None:
        try:
            snapshot = Snapshot.from_json(snapshot_row[0])
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = None

    attention = [_attention_item_from_row(row) for row in rows]
    backend_health = [health.to_dict() for health in snapshot.backend_health] if snapshot is not None else []
    updated_at = snapshot.updated_at if snapshot is not None else utc_timestamp()
    if not attention and attention_row_count == 0 and snapshot is not None and snapshot.attention:
        attention = []
        for signal in snapshot.attention:
            item = signal.to_dict()
            attention.append(
                _attention_lifecycle_payload(
                    item,
                    attention_id=_attention_id_from_item(item),
                    observed_at=updated_at,
                    first_seen_at=updated_at,
                    last_seen_at=updated_at,
                    last_changed_at=updated_at,
                    lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                    signal_count=1,
                )
            )
    if snapshot is None and attention:
        updated_at = str(attention[0].get("last_seen_at") or attention[0].get("observed_at") or updated_at)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "host_id": str(host_id),
        "updated_at": updated_at,
        "attention": attention,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "attention": attention,
            "backend_health": backend_health,
        }
    )
    return sanitize_forbidden_fields(payload)


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return [r[0] for r in rows]


def upsert_worker_bindings(db_path: Path, bindings: Iterable[WorkerBinding]) -> int:
    """Persist observed private worker bindings by private identity.

    The upsert key is host/backend/private_fingerprint so a moved pane or
    changed backend target updates the private routing record while preserving
    the public worker identity associated with that private Herdr identity.
    """
    binding_list = separate_duplicate_worker_bindings(
        binding if isinstance(binding, WorkerBinding) else WorkerBinding(**binding)
        for binding in bindings
    )
    if not binding_list:
        return 0
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO worker_bindings (
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend, private_fingerprint) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                target_kind = excluded.target_kind,
                target_value = excluded.target_value,
                turn_target_kind = excluded.turn_target_kind,
                turn_target_value = excluded.turn_target_value,
                sendable = excluded.sendable,
                reason = excluded.reason,
                observed_at = excluded.observed_at,
                expires_at = excluded.expires_at
            """,
            [
                (
                    binding.host_id,
                    binding.worker_id,
                    binding.worker_fingerprint,
                    binding.backend,
                    binding.target_kind,
                    binding.target_value,
                    binding.turn_target_kind,
                    binding.turn_target_value,
                    int(binding.sendable),
                    binding.reason,
                    binding.observed_at,
                    binding.expires_at,
                    binding.private_fingerprint,
                )
                for binding in binding_list
            ],
        )
    return len(binding_list)


def list_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    include_expired: bool = False,
    now: str | None = None,
) -> list[WorkerBinding]:
    """Return private worker bindings for a host, current/unexpired by default."""
    if not db_path.exists():
        return []
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?"]
    params: list[Any] = [str(host_id)]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if not include_expired:
        clauses.append("expires_at > ?")
        params.append(current_time)
    where = " AND ".join(clauses)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            """,
            params,
        ).fetchall()
    return [_worker_binding_from_row(row) for row in rows]


def resolve_worker_binding(
    db_path: Path,
    host_id: str,
    worker_id: str,
    *,
    worker_fingerprint: str | None = None,
    backend: str | None = None,
    now: str | None = None,
) -> WorkerBinding | None:
    """Resolve a single current, sendable private binding for a public worker."""
    if not db_path.exists():
        return None
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?", "worker_id = ?", "sendable = 1", "expires_at > ?"]
    params: list[Any] = [str(host_id), str(worker_id), current_time]
    if worker_fingerprint:
        clauses.append("worker_fingerprint = ?")
        params.append(str(worker_fingerprint))
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    where = " AND ".join(clauses)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            LIMIT 2
            """,
            params,
        ).fetchall()
    if len(rows) != 1:
        return None
    return _worker_binding_from_row(rows[0])


def expire_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    worker_id: str | None = None,
    private_fingerprints: Iterable[str] | None = None,
    now: str | None = None,
    reason: str = "expired",
) -> int:
    """Mark matching private bindings expired and unsendable without deleting rows."""
    current_time = now or utc_timestamp()
    fingerprints = [str(value) for value in (private_fingerprints or [])]
    clauses = ["host_id = ?", "expires_at > ?"]
    params: list[Any] = [str(host_id), current_time]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if worker_id is not None:
        clauses.append("worker_id = ?")
        params.append(str(worker_id))
    if fingerprints:
        placeholders = ",".join("?" for _ in fingerprints)
        clauses.append(f"private_fingerprint IN ({placeholders})")
        params.extend(fingerprints)
    where = " AND ".join(clauses)
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"""
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE {where}
            """,
            [str(reason), current_time, *params],
        )
        return int(cursor.rowcount or 0)


def expire_stale_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
    current_private_fingerprints: Iterable[str],
    now: str | None = None,
    reason: str = "stale_observation",
) -> int:
    """Expire host/backend bindings absent from a fresh successful observation."""
    current_time = now or utc_timestamp()
    current = {str(value) for value in current_private_fingerprints}
    _ensure_dir(db_path)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if current:
            placeholders = ",".join("?" for _ in current)
            cursor = conn.execute(
                f"""
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                  AND private_fingerprint NOT IN ({placeholders})
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                    *sorted(current),
                ],
            )
        else:
            cursor = conn.execute(
                """
                UPDATE worker_bindings
                SET sendable = 0,
                    reason = ?,
                    expires_at = ?
                WHERE host_id = ?
                  AND backend = ?
                  AND expires_at > ?
                """,
                [
                    str(reason),
                    current_time,
                    str(host_id),
                    str(backend),
                    current_time,
                ],
            )
        return int(cursor.rowcount or 0)


def get_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
) -> dict[str, Any] | None:
    """Return the latest command receipt for a host/request/action key, or None."""
    if not db_path.exists():
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
    if row is None:
        return None
    return _command_receipt_from_row(row)


def reserve_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    pending_result_json: str,
    *,
    status: str = "pending",
    request_json: str = "{}",
) -> dict[str, Any]:
    """Atomically reserve a mutating command receipt key if it is unused.

    Returns {"reserved": True, "receipt": None} when this caller owns the
    mutation attempt. If another receipt already exists for the same key, the
    existing latest receipt is returned and no new row is inserted.
    """
    _ensure_dir(db_path)
    conn = _connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        row = _latest_command_receipt_row(conn, host_id, request_id, action)
        if row is not None:
            _upsert_command_audit_from_receipt_row(conn, row)
            conn.execute("COMMIT")
            return {"reserved": False, "receipt": _command_receipt_from_row(row)}
        now = utc_timestamp()
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id,
                request_id,
                action,
                payload_fingerprint,
                status,
                result_json,
                created_at,
                completed_at,
                uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(host_id),
                str(request_id),
                str(action),
                str(payload_fingerprint),
                str(status),
                str(pending_result_json),
                now,
                None,
                1,
            ),
        )
        _upsert_command_audit(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            action=str(action),
            payload_fingerprint=str(payload_fingerprint),
            status=str(status),
            result_json=str(pending_result_json),
            created_at=now,
            reserved_at=now,
            completed_at=None,
            uncertain=True,
            request_json=str(request_json),
        )
        conn.execute("COMMIT")
        return {"reserved": True, "receipt": None}
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def save_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    *,
    uncertain: bool = False,
) -> None:
    """Persist a neutral command receipt for idempotency tracking.

    Dry-runs must not call this function. The receipt records the final or
    pending state of a mutating command so repeated requests can be detected
    and rejected instead of retried blindly.
    """
    _ensure_dir(db_path)
    now = utc_timestamp()
    conn = _connect(db_path, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT id, payload_fingerprint
            FROM command_receipts
            WHERE host_id = ? AND request_id = ? AND action = ?
            LIMIT 1
            """,
            (str(host_id), str(request_id), str(action)),
        ).fetchone()
        completed_at = None if uncertain else now
        if row is not None:
            if str(row[1]) != str(payload_fingerprint):
                raise ValueError("receipt payload fingerprint mismatch")
            conn.execute(
                """
                UPDATE command_receipts
                SET
                    status = ?,
                    result_json = ?,
                    completed_at = ?,
                    uncertain = ?
                WHERE id = ? AND payload_fingerprint = ?
                """,
                (
                    str(status),
                    str(result_json),
                    completed_at,
                    int(uncertain),
                    int(row[0]),
                    str(payload_fingerprint),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO command_receipts (
                    host_id,
                    request_id,
                    action,
                    payload_fingerprint,
                    status,
                    result_json,
                    created_at,
                    completed_at,
                    uncertain
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(request_id),
                    str(action),
                    str(payload_fingerprint),
                    str(status),
                    str(result_json),
                    now,
                    completed_at,
                    int(uncertain),
                ),
            )
        _upsert_command_audit(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            action=str(action),
            payload_fingerprint=str(payload_fingerprint),
            status=str(status),
            result_json=str(result_json),
            created_at=now,
            reserved_at=now,
            completed_at=completed_at,
            uncertain=uncertain,
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    """Serialize a command envelope for storage in a receipt."""
    return stable_json_dumps(envelope.to_dict())
