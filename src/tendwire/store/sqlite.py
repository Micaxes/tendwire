"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import stat
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, quote, urlsplit

from ..local_state import (
    canonical_path_from_fd,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    local_state_error,
    inspect_sqlite_family_at,
    open_resolved_parent,
    private_file_creation_umask,
    prepare_resolved_private_parent,
    prepare_sqlite_family_at,
    validate_owned_directory_stat,
)
from ..core.commands import CommandEnvelope
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    WorkerBinding,
    normalize_severity,
    separate_duplicate_worker_bindings,
    sanitize_canonical_turn_text,
    sanitize_public_mapping,
    sanitize_public_value,
    stable_fingerprint,
    utc_timestamp,
)
from ..core.turns import (
    Turn,
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_CONTENT_PREVIEW_MAX_CHARS,
    TURN_LIST_SCHEMA_VERSION,
    TURN_TEXT_MAX_CHARS,
    ContentCursorPosition,
    content_cursor,
    content_revision,
    content_segment_id,
    decode_content_cursor,
    is_internal_automation_turn_payload,
    project_persisted_turn_content,
    project_turn_content,
    segment_canonical_text,
    pending_from_snapshot,
    turns_from_snapshot,
    turns_payload_from_snapshot,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS
STORE_SCHEMA_VERSION = 7
ATTENTION_LIFECYCLE_OPEN = "open"
ATTENTION_LIFECYCLE_RESOLVED = "resolved"
ATTENTION_RESOLVED_REASON_GONE = "gone"
ATTENTION_RESOLVED_REASON_SUPERSEDED = "superseded"
ATTENTION_OUTBOX_CONNECTOR = "attention"
ATTENTION_MISSING_REQUIRED = 2
ATTENTION_MISSING_GRACE_SECONDS = 120
_ATTENTION_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


@dataclass(frozen=True)
class SnapshotObservationContext:
    authority: Literal["none", "positive", "complete"] = "none"
    observed_at: str | None = None

@dataclass
class TurnContentWorkCounters:
    """Deterministic canonical bytes and SQL work observed by list/page operations."""

    list_sql_queries: int = 0
    list_descriptor_rows: int = 0
    list_preview_chars_examined: int = 0
    list_inline_chars_examined: int = 0
    page_sql_queries: int = 0
    page_blob_reads: int = 0
    page_bytes_examined: int = 0
    page_chars_examined: int = 0
    max_response_utf8_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "list_sql_queries": self.list_sql_queries,
            "list_descriptor_rows": self.list_descriptor_rows,
            "list_preview_chars_examined": self.list_preview_chars_examined,
            "list_inline_chars_examined": self.list_inline_chars_examined,
            "page_sql_queries": self.page_sql_queries,
            "page_blob_reads": self.page_blob_reads,
            "page_bytes_examined": self.page_bytes_examined,
            "page_chars_examined": self.page_chars_examined,
            "max_response_utf8_bytes": self.max_response_utf8_bytes,
        }


def _record_response_size(
    counters: TurnContentWorkCounters | None,
    payload: Mapping[str, Any],
) -> None:
    if counters is not None:
        counters.max_response_utf8_bytes = max(
            counters.max_response_utf8_bytes,
            len(_canonical_json(payload).encode("utf-8")),
        )

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

CREATE_TURN_CONTENT_REVISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_revisions (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    user_text TEXT,
    assistant_final_text TEXT,
    user_state TEXT NOT NULL
        CHECK (user_state IN ('absent', 'complete', 'known_incomplete')),
    final_state TEXT NOT NULL
        CHECK (final_state IN ('absent', 'complete', 'known_incomplete')),
    user_char_length INTEGER NOT NULL CHECK (user_char_length >= 0),
    user_byte_length INTEGER NOT NULL CHECK (user_byte_length >= 0),
    final_char_length INTEGER NOT NULL CHECK (final_char_length >= 0),
    final_byte_length INTEGER NOT NULL CHECK (final_byte_length >= 0),
    user_page_count INTEGER NOT NULL CHECK (user_page_count >= 0),
    final_page_count INTEGER NOT NULL CHECK (final_page_count >= 0),
    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    created_at TEXT NOT NULL,
    superseded_at TEXT,
    PRIMARY KEY (host_id, turn_id, content_revision),
    FOREIGN KEY (host_id, turn_id)
        REFERENCES turns(host_id, turn_id) ON DELETE RESTRICT
);
"""

CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_page_boundaries (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    field TEXT NOT NULL
        CHECK (field IN ('user_text', 'assistant_final_text')),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    start_char INTEGER NOT NULL CHECK (start_char >= 0),
    start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
    PRIMARY KEY (
        host_id,
        turn_id,
        content_revision,
        field,
        page_index
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_char
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_byte
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE CASCADE
);
"""

CREATE_TURN_CONTENT_REVISION_INDEXES = (
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_content_current "
        "ON turn_content_revisions(host_id, turn_id) WHERE is_current = 1"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_content_cleanup "
        "ON turn_content_revisions(host_id, is_current, superseded_at)"
    ),
)

CREATE_TURN_PRESENTATION_PLANS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_plans (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    plan_token TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    presentation_version TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    part_count INTEGER NOT NULL CHECK (part_count > 0),
    state TEXT NOT NULL
        CHECK (state IN (
            'preparing',
            'waiting_predecessor',
            'active',
            'completed',
            'superseded',
            'failed'
        )),
    replaces_plan_token TEXT,
    recovers_plan_token TEXT,
    created_at TEXT NOT NULL,
    activated_at TEXT,
    completed_at TEXT,
    UNIQUE (host_id, name, plan_token),
    UNIQUE (
        host_id,
        name,
        turn_id,
        content_revision,
        presentation_version,
        generation
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_jobs (
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
    FOREIGN KEY (plan_id) REFERENCES turn_presentation_plans(id) ON DELETE CASCADE,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_RECOVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_recoveries (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    request_id TEXT NOT NULL,
    failed_plan_id INTEGER NOT NULL,
    recovered_plan_id INTEGER NOT NULL,
    failed_plan_token TEXT NOT NULL,
    recovered_plan_token TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 2),
    source_job_count INTEGER NOT NULL CHECK (source_job_count > 0),
    delivered_prefix_count INTEGER NOT NULL CHECK (delivered_prefix_count >= 0),
    fresh_job_count INTEGER NOT NULL CHECK (fresh_job_count > 0),
    retained_failed_job_count INTEGER NOT NULL CHECK (retained_failed_job_count > 0),
    prior_attempt_count INTEGER NOT NULL CHECK (prior_attempt_count > 0),
    outcome TEXT NOT NULL CHECK (outcome = 'recovered'),
    created_at TEXT NOT NULL,
    UNIQUE (host_id, name, request_id),
    UNIQUE (failed_plan_id),
    FOREIGN KEY (failed_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (recovered_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_plan_sequence "
        "ON turn_presentation_jobs(plan_id, sequence_index)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_outbox "
        "ON turn_presentation_jobs(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_recoveries_recovered "
        "ON turn_presentation_recoveries(recovered_plan_id)"
    ),
)

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

CREATE_ATTENTION_LIFECYCLES_TABLE = """
CREATE TABLE IF NOT EXISTS attention_lifecycles (
    host_id TEXT NOT NULL,
    family_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    lifecycle_status TEXT NOT NULL CHECK (lifecycle_status IN ('open','resolved')),
    current_attention_id TEXT,
    first_seen_at TEXT NOT NULL,
    last_positive_at TEXT NOT NULL,
    first_missing_at TEXT,
    missing_observation_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_observation_count >= 0),
    last_accepted_at TEXT NOT NULL,
    last_observation_key TEXT NOT NULL,
    max_notified_severity_rank INTEGER NOT NULL DEFAULT -1,
    PRIMARY KEY (host_id, family_key),
    CHECK (
        (lifecycle_status = 'open' AND current_attention_id IS NOT NULL)
        OR (lifecycle_status = 'resolved' AND current_attention_id IS NULL)
    ),
    CHECK (
        (missing_observation_count = 0 AND first_missing_at IS NULL)
        OR (missing_observation_count > 0 AND first_missing_at IS NOT NULL)
    )
);
"""

CREATE_ATTENTION_LIFECYCLE_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_status "
        "ON attention_lifecycles(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_current "
        "ON attention_lifecycles(host_id, current_attention_id)"
    ),
)

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


_SQLITE_FAMILY_SUFFIXES = ("", "-wal", "-shm", "-journal")


def _is_memory_db(db_path: Path | str) -> bool:
    raw = str(db_path)
    if raw == ":memory:":
        return True
    if not raw.startswith("file:"):
        return False
    try:
        query = urlsplit(raw).query
    except ValueError:
        return False
    mode: str | None = None
    for name, value in parse_qsl(query, keep_blank_values=True):
        if name == "mode":
            mode = value
    return mode == "memory"


def _validate_parent_fd(parent_fd: int, *, private: bool) -> None:
    try:
        current = os.fstat(parent_fd)
    except OSError:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    validate_owned_directory_stat(current)
    forbidden = ~0o700 if private else stat.S_IWGRP | stat.S_IWOTH
    if stat.S_IMODE(current.st_mode) & forbidden:
        raise local_state_error(LocalStateErrorCode.INSECURE_MODE) from None


def _bare_relative_parent(db_path: Path | str) -> bool:
    try:
        raw = os.fspath(db_path)
        return isinstance(raw, str) and not raw.startswith(os.sep) and Path(raw).parent == Path(".")
    except (TypeError, ValueError):
        return False


def _open_filesystem_db(
    db_path: Path | str, *, prepare: bool
) -> tuple[int, str]:
    if prepare and not _bare_relative_parent(db_path):
        parent_fd, leaf, _result = prepare_resolved_private_parent(db_path)
    else:
        parent_fd, leaf = open_resolved_parent(db_path)
        try:
            _validate_parent_fd(parent_fd, private=not prepare)
        except Exception:
            os.close(parent_fd)
            raise
    try:
        if prepare:
            prepare_sqlite_family_at(parent_fd, leaf)
        _validate_sqlite_family_at(parent_fd, leaf)
        return parent_fd, leaf
    except Exception:
        os.close(parent_fd)
        raise


def _validate_sqlite_family_at(parent_fd: int, leaf: str) -> None:
    inspected = inspect_sqlite_family_at(parent_fd, leaf)
    for suffix, result in zip(_SQLITE_FAMILY_SUFFIXES, inspected, strict=True):
        if result.state is PermissionState.ABSENT and suffix:
            continue
        if result.state is PermissionState.ABSENT:
            raise local_state_error(LocalStateErrorCode.MISSING_ENTRY)
        if result.state is PermissionState.REPAIR_REQUIRED:
            raise local_state_error(LocalStateErrorCode.INSECURE_MODE)


def _sqlite_store_exists(db_path: Path | str) -> bool:
    if _is_memory_db(db_path):
        return False
    try:
        parent_fd, leaf = open_resolved_parent(db_path)
    except LocalStateError as exc:
        if exc.code is LocalStateErrorCode.MISSING_ENTRY:
            return False
        raise
    try:
        _validate_parent_fd(parent_fd, private=True)
        inspected = inspect_sqlite_family_at(parent_fd, leaf)
        return inspected[0].state is not PermissionState.ABSENT
    finally:
        os.close(parent_fd)


def _apply_connection_pragmas(conn: sqlite3.Connection, db_path: Path | str) -> None:
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    if not _is_memory_db(db_path):
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class _ClosingConnection(sqlite3.Connection):
    """Connection that owns its resolved database parent descriptor until close."""

    _parent_fd: int | None = None

    def _own_parent_fd(self, parent_fd: int) -> None:
        self._parent_fd = parent_fd

    def close(self) -> None:
        parent_fd = self._parent_fd
        self._parent_fd = None
        try:
            super().close()
        finally:
            if parent_fd is not None:
                os.close(parent_fd)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def _connect(
    db_path: Path | str,
    *,
    isolation_level: str | None = "",
    prepare: bool = False,
) -> sqlite3.Connection:
    raw_db_path = str(db_path)
    memory_db = _is_memory_db(raw_db_path)
    parent_fd: int | None = None
    leaf: str | None = None
    expected_db: os.stat_result | None = None
    if memory_db:
        connect_target = raw_db_path
        connect_uri = raw_db_path.startswith("file:")
    else:
        parent_fd, leaf = _open_filesystem_db(db_path, prepare=prepare)
        try:
            expected_db = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            canonical_path = canonical_path_from_fd(parent_fd, leaf)
            connect_target = f"file:{quote(canonical_path, safe='/')}?mode=rw"
        except Exception:
            os.close(parent_fd)
            raise
        connect_uri = True
    try:
        with private_file_creation_umask():
            conn = sqlite3.connect(
                connect_target,
                timeout=30.0,
                isolation_level=isolation_level,
                factory=_ClosingConnection,
                uri=connect_uri,
            )
    except Exception:
        if parent_fd is not None:
            os.close(parent_fd)
        raise
    if not isinstance(conn, _ClosingConnection):
        try:
            conn.close()
        finally:
            if parent_fd is not None:
                os.close(parent_fd)
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    if parent_fd is not None and leaf is not None and expected_db is not None:
        try:
            # Catch path substitution across sqlite3.connect before any pragma
            # can mutate a database other than the securely resolved one.
            canonical_path_from_fd(parent_fd, leaf)
            current_db = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            if (
                current_db.st_dev != expected_db.st_dev
                or current_db.st_ino != expected_db.st_ino
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
        except Exception:
            conn.close()
            os.close(parent_fd)
            raise
    if parent_fd is not None:
        conn._own_parent_fd(parent_fd)
        parent_fd = None
    try:
        with private_file_creation_umask():
            _apply_connection_pragmas(conn, db_path)
            if leaf is not None:
                # Activate sidecars under the restrictive creation umask, then
                # inspect metadata without opening another family descriptor.
                conn.execute("PRAGMA user_version").fetchone()
                assert conn._parent_fd is not None
                _validate_sqlite_family_at(conn._parent_fd, leaf)
        return conn
    except Exception:
        conn.close()
        raise


def _canonical_json(data: Any) -> str:
    """Serialize private or pre-sanitized data without silently dropping fields."""
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

_CONNECTOR_LEASE_STATUS = "leased"
_CONNECTOR_POLLABLE_STATUSES = frozenset({"queued", "deferred", "retry"})
_CONNECTOR_TERMINAL_OUTBOX_STATUS = "delivered"
_CONNECTOR_EXHAUSTED_OUTBOX_STATUS = "dead_letter"
_CONNECTOR_SUPERSEDED_OUTBOX_STATUS = "superseded"
_CONNECTOR_PUBLIC_OUTBOX_STATUSES = frozenset(
    {
        _CONNECTOR_LEASE_STATUS,
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
        *_CONNECTOR_POLLABLE_STATUSES,
    }
)
_CONNECTOR_REF_PREFIX = "twref1."


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



def _connector_public_reason(value: Any) -> str:
    clean = sanitize_public_mapping(
        {"reason": str(value or "").strip()},
        backend_neutral=True,
    ).get("reason")
    return clean if isinstance(clean, str) else ""


def _store_public_label(value: Any, *, allowed: Collection[str] | None = None) -> str:
    lowered = str(value or "").strip().lower().replace("-", "_")
    label = "".join(
        char if char.isalnum() or char in {"_", "."} else "_"
        for char in lowered
    )
    label = "_".join(part for part in label.split("_") if part).strip("._")[:64]
    if not label or (allowed is not None and label not in allowed):
        return "unknown"
    clean = sanitize_public_value(label, backend_neutral=True)
    return clean if isinstance(clean, str) and clean == label else "unknown"


def _store_public_text(
    value: Any,
    *,
    default: str = "",
    free_text: bool = False,
) -> str:
    text = str(value or "").strip()
    if free_text:
        clean = sanitize_public_mapping(
            {"reason": text},
            backend_neutral=True,
        ).get("reason")
    else:
        clean = sanitize_public_value(text, backend_neutral=True)
    return clean if isinstance(clean, str) and clean else default






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
    return sanitize_public_value(payload)


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
    return sanitize_public_value(payload)


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
                _canonical_json(
                    sanitize_public_mapping({"schema_version": 1, "status": "expired"})
                ),
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
            terminal_after_lease = bool(outbox_state.get("terminal_after_lease"))
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = NULL, updated_at = ?,
                    private_state_json = ?
                WHERE id = ? AND status = ?
                """,
                (
                    (
                        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                        if terminal_after_lease
                        else "queued"
                    ),
                    now,
                    _connector_private_clear_current(outbox_private),
                    int(outbox_id),
                    _CONNECTOR_LEASE_STATUS,
                ),
            )
            terminal_status = (
                _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                if terminal_after_lease
                else "queued"
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=int(outbox_id),
                outbox_status=terminal_status,
                now=now,
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
    _mark_exhausted_presentation_plans_conn(
        conn,
        host_id=str(host_id),
        name=str(name) if name is not None else None,
        now=str(now),
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
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name or ""),
            "reclaimed": 0,
        })
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
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name or ""),
        "reclaimed": int(reclaimed),
    })


def exhaust_connector_retries(
    db_path: Path,
    host_id: str,
    *,
    max_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Move host-scoped retryable outbox rows beyond max attempts to a neutral terminal state."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "updated": 0,
        })
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
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "max_attempts": attempt_limit,
        "updated": int(updated),
    })

_TURN_FINAL_NAME = "turn-final"
_PRESENTATION_SCHEMA_VERSION = 1
_PRESENTATION_MAX_PARTS = 10_000
_PRESENTATION_MAX_SPANS_PER_PART = 64
_PRESENTATION_SEQUENCE_WIDTH = 6
_PRESENTATION_FIELDS = ("user_text", "assistant_final_text")
_PRESENTATION_FIELD_RANK = {
    field: index for index, field in enumerate(_PRESENTATION_FIELDS)
}
_PRESENTATION_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_PRESENTATION_LABEL_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)


def _valid_presentation_opaque(value: Any, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    body = value[len(prefix) :]
    return bool(body) and all(char in _PRESENTATION_TOKEN_CHARS for char in body)


def _valid_presentation_label(value: Any, *, prefix: str | None = None) -> bool:
    if not isinstance(value, str) or not value or len(value) > 128:
        return False
    if prefix is not None and not value.startswith(prefix):
        return False
    if any(char not in _PRESENTATION_LABEL_CHARS for char in value):
        return False
    return sanitize_public_value(value, backend_neutral=True) == value




def _presentation_plan_token(
    *,
    host_id: str,
    name: str,
    turn_id: str,
    content_revision_value: str,
    presentation_version: str,
    part_count: int,
) -> str:
    digest = stable_fingerprint(
        {
            "domain": "tendwire.connector.prepare.v1",
            "host_id": str(host_id),
            "name": str(name),
            "turn_id": str(turn_id),
            "content_revision": str(content_revision_value),
            "presentation_version": str(presentation_version),
            "part_count": int(part_count),
        },
        length=64,
    )
    return f"twplan1.{digest}"


def _presentation_recovery_token(
    *,
    host_id: str,
    name: str,
    failed_plan_token: str,
    request_id: str,
    generation: int,
) -> str:
    digest = stable_fingerprint(
        {
            "domain": "tendwire.connector.prepare.recover.v1",
            "host_id": str(host_id),
            "name": str(name),
            "failed_plan_token": str(failed_plan_token),
            "request_id": str(request_id),
            "generation": int(generation),
        },
        length=64,
    )
    return f"twplan1.{digest}"


def _restore_presentation_tokens(
    sanitized: dict[str, Any],
    original: Mapping[str, Any],
) -> dict[str, Any]:
    for key in (
        "plan_token",
        "replaces_plan_token",
        "recovers_plan_token",
        "failed_plan_token",
        "recovered_plan_token",
    ):
        value = original.get(key)
        if value is None and key in original:
            sanitized[key] = None
        elif (
            isinstance(value, str)
            and value.startswith("twplan1.")
            and value[8:]
            and all(char.isalnum() or char in "-_" for char in value[8:])
        ):
            sanitized[key] = value
    return sanitized


def _presentation_response(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _restore_presentation_tokens(
        dict(sanitize_public_value(dict(payload))),
        payload,
    )


def _presentation_error(
    status: str,
    *,
    host_id: str,
    name: str,
    plan_token: str | None = None,
    failed_plan_token: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _PRESENTATION_SCHEMA_VERSION,
        "ok": False,
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
        "error": {
            "code": str(status),
            "message": "presentation plan request could not be applied",
        },
    }
    if plan_token is not None:
        payload["plan_token"] = str(plan_token)
    if failed_plan_token is not None:
        payload["failed_plan_token"] = str(failed_plan_token)
    return _presentation_response(payload)


def _current_presentation_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> tuple[Any | None, str | None]:
    row = conn.execute(
        """
        SELECT
            user_state,
            final_state,
            user_char_length,
            final_char_length,
            is_current
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    ).fetchone()
    if row is None:
        return None, "content_revision_not_found"
    if int(row[4] or 0) != 1:
        return row, "revision_conflict"
    states = (str(row[0]), str(row[1]))
    if "known_incomplete" in states or "complete" not in states:
        return row, "content_known_incomplete"
    return row, None


def _presentation_plan_row_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    plan_token: str,
) -> Any | None:
    return conn.execute(
        """
        SELECT
            id,
            turn_id,
            content_revision,
            presentation_version,
            part_count,
            state,
            replaces_plan_token,
            generation,
            recovers_plan_token
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND plan_token = ?
        """,
        (str(host_id), str(name), str(plan_token)),
    ).fetchone()


def _presentation_accepted_parts_conn(
    conn: sqlite3.Connection,
    plan_id: int,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM turn_presentation_jobs
        WHERE plan_id = ? AND operation = 'upsert'
        """,
        (int(plan_id),),
    ).fetchone()
    return int(row[0] or 0)


def prepare_connector_plan_begin(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    turn_id: str,
    content_revision: str,
    presentation_version: str,
    part_count: int,
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently begin one bounded range-only presentation plan."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count < 1
        or part_count > _PRESENTATION_MAX_PARTS
        or not _valid_presentation_label(turn_id, prefix="turn-")
        or not _valid_presentation_opaque(content_revision, "twrev1.")
        or not _valid_presentation_label(presentation_version)
    ):
        return _presentation_error("invalid_params", host_id=host_id, name=name)
    count = part_count
    token = _presentation_plan_token(
        host_id=str(host_id),
        name=str(name),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision),
        presentation_version=str(presentation_version),
        part_count=count,
    )
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                )
            existing = conn.execute(
                """
                SELECT
                    id,
                    plan_token,
                    part_count,
                    state,
                    turn_id,
                    content_revision,
                    presentation_version
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                  AND generation = 1
                """,
                (
                    str(host_id),
                    str(name),
                    str(turn_id),
                    str(content_revision),
                    str(presentation_version),
                ),
            ).fetchone()
            if existing is not None:
                if (
                    int(existing[2]) != count
                    or str(existing[1]) != token
                    or str(existing[4]) != str(turn_id)
                    or str(existing[5]) != str(content_revision)
                    or str(existing[6]) != str(presentation_version)
                ):
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                    )
                accepted = _presentation_accepted_parts_conn(conn, int(existing[0]))
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": token,
                        "state": str(existing[3]),
                        "part_count": count,
                        "accepted_parts": accepted,
                        "generation": 1,
                    }
                )
            cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'preparing', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    token,
                    str(turn_id),
                    str(content_revision),
                    str(presentation_version),
                    count,
                    created_at,
                ),
            )
            plan_id = int(cursor.lastrowid)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": token,
                    "state": "preparing",
                    "part_count": count,
                    "accepted_parts": 0,
                    "generation": 1,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _validate_presentation_spans(
    spans: Iterable[Mapping[str, Any]],
    *,
    revision_row: Any,
) -> list[dict[str, Any]] | None:
    normalized: list[dict[str, Any]] = []
    prior_rank = -1
    prior_end_by_field: dict[str, int] = {}
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    for raw in spans:
        if not isinstance(raw, Mapping):
            return None
        field = str(raw.get("field") or "")
        start = raw.get("start_char")
        end = raw.get("end_char")
        if (
            field not in _PRESENTATION_FIELD_RANK
            or states[field] != "complete"
            or isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or start < 0
            or end <= start
            or end > lengths[field]
        ):
            return None
        rank = _PRESENTATION_FIELD_RANK[field]
        if rank < prior_rank or start < prior_end_by_field.get(field, 0):
            return None
        prior_rank = rank
        prior_end_by_field[field] = end
        normalized.append(
            {
                "field": field,
                "start_char": int(start),
                "end_char": int(end),
            }
        )
    if not normalized or len(normalized) > _PRESENTATION_MAX_SPANS_PER_PART:
        return None
    return normalized


def prepare_connector_plan_part(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    ordinal: int,
    spans: Iterable[Mapping[str, Any]],
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently stage one ordinal's bounded canonical coordinate ranges."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
        or isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or not isinstance(spans, list | tuple)
        or not spans
        or len(spans) > _PRESENTATION_MAX_SPANS_PER_PART
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            part_count = int(plan[4])
            if ordinal < 0 or ordinal >= part_count:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row = conn.execute(
                """
                SELECT
                    user_state,
                    final_state,
                    user_char_length,
                    final_char_length,
                    is_current
                FROM turn_content_revisions
                WHERE host_id = ? AND turn_id = ? AND content_revision = ?
                """,
                (str(host_id), str(plan[1]), str(plan[2])),
            ).fetchone()
            if revision_row is None:
                conn.rollback()
                return _presentation_error(
                    "content_revision_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            normalized = _validate_presentation_spans(spans, revision_row=revision_row)
            if normalized is None:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            encoded = _canonical_json(normalized)
            existing = conn.execute(
                """
                SELECT spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert' AND part_ordinal = ?
                """,
                (plan_id, int(ordinal)),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != encoded:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        plan_token=plan_token,
                    )
            elif str(plan[5]) != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'upsert', ?, ?, ?)
                    """,
                    (
                        plan_id,
                        int(ordinal),
                        int(ordinal),
                        encoded,
                        created_at,
                    ),
                )
            accepted = _presentation_accepted_parts_conn(conn, plan_id)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "ordinal": int(ordinal),
                    "accepted_parts": accepted,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _presentation_exact_coverage(
    staged_rows: Iterable[Any],
    *,
    revision_row: Any,
) -> bool:
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    cursors: dict[str, int] = {}
    last_rank = -1
    for row in staged_rows:
        spans = json.loads(str(row[2]))
        for span in spans:
            field = str(span["field"])
            start = int(span["start_char"])
            end = int(span["end_char"])
            rank = _PRESENTATION_FIELD_RANK[field]
            if rank < last_rank:
                return False
            if rank > last_rank and last_rank >= 0:
                prior_field = _PRESENTATION_FIELDS[last_rank]
                if cursors.get(prior_field, 0) != lengths[prior_field]:
                    return False
            if states[field] != "complete" or start != cursors.get(field, 0):
                return False
            cursors[field] = end
            last_rank = rank
    return bool(cursors) and all(
        end == lengths[field] for field, end in cursors.items()
    )


def _materialize_connector_plan_job_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
    job_id: int,
    host_id: str,
    name: str,
    delivery_key: str,
    payload: Mapping[str, Any],
    created_at: str,
) -> int:
    cursor = conn.execute(
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
        ) VALUES (?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        """,
        (
            str(host_id),
            str(name),
            str(delivery_key),
            _canonical_json(dict(payload)),
            str(created_at),
            str(created_at),
        ),
    )
    outbox_id = int(cursor.lastrowid)
    conn.execute(
        """
        UPDATE turn_presentation_jobs
        SET outbox_id = ?
        WHERE id = ? AND plan_id = ? AND outbox_id IS NULL
        """,
        (outbox_id, int(job_id), int(plan_id)),
    )
    return outbox_id


def _mark_obsolete_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    turn_id: str,
    keep_plan_id: int,
    now: str,
) -> bool:
    obsolete = conn.execute(
        """
        SELECT id
        FROM turn_presentation_plans
        WHERE host_id = ?
          AND name = ?
          AND turn_id = ?
          AND id != ?
          AND state IN ('preparing', 'waiting_predecessor', 'active', 'completed')
        """,
        (str(host_id), str(name), str(turn_id), int(keep_plan_id)),
    ).fetchall()
    obsolete_ids = [int(row[0]) for row in obsolete]
    if not obsolete_ids:
        return False
    placeholders = ",".join("?" for _ in obsolete_ids)
    conn.execute(
        f"""
        UPDATE turn_presentation_plans
        SET state = 'superseded'
        WHERE id IN ({placeholders})
        """,
        obsolete_ids,
    )
    conn.execute(
        f"""
        UPDATE connector_outbox
        SET status = 'superseded',
            next_attempt_at = NULL,
            updated_at = ?
        WHERE id IN (
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id IN ({placeholders}) AND outbox_id IS NOT NULL
        )
          AND status IN ('queued', 'retry', 'deferred')
        """,
        (str(now), *obsolete_ids),
    )
    leased = conn.execute(
        f"""
        SELECT outbox.id, outbox.private_state_json
        FROM connector_outbox AS outbox
        JOIN turn_presentation_jobs AS jobs ON jobs.outbox_id = outbox.id
        WHERE jobs.plan_id IN ({placeholders}) AND outbox.status = 'leased'
        """,
        obsolete_ids,
    ).fetchall()
    for outbox_id, private_state_json in leased:
        state = _json_object(private_state_json)
        state["terminal_after_lease"] = True
        conn.execute(
            "UPDATE connector_outbox SET private_state_json = ? WHERE id = ?",
            (_canonical_json(state), int(outbox_id)),
        )
    return bool(leased)


def _activate_waiting_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    now: str,
) -> int:
    cursor = conn.execute(
        """
        UPDATE turn_presentation_plans AS waiting
        SET state = 'active', activated_at = COALESCE(activated_at, ?)
        WHERE waiting.host_id = ?
          AND waiting.name = ?
          AND waiting.state = 'waiting_predecessor'
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS older
              JOIN turn_presentation_jobs AS jobs ON jobs.plan_id = older.id
              JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
              WHERE older.host_id = waiting.host_id
                AND older.name = waiting.name
                AND older.turn_id = waiting.turn_id
                AND older.id != waiting.id
                AND outbox.status = 'leased'
          )
        """,
        (str(now), str(host_id), str(name)),
    )
    return int(cursor.rowcount or 0)


def _update_presentation_plan_after_outbox_conn(
    conn: sqlite3.Connection,
    *,
    outbox_id: int,
    outbox_status: str,
    now: str,
) -> None:
    plan = conn.execute(
        """
        SELECT plans.id, plans.host_id, plans.name, plans.state
        FROM turn_presentation_jobs AS jobs
        JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
        WHERE jobs.outbox_id = ?
        """,
        (int(outbox_id),),
    ).fetchone()
    if plan is not None:
        plan_id = int(plan[0])
        if outbox_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS and str(plan[3]) in {
            "active",
            "waiting_predecessor",
        }:
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = 'failed'
                WHERE id = ?
                """,
                (plan_id,),
            )
        elif outbox_status == _CONNECTOR_TERMINAL_OUTBOX_STATUS and str(plan[3]) == "active":
            remaining = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ? AND outbox.status != 'delivered'
                """,
                (plan_id,),
            ).fetchone()
            if int(remaining[0] or 0) == 0:
                conn.execute(
                    """
                    UPDATE turn_presentation_plans
                    SET state = 'completed', completed_at = COALESCE(completed_at, ?)
                    WHERE id = ? AND state = 'active'
                    """,
                    (str(now), plan_id),
                )
        _activate_waiting_presentation_plans_conn(
            conn,
            host_id=str(plan[1]),
            name=str(plan[2]),
            now=str(now),
        )


def _mark_exhausted_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> None:
    params: list[Any] = [str(host_id)]
    connector_clause = ""
    if name is not None:
        connector_clause = "AND plans.name = ?"
        params.append(str(name))
    conn.execute(
        f"""
        UPDATE turn_presentation_plans AS plans
        SET state = 'failed'
        WHERE plans.host_id = ?
          {connector_clause}
          AND plans.state IN ('active', 'waiting_predecessor')
          AND EXISTS (
              SELECT 1
              FROM turn_presentation_jobs AS jobs
              JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
              WHERE jobs.plan_id = plans.id AND outbox.status = 'dead_letter'
          )
        """,
        params,
    )


def prepare_connector_plan_commit(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically validate exact coverage and materialize one plan's ordered jobs."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            plan_state = str(plan[5])
            job_count_row = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND outbox_id IS NOT NULL
                """,
                (plan_id,),
            ).fetchone()
            materialized_job_count = int(job_count_row[0] or 0)
            if materialized_job_count > 0:
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": str(plan_token),
                        "state": plan_state,
                        "job_count": materialized_job_count,
                        "generation": int(plan[7]),
                    }
                )
            if plan_state != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(plan[1]),
                content_revision_value=str(plan[2]),
            )
            if revision_error is not None or revision_row is None:
                conn.rollback()
                return _presentation_error(
                    revision_error or "revision_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            staged = conn.execute(
                """
                SELECT id, part_ordinal, spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert'
                ORDER BY part_ordinal
                """,
                (plan_id,),
            ).fetchall()
            expected_count = int(plan[4])
            if (
                len(staged) != expected_count
                or [int(row[1]) for row in staged] != list(range(expected_count))
                or not _presentation_exact_coverage(staged, revision_row=revision_row)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_incomplete",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            predecessor = conn.execute(
                """
                SELECT id, plan_token
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            replaces_token = str(predecessor[1]) if predecessor is not None else None
            completed_baseline = conn.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND completed_at IS NOT NULL
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            baseline_id = int(completed_baseline[0] or 0)
            footprint = conn.execute(
                """
                SELECT COALESCE(MAX(part_count), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                  AND id >= ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(plan[1]),
                    plan_id,
                    baseline_id,
                ),
            ).fetchone()
            prior_part_count = int(footprint[0] or 0)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": str(plan_token),
                "content_revision": str(plan[2]),
                "presentation_version": str(plan[3]),
                "part_count": expected_count,
                "replaces_plan_token": replaces_token,
            }
            for job_id, part_ordinal, spans_json in staged:
                sequence = int(part_ordinal)
                payload = {
                    **common,
                    "operation": "upsert",
                    "sequence_index": sequence,
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(job_id),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            sequence = expected_count
            for old_ordinal in range(prior_part_count - 1, expected_count - 1, -1):
                cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'retire', ?, '[]', ?)
                    """,
                    (plan_id, sequence, int(old_ordinal), current_time),
                )
                payload = {
                    **common,
                    "operation": "retire",
                    "sequence_index": sequence,
                    "part_ordinal": int(old_ordinal),
                    "spans": [],
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
                sequence += 1
            leased_predecessor = _mark_obsolete_presentation_plans_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                turn_id=str(plan[1]),
                keep_plan_id=plan_id,
                now=current_time,
            )
            next_state = "waiting_predecessor" if leased_predecessor else "active"
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = ?,
                    replaces_plan_token = ?,
                    activated_at = CASE WHEN ? = 'active' THEN ? ELSE NULL END
                WHERE id = ? AND state = 'preparing'
                """,
                (
                    next_state,
                    replaces_token,
                    next_state,
                    current_time,
                    plan_id,
                ),
            )
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "state": next_state,
                    "job_count": sequence,
                    "generation": int(plan[7]),
                }
            )
        except Exception:
            conn.rollback()
            raise



def _presentation_recovery_result(
    *,
    failed_plan_token: str,
    plan_token: str,
    generation: int,
    content_revision: str,
    acknowledged_prefix_count: int,
    executable_job_count: int,
    retained_failed_job_count: int,
    prior_attempt_count: int,
    idempotent_replay: bool,
) -> dict[str, Any]:
    return _presentation_response(
        {
            "schema_version": _PRESENTATION_SCHEMA_VERSION,
            "ok": True,
            "status": "recovered",
            "failed_plan_token": str(failed_plan_token),
            "plan_token": str(plan_token),
            "generation": int(generation),
            "content_revision": str(content_revision),
            "state": "active",
            "acknowledged_prefix_count": int(acknowledged_prefix_count),
            "executable_job_count": int(executable_job_count),
            "retained_failed_job_count": int(retained_failed_job_count),
            "prior_attempt_count": int(prior_attempt_count),
            "idempotent_replay": bool(idempotent_replay),
        }
    )


def prepare_connector_plan_recover(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    failed_plan_token: str,
    request_id: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Explicitly replace one failed immutable plan with its unfinished suffix."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(failed_plan_token, "twplan1.")
        or not _valid_presentation_label(request_id)
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            prior_request = conn.execute(
                """
                SELECT
                    audit.failed_plan_token,
                    audit.recovered_plan_token,
                    audit.generation,
                    plans.content_revision,
                    audit.delivered_prefix_count,
                    audit.fresh_job_count,
                    audit.retained_failed_job_count,
                    audit.prior_attempt_count
                FROM turn_presentation_recoveries AS audit
                JOIN turn_presentation_plans AS plans
                  ON plans.id = audit.recovered_plan_id
                WHERE audit.host_id = ? AND audit.name = ? AND audit.request_id = ?
                """,
                (str(host_id), str(name), str(request_id)),
            ).fetchone()
            if prior_request is not None:
                if str(prior_request[0]) != str(failed_plan_token):
                    conn.rollback()
                    return _presentation_error(
                        "request_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                conn.commit()
                return _presentation_recovery_result(
                    failed_plan_token=str(prior_request[0]),
                    plan_token=str(prior_request[1]),
                    generation=int(prior_request[2]),
                    content_revision=str(prior_request[3]),
                    acknowledged_prefix_count=int(prior_request[4]),
                    executable_job_count=int(prior_request[5]),
                    retained_failed_job_count=int(prior_request[6]),
                    prior_attempt_count=int(prior_request[7]),
                    idempotent_replay=True,
                )
            failed = conn.execute(
                """
                SELECT
                    id,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state
                FROM turn_presentation_plans
                WHERE host_id = ? AND name = ? AND plan_token = ?
                """,
                (str(host_id), str(name), str(failed_plan_token)),
            ).fetchone()
            if failed is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            if str(failed[6]) != "failed":
                conn.rollback()
                return _presentation_error(
                    "plan_not_failed",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(failed[1]),
                content_revision_value=str(failed[2]),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            latest_generation = conn.execute(
                """
                SELECT MAX(generation)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(failed[1]),
                    str(failed[2]),
                    str(failed[3]),
                ),
            ).fetchone()
            if int(latest_generation[0] or 0) != int(failed[4]):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            inherited_audit = conn.execute(
                """
                SELECT
                    delivered_prefix_count,
                    retained_failed_job_count,
                    prior_attempt_count
                FROM turn_presentation_recoveries
                WHERE recovered_plan_id = ?
                """,
                (int(failed[0]),),
            ).fetchone()
            inherited_prefix_count = (
                int(inherited_audit[0]) if inherited_audit is not None else 0
            )
            inherited_failed_count = (
                int(inherited_audit[1]) if inherited_audit is not None else 0
            )
            inherited_attempt_count = (
                int(inherited_audit[2]) if inherited_audit is not None else 0
            )
            source_jobs = conn.execute(
                """
                SELECT
                    jobs.sequence_index,
                    jobs.operation,
                    jobs.part_ordinal,
                    jobs.spans_json,
                    outbox.delivery_key,
                    outbox.status,
                    outbox.payload_json
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ?
                ORDER BY jobs.sequence_index
                """,
                (int(failed[0]),),
            ).fetchall()
            if (
                not source_jobs
                or [int(row[0]) for row in source_jobs]
                != list(
                    range(
                        inherited_prefix_count,
                        inherited_prefix_count + len(source_jobs),
                    )
                )
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            local_acknowledged_count = 0
            for source_job in source_jobs:
                if str(source_job[5]) != "delivered":
                    break
                local_acknowledged_count += 1
            acknowledged_prefix_count = (
                inherited_prefix_count + local_acknowledged_count
            )
            suffix = source_jobs[local_acknowledged_count:]
            retained_failed_job_count = inherited_failed_count + sum(
                str(row[5]) == "dead_letter" for row in suffix
            )
            if (
                not suffix
                or retained_failed_job_count <= inherited_failed_count
                or any(str(row[5]) in {"delivered", "leased"} for row in suffix)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            current_attempt_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM connector_deliveries AS deliveries
                    JOIN turn_presentation_jobs AS jobs
                      ON jobs.outbox_id = deliveries.outbox_id
                    WHERE jobs.plan_id = ?
                    """,
                    (int(failed[0]),),
                ).fetchone()[0]
                or 0
            )
            prior_attempt_count = inherited_attempt_count + current_attempt_count
            if current_attempt_count < 1:
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            generation = int(failed[4]) + 1
            recovered_token = _presentation_recovery_token(
                host_id=str(host_id),
                name=str(name),
                failed_plan_token=str(failed_plan_token),
                request_id=str(request_id),
                generation=generation,
            )
            plan_cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    replaces_plan_token,
                    recovers_plan_token,
                    created_at,
                    activated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(name),
                    recovered_token,
                    str(failed[1]),
                    str(failed[2]),
                    str(failed[3]),
                    generation,
                    int(failed[5]),
                    str(failed_plan_token),
                    str(failed_plan_token),
                    current_time,
                    current_time,
                ),
            )
            recovered_plan_id = int(plan_cursor.lastrowid)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": recovered_token,
                "content_revision": str(failed[2]),
                "presentation_version": str(failed[3]),
                "part_count": int(failed[5]),
                "replaces_plan_token": str(failed_plan_token),
            }
            if local_acknowledged_count:
                common["predecessor_job_key"] = str(
                    source_jobs[local_acknowledged_count - 1][4]
                )
            elif inherited_prefix_count:
                inherited_payload = _json_object(source_jobs[0][6])
                predecessor_job_key = str(
                    inherited_payload.get("predecessor_job_key") or ""
                )
                if not predecessor_job_key:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                common["predecessor_job_key"] = predecessor_job_key
            for (
                sequence_index,
                operation,
                part_ordinal,
                spans_json,
                _key,
                _status,
                _payload_json,
            ) in suffix:
                job_cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recovered_plan_id,
                        int(sequence_index),
                        str(operation),
                        int(part_ordinal),
                        str(spans_json),
                        current_time,
                    ),
                )
                payload = {
                    **common,
                    "operation": str(operation),
                    "sequence_index": int(sequence_index),
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=recovered_plan_id,
                    job_id=int(job_cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{recovered_token}:"
                        f"{int(sequence_index):0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            conn.execute(
                """
                INSERT INTO turn_presentation_recoveries (
                    host_id,
                    name,
                    request_id,
                    failed_plan_id,
                    recovered_plan_id,
                    failed_plan_token,
                    recovered_plan_token,
                    generation,
                    source_job_count,
                    delivered_prefix_count,
                    fresh_job_count,
                    retained_failed_job_count,
                    prior_attempt_count,
                    outcome,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'recovered', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    str(request_id),
                    int(failed[0]),
                    recovered_plan_id,
                    str(failed_plan_token),
                    recovered_token,
                    generation,
                    len(source_jobs),
                    acknowledged_prefix_count,
                    len(suffix),
                    retained_failed_job_count,
                    prior_attempt_count,
                    current_time,
                ),
            )
            conn.commit()
            return _presentation_recovery_result(
                failed_plan_token=str(failed_plan_token),
                plan_token=recovered_token,
                generation=generation,
                content_revision=str(failed[2]),
                acknowledged_prefix_count=acknowledged_prefix_count,
                executable_job_count=len(suffix),
                retained_failed_job_count=retained_failed_job_count,
                prior_attempt_count=prior_attempt_count,
                idempotent_replay=False,
            )
        except Exception:
            conn.rollback()
            raise


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
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name),
            "items": [],
        })
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
                _mark_exhausted_presentation_plans_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    now=current_time,
                )
            rows = conn.execute(
                """
                SELECT
                    outbox.id,
                    outbox.delivery_key,
                    outbox.payload_json,
                    outbox.private_state_json
                FROM connector_outbox AS outbox
                WHERE outbox.host_id = ?
                  AND outbox.connector = ?
                  AND outbox.status IN ('queued', 'deferred', 'retry')
                  AND (
                      outbox.next_attempt_at IS NULL
                      OR outbox.next_attempt_at = ''
                      OR outbox.next_attempt_at <= ?
                  )
                  AND (
                      NOT EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS linked
                          WHERE linked.outbox_id = outbox.id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS current_job
                          JOIN turn_presentation_plans AS current_plan
                            ON current_plan.id = current_job.plan_id
                          WHERE current_job.outbox_id = outbox.id
                            AND current_plan.state = 'active'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM turn_presentation_jobs AS predecessor
                                JOIN connector_outbox AS predecessor_outbox
                                  ON predecessor_outbox.id = predecessor.outbox_id
                                WHERE predecessor.plan_id = current_job.plan_id
                                  AND predecessor.sequence_index
                                      < current_job.sequence_index
                                  AND predecessor_outbox.status != 'delivered'
                            )
                      )
                  )
                ORDER BY outbox.id
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
                        _canonical_json(sanitize_public_mapping({})),
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
                        "payload": _restore_presentation_tokens(
                            sanitize_public_mapping(
                                _json_object(row[2]),
                                backend_neutral=True,
                            ),
                            _json_object(row[2]),
                        ),
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    result = dict(sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name),
        "items": items,
    }))
    sanitized_items = result.get("items")
    if isinstance(sanitized_items, list):
        for sanitized_item, original_item in zip(sanitized_items, items, strict=True):
            if not isinstance(sanitized_item, dict):
                continue
            sanitized_payload = sanitized_item.get("payload")
            original_payload = original_item.get("payload")
            if isinstance(sanitized_payload, dict) and isinstance(original_payload, Mapping):
                _restore_presentation_tokens(sanitized_payload, original_payload)
    return result


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
    if not _sqlite_store_exists(db_path):
        return _connector_error_response(status="store_unavailable", host_id=host_id, name=name, ref=ref)
    current_time = _connector_now(now)
    sanitized_response = sanitize_public_mapping(response or {}, backend_neutral=True)
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
                    sanitize_public_value({
                        "schema_version": 1,
                        "status": "acknowledged",
                        "response": dict(sanitized_response),
                    })
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
                migration_group = str(
                    _json_object(row[9]).get("migration_group") or ""
                )
                if migration_group:
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET status = ?, next_attempt_at = NULL, updated_at = ?
                        WHERE id != ? AND status IN ('queued', 'retry', 'deferred')
                          AND json_extract(private_state_json, '$.migration_group') = ?
                        """,
                        (
                            _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
                            current_time,
                            outbox_id,
                            migration_group,
                        ),
                    )
                    leased_siblings = conn.execute(
                        """
                        SELECT id, private_state_json
                        FROM connector_outbox
                        WHERE id != ? AND status = 'leased'
                          AND json_extract(
                              private_state_json, '$.migration_group'
                          ) = ?
                        """,
                        (outbox_id, migration_group),
                    ).fetchall()
                    for sibling_id, sibling_private in leased_siblings:
                        conn.execute(
                            """
                            UPDATE connector_outbox
                            SET private_state_json = ?
                            WHERE id = ? AND status = 'leased'
                            """,
                            (
                                _migration_private_state(
                                    sibling_private,
                                    group=migration_group,
                                    canonical=False,
                                    terminal_after_lease=True,
                                ),
                                int(sibling_id),
                            ),
                        )
                _update_presentation_plan_after_outbox_conn(
                    conn,
                    outbox_id=outbox_id,
                    outbox_status=_CONNECTOR_TERMINAL_OUTBOX_STATUS,
                    now=current_time,
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
            terminal_after_lease = bool(
                _json_object(row[9]).get("terminal_after_lease")
            )
            if terminal_after_lease:
                result_status = "superseded"
                outbox_status = _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
            response_json = _canonical_json(
                sanitize_public_value({
                    "schema_version": 1,
                    "status": result_status,
                    "reason": sanitized_reason,
                    "available_at": None if terminal_after_lease else available_at,
                    "response": dict(sanitized_response),
                })
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
                    None if exhausted or terminal_after_lease else available_at,
                    current_time,
                    _connector_private_clear_current(row[9]),
                    int(outbox_id),
                ),
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=outbox_id,
                outbox_status=outbox_status,
                now=current_time,
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
                available_at=None if exhausted or terminal_after_lease else available_at,
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
    payload_data = sanitize_public_mapping(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
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
    with _connect(db_path, prepare=True) as conn:
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


def _delete_turn_if_unreferenced_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> bool:
    """Delete one whole historical turn only when no durable delivery reference remains."""
    protected = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions AS revisions
        WHERE revisions.host_id = ? AND revisions.turn_id = ?
          AND (
              EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              OR EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = ?
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
          )
        LIMIT 1
        """,
        (str(host_id), str(turn_id), _TURN_FINAL_NAME),
    ).fetchone()
    if protected is not None:
        return False
    conn.execute(
        "DELETE FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    cursor = conn.execute(
        "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    return cursor.rowcount > 0


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
        _delete_turn_if_unreferenced_conn(conn, str(host_id), str(turn_id))


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
    return sanitize_public_value(payload)


def _attention_severity_rank(value: Any) -> int:
    return _ATTENTION_SEVERITY_RANK.get(normalize_severity(value), 0)


def _strict_utc_timestamp(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def _attention_family_key(host_id: str, item: Mapping[str, Any]) -> str:
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.lifecycle-family.v1",
            "host_id": str(host_id),
            "source": source,
            "kind": kind,
        }
    )


def _attention_observation_key(
    *,
    host_id: str,
    authority: str,
    observed_at: str,
    content_fingerprint: str,
) -> str:
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.observation.v1",
            "host_id": str(host_id),
            "authority": str(authority),
            "observed_at": observed_at,
            "snapshot_content_fingerprint": str(content_fingerprint),
        }
    )


@dataclass(frozen=True)
class _AttentionLifecycleState:
    host_id: str
    family_key: str
    generation: int
    lifecycle_status: str
    current_attention_id: str | None
    first_seen_at: str
    last_positive_at: str
    first_missing_at: str | None
    missing_observation_count: int
    last_accepted_at: str
    last_observation_key: str
    max_notified_severity_rank: int


@dataclass(frozen=True)
class _AttentionObservation:
    host_id: str
    family_key: str
    authority: str
    observed_at: str
    observation_key: str
    signal: Mapping[str, Any] | None


@dataclass(frozen=True)
class _AttentionTransition:
    action: str
    next_state: _AttentionLifecycleState | None
    upsert_signal: Mapping[str, Any] | None = None
    superseded_attention_id: str | None = None
    resolve_attention_id: str | None = None
    delivery: tuple[str, str] | None = None


def _plan_attention_transition(
    state: _AttentionLifecycleState | None,
    observation: _AttentionObservation,
) -> _AttentionTransition:
    signal = observation.signal
    if state is not None:
        if observation.observed_at < state.last_accepted_at:
            return _AttentionTransition("no-op", state)
        if observation.observed_at == state.last_accepted_at:
            return _AttentionTransition("no-op", state)

    if signal is not None:
        attention_id = _attention_id_from_item(signal)
        severity = normalize_severity(signal.get("severity"))
        severity_rank = _attention_severity_rank(severity)
        if state is None:
            next_state = _AttentionLifecycleState(
                host_id=observation.host_id,
                family_key=observation.family_key,
                generation=1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        if state.lifecycle_status == ATTENTION_LIFECYCLE_RESOLVED:
            next_state = _AttentionLifecycleState(
                host_id=state.host_id,
                family_key=state.family_key,
                generation=state.generation + 1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        escalated = severity_rank > state.max_notified_severity_rank
        next_state = _AttentionLifecycleState(
            host_id=state.host_id,
            family_key=state.family_key,
            generation=state.generation,
            lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
            current_attention_id=attention_id,
            first_seen_at=state.first_seen_at,
            last_positive_at=observation.observed_at,
            first_missing_at=None,
            missing_observation_count=0,
            last_accepted_at=observation.observed_at,
            last_observation_key=observation.observation_key,
            max_notified_severity_rank=max(
                state.max_notified_severity_rank, severity_rank
            ),
        )
        return _AttentionTransition(
            "escalate" if escalated else "update",
            next_state,
            upsert_signal=signal,
            superseded_attention_id=(
                state.current_attention_id
                if state.current_attention_id != attention_id
                else None
            ),
            delivery=(
                ("attention_escalated", f"severity:{severity}")
                if escalated
                else None
            ),
        )

    if state is None or state.lifecycle_status != ATTENTION_LIFECYCLE_OPEN:
        return _AttentionTransition("no-op", state)
    if observation.authority != "complete":
        return _AttentionTransition("no-op", state)

    first_missing_at = state.first_missing_at or observation.observed_at
    missing_count = state.missing_observation_count + 1
    elapsed = (
        datetime.fromisoformat(observation.observed_at)
        - datetime.fromisoformat(first_missing_at)
    ).total_seconds()
    resolves = (
        missing_count >= ATTENTION_MISSING_REQUIRED
        and elapsed >= ATTENTION_MISSING_GRACE_SECONDS
    )
    next_state = _AttentionLifecycleState(
        host_id=state.host_id,
        family_key=state.family_key,
        generation=state.generation,
        lifecycle_status=(
            ATTENTION_LIFECYCLE_RESOLVED
            if resolves
            else ATTENTION_LIFECYCLE_OPEN
        ),
        current_attention_id=None if resolves else state.current_attention_id,
        first_seen_at=state.first_seen_at,
        last_positive_at=state.last_positive_at,
        first_missing_at=first_missing_at,
        missing_observation_count=missing_count,
        last_accepted_at=observation.observed_at,
        last_observation_key=observation.observation_key,
        max_notified_severity_rank=state.max_notified_severity_rank,
    )
    return _AttentionTransition(
        "resolve" if resolves else (
            "start-missing" if state.first_missing_at is None else "advance-missing"
        ),
        next_state,
        resolve_attention_id=state.current_attention_id if resolves else None,
    )


def _enqueue_attention_lifecycle_job_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    event_type: str,
    stage: str,
    attention_payload: Mapping[str, Any],
    transition_at: str,
) -> None:
    transition_key = stable_fingerprint(
        {
            "domain": "tendwire.attention.transition.v1",
            "host_id": state.host_id,
            "family_key": state.family_key,
            "generation": state.generation,
            "event_type": event_type,
            "stage": stage,
        }
    )
    delivery_key = f"attention:{event_type}:{transition_key}"
    payload = sanitize_public_value(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": state.host_id,
            "attention": dict(attention_payload),
            "transition_at": transition_at,
        }
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at, next_attempt_at
        ) VALUES (?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
        """,
        (
            state.host_id,
            ATTENTION_OUTBOX_CONNECTOR,
            delivery_key,
            _canonical_json(payload),
            transition_at,
            transition_at,
        ),
    )


def _upsert_attention_projection_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    item: Mapping[str, Any],
    content_fingerprint: str,
    observed_at: str,
    prior_signal_count: int = 0,
) -> dict[str, Any]:
    item = sanitize_public_mapping(item)
    attention_id = _attention_id_from_item(item)
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    severity = normalize_severity(item.get("severity"))
    signal_status = str(item.get("status") or "unknown")
    fingerprint = str(item.get("fingerprint") or "")
    existing = conn.execute(
        """
        SELECT fingerprint, lifecycle_status, signal_count, last_changed_at,
               severity, status
        FROM attention_items
        WHERE host_id = ? AND attention_id = ?
        """,
        (state.host_id, attention_id),
    ).fetchone()
    signal_count = max(
        max(0, int(prior_signal_count)),
        0 if existing is None else max(0, int(existing[2] or 0)),
    ) + 1
    changed = (
        existing is None
        or str(existing[0] or "") != fingerprint
        or str(existing[1] or "") != ATTENTION_LIFECYCLE_OPEN
        or normalize_severity(existing[4]) != severity
        or str(existing[5] or "") != signal_status
    )
    last_changed_at = (
        observed_at if changed else str(existing[3] or observed_at)
    )
    conn.execute(
        """
        INSERT INTO attention_items (
            host_id, attention_id, source, kind, severity, status, updated_at,
            fingerprint, snapshot_content_fingerprint, observed_at,
            first_seen_at, last_seen_at, last_changed_at, resolved_at,
            lifecycle_status, resolved_reason, signal_count, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'open', NULL, ?, ?)
        ON CONFLICT(host_id, attention_id) DO UPDATE SET
            source = excluded.source,
            kind = excluded.kind,
            severity = excluded.severity,
            status = excluded.status,
            updated_at = excluded.updated_at,
            fingerprint = excluded.fingerprint,
            snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
            observed_at = excluded.observed_at,
            first_seen_at = excluded.first_seen_at,
            last_seen_at = excluded.last_seen_at,
            last_changed_at = excluded.last_changed_at,
            resolved_at = NULL,
            lifecycle_status = 'open',
            resolved_reason = NULL,
            signal_count = excluded.signal_count,
            payload_json = excluded.payload_json
        """,
        (
            state.host_id,
            attention_id,
            source,
            kind,
            severity,
            signal_status,
            item.get("updated_at"),
            fingerprint,
            str(content_fingerprint),
            observed_at,
            state.first_seen_at,
            observed_at,
            last_changed_at,
            signal_count,
            _canonical_json(dict(item)),
        ),
    )
    return _attention_lifecycle_payload(
        item,
        attention_id=attention_id,
        observed_at=observed_at,
        first_seen_at=state.first_seen_at,
        last_seen_at=observed_at,
        last_changed_at=last_changed_at,
        lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
        signal_count=signal_count,
    )


def _attention_state_from_row(row: Any) -> _AttentionLifecycleState:
    return _AttentionLifecycleState(
        host_id=str(row[0]),
        family_key=str(row[1]),
        generation=int(row[2]),
        lifecycle_status=str(row[3]),
        current_attention_id=str(row[4]) if row[4] is not None else None,
        first_seen_at=str(row[5]),
        last_positive_at=str(row[6]),
        first_missing_at=str(row[7]) if row[7] is not None else None,
        missing_observation_count=int(row[8]),
        last_accepted_at=str(row[9]),
        last_observation_key=str(row[10]),
        max_notified_severity_rank=int(row[11]),
    )


def _apply_attention_observation_conn(
    conn: sqlite3.Connection,
    *,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    content_fingerprint: str,
    observation: SnapshotObservationContext,
) -> None:
    authority = (
        observation.authority
        if observation.authority in {"none", "positive", "complete"}
        else "none"
    )
    if authority == "none":
        return
    observed_at = _strict_utc_timestamp(observation.observed_at)
    if observed_at is None:
        return
    host_id = str(snapshot.host_id)
    observation_key = _attention_observation_key(
        host_id=host_id,
        authority=authority,
        observed_at=observed_at,
        content_fingerprint=content_fingerprint,
    )

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for raw_item in payload_data.get("attention", []):
        if not isinstance(raw_item, Mapping):
            continue
        item = sanitize_public_mapping(raw_item)
        family_key = _attention_family_key(host_id, item)
        grouped.setdefault(family_key, []).append(item)

    def candidate_rank(item: Mapping[str, Any]) -> tuple[int, str, str]:
        updated_at = _strict_utc_timestamp(item.get("updated_at")) or ""
        return (
            _attention_severity_rank(item.get("severity")),
            updated_at,
            "".join(chr(0x10FFFF - ord(ch)) for ch in _attention_id_from_item(item)),
        )

    selected = {
        family_key: max(items, key=candidate_rank)
        for family_key, items in grouped.items()
    }
    rows = conn.execute(
        """
        SELECT host_id, family_key, generation, lifecycle_status,
               current_attention_id, first_seen_at, last_positive_at,
               first_missing_at, missing_observation_count, last_accepted_at,
               last_observation_key, max_notified_severity_rank
        FROM attention_lifecycles
        WHERE host_id = ?
        """,
        (host_id,),
    ).fetchall()
    states = {
        state.family_key: state
        for state in (_attention_state_from_row(row) for row in rows)
    }
    family_keys = set(selected)
    if authority == "complete":
        family_keys.update(
            key
            for key, state in states.items()
            if state.lifecycle_status == ATTENTION_LIFECYCLE_OPEN
        )

    for family_key in sorted(family_keys):
        state = states.get(family_key)
        signal = selected.get(family_key)
        transition = _plan_attention_transition(
            state,
            _AttentionObservation(
                host_id=host_id,
                family_key=family_key,
                authority=authority,
                observed_at=observed_at,
                observation_key=observation_key,
                signal=signal,
            ),
        )
        next_state = transition.next_state
        if transition.action == "no-op" or next_state is None:
            continue
        if state is None:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO attention_lifecycles (
                    host_id, family_key, generation, lifecycle_status,
                    current_attention_id, first_seen_at, last_positive_at,
                    first_missing_at, missing_observation_count, last_accepted_at,
                    last_observation_key, max_notified_severity_rank
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_state.host_id,
                    next_state.family_key,
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                ),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE attention_lifecycles
                SET generation = ?, lifecycle_status = ?,
                    current_attention_id = ?, first_seen_at = ?,
                    last_positive_at = ?, first_missing_at = ?,
                    missing_observation_count = ?, last_accepted_at = ?,
                    last_observation_key = ?, max_notified_severity_rank = ?
                WHERE host_id = ? AND family_key = ? AND generation = ?
                  AND lifecycle_status = ? AND last_accepted_at < ?
                """,
                (
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                    state.host_id,
                    state.family_key,
                    state.generation,
                    state.lifecycle_status,
                    observed_at,
                ),
            )
        if int(cursor.rowcount or 0) != 1:
            continue

        prior_signal_count = 0
        if transition.superseded_attention_id is not None:
            prior_row = conn.execute(
                """
                SELECT signal_count FROM attention_items
                WHERE host_id = ? AND attention_id = ?
                """,
                (host_id, transition.superseded_attention_id),
            ).fetchone()
            prior_signal_count = int(prior_row[0] or 0) if prior_row else 0
        if transition.superseded_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_SUPERSEDED,
                    observed_at,
                    host_id,
                    transition.superseded_attention_id,
                ),
            )
        public_payload: dict[str, Any] | None = None
        if transition.upsert_signal is not None:
            public_payload = _upsert_attention_projection_conn(
                conn,
                state=next_state,
                item=transition.upsert_signal,
                content_fingerprint=content_fingerprint,
                observed_at=observed_at,
                prior_signal_count=prior_signal_count,
            )
        if transition.resolve_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?,
                    snapshot_content_fingerprint = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_GONE,
                    observed_at,
                    str(content_fingerprint),
                    host_id,
                    transition.resolve_attention_id,
                ),
            )
        if transition.delivery is not None and public_payload is not None:
            _enqueue_attention_lifecycle_job_conn(
                conn,
                state=next_state,
                event_type=transition.delivery[0],
                stage=transition.delivery[1],
                attention_payload=public_payload,
                transition_at=observed_at,
            )
        states[family_key] = next_state


def _upsert_snapshot_projections(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    *,
    snapshot_id: int,
    content_fingerprint: str,
    private_snapshot_data: Mapping[str, Any] | None = None,
) -> None:
    private_event_snapshot = dict(
        payload_data if private_snapshot_data is None else private_snapshot_data
    )
    payload_data = sanitize_public_mapping(payload_data)
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
            "snapshot": private_event_snapshot,
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


    turn_ids: set[str] = set()
    for turn in turns_from_snapshot(snapshot):
        item = sanitize_public_mapping(turn.to_dict())
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
        _ensure_payload_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            payload=item,
            observed_at=str(observed_at) if observed_at else None,
        )
    _prune_turn_projection(conn, host_id, turn_ids)

    pending_ids: set[str] = set()
    for pending in pending_from_snapshot(snapshot):
        item = sanitize_public_mapping(pending.to_dict())
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
    if not _sqlite_store_exists(db_path) or not str(worker_id).strip() or not str(instruction_text):
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
        return sanitize_public_value({
            "request_id": str(row[0] or ""),
            "status": str(row[1] or ""),
            "created_at": str(row[3] or ""),
            "updated_at": str(row[4] or ""),
        })
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


def _backfill_legacy_attention_columns(conn: sqlite3.Connection) -> None:
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


def _migration_private_state(
    raw: Any,
    *,
    group: str,
    canonical: bool,
    terminal_after_lease: bool = False,
) -> str:
    state = _json_object(raw)
    state["migration_group"] = group
    state["migration_canonical"] = bool(canonical)
    if terminal_after_lease:
        state["terminal_after_lease"] = True
    else:
        state.pop("terminal_after_lease", None)
    return _canonical_json(state)


def _legacy_attention_job_identity(
    row_host_id: str,
    payload_json: Any,
) -> tuple[str, str, str, str, str, str | None] | None:
    payload = sanitize_public_mapping(_json_object(payload_json), backend_neutral=True)
    if str(payload.get("host_id") or "") != str(row_host_id):
        return None
    event_type = str(payload.get("event_type") or "")
    if event_type not in {"attention_created", "attention_escalated"}:
        return None
    attention = payload.get("attention")
    if not isinstance(attention, Mapping):
        return None
    source = _store_public_text(attention.get("source"), default="")
    kind = _store_public_label(attention.get("kind"))
    if not source or kind == "unknown":
        return None
    family_key = _attention_family_key(str(row_host_id), attention)
    stage = (
        "initial"
        if event_type == "attention_created"
        else f"severity:{normalize_severity(attention.get('severity'))}"
    )
    return (
        str(row_host_id),
        family_key,
        event_type,
        stage,
        _attention_id_from_item(attention),
        _strict_utc_timestamp(payload.get("transition_at")),
    )


def _migrate_v4_attention_rows_conn(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp.attention_v5_rows")
    conn.execute(
        """
        CREATE TEMP TABLE attention_v5_rows (
            host_id TEXT NOT NULL,
            family_key TEXT NOT NULL,
            attention_id TEXT NOT NULL,
            is_open INTEGER NOT NULL,
            positive_at TEXT,
            changed_at TEXT,
            first_seen_at TEXT,
            severity_rank INTEGER NOT NULL,
            signal_count INTEGER NOT NULL
        )
        """
    )
    cursor = conn.execute(
        """
        SELECT host_id, attention_id, source, kind, severity, lifecycle_status,
               updated_at, observed_at, first_seen_at, last_seen_at,
               last_changed_at, signal_count
        FROM attention_items
        ORDER BY host_id, attention_id
        """
    )
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        values: list[tuple[Any, ...]] = []
        for row in batch:
            host_id = str(row[0])
            item = {"source": row[2], "kind": row[3]}
            positive_at = (
                _strict_utc_timestamp(row[9])
                or _strict_utc_timestamp(row[7])
                or _strict_utc_timestamp(row[6])
            )
            values.append(
                (
                    host_id,
                    _attention_family_key(host_id, item),
                    str(row[1]),
                    int(str(row[5] or "open") == ATTENTION_LIFECYCLE_OPEN),
                    positive_at,
                    _strict_utc_timestamp(row[10]),
                    _strict_utc_timestamp(row[8]),
                    _attention_severity_rank(row[4]),
                    max(1, int(row[11] or 1)),
                )
            )
        conn.executemany(
            """
            INSERT INTO attention_v5_rows (
                host_id, family_key, attention_id, is_open, positive_at,
                changed_at, first_seen_at, severity_rank, signal_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    family_cursor = conn.execute(
        """
        SELECT host_id, family_key
        FROM attention_v5_rows
        GROUP BY host_id, family_key
        ORDER BY host_id, family_key
        """
    )
    while True:
        families = family_cursor.fetchmany(500)
        if not families:
            break
        for host_id_raw, family_key_raw in families:
            host_id = str(host_id_raw)
            family_key = str(family_key_raw)
            candidates = conn.execute(
                """
                SELECT attention_id, is_open, positive_at, changed_at,
                       first_seen_at, severity_rank, signal_count
                FROM attention_v5_rows
                WHERE host_id = ? AND family_key = ?
                ORDER BY attention_id
                """,
                (host_id, family_key),
            )
            winner: tuple[Any, ...] | None = None
            earliest_first: str | None = None
            latest_positive: str | None = None
            latest_progress: str | None = None
            total_signals = 0
            max_severity = -1
            while True:
                candidate_batch = candidates.fetchmany(500)
                if not candidate_batch:
                    break
                for candidate in candidate_batch:
                    total_signals += max(1, int(candidate[6] or 1))
                    max_severity = max(max_severity, int(candidate[5]))
                    if candidate[4] and (
                        earliest_first is None or str(candidate[4]) < earliest_first
                    ):
                        earliest_first = str(candidate[4])
                    if candidate[2] and (
                        latest_positive is None or str(candidate[2]) > latest_positive
                    ):
                        latest_positive = str(candidate[2])
                    progress_at = max(
                        str(candidate[2] or ""),
                        str(candidate[3] or ""),
                    )
                    if progress_at and (
                        latest_progress is None or progress_at > latest_progress
                    ):
                        latest_progress = progress_at
                    rank = (
                        progress_at,
                        int(candidate[1]),
                        str(candidate[2] or ""),
                        str(candidate[3] or ""),
                        int(candidate[5]),
                        int(candidate[6]),
                        "".join(
                            chr(0x10FFFF - ord(ch)) for ch in str(candidate[0])
                        ),
                    )
                    if winner is None or rank > winner[0]:
                        winner = (rank, *candidate)
            if winner is None or latest_positive is None:
                continue
            winner_attention_id = str(winner[1])
            is_open = bool(winner[2])
            first_seen_at = earliest_first or latest_positive
            # The lifecycle watermark (last_accepted_at) must be the newest
            # lifecycle progress — max(latest positive, latest change/resolve) —
            # not merely the latest positive. A resolved episode whose resolution
            # (t10) is newer than its last positive (t0) would otherwise seed the
            # watermark at t0, letting a delayed positive at t5 (< the authoritative
            # resolution) pass the observation guard and spuriously reopen
            # generation 2 with a fresh notification. last_positive_at stays the
            # actual latest positive; the observation key is anchored to the
            # accepted progress so replaying the authoritative resolution is a no-op.
            accepted_progress = latest_progress or latest_positive
            observation_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.observation.v1",
                    "host_id": host_id,
                    "authority": "migration",
                    "observed_at": accepted_progress,
                    "snapshot_content_fingerprint": family_key,
                }
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO attention_lifecycles (
                    host_id, family_key, generation, lifecycle_status,
                    current_attention_id, first_seen_at, last_positive_at,
                    first_missing_at, missing_observation_count, last_accepted_at,
                    last_observation_key, max_notified_severity_rank
                ) VALUES (?, ?, 1, ?, ?, ?, ?, NULL, 0, ?, ?, ?)
                """,
                (
                    host_id,
                    family_key,
                    (
                        ATTENTION_LIFECYCLE_OPEN
                        if is_open
                        else ATTENTION_LIFECYCLE_RESOLVED
                    ),
                    winner_attention_id if is_open else None,
                    first_seen_at,
                    latest_positive,
                    accepted_progress,
                    observation_key,
                    max_severity,
                ),
            )
            if is_open:
                conn.execute(
                    """
                    UPDATE attention_items
                    SET first_seen_at = ?, last_seen_at = ?,
                        signal_count = ?, lifecycle_status = 'open',
                        resolved_at = NULL, resolved_reason = NULL
                    WHERE host_id = ? AND attention_id = ?
                    """,
                    (
                        first_seen_at,
                        latest_positive,
                        total_signals,
                        host_id,
                        winner_attention_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE attention_items
                    SET lifecycle_status = 'resolved',
                        resolved_at = COALESCE(NULLIF(resolved_at, ''), ?),
                        resolved_reason = ?,
                        last_changed_at = ?
                    WHERE host_id = ? AND lifecycle_status = 'open'
                      AND attention_id != ?
                      AND attention_id IN (
                          SELECT attention_id FROM attention_v5_rows
                          WHERE host_id = ? AND family_key = ? AND is_open = 1
                      )
                    """,
                    (
                        latest_positive,
                        ATTENTION_RESOLVED_REASON_SUPERSEDED,
                        latest_positive,
                        host_id,
                        winner_attention_id,
                        host_id,
                        family_key,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE attention_items
                    SET lifecycle_status = 'resolved',
                        resolved_at = COALESCE(NULLIF(resolved_at, ''), ?),
                        resolved_reason = CASE
                            WHEN attention_id = ? THEN COALESCE(resolved_reason, 'gone')
                            ELSE ?
                        END,
                        last_changed_at = ?
                    WHERE host_id = ? AND lifecycle_status = 'open'
                      AND attention_id IN (
                          SELECT attention_id FROM attention_v5_rows
                          WHERE host_id = ? AND family_key = ? AND is_open = 1
                      )
                    """,
                    (
                        latest_positive,
                        winner_attention_id,
                        ATTENTION_RESOLVED_REASON_SUPERSEDED,
                        latest_positive,
                        host_id,
                        host_id,
                        family_key,
                    ),
                )
    conn.execute("DROP TABLE temp.attention_v5_rows")


def _migrate_v4_attention_outbox_conn(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS temp.attention_v5_jobs")
    conn.execute(
        """
        CREATE TEMP TABLE attention_v5_jobs (
            outbox_id INTEGER PRIMARY KEY,
            host_id TEXT NOT NULL,
            family_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            stage TEXT NOT NULL,
            attention_id TEXT NOT NULL,
            transition_at TEXT,
            group_key TEXT NOT NULL
        )
        """
    )
    cursor = conn.execute(
        """
        SELECT id, host_id, payload_json
        FROM connector_outbox
        WHERE connector = ?
        ORDER BY id
        """,
        (ATTENTION_OUTBOX_CONNECTOR,),
    )
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        values: list[tuple[Any, ...]] = []
        for outbox_id, host_id, payload_json in batch:
            identity = _legacy_attention_job_identity(str(host_id), payload_json)
            if identity is None:
                continue
            (
                identity_host,
                family_key,
                event_type,
                stage,
                attention_id,
                transition_at,
            ) = identity
            group_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.migration-group.v1",
                    "host_id": identity_host,
                    "family_key": family_key,
                    "generation": 1,
                    "event_type": event_type,
                    "stage": stage,
                }
            )
            values.append(
                (
                    int(outbox_id),
                    identity_host,
                    family_key,
                    event_type,
                    stage,
                    attention_id,
                    transition_at,
                    group_key,
                )
            )
            if event_type == "attention_escalated":
                severity = stage.removeprefix("severity:")
                conn.execute(
                    """
                    UPDATE attention_lifecycles
                    SET max_notified_severity_rank =
                        MAX(max_notified_severity_rank, ?)
                    WHERE host_id = ? AND family_key = ?
                    """,
                    (
                        _attention_severity_rank(severity),
                        identity_host,
                        family_key,
                    ),
                )
        conn.executemany(
            """
            INSERT INTO attention_v5_jobs (
                outbox_id, host_id, family_key, event_type, stage,
                attention_id, transition_at, group_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    group_cursor = conn.execute(
        """
        SELECT host_id, family_key, event_type, stage, group_key
        FROM attention_v5_jobs
        GROUP BY host_id, family_key, event_type, stage, group_key
        ORDER BY group_key
        """
    )
    while True:
        groups = group_cursor.fetchmany(500)
        if not groups:
            break
        for host_id, family_key, event_type, stage, group_key in groups:
            candidate_rows = conn.execute(
                """
                SELECT o.id, o.status, o.payload_json, o.private_state_json,
                       j.attention_id, j.transition_at
                FROM connector_outbox o
                JOIN attention_v5_jobs j ON j.outbox_id = o.id
                WHERE j.group_key = ?
                  AND o.status IN ('queued', 'retry', 'deferred', 'leased')
                ORDER BY o.id
                """,
                (group_key,),
            ).fetchall()
            if not candidate_rows:
                continue
            lifecycle_row = conn.execute(
                """
                SELECT l.lifecycle_status, l.current_attention_id,
                       l.first_seen_at, l.last_positive_at, l.last_accepted_at,
                       i.last_changed_at, i.last_seen_at, i.observed_at,
                       i.updated_at
                FROM attention_lifecycles l
                LEFT JOIN attention_items i
                  ON i.host_id = l.host_id
                 AND i.attention_id = l.current_attention_id
                WHERE l.host_id = ? AND l.family_key = ?
                """,
                (host_id, family_key),
            ).fetchone()
            lifecycle_open = (
                lifecycle_row is not None
                and str(lifecycle_row[0]) == ATTENTION_LIFECYCLE_OPEN
                and lifecycle_row[1] is not None
            )
            current_attention_id = (
                str(lifecycle_row[1]) if lifecycle_open else ""
            )
            current_anchor_candidates = (
                [
                    canonical
                    for canonical in (
                        _strict_utc_timestamp(value)
                        # Include the lifecycle's persisted last_accepted_at
                        # (index 4) alongside the attention_items timestamps so
                        # the current-episode anchor reflects the authoritative
                        # accepted-progress watermark even when the row's own
                        # timestamps are skewed (e.g. a delayed positive).
                        for value in lifecycle_row[4:9]
                    )
                    if canonical is not None
                ]
                if lifecycle_open
                else []
            )
            current_episode_anchor = (
                max(current_anchor_candidates)
                if current_anchor_candidates
                else ""
            )
            terminalized_current_episode = False
            if lifecycle_open:
                terminal_rows = conn.execute(
                    """
                    SELECT j.attention_id, j.transition_at
                    FROM connector_outbox o
                    JOIN attention_v5_jobs j ON j.outbox_id = o.id
                    WHERE j.group_key = ?
                      AND o.status IN ('delivered', 'dead_letter')
                    """,
                    (group_key,),
                ).fetchall()
                terminalized_current_episode = bool(current_episode_anchor) and any(
                    str(terminal_attention_id) == current_attention_id
                    and bool(terminal_transition_at)
                    and str(terminal_transition_at) >= current_episode_anchor
                    for terminal_attention_id, terminal_transition_at in terminal_rows
                )

            leased_rows = [
                row
                for row in candidate_rows
                if str(row[1]) == _CONNECTOR_LEASE_STATUS
                and conn.execute(
                    """
                    SELECT 1 FROM connector_deliveries
                    WHERE outbox_id = ? AND status = 'leased'
                    LIMIT 1
                    """,
                    (int(row[0]),),
                ).fetchone()
                is not None
            ]
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = NULL
                WHERE id IN (
                    SELECT j.outbox_id FROM attention_v5_jobs j
                    WHERE j.group_key = ?
                ) AND (
                    status IN ('queued', 'retry', 'deferred')
                    OR (
                        status = 'leased'
                        AND id NOT IN (
                            SELECT d.outbox_id FROM connector_deliveries d
                            WHERE d.status = 'leased' AND d.outbox_id IS NOT NULL
                        )
                    )
                )
                """,
                (_CONNECTOR_SUPERSEDED_OUTBOX_STATUS, group_key),
            )

            def active_rank(row: Any) -> tuple[int, str, int, int]:
                return (
                    int(str(row[4]) == current_attention_id),
                    str(row[5] or ""),
                    int(str(row[1]) == _CONNECTOR_LEASE_STATUS),
                    -int(row[0]),
                )

            if not lifecycle_open or terminalized_current_episode:
                for leased_row in leased_rows:
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET private_state_json = ?
                        WHERE id = ? AND status = 'leased'
                        """,
                        (
                            _migration_private_state(
                                leased_row[3],
                                group=str(group_key),
                                canonical=False,
                                terminal_after_lease=True,
                            ),
                            int(leased_row[0]),
                        ),
                    )
                continue

            pollable_candidates = [
                row
                for row in candidate_rows
                if str(row[1]) in _CONNECTOR_POLLABLE_STATUSES
            ]
            active_candidates = [*pollable_candidates, *leased_rows]
            if not active_candidates:
                continue
            selected = max(active_candidates, key=active_rank)
            selected_id = int(selected[0])
            selected_is_lease = (
                str(selected[1]) == _CONNECTOR_LEASE_STATUS
                and any(int(row[0]) == selected_id for row in leased_rows)
            )
            for leased_row in leased_rows:
                leased_id = int(leased_row[0])
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET private_state_json = ?
                    WHERE id = ? AND status = 'leased'
                    """,
                    (
                        _migration_private_state(
                            leased_row[3],
                            group=str(group_key),
                            canonical=selected_is_lease and leased_id == selected_id,
                            terminal_after_lease=(
                                not selected_is_lease or leased_id != selected_id
                            ),
                        ),
                        leased_id,
                    ),
                )
            if selected_is_lease:
                continue
            transition_key = stable_fingerprint(
                {
                    "domain": "tendwire.attention.transition.v1",
                    "host_id": str(host_id),
                    "family_key": str(family_key),
                    "generation": 1,
                    "event_type": str(event_type),
                    "stage": str(stage),
                }
            )
            canonical_key = f"attention:{event_type}:{transition_key}"
            payload = sanitize_public_mapping(
                _json_object(selected[2]), backend_neutral=True
            )
            transition_at = str(selected[5] or "")
            if not transition_at:
                transition_at = (
                    str(lifecycle_row[4])
                    if lifecycle_row is not None
                    else "1970-01-01T00:00:00+00:00"
                )
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, NULL)
                ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
                """,
                (
                    str(host_id),
                    ATTENTION_OUTBOX_CONNECTOR,
                    canonical_key,
                    _canonical_json(payload),
                    _migration_private_state(
                        {},
                        group=str(group_key),
                        canonical=True,
                    ),
                    transition_at,
                    transition_at,
                ),
            )
    conn.execute("DROP TABLE temp.attention_v5_jobs")


def _migrate_v4_to_v5_conn(conn: sqlite3.Connection) -> None:
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 5:
        return
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 5:
            if owns_transaction:
                conn.commit()
            return
        conn.execute(CREATE_ATTENTION_LIFECYCLES_TABLE)
        for statement in CREATE_ATTENTION_LIFECYCLE_INDEXES:
            conn.execute(statement)
        _migrate_v4_attention_rows_conn(conn)
        _migrate_v4_attention_outbox_conn(conn)
        conn.execute("PRAGMA user_version = 5")
        if owns_transaction:
            conn.commit()
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise


_LEGACY_TRUNCATION_MARKER = "\n[truncated]"


def _legacy_canonical_field(value: Any) -> tuple[str | None, str]:
    text = sanitize_canonical_turn_text(value)
    if text is None or text == "":
        return None, "absent"
    state = "known_incomplete" if text.endswith(_LEGACY_TRUNCATION_MARKER) else "complete"
    return text, state


def _insert_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    segments: Iterable[Any],
) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO turn_content_page_boundaries (
            host_id,
            turn_id,
            content_revision,
            field,
            page_index,
            start_char,
            start_byte
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
                int(segment.index),
                int(segment.start_char),
                int(segment.start_byte),
            )
            for segment in segments
        ),
    )


def _insert_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    user_text: str | None,
    assistant_final_text: str | None,
    user_state: str,
    final_state: str,
    created_at: str,
    is_current: bool = True,
) -> str:
    revision = content_revision(
        str(turn_id),
        user_text,
        assistant_final_text,
        user_state,
        final_state,
    )
    user_segments = segment_canonical_text(user_text or "") if user_state == "complete" else ()
    final_segments = (
        segment_canonical_text(assistant_final_text or "")
        if final_state == "complete"
        else ()
    )
    conn.execute(
        """
        INSERT INTO turn_content_revisions (
            host_id, turn_id, content_revision, user_text, assistant_final_text,
            user_state, final_state, user_char_length, user_byte_length,
            final_char_length, final_byte_length, user_page_count,
            final_page_count, is_current, created_at, superseded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(host_id, turn_id, content_revision) DO NOTHING
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            user_text,
            assistant_final_text,
            user_state,
            final_state,
            len(user_text or ""),
            len((user_text or "").encode("utf-8")),
            len(assistant_final_text or ""),
            len((assistant_final_text or "").encode("utf-8")),
            len(user_segments),
            len(final_segments),
            int(bool(is_current)),
            str(created_at),
        ),
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="user_text",
        segments=user_segments,
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="assistant_final_text",
        segments=final_segments,
    )
    return revision


def _backfill_legacy_turn_content_conn(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT host_id, turn_id, observed_at, payload_json
        FROM turns
        ORDER BY host_id, turn_id
        """
    ).fetchall()
    for host_id, turn_id, observed_at, payload_json in rows:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        user_text, user_state = _legacy_canonical_field(payload.get("user_text"))
        final_text, final_state = _legacy_canonical_field(
            payload.get("assistant_final_text")
        )
        if user_state != "absent" or final_state != "absent":
            _insert_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                user_text=user_text,
                assistant_final_text=final_text,
                user_state=user_state,
                final_state=final_state,
                created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
            )
        for key in (
            "user_text",
            "assistant_final_text",
            "user_preview",
            "assistant_final_preview",
            "content",
        ):
            payload.pop(key, None)
        encoded = _canonical_json(payload)
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?, fingerprint = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (
                encoded,
                stable_fingerprint(payload),
                str(host_id),
                str(turn_id),
            ),
        )


def _ensure_payload_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    payload: Mapping[str, Any],
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    user_text = sanitize_canonical_turn_text(payload.get("user_text"))
    final_text = sanitize_canonical_turn_text(
        payload.get("assistant_final_text")
    )
    if user_text == "":
        user_text = None
    if final_text == "":
        final_text = None
    user_state = "complete" if user_text else "absent"
    final_state = "complete" if final_text else "absent"
    if user_state == "absent" and final_state == "absent":
        return _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            observed_at=observed_at,
        )
    _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=user_text,
        assistant_final_text=final_text,
        user_state=user_state,
        final_state=final_state,
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    return True


def _ensure_absent_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    revision = _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=None,
        assistant_final_text=None,
        user_state="absent",
        final_state="absent",
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    cursor = conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 1, superseded_at = NULL
        WHERE host_id = ?
          AND turn_id = ?
          AND content_revision = ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_content_revisions AS current_revision
              WHERE current_revision.host_id = ?
                AND current_revision.turn_id = ?
                AND current_revision.is_current = 1
          )
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            str(host_id),
            str(turn_id),
        ),
    )
    return bool(cursor.rowcount)


def _backfill_missing_turn_content_revisions_conn(
    conn: sqlite3.Connection,
) -> int:
    """Give every stored turn one stable authoritative v2 content descriptor."""
    repaired = 0
    cursor = conn.execute(
        """
        SELECT turns.host_id, turns.turn_id, turns.observed_at
        FROM turns
        WHERE NOT EXISTS (
            SELECT 1
            FROM turn_content_revisions AS revisions
            WHERE revisions.host_id = turns.host_id
              AND revisions.turn_id = turns.turn_id
              AND revisions.is_current = 1
        )
        ORDER BY turns.host_id, turns.turn_id
        """
    )
    while True:
        rows = cursor.fetchmany(500)
        if not rows:
            return repaired
        for host_id, turn_id, observed_at in rows:
            if _ensure_absent_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                observed_at=str(observed_at) if observed_at else None,
            ):
                repaired += 1


def _rebuild_v6_presentation_plans_conn(conn: sqlite3.Connection) -> None:
    """Rebuild the two bounded plan tables with generation-aware v7 keys."""
    conn.execute(
        """
        CREATE TABLE turn_presentation_plans_v7 (
            id INTEGER PRIMARY KEY,
            host_id TEXT NOT NULL,
            name TEXT NOT NULL,
            plan_token TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            content_revision TEXT NOT NULL,
            presentation_version TEXT NOT NULL,
            generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
            part_count INTEGER NOT NULL CHECK (part_count > 0),
            state TEXT NOT NULL
                CHECK (state IN (
                    'preparing',
                    'waiting_predecessor',
                    'active',
                    'completed',
                    'superseded',
                    'failed'
                )),
            replaces_plan_token TEXT,
            recovers_plan_token TEXT,
            created_at TEXT NOT NULL,
            activated_at TEXT,
            completed_at TEXT,
            UNIQUE (host_id, name, plan_token),
            UNIQUE (
                host_id,
                name,
                turn_id,
                content_revision,
                presentation_version,
                generation
            ),
            FOREIGN KEY (host_id, turn_id, content_revision)
                REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
                ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO turn_presentation_plans_v7 (
            id, host_id, name, plan_token, turn_id, content_revision,
            presentation_version, generation, part_count, state,
            replaces_plan_token, recovers_plan_token, created_at,
            activated_at, completed_at
        )
        SELECT
            id, host_id, name, plan_token, turn_id, content_revision,
            presentation_version, 1, part_count, state,
            replaces_plan_token, NULL, created_at, activated_at, completed_at
        FROM turn_presentation_plans
        ORDER BY id
        """
    )
    conn.execute(
        """
        CREATE TABLE turn_presentation_jobs_v7 (
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
                REFERENCES turn_presentation_plans_v7(id) ON DELETE CASCADE,
            FOREIGN KEY (outbox_id)
                REFERENCES connector_outbox(id) ON DELETE RESTRICT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO turn_presentation_jobs_v7 (
            id, plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        )
        SELECT
            id, plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        FROM turn_presentation_jobs
        ORDER BY id
        """
    )
    conn.execute("DROP TABLE turn_presentation_jobs")
    conn.execute("DROP TABLE turn_presentation_plans")
    conn.execute(
        "ALTER TABLE turn_presentation_plans_v7 RENAME TO turn_presentation_plans"
    )
    conn.execute(
        "ALTER TABLE turn_presentation_jobs_v7 RENAME TO turn_presentation_jobs"
    )


def _migrate_v6_to_v7_conn(conn: sqlite3.Connection) -> None:
    """Atomically add explicit failed-plan generations and immutable recovery audit."""
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 7:
        return
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 7:
            if owns_transaction:
                conn.commit()
            return
        conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
        _backfill_missing_turn_content_revisions_conn(conn)
        _backfill_missing_turn_content_page_boundaries_conn(conn)
        plan_columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(turn_presentation_plans)"
            ).fetchall()
        }
        if "generation" not in plan_columns:
            _rebuild_v6_presentation_plans_conn(conn)
        conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
        for statement in CREATE_TURN_PRESENTATION_INDEXES:
            conn.execute(statement)
        conn.execute("PRAGMA user_version = 7")
        if owns_transaction:
            conn.commit()
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise


def _migrate_v5_to_v6_conn(conn: sqlite3.Connection) -> None:
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 6:
        return
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        if int(conn.execute("PRAGMA user_version").fetchone()[0]) >= 6:
            if owns_transaction:
                conn.commit()
            return
        conn.execute(CREATE_TURN_CONTENT_REVISIONS_TABLE)
        conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
        for statement in CREATE_TURN_CONTENT_REVISION_INDEXES:
            conn.execute(statement)
        conn.execute(CREATE_TURN_PRESENTATION_PLANS_TABLE)
        conn.execute(CREATE_TURN_PRESENTATION_JOBS_TABLE)
        conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
        for statement in CREATE_TURN_PRESENTATION_INDEXES:
            conn.execute(statement)
        _backfill_legacy_turn_content_conn(conn)
        _backfill_missing_turn_content_revisions_conn(conn)
        conn.execute("PRAGMA user_version = 6")
        if owns_transaction:
            conn.commit()
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise


def _ensure_schema(conn: sqlite3.Connection) -> None:
    entered_with_transaction = conn.in_transaction
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
    _backfill_legacy_attention_columns(conn)
    for statement in CREATE_PR6_INDEXES:
        conn.execute(statement)
    _backfill_command_audit(conn)
    if (
        not entered_with_transaction
        and conn.in_transaction
        and int(conn.execute("PRAGMA user_version").fetchone()[0]) < STORE_SCHEMA_VERSION
    ):
        conn.commit()
    _migrate_v4_to_v5_conn(conn)
    conn.execute(CREATE_ATTENTION_LIFECYCLES_TABLE)
    for statement in CREATE_ATTENTION_LIFECYCLE_INDEXES:
        conn.execute(statement)
    _migrate_v5_to_v6_conn(conn)
    _migrate_v6_to_v7_conn(conn)
    conn.execute(CREATE_TURN_CONTENT_REVISIONS_TABLE)
    conn.execute(CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE)
    for statement in CREATE_TURN_CONTENT_REVISION_INDEXES:
        conn.execute(statement)
    conn.execute(CREATE_TURN_PRESENTATION_PLANS_TABLE)
    conn.execute(CREATE_TURN_PRESENTATION_JOBS_TABLE)
    conn.execute(CREATE_TURN_PRESENTATION_RECOVERIES_TABLE)
    for statement in CREATE_TURN_PRESENTATION_INDEXES:
        conn.execute(statement)


def init_store(db_path: Path) -> None:
    """Initialize or migrate the sqlite store to the current schema."""
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        _backfill_missing_turn_content_revisions_conn(conn)
        _backfill_missing_turn_content_page_boundaries_conn(conn)


def store_status(db_path: Path, host_id: str) -> dict[str, Any]:
    """Return bounded public-safe host-scoped store and outbox counts."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "counts": {},
            "outbox": {"pending": 0, "leased": 0, "terminal": 0, "by_status": {}},
        })
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
    terminal_statuses = {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
    }
    outbox = {
        "pending": sum(count for status, count in by_status.items() if status in pending_statuses),
        "leased": int(by_status.get(_CONNECTOR_LEASE_STATUS, 0)),
        "terminal": sum(count for status, count in by_status.items() if status in terminal_statuses),
        "by_status": by_status,
    }
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "counts": counts,
        "outbox": outbox,
        "last_event_at": last_event_row[0] if last_event_row is not None else None,
        "last_snapshot_at": last_snapshot_row[0] if last_snapshot_row is not None else None,
    })


def tail_event_metadata(
    db_path: Path,
    host_id: str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Return bounded event/history metadata without raw payloads."""
    row_limit = max(1, min(int(limit), 100))
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "limit": row_limit,
            "events": [],
        })
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
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "limit": row_limit,
        "events": events,
    })


_TURN_CONTENT_MAINTENANCE_BATCH = 100
_TURN_CONTENT_MAINTENANCE_BATCH_MAX = 1_000
_TURN_CONTENT_TERMINAL_PLAN_STATES = frozenset(
    {"completed", "superseded", "failed"}
)
_TURN_CONTENT_TERMINAL_OUTBOX_STATES = frozenset(
    {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
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
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "deleted": 0,
        })
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
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "deleted": deleted,
    })


def _turn_content_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    cutoff_at: str,
    batch_size: int,
) -> list[tuple[str, int]]:
    rows = conn.execute(
        """
        SELECT candidate_type, candidate_id
        FROM (
            SELECT
                'plan' AS candidate_type,
                plans.id AS candidate_id,
                CASE
                    WHEN plans.state = 'preparing' THEN plans.created_at
                    ELSE COALESCE(
                        plans.completed_at,
                        plans.activated_at,
                        plans.created_at
                    )
                END AS eligible_at
            FROM turn_presentation_plans AS plans
            WHERE plans.host_id = :host_id
              AND (
                  (
                      plans.state = 'preparing'
                      AND plans.created_at < :cutoff_at
                  )
                  OR (
                      plans.state IN ('completed', 'superseded', 'failed')
                      AND COALESCE(
                          plans.completed_at,
                          plans.activated_at,
                          plans.created_at
                      ) < :cutoff_at
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS replacement
                  WHERE replacement.host_id = plans.host_id
                    AND replacement.name = plans.name
                    AND replacement.turn_id = plans.turn_id
                    AND replacement.replaces_plan_token = plans.plan_token
                    AND replacement.state IN (
                        'preparing',
                        'waiting_predecessor',
                        'active'
                    )
              )
              AND (
                  plans.state = 'preparing'
                  OR plans.activated_at IS NULL
                  OR plans.id < COALESCE(
                      (
                          SELECT MAX(completed.id)
                          FROM turn_presentation_plans AS completed
                          WHERE completed.host_id = plans.host_id
                            AND completed.name = plans.name
                            AND completed.turn_id = plans.turn_id
                            AND completed.completed_at IS NOT NULL
                      ),
                      0
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS jobs
                  LEFT JOIN connector_outbox AS outbox
                    ON outbox.id = jobs.outbox_id
                  LEFT JOIN connector_deliveries AS deliveries
                    ON deliveries.outbox_id = outbox.id
                  WHERE jobs.plan_id = plans.id
                    AND (
                        (
                            outbox.id IS NOT NULL
                            AND (
                                plans.state = 'preparing'
                                OR outbox.status NOT IN (
                                    'delivered',
                                    'dead_letter',
                                    'superseded'
                                )
                                OR outbox.updated_at IS NULL
                                OR outbox.updated_at >= :cutoff_at
                            )
                        )
                        OR (
                            deliveries.id IS NOT NULL
                            AND (
                                deliveries.status = 'leased'
                                OR COALESCE(
                                    deliveries.delivered_at,
                                    deliveries.created_at
                                ) >= :cutoff_at
                            )
                        )
                    )
              )
            UNION ALL
            SELECT
                'revision' AS candidate_type,
                revisions.rowid AS candidate_id,
                revisions.superseded_at AS eligible_at
            FROM turn_content_revisions AS revisions
            WHERE revisions.host_id = :host_id
              AND revisions.is_current = 0
              AND revisions.superseded_at IS NOT NULL
              AND revisions.superseded_at < :cutoff_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = :turn_final_name
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turns
                  WHERE turns.host_id = revisions.host_id
                    AND turns.turn_id = revisions.turn_id
                    AND json_valid(turns.payload_json)
                    AND (
                        json_extract(
                            turns.payload_json,
                            '$.content_revision'
                        ) = revisions.content_revision
                        OR json_extract(
                            turns.payload_json,
                            '$.content.content_revision'
                        ) = revisions.content_revision
                    )
              )
        )
        ORDER BY eligible_at, candidate_type, candidate_id
        LIMIT :batch_size
        """,
        {
            "host_id": str(host_id),
            "cutoff_at": str(cutoff_at),
            "turn_final_name": _TURN_FINAL_NAME,
            "batch_size": int(batch_size),
        },
    ).fetchall()
    return [(str(row[0]), int(row[1])) for row in rows]


def _terminal_plan_reference_reason_conn(
    conn: sqlite3.Connection,
    *,
    plan: sqlite3.Row | tuple[Any, ...],
    cutoff_at: str,
) -> str | None:
    plan_id = int(plan[0])
    host_id = str(plan[1])
    name = str(plan[2])
    plan_token = str(plan[3])
    turn_id = str(plan[4])
    state = str(plan[5])
    activated_at = plan[7]
    if state == "preparing":
        unexpected_anchor = conn.execute(
            """
            SELECT 1
            FROM turn_presentation_jobs AS jobs
            LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            LEFT JOIN connector_deliveries AS deliveries
              ON deliveries.outbox_id = outbox.id
            WHERE jobs.plan_id = ?
              AND (jobs.outbox_id IS NOT NULL OR deliveries.id IS NOT NULL)
            LIMIT 1
            """,
            (plan_id,),
        ).fetchone()
        return "reference" if unexpected_anchor is not None else None
    if state not in _TURN_CONTENT_TERMINAL_PLAN_STATES:
        return "reference"
    live_replacement = conn.execute(
        """
        SELECT 1
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND replaces_plan_token = ?
          AND state IN ('preparing', 'waiting_predecessor', 'active')
        LIMIT 1
        """,
        (host_id, name, turn_id, plan_token),
    ).fetchone()
    if live_replacement is not None:
        return "replacement"
    latest_completed = conn.execute(
        """
        SELECT COALESCE(MAX(id), 0)
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND completed_at IS NOT NULL
        """,
        (host_id, name, turn_id),
    ).fetchone()
    baseline_id = int(latest_completed[0] or 0)
    if activated_at is not None and (baseline_id == 0 or plan_id >= baseline_id):
        return "failed_prefix_or_current_baseline"
    anchors = conn.execute(
        """
        SELECT
            outbox.id,
            outbox.status,
            outbox.updated_at,
            deliveries.id,
            deliveries.status,
            COALESCE(deliveries.delivered_at, deliveries.created_at)
        FROM turn_presentation_jobs AS jobs
        LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
        LEFT JOIN connector_deliveries AS deliveries
          ON deliveries.outbox_id = outbox.id
        WHERE jobs.plan_id = ?
        """,
        (plan_id,),
    ).fetchall()
    for outbox_id, outbox_status, updated_at, delivery_id, delivery_status, audit_at in anchors:
        if outbox_id is not None and (
            str(outbox_status) not in _TURN_CONTENT_TERMINAL_OUTBOX_STATES
            or not updated_at
            or str(updated_at) >= str(cutoff_at)
        ):
            return "outbox"
        if delivery_id is not None and (
            str(delivery_status) == _CONNECTOR_LEASE_STATUS
            or not audit_at
            or str(audit_at) >= str(cutoff_at)
        ):
            return "delivery"
    return None


def _delete_retained_plan_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
) -> dict[str, int]:
    outbox_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id = ? AND outbox_id IS NOT NULL
            """,
            (int(plan_id),),
        ).fetchall()
    ]
    deliveries_deleted = 0
    outbox_deleted = 0
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        deliveries_deleted = int(
            conn.execute(
                f"DELETE FROM connector_deliveries WHERE outbox_id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    jobs_deleted = int(
        conn.execute(
            "DELETE FROM turn_presentation_jobs WHERE plan_id = ?",
            (int(plan_id),),
        ).rowcount
        or 0
    )
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        outbox_deleted = int(
            conn.execute(
                f"DELETE FROM connector_outbox WHERE id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    plan_deleted = int(
        conn.execute(
            "DELETE FROM turn_presentation_plans WHERE id = ?",
            (int(plan_id),),
        ).rowcount
        or 0
    )
    return {
        "plans": plan_deleted,
        "jobs": jobs_deleted,
        "queue_anchors": outbox_deleted,
        "attempts": deliveries_deleted,
    }


def _delete_superseded_revision_conn(
    conn: sqlite3.Connection,
    *,
    revision_rowid: int,
    host_id: str,
    cutoff_at: str,
) -> bool:
    cursor = conn.execute(
        """
        DELETE FROM turn_content_revisions AS revisions
        WHERE revisions.rowid = ?
          AND revisions.host_id = ?
          AND revisions.is_current = 0
          AND revisions.superseded_at IS NOT NULL
          AND revisions.superseded_at < ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS plans
              WHERE plans.host_id = revisions.host_id
                AND plans.turn_id = revisions.turn_id
                AND plans.content_revision = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM connector_outbox AS outbox
              WHERE outbox.host_id = revisions.host_id
                AND outbox.connector = ?
                AND json_valid(outbox.payload_json)
                AND json_extract(
                    outbox.payload_json,
                    '$.content_revision'
                ) = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM turns
              WHERE turns.host_id = revisions.host_id
                AND turns.turn_id = revisions.turn_id
                AND json_valid(turns.payload_json)
                AND (
                    json_extract(
                        turns.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
                    OR json_extract(
                        turns.payload_json,
                        '$.content.content_revision'
                    ) = revisions.content_revision
                )
          )
        """,
        (
            int(revision_rowid),
            str(host_id),
            str(cutoff_at),
            _TURN_FINAL_NAME,
        ),
    )
    return bool(cursor.rowcount)


def cleanup_turn_content_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
) -> dict[str, Any]:
    """Remove old presentation anchors, then superseded canonical revisions, in one bounded batch."""
    days = max(1, int(retention_days))
    bounded_batch = max(
        1,
        min(int(batch_size), _TURN_CONTENT_MAINTENANCE_BATCH_MAX),
    )
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    empty_counts = {
        "plans": 0,
        "jobs": 0,
        "queue_anchors": 0,
        "attempts": 0,
        "revisions": 0,
    }
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "batch_size": bounded_batch,
            "examined": 0,
            "deleted": 0,
            "skipped_reference": 0,
            "deleted_rows": empty_counts,
        })
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        candidates = _turn_content_retention_candidates_conn(
            conn,
            host_id=str(host_id),
            cutoff_at=cutoff_at,
            batch_size=bounded_batch,
        )
    deleted_rows = dict(empty_counts)
    skipped_reference = 0
    deleted = 0
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for candidate_type, candidate_id in candidates:
                if candidate_type == "plan":
                    plan = conn.execute(
                        """
                        SELECT
                            id, host_id, name, plan_token, turn_id, state,
                            created_at, activated_at, completed_at
                        FROM turn_presentation_plans
                        WHERE id = ? AND host_id = ?
                          AND (
                              (state = 'preparing' AND created_at < ?)
                              OR (
                                  state IN ('completed', 'superseded', 'failed')
                                  AND COALESCE(
                                      completed_at,
                                      activated_at,
                                      created_at
                                  ) < ?
                              )
                          )
                        """,
                        (
                            int(candidate_id),
                            str(host_id),
                            cutoff_at,
                            cutoff_at,
                        ),
                    ).fetchone()
                    if plan is None:
                        skipped_reference += 1
                        continue
                    if _terminal_plan_reference_reason_conn(
                        conn,
                        plan=plan,
                        cutoff_at=cutoff_at,
                    ) is not None:
                        skipped_reference += 1
                        continue
                    plan_counts = _delete_retained_plan_conn(
                        conn,
                        plan_id=int(candidate_id),
                    )
                    if not plan_counts["plans"]:
                        skipped_reference += 1
                        continue
                    deleted += 1
                    for key, count in plan_counts.items():
                        deleted_rows[key] += int(count)
                    continue
                if _delete_superseded_revision_conn(
                    conn,
                    revision_rowid=int(candidate_id),
                    host_id=str(host_id),
                    cutoff_at=cutoff_at,
                ):
                    deleted += 1
                    deleted_rows["revisions"] += 1
                else:
                    skipped_reference += 1
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "stale_preparing_before": cutoff_at,
        "batch_size": bounded_batch,
        "examined": len(candidates),
        "deleted": deleted,
        "skipped_reference": skipped_reference,
        "deleted_rows": deleted_rows,
    })


def run_store_maintenance(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    max_outbox_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
    content_batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
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
    turn_content = cleanup_turn_content_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
        batch_size=content_batch_size,
    )
    ok = (
        bool(retention.get("ok"))
        and bool(outbox.get("ok"))
        and bool(turn_content.get("ok"))
    )
    return sanitize_public_value({
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
        "turn_content": {
            "dry_run": bool(turn_content.get("dry_run")),
            "retention_days": int(
                turn_content.get("retention_days") or retention_days
            ),
            "cutoff_at": turn_content.get("cutoff_at"),
            "stale_preparing_before": turn_content.get(
                "stale_preparing_before"
            ),
            "batch_size": int(
                turn_content.get("batch_size") or content_batch_size
            ),
            "examined": int(turn_content.get("examined") or 0),
            "deleted": int(turn_content.get("deleted") or 0),
            "skipped_reference": int(
                turn_content.get("skipped_reference") or 0
            ),
            "deleted_rows": dict(
                turn_content.get("deleted_rows")
                or {
                    "plans": 0,
                    "jobs": 0,
                    "queue_anchors": 0,
                    "attempts": 0,
                    "revisions": 0,
                }
            ),
        },
    })


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
    if not _sqlite_store_exists(db_path):
        return False
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if pending is None:
            cur = conn.execute(
                "DELETE FROM backend_pending WHERE host_id = ? AND worker_id = ?",
                (host_id, worker_id),
            )
            return cur.rowcount > 0
        payload = _canonical_json(sanitize_public_mapping(pending))
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
    if not _sqlite_store_exists(db_path):
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
            if isinstance(payload, Mapping):
                out[str(worker_id)] = sanitize_public_mapping(payload)
    return sanitize_public_mapping(out)


def prune_backend_pending(db_path: Path | str, host_id: str, live_worker_ids: Iterable[str]) -> int:
    """Delete backend_pending rows whose worker no longer has a live binding. Presence-sync only
    prunes workers still being polled; this reaps rows orphaned when a worker/pane disappears with
    a prompt still open (otherwise get_pending would surface a phantom prompt for a dead worker)."""
    if not _sqlite_store_exists(db_path):
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


def _current_turn_content_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
) -> list[tuple[Any, dict[str, Any], dict[str, Any] | None]]:
    rows = conn.execute(
        """
        SELECT
            turns.turn_id,
            turns.payload_json,
            revisions.content_revision,
            revisions.user_text,
            revisions.assistant_final_text,
            revisions.user_state,
            revisions.final_state
        FROM turns
        LEFT JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        WHERE turns.host_id = ? AND turns.worker_id = ?
        """,
        (str(host_id), str(worker_id)),
    ).fetchall()
    decoded: list[tuple[Any, dict[str, Any], dict[str, Any] | None]] = []
    for (
        turn_id,
        payload_json,
        revision,
        user_text,
        final_text,
        user_state,
        final_state,
    ) in rows:
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        payload = sanitize_public_mapping(loaded) if isinstance(loaded, Mapping) else {}
        current = (
            {
                "content_revision": str(revision),
                "user_text": user_text,
                "assistant_final_text": final_text,
                "user_state": str(user_state),
                "final_state": str(final_state),
            }
            if revision is not None
            else None
        )
        decoded.append((turn_id, payload, current))
    return decoded


def _turn_with_current_content(
    payload: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(payload)
    if current is not None:
        merged["user_text"] = current.get("user_text")
        merged["assistant_final_text"] = current.get("assistant_final_text")
    return merged


def _source_turn_matches(payload: Mapping[str, Any], incoming_source_turn: str) -> bool:
    stored = str(payload.get("source_turn_id") or "").strip()
    if not stored or not incoming_source_turn:
        return False
    candidate = Turn.from_dict({**dict(payload), "source_turn_id": incoming_source_turn})
    return candidate.source_turn_id == stored


def _merge_canonical_field(
    incoming: str | None,
    current_text: Any,
    current_state: Any,
) -> tuple[str | None, str]:
    if incoming is None or incoming == "":
        state = str(current_state or "absent")
        if state not in {"absent", "complete", "known_incomplete"}:
            state = "absent"
        return (
            str(current_text) if current_text is not None and state != "absent" else None,
            state,
        )
    return incoming, "complete"


def _retain_authoritative_completion(
    metadata: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(metadata)
    if current is None or str(current.get("final_state") or "") != "complete":
        return merged
    if merged.get("complete") is False:
        merged.pop("complete", None)
    if merged.get("has_open_turn") is True:
        merged.pop("has_open_turn", None)
    return merged


def _replace_current_turn_content_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    current: Mapping[str, Any] | None,
    incoming_user: str | None,
    incoming_final: str | None,
    current_time: str,
) -> bool:
    user_text, user_state = _merge_canonical_field(
        incoming_user,
        current.get("user_text") if current else None,
        current.get("user_state") if current else None,
    )
    final_text, final_state = _merge_canonical_field(
        incoming_final,
        current.get("assistant_final_text") if current else None,
        current.get("final_state") if current else None,
    )
    if user_state == "absent" and final_state == "absent":
        return False
    revision = content_revision(
        str(turn_id),
        user_text,
        final_text,
        user_state,
        final_state,
    )
    if current is not None and str(current.get("content_revision") or "") == revision:
        return False
    conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 0, superseded_at = ?
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        """,
        (current_time, str(host_id), str(turn_id)),
    )
    existing = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), revision),
    ).fetchone()
    if existing is None:
        _insert_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            user_text=user_text,
            assistant_final_text=final_text,
            user_state=user_state,
            final_state=final_state,
            created_at=current_time,
        )
    else:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 1, superseded_at = NULL
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (str(host_id), str(turn_id), revision),
        )
    return True


def _strip_canonical_turn_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    lightweight = dict(payload)
    for key in (
        "user_text",
        "assistant_final_text",
        "user_preview",
        "assistant_final_preview",
        "content",
    ):
        lightweight.pop(key, None)
    return lightweight


def merge_turn_content(
    db_path: Path,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> int:
    """Atomically merge authoritative turn content into immutable canonical revisions."""
    if not _sqlite_store_exists(db_path):
        return 0
    if not any(key in content for key in _TURN_CONTENT_FIELDS):
        return 0
    current_time = observed_at or utc_timestamp()
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            incoming_user = (
                sanitize_canonical_turn_text(content.get("user_text"))
                if "user_text" in content
                else None
            )
            incoming_final = (
                sanitize_canonical_turn_text(content.get("assistant_final_text"))
                if "assistant_final_text" in content
                else None
            )
            clean_content = sanitize_public_mapping(
                {
                    key: content.get(key)
                    for key in _TURN_CONTENT_FIELDS
                    if key in content
                    and key not in {"user_text", "assistant_final_text"}
                }
            )
            automation_probe = {
                **clean_content,
                "user_text": incoming_user,
                "assistant_final_text": incoming_final,
            }
            if is_internal_automation_turn_payload(automation_probe):
                conn.rollback()
                return 0
            rows = _current_turn_content_rows_conn(conn, host_id, worker_id)
            if not rows:
                conn.rollback()
                return 0
            incoming_source_turn = str(clean_content.get("source_turn_id") or "").strip()
            exact_source_rows = [
                row
                for row in rows
                if incoming_source_turn
                and _source_turn_matches(row[1], incoming_source_turn)
            ]
            scored_rows = [
                (
                    turn_id,
                    payload,
                    current,
                    _turn_with_current_content(payload, current),
                )
                for turn_id, payload, current in rows
            ]
            base_turn_id, base_payload, base_current, base_view = max(
                scored_rows,
                key=lambda row: _turn_merge_score(row[3], automation_probe),
            )
            changed = False
            if exact_source_rows:
                turn_id, payload, current = exact_source_rows[0]
                payload.update(_retain_authoritative_completion(clean_content, current))
                metadata_changed = _update_turn_row(
                    conn,
                    host_id,
                    turn_id,
                    payload,
                    current_time,
                )
                revision_changed = _replace_current_turn_content_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(turn_id),
                    current=current,
                    incoming_user=incoming_user,
                    incoming_final=incoming_final,
                    current_time=current_time,
                )
                revision_repaired = _ensure_absent_turn_content_revision_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(turn_id),
                    observed_at=current_time,
                )
                changed = metadata_changed or revision_changed or revision_repaired
            elif incoming_source_turn:
                seed = {
                    key: base_payload.get(key)
                    for key in _TURN_IDENTITY_SEED_FIELDS
                    if base_payload.get(key) is not None
                }
                if seed.get("origin_command_id") and not _turn_content_matches_origin(
                    base_view,
                    automation_probe,
                ):
                    seed.pop("origin_command_id", None)
                    if str(seed.get("source") or "") == "command":
                        seed["source"] = "snapshot"
                seed.update(clean_content)
                item = _strip_canonical_turn_payload(Turn.from_dict(seed).to_dict())
                turn_id = str(item.get("id") or "unknown")
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
                        turn_id,
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
                _replace_current_turn_content_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=turn_id,
                    current=None,
                    incoming_user=incoming_user,
                    incoming_final=incoming_final,
                    current_time=current_time,
                )
                _ensure_absent_turn_content_revision_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(turn_id),
                    observed_at=current_time,
                )
                if not str(base_payload.get("source_turn_id") or "").strip():
                    base_payload["assistant_stream_text"] = None
                    _update_turn_row(
                        conn,
                        host_id,
                        base_turn_id,
                        base_payload,
                        current_time,
                    )
                _prune_source_turn_history(conn, host_id, worker_id)
                changed = True
            else:
                payload = dict(base_payload)
                payload.update(
                    _retain_authoritative_completion(clean_content, base_current)
                )
                metadata_changed = _update_turn_row(
                    conn,
                    host_id,
                    base_turn_id,
                    payload,
                    current_time,
                )
                revision_changed = _replace_current_turn_content_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(base_turn_id),
                    current=base_current,
                    incoming_user=incoming_user,
                    incoming_final=incoming_final,
                    current_time=current_time,
                )
                revision_repaired = _ensure_absent_turn_content_revision_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(base_turn_id),
                    observed_at=current_time,
                )
                changed = metadata_changed or revision_changed or revision_repaired
            conn.commit()
            return int(changed)
        except Exception:
            conn.rollback()
            raise


def _update_turn_row(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: Any,
    payload: dict[str, Any],
    current_time: str,
) -> bool:
    item = _strip_canonical_turn_payload(Turn.from_dict(payload).to_dict())
    encoded = _canonical_json(item)
    row = conn.execute(
        """
        SELECT status, kind, updated_at, fingerprint, payload_json
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    values = (
        str(item.get("status") or "unknown"),
        str(item.get("kind") or "unknown"),
        item.get("updated_at") or (row[2] if row is not None else None) or current_time,
        str(item.get("fingerprint") or ""),
        encoded,
    )
    if row is not None and tuple(row) == values:
        return False
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
            values[0],
            values[1],
            values[2],
            values[3],
            current_time,
            values[4],
            str(host_id),
            str(turn_id),
        ),
    )
    return True


def _prune_source_turn_history(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
) -> None:
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
        if not isinstance(payload, Mapping) or not str(
            payload.get("source_turn_id") or ""
        ).strip():
            continue
        kept += 1
        if kept <= _SOURCE_TURN_HISTORY_LIMIT:
            continue
        _delete_turn_if_unreferenced_conn(conn, str(host_id), str(turn_id))


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
    item = sanitize_public_mapping(Turn(
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
    ).to_dict())
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
    with _connect(db_path, prepare=True) as conn:
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
        _ensure_payload_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            payload=item,
            observed_at=current_time,
        )
    return sanitize_public_mapping(item)


def turns_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    snapshot: Snapshot | None = None,
    schema_version: int = 1,
    work_counters: TurnContentWorkCounters | None = None,
) -> dict[str, Any]:
    """Return a negotiated bounded turn-list projection from canonical content."""
    requested_schema = int(schema_version)
    if requested_schema not in {1, TURN_LIST_SCHEMA_VERSION}:
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": "unsupported_turn_schema_version",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    if not _sqlite_store_exists(db_path):
        if snapshot is not None:
            projected = turns_payload_from_snapshot(
                snapshot,
                schema_version=TURN_LIST_SCHEMA_VERSION,
            )
            if requested_schema == TURN_LIST_SCHEMA_VERSION:
                return projected
            incompatible = any(
                field.get("availability") != "absent" and not field.get("inline")
                for turn in projected.get("turns", [])
                for field in (turn.get("content") or {}).get("fields", {}).values()
            )
            if incompatible:
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "upgrade_required",
                    "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
                }
            projected["schema_version"] = 1
            for turn in projected.get("turns", []):
                turn["schema_version"] = 1
                turn.pop("content", None)
            return projected
        return {
            "schema_version": requested_schema,
            "host_id": str(host_id),
            "updated_at": None,
            "content_fingerprint": stable_fingerprint(
                {"host_id": str(host_id), "turns": []}
            ),
            "turns": [],
            "backend_health": [],
        }
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT
                turns.payload_json,
                turns.observed_at,
                turns.turn_id,
                revisions.content_revision,
                revisions.user_state,
                revisions.user_char_length,
                revisions.user_byte_length,
                CASE WHEN revisions.user_state = 'complete'
                     THEN revisions.user_page_count ELSE 0 END,
                CASE
                    WHEN revisions.user_state = 'complete'
                     AND revisions.user_char_length BETWEEN 1 AND ?
                    THEN revisions.user_text
                END,
                CASE
                    WHEN revisions.user_state != 'absent'
                     AND NOT (
                         revisions.user_state = 'complete'
                         AND revisions.user_char_length BETWEEN 1 AND ?
                     )
                    THEN substr(revisions.user_text, 1, ?)
                END,
                revisions.final_state,
                revisions.final_char_length,
                revisions.final_byte_length,
                CASE WHEN revisions.final_state = 'complete'
                     THEN revisions.final_page_count ELSE 0 END,
                CASE
                    WHEN revisions.final_state = 'complete'
                     AND revisions.final_char_length BETWEEN 1 AND ?
                    THEN revisions.assistant_final_text
                END,
                CASE
                    WHEN revisions.final_state != 'absent'
                     AND NOT (
                         revisions.final_state = 'complete'
                         AND revisions.final_char_length BETWEEN 1 AND ?
                     )
                    THEN substr(revisions.assistant_final_text, 1, ?)
                END
            FROM turns
            LEFT JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ?
            ORDER BY
                turns.worker_id,
                COALESCE(turns.updated_at, turns.observed_at, '') DESC,
                turns.turn_id
            """,
            (
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                str(host_id),
            ),
        ).fetchall()
        if work_counters is not None:
            work_counters.list_sql_queries += 1
            work_counters.list_descriptor_rows += len(rows)
            work_counters.list_inline_chars_examined += sum(
                len(value)
                for row in rows
                for value in (row[8], row[14])
                if isinstance(value, str)
            )
            work_counters.list_preview_chars_examined += sum(
                len(value)
                for row in rows
                for value in (row[9], row[15])
                if isinstance(value, str)
            )
    if not rows and snapshot is not None:
        projected = turns_payload_from_snapshot(
            snapshot,
            schema_version=TURN_LIST_SCHEMA_VERSION,
        )
        if requested_schema == TURN_LIST_SCHEMA_VERSION:
            return projected
        incompatible = any(
            field.get("availability") != "absent" and not field.get("inline")
            for turn in projected.get("turns", [])
            for field in (turn.get("content") or {}).get("fields", {}).values()
        )
        if incompatible:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "upgrade_required",
                "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
            }
        projected["schema_version"] = 1
        for turn in projected.get("turns", []):
            turn["schema_version"] = 1
            turn.pop("content", None)
        return projected
    turns: list[dict[str, Any]] = []
    observed_values: list[str] = []
    incompatible_v1 = False
    for (
        payload_json,
        observed_at,
        turn_id,
        revision,
        user_state,
        user_char_length,
        user_byte_length,
        user_page_count,
        user_inline,
        user_preview,
        final_state,
        final_char_length,
        final_byte_length,
        final_page_count,
        final_inline,
        final_preview,
    ) in rows:
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        if not isinstance(loaded, Mapping):
            loaded = {}
        turn_payload = sanitize_public_mapping(loaded)
        if turn_payload and not is_internal_automation_turn_payload(turn_payload):
            serialized = Turn.from_dict(turn_payload).to_dict()
            item = _strip_canonical_turn_payload(serialized)
            if revision is not None:
                projection = project_persisted_turn_content(
                    str(revision),
                    user_state=str(user_state),
                    user_char_length=int(user_char_length),
                    user_byte_length=int(user_byte_length),
                    user_page_count=int(user_page_count),
                    user_inline=user_inline,
                    user_preview=user_preview,
                    final_state=str(final_state),
                    final_char_length=int(final_char_length),
                    final_byte_length=int(final_byte_length),
                    final_page_count=int(final_page_count),
                    final_inline=final_inline,
                    final_preview=final_preview,
                )
                fields = projection["content"]["fields"]
                incompatible_v1 = incompatible_v1 or any(
                    descriptor["availability"] != "absent"
                    and not descriptor["inline"]
                    for descriptor in fields.values()
                )
                item.update(projection)
            else:
                legacy_user, legacy_user_state = _legacy_canonical_field(
                    serialized.get("user_text")
                )
                legacy_final, legacy_final_state = _legacy_canonical_field(
                    serialized.get("assistant_final_text")
                )
                if requested_schema == TURN_LIST_SCHEMA_VERSION and (
                    legacy_user_state != "absent" or legacy_final_state != "absent"
                ):
                    item.update(
                        project_turn_content(
                            str(turn_id),
                            legacy_user,
                            legacy_final,
                            user_state=legacy_user_state,
                            final_state=legacy_final_state,
                        )
                    )
                elif requested_schema == 1:
                    incompatible_v1 = incompatible_v1 or (
                        legacy_user_state == "known_incomplete"
                        or legacy_final_state == "known_incomplete"
                    )
                    if legacy_user_state == "complete":
                        item["user_text"] = legacy_user
                    if legacy_final_state == "complete":
                        item["assistant_final_text"] = legacy_final
            if requested_schema == 1:
                item["schema_version"] = 1
                item.pop("content", None)
                item.pop("user_preview", None)
                item.pop("assistant_final_preview", None)
                item.setdefault("user_text", None)
                item.setdefault("assistant_final_text", None)
            turns.append(item)
        if observed_at:
            observed_values.append(str(observed_at))
    if requested_schema == 1 and incompatible_v1:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "upgrade_required",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    backend_health = sanitize_public_value(
        [health.to_dict() for health in snapshot.backend_health]
        if snapshot is not None
        else []
    )
    if not isinstance(backend_health, list):
        backend_health = []
    payload = {
        "schema_version": requested_schema,
        "host_id": str(host_id),
        "updated_at": (
            max(observed_values)
            if observed_values
            else (snapshot.updated_at if snapshot is not None else None)
        ),
        "turns": turns,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "turns": payload["turns"],
            "backend_health": payload["backend_health"],
        }
    )
    _record_response_size(work_counters, payload)
    return payload


def _bounded_utf8_blob_page(raw: bytes) -> str:
    """Decode the longest code-point-complete prefix of one bounded byte window."""
    end = len(raw)
    minimum = max(0, end - 3)
    while end >= minimum:
        try:
            return raw[:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            if exc.end != end:
                raise ValueError("invalid_canonical_utf8") from None
            end -= 1
    raise ValueError("invalid_canonical_utf8")


def _ensure_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    rowid: int,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    column: str,
    total_char_length: int,
    total_byte_length: int,
    page_count: int,
    work_counters: TurnContentWorkCounters | None,
) -> None:
    existing_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = ?
            """,
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
            ),
        ).fetchone()[0]
        or 0
    )
    if existing_count == page_count:
        return
    if existing_count:
        raise ValueError("invalid_content_metadata")
    blob = conn.blobopen(
        "turn_content_revisions",
        str(column),
        int(rowid),
        readonly=True,
    )
    try:
        if len(blob) != total_byte_length:
            raise ValueError("invalid_content_metadata")
        start_byte = 0
        start_char = 0
        page_index = 0
        while start_byte < total_byte_length:
            blob.seek(start_byte)
            raw = blob.read(
                min(
                    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                    total_byte_length - start_byte,
                )
            )
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if (
                not segment_byte_length
                or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES
            ):
                raise ValueError("invalid_content_metadata")
            conn.execute(
                """
                INSERT INTO turn_content_page_boundaries (
                    host_id,
                    turn_id,
                    content_revision,
                    field,
                    page_index,
                    start_char,
                    start_byte
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision_value),
                    str(field),
                    page_index,
                    start_char,
                    start_byte,
                ),
            )
            page_index += 1
            start_byte += segment_byte_length
            start_char += segment_char_length
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
        if (
            page_index != page_count
            or start_byte != total_byte_length
            or start_char != total_char_length
        ):
            raise ValueError("invalid_content_metadata")
    finally:
        blob.close()


def _backfill_missing_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
) -> int:
    """Stream complete legacy fields once and persist exact non-content boundaries."""
    repaired_fields = 0
    cursor = conn.execute(
        """
        SELECT
            rowid,
            host_id,
            turn_id,
            content_revision,
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count
        FROM turn_content_revisions
        WHERE (user_state = 'complete' AND user_page_count > 0)
           OR (final_state = 'complete' AND final_page_count > 0)
        ORDER BY host_id, turn_id, content_revision
        """
    )
    while True:
        rows = cursor.fetchmany(64)
        if not rows:
            return repaired_fields
        for row in rows:
            fields = (
                (
                    "user_text",
                    "user_text",
                    str(row[4]),
                    int(row[5]),
                    int(row[6]),
                    int(row[7]),
                ),
                (
                    "assistant_final_text",
                    "assistant_final_text",
                    str(row[8]),
                    int(row[9]),
                    int(row[10]),
                    int(row[11]),
                ),
            )
            for (
                field,
                column,
                state,
                total_char_length,
                total_byte_length,
                page_count,
            ) in fields:
                if state != "complete" or page_count < 1:
                    continue
                existing_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    ).fetchone()[0]
                    or 0
                )
                if existing_count == page_count:
                    continue
                if existing_count:
                    conn.execute(
                        """
                        DELETE FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    )
                _ensure_turn_content_page_boundaries_conn(
                    conn,
                    rowid=int(row[0]),
                    host_id=str(row[1]),
                    turn_id=str(row[2]),
                    content_revision_value=str(row[3]),
                    field=field,
                    column=column,
                    total_char_length=total_char_length,
                    total_byte_length=total_byte_length,
                    page_count=page_count,
                    work_counters=None,
                )
                repaired_fields += 1


def get_turn_content(
    db_path: Path,
    host_id: str,
    *,
    turn_id: str,
    content_revision: str,
    field: str,
    cursor: str | None = None,
    schema_version: int = 1,
    work_counters: TurnContentWorkCounters | None = None,
) -> dict[str, Any]:
    """Read one bounded page directly from the canonical SQLite value."""
    if schema_version != 1:
        return {
            "schema_version": int(schema_version),
            "ok": False,
            "status": "unsupported_content_schema_version",
            "required_content_schema_version": 1,
        }
    field_columns = {
        "user_text": (1, 3, 4, 5, "user_text"),
        "assistant_final_text": (2, 6, 7, 8, "assistant_final_text"),
    }
    if field not in field_columns:
        return {"schema_version": 1, "ok": False, "status": "invalid_content_field"}
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_revision_not_found",
        }
    try:
        with _connect(db_path) as conn:
            _ensure_schema(conn)
            row = conn.execute(
                """
                SELECT
                    revisions.rowid,
                    revisions.user_state,
                    revisions.final_state,
                    revisions.user_char_length,
                    revisions.user_byte_length,
                    revisions.user_page_count,
                    revisions.final_char_length,
                    revisions.final_byte_length,
                    revisions.final_page_count,
                    revisions.is_current,
                    EXISTS (
                        SELECT 1
                        FROM turn_presentation_plans AS plans
                        WHERE plans.host_id = revisions.host_id
                          AND plans.turn_id = revisions.turn_id
                          AND plans.content_revision = revisions.content_revision
                    )
                FROM turn_content_revisions AS revisions
                WHERE host_id = ? AND turn_id = ? AND content_revision = ?
                """,
                (str(host_id), str(turn_id), str(content_revision)),
            ).fetchone()
            if work_counters is not None:
                work_counters.page_sql_queries += 1
            if row is None or (not bool(row[9]) and not bool(row[10])):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_revision_not_found",
                }
            state_index, char_index, byte_index, count_index, column = field_columns[field]
            availability = str(row[state_index])
            if availability == "known_incomplete":
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_known_incomplete",
                }
            total_char_length = int(row[char_index])
            total_byte_length = int(row[byte_index])
            count = int(row[count_index])
            if (
                availability != "complete"
                or total_char_length < 1
                or total_byte_length < 1
                or count < 1
            ):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_not_available",
                }
            position = (
                ContentCursorPosition(
                    index=0,
                    segment_id=content_segment_id(content_revision, field, 0),
                    start_char=0,
                    start_byte=0,
                )
                if cursor is None
                else decode_content_cursor(
                    cursor,
                    revision=content_revision,
                    field=field,
                    count=count,
                )
            )
            _ensure_turn_content_page_boundaries_conn(
                conn,
                rowid=int(row[0]),
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
                field=str(field),
                column=column,
                total_char_length=total_char_length,
                total_byte_length=total_byte_length,
                page_count=count,
                work_counters=work_counters,
            )
            expected_boundary = conn.execute(
                """
                SELECT start_char, start_byte
                FROM turn_content_page_boundaries
                WHERE host_id = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND field = ?
                  AND page_index = ?
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision),
                    str(field),
                    position.index,
                ),
            ).fetchone()
            if expected_boundary is None:
                raise ValueError("invalid_content_metadata")
            if (
                position.start_char != int(expected_boundary[0])
                or position.start_byte != int(expected_boundary[1])
            ):
                raise ValueError("invalid_cursor")
            blob = conn.blobopen(
                "turn_content_revisions",
                column,
                int(row[0]),
                readonly=True,
            )
            try:
                if len(blob) != total_byte_length:
                    raise ValueError("invalid_content_metadata")
                blob.seek(position.start_byte)
                raw = blob.read(
                    min(
                        TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                        total_byte_length - position.start_byte,
                    )
                )
            finally:
                blob.close()
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if not segment_byte_length or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
                raise ValueError("invalid_content_metadata")
            end_byte = position.start_byte + segment_byte_length
            end_char = position.start_char + segment_char_length
            has_next = position.index + 1 < count
            if has_next:
                next_boundary = conn.execute(
                    """
                    SELECT start_char, start_byte
                    FROM turn_content_page_boundaries
                    WHERE host_id = ?
                      AND turn_id = ?
                      AND content_revision = ?
                      AND field = ?
                      AND page_index = ?
                    """,
                    (
                        str(host_id),
                        str(turn_id),
                        str(content_revision),
                        str(field),
                        position.index + 1,
                    ),
                ).fetchone()
                if (
                    next_boundary is None
                    or end_char != int(next_boundary[0])
                    or end_byte != int(next_boundary[1])
                ):
                    raise ValueError("invalid_content_metadata")
            elif (
                end_byte != total_byte_length
                or end_char != total_char_length
            ):
                raise ValueError("invalid_content_metadata")
            payload = {
                "schema_version": 1,
                "turn_id": str(turn_id),
                "content_revision": str(content_revision),
                "field": field,
                "availability": "complete",
                "segment_id": position.segment_id,
                "index": position.index,
                "count": count,
                "text": text,
                "segment_char_length": segment_char_length,
                "segment_byte_length": segment_byte_length,
                "total_char_length": total_char_length,
                "total_byte_length": total_byte_length,
                "next_cursor": (
                    content_cursor(
                        content_revision,
                        field,
                        position.index + 1,
                        start_char=end_char,
                        start_byte=end_byte,
                    )
                    if has_next
                    else None
                ),
            }
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
            _record_response_size(work_counters, payload)
            return payload
    except ValueError as exc:
        status = "invalid_cursor" if str(exc) == "invalid_cursor" else "content_not_available"
        return {"schema_version": 1, "ok": False, "status": status}
    except sqlite3.Error:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_not_available",
        }


def save_snapshot(
    db_path: Path,
    snapshot: Snapshot,
    *,
    observation: SnapshotObservationContext | None = None,
) -> None:
    """Persist a canonical snapshot and its authorized lifecycle transitions."""
    context = observation or SnapshotObservationContext()
    private_snapshot_data = _snapshot_dict(snapshot)
    public_snapshot = Snapshot.from_dict(
        sanitize_public_mapping(private_snapshot_data)
    )
    data, fingerprint = _snapshot_payload(public_snapshot.to_dict())
    payload = _canonical_json(data)
    with _connect(db_path, prepare=True, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute(
                """
                INSERT INTO snapshots (
                    host_id, created_at, content_fingerprint, payload
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    public_snapshot.host_id,
                    public_snapshot.updated_at,
                    fingerprint,
                    payload,
                ),
            )
            _upsert_snapshot_projections(
                conn,
                public_snapshot,
                data,
                snapshot_id=int(cursor.lastrowid),
                content_fingerprint=fingerprint,
                private_snapshot_data=private_snapshot_data,
            )
            _apply_attention_observation_conn(
                conn,
                snapshot=public_snapshot,
                payload_data=data,
                content_fingerprint=fingerprint,
                observation=context,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not _sqlite_store_exists(db_path):
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
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def latest_healthy_backend_snapshot(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
) -> Snapshot | None:
    """Return the newest snapshot reporting a healthy named backend."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT snapshot.payload
            FROM snapshots AS snapshot
            WHERE snapshot.host_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM json_each(snapshot.payload, '$.backend_health') AS health
                  WHERE json_extract(health.value, '$.name') = ?
                    AND json_extract(health.value, '$.status') = 'healthy'
              )
            ORDER BY snapshot.id DESC
            LIMIT 1
            """,
            (str(host_id), str(backend)),
        ).fetchone()
    if row is None:
        return None
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def _attention_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[Any]:
    columns = """
        i.attention_id,
        i.source,
        i.kind,
        i.severity,
        i.status,
        i.updated_at,
        i.fingerprint,
        i.snapshot_content_fingerprint,
        i.observed_at,
        i.payload_json,
        i.first_seen_at,
        i.last_seen_at,
        i.last_changed_at,
        i.resolved_at,
        i.lifecycle_status,
        i.resolved_reason,
        i.signal_count
    """
    if not include_resolved:
        return conn.execute(
            f"""
            SELECT {columns}
            FROM attention_lifecycles l
            JOIN attention_items i
              ON i.host_id = l.host_id
             AND i.attention_id = l.current_attention_id
            WHERE l.host_id = ? AND l.lifecycle_status = 'open'
            ORDER BY i.last_changed_at DESC, i.attention_id
            """,
            (str(host_id),),
        ).fetchall()
    return conn.execute(
        f"""
        SELECT {columns}, 0 AS sort_group
        FROM attention_lifecycles l
        JOIN attention_items i
          ON i.host_id = l.host_id
         AND i.attention_id = l.current_attention_id
        WHERE l.host_id = ? AND l.lifecycle_status = 'open'
        UNION ALL
        SELECT {columns}, 1 AS sort_group
        FROM attention_items i
        WHERE i.host_id = ?
          AND i.lifecycle_status != 'open'
          AND NOT EXISTS (
              SELECT 1 FROM attention_lifecycles l
              WHERE l.host_id = i.host_id
                AND l.current_attention_id = i.attention_id
          )
        ORDER BY sort_group, last_changed_at DESC, attention_id
        """,
        (str(host_id), str(host_id)),
    ).fetchall()


def _attention_item_from_row(row: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(row[9] or "{}")
    except (TypeError, json.JSONDecodeError):
        parsed = {}
    payload = sanitize_public_mapping(parsed)
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
    payload["reason"] = _store_public_text(
        payload.get("reason"),
        default="",
        free_text=True,
    )
    return _attention_lifecycle_payload(
        payload,
        attention_id=str(row[0] or ""),
        observed_at=str(row[8] or row[11] or ""),
        first_seen_at=str(row[10] or row[8] or ""),
        last_seen_at=str(row[11] or row[8] or ""),
        last_changed_at=str(row[12] or row[8] or ""),
        resolved_at=row[13],
        lifecycle_status=str(row[14] or ATTENTION_LIFECYCLE_OPEN),
        resolved_reason=_store_public_text(
            row[15],
            default="",
            free_text=True,
        ) or None,
        signal_count=int(row[16] or 1),
    )


def list_attention_items(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[dict[str, Any]]:
    """Return public-safe persisted attention items for a host."""
    if not _sqlite_store_exists(db_path):
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
    return sanitize_public_value([_attention_item_from_row(row) for row in rows])


def attention_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> dict[str, Any] | None:
    """Return a public attention.list payload from lifecycle rows or snapshot fallback."""
    if not _sqlite_store_exists(db_path):
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
        lifecycle_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_lifecycles WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )

    if snapshot_row is None and not rows:
        return None

    snapshot: Snapshot | None = None
    if snapshot_row is not None:
        try:
            snapshot = Snapshot.from_dict(
                sanitize_public_mapping(_json_object(snapshot_row[0]))
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = None

    attention = [_attention_item_from_row(row) for row in rows]
    backend_health = [health.to_dict() for health in snapshot.backend_health] if snapshot is not None else []
    updated_at = snapshot.updated_at if snapshot is not None else utc_timestamp()
    if (
        not attention
        and attention_row_count == 0
        and lifecycle_row_count == 0
        and snapshot is not None
        and snapshot.attention
    ):
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
    return sanitize_public_value(payload)


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not _sqlite_store_exists(db_path):
        return []
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return sanitize_public_value([row[0] for row in rows])


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
    with _connect(db_path, prepare=True) as conn:
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
    if not _sqlite_store_exists(db_path):
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
    if not _sqlite_store_exists(db_path):
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
    with _connect(db_path, prepare=True) as conn:
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
    with _connect(db_path, prepare=True) as conn:
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
    if not _sqlite_store_exists(db_path):
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
    conn = _connect(db_path, isolation_level=None, prepare=True)
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
    now = utc_timestamp()
    conn = _connect(db_path, isolation_level=None, prepare=True)
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
    return _canonical_json(envelope.to_dict())
