"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..core.commands import CommandEnvelope
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    stable_fingerprint,
    stable_json_dumps,
    utc_timestamp,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS

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


def _ensure_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _canonical_json(data: Any) -> str:
    return stable_json_dumps(data)


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
            if str(key).lower() not in {"updated_at", "content_fingerprint"}
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


def _snapshot_payload(data: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload_data = dict(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
    raw = payload_data.get("content_fingerprint")
    if not isinstance(raw, str) or not raw:
        payload_data["content_fingerprint"] = fingerprint
    return payload_data, fingerprint


def _table_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(snapshots)").fetchall()
    return {str(row[1]) for row in rows}


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
    for statement in CREATE_COMMAND_RECEIPT_INDEXES:
        conn.execute(statement)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def init_store(db_path: Path) -> None:
    """Initialize or migrate the snapshots store to schema version 2."""
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)


def save_snapshot(db_path: Path, snapshot: Snapshot) -> None:
    """Persist a canonical snapshot JSON blob in the sqlite store."""
    data, fingerprint = _snapshot_payload(_snapshot_dict(snapshot))
    payload = _canonical_json(data)
    _ensure_dir(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (snapshot.host_id, snapshot.updated_at, fingerprint, payload),
        )


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not db_path.exists():
        return None
    with sqlite3.connect(str(db_path)) as conn:
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


def list_hosts(db_path: Path) -> list[str]:
    """Return distinct host_ids seen in the store."""
    if not db_path.exists():
        return []
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT host_id FROM snapshots ORDER BY host_id"
        ).fetchall()
    return [r[0] for r in rows]


def get_command_receipt(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
) -> dict[str, Any] | None:
    """Return the latest command receipt for a host/request/action key, or None."""
    if not db_path.exists():
        return None
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
        row = conn.execute(
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
    if row is None:
        return None
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
    with sqlite3.connect(str(db_path)) as conn:
        _ensure_schema(conn)
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
                None if uncertain else now,
                int(uncertain),
            ),
        )


def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    """Serialize a command envelope for storage in a receipt."""
    return stable_json_dumps(envelope.to_dict())
