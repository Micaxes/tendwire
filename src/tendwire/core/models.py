"""Neutral data models for Tendwire snapshots.

These models are intentionally device-neutral. They contain no Telegram,
Herdres delivery, chat/topic/message ID, or connector-specific routing state.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = 2
FINGERPRINT_HEX_CHARS = 24

CANONICAL_STATUSES = frozenset(
    {"unknown", "active", "idle", "waiting", "blocked", "warning", "failed", "closed"}
)

_STATUS_ALIASES = {
    "": "unknown",
    "ok": "active",
    "okay": "active",
    "ready": "active",
    "running": "active",
    "run": "active",
    "online": "active",
    "connected": "active",
    "healthy": "active",
    "success": "active",
    "open": "active",
    "working": "active",
    "busy": "active",
    "processing": "active",
    "in-progress": "active",
    "in_progress": "active",
    "thinking": "active",
    "executing": "active",
    "responding": "waiting",
    "awaiting-input": "waiting",
    "awaiting_input": "waiting",
    "needs-input": "waiting",
    "needs_input": "waiting",
    "paused": "idle",
    "pause": "idle",
    "sleeping": "idle",
    "wait": "waiting",
    "pending": "waiting",
    "queued": "waiting",
    "queue": "waiting",
    "blocked": "blocked",
    "block": "blocked",
    "stalled": "blocked",
    "stuck": "blocked",
    "warn": "warning",
    "warning": "warning",
    "degraded": "warning",
    "error": "failed",
    "errors": "failed",
    "fail": "failed",
    "failure": "failed",
    "crashed": "failed",
    "crash": "failed",
    "panic": "failed",
    "closed": "closed",
    "complete": "closed",
    "completed": "closed",
    "done": "closed",
    "stopped": "closed",
    "exited": "closed",
    "terminated": "closed",
}

_SEVERITY_ALIASES = {
    "": "info",
    "warn": "warning",
    "warning": "warning",
    "critical": "critical",
    "error": "critical",
    "failed": "critical",
    "failure": "critical",
    "info": "info",
    "notice": "info",
    "debug": "info",
}

FORBIDDEN_FIELD_NAMES = frozenset(
    {
        "telegram",
        "chat_id",
        "topic_id",
        "message_id",
        "thread_id",
        "token",
        "bot_token",
        "delivery",
        "route",
        "herdres_delivery",
    }
)
_FORBIDDEN_FIELD_COMPACT = frozenset(name.replace("_", "") for name in FORBIDDEN_FIELD_NAMES)


def _is_forbidden_field_name(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    compact = normalized.replace("_", "")
    return normalized in FORBIDDEN_FIELD_NAMES or compact in _FORBIDDEN_FIELD_COMPACT


_SNAPSHOT_CONTENT_IGNORED_KEYS = frozenset({"updated_at", "content_fingerprint"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp(dt: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp string."""
    if dt is None:
        dt = _utc_now()
    return dt.astimezone(timezone.utc).isoformat()


def stable_json_dumps(value: Any, *, indent: int | None = None) -> str:
    """Serialize JSON deterministically for hashing and snapshot output."""
    return json.dumps(
        sanitize_forbidden_fields(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def stable_sha256(value: Any) -> str:
    """Return the SHA-256 hex digest of Tendwire's stable JSON encoding."""
    return hashlib.sha256(stable_json_dumps(value).encode("utf-8")).hexdigest()


def stable_fingerprint(value: Any, *, length: int = FINGERPRINT_HEX_CHARS) -> str:
    """Return a fixed-width stable fingerprint for Tendwire content."""
    return stable_sha256(value)[:length]


def normalize_status(status: Any) -> str:
    """Map arbitrary adapter status values into Tendwire's canonical set."""
    raw = "" if status is None else str(status).strip().lower().replace("_", "-")
    if raw in CANONICAL_STATUSES:
        return raw
    return _STATUS_ALIASES.get(raw, "unknown")


def normalize_severity(severity: Any) -> str:
    """Normalize historical attention levels into a compact severity string."""
    raw = "" if severity is None else str(severity).strip().lower().replace("_", "-")
    return _SEVERITY_ALIASES.get(raw, raw or "info")


def sanitize_forbidden_fields(value: Any) -> Any:
    """Return a JSON-safe value with connector/routing field names removed."""
    if isinstance(value, datetime):
        return utc_timestamp(value)
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if _is_forbidden_field_name(key):
                continue
            key_text = str(key)
            sanitized[key_text] = sanitize_forbidden_fields(item)
        return sanitized
    if isinstance(value, tuple | list):
        return [sanitize_forbidden_fields(item) for item in value]
    if isinstance(value, set | frozenset):
        items = [sanitize_forbidden_fields(item) for item in value]
        return sorted(items, key=stable_json_dumps)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _string_value(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return utc_timestamp(value)
    return str(value)


def _status_and_meta(status: Any, meta: Any) -> tuple[str, dict[str, Any]]:
    raw_status = _string_value(status, "unknown").strip()
    normalized = normalize_status(raw_status)
    clean_meta = sanitize_forbidden_fields(meta if isinstance(meta, Mapping) else {})
    if raw_status and raw_status.lower().replace("_", "-") != normalized:
        clean_meta["raw_status"] = raw_status
    return normalized, clean_meta


def _merge_meta(data: Mapping[str, Any], known_keys: set[str]) -> dict[str, Any]:
    explicit_meta = data.get("meta", {})
    merged: dict[str, Any] = {
        str(key): value for key, value in data.items() if str(key) not in known_keys
    }
    if isinstance(explicit_meta, Mapping):
        merged.update(explicit_meta)
    return sanitize_forbidden_fields(merged)


def _strip_snapshot_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_snapshot_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in _SNAPSHOT_CONTENT_IGNORED_KEYS
        }
    if isinstance(value, list | tuple):
        return [_strip_snapshot_content_volatile(item) for item in value]
    return value


def attention_identity_payload(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> dict[str, str]:
    """Return the stable identity payload for an attention condition."""
    return {
        "host_id": str(host_id),
        "source": str(source),
        "kind": str(kind),
        "severity": normalize_severity(severity),
        "reason": str(reason),
        "status": normalize_status(status),
    }


def attention_fingerprint(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> str:
    """Return the deterministic fingerprint for an attention condition."""
    return stable_fingerprint(
        attention_identity_payload(
            host_id=host_id,
            source=source,
            kind=kind,
            severity=severity,
            reason=reason,
            status=status,
        )
    )


def attention_id(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> str:
    """Return the deterministic public ID for an attention condition."""
    return f"attn-{attention_fingerprint(host_id=host_id, source=source, kind=kind, severity=severity, reason=reason, status=status)}"


@dataclass(frozen=True)
class SuggestedAction:
    """A neutral action suggestion with no connector delivery state."""

    action_id: str = ""
    label: str = ""
    command: str = ""
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        label = _string_value(self.label)
        command = _string_value(self.command)
        params = sanitize_forbidden_fields(self.params if isinstance(self.params, Mapping) else {})
        action_id = _string_value(self.action_id) or stable_fingerprint(
            {"label": label, "command": command, "params": params}
        )
        object.__setattr__(self, "action_id", action_id)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "command", command)
        object.__setattr__(self, "params", params)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "label": self.label,
            "command": self.command,
            "params": sanitize_forbidden_fields(self.params),
        }

    @classmethod
    def from_dict(cls, data: "SuggestedAction | Mapping[str, Any]") -> "SuggestedAction":
        if isinstance(data, SuggestedAction):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            action_id=_string_value(clean.get("action_id")),
            label=_string_value(clean.get("label")),
            command=_string_value(clean.get("command")),
            params=clean.get("params", {}),
        )


@dataclass(frozen=True)
class Space:
    """A neutral space observation (e.g. a Herdr space / project context)."""

    id: str
    name: str
    status: str = "unknown"
    meta: dict[str, Any] = field(default_factory=dict)
    updated_at: str | None = None
    status_line: str | None = None
    fingerprint: str = ""

    def __post_init__(self) -> None:
        space_id = _string_value(self.id, "unknown")
        name = _string_value(self.name, space_id)
        status, meta = _status_and_meta(self.status, self.meta)
        updated_at = _optional_timestamp(self.updated_at)
        status_line = _optional_string(self.status_line)
        fingerprint = _string_value(self.fingerprint) or stable_fingerprint(
            {
                "type": "space",
                "id": space_id,
                "name": name,
                "status": status,
                "status_line": status_line,
                "meta": meta,
            }
        )

        object.__setattr__(self, "id", space_id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "meta", meta)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "status_line", status_line)
        object.__setattr__(self, "fingerprint", fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "updated_at": self.updated_at,
            "status_line": self.status_line,
            "fingerprint": self.fingerprint,
            "meta": sanitize_forbidden_fields(self.meta),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Space":
        clean = sanitize_forbidden_fields(data)
        known = {"id", "name", "status", "meta", "updated_at", "status_line", "summary", "fingerprint"}
        space_id = _string_value(clean.get("id", clean.get("name", "unknown")), "unknown")
        return cls(
            id=space_id,
            name=_string_value(clean.get("name", space_id), space_id),
            status=clean.get("status", "unknown"),
            meta=_merge_meta(clean, known),
            updated_at=clean.get("updated_at"),
            status_line=clean.get("status_line", clean.get("summary")),
            fingerprint=_string_value(clean.get("fingerprint")),
        )


@dataclass(frozen=True)
class Worker:
    """A neutral worker observation (e.g. a running terminal agent)."""

    id: str
    name: str
    status: str = "unknown"
    space_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    last_seen_at: str | None = None
    summary: str | None = None
    fingerprint: str = ""

    def __post_init__(self) -> None:
        worker_id = _string_value(self.id, "unknown")
        name = _string_value(self.name, worker_id)
        status, meta = _status_and_meta(self.status, self.meta)
        space_id = _optional_string(self.space_id)
        last_seen_at = _optional_timestamp(self.last_seen_at)
        summary = _optional_string(self.summary)
        fingerprint = _string_value(self.fingerprint) or stable_fingerprint(
            {
                "type": "worker",
                "id": worker_id,
                "name": name,
                "status": status,
                "space_id": space_id,
                "summary": summary,
                "meta": meta,
            }
        )

        object.__setattr__(self, "id", worker_id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "meta", meta)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "fingerprint", fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "space_id": self.space_id,
            "last_seen_at": self.last_seen_at,
            "summary": self.summary,
            "fingerprint": self.fingerprint,
            "meta": sanitize_forbidden_fields(self.meta),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Worker":
        clean = sanitize_forbidden_fields(data)
        known = {
            "id",
            "name",
            "status",
            "space_id",
            "space",
            "meta",
            "last_seen_at",
            "updated_at",
            "summary",
            "status_line",
            "fingerprint",
        }
        worker_id = _string_value(clean.get("id", clean.get("name", "unknown")), "unknown")
        return cls(
            id=worker_id,
            name=_string_value(clean.get("name", worker_id), worker_id),
            status=clean.get("status", "unknown"),
            space_id=clean.get("space_id", clean.get("space")),
            meta=_merge_meta(clean, known),
            last_seen_at=clean.get("last_seen_at", clean.get("updated_at")),
            summary=clean.get("summary", clean.get("status_line")),
            fingerprint=_string_value(clean.get("fingerprint")),
        )


@dataclass(frozen=True, init=False)
class AttentionSignal:
    """A pure, neutral attention signal produced from snapshot state."""

    id: str
    kind: str
    severity: str
    status: str
    reason: str
    source: str
    updated_at: str | None
    suggested_actions: list[SuggestedAction]
    fingerprint: str
    meta: dict[str, Any]

    def __init__(
        self,
        id: str | None = None,
        level: str | None = None,
        reason: str = "",
        source: str = "",
        *,
        kind: str = "general",
        severity: str | None = None,
        status: str = "unknown",
        updated_at: Any = None,
        suggested_actions: Iterable[SuggestedAction | Mapping[str, Any]] | SuggestedAction | Mapping[str, Any] | None = None,
        fingerprint: str | None = None,
        meta: Mapping[str, Any] | None = None,
        host_id: str | None = None,
    ) -> None:
        resolved_kind = _string_value(kind, "general")
        resolved_severity = normalize_severity(severity if severity is not None else level)
        resolved_status, clean_meta = _status_and_meta(status, meta or {})
        resolved_reason = _string_value(reason)
        resolved_source = _string_value(source)
        resolved_updated_at = _optional_timestamp(updated_at)
        actions = self._coerce_actions(suggested_actions)
        resolved_fingerprint = _string_value(fingerprint) or attention_fingerprint(
            host_id=_string_value(host_id),
            source=resolved_source,
            kind=resolved_kind,
            severity=resolved_severity,
            reason=resolved_reason,
            status=resolved_status,
        )
        resolved_id = _string_value(id) or f"attn-{resolved_fingerprint}"

        object.__setattr__(self, "id", resolved_id)
        object.__setattr__(self, "kind", resolved_kind)
        object.__setattr__(self, "severity", resolved_severity)
        object.__setattr__(self, "status", resolved_status)
        object.__setattr__(self, "reason", resolved_reason)
        object.__setattr__(self, "source", resolved_source)
        object.__setattr__(self, "updated_at", resolved_updated_at)
        object.__setattr__(self, "suggested_actions", actions)
        object.__setattr__(self, "fingerprint", resolved_fingerprint)
        object.__setattr__(self, "meta", clean_meta)

    @staticmethod
    def _coerce_actions(
        suggested_actions: Iterable[SuggestedAction | Mapping[str, Any]] | SuggestedAction | Mapping[str, Any] | None,
    ) -> list[SuggestedAction]:
        if suggested_actions is None:
            return []
        if isinstance(suggested_actions, SuggestedAction) or isinstance(suggested_actions, Mapping):
            return [SuggestedAction.from_dict(suggested_actions)]
        return [SuggestedAction.from_dict(action) for action in suggested_actions]

    @property
    def level(self) -> str:
        """Backward-compatible alias for severity."""
        return self.severity

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "severity": self.severity,
            "status": self.status,
            "reason": self.reason,
            "source": self.source,
            "updated_at": self.updated_at,
            "suggested_actions": [action.to_dict() for action in self.suggested_actions],
            "fingerprint": self.fingerprint,
            "meta": sanitize_forbidden_fields(self.meta),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AttentionSignal":
        clean = sanitize_forbidden_fields(data)
        known = {
            "id",
            "kind",
            "severity",
            "level",
            "status",
            "reason",
            "source",
            "updated_at",
            "suggested_actions",
            "fingerprint",
            "meta",
            "host_id",
        }
        return cls(
            id=_string_value(clean.get("id")) or None,
            level=clean.get("level"),
            kind=_string_value(clean.get("kind", "general"), "general"),
            severity=clean.get("severity"),
            status=_string_value(clean.get("status", "unknown"), "unknown"),
            reason=_string_value(clean.get("reason")),
            source=_string_value(clean.get("source")),
            updated_at=clean.get("updated_at"),
            suggested_actions=clean.get("suggested_actions", []),
            fingerprint=_string_value(clean.get("fingerprint")) or None,
            meta=_merge_meta(clean, known),
            host_id=_string_value(clean.get("host_id")),
        )


@dataclass(frozen=True)
class Snapshot:
    """Device-neutral top-level snapshot shape."""

    host_id: str
    updated_at: str
    spaces: list[Space] = field(default_factory=list)
    workers: list[Worker] = field(default_factory=list)
    attention: list[AttentionSignal] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION
    content_fingerprint: str = ""

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        updated_at = _string_value(_optional_timestamp(self.updated_at), utc_timestamp())
        spaces = sorted(
            (space if isinstance(space, Space) else Space.from_dict(space) for space in self.spaces),
            key=lambda space: (space.id, space.fingerprint),
        )
        workers = sorted(
            (worker if isinstance(worker, Worker) else Worker.from_dict(worker) for worker in self.workers),
            key=lambda worker: (worker.id, worker.fingerprint),
        )
        attention = sorted(
            (
                signal if isinstance(signal, AttentionSignal) else AttentionSignal.from_dict(signal)
                for signal in self.attention
            ),
            key=lambda signal: (signal.id, signal.fingerprint),
        )

        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "spaces", list(spaces))
        object.__setattr__(self, "workers", list(workers))
        object.__setattr__(self, "attention", list(attention))
        object.__setattr__(self, "schema_version", SCHEMA_VERSION)
        object.__setattr__(self, "content_fingerprint", self.compute_content_fingerprint())

    def _content_dict(self) -> dict[str, Any]:
        return _strip_snapshot_content_volatile(
            {
                "schema_version": self.schema_version,
                "host_id": self.host_id,
                "spaces": [space.to_dict() for space in self.spaces],
                "workers": [worker.to_dict() for worker in self.workers],
                "attention": [signal.to_dict() for signal in self.attention],
            }
        )

    def compute_content_fingerprint(self) -> str:
        """Return the deterministic fingerprint excluding volatile timestamps."""
        return stable_fingerprint(self._content_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "host_id": self.host_id,
            "updated_at": self.updated_at,
            "spaces": [space.to_dict() for space in self.spaces],
            "workers": [worker.to_dict() for worker in self.workers],
            "attention": [signal.to_dict() for signal in self.attention],
            "content_fingerprint": self.content_fingerprint,
        }

    def to_json(self, indent: int | None = None) -> str:
        return stable_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Snapshot":
        clean = sanitize_forbidden_fields(data)
        return cls(
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            updated_at=_string_value(clean.get("updated_at"), utc_timestamp()),
            spaces=[Space.from_dict(space) for space in clean.get("spaces", [])],
            workers=[Worker.from_dict(worker) for worker in clean.get("workers", [])],
            attention=[AttentionSignal.from_dict(signal) for signal in clean.get("attention", [])],
            schema_version=SCHEMA_VERSION,
            content_fingerprint=_string_value(clean.get("content_fingerprint")),
        )

    @classmethod
    def from_json(cls, payload: str) -> "Snapshot":
        return cls.from_dict(json.loads(payload))
