"""Public turn and pending-interaction contracts for Tendwire.

This module is pure stdlib plus sibling core models. It owns public, neutral
turn/pending JSON shapes and conservative projections from public snapshots.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .models import (
    AttentionSignal,
    BackendHealth,
    FORBIDDEN_FIELD_NAMES,
    Snapshot,
    Worker,
    _FORBIDDEN_BACKEND_NAME_TEXT,
    _is_forbidden_public_mapping_key,
    _is_forbidden_public_text_phrase,
    _TEXT_FORBIDDEN_FIELD_NAMES,
    normalize_status,
    sanitize_forbidden_fields,
    stable_fingerprint,
    stable_json_dumps,
    utc_timestamp,
    _optional_string,
    _optional_timestamp,
    _string_value,
)


TURN_SCHEMA_VERSION = 1
TURN_TEXT_MAX_CHARS = 12000
TURN_STREAM_TEXT_MAX_CHARS = 4000

TURN_KINDS = frozenset({"task", "message", "review", "unknown"})
PENDING_KINDS = frozenset(
    {
        "approval",
        "question",
        "choice",
        "review",
        "confirm_destructive_action",
        "unknown",
    }
)
PENDING_STATUSES = frozenset({"open", "answered", "cancelled", "expired", "unknown"})

_TURN_KIND_ALIASES = {
    "": "unknown",
    "task": "task",
    "work": "task",
    "job": "task",
    "message": "message",
    "note": "message",
    "review": "review",
    "inspect": "review",
}
_PENDING_KIND_ALIASES = {
    "": "unknown",
    "approve": "approval",
    "approval": "approval",
    "requires_approval": "approval",
    "requires-approval": "approval",
    "question": "question",
    "ask": "question",
    "input": "question",
    "choice": "choice",
    "select": "choice",
    "review": "review",
    "manual_review": "review",
    "manual-review": "review",
    "confirm": "confirm_destructive_action",
    "confirmation": "confirm_destructive_action",
    "destructive": "confirm_destructive_action",
    "confirm_destructive": "confirm_destructive_action",
    "confirm-destructive": "confirm_destructive_action",
    "confirm_destructive_action": "confirm_destructive_action",
    "confirm-destructive-action": "confirm_destructive_action",
}
_PENDING_STATUS_ALIASES = {
    "": "unknown",
    "open": "open",
    "pending": "open",
    "waiting": "open",
    "active": "open",
    "new": "open",
    "answered": "answered",
    "done": "answered",
    "complete": "answered",
    "completed": "answered",
    "resolved": "answered",
    "accepted": "answered",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "closed": "cancelled",
    "rejected": "cancelled",
    "expired": "expired",
    "timed_out": "expired",
    "timed-out": "expired",
    "timeout": "expired",
}
_PRIVATE_TEXT_LABEL_RE = re.compile(
    r"(?i)\b("
    r"pane[_ -]?id|terminal[_ -]?id|backend[_ -]?target|raw[_ -]?target|"
    r"chat[_ -]?id|topic[_ -]?id|message[_ -]?id|thread[_ -]?id|"
    r"socket[_ -]?path|argv|env|stdout|stderr|token|secret"
    r")\b\s*[:=]?\s*\S*"
)
_VOLATILE_KEYS = frozenset(
    {
        "updated_at",
        "observed_at",
        "created_at",
        "started_at",
        "completed_at",
        "expires_at",
        "last_seen_at",
        "timestamp",
        "fingerprint",
        "content_fingerprint",
    }
)
_HUMAN_META_KEYS = frozenset(
    {
        "needs_human",
        "human_input_required",
        "requires_human",
        "approval_required",
        "requires_approval",
        "needs_input",
        "awaiting_input",
    }
)
_APPROVAL_RE = re.compile(r"\bapproval|approve|approved\b", re.IGNORECASE)
_DESTRUCTIVE_RE = re.compile(r"\bdelete|destroy|destructive|irreversible|remove|wipe\b", re.IGNORECASE)
_QUESTION_RE = re.compile(r"\?|question|\bask(?:ing)?\b|\binput\b", re.IGNORECASE)
_REVIEW_RE = re.compile(r"\breview|inspect|manual\b", re.IGNORECASE)
_RAW_COMMAND_HEAD_RE = re.compile(
    r"^(?:sudo\s+)?(?:env\s+)?"
    r"(?:bash|sh|zsh|fish|cmd|powershell|pwsh|python\d*|node|npm|npx|git|gh|docker|"
    r"kubectl|make|pytest|herdr|tendwire|tmux|screen)(?:\s|$)",
    re.IGNORECASE,
)
_RAW_COMMAND_OPTION_RE = re.compile(r"\s--?[A-Za-z0-9][A-Za-z0-9_-]*")
_SHELL_META_RE = re.compile(r"[;&|`$<>]")
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_PUBLIC_DROP = object()


def _normalize_turn_kind(kind: Any) -> str:
    raw = _string_value(kind, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("-", "_")
    return _TURN_KIND_ALIASES.get(normalized, _TURN_KIND_ALIASES.get(raw, "unknown"))


def _normalize_pending_kind(kind: Any) -> str:
    raw = _string_value(kind, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("-", "_")
    return _PENDING_KIND_ALIASES.get(normalized, _PENDING_KIND_ALIASES.get(raw, "unknown"))


def _normalize_pending_status(status: Any) -> str:
    raw = _string_value(status, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("_", "-")
    return _PENDING_STATUS_ALIASES.get(raw, _PENDING_STATUS_ALIASES.get(normalized, "unknown"))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text not in {"", "0", "false", "no", "off", "none", "null"}


def _clean_meta(value: Any) -> dict[str, Any]:
    clean = _clean_public_value(value if isinstance(value, Mapping) else {})
    return clean if isinstance(clean, dict) else {}


def _public_text_tokens(value: str) -> list[str]:
    separated = _CAMEL_CASE_BOUNDARY_RE.sub(" ", value)
    return [
        part.lower()
        for part in re.split(r"[^A-Za-z0-9]+", separated)
        if part
    ]


def _contains_forbidden_public_text(value: str) -> bool:
    tokens = _public_text_tokens(value)
    if not tokens:
        return False
    if set(tokens) & (_FORBIDDEN_BACKEND_NAME_TEXT - {"raw"}):
        return True
    for index in range(len(tokens)):
        for size in range(1, min(4, len(tokens) - index) + 1):
            phrase = "_".join(tokens[index : index + size])
            if phrase == "command":
                continue
            if _is_forbidden_public_text_phrase(phrase):
                return True
    return bool(set(tokens) & (_TEXT_FORBIDDEN_FIELD_NAMES - {"command"}))


def _public_text(value: Any, *, default: str = "") -> str:
    text = _string_value(value).strip()
    if not text or _contains_forbidden_public_text(text):
        return default
    return " ".join(text.split())


def _optional_public_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _public_text(value)
    return text or None


def _public_turn_text(value: Any, *, max_chars: int = TURN_TEXT_MAX_CHARS) -> str | None:
    if value is None:
        return None
    text = _string_value(value).replace("\x00", "").strip()
    if not text:
        return None
    text = _PRIVATE_TEXT_LABEL_RE.sub("[redacted]", text)
    text = "\n".join(" ".join(line.split()) for line in text.splitlines()).strip()
    if len(text) > max_chars:
        text = text[: max(0, max_chars - 14)].rstrip() + "\n[truncated]"
    return text or None


def _public_identity(value: Any, *, prefix: str, default: str = "unknown") -> str:
    text = _string_value(value, default).strip()
    if not text:
        text = default
    if not _contains_forbidden_public_text(text):
        return " ".join(text.split())
    return f"{prefix}-{stable_fingerprint({'type': prefix, 'raw_id': text})}"


def _optional_public_identity(value: Any, *, prefix: str) -> str | None:
    if value is None:
        return None
    return _public_identity(value, prefix=prefix)


def _clean_public_value(value: Any) -> Any:
    clean = sanitize_forbidden_fields(value)
    if isinstance(clean, Mapping):
        result: dict[str, Any] = {}
        for key, item in clean.items():
            key_text = str(key)
            if _is_forbidden_public_mapping_key(key_text):
                continue
            sanitized = _clean_public_value(item)
            if sanitized is not _PUBLIC_DROP:
                result[key_text] = sanitized
        return result
    if isinstance(clean, list):
        result_list: list[Any] = []
        for item in clean:
            sanitized = _clean_public_value(item)
            if sanitized is not _PUBLIC_DROP:
                result_list.append(sanitized)
        return result_list
    if isinstance(clean, str):
        return _PUBLIC_DROP if _contains_forbidden_public_text(clean) else clean
    return clean


def _normalized_key(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _compact_key(value: Any) -> str:
    return _normalized_key(value).replace("_", "")


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in _VOLATILE_KEYS
        }
    if isinstance(value, list | tuple):
        return [_strip_volatile(item) for item in value]
    return value


def _stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}-{stable_fingerprint(_strip_volatile(sanitize_forbidden_fields(value)))}"


def _content_fingerprint(value: Any) -> str:
    return stable_fingerprint(_strip_volatile(sanitize_forbidden_fields(value)))


def _meta_value(meta: Mapping[str, Any], normalized_key: str) -> Any | None:
    normalized_target = _normalized_key(normalized_key)
    compact_target = normalized_target.replace("_", "")
    for key, value in meta.items():
        if _normalized_key(key) == normalized_target or _compact_key(key) == compact_target:
            return value
    return None


def _optional_public_description(value: Any) -> str | None:
    clean = _clean_public_value(value)
    if clean is _PUBLIC_DROP:
        return None
    if clean in ({}, []):
        return None
    if isinstance(clean, Mapping) or isinstance(clean, list):
        return stable_json_dumps(clean)
    return _optional_string(clean)


def _looks_like_raw_command(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if any(ord(char) < 32 or 0x80 <= ord(char) <= 0x9F for char in text):
        return True
    if _SHELL_META_RE.search(text):
        return True
    if _RAW_COMMAND_HEAD_RE.search(text):
        return True
    if _RAW_COMMAND_OPTION_RE.search(text):
        return True
    first = text.split(maxsplit=1)[0]
    return ("/" in first or first.endswith((".bat", ".cmd", ".exe", ".py", ".sh"))) and " " in text


def _public_choice_value(value: Any) -> Any | None:
    clean = _clean_public_value(value)
    if clean is _PUBLIC_DROP:
        return None
    if clean in ({}, [], ""):
        return None
    if isinstance(clean, str):
        return None if _looks_like_raw_command(clean) else clean
    return clean


def _public_suggested_action_value(action: Any) -> Any | None:
    if not getattr(action, "has_public_tendwire_action", False):
        return None
    return _public_choice_value(getattr(action, "tendwire_action", ""))


def _is_pending_routing_meta_key(key: Any) -> bool:
    return _compact_key(key) in {"workerid", "spaceid"}


@dataclass(frozen=True)
class InteractionChoice:
    """A finite public-safe choice for a pending interaction."""

    choice_id: str = ""
    label: str = ""
    value: Any | None = None
    description: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        label = _public_text(self.label, default="Action")
        description = _optional_public_description(self.description)
        params = _clean_meta(self.params)
        value = _public_choice_value(self.value)
        choice_id = _public_text(self.choice_id) or stable_fingerprint(
            {
                "label": label,
                "value": value,
                "description": description,
                "params": params,
            }
        )

        object.__setattr__(self, "choice_id", choice_id)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "description", description)
        object.__setattr__(self, "params", params)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "choice_id": self.choice_id,
            "label": self.label,
            "params": _clean_meta(self.params),
        }
        public_value = _public_choice_value(self.value)
        if public_value is not None:
            payload["value"] = public_value
        if self.description is not None:
            payload["description"] = self.description
        return payload

    @classmethod
    def from_dict(cls, data: "InteractionChoice | Mapping[str, Any]") -> "InteractionChoice":
        if isinstance(data, InteractionChoice):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            choice_id=_string_value(clean.get("choice_id")),
            label=_string_value(clean.get("label")),
            value=clean.get("value"),
            description=clean.get("description"),
            params=clean.get("params", {}),
        )


@dataclass(frozen=True)
class Turn:
    """A public, neutral representation of a worker turn."""

    host_id: str
    worker_id: str
    status: str = "unknown"
    kind: str = "unknown"
    source: str = "snapshot"
    worker_fingerprint: str | None = None
    space_id: str | None = None
    title: str | None = None
    summary: str | None = None
    user_text: str | None = None
    assistant_final_text: str | None = None
    assistant_stream_text: str | None = None
    complete: bool | None = None
    has_open_turn: bool | None = None
    started_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    origin_command_id: str | None = None
    source_turn_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    id: str = ""
    fingerprint: str = ""
    schema_version: int = TURN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        worker_id = _public_identity(self.worker_id, prefix="worker")
        status = normalize_status(self.status)
        kind = _normalize_turn_kind(self.kind)
        source = _public_text(self.source, default="snapshot")
        worker_fingerprint = _optional_public_text(self.worker_fingerprint)
        space_id = _optional_public_identity(self.space_id, prefix="space")
        title = _optional_public_text(self.title)
        summary = _optional_public_text(self.summary)
        user_text = _public_turn_text(self.user_text)
        assistant_final_text = _public_turn_text(self.assistant_final_text)
        assistant_stream_text = _public_turn_text(
            self.assistant_stream_text,
            max_chars=TURN_STREAM_TEXT_MAX_CHARS,
        )
        started_at = _optional_timestamp(self.started_at)
        updated_at = _optional_timestamp(self.updated_at)
        completed_at = _optional_timestamp(self.completed_at)
        origin_command_id = _optional_public_text(self.origin_command_id)
        source_turn_id = _optional_public_text(self.source_turn_id)
        meta = _clean_meta(self.meta)
        identity_payload = {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "worker_id": worker_id,
            "space_id": space_id,
            "kind": kind,
            "source": source,
            "origin_command_id": origin_command_id,
        }
        if source_turn_id:
            # Distinct backend turns must mint distinct public turn ids;
            # omitted for legacy rows so their identities stay stable.
            identity_payload["source_turn_id"] = source_turn_id
        content_payload = {
            **identity_payload,
            "worker_fingerprint": worker_fingerprint,
            "status": status,
            "title": title,
            "summary": summary,
            "user_text": user_text,
            "assistant_final_text": assistant_final_text,
            "assistant_stream_text": assistant_stream_text,
            "complete": self.complete if isinstance(self.complete, bool) else None,
            "has_open_turn": self.has_open_turn if isinstance(self.has_open_turn, bool) else None,
            "meta": meta,
        }
        turn_id = _stable_id("turn", identity_payload)
        fingerprint = _content_fingerprint(content_payload)

        object.__setattr__(self, "schema_version", TURN_SCHEMA_VERSION)
        object.__setattr__(self, "id", turn_id)
        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "worker_fingerprint", worker_fingerprint)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "user_text", user_text)
        object.__setattr__(self, "assistant_final_text", assistant_final_text)
        object.__setattr__(self, "assistant_stream_text", assistant_stream_text)
        object.__setattr__(self, "complete", self.complete if isinstance(self.complete, bool) else None)
        object.__setattr__(self, "has_open_turn", self.has_open_turn if isinstance(self.has_open_turn, bool) else None)
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "origin_command_id", origin_command_id)
        object.__setattr__(self, "source_turn_id", source_turn_id)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(self, "meta", meta)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "host_id": self.host_id,
            "worker_id": self.worker_id,
            "worker_fingerprint": self.worker_fingerprint,
            "space_id": self.space_id,
            "status": self.status,
            "kind": self.kind,
            "title": self.title,
            "summary": self.summary,
            "user_text": self.user_text,
            "assistant_final_text": self.assistant_final_text,
            "assistant_stream_text": self.assistant_stream_text,
            "complete": self.complete,
            "has_open_turn": self.has_open_turn,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "source": self.source,
            "origin_command_id": self.origin_command_id,
            "source_turn_id": self.source_turn_id,
            "fingerprint": self.fingerprint,
            "meta": _clean_meta(self.meta),
        }

    def to_json(self, indent: int | None = None) -> str:
        return stable_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: "Turn | Mapping[str, Any]") -> "Turn":
        if isinstance(data, Turn):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            id=_string_value(clean.get("id")),
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            worker_id=_string_value(clean.get("worker_id", "unknown"), "unknown"),
            worker_fingerprint=clean.get("worker_fingerprint"),
            space_id=clean.get("space_id"),
            status=clean.get("status", "unknown"),
            kind=clean.get("kind", "unknown"),
            title=clean.get("title"),
            summary=clean.get("summary"),
            user_text=clean.get("user_text"),
            assistant_final_text=clean.get("assistant_final_text"),
            assistant_stream_text=clean.get("assistant_stream_text"),
            complete=clean.get("complete") if isinstance(clean.get("complete"), bool) else None,
            has_open_turn=clean.get("has_open_turn") if isinstance(clean.get("has_open_turn"), bool) else None,
            started_at=clean.get("started_at"),
            updated_at=clean.get("updated_at"),
            completed_at=clean.get("completed_at"),
            source=clean.get("source", "snapshot"),
            origin_command_id=clean.get("origin_command_id"),
            source_turn_id=clean.get("source_turn_id"),
            fingerprint=_string_value(clean.get("fingerprint")),
            meta=clean.get("meta", {}),
        )

    @classmethod
    def from_json(cls, payload: str) -> "Turn":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class PendingInteraction:
    """A public, neutral human interaction request."""

    host_id: str
    worker_id: str
    question: str
    kind: str = "unknown"
    choices: list[InteractionChoice] = field(default_factory=list)
    status: str = "open"
    worker_fingerprint: str | None = None
    space_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    expires_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    id: str = ""
    fingerprint: str = ""
    schema_version: int = TURN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        worker_id = _public_identity(self.worker_id, prefix="worker")
        worker_fingerprint = _optional_public_text(self.worker_fingerprint)
        space_id = _optional_public_identity(self.space_id, prefix="space")
        kind = _normalize_pending_kind(self.kind)
        question = _public_text(self.question, default="Action requires attention")
        choices = sorted(
            (choice if isinstance(choice, InteractionChoice) else InteractionChoice.from_dict(choice) for choice in self.choices),
            key=lambda choice: (choice.choice_id, choice.label),
        )
        status = _normalize_pending_status(self.status)
        created_at = _optional_timestamp(self.created_at)
        updated_at = _optional_timestamp(self.updated_at)
        expires_at = _optional_timestamp(self.expires_at)
        meta = _clean_meta(self.meta)
        identity_payload = {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "worker_id": worker_id,
            "space_id": space_id,
            "kind": kind,
            "question": question,
            "choice_ids": [choice.choice_id for choice in choices],
            "source": _meta_value(meta, "source") or _meta_value(meta, "attention_id"),
        }
        content_payload = {
            **identity_payload,
            "worker_fingerprint": worker_fingerprint,
            "choices": [choice.to_dict() for choice in choices],
            "status": status,
            "meta": meta,
        }
        interaction_id = _stable_id("pending", identity_payload)
        fingerprint = _content_fingerprint(content_payload)

        object.__setattr__(self, "schema_version", TURN_SCHEMA_VERSION)
        object.__setattr__(self, "id", interaction_id)
        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "worker_fingerprint", worker_fingerprint)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "question", question)
        object.__setattr__(self, "choices", list(choices))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(self, "meta", meta)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "host_id": self.host_id,
            "worker_id": self.worker_id,
            "worker_fingerprint": self.worker_fingerprint,
            "space_id": self.space_id,
            "kind": self.kind,
            "question": self.question,
            "choices": [choice.to_dict() for choice in self.choices],
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "fingerprint": self.fingerprint,
            "meta": _clean_meta(self.meta),
        }

    def to_json(self, indent: int | None = None) -> str:
        return stable_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: "PendingInteraction | Mapping[str, Any]") -> "PendingInteraction":
        if isinstance(data, PendingInteraction):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            id=_string_value(clean.get("id")),
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            worker_id=_string_value(clean.get("worker_id", "unknown"), "unknown"),
            worker_fingerprint=clean.get("worker_fingerprint"),
            space_id=clean.get("space_id"),
            kind=clean.get("kind", "unknown"),
            question=_string_value(clean.get("question")),
            choices=[InteractionChoice.from_dict(choice) for choice in clean.get("choices", [])],
            status=clean.get("status", "open"),
            created_at=clean.get("created_at"),
            updated_at=clean.get("updated_at"),
            expires_at=clean.get("expires_at"),
            fingerprint=_string_value(clean.get("fingerprint")),
            meta=clean.get("meta", {}),
        )

    @classmethod
    def from_json(cls, payload: str) -> "PendingInteraction":
        return cls.from_dict(json.loads(payload))


def _worker_origin_command_id(worker: Worker) -> str | None:
    value = _meta_value(worker.meta, "origin_command_id")
    return _optional_string(value)


def turns_from_snapshot(snapshot: Snapshot) -> list[Turn]:
    """Derive deterministic public turns from public snapshot workers."""
    turns: list[Turn] = []
    for worker in snapshot.workers:
        worker_meta = _clean_meta(worker.meta)
        turns.append(
            Turn(
                host_id=snapshot.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                space_id=worker.space_id,
                status=worker.status,
                kind="task",
                title=worker.name,
                summary=worker.summary,
                updated_at=worker.last_seen_at,
                source=f"worker:{worker.id}",
                origin_command_id=_worker_origin_command_id(worker),
                meta=worker_meta,
            )
        )
    return sorted(turns, key=lambda turn: (turn.id, turn.fingerprint))


def _signal_worker_id(signal: AttentionSignal) -> str | None:
    worker_id = _optional_string(_meta_value(signal.meta, "worker_id"))
    if worker_id:
        return worker_id
    source = signal.source
    if source.startswith("worker:"):
        return source.split(":", 1)[1] or None
    return None


def _signal_is_human_actionable(signal: AttentionSignal) -> bool:
    if signal.suggested_actions:
        return True
    for key, value in signal.meta.items():
        if str(key).strip().lower().replace("-", "_") in _HUMAN_META_KEYS and _truthy(value):
            return True
    reason = signal.reason.strip()
    return bool(reason and (_APPROVAL_RE.search(reason) or _QUESTION_RE.search(reason) or _REVIEW_RE.search(reason)))


def _kind_from_signal(signal: AttentionSignal) -> str:
    text_parts = [signal.kind, signal.reason]
    for action in signal.suggested_actions:
        public_action_value = _public_suggested_action_value(action)
        text_parts.append(action.label)
        if isinstance(public_action_value, str):
            text_parts.append(public_action_value)
    text = " ".join(part for part in text_parts if part)
    explicit_kind = _normalize_pending_kind(_meta_value(signal.meta, "interaction_kind"))
    if explicit_kind != "unknown":
        return explicit_kind
    if _DESTRUCTIVE_RE.search(text):
        return "confirm_destructive_action"
    if _APPROVAL_RE.search(text):
        return "approval"
    if signal.suggested_actions:
        return "choice"
    if _REVIEW_RE.search(text):
        return "review"
    if _QUESTION_RE.search(text):
        return "question"
    return "review"


def _choices_from_signal(signal: AttentionSignal) -> list[InteractionChoice]:
    choices: list[InteractionChoice] = []
    for action in signal.suggested_actions:
        public_value = _public_suggested_action_value(action)
        label = action.label or (public_value if isinstance(public_value, str) else "") or "Action"
        choice_id = action.action_id if public_value is not None else ""
        choices.append(
            InteractionChoice(
                choice_id=choice_id,
                label=label,
                value=public_value,
                params=action.params,
            )
        )
    return sorted(choices, key=lambda choice: (choice.choice_id, choice.label))


def _pending_status_from_signal(signal: AttentionSignal) -> str:
    explicit = _meta_value(signal.meta, "pending_status")
    if explicit is not None:
        return _normalize_pending_status(explicit)
    normalized = normalize_status(signal.status)
    if normalized in {"done", "closed"}:
        return "answered"
    if normalized == "failed":
        return "open"
    return "open"


def _pending_public_meta_from_signal(signal: AttentionSignal) -> dict[str, Any]:
    meta = _clean_meta(
        {
            "attention_id": signal.id,
            "attention_kind": signal.kind,
            "attention_severity": signal.severity,
            "attention_status": signal.status,
            "source": signal.source,
        }
    )
    for key, value in _clean_meta(signal.meta).items():
        if _is_pending_routing_meta_key(key):
            continue
        meta[str(key)] = value
    return _clean_meta(meta)


def pending_from_snapshot(snapshot: Snapshot) -> list[PendingInteraction]:
    """Derive deterministic public pending interactions from attention signals."""
    workers = {worker.id: worker for worker in snapshot.workers}
    interactions: list[PendingInteraction] = []
    for signal in snapshot.attention:
        if not _signal_is_human_actionable(signal):
            continue
        worker_id = _signal_worker_id(signal)
        if not worker_id:
            continue
        worker = workers.get(worker_id)
        space_id = _optional_string(_meta_value(signal.meta, "space_id"))
        if space_id is None and worker is not None:
            space_id = worker.space_id
        meta = _pending_public_meta_from_signal(signal)
        interactions.append(
            PendingInteraction(
                host_id=snapshot.host_id,
                worker_id=worker_id,
                worker_fingerprint=worker.fingerprint if worker is not None else None,
                space_id=space_id,
                kind=_kind_from_signal(signal),
                question=signal.reason,
                choices=_choices_from_signal(signal),
                status=_pending_status_from_signal(signal),
                created_at=signal.updated_at,
                updated_at=signal.updated_at,
                meta=meta,
            )
        )
    return sorted(interactions, key=lambda item: (item.id, item.fingerprint))


def _backend_health_payload(backend_health: Iterable[BackendHealth]) -> list[dict[str, Any]]:
    return [BackendHealth.from_dict(health).to_dict() for health in backend_health]


def turns_payload_from_snapshot(snapshot: Snapshot) -> dict[str, Any]:
    """Return the public JSON wrapper for projected turns."""
    turns = [turn.to_dict() for turn in turns_from_snapshot(snapshot)]
    backend_health = _backend_health_payload(snapshot.backend_health)
    payload = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": snapshot.host_id,
        "updated_at": snapshot.updated_at,
        "turns": turns,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = _content_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "turns": turns,
            "backend_health": backend_health,
        }
    )
    return sanitize_forbidden_fields(payload)


def pending_payload_from_snapshot(snapshot: Snapshot) -> dict[str, Any]:
    """Return the public JSON wrapper for projected pending interactions."""
    pending = [interaction.to_dict() for interaction in pending_from_snapshot(snapshot)]
    backend_health = _backend_health_payload(snapshot.backend_health)
    payload = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": snapshot.host_id,
        "updated_at": snapshot.updated_at,
        "pending_interactions": pending,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = _content_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "pending_interactions": pending,
            "backend_health": backend_health,
        }
    )
    return sanitize_forbidden_fields(payload)


def payload_to_json(payload: Mapping[str, Any], *, indent: int | None = None) -> str:
    """Serialize a turn/pending wrapper using Tendwire stable JSON."""
    return stable_json_dumps(payload, indent=indent)
