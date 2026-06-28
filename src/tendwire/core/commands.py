"""Neutral command request/result/envelope contract for Tendwire.

This module defines the milestone-1 command contract: JSON request shapes,
result/envelope shapes, validation, and sanitization. It depends only on the
Python standard library and sibling core helpers. It must not import subprocess,
backends, stores, Herdr, Herdres, Telegram, or connector modules.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .models import (
    Snapshot,
    Worker,
    sanitize_forbidden_fields,
    stable_fingerprint,
    stable_json_dumps,
    _optional_string,
    _string_value,
)


COMMAND_SCHEMA_VERSION = 1

ALLOWED_ACTIONS = frozenset({"noop", "read_snapshot", "resolve_target", "send_instruction"})

# Canonical status values for command results/envelopes.
STATUS_NOOP = "noop"
STATUS_SNAPSHOT = "snapshot"
STATUS_RESOLVED = "resolved"
STATUS_DRY_RUN = "dry_run"
STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"
STATUS_NOT_FOUND = "not_found"
STATUS_AMBIGUOUS_TARGET = "ambiguous_target"
STATUS_STALE_TARGET = "stale_target"
STATUS_BACKEND_UNAVAILABLE = "backend_unavailable"
STATUS_BACKEND_UNSUPPORTED = "backend_unsupported"
STATUS_BACKEND_FAILED = "backend_failed"
STATUS_DUPLICATE_REQUEST = "duplicate_request"
STATUS_REQUEST_STATE_UNCERTAIN = "request_state_uncertain"
STATUS_INVALID_REQUEST = "invalid_request"

VALID_STATUSES = frozenset(
    {
        STATUS_NOOP,
        STATUS_SNAPSHOT,
        STATUS_RESOLVED,
        STATUS_DRY_RUN,
        STATUS_ACCEPTED,
        STATUS_REJECTED,
        STATUS_NOT_FOUND,
        STATUS_AMBIGUOUS_TARGET,
        STATUS_STALE_TARGET,
        STATUS_BACKEND_UNAVAILABLE,
        STATUS_BACKEND_UNSUPPORTED,
        STATUS_BACKEND_FAILED,
        STATUS_DUPLICATE_REQUEST,
        STATUS_REQUEST_STATE_UNCERTAIN,
        STATUS_INVALID_REQUEST,
    }
)

# Neutral target fields permitted in command requests.
TARGET_ALLOWED_FIELDS = frozenset({"worker_id", "worker_fingerprint", "space_id", "name"})
INSTRUCTION_ALLOWED_FIELDS = frozenset({"text"})

# Connector and low-level terminal fields that are rejected anywhere in a request.
FORBIDDEN_REQUEST_FIELDS = frozenset(
    {
        "telegram",
        "chat_id",
        "topic_id",
        "message_id",
        "thread_id",
        "route",
        "routes",
        "delivery",
        "deliveries",
        "token",
        "tokens",
        "bot_token",
        "pane_id",
        "terminal_id",
        "tty",
        "pty",
        "pid",
        "tmux",
        "screen_session",
        "window_id",
        "tab_id",
        "argv",
        "command",
        "shell",
        "connector",
        "connectors",
    }
)

# Conservative forbidden-field matching including common case and separator variants.
_FORBIDDEN_REQUEST_COMPACT = frozenset(name.replace("_", "") for name in FORBIDDEN_REQUEST_FIELDS)

MAX_INSTRUCTION_LENGTH = 4096

# Public command results use a stricter forbidden-field set than snapshots.
_COMMAND_RESULT_FORBIDDEN_FIELDS = frozenset(
    {
        "telegram",
        "chat_id",
        "topic_id",
        "message_id",
        "thread_id",
        "route",
        "routes",
        "delivery",
        "deliveries",
        "token",
        "tokens",
        "bot_token",
        "pane_id",
        "terminal_id",
        "tty",
        "pty",
        "pid",
        "tmux",
        "screen_session",
        "window_id",
        "tab_id",
        "argv",
        "shell",
        "command",
        "connector",
        "connectors",
        "herdres_delivery",
    }
)
_COMMAND_RESULT_FORBIDDEN_COMPACT = frozenset(
    name.replace("_", "") for name in _COMMAND_RESULT_FORBIDDEN_FIELDS
)

# Workers that must not receive instructions.
_DISALLOWED_WORKER_STATUSES = frozenset({"closed", "failed", "unknown"})


def _compact_field_name(key: Any) -> str:
    return str(key).lower().replace("-", "_").replace("_", "")


def _is_forbidden_request_field(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    compact = _compact_field_name(key)
    return normalized in FORBIDDEN_REQUEST_FIELDS or compact in _FORBIDDEN_REQUEST_COMPACT


def _is_command_result_forbidden_field(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    compact = normalized.replace("_", "")
    return normalized in _COMMAND_RESULT_FORBIDDEN_FIELDS or compact in _COMMAND_RESULT_FORBIDDEN_COMPACT


def sanitize_command_result(value: Any) -> Any:
    """Return a JSON-safe value with command-public forbidden fields removed.

    This is stricter than the snapshot sanitizer: it also strips raw terminal
    identifiers (pane_id, terminal_id, pid, tty, ...) and any route/delivery/token
    or connector variants. It preserves ordinary snapshot schema v2 output.
    """
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if _is_command_result_forbidden_field(key):
                continue
            sanitized[str(key)] = sanitize_command_result(item)
        return sanitized
    if isinstance(value, tuple | list):
        return [sanitize_command_result(item) for item in value]
    if isinstance(value, set | frozenset):
        items = [sanitize_command_result(item) for item in value]
        return sorted(items, key=stable_json_dumps)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _clean_mapping(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    return {}


def _find_forbidden_fields(value: Any, path: str = "$") -> list[str]:
    """Return paths of forbidden connector/terminal fields found in a value."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if _is_forbidden_request_field(key):
                found.append(f"{path}.{key}")
            found.extend(_find_forbidden_fields(item, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_find_forbidden_fields(item, f"{path}[{index}]"))
    return found


def error_value(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a neutral, sanitized error object."""
    return {
        "code": _string_value(code, STATUS_REJECTED),
        "message": _string_value(message),
        "details": sanitize_forbidden_fields(details) if details else {},
    }


def instruction_text_error(code: str, message: str) -> dict[str, Any]:
    return error_value(code, message, details={"field": "instruction.text"})


def _contains_bracketed_paste(text: str) -> bool:
    return "\x1b[200~" in text or "\x1b[201~" in text


def validate_instruction_text(text: Any) -> dict[str, Any] | None:
    """Validate instruction text and return an error dict, or None if valid."""
    if text is None:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text is required")
    if not isinstance(text, str):
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must be a string")
    if not text:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must not be empty")
    if len(text) > MAX_INSTRUCTION_LENGTH:
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            f"instruction.text exceeds maximum length of {MAX_INSTRUCTION_LENGTH}",
        )
    if "\x00" in text:
        return instruction_text_error(STATUS_INVALID_REQUEST, "instruction.text must not contain NUL")
    if _contains_bracketed_paste(text):
        return instruction_text_error(
            STATUS_INVALID_REQUEST,
            "instruction.text must not contain bracketed-paste sequences",
        )
    # Reject C0 control characters (0x00-0x1f) and DEL (0x7f).
    for char in text:
        code = ord(char)
        if code < 32 or code == 127:
            return instruction_text_error(
                STATUS_INVALID_REQUEST,
                "instruction.text must not contain raw control characters",
            )
    return None


def _validate_target_shape(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if target is None:
        return None
    if not isinstance(target, dict):
        return error_value(STATUS_INVALID_REQUEST, "target must be an object", details={"field": "target"})
    extra = set(target.keys()) - TARGET_ALLOWED_FIELDS
    if extra:
        return error_value(
            STATUS_INVALID_REQUEST,
            f"target contains disallowed fields: {sorted(extra)}",
            details={"field": "target", "disallowed": sorted(extra)},
        )
    return None


def _validate_instruction_shape(instruction: dict[str, Any] | None) -> dict[str, Any] | None:
    if instruction is None:
        return None
    if not isinstance(instruction, dict):
        return error_value(
            STATUS_INVALID_REQUEST, "instruction must be an object", details={"field": "instruction"}
        )
    extra = set(instruction.keys()) - INSTRUCTION_ALLOWED_FIELDS
    if extra:
        return error_value(
            STATUS_INVALID_REQUEST,
            f"instruction contains disallowed fields: {sorted(extra)}",
            details={"field": "instruction", "disallowed": sorted(extra)},
        )
    return validate_instruction_text(instruction.get("text"))


@dataclass(frozen=True)
class CommandRequest:
    """A neutral, validated command request."""

    action: str
    schema_version: int = COMMAND_SCHEMA_VERSION
    request_id: str | None = None
    dry_run: bool = True
    target: dict[str, Any] | None = None
    instruction: dict[str, Any] | None = None
    params: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "action", _string_value(self.action))
        object.__setattr__(self, "schema_version", int(self.schema_version))
        object.__setattr__(self, "request_id", _optional_string(self.request_id))
        object.__setattr__(self, "dry_run", bool(self.dry_run))
        object.__setattr__(self, "target", _clean_mapping(self.target))
        object.__setattr__(self, "instruction", _clean_mapping(self.instruction))
        object.__setattr__(self, "params", _clean_mapping(self.params))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "action": self.action,
            "request_id": self.request_id,
            "dry_run": self.dry_run,
            "target": self.target,
            "instruction": self.instruction,
            "params": self.params,
        }

    def payload_fingerprint(self) -> str:
        """Return a stable fingerprint of this request for idempotency receipts."""
        return stable_fingerprint(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandRequest":
        return cls(
            action=data.get("action", ""),
            schema_version=data.get("schema_version", COMMAND_SCHEMA_VERSION),
            request_id=data.get("request_id"),
            dry_run=data.get("dry_run", True),
            target=data.get("target"),
            instruction=data.get("instruction"),
            params=data.get("params"),
        )


def validate_request(request: CommandRequest) -> dict[str, Any] | None:
    """Validate a command request; return an error dict or None if valid."""
    if request.schema_version != COMMAND_SCHEMA_VERSION:
        return error_value(
            STATUS_INVALID_REQUEST,
            f"schema_version must be {COMMAND_SCHEMA_VERSION}",
            details={"field": "schema_version"},
        )

    if not request.action:
        return error_value(STATUS_INVALID_REQUEST, "action is required", details={"field": "action"})

    if request.action not in ALLOWED_ACTIONS:
        return error_value(
            STATUS_REJECTED,
            f"unknown action {request.action!r}",
            details={"field": "action", "allowed": sorted(ALLOWED_ACTIONS)},
        )

    forbidden = _find_forbidden_fields(request.to_dict())
    if forbidden:
        return error_value(
            STATUS_INVALID_REQUEST,
            "request contains forbidden connector or terminal fields",
            details={"fields": forbidden},
        )

    target_err = _validate_target_shape(request.target)
    if target_err:
        return target_err

    instruction_err = _validate_instruction_shape(request.instruction)
    if instruction_err:
        return instruction_err

    if request.action == "send_instruction":
        if request.target is None:
            return error_value(
                STATUS_INVALID_REQUEST,
                "send_instruction requires a target",
                details={"field": "target"},
            )
        if request.instruction is None or not _string_value(request.instruction.get("text")):
            return error_value(
                STATUS_INVALID_REQUEST,
                "send_instruction requires instruction.text",
                details={"field": "instruction.text"},
            )
        if not request.dry_run and not request.request_id:
            return error_value(
                STATUS_INVALID_REQUEST,
                "non-dry-run send_instruction requires request_id",
                details={"field": "request_id"},
            )

    return None


def parse_command_request(payload: str) -> tuple[CommandRequest | None, dict[str, Any] | None]:
    """Parse a JSON string into a CommandRequest or an error dict."""
    try:
        data = json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"invalid JSON: {exc}",
            details={"field": "request"},
        )
    if not isinstance(data, dict):
        return None, error_value(
            STATUS_INVALID_REQUEST,
            "request must be a JSON object",
            details={"field": "request"},
        )
    forbidden = [f"$.{key}" for key in data if _is_forbidden_request_field(key)]
    if forbidden:
        return None, error_value(
            STATUS_INVALID_REQUEST,
            "request contains forbidden connector or terminal fields",
            details={"fields": forbidden},
        )
    try:
        request = CommandRequest.from_dict(data)
    except Exception as exc:  # noqa: BLE001
        return None, error_value(
            STATUS_INVALID_REQUEST,
            f"request shape error: {exc}",
            details={"field": "request"},
        )
    return request, None


@dataclass(frozen=True)
class CommandEnvelope:
    """A neutral command result/envelope."""

    ok: bool
    status: str
    action: str
    request_id: str | None = None
    dry_run: bool = True
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)
    schema_version: int = COMMAND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "ok", bool(self.ok))
        object.__setattr__(self, "status", _string_value(self.status))
        object.__setattr__(self, "action", _string_value(self.action))
        object.__setattr__(self, "request_id", _optional_string(self.request_id))
        object.__setattr__(self, "dry_run", bool(self.dry_run))
        object.__setattr__(self, "result", _clean_mapping(self.result))
        object.__setattr__(self, "error", _clean_mapping(self.error))
        object.__setattr__(self, "warnings", [str(w) for w in self.warnings])
        object.__setattr__(self, "schema_version", int(self.schema_version))

    def to_dict(self) -> dict[str, Any]:
        return sanitize_command_result(
            {
                "schema_version": self.schema_version,
                "action": self.action,
                "request_id": self.request_id,
                "ok": self.ok,
                "dry_run": self.dry_run,
                "status": self.status,
                "result": self.result,
                "error": self.error,
                "warnings": self.warnings,
            }
        )

    def to_json(self, indent: int | None = None) -> str:
        return stable_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CommandEnvelope":
        clean = sanitize_forbidden_fields(data) if isinstance(data, dict) else {}
        return cls(
            ok=bool(clean.get("ok", False)),
            status=_string_value(clean.get("status")),
            action=_string_value(clean.get("action")),
            request_id=_optional_string(clean.get("request_id")),
            dry_run=bool(clean.get("dry_run", True)),
            result=clean.get("result") if isinstance(clean.get("result"), dict) else None,
            error=clean.get("error") if isinstance(clean.get("error"), dict) else None,
            warnings=list(clean.get("warnings", [])) if isinstance(clean.get("warnings"), list) else [],
            schema_version=int(clean.get("schema_version", COMMAND_SCHEMA_VERSION)),
        )

    @classmethod
    def from_result(
        cls,
        request: CommandRequest,
        *,
        ok: bool,
        status: str,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
    ) -> "CommandEnvelope":
        return cls(
            ok=ok,
            status=status,
            action=request.action,
            request_id=request.request_id,
            dry_run=request.dry_run,
            result=result,
            error=error,
            warnings=list(warnings or []),
        )

    @classmethod
    def error(cls, request: CommandRequest | None, error: dict[str, Any]) -> "CommandEnvelope":
        """Build a rejection envelope from a partial or missing request."""
        if request is None:
            return cls(
                ok=False,
                status=error.get("code", STATUS_INVALID_REQUEST),
                action="",
                request_id=None,
                dry_run=True,
                result=None,
                error=error,
            )
        return cls.from_result(request, ok=False, status=error.get("code", STATUS_REJECTED), error=error)


def worker_candidate(worker: Worker) -> dict[str, Any]:
    """Return a sanitized neutral candidate description for a worker."""
    candidate: dict[str, Any] = {
        "worker_id": worker.id,
        "name": worker.name,
        "space_id": worker.space_id,
        "status": worker.status,
        "worker_fingerprint": worker.fingerprint,
    }
    if worker.summary:
        candidate["summary"] = worker.summary
    return candidate


def resolve_target(
    target: dict[str, Any] | None,
    workers: list[Worker],
    *,
    allow_disallowed_status: bool = False,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    """Resolve a target dict against live workers.

    Returns a tuple of (resolved_candidate, candidates, status). When status is
    STATUS_RESOLVED, resolved_candidate is set and candidates is a one-element
    list. For other statuses resolved_candidate is None and candidates contains
    zero or more sanitized candidates.
    """
    if target is None:
        return None, [], STATUS_NOT_FOUND

    worker_id = _string_value(target.get("worker_id"))
    name = _string_value(target.get("name"))
    space_id = _optional_string(target.get("space_id"))
    fingerprint = _optional_string(target.get("worker_fingerprint"))

    # First match by identity/name/space, excluding fingerprint.
    identity_matches: list[Worker] = []
    for worker in workers:
        if worker_id and worker.id != worker_id:
            continue
        if name and worker.name != name:
            continue
        if space_id is not None and worker.space_id != space_id:
            continue
        identity_matches.append(worker)

    # If a fingerprint was supplied, filter further. A non-empty identity match
    # that becomes empty due to fingerprint mismatch signals a stale target.
    if fingerprint is not None:
        fingerprint_matches = [w for w in identity_matches if w.fingerprint == fingerprint]
        if identity_matches and not fingerprint_matches:
            if len(identity_matches) == 1:
                return None, [worker_candidate(identity_matches[0])], STATUS_STALE_TARGET
            # Multiple identity matches with no fingerprint match is ambiguous.
            return None, [worker_candidate(w) for w in identity_matches], STATUS_AMBIGUOUS_TARGET
        candidates = fingerprint_matches
    else:
        candidates = identity_matches

    sanitized = [worker_candidate(worker) for worker in candidates]

    if len(candidates) == 0:
        return None, [], STATUS_NOT_FOUND
    if len(candidates) > 1:
        return None, sanitized, STATUS_AMBIGUOUS_TARGET

    resolved = candidates[0]
    if not allow_disallowed_status and resolved.status in _DISALLOWED_WORKER_STATUSES:
        return None, sanitized, STATUS_REJECTED

    return sanitized[0], sanitized, STATUS_RESOLVED


def snapshot_result(snapshot: Snapshot) -> dict[str, Any]:
    """Return a neutral result payload for read_snapshot."""
    return {"snapshot": snapshot.to_dict()}
