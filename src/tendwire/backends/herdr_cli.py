"""Thin adapter boundary around the Herdr CLI.

This module shells out read-only to a `herdr` binary when available and parses
output on a best-effort basis. If the binary is missing or fails, it returns
empty neutral data rather than blocking the snapshot contract.

This module must not import Herdres code or leak delivery/routing state into
core models.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..config import Config
from ..core.models import (
    BackendHealth,
    Space,
    Worker,
    WorkerBinding,
    normalize_status,
    separate_duplicate_worker_bindings,
    stable_fingerprint,
    utc_timestamp,
    worker_binding_private_fingerprint,
)


_HERDR_TIMEOUT_SECONDS = 5.0
_BACKEND_NAME = "herdr"
_AMBIGUOUS_BINDING_REASONS = frozenset({"duplicate_backend_target", "not_unique"})

@dataclass(frozen=True)
class _WorkerRecord:
    worker: Worker
    private_fingerprint: str
    turn_target_kind: str | None = None
    turn_target_value: str | None = None

_FORBIDDEN_CONNECTOR_FIELDS = {
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
    "backend_target",
}

_STATUS_KEYS = (
    "agent_status",
    "status",
    "state",
    "phase",
    "lifecycle",
    "lifecycle_state",
    "raw_status",
)

_BACKEND_TARGET_KINDS = frozenset(
    {"agent_id", "terminal_id", "pane_id", "agent", "name", "label"}
)
_DEADLINE_EXHAUSTED_OUTCOMES = frozenset({"timeout", "deadline_exhausted"})
_UNAVAILABLE_HEALTH_OUTCOMES = frozenset({"missing_binary", "launch_error", "socket_disconnected"})
_DEGRADED_HEALTH_OUTCOMES = frozenset(
    {
        "timeout",
        "deadline_exhausted",
        "nonzero",
        "malformed_json",
        "protocol_error",
        "worker_cap_exceeded",
    }
)

_HEALTH_MESSAGES = {
    "healthy_non_empty": "Herdr observation is healthy",
    "empty_healthy": "Herdr observation is healthy but empty",
    "missing_binary": "Herdr binary is unavailable",
    "launch_error": "Herdr launch failed",
    "timeout": "Herdr observation timed out",
    "deadline_exhausted": "Herdr observation deadline was exhausted",
    "nonzero": "Herdr command returned nonzero status",
    "malformed_json": "Herdr command returned malformed JSON",
    "protocol_error": "Herdr protocol returned an invalid envelope",
    "socket_disconnected": "Herdr socket disconnected",
    "worker_cap_exceeded": "Herdr observation exceeded the configured worker cap",
    "unknown": "Herdr observation state is unknown",
}


@dataclass(frozen=True)
class _ProbeBudget:
    """Aggregate deadline for a read-only Herdr observation chain."""

    started_at: float
    per_probe_timeout_seconds: float
    aggregate_deadline_seconds: float

    @classmethod
    def from_config(cls, config: Config, *, planned_probes: int) -> "_ProbeBudget":
        per_probe = float(config.herdr_timeout_seconds)
        planned = max(1, int(planned_probes))
        return cls(
            started_at=time.monotonic(),
            per_probe_timeout_seconds=per_probe,
            aggregate_deadline_seconds=per_probe * planned,
        )

    def remaining_seconds(self) -> float:
        return self.aggregate_deadline_seconds - (time.monotonic() - self.started_at)

    def subprocess_timeout_seconds(self) -> float | None:
        remaining = self.remaining_seconds()
        if remaining <= 0:
            return None
        if remaining >= self.per_probe_timeout_seconds:
            return self.per_probe_timeout_seconds
        return max(0.001, remaining)


def herdr_health_status_for_outcome(outcome: str) -> str:
    """Map a Herdr adapter outcome into the public backend health status."""
    normalized = str(outcome or "unknown").strip().lower().replace("-", "_")
    if normalized in {"healthy_non_empty", "empty_healthy"}:
        return "healthy"
    if normalized in _UNAVAILABLE_HEALTH_OUTCOMES:
        return "unavailable"
    if normalized in _DEGRADED_HEALTH_OUTCOMES:
        return "degraded"
    return "unknown"


def herdr_backend_health(
    outcome: str,
    *,
    observed_at: str | None = None,
    message: str | None = None,
    spaces: Sequence[Space] | None = None,
    workers: Sequence[Worker] | None = None,
) -> BackendHealth:
    """Return the fixed public-safe health object for a Herdr observation."""
    normalized_outcome = str(outcome or "unknown").strip().lower().replace("-", "_")
    if normalized_outcome == "ok":
        normalized_outcome = (
            "healthy_non_empty"
            if (spaces and len(spaces) > 0) or (workers and len(workers) > 0)
            else "empty_healthy"
        )
    if normalized_outcome not in {
        "healthy_non_empty",
        "empty_healthy",
        "missing_binary",
        "launch_error",
        "timeout",
        "deadline_exhausted",
        "nonzero",
        "malformed_json",
        "protocol_error",
        "socket_disconnected",
        "worker_cap_exceeded",
        "unknown",
    }:
        normalized_outcome = "unknown"
    counts = {
        "spaces": len(spaces or []),
        "workers": len(workers or []),
    }
    return BackendHealth(
        name=_BACKEND_NAME,
        status=herdr_health_status_for_outcome(normalized_outcome),
        outcome=normalized_outcome,
        observed_at=observed_at or utc_timestamp(),
        message=message if message is not None else _HEALTH_MESSAGES[normalized_outcome],
        counts=counts,
    )


@dataclass(frozen=True)
class HerdrSnapshotObservation:
    """Snapshot observation plus public backend health and private bindings."""

    spaces: list[Space]
    workers: list[Worker]
    bindings: list[WorkerBinding] = field(default_factory=list)
    backend_health: list[BackendHealth] = field(default_factory=list)

    def __post_init__(self) -> None:
        spaces = list(self.spaces)
        workers = list(self.workers)
        bindings = list(self.bindings)
        backend_health = list(self.backend_health)
        if not backend_health:
            outcome = "healthy_non_empty" if spaces or workers else "empty_healthy"
            backend_health = [herdr_backend_health(outcome, spaces=spaces, workers=workers)]
        object.__setattr__(self, "spaces", spaces)
        object.__setattr__(self, "workers", workers)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "backend_health", backend_health)

    @property
    def health(self) -> BackendHealth:
        for item in self.backend_health:
            if item.name == _BACKEND_NAME:
                return item
        return herdr_backend_health("unknown", spaces=self.spaces, workers=self.workers)

    @property
    def authoritative(self) -> bool:
        return self.health.status == "healthy"


@dataclass(frozen=True)
class HerdrCommandObservation:
    """Command execution observation with health metadata."""

    spaces: list[Space]
    workers: list[Worker]
    status: str
    outcome: str
    message: str = ""
    bindings: list[WorkerBinding] = field(default_factory=list)
    backend_health: list[BackendHealth] = field(default_factory=list)

    def __post_init__(self) -> None:
        spaces = list(self.spaces)
        workers = list(self.workers)
        bindings = list(self.bindings)
        backend_health = list(self.backend_health)
        if not backend_health:
            backend_health = [
                herdr_backend_health(
                    self.outcome,
                    message=self.message or None,
                    spaces=spaces,
                    workers=workers,
                )
            ]
        object.__setattr__(self, "spaces", spaces)
        object.__setattr__(self, "workers", workers)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "backend_health", backend_health)

    @property
    def healthy(self) -> bool:
        return self.status == "healthy" and self.health.status == "healthy"

    @property
    def health(self) -> BackendHealth:
        for item in self.backend_health:
            if item.name == _BACKEND_NAME:
                return item
        return herdr_backend_health(self.outcome, message=self.message or None, spaces=self.spaces, workers=self.workers)


_FORBIDDEN_CONNECTOR_FIELDS_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_CONNECTOR_FIELDS}


def _compact_field_name(key: object) -> str:
    """Normalize field names for conservative connector/status-key matching."""
    return str(key).lower().replace("-", "_").replace(".", "_").replace("_", "")


def _field_matches(key: object, expected: str) -> bool:
    """Return True when a payload key matches snake_case or camelCase spelling."""
    return _compact_field_name(key) == _compact_field_name(expected)


def _private_fingerprint(value: Any) -> str:
    """Return a private adapter-only fingerprint without public sanitization."""
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _is_forbidden_connector_field(key: object) -> bool:
    """Return True for forbidden connector fields, including common case variants."""
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    compact = _compact_field_name(key)
    segments = {part for part in normalized.split("_") if part}
    return (
        normalized in _FORBIDDEN_CONNECTOR_FIELDS
        or compact in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT
        or bool(segments & _FORBIDDEN_CONNECTOR_FIELDS)
    )


def _contains_forbidden_connector_text(value: object) -> bool:
    """Return True when diagnostic text names connector/private delivery fields."""
    normalized = str(value).lower().replace("-", "_").replace(".", "_")
    compact = normalized.replace("_", "")
    return any(field in normalized for field in _FORBIDDEN_CONNECTOR_FIELDS) or any(
        field in compact for field in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT
    )


def _run_herdr(
    args: Sequence[str],
    config: Config,
    *,
    timeout_seconds: float | None = None,
) -> subprocess.CompletedProcess[str] | None:
    """Run the Herdr CLI with read-only arguments; return None on launch failure."""
    timeout = config.herdr_timeout_seconds if timeout_seconds is None else timeout_seconds
    try:
        return subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError):
        return None


def _run_herdr_probe(
    args: Sequence[str],
    config: Config,
    timeout_seconds: float | None,
) -> subprocess.CompletedProcess[str] | None:
    """Call _run_herdr with timeout support while preserving simple test fakes."""
    if timeout_seconds is None:
        return _run_herdr(args, config)
    try:
        return _run_herdr(args, config, timeout_seconds=timeout_seconds)
    except TypeError:
        return _run_herdr(args, config)


def _probe_herdr(
    args: Sequence[str],
    config: Config,
    budget: _ProbeBudget | None = None,
) -> tuple[str, Any]:
    """Run a read-only Herdr command and retain failure class for mutations."""
    timeout_seconds: float | None = None
    if budget is not None:
        timeout_seconds = budget.subprocess_timeout_seconds()
        if timeout_seconds is None:
            return "deadline_exhausted", None
    try:
        completed = _run_herdr_probe(args, config, timeout_seconds)
    except subprocess.TimeoutExpired:
        return "timeout", None
    if completed is None:
        return "launch_error", None

    if completed.returncode != 0:
        return "nonzero", None
    payload = _parse_json_output(completed.stdout)
    if payload is None:
        return "malformed_json", None
    return "ok", payload


def _parse_json_output(stdout: str | None) -> Any:
    """Best-effort parse of herdr JSON output; None on failure."""
    if not stdout or not stdout.strip():
        return None
    try:
        return json.loads(stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _command_payload(args: Sequence[str], config: Config) -> Any:
    """Return parsed JSON for a single herdr command, or None on any bad output."""
    outcome, payload = _probe_herdr(args, config)
    if outcome != "ok":
        return None
    return payload


def _command_payload_variants(variants: Sequence[Sequence[str]], config: Config) -> Any:
    """Try a sequence of herdr arg lists in order; return first successful payload."""
    outcome, payload = _probe_payload_variants(variants, config)
    return payload if outcome == "ok" else None


def _safe_text_sample(value: str | None) -> str | None:
    """Return a short diagnostic text sample with obvious sensitive markers redacted."""
    if not value:
        return None
    sample = value.strip()
    if not sample:
        return None
    lowered = sample.lower()
    if _contains_forbidden_connector_text(lowered):
        return None
    redacted_words: list[str] = []
    for word in sample.split():
        word_lower = word.lower()
        if "token" in word_lower or "secret" in word_lower or "password" in word_lower:
            redacted_words.append("[redacted]")
        else:
            redacted_words.append(word)
    sanitized = " ".join(redacted_words)
    if len(sanitized) > 200:
        sanitized = sanitized[:197] + "..."
    return sanitized


def _diagnostic_item_count(payload: Any, keys: Sequence[str]) -> int:
    return len(_payload_items(payload, keys))


def _diagnostic_check(
    name: str,
    args: Sequence[str],
    config: Config,
    keys: Sequence[str],
    budget: _ProbeBudget,
) -> dict[str, Any]:
    """Run one read-only Herdr command and return a sanitized diagnostic record."""
    check: dict[str, Any] = {
        "name": name,
        "ok": False,
        "outcome": "unknown",
        "timeout_seconds": config.herdr_timeout_seconds,
        "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
    }
    timeout_seconds = budget.subprocess_timeout_seconds()
    if timeout_seconds is None:
        check["outcome"] = "deadline_exhausted"
        return check
    try:
        completed = subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        check["outcome"] = "timeout"
        return check
    except (OSError, UnicodeDecodeError, ValueError, TypeError):
        check["outcome"] = "launch_error"
        return check

    check["exit_code"] = int(completed.returncode)
    if completed.returncode != 0:
        check["outcome"] = "nonzero"
        stdout_sample = _safe_text_sample(completed.stdout)
        stderr_sample = _safe_text_sample(completed.stderr)
        if stdout_sample is not None:
            check["stdout_sample"] = stdout_sample
        if stderr_sample is not None:
            check["stderr_sample"] = stderr_sample
        return check

    payload = _parse_json_output(completed.stdout)
    if payload is None:
        check["outcome"] = "malformed_json"
        stdout_sample = _safe_text_sample(completed.stdout)
        if stdout_sample is not None:
            check["stdout_sample"] = stdout_sample
        return check

    item_count = _diagnostic_item_count(payload, keys)
    check["ok"] = True
    check["item_count"] = item_count
    check["outcome"] = "healthy_non_empty" if item_count else "empty_healthy"
    return check


def diagnose_herdr(config: Config) -> dict[str, Any]:
    """Return JSON-serializable read-only Herdr CLI diagnostics."""
    groups = [
        [
            ("workspace_list", ["workspace", "list"], ("workspaces", "spaces", "data", "items", "results", "result")),
            ("workspace_list_json", ["workspace", "list", "--json"], ("workspaces", "spaces", "data", "items", "results", "result")),
        ],
        [
            ("agent_list", ["agent", "list"], ("agents", "workers", "data", "items", "results", "result")),
            ("agent_list_json", ["agent", "list", "--json"], ("agents", "workers", "data", "items", "results", "result")),
        ],
        [
            ("pane_list", ["pane", "list"], ("panes", "items", "data", "results", "result")),
            ("pane_list_json", ["pane", "list", "--json"], ("panes", "items", "data", "results", "result")),
        ],
    ]
    planned = [check for group in groups for check in group]
    result: dict[str, Any] = {
        "schema_version": 1,
        "command": "doctor",
        "herdr_bin": config.herdr_bin,
        "timeout_seconds": config.herdr_timeout_seconds,
        "aggregate_deadline_seconds": config.herdr_timeout_seconds * len(planned),
        "status": "ok",
        "checks": [],
    }
    try:
        binary_path = shutil.which(config.herdr_bin)
    except (TypeError, ValueError, OSError):
        binary_path = None

    if binary_path is None:
        result["status"] = "unavailable"
        result["checks"] = [
            {
                "name": name,
                "ok": False,
                "outcome": "missing_binary",
                "timeout_seconds": config.herdr_timeout_seconds,
                "aggregate_deadline_seconds": result["aggregate_deadline_seconds"],
            }
            for name, args, _keys in planned
        ]
        return result

    budget = _ProbeBudget.from_config(config, planned_probes=len(planned))
    checks: list[dict[str, Any]] = []
    stop_after_timeout = False
    stop_after_outcome = ""
    for group in groups:
        if stop_after_timeout:
            break
        for index, (name, args, keys) in enumerate(group):
            if index > 0 and checks[-1]["ok"]:
                break
            check = _diagnostic_check(name, args, config, keys, budget)
            checks.append(check)
            if check["outcome"] in _DEADLINE_EXHAUSTED_OUTCOMES:
                stop_after_timeout = True
                stop_after_outcome = str(check["outcome"])
                break

    names_seen = {str(check["name"]) for check in checks}
    remaining_planned = [
        (name, args, keys)
        for name, args, keys in planned
        if name not in names_seen
    ]
    if stop_after_timeout:
        skipped_outcome = (
            "skipped_after_deadline"
            if stop_after_outcome == "deadline_exhausted"
            else "skipped_after_timeout"
        )
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "outcome": skipped_outcome,
                    "timeout_seconds": config.herdr_timeout_seconds,
                    "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
                }
            )
    else:
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "ok": True,
                    "outcome": "skipped_not_needed",
                    "timeout_seconds": config.herdr_timeout_seconds,
                    "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
                }
            )

    result["checks"] = checks
    if any(check["outcome"] in _DEADLINE_EXHAUSTED_OUTCOMES for check in checks):
        result["status"] = "timeout"
    elif any(not check["ok"] for check in checks):
        result["status"] = "degraded"
    return result


def _strip_connector_fields(value: Any) -> Any:
    """Recursively drop connector/delivery fields from arbitrary JSON-like values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if _is_forbidden_connector_field(key):
                continue
            clean[key_text] = _strip_connector_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_connector_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_connector_fields(item) for item in value]
    return value


def _strip_status_fields(value: Any) -> Any:
    """Recursively drop raw status fields from metadata values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            clean[str(key)] = _strip_status_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_status_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_status_fields(item) for item in value]
    return value


def _payload_items(payload: Any, keys: Sequence[str]) -> list[dict[str, Any]]:
    """Extract object records from conservative herdr list payload shapes."""
    if isinstance(payload, list):
        candidates: Iterable[Any] = payload
    elif isinstance(payload, Mapping):
        candidates = ()
        for key in keys:
            value = _value_for_key(payload, key)
            if isinstance(value, list):
                candidates = value
                break
            if isinstance(value, Mapping):
                nested = _payload_items(value, keys)
                if nested:
                    return nested
    else:
        return []

    items: list[dict[str, Any]] = []
    for item in candidates:
        if isinstance(item, Mapping):
            stripped = _strip_connector_fields(item)
            if isinstance(stripped, dict):
                items.append(stripped)
    return items


def _first_text(item: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    """Return the first scalar string value for any key."""
    for key in keys:
        value = _value_for_key(item, key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            return str(value)
    return None


def _nested_text(item: Mapping[str, Any], *path: str) -> str | None:
    """Return the first scalar string value reachable via a dotted key path."""
    current: Any = item
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = _value_for_key(current, key)
    if current is None:
        return None
    if isinstance(current, (str, int, float, bool)):
        return str(current)
    if isinstance(current, Mapping):
        return _first_text(current, ("id", "value", "name", "label"))
    return None


def _related_id(value: Any) -> str | None:
    """Return a neutral related-object id from a scalar or mapping."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        return _first_text(value, ("id", "workspace_id", "space_id", "slug", "name"))
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return None


def _normalize_status(raw_status: Any) -> tuple[str, str | None]:
    """Return canonical status plus original raw string when normalization changed it."""
    if raw_status is None:
        return "unknown", None

    raw_text = str(raw_status)
    canonical = normalize_status(raw_text)
    raw_key = raw_text.strip().lower().replace("_", "-")
    raw_meta = raw_text if raw_text and raw_key != canonical else None
    return canonical, raw_meta


def _value_for_key(item: Mapping[str, Any], expected_key: str) -> Any:
    """Return a value by exact key or snake/camel-case equivalent."""
    if expected_key in item:
        return item[expected_key]
    for key, value in item.items():
        if _field_matches(key, expected_key):
            return value
    return None


def _status_from_item(item: Mapping[str, Any]) -> tuple[str, str | None]:
    """Extract and normalize status-like fields from a herdr record."""
    for key in _STATUS_KEYS:
        value = _value_for_key(item, key)
        if value is not None:
            return _normalize_status(value)
    return "unknown", None


def _meta_from_item(item: Mapping[str, Any], excluded_keys: set[str], raw_status: str | None) -> dict[str, Any]:
    """Build sanitized neutral metadata for a projected model."""
    explicit_meta = _value_for_key(item, "meta")
    meta = {
        str(key): _strip_status_fields(value)
        for key, value in item.items()
        if not _field_matches(key, "meta")
        and not any(_field_matches(key, excluded_key) for excluded_key in excluded_keys)
        and not any(_field_matches(key, status_key) for status_key in _STATUS_KEYS)
    }
    if isinstance(explicit_meta, Mapping):
        for key, value in explicit_meta.items():
            if _is_forbidden_connector_field(key):
                continue
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            meta[str(key)] = _strip_status_fields(value)
    if raw_status is not None:
        meta["raw_status"] = raw_status
    return meta


def _space_id_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a stable space id, preferring explicit Herdr workspace_id."""
    return _first_text(item, ("workspace_id", "space_id", "id", "slug", "name")) or "unknown"


def _space_name_from_item(item: Mapping[str, Any], space_id: str) -> str:
    """Resolve a space name, preferring label then workspace_id."""
    return _first_text(item, ("label", "name", "title", "workspace_id", "space_id")) or space_id


def _public_worker_id_base_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a neutral public worker id without terminal/session handles."""
    return (
        _first_text(item, ("worker_id", "id", "slug", "agent_id"))
        or _first_text(item, ("agent", "name", "label", "title"))
        or "unknown"
    )


def _worker_id_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a stable public worker id."""
    return _public_worker_id_base_from_item(item)


def _private_identity_material_from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Return private Herdr identity material that is never serialized publicly."""
    agent_id = _first_text(item, ("agent_id",))
    agent_session = _nested_text(item, "agent_session", "value")
    session_id = _first_text(item, ("session_id",))
    if agent_id or agent_session or session_id:
        return {
            "agent_id": agent_id,
            "agent_session": agent_session,
            "session_id": session_id,
            "space_id": _worker_space_id_from_item(item),
        }
    base = {
        "public_id": _public_worker_id_base_from_item(item),
        "name": _first_text(item, ("agent", "name", "label", "title")),
        "space_id": _worker_space_id_from_item(item),
    }
    base["terminal_id"] = _first_text(item, ("terminal_id",))
    base["pane_id"] = _first_text(item, ("pane_id",))
    return base


def _private_identity_from_item(item: Mapping[str, Any], config: Config | None = None) -> str:
    """Return a private identity used only to avoid collapsing distinct workers."""
    material = _private_identity_material_from_item(item)
    if config is None:
        return _private_fingerprint(material)
    return worker_binding_private_fingerprint(
        host_id=config.host_id,
        backend=_BACKEND_NAME,
        identity_material=material,
    )


def _private_backend_target(kind: str, value: str, *, sendable: bool = True, reason: str | None = None) -> dict[str, Any]:
    """Return the internal backend target shape."""
    return {
        "kind": kind,
        "value": value,
        "sendable": bool(sendable),
        "reason": reason,
    }


def _backend_target_from_item(item: Mapping[str, Any]) -> dict[str, Any] | None:
    """Resolve the private Herdr send target from backend-observed fields."""
    candidates = (
        ("agent_id", _first_text(item, ("agent_id",))),
        ("terminal_id", _first_text(item, ("terminal_id",))),
        ("pane_id", _first_text(item, ("pane_id",))),
        ("agent", _first_text(item, ("agent",))),
        ("name", _first_text(item, ("name",))),
        ("label", _first_text(item, ("label",))),
    )
    for kind, value in candidates:
        if value:
            return _private_backend_target(kind, value)
    return None


def _turn_target_from_item(item: Mapping[str, Any]) -> tuple[str, str] | None:
    """Resolve the private Herdr structured-turn target from backend-observed fields."""
    agent_name = (_first_text(item, ("agent", "name")) or "").strip().lower()
    agent_session = _nested_text(item, "agent_session", "value")
    if agent_name == "codex" and agent_session:
        return "codex_session_id", agent_session
    if agent_name == "omp" and agent_session:
        # oh-my-pi reports its native session file path (herdr:omp source).
        return "omp_session_path", agent_session
    pane_id = _first_text(item, ("pane_id",))
    if pane_id:
        return "pane_id", pane_id
    return None


def _worker_with_id(worker: Worker, worker_id: str) -> Worker:
    """Return a worker copy with a disambiguated public id."""
    return Worker(
        id=worker_id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
        backend_target=worker.backend_target,
    )


def _worker_name_from_item(item: Mapping[str, Any], worker_id: str) -> str:
    """Resolve a worker display name, preferring agent then label."""
    return _first_text(item, ("agent", "label", "name", "title")) or worker_id


def _worker_space_id_from_item(item: Mapping[str, Any]) -> str | None:
    """Resolve a worker's parent space id, preferring workspace_id."""
    return (
        _first_text(item, ("workspace_id", "space_id", "spaceId", "workspaceId"))
        or _related_id(_value_for_key(item, "space"))
        or _related_id(_value_for_key(item, "workspace"))
    )


def _spaces_from_payload(payload: Any) -> list[Space]:
    """Extract neutral Space objects from a herdr workspace-list payload."""
    spaces: list[Space] = []
    for item in _payload_items(payload, ("workspaces", "spaces", "data", "items", "results", "result")):
        space_id = _space_id_from_item(item)
        name = _space_name_from_item(item, space_id)
        status, raw_status = _status_from_item(item)
        updated_at = _first_text(item, ("updated_at", "last_seen_at", "observed_at", "timestamp"))
        status_line = _first_text(item, ("status_line", "summary", "description"))
        meta = _meta_from_item(
            item,
            {
                "id",
                "workspace_id",
                "space_id",
                "slug",
                "name",
                "title",
                "label",
                "meta",
                "updated_at",
                "last_seen_at",
                "observed_at",
                "timestamp",
                "status_line",
                "summary",
                "description",
                "fingerprint",
                "agent_status",
            },
            raw_status,
        )
        spaces.append(
            Space(
                id=space_id,
                name=name,
                status=status,
                meta=meta,
                updated_at=updated_at,
                status_line=status_line,
            )
        )
    return spaces


def _worker_from_item(item: Mapping[str, Any]) -> Worker:
    """Build a neutral Worker from a single herdr agent/pane record."""
    worker_id = _worker_id_from_item(item)
    name = _worker_name_from_item(item, worker_id)
    status, raw_status = _status_from_item(item)
    last_seen_at = _first_text(item, ("last_seen_at", "updated_at", "observed_at", "timestamp"))
    summary = _first_text(item, ("summary", "status_line", "description"))
    space_id = _worker_space_id_from_item(item)
    meta = _meta_from_item(
        item,
        {
            "id",
            "agent_id",
            "worker_id",
            "slug",
            "name",
            "title",
            "agent",
            "meta",
            "space_id",
            "workspace_id",
            "spaceId",
            "workspaceId",
            "space",
            "workspace",
            "last_seen_at",
            "updated_at",
            "observed_at",
            "timestamp",
            "summary",
            "status_line",
            "description",
            "fingerprint",
            "agent_status",
            "backend_target",
            "terminal_id",
            "pane_id",
            "agent_session",
            "session_id",
        },
        raw_status,
    )
    return Worker(
        id=worker_id,
        name=name,
        status=status,
        space_id=space_id,
        meta=meta,
        last_seen_at=last_seen_at,
        summary=summary,
        backend_target=_backend_target_from_item(item),
    )


def _output_excerpt_limit(config: Config | None) -> int | None:
    if config is None:
        return None
    try:
        limit = int(getattr(config, "output_excerpt_chars"))
    except (TypeError, ValueError):
        return None
    return max(1, limit)


def _bounded_excerpt(value: str | None, limit: int | None) -> str | None:
    if value is None or limit is None:
        return value
    text = str(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def _worker_with_summary(worker: Worker, summary: str | None) -> Worker:
    if summary == worker.summary:
        return worker
    return Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=summary,
        backend_target=worker.backend_target,
    )


def _worker_record_from_item(item: Mapping[str, Any], config: Config | None = None) -> _WorkerRecord:
    worker = _worker_from_item(item)
    worker = _worker_with_summary(worker, _bounded_excerpt(worker.summary, _output_excerpt_limit(config)))
    turn_target = _turn_target_from_item(item)
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=_private_identity_from_item(item, config),
        turn_target_kind=turn_target[0] if turn_target is not None else None,
        turn_target_value=turn_target[1] if turn_target is not None else None,
    )


def _worker_with_backend_target(worker: Worker, backend_target: dict[str, Any] | None) -> Worker:
    return Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
        fingerprint=worker.fingerprint,
        backend_target=backend_target,
    )


def _backend_target_send_token(target: Mapping[str, Any] | None) -> str:
    """Return the exact value passed as herdr agent send's target argv token."""
    if not isinstance(target, Mapping):
        return ""
    return str(target.get("value") or "")


def _mark_backend_sendability(workers: list[Worker]) -> list[Worker]:
    """Mark unsupported or duplicate final Herdr send tokens as not sendable."""
    marked: list[Worker] = []
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, dict):
            marked.append(worker)
            continue
        kind = str(target.get("kind") or "")
        value = _backend_target_send_token(target)
        if kind not in _BACKEND_TARGET_KINDS or not value:
            marked.append(
                _worker_with_backend_target(
                    worker,
                    _private_backend_target(kind or "agent", value, sendable=False, reason="backend_unsupported"),
                )
            )
            continue
        marked.append(_worker_with_backend_target(worker, _private_backend_target(kind, value)))

    send_token_counts = Counter(
        _backend_target_send_token(worker.backend_target)
        for worker in marked
        if isinstance(worker.backend_target, dict) and worker.backend_target.get("sendable") is True
    )
    final: list[Worker] = []
    for worker in marked:
        target = worker.backend_target
        if not isinstance(target, dict) or target.get("sendable") is not True:
            final.append(worker)
            continue
        kind = str(target.get("kind") or "")
        value = _backend_target_send_token(target)
        if send_token_counts[value] > 1:
            final.append(
                _worker_with_backend_target(
                    worker,
                    _private_backend_target(kind, value, sendable=False, reason="duplicate_backend_target"),
                )
            )
            continue
        final.append(worker)
    return final


def assert_unique_sendable_backend_targets(workers: Iterable[Worker]) -> bool:
    """Prove no sendable workers share the same final Herdr argv target token."""
    seen: set[str] = set()
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, Mapping) or target.get("sendable") is not True:
            continue
        token = _backend_target_send_token(target)
        if not token:
            raise AssertionError("sendable backend target is missing a send token")
        if token in seen:
            raise AssertionError("duplicate sendable backend target token")
        seen.add(token)
    return True


def _binding_target_key(binding: WorkerBinding) -> tuple[str, str]:
    return (binding.target_kind, binding.target_value)


def _safe_stored_binding_for_reuse(binding: WorkerBinding) -> bool:
    return bool(binding.worker_id) and (binding.reason or "") not in _AMBIGUOUS_BINDING_REASONS


def _worker_target_key(worker: Worker) -> tuple[str, str] | None:
    target = worker.backend_target
    if not isinstance(target, Mapping):
        return None
    kind = str(target.get("kind") or "")
    value = str(target.get("value") or "")
    if not kind or not value:
        return None
    return kind, value


def _record_with_worker(record: _WorkerRecord, worker: Worker) -> _WorkerRecord:
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=record.private_fingerprint,
        turn_target_kind=record.turn_target_kind,
        turn_target_value=record.turn_target_value,
    )


def _reuse_worker_ids_from_bindings(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None,
) -> list[_WorkerRecord]:
    """Reuse stable public ids from private binding matches when safe."""
    if not stored_bindings:
        return records

    by_private: dict[str, list[WorkerBinding]] = {}
    by_target: dict[tuple[str, str], list[WorkerBinding]] = {}
    for binding in stored_bindings:
        if binding.backend != _BACKEND_NAME:
            continue
        by_private.setdefault(binding.private_fingerprint, []).append(binding)
        if _safe_stored_binding_for_reuse(binding):
            key = _binding_target_key(binding)
            if key[0] and key[1]:
                by_target.setdefault(key, []).append(binding)

    current_private_counts = Counter(record.private_fingerprint for record in records)
    current_target_counts = Counter(
        key
        for key in (_worker_target_key(record.worker) for record in records)
        if key is not None
    )

    reused: list[_WorkerRecord] = []
    for record in records:
        worker = record.worker
        private_fingerprint = record.private_fingerprint
        matched = None
        private_candidates = [
            binding
            for binding in by_private.get(private_fingerprint, [])
            if _safe_stored_binding_for_reuse(binding)
        ]
        if current_private_counts[private_fingerprint] == 1 and len(private_candidates) == 1:
            matched = private_candidates[0]
        if matched is None:
            key = _worker_target_key(worker)
            candidates = by_target.get(key or ("", ""), [])
            if key is not None and current_target_counts[key] == 1 and len(candidates) == 1:
                matched = candidates[0]
        if matched is not None and matched.worker_id:
            reused.append(_record_with_worker(record, _worker_with_id(worker, matched.worker_id)))
        else:
            reused.append(record)
    return reused


def _deduplicated_worker_records(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[_WorkerRecord]:
    """Drop exact duplicates, then disambiguate duplicate public ids."""
    records = _reuse_worker_ids_from_bindings(records, stored_bindings)
    seen: set[tuple[str, str, str | None, str, str, str]] = set()
    unique: list[_WorkerRecord] = []
    for record in records:
        worker = record.worker
        backend_kind = ""
        backend_value = ""
        if worker.backend_target:
            backend_kind = str(worker.backend_target.get("kind", ""))
            backend_value = str(worker.backend_target.get("value", ""))
        key = (worker.id, worker.name, worker.space_id, backend_kind, backend_value, record.private_fingerprint)
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)

    groups: dict[str, list[_WorkerRecord]] = {}
    for record in unique:
        groups.setdefault(record.worker.id, []).append(record)

    disambiguated: list[_WorkerRecord] = []
    for worker_id, group in groups.items():
        if len(group) == 1:
            disambiguated.append(group[0])
            continue
        ordered = sorted(
            group,
            key=lambda record: (
                record.worker.name,
                record.worker.space_id or "",
                str((record.worker.backend_target or {}).get("kind", "")),
                str((record.worker.backend_target or {}).get("value", "")),
                record.private_fingerprint,
                stable_fingerprint(
                    {
                        "id": record.worker.id,
                        "name": record.worker.name,
                        "space_id": record.worker.space_id,
                        "status": record.worker.status,
                        "summary": record.worker.summary,
                    }
                ),
            ),
        )
        for index, record in enumerate(ordered, start=1):
            disambiguated.append(_record_with_worker(record, _worker_with_id(record.worker, f"{worker_id}-{index}")))

    disambiguated = sorted(disambiguated, key=lambda record: record.worker.id)
    workers = _mark_backend_sendability([record.worker for record in disambiguated])
    assert_unique_sendable_backend_targets(workers)
    return [
        _record_with_worker(record, worker)
        for record, worker in zip(disambiguated, workers, strict=True)
    ]


def _deduplicate_worker_records(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    return [record.worker for record in _deduplicated_worker_records(records, stored_bindings)]


def _deduplicate_workers(workers: list[Worker]) -> list[Worker]:
    return _deduplicate_worker_records(
        [_WorkerRecord(worker=worker, private_fingerprint=worker.fingerprint) for worker in workers]
    )


def _binding_from_worker_record(
    config: Config,
    record: _WorkerRecord,
    observed_at: str,
) -> WorkerBinding | None:
    worker = record.worker
    target = worker.backend_target
    if not isinstance(target, Mapping):
        return None
    target_kind = str(target.get("kind") or "")
    target_value = str(target.get("value") or "")
    if not target_kind or not target_value:
        return None
    reason = target.get("reason")
    return WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend=_BACKEND_NAME,
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=record.turn_target_kind,
        turn_target_value=record.turn_target_value,
        sendable=target.get("sendable") is True,
        reason=str(reason) if reason is not None else None,
        observed_at=observed_at,
        expires_at=None,
        private_fingerprint=record.private_fingerprint,
    )


def _workers_and_bindings_from_records(
    config: Config,
    records: list[_WorkerRecord],
    *,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> tuple[list[Worker], list[WorkerBinding]]:
    observed_at = utc_timestamp()
    deduplicated = _deduplicated_worker_records(records, stored_bindings)
    workers = [record.worker for record in deduplicated]
    bindings = [
        binding
        for record in deduplicated
        if (binding := _binding_from_worker_record(config, record, observed_at)) is not None
    ]
    return workers, separate_duplicate_worker_bindings(bindings)


def bindings_from_workers(
    config: Config,
    workers: Sequence[Worker],
    *,
    observed_at: str | None = None,
) -> list[WorkerBinding]:
    """Build private Herdr bindings from in-memory workers when raw records are absent."""
    timestamp = observed_at or utc_timestamp()
    workers = _mark_backend_sendability(list(workers))
    bindings: list[WorkerBinding] = []
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, Mapping):
            continue
        target_kind = str(target.get("kind") or "")
        target_value = str(target.get("value") or "")
        if not target_kind or not target_value:
            continue
        private_fingerprint = worker_binding_private_fingerprint(
            host_id=config.host_id,
            backend=_BACKEND_NAME,
            identity_material={
                "worker_id": worker.id,
                "worker_fingerprint": worker.fingerprint,
                "target_kind": target_kind,
                "target_value": target_value,
            },
        )
        record = _WorkerRecord(worker=worker, private_fingerprint=private_fingerprint)
        binding = _binding_from_worker_record(config, record, timestamp)
        if binding is not None:
            bindings.append(binding)
    return separate_duplicate_worker_bindings(bindings)


def _binding_for_worker(worker: Worker, bindings: Sequence[WorkerBinding]) -> WorkerBinding | None:
    candidates = [binding for binding in bindings if binding.worker_id == worker.id]
    if not candidates:
        return None
    exact = [binding for binding in candidates if binding.worker_fingerprint == worker.fingerprint]
    if len(exact) == 1:
        return exact[0]
    if exact:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return None


def rehydrate_workers_from_bindings(
    workers: Sequence[Worker],
    current_bindings: Sequence[WorkerBinding] | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Attach private backend targets to workers from current, then stored bindings."""
    current = list(current_bindings or [])
    stored = list(stored_bindings or [])
    rehydrated: list[Worker] = []
    for worker in workers:
        binding = _binding_for_worker(worker, current)
        if binding is not None:
            rehydrated.append(_worker_with_backend_target(worker, binding.backend_target()))
            continue
        if isinstance(worker.backend_target, Mapping):
            rehydrated.append(worker)
            continue
        binding = _binding_for_worker(worker, stored)
        if binding is None:
            rehydrated.append(worker)
        else:
            rehydrated.append(_worker_with_backend_target(worker, binding.backend_target()))
    return rehydrated


def _workers_from_payload(
    payload: Any,
    config: Config | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Extract neutral Worker objects from a herdr agent-list payload."""
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results", "result")):
        records.append(_worker_record_from_item(item, config))
    return _deduplicate_worker_records(records, stored_bindings)


def _workers_and_bindings_from_payload(
    payload: Any,
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> tuple[list[Worker], list[WorkerBinding]]:
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results", "result")):
        records.append(_worker_record_from_item(item, config))
    return _workers_and_bindings_from_records(config, records, stored_bindings=stored_bindings)


def _pane_has_agent(item: Mapping[str, Any]) -> bool:
    """Return True when a pane record carries an agent or explicit agent marker."""
    if _value_for_key(item, "agent") is not None:
        return True
    if _value_for_key(item, "agent_session") is not None:
        return True
    if _nested_text(item, "agent_session", "value"):
        return True
    markers = _value_for_key(item, "state_labels") or _value_for_key(item, "labels") or []
    if isinstance(markers, list):
        for marker in markers:
            if isinstance(marker, str) and "agent" in marker.lower():
                return True
    return False


def _workers_from_pane_payload(
    payload: Any,
    config: Config | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Extract worker objects from herdr pane list, only for agent-bearing panes."""
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("panes", "items", "data", "results", "result")):
        if not _pane_has_agent(item):
            continue
        records.append(_worker_record_from_item(item, config))
    return _deduplicate_worker_records(records, stored_bindings)


def _workers_and_bindings_from_pane_payload(
    payload: Any,
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> tuple[list[Worker], list[WorkerBinding]]:
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("panes", "items", "data", "results", "result")):
        if not _pane_has_agent(item):
            continue
        records.append(_worker_record_from_item(item, config))
    return _workers_and_bindings_from_records(config, records, stored_bindings=stored_bindings)


def _probe_payload_variants(
    variants: Sequence[Sequence[str]],
    config: Config,
    budget: _ProbeBudget | None = None,
) -> tuple[str, Any]:
    outcomes: list[str] = []
    for args in variants:
        if budget is None:
            outcome, payload = _probe_herdr(args, config)
        else:
            try:
                outcome, payload = _probe_herdr(args, config, budget)
            except TypeError:
                outcome, payload = _probe_herdr(args, config)
        if outcome == "ok":
            return outcome, payload
        if outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
            return outcome, None
        outcomes.append(outcome)
    if outcomes and all(outcome == "launch_error" for outcome in outcomes):
        return "launch_error", None
    if "malformed_json" in outcomes:
        return "malformed_json", None
    if "nonzero" in outcomes:
        return "nonzero", None
    return outcomes[-1] if outcomes else "nonzero", None


def _degraded_observation(outcome: str, message: str) -> HerdrCommandObservation:
    health = herdr_backend_health(outcome, message=message)
    return HerdrCommandObservation(
        spaces=[],
        workers=[],
        status=health.status,
        outcome=outcome,
        message=message,
        backend_health=[health],
    )


def _snapshot_observation(
    spaces: list[Space],
    workers: list[Worker],
    bindings: list[WorkerBinding],
    outcome: str,
    *,
    message: str | None = None,
) -> HerdrSnapshotObservation:
    health = herdr_backend_health(
        outcome,
        message=message,
        spaces=spaces,
        workers=workers,
    )
    return HerdrSnapshotObservation(
        spaces=spaces,
        workers=workers,
        bindings=bindings,
        backend_health=[health],
    )


def fetch_herdr_command_observation(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> HerdrCommandObservation:
    """Return Herdr observations plus health metadata for mutation safety."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return _degraded_observation("missing_binary", "Herdr binary is unavailable")
    except (TypeError, ValueError, OSError):
        return _degraded_observation("launch_error", "Herdr binary could not be inspected")

    budget = _ProbeBudget.from_config(config, planned_probes=5)
    workspace_outcome, workspace_payload = _probe_payload_variants(
        [
            ["workspace", "list"],
            ["workspace", "list", "--json"],
        ],
        config,
        budget,
    )
    if workspace_outcome != "ok":
        return _degraded_observation(
            workspace_outcome,
            "Herdr workspace observation is not healthy",
        )

    agent_outcome, agent_payload = _probe_payload_variants(
        [
            ["agent", "list"],
            ["agent", "list", "--json"],
        ],
        config,
        budget,
    )
    if agent_outcome != "ok":
        return _degraded_observation(
            agent_outcome,
            "Herdr agent observation is not healthy",
        )

    spaces = _spaces_from_payload(workspace_payload)
    workers, bindings = _workers_and_bindings_from_payload(
        agent_payload,
        config,
        stored_bindings=stored_bindings,
    )
    if not workers:
        try:
            pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config, budget)
        except TypeError:
            pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config)
        if pane_outcome != "ok":
            return _degraded_observation(
                pane_outcome,
                "Herdr pane fallback observation is not healthy",
            )
        workers, bindings = _workers_and_bindings_from_pane_payload(
            pane_payload,
            config,
            stored_bindings=stored_bindings,
        )

    return HerdrCommandObservation(
        spaces=spaces,
        workers=workers,
        status="healthy",
        outcome="healthy_non_empty" if spaces or workers else "empty_healthy",
        bindings=bindings,
        backend_health=[
            herdr_backend_health(
                "healthy_non_empty" if spaces or workers else "empty_healthy",
                spaces=spaces,
                workers=workers,
            )
        ],
    )


def _state_result(
    spaces: list[Space],
    workers: list[Worker],
    bindings: list[WorkerBinding],
    include_bindings: bool,
) -> tuple[list[Space], list[Worker]] | tuple[list[Space], list[Worker], list[WorkerBinding]]:
    if include_bindings:
        return spaces, workers, bindings
    return spaces, workers


def fetch_herdr_snapshot_observation(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> HerdrSnapshotObservation:
    """Return Herdr snapshot observations plus public backend health."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return _snapshot_observation(
                [],
                [],
                [],
                "missing_binary",
                message=_HEALTH_MESSAGES["missing_binary"],
            )
    except (TypeError, ValueError, OSError):
        return _snapshot_observation(
            [],
            [],
            [],
            "launch_error",
            message="Herdr binary could not be inspected",
        )

    budget = _ProbeBudget.from_config(config, planned_probes=5)
    workspace_outcome, workspace_payload = _probe_payload_variants(
        [
            ["workspace", "list"],
            ["workspace", "list", "--json"],
        ],
        config,
        budget,
    )
    if workspace_outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
        return _snapshot_observation(
            [],
            [],
            [],
            workspace_outcome,
            message=_HEALTH_MESSAGES[workspace_outcome],
        )

    agent_outcome, agent_payload = _probe_payload_variants(
        [
            ["agent", "list"],
            ["agent", "list", "--json"],
        ],
        config,
        budget,
    )

    spaces = _spaces_from_payload(workspace_payload)
    if agent_outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
        return _snapshot_observation(
            spaces,
            [],
            [],
            agent_outcome,
            message=_HEALTH_MESSAGES[agent_outcome],
        )

    workers, bindings = _workers_and_bindings_from_payload(
        agent_payload,
        config,
        stored_bindings=stored_bindings,
    )
    pane_outcome = "ok"
    if not workers:
        try:
            pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config, budget)
        except TypeError:
            pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config)
        if pane_outcome != "ok":
            outcome = pane_outcome if pane_outcome in _DEADLINE_EXHAUSTED_OUTCOMES else pane_outcome
            return _snapshot_observation(
                spaces,
                [],
                [],
                outcome,
                message=_HEALTH_MESSAGES.get(outcome, _HEALTH_MESSAGES["unknown"]),
            )
        workers, bindings = _workers_and_bindings_from_pane_payload(
            pane_payload,
            config,
            stored_bindings=stored_bindings,
        )

    failed_outcomes = [
        outcome
        for outcome in (workspace_outcome, agent_outcome, pane_outcome)
        if outcome not in {"ok"}
    ]
    if failed_outcomes:
        outcome = failed_outcomes[0]
        return _snapshot_observation(
            spaces,
            workers,
            bindings,
            outcome,
            message=_HEALTH_MESSAGES.get(outcome, _HEALTH_MESSAGES["unknown"]),
        )

    return _snapshot_observation(
        spaces,
        workers,
        bindings,
        "healthy_non_empty" if spaces or workers else "empty_healthy",
    )


def fetch_herdr_state(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
    *,
    include_bindings: bool = False,
) -> tuple[list[Space], list[Worker]] | tuple[list[Space], list[Worker], list[WorkerBinding]]:
    """Return neutral spaces and workers from the Herdr CLI, or empty lists."""
    observation = fetch_herdr_snapshot_observation(
        config,
        stored_bindings=stored_bindings,
    )
    return _state_result(observation.spaces, observation.workers, observation.bindings, include_bindings)
