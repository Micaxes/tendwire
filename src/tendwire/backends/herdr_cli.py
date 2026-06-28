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
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..config import Config
from ..core.models import Space, Worker, normalize_status, stable_fingerprint


_HERDR_TIMEOUT_SECONDS = 5.0

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


_FORBIDDEN_CONNECTOR_FIELDS_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_CONNECTOR_FIELDS}


def _compact_field_name(key: object) -> str:
    """Normalize field names for conservative connector/status-key matching."""
    return str(key).lower().replace("-", "_").replace("_", "")


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
    compact = _compact_field_name(key)
    return str(key).lower().replace("-", "_") in _FORBIDDEN_CONNECTOR_FIELDS or compact in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT


def _run_herdr(args: Sequence[str], config: Config) -> subprocess.CompletedProcess[str] | None:
    """Run the Herdr CLI with read-only arguments; return None on any CLI failure."""
    try:
        return subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=config.herdr_timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError, ValueError, TypeError):
        return None


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
    result = _run_herdr(args, config)
    if result is None or result.returncode != 0:
        return None
    return _parse_json_output(result.stdout)


def _command_payload_variants(variants: Sequence[Sequence[str]], config: Config) -> Any:
    """Try a sequence of herdr arg lists in order; return first successful payload."""
    for args in variants:
        payload = _command_payload(args, config)
        if payload is not None:
            return payload
    return None


def _safe_text_sample(value: str | None) -> str | None:
    """Return a short diagnostic text sample with obvious sensitive markers redacted."""
    if not value:
        return None
    sample = value.strip()
    if not sample:
        return None
    lowered = sample.lower()
    if any(field in lowered for field in _FORBIDDEN_CONNECTOR_FIELDS):
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


def _diagnostic_check(name: str, args: Sequence[str], config: Config, keys: Sequence[str]) -> dict[str, Any]:
    """Run one read-only Herdr command and return a sanitized diagnostic record."""
    check: dict[str, Any] = {
        "name": name,
        "argv": [config.herdr_bin, *args],
        "ok": False,
        "outcome": "unknown",
        "timeout_seconds": config.herdr_timeout_seconds,
    }
    try:
        completed = subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=config.herdr_timeout_seconds,
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
                "argv": [config.herdr_bin, *args],
                "ok": False,
                "outcome": "missing_binary",
                "timeout_seconds": config.herdr_timeout_seconds,
            }
            for name, args, _keys in planned
        ]
        return result

    checks: list[dict[str, Any]] = []
    stop_after_timeout = False
    for group in groups:
        if stop_after_timeout:
            break
        for index, (name, args, keys) in enumerate(group):
            if index > 0 and checks[-1]["ok"]:
                break
            check = _diagnostic_check(name, args, config, keys)
            checks.append(check)
            if check["outcome"] == "timeout":
                stop_after_timeout = True
                break

    names_seen = {str(check["name"]) for check in checks}
    remaining_planned = [
        (name, args, keys)
        for name, args, keys in planned
        if name not in names_seen
    ]
    if stop_after_timeout:
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "argv": [config.herdr_bin, *args],
                    "ok": False,
                    "outcome": "skipped_after_timeout",
                    "timeout_seconds": config.herdr_timeout_seconds,
                }
            )
    else:
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "argv": [config.herdr_bin, *args],
                    "ok": True,
                    "outcome": "skipped_not_needed",
                    "timeout_seconds": config.herdr_timeout_seconds,
                }
            )

    result["checks"] = checks
    if any(check["outcome"] == "timeout" for check in checks):
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


def _backend_target_from_item(item: Mapping[str, Any]) -> dict[str, str] | None:
    """Resolve the private Herdr send target without using agent_session."""
    identity = _private_fingerprint(
        {
            "public_id": _public_worker_id_base_from_item(item),
            "name": _first_text(item, ("agent", "name", "label", "title")),
            "space_id": _worker_space_id_from_item(item),
            "agent_session": _nested_text(item, "agent_session", "value"),
            "session_id": _first_text(item, ("session_id",)),
            "terminal_id": _first_text(item, ("terminal_id",)),
            "pane_id": _first_text(item, ("pane_id",)),
        }
    )
    candidates = (
        ("agent_id", _first_text(item, ("agent_id",))),
        ("agent", _first_text(item, ("agent", "name", "label", "title"))),
        ("terminal_id", _first_text(item, ("terminal_id",))),
        ("pane_id", _first_text(item, ("pane_id",))),
    )
    for kind, value in candidates:
        if value:
            return {"kind": kind, "value": value, "identity": identity}
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
            "label",
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


def _deduplicate_workers(workers: list[Worker]) -> list[Worker]:
    """Drop exact duplicates, then disambiguate duplicate public ids."""
    seen: set[tuple[str, str, str | None, str, str, str]] = set()
    unique: list[Worker] = []
    for worker in workers:
        backend_kind = ""
        backend_value = ""
        if worker.backend_target:
            backend_kind = str(worker.backend_target.get("kind", ""))
            backend_value = str(worker.backend_target.get("value", ""))
            backend_identity = str(worker.backend_target.get("identity", ""))
        else:
            backend_identity = ""
        key = (worker.id, worker.name, worker.space_id, backend_kind, backend_value, backend_identity)
        if key in seen:
            continue
        seen.add(key)
        unique.append(worker)

    groups: dict[str, list[Worker]] = {}
    for worker in unique:
        groups.setdefault(worker.id, []).append(worker)

    disambiguated: list[Worker] = []
    for worker_id, group in groups.items():
        if len(group) == 1:
            disambiguated.extend(group)
            continue
        ordered = sorted(
            group,
            key=lambda w: (
                w.name,
                w.space_id or "",
                str((w.backend_target or {}).get("kind", "")),
                str((w.backend_target or {}).get("value", "")),
                str((w.backend_target or {}).get("identity", "")),
                stable_fingerprint(
                    {
                        "id": w.id,
                        "name": w.name,
                        "space_id": w.space_id,
                        "status": w.status,
                        "summary": w.summary,
                    }
                ),
            ),
        )
        for index, worker in enumerate(ordered, start=1):
            disambiguated.append(_worker_with_id(worker, f"{worker_id}-{index}"))

    return sorted(disambiguated, key=lambda w: w.id)


def _workers_from_payload(payload: Any) -> list[Worker]:
    """Extract neutral Worker objects from a herdr agent-list payload."""
    workers: list[Worker] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results", "result")):
        workers.append(_worker_from_item(item))
    return _deduplicate_workers(workers)


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


def _workers_from_pane_payload(payload: Any) -> list[Worker]:
    """Extract worker objects from herdr pane list, only for agent-bearing panes."""
    workers: list[Worker] = []
    for item in _payload_items(payload, ("panes", "items", "data", "results", "result")):
        if not _pane_has_agent(item):
            continue
        workers.append(_worker_from_item(item))
    return _deduplicate_workers(workers)


def fetch_herdr_state(config: Config) -> tuple[list[Space], list[Worker]]:
    """Return neutral spaces and workers from the Herdr CLI, or empty lists."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return [], []
    except (TypeError, ValueError, OSError):
        return [], []

    workspace_payload = _command_payload_variants(
        [
            ["workspace", "list"],
            ["workspace", "list", "--json"],
        ],
        config,
    )
    agent_payload = _command_payload_variants(
        [
            ["agent", "list"],
            ["agent", "list", "--json"],
        ],
        config,
    )

    spaces = _spaces_from_payload(workspace_payload)
    workers = _workers_from_payload(agent_payload)
    if not workers:
        pane_payload = _command_payload(["pane", "list"], config)
        workers = _workers_from_pane_payload(pane_payload)

    return spaces, workers
