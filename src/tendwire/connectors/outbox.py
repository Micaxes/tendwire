"""Neutral connector outbox API above the SQLite store.

This module is intentionally Tendwire-only. It exposes opaque refs and sanitized
payloads without importing core runtime connectors or backend-specific concepts.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..core.models import sanitize_forbidden_fields
from ..store.sqlite import (
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    poll_connector_outbox,
    reclaim_expired_connector_leases,
)


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    result = str(value).strip()
    return result if result else default


def _int(value: Any, default: int, *, minimum: int = 1, maximum: int = 100) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


_DROP = object()
_CONNECTOR_REF_PREFIX = "twref1."
_CONNECTOR_REF_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_CONNECTOR_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
_FORBIDDEN_PUBLIC_TEXT = (
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


def _contains_forbidden_public_text(value: str) -> bool:
    lowered = value.lower()
    compact = lowered.replace("-", "").replace("_", "").replace(".", "")
    return any(token in lowered or token.replace("_", "") in compact for token in _FORBIDDEN_PUBLIC_TEXT)


def _clean_public_value(value: Any) -> Any:
    clean = sanitize_forbidden_fields(value)
    if isinstance(clean, Mapping):
        result: dict[str, Any] = {}
        for key, item in clean.items():
            sanitized = _clean_public_value(item)
            if sanitized is not _DROP:
                result[str(key)] = sanitized
        return result
    if isinstance(clean, list):
        result_list: list[Any] = []
        for item in clean:
            sanitized = _clean_public_value(item)
            if sanitized is not _DROP:
                result_list.append(sanitized)
        return result_list
    if isinstance(clean, str) and _contains_forbidden_public_text(clean):
        return _DROP
    return clean


def _clean_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    clean = _clean_public_value(dict(value))
    return dict(clean) if isinstance(clean, Mapping) else {}


def _error(status: str, *, host_id: str, name: str = "", ref: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ok": False,
        "status": status,
        "host_id": host_id,
        "name": name,
        "error": {
            "code": status,
            "message": "request is invalid or no longer live",
        },
    }
    if ref is not None:
        payload["ref"] = ref
    return sanitize_forbidden_fields(payload)


def _ref(value: Any) -> str:
    ref = _text(value)
    if not ref.startswith(_CONNECTOR_REF_PREFIX):
        return ""
    token = ref[len(_CONNECTOR_REF_PREFIX) :]
    if not token or any(char not in _CONNECTOR_REF_CHARS for char in token):
        return ""
    return ref


def _name(value: Any) -> str:
    name = _text(value)
    if not name or len(name) > 64:
        return ""
    if any(char not in _CONNECTOR_NAME_CHARS for char in name):
        return ""
    if _contains_forbidden_public_text(name):
        return ""
    return name


class ConnectorOutboxAPI:
    """Public-neutral facade for connector.poll/ack/fail/defer."""

    def __init__(
        self,
        db_path: str | Path | None,
        host_id: str,
        *,
        default_lease_seconds: int = 60,
        max_attempts: int = 10,
    ) -> None:
        self.db_path = Path(db_path) if db_path is not None else None
        self.host_id = str(host_id)
        self.default_lease_seconds = max(1, int(default_lease_seconds))
        self.max_attempts = max(1, int(max_attempts))

    def _require_store(self, name: str = "") -> dict[str, Any] | None:
        if self.db_path is None:
            return _error("store_unavailable", host_id=self.host_id, name=name)
        return None

    def poll(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        name = _name(data.get("name"))
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        store_result = poll_connector_outbox(
            self.db_path,
            self.host_id,
            name,
            limit=_int(data.get("limit"), 1, minimum=1, maximum=100),
            lease_seconds=_int(
                data.get("lease_seconds"),
                self.default_lease_seconds,
                minimum=1,
                maximum=86400,
            ),
            max_attempts=self.max_attempts,
        )
        items: list[dict[str, Any]] = []
        for item in store_result.get("items", []):
            if not isinstance(item, Mapping):
                continue
            ref = _ref(item.get("ref"))
            if not ref:
                continue
            items.append(
                sanitize_forbidden_fields(
                    {
                        "ref": ref,
                        "key": str(item.get("key") or ""),
                        "attempt": int(item.get("attempt") or 0),
                        "leased_until": str(item.get("leased_until") or ""),
                        "available_at": str(item.get("available_at") or ""),
                        "payload": _clean_mapping(item.get("payload")),
                    }
                )
            )
        return sanitize_forbidden_fields(
            {
                "schema_version": 1,
                "ok": bool(store_result.get("ok", False)),
                "status": str(store_result.get("status") or "ok"),
                "host_id": self.host_id,
                "name": name,
                "items": items,
            }
        )

    def reclaim(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        name = _name(data.get("name"))
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return reclaim_expired_connector_leases(self.db_path, self.host_id, name)

    def _mutation_parts(self, params: Mapping[str, Any] | None) -> tuple[dict[str, Any], str, str | None]:
        data = dict(params or {})
        name = _name(data.get("name"))
        ref = _ref(data.get("ref"))
        if not name or not ref:
            return data, name, None
        return data, name, ref

    def ack(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data, name, live_ref = self._mutation_parts(params)
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        if live_ref is None:
            return _error("invalid_ref", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return ack_connector_delivery(
            self.db_path,
            host_id=self.host_id,
            name=name,
            ref=live_ref,
            response=_clean_mapping(data.get("response")),
        )

    def fail(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("fail", params)

    def defer(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("defer", params)

    def _schedule(self, action: str, params: Mapping[str, Any] | None) -> dict[str, Any]:
        data, name, live_ref = self._mutation_parts(params)
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        if live_ref is None:
            return _error("invalid_ref", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        kwargs = {
            "host_id": self.host_id,
            "name": name,
            "ref": live_ref,
            "reason": _text(data.get("reason")),
            "response": _clean_mapping(data.get("response")),
            "available_at": _text(data.get("available_at")) or None,
            "delay_seconds": _int(data.get("delay_seconds"), 60, minimum=0, maximum=31536000)
            if data.get("delay_seconds") is not None
            else None,
        }
        if action == "fail":
            return fail_connector_delivery(self.db_path, max_attempts=self.max_attempts, **kwargs)
        return defer_connector_delivery(self.db_path, **kwargs)

    def dispatch(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if method == "connector.poll":
            return self.poll(params)
        if method == "connector.ack":
            return self.ack(params)
        if method == "connector.fail":
            return self.fail(params)
        if method == "connector.defer":
            return self.defer(params)
        if method == "connector.reclaim":
            return self.reclaim(params)
        return _error("unknown_method", host_id=self.host_id)
