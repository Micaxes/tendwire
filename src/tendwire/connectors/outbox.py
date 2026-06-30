"""Neutral connector outbox API above the SQLite store.

This module is intentionally Tendwire-only. It exposes opaque refs and sanitized
payloads without importing core runtime connectors or backend-specific concepts.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..core.models import sanitize_forbidden_fields, stable_json_dumps
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


def _clean_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    clean = sanitize_forbidden_fields(dict(value))
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


def _encode_ref(
    *,
    host_id: str,
    name: str,
    outbox_id: int,
    delivery_id: int,
    attempt: int,
    lease_token: str,
) -> str:
    raw = stable_json_dumps(
        {
            "v": 1,
            "h": host_id,
            "n": name,
            "o": int(outbox_id),
            "d": int(delivery_id),
            "a": int(attempt),
            "l": lease_token,
        }
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"twref1.{encoded}"


def _decode_ref(ref: Any) -> dict[str, Any] | None:
    raw_ref = _text(ref)
    if not raw_ref.startswith("twref1."):
        return None
    encoded = raw_ref.split(".", 1)[1]
    padding = "=" * ((4 - len(encoded) % 4) % 4)
    try:
        parsed = json.loads(base64.urlsafe_b64decode((encoded + padding).encode("ascii")).decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, Mapping) or parsed.get("v") != 1:
        return None
    try:
        return {
            "host_id": str(parsed["h"]),
            "name": str(parsed["n"]),
            "outbox_id": int(parsed["o"]),
            "delivery_id": int(parsed["d"]),
            "attempt": int(parsed["a"]),
            "lease_token": str(parsed["l"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


class ConnectorOutboxAPI:
    """Public-neutral facade for connector.poll/ack/fail/defer."""

    def __init__(self, db_path: str | Path | None, host_id: str) -> None:
        self.db_path = Path(db_path) if db_path is not None else None
        self.host_id = str(host_id)

    def _require_store(self, name: str = "") -> dict[str, Any] | None:
        if self.db_path is None:
            return _error("store_unavailable", host_id=self.host_id, name=name)
        return None

    def poll(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        name = _text(data.get("name"))
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
            lease_seconds=_int(data.get("lease_seconds"), 60, minimum=1, maximum=86400),
        )
        items: list[dict[str, Any]] = []
        for item in store_result.get("items", []):
            if not isinstance(item, Mapping):
                continue
            ref = _encode_ref(
                host_id=self.host_id,
                name=name,
                outbox_id=int(item["outbox_id"]),
                delivery_id=int(item["delivery_id"]),
                attempt=int(item["attempt"]),
                lease_token=str(item["lease_token"]),
            )
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
        name = _text(data.get("name"))
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return reclaim_expired_connector_leases(self.db_path, self.host_id, name)

    def _mutation_parts(self, params: Mapping[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any] | None]:
        data = dict(params or {})
        name = _text(data.get("name"))
        ref = _text(data.get("ref"))
        if not name or not ref:
            return data, None
        decoded = _decode_ref(ref)
        if decoded is None:
            return data, None
        if decoded["host_id"] != self.host_id or decoded["name"] != name:
            return data, None
        return data, decoded

    def ack(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data, decoded = self._mutation_parts(params)
        name = _text(data.get("name"))
        ref = _text(data.get("ref"))
        if decoded is None:
            return _error("invalid_ref", host_id=self.host_id, name=name, ref=ref or None)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return ack_connector_delivery(
            self.db_path,
            host_id=self.host_id,
            name=name,
            ref=ref,
            outbox_id=int(decoded["outbox_id"]),
            delivery_id=int(decoded["delivery_id"]),
            attempt=int(decoded["attempt"]),
            lease_token=str(decoded["lease_token"]),
            response=_clean_mapping(data.get("response")),
        )

    def fail(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("fail", params)

    def defer(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("defer", params)

    def _schedule(self, action: str, params: Mapping[str, Any] | None) -> dict[str, Any]:
        data, decoded = self._mutation_parts(params)
        name = _text(data.get("name"))
        ref = _text(data.get("ref"))
        if decoded is None:
            return _error("invalid_ref", host_id=self.host_id, name=name, ref=ref or None)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        kwargs = {
            "host_id": self.host_id,
            "name": name,
            "ref": ref,
            "outbox_id": int(decoded["outbox_id"]),
            "delivery_id": int(decoded["delivery_id"]),
            "attempt": int(decoded["attempt"]),
            "lease_token": str(decoded["lease_token"]),
            "reason": _text(data.get("reason")),
            "response": _clean_mapping(data.get("response")),
            "available_at": _text(data.get("available_at")) or None,
            "delay_seconds": _int(data.get("delay_seconds"), 60, minimum=0, maximum=31536000)
            if data.get("delay_seconds") is not None
            else None,
        }
        if action == "fail":
            return fail_connector_delivery(self.db_path, **kwargs)
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
