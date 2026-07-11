"""Neutral connector outbox API above the SQLite store.

This module is intentionally Tendwire-only. It exposes opaque refs and sanitized
payloads without importing core runtime connectors or backend-specific concepts.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..core.models import sanitize_public_mapping, sanitize_public_value
from ..store.sqlite import (
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    poll_connector_outbox,
    prepare_connector_plan_begin,
    prepare_connector_plan_commit,
    prepare_connector_plan_recover,
    prepare_connector_plan_part,
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


_CONNECTOR_REF_PREFIX = "twref1."
_CONNECTOR_REF_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_CONNECTOR_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
_PLAN_TOKEN_PREFIX = "twplan1."
_REVISION_PREFIX = "twrev1."
_PREPARE_NAME = "turn-final"
_PREPARE_MAX_PARTS = 10_000
_PREPARE_MAX_SPANS = 64
_PREPARE_FIELDS = frozenset({"user_text", "assistant_final_text"})
_PREPARE_VERSION_CHARS = _CONNECTOR_NAME_CHARS
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


def _compact_public_text(value: str) -> str:
    return "".join(char for char in value.lower() if char.isalnum())


def _contains_forbidden_public_text(value: str) -> bool:
    lowered = value.lower()
    compact = _compact_public_text(lowered)
    return any(token in lowered or token.replace("_", "") in compact for token in _FORBIDDEN_PUBLIC_TEXT)


def _opaque_token(value: Any, prefix: str) -> str:
    token = _text(value)
    if not token.startswith(prefix):
        return ""
    body = token[len(prefix) :]
    if not body or any(char not in _CONNECTOR_REF_CHARS for char in body):
        return ""
    return token


def _plan_token(value: Any) -> str:
    return _opaque_token(value, _PLAN_TOKEN_PREFIX)


def _revision(value: Any) -> str:
    return _opaque_token(value, _REVISION_PREFIX)


def _restore_plan_tokens(clean: dict[str, Any], original: Mapping[str, Any]) -> dict[str, Any]:
    for key in (
        "plan_token",
        "replaces_plan_token",
        "failed_plan_token",
    ):
        if key not in original:
            continue
        value = original.get(key)
        if value is None:
            clean[key] = None
            continue
        token = _plan_token(value)
        if token:
            clean[key] = token
    return clean


def _clean_mapping(value: Any) -> dict[str, Any]:
    original = dict(value) if isinstance(value, Mapping) else {}
    return _restore_plan_tokens(
        sanitize_public_mapping(original, backend_neutral=True),
        original,
    )


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
    return sanitize_public_value(payload)


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

def _request_id(value: Any) -> str:
    request_id = _text(value)
    if not request_id or len(request_id) > 128:
        return ""
    if any(char not in _CONNECTOR_NAME_CHARS for char in request_id):
        return ""
    if _contains_forbidden_public_text(request_id):
        return ""
    return request_id


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

    def prepare(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if data.get("schema_version") != 1 or isinstance(
            data.get("schema_version"), bool
        ):
            return _error("invalid_params", host_id=self.host_id)
        action = data.get("action")
        name = _name(data.get("name"))
        if name != _PREPARE_NAME or action not in {"begin", "part", "commit", "recover"}:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None

        if action == "begin":
            if set(data) != {
                "schema_version",
                "action",
                "name",
                "turn_id",
                "content_revision",
                "presentation_version",
                "part_count",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            turn_id = _text(data.get("turn_id"))
            revision = _revision(data.get("content_revision"))
            version = _text(data.get("presentation_version"))
            part_count = data.get("part_count")
            if (
                not turn_id.startswith("turn-")
                or len(turn_id) > 128
                or any(char not in _CONNECTOR_NAME_CHARS for char in turn_id)
                or not revision
                or not version
                or len(version) > 128
                or any(char not in _PREPARE_VERSION_CHARS for char in version)
                or _contains_forbidden_public_text(version)
                or isinstance(part_count, bool)
                or not isinstance(part_count, int)
                or part_count < 1
                or part_count > _PREPARE_MAX_PARTS
            ):
                return _error("invalid_params", host_id=self.host_id, name=name)
            return prepare_connector_plan_begin(
                self.db_path,
                self.host_id,
                name=name,
                turn_id=turn_id,
                content_revision=revision,
                presentation_version=version,
                part_count=part_count,
            )

        if action == "recover":
            if set(data) != {
                "schema_version",
                "action",
                "name",
                "failed_plan_token",
                "request_id",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            failed_plan_token = _plan_token(data.get("failed_plan_token"))
            request_id = _request_id(data.get("request_id"))
            if not failed_plan_token or not request_id:
                return _error("invalid_params", host_id=self.host_id, name=name)
            return prepare_connector_plan_recover(
                self.db_path,
                self.host_id,
                name=name,
                failed_plan_token=failed_plan_token,
                request_id=request_id,
            )

        token = _plan_token(data.get("plan_token"))
        if not token:
            return _error("invalid_params", host_id=self.host_id, name=name)
        if action == "commit":
            if set(data) != {
                "schema_version",
                "action",
                "name",
                "plan_token",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            return prepare_connector_plan_commit(
                self.db_path,
                self.host_id,
                name=name,
                plan_token=token,
            )

        if set(data) != {
            "schema_version",
            "action",
            "name",
            "plan_token",
            "ordinal",
            "spans",
        }:
            return _error("invalid_params", host_id=self.host_id, name=name)
        ordinal = data.get("ordinal")
        raw_spans = data.get("spans")
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or ordinal < 0
            or not isinstance(raw_spans, list)
            or not raw_spans
            or len(raw_spans) > _PREPARE_MAX_SPANS
        ):
            return _error("invalid_params", host_id=self.host_id, name=name)
        spans: list[dict[str, Any]] = []
        for raw_span in raw_spans:
            if not isinstance(raw_span, Mapping) or set(raw_span) != {
                "field",
                "start_char",
                "end_char",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            field = raw_span.get("field")
            start = raw_span.get("start_char")
            end = raw_span.get("end_char")
            if (
                field not in _PREPARE_FIELDS
                or isinstance(start, bool)
                or isinstance(end, bool)
                or not isinstance(start, int)
                or not isinstance(end, int)
                or start < 0
                or end <= start
            ):
                return _error("invalid_params", host_id=self.host_id, name=name)
            spans.append(
                {
                    "field": str(field),
                    "start_char": start,
                    "end_char": end,
                }
            )
        return prepare_connector_plan_part(
            self.db_path,
            self.host_id,
            name=name,
            plan_token=token,
            ordinal=ordinal,
            spans=spans,
        )


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
            clean_payload = _clean_mapping(item.get("payload"))
            clean_item = sanitize_public_value(
                {
                    "ref": ref,
                    "key": str(item.get("key") or ""),
                    "attempt": int(item.get("attempt") or 0),
                    "leased_until": str(item.get("leased_until") or ""),
                    "available_at": str(item.get("available_at") or ""),
                    "payload": clean_payload,
                }
            )
            if isinstance(clean_item, dict):
                sanitized_payload = clean_item.get("payload")
                if isinstance(sanitized_payload, dict):
                    _restore_plan_tokens(sanitized_payload, clean_payload)
                items.append(clean_item)
        return {
            "schema_version": 1,
            "ok": bool(store_result.get("ok", False)),
            "status": str(store_result.get("status") or "ok"),
            "host_id": self.host_id,
            "name": name,
            "items": items,
        }

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
        if method == "connector.prepare":
            return self.prepare(params)
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
