"""Local stdlib JSON request/response API for the Tendwire daemon."""

from __future__ import annotations

import errno
import json
import math
import os
import re
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .core.attention import attention_payload_from_snapshot
from .core.commands import CommandEnvelope, STATUS_INVALID_REQUEST, error_value
from .core.models import (
    Snapshot,
    public_json_dumps,
    sanitize_public_mapping,
)
from .core.turns import pending_payload_from_snapshot, turns_payload_from_snapshot
from .local_state import (
    EntryIdentity,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    enforce_bound_socket_permissions_at,
    inspect_owned_socket_at,
    open_resolved_parent,
    owned_socket_identity_at,
    pin_group_socket_for_client_at,
    pin_owned_socket_at,
    prepare_resolved_private_parent,
    proc_fd_path,
    resolve_socket_group,
    socket_bind_umask,
    unlink_verified_socket_at,
    validate_private_socket_parent_at,
    validate_socket_group_parent_at,
)


API_SCHEMA_VERSION = 1
MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_PUBLIC_REQUEST_ID_CHARS = 128
_SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS = 1.0
_SOCKET_STARTUP_LOCK_RETRY_SECONDS = 0.01
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_REQUEST_ID_FORBIDDEN_SEGMENTS = frozenset(
    {
        "telegram",
        "chat",
        "chats",
        "topic",
        "topics",
        "message",
        "messages",
        "thread",
        "threads",
        "token",
        "tokens",
        "auth",
        "authorization",
        "bearer",
        "cookie",
        "cookies",
        "credential",
        "credentials",
        "delivery",
        "deliveries",
        "route",
        "routes",
        "connector",
        "connectors",
        "herdres",
        "backend",
        "target",
        "targets",
        "terminal",
        "terminals",
        "pane",
        "panes",
        "tab",
        "tabs",
        "window",
        "windows",
        "tty",
        "pty",
        "pid",
        "pids",
        "process",
        "processes",
        "tmux",
        "screen",
        "agent_session",
        "session",
        "sessions",
        "private",
        "argv",
        "args",
        "env",
        "raw",
        "payload",
        "payloads",
        "control",
        "controls",
        "escape",
        "escapes",
        "stdin",
        "stderr",
        "stdout",
        "shell",
        "secret",
        "secrets",
        "password",
        "passwords",
        "api_key",
        "api_keys",
        "apikey",
    }
)
_REQUEST_ID_FORBIDDEN_COMPACT = frozenset(
    segment.replace("_", "") for segment in _REQUEST_ID_FORBIDDEN_SEGMENTS
)

REQUIRED_METHODS = frozenset(
    {
        "ping",
        "health.get",
        "snapshot.get",
        "attention.list",
        "turn.list",
        "turn.content.get",
        "pending.list",
        "command.submit",
        "connector.prepare",
        "connector.poll",
        "connector.ack",
        "connector.fail",
        "connector.defer",
        "connector.reclaim",
    }
)


class DaemonAPIError(Exception):
    """Base class for daemon transport and protocol failures."""


class DaemonUnavailable(DaemonAPIError):
    """Raised when the local daemon socket cannot be reached safely."""

    def __init__(
        self,
        message: str = "daemon socket is unavailable",
        *,
        code: LocalStateErrorCode | None = None,
        timed_out: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.timed_out = timed_out


class DaemonProtocolError(DaemonAPIError):
    """Raised when a daemon response cannot be parsed or trusted."""


def _request_id_has_forbidden_segment_sequence(normalized: str) -> bool:
    parts = tuple(part for part in normalized.split("_") if part)
    for forbidden in _REQUEST_ID_FORBIDDEN_SEGMENTS:
        forbidden_parts = tuple(part for part in forbidden.split("_") if part)
        if not forbidden_parts:
            continue
        part_count = len(forbidden_parts)
        if any(
            parts[index : index + part_count] == forbidden_parts
            for index in range(len(parts) - part_count + 1)
        ):
            return True
    return False


def _public_request_id(value: Any) -> str | int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if not isinstance(value, str):
        return None
    if not value or len(value) > MAX_PUBLIC_REQUEST_ID_CHARS:
        return None
    if not all(char.isascii() and (char.isalnum() or char in "._:-") for char in value):
        return None
    separated = _CAMEL_CASE_BOUNDARY_RE.sub("_", value)
    normalized = re.sub(r"[^a-z0-9]+", "_", separated.lower()).strip("_")
    compact = normalized.replace("_", "")
    if normalized in _REQUEST_ID_FORBIDDEN_SEGMENTS or compact in _REQUEST_ID_FORBIDDEN_COMPACT:
        return None
    if _request_id_has_forbidden_segment_sequence(normalized):
        return None
    if any(
        compact.endswith(forbidden)
        for forbidden in _REQUEST_ID_FORBIDDEN_COMPACT
        if len(forbidden) >= 5
    ):
        return None
    return value


def success_response(result: Mapping[str, Any] | None = None, *, request_id: Any = None) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": API_SCHEMA_VERSION,
        "ok": True,
        "status": "ok",
        "result": sanitize_public_mapping(dict(result or {})),
        "error": None,
    }
    public_id = _public_request_id(request_id)
    if public_id is not None:
        response["id"] = public_id
    return sanitize_public_mapping(response)


def error_response(
    code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
    request_id: Any = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": API_SCHEMA_VERSION,
        "ok": False,
        "status": "error",
        "result": None,
        "error": error_value(code, message, details=dict(details or {})),
    }
    public_id = _public_request_id(request_id)
    if public_id is not None:
        response["id"] = public_id
    return sanitize_public_mapping(response)


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    return sanitize_public_mapping(snapshot.to_dict())


def _command_result(value: Any) -> dict[str, Any]:
    if isinstance(value, CommandEnvelope):
        return value.to_dict()
    if hasattr(value, "to_dict"):
        data = value.to_dict()
        if isinstance(data, Mapping):
            return sanitize_public_mapping(dict(data))
    if isinstance(value, Mapping):
        return sanitize_public_mapping(dict(value))
    return CommandEnvelope.error(
        None,
        error_value(
            STATUS_INVALID_REQUEST,
            "command.submit returned an invalid result",
        ),
    ).to_dict()


class TendwireDaemonAPI:
    """Dispatch stable local daemon methods through injected public helpers."""

    def __init__(
        self,
        *,
        get_snapshot: Callable[[], Snapshot],
        get_health: Callable[[], Mapping[str, Any]],
        submit_command: Callable[[Mapping[str, Any]], Mapping[str, Any] | CommandEnvelope],
        get_attention: Callable[[], Mapping[str, Any]] | None = None,
        get_turns: Callable[..., Mapping[str, Any]] | None = None,
        get_turn_content: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        get_pending: Callable[[], Mapping[str, Any]] | None = None,
        connector_call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> None:
        self._get_snapshot = get_snapshot
        self._get_health = get_health
        self._submit_command = submit_command
        self._get_attention = get_attention
        self._get_turns = get_turns
        self._get_turn_content = get_turn_content
        self._get_pending = get_pending
        self._connector_call = connector_call

    def dispatch(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            return error_response("invalid_request", "request must be a JSON object")

        request_id = request.get("id")
        unknown = sorted(str(key) for key in request if str(key) not in {"id", "method", "params"})
        if unknown:
            return error_response(
                "invalid_request",
                "request contains unknown top-level fields",
                details={"field_count": len(unknown)},
                request_id=request_id,
            )

        method = request.get("method")
        if not isinstance(method, str) or not method:
            return error_response(
                "invalid_request",
                "method is required",
                details={"field": "method"},
                request_id=request_id,
            )
        if method not in REQUIRED_METHODS:
            return error_response(
                "unknown_method",
                "unknown method",
                details={"allowed": sorted(REQUIRED_METHODS)},
                request_id=request_id,
            )

        params = request.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, Mapping):
            return error_response(
                "invalid_params",
                "params must be a JSON object",
                details={"field": "params"},
                request_id=request_id,
            )

        try:
            if method == "ping":
                return success_response(
                    {"pong": True, "methods": sorted(REQUIRED_METHODS)},
                    request_id=request_id,
                )
            if method == "health.get":
                return success_response(self._get_health(), request_id=request_id)
            if method == "snapshot.get":
                return success_response(_snapshot_dict(self._get_snapshot()), request_id=request_id)
            if method == "attention.list":
                if self._get_attention is not None:
                    return success_response(self._get_attention(), request_id=request_id)
                return success_response(attention_payload_from_snapshot(self._get_snapshot()), request_id=request_id)
            if method == "turn.list":
                unknown_params = sorted(str(key) for key in params if str(key) != "schema_version")
                if unknown_params:
                    return error_response(
                        "invalid_params",
                        "turn.list contains unknown parameters",
                        details={"field_count": len(unknown_params)},
                        request_id=request_id,
                    )
                schema_version = params.get("schema_version", 1)
                if schema_version not in {1, 2} or isinstance(schema_version, bool):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn list schema version",
                        details={"supported_turn_schema_versions": [1, 2]},
                        request_id=request_id,
                    )
                if self._get_turns is not None:
                    turn_result = dict(self._get_turns(schema_version=schema_version))
                else:
                    snapshot = self._get_snapshot()
                    if schema_version == 1:
                        try:
                            turn_result = turns_payload_from_snapshot(snapshot)
                        except ValueError as exc:
                            if str(exc) != "upgrade_required":
                                raise
                            turn_result = {
                                "schema_version": 1,
                                "ok": False,
                                "status": "upgrade_required",
                                "required_turn_schema_version": 2,
                            }
                    else:
                        turn_result = turns_payload_from_snapshot(snapshot, schema_version=2)
                response = success_response(turn_result, request_id=request_id)
                _restore_turn_list_text(response, turn_result)
                return response
            if method == "turn.content.get":
                allowed_content_params = {
                    "schema_version",
                    "turn_id",
                    "content_revision",
                    "field",
                    "cursor",
                }
                unknown_content_params = sorted(
                    str(key) for key in params if str(key) not in allowed_content_params
                )
                if unknown_content_params:
                    return error_response(
                        "invalid_params",
                        "turn.content.get contains unknown parameters",
                        details={"field_count": len(unknown_content_params)},
                        request_id=request_id,
                    )
                content_schema = params.get("schema_version", 1)
                if content_schema != 1 or isinstance(content_schema, bool):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn content schema version",
                        details={"supported_content_schema_versions": [1]},
                        request_id=request_id,
                    )
                if not isinstance(params.get("turn_id"), str) or not params.get("turn_id"):
                    return error_response(
                        "invalid_params",
                        "turn_id is required",
                        details={"field": "turn_id"},
                        request_id=request_id,
                    )
                if not isinstance(params.get("content_revision"), str) or not params.get(
                    "content_revision"
                ):
                    return error_response(
                        "invalid_params",
                        "content_revision is required",
                        details={"field": "content_revision"},
                        request_id=request_id,
                    )
                if params.get("field") not in {"user_text", "assistant_final_text"}:
                    return error_response(
                        "invalid_params",
                        "field is invalid",
                        details={"field": "field"},
                        request_id=request_id,
                    )
                cursor = params.get("cursor")
                if cursor is not None and (not isinstance(cursor, str) or not cursor):
                    return error_response(
                        "invalid_params",
                        "cursor must be a non-empty string or null",
                        details={"field": "cursor"},
                        request_id=request_id,
                    )
                if self._get_turn_content is None:
                    return success_response(
                        {
                            "schema_version": 1,
                            "ok": False,
                            "status": "store_unavailable",
                            "error": {
                                "code": "store_unavailable",
                                "message": "content store is unavailable",
                            },
                        },
                        request_id=request_id,
                    )
                result = dict(self._get_turn_content(dict(params)))
                response = success_response(result, request_id=request_id)
                _restore_content_page_text(response, result)
                return response
            if method == "pending.list":
                if self._get_pending is not None:
                    return success_response(self._get_pending(), request_id=request_id)
                return success_response(pending_payload_from_snapshot(self._get_snapshot()), request_id=request_id)
            if method == "command.submit":
                return success_response(
                    _command_result(self._submit_command(dict(params))),
                    request_id=request_id,
                )
            if method.startswith("connector."):
                if self._connector_call is None:
                    return success_response(
                        {
                            "schema_version": 1,
                            "ok": False,
                            "status": "store_unavailable",
                            "error": {
                                "code": "store_unavailable",
                                "message": "store is unavailable",
                            },
                        },
                        request_id=request_id,
                    )
                connector_result = dict(self._connector_call(method, dict(params)))
                response = success_response(connector_result, request_id=request_id)
                if method == "connector.prepare":
                    _restore_plan_token(response, connector_result)
                return response
        except Exception as exc:  # noqa: BLE001
            return error_response(
                "internal_error",
                "daemon method failed",
                details={"type": type(exc).__name__},
                request_id=request_id,
            )

        return error_response(
            "unknown_method",
            "unknown method",
            request_id=request_id,
        )


def _restore_turn_list_text(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    """Restore trusted canonical inline fields/previews after generic sanitation."""
    if original_result.get("schema_version") not in {1, 2}:
        return
    original_turns = original_result.get("turns")
    result = response.get("result")
    if not isinstance(original_turns, list) or not isinstance(result, dict):
        return
    sanitized_turns = result.get("turns")
    if not isinstance(sanitized_turns, list):
        return
    by_id = {
        item.get("id"): item
        for item in sanitized_turns
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    for original in original_turns:
        if not isinstance(original, Mapping):
            continue
        target = by_id.get(original.get("id"))
        if not isinstance(target, dict):
            continue
        descriptors = (original.get("content") or {}).get("fields", {})
        for field in ("user_text", "assistant_final_text"):
            text = original.get(field)
            descriptor = descriptors.get(field) if isinstance(descriptors, Mapping) else None
            trusted_inline = original_result.get("schema_version") == 1 or (
                isinstance(descriptor, Mapping)
                and descriptor.get("availability") == "complete"
                and descriptor.get("inline") is True
            )
            if trusted_inline and isinstance(text, str):
                target[field] = text
        for preview_key in ("user_preview", "assistant_final_preview"):
            preview = original.get(preview_key)
            if isinstance(preview, str):
                target[preview_key] = preview


def _restore_content_page_text(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    """Restore an already-canonical page after generic public string bounding."""
    text = original_result.get("text")
    if not isinstance(text, str):
        return
    if original_result.get("schema_version") != 1:
        return
    if original_result.get("field") not in {"user_text", "assistant_final_text"}:
        return
    if original_result.get("availability") != "complete":
        return
    result = response.get("result")
    if isinstance(result, dict):
        result["text"] = text


def _restore_plan_token(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    result = response.get("result")
    if not isinstance(result, dict):
        return
    for key in ("plan_token", "failed_plan_token"):
        plan_token = original_result.get(key)
        if (
            isinstance(plan_token, str)
            and re.fullmatch(r"twplan1\.[A-Za-z0-9_-]+", plan_token) is not None
        ):
            result[key] = plan_token


def _serialized_response(response: Mapping[str, Any]) -> bytes:
    """Serialize one public response while preserving canonical content pages."""
    sanitized = sanitize_public_mapping(response)
    original_result = response.get("result")
    if isinstance(original_result, Mapping):
        _restore_turn_list_text(sanitized, original_result)
        _restore_content_page_text(sanitized, original_result)
        _restore_plan_token(sanitized, original_result)
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _ensure_unix_socket_supported() -> None:
    if not hasattr(socket, "AF_UNIX"):
        raise DaemonUnavailable("Unix domain sockets are not supported on this platform")


def _read_json_frame(conn: socket.socket, *, max_bytes: int = MAX_REQUEST_BYTES) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        if b"\n" in chunk:
            head, _tail = chunk.split(b"\n", 1)
            chunks.append(head)
            total += len(head)
            if total > max_bytes:
                raise DaemonProtocolError("request exceeds maximum frame size")
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise DaemonProtocolError("request exceeds maximum frame size")
    return b"".join(chunks)


def _local_state_unavailable(exc: LocalStateError) -> DaemonUnavailable:
    return DaemonUnavailable(
        "daemon socket local state is invalid",
        code=exc.code,
    )


@contextmanager
def _socket_startup_lock(parent_fd: int) -> Iterator[None]:
    """Serialize stale cleanup and socket publication within one parent."""

    try:
        import fcntl
    except ImportError:
        raise DaemonUnavailable(
            "secure daemon socket startup is unsupported",
            code=LocalStateErrorCode.UNSUPPORTED_PLATFORM,
        ) from None
    deadline = time.monotonic() + _SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError as exc:
            if exc.errno == errno.EINTR:
                continue
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise DaemonUnavailable(
                    "daemon socket startup lock failed",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                ) from None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DaemonUnavailable(
                    "daemon socket startup lock timed out",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                ) from None
            time.sleep(min(_SOCKET_STARTUP_LOCK_RETRY_SECONDS, remaining))
    try:
        yield
    finally:
        while True:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_UN)
                break
            except OSError as exc:
                if exc.errno != errno.EINTR:
                    break


def _cleanup_stale_socket(parent_fd: int, leaf: str, address: str) -> None:
    pinned = pin_owned_socket_at(parent_fd, leaf)
    if pinned is None:
        return
    pin_fd, identity = pinned
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.1)
        probe.connect(address)
    except OSError as exc:
        if exc.errno != errno.ECONNREFUSED:
            raise DaemonUnavailable("daemon socket state is ambiguous") from None
        unlink_verified_socket_at(parent_fd, leaf, identity)
    else:
        raise DaemonUnavailable("daemon socket is already active")
    finally:
        probe.close()
        try:
            os.close(pin_fd)
        except OSError:
            pass


class UnixSocketJSONServer:
    """Small sequential Unix-socket JSON server for local daemon requests."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        dispatcher: Callable[[Any], Mapping[str, Any]],
        *,
        stop_event: threading.Event | None = None,
        accept_timeout_seconds: float = 0.2,
        client_timeout_seconds: float = 1.0,
        max_request_bytes: int = MAX_REQUEST_BYTES,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        socket_group: str | None = None,
        prepare_parent: bool = False,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.dispatcher = dispatcher
        self.stop_event = stop_event or threading.Event()
        self.accept_timeout_seconds = accept_timeout_seconds
        self.client_timeout_seconds = client_timeout_seconds
        self.max_request_bytes = max_request_bytes
        self.max_response_bytes = max_response_bytes
        self.socket_group = socket_group
        self.prepare_parent = prepare_parent
        self._listener: socket.socket | None = None
        self._identity: EntryIdentity | None = None
        self._pin_fd: int | None = None
        self._parent_fd: int | None = None
        self._leaf: str | None = None
        self._lifecycle_lock = threading.Lock()

    @property
    def listening(self) -> bool:
        return self._listener is not None

    def start(self) -> None:
        if self._listener is not None:
            return
        if (
            self._identity is not None
            or self._pin_fd is not None
            or self._parent_fd is not None
            or self._leaf is not None
        ):
            raise DaemonUnavailable(
                "daemon socket cleanup is pending",
                code=LocalStateErrorCode.OPERATION_FAILED,
            )
        if self.stop_event.is_set():
            raise DaemonUnavailable("daemon socket server has already stopped")
        _ensure_unix_socket_supported()

        parent_fd: int | None = None
        listener: socket.socket | None = None
        try:
            resolved_group = resolve_socket_group(self.socket_group)
            if self.prepare_parent:
                if resolved_group is not None:
                    raise DaemonUnavailable(
                        "group sharing requires an explicit protected socket parent",
                        code=LocalStateErrorCode.INSECURE_SOCKET_PARENT,
                    )
                parent_fd, leaf, _result = prepare_resolved_private_parent(
                    self.socket_path
                )
            elif resolved_group is not None:
                parent_fd, leaf = open_resolved_parent(self.socket_path)
                validate_socket_group_parent_at(
                    parent_fd,
                    self.socket_group,
                )
            else:
                try:
                    parent_fd, leaf = open_resolved_parent(self.socket_path)
                except LocalStateError as exc:
                    if exc.code is not LocalStateErrorCode.MISSING_ENTRY:
                        raise
                    parent_fd, leaf = open_resolved_parent(
                        self.socket_path,
                        create_missing=True,
                    )
                validate_private_socket_parent_at(parent_fd)

            address = proc_fd_path(parent_fd, leaf)
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            identity: EntryIdentity | None = None
            pin_fd: int | None = None
            with _socket_startup_lock(parent_fd):
                try:
                    _cleanup_stale_socket(parent_fd, leaf, address)
                    with socket_bind_umask(self.socket_group):
                        listener.bind(address)
                    identity = owned_socket_identity_at(parent_fd, leaf)
                    if identity is None:
                        raise DaemonUnavailable("daemon socket could not be started")
                    pinned = pin_owned_socket_at(parent_fd, leaf)
                    if pinned is None:
                        raise DaemonUnavailable("daemon socket could not be started")
                    pin_fd, pinned_identity = pinned
                    if pinned_identity != identity:
                        raise DaemonUnavailable(
                            "daemon socket changed during startup",
                            code=LocalStateErrorCode.ENTRY_CHANGED,
                        )
                    enforce_bound_socket_permissions_at(
                        parent_fd,
                        leaf,
                        socket_group=self.socket_group,
                        expected=identity,
                    )
                    listener.listen()
                    listener.settimeout(self.accept_timeout_seconds)
                except Exception:
                    self._rollback_bound_socket(
                        listener,
                        parent_fd,
                        leaf,
                        identity,
                        pin_fd,
                    )
                    raise
            self._listener = listener
            self._identity = identity
            self._pin_fd = pin_fd
            self._parent_fd = parent_fd
            self._leaf = leaf
            listener = None
            parent_fd = None
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        except OSError:
            raise DaemonUnavailable("daemon socket could not be started") from None
        finally:
            if listener is not None:
                listener.close()
            if parent_fd is not None and self._parent_fd != parent_fd:
                try:
                    os.close(parent_fd)
                except OSError:
                    pass

    def _rollback_bound_socket(
        self,
        listener: socket.socket,
        parent_fd: int,
        leaf: str,
        identity: EntryIdentity | None,
        pin_fd: int | None,
    ) -> None:
        listener.close()
        if identity is None:
            if pin_fd is not None:
                try:
                    os.close(pin_fd)
                except OSError:
                    pass
            return
        try:
            unlink_verified_socket_at(parent_fd, leaf, identity)
        except LocalStateError as exc:
            if exc.code not in {
                LocalStateErrorCode.MISSING_ENTRY,
                LocalStateErrorCode.WRONG_TYPE,
                LocalStateErrorCode.WRONG_OWNER,
                LocalStateErrorCode.ENTRY_CHANGED,
            }:
                self._identity = identity
                self._pin_fd = pin_fd
                self._parent_fd = parent_fd
                self._leaf = leaf
                return
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass


    def serve_forever(self) -> None:
        self.start()
        try:
            while not self.stop_event.is_set():
                listener = self._listener
                if listener is None:
                    break
                try:
                    conn, _addr = listener.accept()
                except TimeoutError:
                    continue
                except socket.timeout:
                    continue
                except OSError:
                    if self.stop_event.is_set():
                        break
                    raise DaemonUnavailable("daemon socket request loop failed") from None
                self._handle_connection(conn)
        finally:
            self.close()

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(self.client_timeout_seconds)
            try:
                raw = _read_json_frame(conn, max_bytes=self.max_request_bytes)
                if not raw:
                    response = error_response("invalid_request", "empty request")
                else:
                    request = json.loads(raw.decode("utf-8"))
                    response = dict(self.dispatcher(request))
            except json.JSONDecodeError:
                response = error_response(
                    "invalid_request",
                    "invalid request JSON",
                    details={"field": "request"},
                )
            except Exception as exc:  # noqa: BLE001
                response = error_response(
                    "internal_error",
                    "daemon request failed",
                    details={"type": type(exc).__name__},
                )
            try:
                encoded = _serialized_response(response)
                if len(encoded) > self.max_response_bytes:
                    encoded = _serialized_response(
                        error_response(
                            "response_too_large",
                            "daemon response exceeds maximum frame size",
                            details={"max_response_bytes": self.max_response_bytes},
                        )
                    )
                conn.sendall(encoded + b"\n")
            except OSError:
                return

    def close(self) -> None:
        with self._lifecycle_lock:
            self._close_locked()

    def _close_locked(self) -> None:
        self.stop_event.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()
        identity = self._identity
        pin_fd = self._pin_fd
        parent_fd = self._parent_fd
        leaf = self._leaf
        if identity is not None:
            if parent_fd is None or leaf is None:
                raise DaemonUnavailable(
                    "daemon socket cleanup failed",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                )
            try:
                unlink_verified_socket_at(parent_fd, leaf, identity)
            except LocalStateError as exc:
                if exc.code not in {
                    LocalStateErrorCode.MISSING_ENTRY,
                    LocalStateErrorCode.WRONG_TYPE,
                    LocalStateErrorCode.WRONG_OWNER,
                    LocalStateErrorCode.ENTRY_CHANGED,
                }:
                    raise DaemonUnavailable(
                        "daemon socket cleanup failed",
                        code=exc.code,
                    ) from None
        self._identity = None
        self._pin_fd = None
        self._parent_fd = None
        self._leaf = None
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass
        if parent_fd is not None:
            try:
                os.close(parent_fd)
            except OSError:
                pass


@contextmanager
def _validated_client_socket(
    path: Path,
    socket_group: str | None,
) -> Iterator[tuple[int, str, EntryIdentity, int, str]]:
    parent_fd: int | None = None
    pin_fd: int | None = None
    try:
        try:
            parent_fd, leaf = open_resolved_parent(path, path_only=True)
            if socket_group is not None:
                pin_fd, identity, expected_peer_uid = (
                    pin_group_socket_for_client_at(
                        parent_fd,
                        leaf,
                        socket_group,
                    )
                )
            else:
                pinned = pin_owned_socket_at(parent_fd, leaf)
                if pinned is None:
                    raise DaemonUnavailable(
                        "daemon socket local state is invalid",
                        code=LocalStateErrorCode.MISSING_ENTRY,
                    )
                pin_fd, identity = pinned
                inspected = inspect_owned_socket_at(parent_fd, leaf)
                current_identity = owned_socket_identity_at(parent_fd, leaf)
                if (
                    current_identity != identity
                    or inspected.state is PermissionState.ABSENT
                    or inspected.mode != 0o600
                ):
                    raise DaemonUnavailable(
                        "daemon socket local state is invalid",
                        code=(
                            LocalStateErrorCode.ENTRY_CHANGED
                            if current_identity != identity
                            else LocalStateErrorCode.INSECURE_MODE
                        ),
                    )
                expected_peer_uid = os.geteuid()
            address = proc_fd_path(parent_fd, leaf)
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        yield parent_fd, leaf, identity, expected_peer_uid, address
    finally:
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass
        if parent_fd is not None:
            try:
                os.close(parent_fd)
            except OSError:
                pass


def _recheck_connected_socket(
    parent_fd: int,
    leaf: str,
    expected: EntryIdentity,
    socket_group: str | None,
) -> None:
    recheck_fd: int | None = None
    try:
        try:
            if socket_group is None:
                current = owned_socket_identity_at(parent_fd, leaf)
            else:
                recheck_fd, current, _owner_uid = pin_group_socket_for_client_at(
                    parent_fd,
                    leaf,
                    socket_group,
                )
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        if current != expected:
            raise DaemonUnavailable(
                "daemon socket local state is invalid",
                code=LocalStateErrorCode.ENTRY_CHANGED,
            )
    finally:
        if recheck_fd is not None:
            try:
                os.close(recheck_fd)
            except OSError:
                pass


def _validate_connected_peer(conn: socket.socket, expected_uid: int) -> None:
    if not hasattr(socket, "SO_PEERCRED"):
        raise DaemonUnavailable(
            "daemon peer validation is unsupported",
            code=LocalStateErrorCode.UNSUPPORTED_PLATFORM,
        )
    credentials = struct.Struct("3i")
    try:
        raw = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, credentials.size)
        _pid, peer_uid, _gid = credentials.unpack(raw)
    except (OSError, struct.error):
        raise DaemonUnavailable("daemon peer validation failed") from None
    if peer_uid != expected_uid:
        raise DaemonUnavailable(
            "daemon peer ownership is invalid",
            code=LocalStateErrorCode.WRONG_OWNER,
        )


class DaemonAPIClient:
    """Blocking one-request client for the local daemon JSON socket."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        *,
        timeout_seconds: float = 1.0,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        socket_group: str | None = None,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.socket_group = socket_group

    def request(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        _ensure_unix_socket_supported()
        payload = {"method": method, "params": dict(params or {})}
        raw_payload = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        with _validated_client_socket(
            self.socket_path,
            self.socket_group,
        ) as (parent_fd, leaf, identity, expected_peer_uid, address):
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                conn.settimeout(self.timeout_seconds)
                conn.connect(address)
                _recheck_connected_socket(
                    parent_fd,
                    leaf,
                    identity,
                    self.socket_group,
                )
                _validate_connected_peer(conn, expected_peer_uid)
            except (TimeoutError, socket.timeout):
                conn.close()
                raise DaemonUnavailable(
                    "daemon socket request timed out",
                    timed_out=True,
                ) from None
            except DaemonUnavailable:
                conn.close()
                raise
            except OSError:
                conn.close()
                raise DaemonUnavailable("daemon socket is unavailable") from None
            try:
                conn.sendall(raw_payload)
                raw_response = _read_json_frame(
                    conn,
                    max_bytes=self.max_response_bytes,
                )
            except (TimeoutError, socket.timeout):
                raise DaemonUnavailable(
                    "daemon socket request timed out",
                    timed_out=True,
                ) from None
            except OSError:
                raise DaemonProtocolError(
                    "daemon request outcome is uncertain"
                ) from None
            finally:
                conn.close()

        if not raw_response:
            raise DaemonProtocolError("empty daemon response")
        try:
            response = json.loads(raw_response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DaemonProtocolError("invalid daemon response JSON") from None
        if not isinstance(response, dict):
            raise DaemonProtocolError("daemon response must be a JSON object")
        return response
