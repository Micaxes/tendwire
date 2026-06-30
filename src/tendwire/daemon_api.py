"""Local stdlib JSON request/response API for the Tendwire daemon."""

from __future__ import annotations

import json
import os
import socket
import stat
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from .core.attention import attention_payload_from_snapshot
from .core.commands import CommandEnvelope, STATUS_INVALID_REQUEST, error_value
from .core.models import (
    Snapshot,
    sanitize_forbidden_fields,
    stable_json_dumps,
)
from .core.turns import pending_payload_from_snapshot, turns_payload_from_snapshot


API_SCHEMA_VERSION = 1
MAX_REQUEST_BYTES = 1024 * 1024

REQUIRED_METHODS = frozenset(
    {
        "ping",
        "health.get",
        "snapshot.get",
        "attention.list",
        "turn.list",
        "pending.list",
        "command.submit",
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
    """Raised when the local daemon socket cannot be reached."""


class DaemonProtocolError(DaemonAPIError):
    """Raised when a daemon response cannot be parsed or trusted."""


def success_response(result: Mapping[str, Any] | None = None, *, request_id: Any = None) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": API_SCHEMA_VERSION,
        "ok": True,
        "status": "ok",
        "result": sanitize_forbidden_fields(dict(result or {})),
        "error": None,
    }
    if request_id is not None:
        response["id"] = sanitize_forbidden_fields(request_id)
    return response


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
    if request_id is not None:
        response["id"] = sanitize_forbidden_fields(request_id)
    return response


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    return sanitize_forbidden_fields(snapshot.to_dict())


def _command_result(value: Any) -> dict[str, Any]:
    if isinstance(value, CommandEnvelope):
        return value.to_dict()
    if hasattr(value, "to_dict"):
        data = value.to_dict()
        if isinstance(data, Mapping):
            return sanitize_forbidden_fields(dict(data))
    if isinstance(value, Mapping):
        return sanitize_forbidden_fields(dict(value))
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
        connector_call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> None:
        self._get_snapshot = get_snapshot
        self._get_health = get_health
        self._submit_command = submit_command
        self._get_attention = get_attention
        self._connector_call = connector_call

    def dispatch(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            return error_response("invalid_request", "request must be a JSON object")

        request_id = request.get("id")
        unknown = sorted(str(key) for key in request if str(key) not in {"id", "method", "params"})
        if unknown:
            return error_response(
                "invalid_request",
                f"request contains unknown top-level fields: {unknown}",
                details={"fields": unknown},
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
                f"unknown method {method!r}",
                details={"method": method, "allowed": sorted(REQUIRED_METHODS)},
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
                return success_response(turns_payload_from_snapshot(self._get_snapshot()), request_id=request_id)
            if method == "pending.list":
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
                return success_response(
                    self._connector_call(method, dict(params)),
                    request_id=request_id,
                )
        except Exception as exc:  # noqa: BLE001
            return error_response(
                "internal_error",
                "daemon method failed",
                details={"type": type(exc).__name__},
                request_id=request_id,
            )

        return error_response(
            "unknown_method",
            f"unknown method {method!r}",
            request_id=request_id,
        )


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


def _socket_identity(path: Path) -> tuple[int, int] | None:
    try:
        current = path.stat()
    except FileNotFoundError:
        return None
    if not stat.S_ISSOCK(current.st_mode):
        return None
    return (int(current.st_dev), int(current.st_ino))


def _cleanup_stale_socket(path: Path) -> None:
    if not path.exists():
        return
    mode = path.stat().st_mode
    if not stat.S_ISSOCK(mode):
        raise DaemonUnavailable(f"socket path exists and is not a socket: {path}")

    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.1)
        probe.connect(str(path))
    except OSError:
        path.unlink()
    else:
        raise DaemonUnavailable(f"daemon already appears to be listening on {path}")
    finally:
        probe.close()


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
    ) -> None:
        self.socket_path = Path(socket_path)
        self.dispatcher = dispatcher
        self.stop_event = stop_event or threading.Event()
        self.accept_timeout_seconds = accept_timeout_seconds
        self.client_timeout_seconds = client_timeout_seconds
        self.max_request_bytes = max_request_bytes
        self._listener: socket.socket | None = None
        self._identity: tuple[int, int] | None = None

    @property
    def listening(self) -> bool:
        return self._listener is not None

    def start(self) -> None:
        if self._listener is not None:
            return
        _ensure_unix_socket_supported()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        _cleanup_stale_socket(self.socket_path)

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.socket_path))
            listener.listen()
            listener.settimeout(self.accept_timeout_seconds)
            self._listener = listener
            self._identity = _socket_identity(self.socket_path)
        except Exception:
            listener.close()
            raise

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
                    raise
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
            except json.JSONDecodeError as exc:
                response = error_response(
                    "invalid_request",
                    f"invalid JSON: {exc}",
                    details={"field": "request"},
                )
            except Exception as exc:  # noqa: BLE001
                response = error_response(
                    "internal_error",
                    "daemon request failed",
                    details={"type": type(exc).__name__},
                )
            conn.sendall(stable_json_dumps(response).encode("utf-8") + b"\n")

    def close(self) -> None:
        self.stop_event.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()
        current_identity = _socket_identity(self.socket_path)
        if current_identity is not None and current_identity == self._identity:
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
        self._identity = None


class DaemonAPIClient:
    """Blocking one-request client for the local daemon JSON socket."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        *,
        timeout_seconds: float = 1.0,
        max_response_bytes: int = MAX_REQUEST_BYTES,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes

    def request(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        _ensure_unix_socket_supported()
        payload = {"method": method, "params": dict(params or {})}
        raw_payload = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            conn.settimeout(self.timeout_seconds)
            conn.connect(str(self.socket_path))
            conn.sendall(raw_payload)
            raw_response = _read_json_frame(conn, max_bytes=self.max_response_bytes)
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError, socket.timeout, OSError) as exc:
            raise DaemonUnavailable(str(exc)) from exc
        finally:
            conn.close()

        if not raw_response:
            raise DaemonProtocolError("empty daemon response")
        try:
            response = json.loads(raw_response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DaemonProtocolError("invalid daemon response JSON") from exc
        if not isinstance(response, dict):
            raise DaemonProtocolError("daemon response must be a JSON object")
        return response
