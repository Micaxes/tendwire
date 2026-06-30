"""Long-running Tendwire daemon lifecycle skeleton."""

from __future__ import annotations

import json
import signal
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Config
from .core.commands import CommandEnvelope
from .core.models import Snapshot, utc_timestamp
from .daemon_api import TendwireDaemonAPI, UnixSocketJSONServer


def default_socket_path(config: Config) -> Path:
    """Return the daemon socket path for this config."""
    if config.socket_path is not None:
        return Path(config.socket_path)
    return Path(config.data_dir) / "tendwire.sock"


def _default_init_store(db_path: Path) -> None:
    from .store.sqlite import init_store

    init_store(db_path)


def _default_observe_initial_snapshot(config: Config) -> Snapshot:
    from .cli import observe_public_snapshot

    return observe_public_snapshot(config, store_snapshot=True)


def _default_submit_command(config: Config, payload: str) -> CommandEnvelope:
    from .command_submission import submit_command

    return submit_command(config, payload)


@dataclass(frozen=True)
class DaemonHooks:
    """Dependency injection points for deterministic daemon tests."""

    init_store: Callable[[Path], None] = _default_init_store
    observe_initial_snapshot: Callable[[Config], Snapshot] = _default_observe_initial_snapshot
    submit_command: Callable[[Config, str], CommandEnvelope | Mapping[str, Any]] = _default_submit_command
    event_backend_factory: Callable[[Config, threading.Event], Any] | None = None


class TendwireDaemon:
    """Owns store initialization, initial observation, API dispatch, and shutdown."""

    def __init__(
        self,
        config: Config,
        *,
        socket_path: str | Path | None = None,
        hooks: DaemonHooks | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.config = config
        self.socket_path = Path(socket_path) if socket_path is not None else default_socket_path(config)
        self.hooks = hooks or DaemonHooks()
        self.stop_event = stop_event or threading.Event()
        self.started_at = utc_timestamp()
        self._snapshot: Snapshot | None = None
        self._server: UnixSocketJSONServer | None = None
        self._event_backend: Any | None = None

    @property
    def snapshot(self) -> Snapshot | None:
        return self._snapshot

    @property
    def server(self) -> UnixSocketJSONServer | None:
        return self._server

    def start(self) -> None:
        if self.config.db_path is None:
            raise RuntimeError("daemon requires a sqlite db path")
        self.hooks.init_store(Path(self.config.db_path))
        if self.config.herdr_backend == "socket":
            self._snapshot = self._start_socket_event_backend()
        else:
            self._snapshot = self.hooks.observe_initial_snapshot(self.config)

        api = TendwireDaemonAPI(
            get_snapshot=self.get_snapshot,
            get_health=self.get_health,
            submit_command=self.submit_command,
            get_attention=self.get_attention,
            connector_call=self.connector_call,
        )
        self._server = UnixSocketJSONServer(
            self.socket_path,
            api.dispatch,
            stop_event=self.stop_event,
        )
        self._server.start()

    def serve_forever(self) -> None:
        if self._server is None:
            self.start()
        server = self._server
        if server is None:
            raise RuntimeError("daemon server did not start")
        server.serve_forever()

    def stop(self) -> None:
        self.stop_event.set()
        if self._event_backend is not None:
            self._event_backend.stop()
        if self._server is not None:
            self._server.close()

    def _start_socket_event_backend(self) -> Snapshot:
        if self.hooks.event_backend_factory is None:
            from .backends.herdr_events import HerdrEventBackend

            backend = HerdrEventBackend(self.config, stop_event=self.stop_event)
        else:
            backend = self.hooks.event_backend_factory(self.config, self.stop_event)
        self._event_backend = backend
        backend.start(wait_for_reconcile=True)
        from .store.sqlite import latest_snapshot, save_snapshot

        snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
        if snapshot is not None:
            return snapshot
        from .backends.herdr_cli import herdr_backend_health
        from .core.projector import project_from_observations

        backend_health = (
            backend.health.to_backend_health()
            if hasattr(backend, "health")
            else herdr_backend_health("unknown")
        )
        snapshot = project_from_observations(
            self.config,
            backend_health=[backend_health],
        )
        save_snapshot(Path(self.config.db_path), snapshot)
        return snapshot

    def get_snapshot(self) -> Snapshot:
        if self.config.db_path is not None:
            from .store.sqlite import latest_snapshot

            snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
            if snapshot is not None:
                self._snapshot = snapshot
                return snapshot
        if self._snapshot is not None:
            return self._snapshot
        raise RuntimeError("daemon has no initial snapshot")

    def get_health(self) -> dict[str, Any]:
        snapshot = self.get_snapshot()
        store_status = "healthy"
        if self.config.db_path is None or not Path(self.config.db_path).exists():
            store_status = "unavailable"
        return {
            "schema_version": 1,
            "status": "ok" if store_status == "healthy" else "degraded",
            "host_id": self.config.host_id,
            "daemon": {
                "status": "healthy",
                "started_at": self.started_at,
            },
            "store": {
                "status": store_status,
            },
            "snapshot": {
                "updated_at": snapshot.updated_at,
                "content_fingerprint": snapshot.content_fingerprint,
            },
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        }

    def get_attention(self) -> Mapping[str, Any]:
        if self.config.db_path is not None:
            from .store.sqlite import attention_payload_from_store

            payload = attention_payload_from_store(
                Path(self.config.db_path),
                self.config.host_id,
            )
            if payload is not None:
                return payload
        from .core.attention import attention_payload_from_snapshot

        return attention_payload_from_snapshot(self.get_snapshot())

    def connector_call(self, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": self.config.host_id,
                "name": str(params.get("name") or ""),
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .connectors import ConnectorOutboxAPI

        return ConnectorOutboxAPI(Path(self.config.db_path), self.config.host_id).dispatch(method, params)

    def submit_command(self, params: Mapping[str, Any]) -> CommandEnvelope | Mapping[str, Any]:
        # Preserve the submitted keys exactly so the existing command parser can
        # reject private/connector fields instead of receiving sanitized input.
        payload = json.dumps(
            dict(params),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return self.hooks.submit_command(self.config, payload)


def run_daemon(
    config: Config,
    *,
    socket_path: str | Path | None = None,
    hooks: DaemonHooks | None = None,
    install_signal_handlers: bool = True,
) -> int:
    """Run the daemon until SIGINT, SIGTERM, or an injected stop event."""
    daemon = TendwireDaemon(config, socket_path=socket_path, hooks=hooks)
    previous_handlers: dict[int, Any] = {}

    def _handle_stop(_signum: int, _frame: Any) -> None:
        daemon.stop()

    if install_signal_handlers:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle_stop)

    try:
        daemon.start()
        daemon.serve_forever()
        return 0
    except KeyboardInterrupt:
        daemon.stop()
        return 0
    finally:
        daemon.stop()
        if install_signal_handlers:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
