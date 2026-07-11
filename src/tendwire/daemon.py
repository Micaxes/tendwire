"""Long-running Tendwire daemon lifecycle skeleton."""

from __future__ import annotations

import json
import signal
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config
from .core.commands import CommandEnvelope
from .core.models import Snapshot, sanitize_public_mapping, utc_timestamp
from .daemon_api import TendwireDaemonAPI, UnixSocketJSONServer
from .local_state import repair_config_state


def _valid_observation_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return value if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


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
        self._prepare_socket_parent = socket_path is None and config.socket_path is None
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
        if self._server is not None and self._server.listening:
            return
        if self.stop_event.is_set():
            raise RuntimeError("daemon cannot start after shutdown")
        if self.config.db_path is None:
            raise RuntimeError("daemon requires a sqlite db path")
        repair_config_state(
            self.config.data_dir,
            self.config.db_path,
            private_files=(
                self.config.installation_key_path,
                self.config.installation_key_marker_path,
                self.config.installation_key_sentinel_path,
            ),
        )

        api = TendwireDaemonAPI(
            get_snapshot=self.get_snapshot,
            get_health=self.get_health,
            submit_command=self.submit_command,
            get_attention=self.get_attention,
            get_turns=self.get_turns,
            get_turn_content=self.get_turn_content,
            get_pending=self.get_pending,
            connector_call=self.connector_call,
        )
        server = UnixSocketJSONServer(
            self.socket_path,
            api.dispatch,
            stop_event=self.stop_event,
            socket_group=self.config.socket_group,
            prepare_parent=self._prepare_socket_parent,
        )
        self._server = server
        try:
            server.start()
        except Exception:
            try:
                server.close()
            except Exception:
                pass
            else:
                self._server = None
            raise
        try:
            self.hooks.init_store(Path(self.config.db_path))
            if self.config.herdr_backend == "socket":
                self._snapshot = self._start_socket_event_backend()
            else:
                self._snapshot = self.hooks.observe_initial_snapshot(self.config)
        except Exception:
            backend = self._event_backend
            self._event_backend = None
            if backend is not None:
                try:
                    backend.stop()
                except Exception:
                    pass
            try:
                server.close()
            except Exception:
                pass
            else:
                self._server = None
            self._snapshot = None
            raise

    def serve_forever(self) -> None:
        if self._server is None:
            self.start()
        server = self._server
        if server is None:
            raise RuntimeError("daemon server did not start")
        server.serve_forever()

    def stop(self) -> None:
        self.stop_event.set()
        server = self._server
        backend = self._event_backend
        self._event_backend = None
        try:
            if server is not None:
                server.close()
        finally:
            if backend is not None:
                backend.stop()

    def _start_socket_event_backend(self) -> Snapshot:
        if self.hooks.event_backend_factory is None:
            from .backends.herdr_events import HerdrEventBackend

            backend = HerdrEventBackend(self.config, stop_event=self.stop_event)
        else:
            backend = self.hooks.event_backend_factory(self.config, self.stop_event)
        self._event_backend = backend
        backend.start(wait_for_reconcile=True)
        from .store.sqlite import SnapshotObservationContext, latest_snapshot, save_snapshot

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
        save_snapshot(
            Path(self.config.db_path),
            snapshot,
            observation=SnapshotObservationContext(
                authority="none",
                observed_at=_valid_observation_timestamp(backend_health.observed_at),
            ),
        )
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
        store_payload: dict[str, Any] = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": self.config.host_id,
            "counts": {},
            "outbox": {"pending": 0, "leased": 0, "terminal": 0, "by_status": {}},
        }
        if self.config.db_path is not None:
            from .store.sqlite import store_status

            store_payload = store_status(Path(self.config.db_path), self.config.host_id)
        store_ok = bool(store_payload.get("ok"))
        backend_runtime: dict[str, Any] = {}
        if self._event_backend is not None and hasattr(self._event_backend, "operational_status"):
            status_value = getattr(self._event_backend, "operational_status")
            if isinstance(status_value, Mapping):
                backend_runtime = dict(status_value)
        last_event_at = backend_runtime.get("last_event_at") or store_payload.get("last_event_at")
        last_snapshot_at = backend_runtime.get("last_snapshot_at") or store_payload.get("last_snapshot_at") or snapshot.updated_at
        payload = {
            "schema_version": 1,
            "status": "ok" if store_ok else "degraded",
            "host_id": self.config.host_id,
            "daemon": {
                "status": "healthy",
                "started_at": self.started_at,
            },
            "store": {
                "status": "healthy" if store_ok else "unavailable",
                "counts": store_payload.get("counts", {}),
                "outbox": store_payload.get("outbox", {}),
                "last_event_at": store_payload.get("last_event_at"),
                "last_snapshot_at": store_payload.get("last_snapshot_at"),
            },
            "snapshot": {
                "updated_at": snapshot.updated_at,
                "content_fingerprint": snapshot.content_fingerprint,
            },
            "timestamps": {
                "last_snapshot_at": last_snapshot_at,
                "last_event_at": last_event_at,
                "last_reconcile_at": backend_runtime.get("last_reconcile_at"),
            },
            "backend": {
                "status": backend_runtime.get("status"),
                "outcome": backend_runtime.get("outcome"),
                "ready": backend_runtime.get("ready"),
                "running": backend_runtime.get("running"),
                "reconcile_enabled": backend_runtime.get(
                    "reconcile_enabled",
                    self.config.reconcile_interval_seconds > 0,
                ),
            },
            "limits": {
                "event_debounce_seconds": self.config.event_debounce_seconds,
                "reconcile_interval_seconds": self.config.reconcile_interval_seconds,
                "event_retention_days": self.config.event_retention_days,
                "output_excerpt_chars": self.config.output_excerpt_chars,
                "max_workers": self.config.max_workers,
                "max_outbox_attempts": self.config.max_outbox_attempts,
                "outbox_claim_ttl_seconds": self.config.connector_claim_ttl_seconds,
            },
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        }
        return sanitize_public_mapping(payload)

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

    def get_pending(self) -> Mapping[str, Any]:
        """pending.list: snapshot-derived (attention) pendings, with backend-provided REAL prompts
        (question + choices captured through the turn adapter) superseding a worker's synthetic row."""
        snapshot = self.get_snapshot()
        from .core.turns import (
            PendingInteraction,
            pending_payload_from_snapshot,
            recompute_pending_content_fingerprint,
        )

        payload = dict(pending_payload_from_snapshot(snapshot))
        if self.config.db_path is None:
            return payload
        from .store.sqlite import list_backend_pending

        backend = list_backend_pending(Path(self.config.db_path), self.config.host_id)
        if not backend:
            return payload
        workers = {worker.id: worker for worker in snapshot.workers}
        # Build backend interactions first; only a worker whose interaction builds successfully
        # supersedes its synthetic row (atomic per worker — a build failure never drops both).
        built: dict[str, dict[str, Any]] = {}
        for worker_id, pending in sorted(backend.items()):
            worker = workers.get(worker_id)
            try:
                interaction = PendingInteraction.from_dict(
                    {
                        "host_id": self.config.host_id,
                        "worker_id": worker_id,
                        "question": pending.get("question"),
                        "kind": pending.get("kind"),
                        "choices": pending.get("choices") or [],
                        "status": "open",
                        "worker_fingerprint": worker.fingerprint if worker is not None else None,
                        "space_id": worker.space_id if worker is not None else None,
                        "meta": pending.get("meta") or {"source": "backend"},
                    }
                )
            except Exception:
                continue
            built[worker_id] = interaction.to_dict()
        rows = [row for row in payload.get("pending_interactions", []) if row.get("worker_id") not in built]
        rows.extend(built.values())
        payload["pending_interactions"] = rows
        # The wrapper change-token must reflect the overlaid list, not the pre-overlay snapshot one.
        payload["content_fingerprint"] = recompute_pending_content_fingerprint(payload)
        return payload

    def get_turns(self, *, schema_version: int = 1) -> Mapping[str, Any]:
        snapshot = self.get_snapshot()
        if self.config.db_path is not None:
            from .backends.herdr_turns import refresh_structured_turn_content
            from .store.sqlite import turns_payload_from_store

            refresh_structured_turn_content(self.config)
            if schema_version == 1:
                return turns_payload_from_store(
                    Path(self.config.db_path),
                    self.config.host_id,
                    snapshot=snapshot,
                )
            return turns_payload_from_store(
                Path(self.config.db_path),
                self.config.host_id,
                snapshot=snapshot,
                schema_version=2,
            )
        from .core.turns import turns_payload_from_snapshot

        if schema_version == 1:
            try:
                return turns_payload_from_snapshot(snapshot)
            except ValueError as exc:
                if str(exc) != "upgrade_required":
                    raise
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "upgrade_required",
                    "required_turn_schema_version": 2,
                }
        return turns_payload_from_snapshot(snapshot, schema_version=2)

    def get_turn_content(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        """turn.content.get: return one immutable bounded canonical page."""
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .store.sqlite import get_turn_content

        return get_turn_content(
            Path(self.config.db_path),
            self.config.host_id,
            turn_id=params.get("turn_id"),
            content_revision=params.get("content_revision"),
            field=params.get("field"),
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
        )

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

        return ConnectorOutboxAPI(
            Path(self.config.db_path),
            self.config.host_id,
            default_lease_seconds=self.config.connector_claim_ttl_seconds,
            max_attempts=self.config.max_outbox_attempts,
        ).dispatch(method, params)

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
