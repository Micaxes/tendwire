"""Opt-in Herdr socket event backend and reconciliation layer.

This module is intentionally imported only from the explicit socket backend
path. It reuses the PR8 socket client for transport and the Herdr CLI adapter's
projection helpers for Tendwire model normalization.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import (
    BackendHealth,
    Snapshot,
    Space,
    Worker,
    WorkerBinding,
    normalize_status,
    private_stable_sha256,
    utc_timestamp,
)
from ..core.projector import project_from_observations
from ..store.sqlite import (
    expire_stale_worker_bindings,
    expire_worker_bindings,
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
    upsert_worker_bindings,
)
from .herdr_cli import (
    _pane_has_agent,
    _payload_items,
    _spaces_from_payload,
    _worker_record_from_item,
    _workers_and_bindings_from_records,
    herdr_backend_health,
)
from .herdr_protocol import (
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAME_SET,
    HERDR_OFFICIAL_EVENT_NAMES,
    HerdrEnvelopeError,
    HerdrMalformedLineError,
    HerdrProtocolError,
    build_events_subscribe_params,
)
from .herdr_socket import (
    HerdrSocketClient,
    HerdrSocketConnectionError,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)


BACKEND_NAME = "herdr"
DEFAULT_SUBSCRIBE_METHOD = HERDR_EVENTS_SUBSCRIBE_METHOD
DEFAULT_DEBOUNCE_SECONDS = 0.05
DEFAULT_DEDUPE_SIZE = 512
DEFAULT_MAX_BATCH_SIZE = 64
DEFAULT_RECONNECT_DELAY_SECONDS = 0.25

_AGENT_PAYLOAD_KEYS = ("agents", "workers", "data", "items", "results", "result")
_PANE_PAYLOAD_KEYS = ("panes", "items", "data", "results", "result")
_SUPPORTED_EVENT_NAMES = HERDR_OFFICIAL_EVENT_NAMES
_CLOSED_EVENT_NAMES = frozenset({"pane.closed", "pane.exited"})
_SPACE_EVENT_NAMES = frozenset(
    {
        "workspace.created",
        "workspace.updated",
        "workspace.renamed",
        "workspace.closed",
        "workspace.focused",
    }
)
_WORKTREE_EVENT_NAMES = frozenset({"worktree.created", "worktree.opened", "worktree.removed"})
_PANE_WORKER_EVENT_NAMES = frozenset({"pane.created", "pane.focused"})


class HerdrEventBackendError(Exception):
    """Base error for the opt-in Herdr socket event backend."""


@dataclass(frozen=True)
class HerdrEventBackendHealth:
    """Small in-memory health state for the Herdr socket backend."""

    status: str
    outcome: str
    observed_at: str
    message: str

    def to_backend_health(
        self,
        *,
        spaces: Sequence[Space] | None = None,
        workers: Sequence[Worker] | None = None,
    ) -> BackendHealth:
        return herdr_backend_health(
            self.outcome,
            observed_at=self.observed_at,
            message=self.message,
            spaces=spaces or [],
            workers=workers or [],
        )


@dataclass(frozen=True)
class NormalizedHerdrEvent:
    """A validated, deduplicated Herdr event description."""

    name: str
    payload: Mapping[str, Any]
    dedupe_key: str


def _compact_key(value: object) -> str:
    return str(value).strip().lower().replace("-", "_").replace(".", "_").replace(":", "_")


def _field_value(item: Mapping[str, Any], expected_key: str) -> Any:
    expected = _compact_key(expected_key)
    for key, value in item.items():
        if _compact_key(key) == expected:
            return value
    return None


def _first_text(item: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = _field_value(item, key)
        if value is None:
            continue
        if isinstance(value, Mapping):
            nested = _first_text(value, ("id", "value", "name", "label"))
            if nested:
                return nested
            continue
        if isinstance(value, (str, int, float, bool)):
            text = str(value)
            if text:
                return text
    return None


def _safe_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _entity_payload(payload: Mapping[str, Any], *entity_names: str) -> dict[str, Any]:
    """Return a single object for an event entity while preserving scalar hints."""
    merged: dict[str, Any] = {}
    nested_entity_keys: set[str] = set()
    for entity_name in entity_names:
        nested = _field_value(payload, entity_name)
        if isinstance(nested, Mapping):
            merged.update(dict(nested))
            nested_entity_keys.add(_compact_key(entity_name))
            break
    for key, value in payload.items():
        if _compact_key(key) in nested_entity_keys:
            continue
        merged.setdefault(str(key), value)
    return merged


def _event_alias_key(name: str) -> str:
    return "_".join(part for part in _compact_key(name).split("_") if part)


def _canonical_event_name(raw_name: Any) -> str | None:
    if not isinstance(raw_name, str) or not raw_name.strip():
        return None
    event_name = raw_name.strip()
    if event_name in HERDR_OFFICIAL_EVENT_NAME_SET:
        return event_name
    aliases = {_event_alias_key(name): name for name in HERDR_OFFICIAL_EVENT_NAMES}
    aliases.update(
        {
            "agent_detected": "pane.agent_detected",
            "agent_observed": "pane.agent_detected",
            "agent_status_changed": "pane.agent_status_changed",
            "agent_status_updated": "pane.agent_status_changed",
            "pane_observed": "pane.created",
            "pane_detected": "pane.created",
            "workspace_observed": "workspace.updated",
            "workspace_detected": "workspace.created",
            "worktree_observed": "worktree.opened",
            "worktree_detected": "worktree.created",
            "worktree_updated": "worktree.opened",
            "worktree_changed": "worktree.opened",
            "worktree_closed": "worktree.removed",
            "worktree_deleted": "worktree.removed",
        }
    )
    return aliases.get(_event_alias_key(event_name))


def _server_sequence_key(envelope: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
    server_id = _first_text(envelope, ("server_id", "serverId", "source_id", "sourceId"))
    if server_id is None:
        server_id = _first_text(payload, ("server_id", "serverId", "source_id", "sourceId"))
    sequence = _first_text(envelope, ("sequence", "seq", "offset", "revision"))
    if sequence is None:
        sequence = _first_text(payload, ("sequence", "seq", "offset", "revision"))
    if server_id and sequence:
        return f"server:{server_id}:{sequence}"
    return None


def _event_id_key(envelope: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
    event_id = _first_text(envelope, ("event_id", "eventId", "event_uid", "eventUid"))
    if event_id is None:
        event_id = _first_text(payload, ("event_id", "eventId", "event_uid", "eventUid"))
    return f"event:{event_id}" if event_id else None


def normalize_event(envelope: Mapping[str, Any]) -> NormalizedHerdrEvent | None:
    """Normalize a raw Herdr event envelope; unsupported events return None."""
    name = _canonical_event_name(envelope.get("event"))
    if name is None:
        return None
    payload = envelope.get("payload", {})
    if payload is None:
        payload = {}
    if not isinstance(payload, Mapping):
        return None
    payload_map = dict(payload)
    dedupe_key = _server_sequence_key(envelope, payload_map)
    if dedupe_key is None:
        dedupe_key = _event_id_key(envelope, payload_map)
    if dedupe_key is None:
        digest = private_stable_sha256({"event": name, "payload": payload_map})[:24]
        dedupe_key = f"fallback:{digest}"
    return NormalizedHerdrEvent(name=name, payload=payload_map, dedupe_key=dedupe_key)


def _worker_copy(
    worker: Worker,
    *,
    worker_id: str | None = None,
    name: str | None = None,
    status: str | None = None,
    space_id: str | None = None,
    meta: Mapping[str, Any] | None = None,
    last_seen_at: str | None = None,
    summary: str | None = None,
    backend_target: Mapping[str, Any] | None = None,
) -> Worker:
    return Worker(
        id=worker_id if worker_id is not None else worker.id,
        name=name if name is not None else worker.name,
        status=status if status is not None else worker.status,
        space_id=space_id if space_id is not None else worker.space_id,
        meta=dict(meta) if meta is not None else dict(worker.meta),
        last_seen_at=last_seen_at if last_seen_at is not None else worker.last_seen_at,
        summary=summary if summary is not None else worker.summary,
        backend_target=dict(backend_target) if isinstance(backend_target, Mapping) else worker.backend_target,
    )


def _merge_worker_update(existing: Worker | None, observed: Worker, *, status: str | None = None) -> Worker:
    if existing is None:
        if status is not None:
            return _worker_copy(observed, status=status)
        return observed
    merged_meta = dict(existing.meta)
    merged_meta.update(observed.meta)
    observed_name_is_identity = observed.name in {observed.id, "unknown"}
    resolved_status = status if status is not None else observed.status
    if resolved_status == "unknown" and existing.status != "unknown":
        resolved_status = existing.status
    return Worker(
        id=existing.id,
        name=existing.name if observed_name_is_identity else observed.name,
        status=resolved_status,
        space_id=observed.space_id or existing.space_id,
        meta=merged_meta,
        last_seen_at=observed.last_seen_at or utc_timestamp(),
        summary=observed.summary or existing.summary,
        backend_target=observed.backend_target or existing.backend_target,
    )


def _closed_worker(worker: Worker) -> Worker:
    return _worker_copy(worker, status="closed", last_seen_at=worker.last_seen_at or utc_timestamp())


def _observed_worker_count(workers: Sequence[Worker]) -> int:
    return len([worker for worker in workers if worker.status != "closed"])


def _binding_target(binding: WorkerBinding) -> tuple[str, str]:
    return (binding.target_kind, binding.target_value)


def _target_pairs_from_item(item: Mapping[str, Any], *, old_first: bool = False) -> list[tuple[str, str]]:
    old_pairs = [
        ("pane_id", _first_text(item, ("old_pane_id", "previous_pane_id", "from_pane_id", "source_pane_id"))),
        (
            "terminal_id",
            _first_text(item, ("old_terminal_id", "previous_terminal_id", "from_terminal_id", "source_terminal_id")),
        ),
    ]
    current_pairs = [
        ("agent_id", _first_text(item, ("agent_id", "agentId"))),
        ("terminal_id", _first_text(item, ("terminal_id", "terminalId"))),
        ("pane_id", _first_text(item, ("pane_id", "paneId", "id"))),
        ("agent", _first_text(item, ("agent",))),
        ("name", _first_text(item, ("name", "label"))),
    ]
    pairs = [*old_pairs, *current_pairs] if old_first else [*current_pairs, *old_pairs]
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for kind, value in pairs:
        if not value:
            continue
        pair = (kind, value)
        if pair in seen:
            continue
        seen.add(pair)
        result.append(pair)
    return result


def _new_move_target(item: Mapping[str, Any]) -> tuple[str, str] | None:
    candidates = (
        ("pane_id", ("new_pane_id", "to_pane_id", "target_pane_id", "pane_id", "paneId", "id")),
        ("terminal_id", ("new_terminal_id", "to_terminal_id", "target_terminal_id", "terminal_id", "terminalId")),
        ("agent_id", ("agent_id", "agentId")),
    )
    for kind, keys in candidates:
        value = _first_text(item, keys)
        if value:
            return kind, value
    return None


def _has_public_worker_identity(item: Mapping[str, Any]) -> bool:
    return (
        _first_text(item, ("worker_id", "id", "slug", "agent_id", "agent", "name", "label", "title"))
        is not None
    )


class HerdrEventBackend:
    """Maintain Tendwire projections from Herdr socket reconcile and events."""

    def __init__(
        self,
        config: Config,
        *,
        client_factory: Callable[[Config], HerdrSocketClient] | None = None,
        subscribe_method: str = DEFAULT_SUBSCRIBE_METHOD,
        debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
        dedupe_size: int = DEFAULT_DEDUPE_SIZE,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
        reconnect_delay_seconds: float = DEFAULT_RECONNECT_DELAY_SECONDS,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.config = config
        self.client_factory = client_factory or self._default_client_factory
        requested_subscribe_method = str(subscribe_method or DEFAULT_SUBSCRIBE_METHOD)
        if requested_subscribe_method != HERDR_EVENTS_SUBSCRIBE_METHOD:
            raise HerdrEventBackendError("Herdr event backend requires events.subscribe")
        self.subscribe_method = HERDR_EVENTS_SUBSCRIBE_METHOD
        self.debounce_seconds = max(0.0, float(debounce_seconds))
        self.dedupe_size = max(1, int(dedupe_size))
        self.max_batch_size = max(1, int(max_batch_size))
        self.reconnect_delay_seconds = max(0.0, float(reconnect_delay_seconds))
        self.stop_event = stop_event or threading.Event()
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._dedupe: OrderedDict[str, None] = OrderedDict()
        self._pending_events: list[NormalizedHerdrEvent] = []
        self._spaces: dict[str, Space] = {}
        self._workers: dict[str, Worker] = {}
        self._bindings: dict[str, WorkerBinding] = {}
        self._health = self._health_for("unknown")
        self._load_existing_state()

    @staticmethod
    def _default_client_factory(config: Config) -> HerdrSocketClient:
        return HerdrSocketClient(timeout=config.herdr_timeout_seconds)

    @property
    def db_path(self) -> Path:
        if self.config.db_path is None:
            raise HerdrEventBackendError("socket event backend requires a sqlite db path")
        return Path(self.config.db_path)

    @property
    def health(self) -> HerdrEventBackendHealth:
        with self._lock:
            return self._health

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def _health_for(self, outcome: str) -> HerdrEventBackendHealth:
        health = herdr_backend_health(outcome)
        return HerdrEventBackendHealth(
            status=health.status,
            outcome=health.outcome,
            observed_at=health.observed_at or utc_timestamp(),
            message=health.message,
        )

    def _load_existing_state(self) -> None:
        try:
            snapshot = latest_snapshot(self.db_path, self.config.host_id)
        except Exception:
            snapshot = None
        if snapshot is not None:
            self._spaces = {space.id: space for space in snapshot.spaces}
            self._workers = {worker.id: worker for worker in snapshot.workers}
            for health in snapshot.backend_health:
                if health.name == BACKEND_NAME:
                    self._health = HerdrEventBackendHealth(
                        status=health.status,
                        outcome=health.outcome,
                        observed_at=health.observed_at or utc_timestamp(),
                        message=health.message,
                    )
                    break
        try:
            bindings = list_worker_bindings(self.db_path, self.config.host_id, backend=BACKEND_NAME)
        except Exception:
            bindings = []
        self._bindings = {binding.private_fingerprint: binding for binding in bindings}

    def start(self, *, wait_for_reconcile: bool = True, timeout_seconds: float | None = None) -> None:
        if self._thread is not None:
            return
        self.stop_event.clear()
        self._ready.clear()
        thread = threading.Thread(target=self.run_forever, name="tendwire-herdr-events", daemon=True)
        self._thread = thread
        thread.start()
        if wait_for_reconcile:
            timeout = self.config.herdr_timeout_seconds if timeout_seconds is None else timeout_seconds
            self._ready.wait(max(0.001, float(timeout)))

    def stop(self) -> None:
        self.stop_event.set()
        self.flush()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.config.herdr_timeout_seconds))
        self._thread = None

    def run_forever(self) -> None:
        while not self.stop_event.is_set():
            try:
                client = self.client_factory(self.config)
                if hasattr(client, "connect"):
                    client.connect()
                try:
                    self.reconcile_once(client=client)
                    self._ready.set()
                    if self.stop_event.is_set():
                        break
                    stream = self._subscribe_event_stream(client)
                    self._read_event_stream(client, stream.subscription_id)
                finally:
                    if hasattr(client, "close"):
                        client.close()
            except HerdrSocketTimeoutError:
                self._mark_unhealthy_safe("timeout")
            except (HerdrSocketDisconnectedError, HerdrSocketConnectionError):
                self._mark_unhealthy_safe("socket_disconnected")
            except (HerdrMalformedLineError, HerdrEnvelopeError, HerdrProtocolError, ValueError, TypeError):
                self._mark_unhealthy_safe("protocol_error")
            except Exception:
                self._mark_unhealthy_safe("unknown")
            if self.stop_event.is_set():
                break
            if self.reconnect_delay_seconds:
                self.stop_event.wait(self.reconnect_delay_seconds)

    def _read_event_stream(self, client: Any, subscription_id: str) -> None:
        while not self.stop_event.is_set():
            try:
                envelope = client.read_event(subscription_id, timeout=self.config.herdr_timeout_seconds)
            except HerdrSocketTimeoutError:
                continue
            self.queue_event_envelope(envelope)
            disconnected = False
            deadline = time.monotonic() + self.debounce_seconds
            while (
                self.debounce_seconds > 0
                and self._pending_event_count() < self.max_batch_size
                and not self.stop_event.is_set()
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    extra = client.read_event(subscription_id, timeout=max(0.001, remaining))
                except HerdrSocketTimeoutError:
                    break
                except HerdrSocketDisconnectedError:
                    disconnected = True
                    break
                self.queue_event_envelope(extra, flush=False)
            self.flush()
            if disconnected:
                raise HerdrSocketDisconnectedError("Herdr socket disconnected during event drain")

    def _pending_event_count(self) -> int:
        with self._lock:
            return len(self._pending_events)

    def _current_bindings(self) -> list[WorkerBinding]:
        return list(self._bindings.values())

    def _records_from_reconcile_payloads(self, agent_payload: Any, pane_payload: Any) -> list[tuple[Worker, str]]:
        records_by_identity: OrderedDict[str, tuple[Worker, str]] = OrderedDict()
        for item in _payload_items(agent_payload, _AGENT_PAYLOAD_KEYS):
            worker, identity = _worker_record_from_item(item, self.config)
            records_by_identity.setdefault(identity, (worker, identity))
        for item in _payload_items(pane_payload, _PANE_PAYLOAD_KEYS):
            if not _pane_has_agent(item):
                continue
            worker, identity = _worker_record_from_item(item, self.config)
            records_by_identity.setdefault(identity, (worker, identity))
        return list(records_by_identity.values())

    def reconcile_once(self, *, client: Any | None = None) -> Snapshot:
        """Perform a full Herdr list reconcile and persist Tendwire projections."""
        owns_client = client is None
        if client is None:
            client = self.client_factory(self.config)
        try:
            if owns_client and hasattr(client, "connect"):
                client.connect()
            payloads = {
                "workspace.list": self._call_list_method(client, "workspace_list"),
                "tab.list": self._call_list_method(client, "tab_list"),
                "pane.list": self._call_list_method(client, "pane_list"),
                "agent.list": self._call_list_method(client, "agent_list"),
            }
            # tab.list is intentionally part of the authoritative reconcile barrier;
            # Tendwire has no current public tab model to project into.
            _ = payloads["tab.list"]
            with self._lock:
                stored_bindings = list_worker_bindings(
                    self.db_path,
                    self.config.host_id,
                    backend=BACKEND_NAME,
                )
                spaces = _spaces_from_payload(payloads["workspace.list"])
                records = self._records_from_reconcile_payloads(
                    payloads["agent.list"],
                    payloads["pane.list"],
                )
                workers, bindings = _workers_and_bindings_from_records(
                    self.config,
                    records,
                    stored_bindings=stored_bindings,
                )
                outcome = "healthy_non_empty" if spaces or workers else "empty_healthy"
                health = herdr_backend_health(outcome, spaces=spaces, workers=workers)
                previous = latest_snapshot(self.db_path, self.config.host_id)
                snapshot_workers = self._workers_with_closed_missing(
                    previous.workers if previous is not None else [],
                    workers,
                )
                snapshot = project_from_observations(
                    self.config,
                    spaces=spaces,
                    workers=snapshot_workers,
                    backend_health=[health],
                )
                save_snapshot(self.db_path, snapshot)
                if bindings:
                    upsert_worker_bindings(self.db_path, bindings)
                expire_stale_worker_bindings(
                    self.db_path,
                    self.config.host_id,
                    backend=BACKEND_NAME,
                    current_private_fingerprints=[binding.private_fingerprint for binding in bindings],
                    now=snapshot.updated_at,
                )
                self._spaces = {space.id: space for space in snapshot.spaces}
                self._workers = {worker.id: worker for worker in snapshot.workers}
                self._bindings = {binding.private_fingerprint: binding for binding in bindings}
                self._health = HerdrEventBackendHealth(
                    status=health.status,
                    outcome=health.outcome,
                    observed_at=health.observed_at or snapshot.updated_at,
                    message=health.message,
                )
                return snapshot
        except Exception:
            self._mark_unhealthy("unknown")
            raise
        finally:
            if owns_client and hasattr(client, "close"):
                client.close()

    def _call_list_method(self, client: Any, method_name: str) -> Any:
        method = getattr(client, method_name)
        try:
            return method(timeout=self.config.herdr_timeout_seconds)
        except TypeError:
            return method()

    def _subscribe_event_stream(self, client: Any) -> Any:
        if self.subscribe_method == HERDR_EVENTS_SUBSCRIBE_METHOD and hasattr(client, "events_subscribe"):
            try:
                return client.events_subscribe(
                    _SUPPORTED_EVENT_NAMES,
                    timeout=self.config.herdr_timeout_seconds,
                    event_timeout=self.config.herdr_timeout_seconds,
                )
            except TypeError:
                return client.events_subscribe(_SUPPORTED_EVENT_NAMES)
        params = build_events_subscribe_params(_SUPPORTED_EVENT_NAMES)
        try:
            return client.subscribe(
                self.subscribe_method,
                params,
                timeout=self.config.herdr_timeout_seconds,
                event_timeout=self.config.herdr_timeout_seconds,
            )
        except TypeError:
            return client.subscribe(self.subscribe_method, params)

    def _workers_with_closed_missing(
        self,
        previous_workers: Sequence[Worker],
        current_workers: Sequence[Worker],
    ) -> list[Worker]:
        current_by_id = {worker.id: worker for worker in current_workers}
        merged = list(current_workers)
        for worker in previous_workers:
            if worker.id in current_by_id:
                continue
            merged.append(_closed_worker(worker))
        return merged

    def queue_event_envelope(self, envelope: Mapping[str, Any], *, flush: bool | None = None) -> bool:
        event = normalize_event(envelope)
        if event is None:
            return False
        with self._lock:
            if self._seen_duplicate(event.dedupe_key):
                return False
            self._pending_events.append(event)
            should_flush = self.debounce_seconds <= 0 if flush is None else flush
        if should_flush:
            self.flush()
        return True

    def _seen_duplicate(self, key: str) -> bool:
        if key in self._dedupe:
            self._dedupe.move_to_end(key)
            return True
        self._dedupe[key] = None
        while len(self._dedupe) > self.dedupe_size:
            self._dedupe.popitem(last=False)
        return False

    def flush(self) -> None:
        with self._lock:
            events = list(self._pending_events)
            self._pending_events.clear()
        if not events:
            return
        with self._lock:
            changed = False
            for event in events:
                changed = self._apply_event(event) or changed
            if changed:
                self._persist_current_state()

    def _apply_event(self, event: NormalizedHerdrEvent) -> bool:
        if event.name in _SPACE_EVENT_NAMES:
            status = "closed" if event.name == "workspace.closed" else None
            return self._apply_space_event(event.payload, status=status)
        if event.name in _WORKTREE_EVENT_NAMES:
            status = "closed" if event.name == "worktree.removed" else None
            return self._apply_worktree_event(event.payload, status=status)
        if event.name in _PANE_WORKER_EVENT_NAMES:
            item = _entity_payload(event.payload, "pane", "agent", "worker")
            if not _pane_has_agent(item) and self._match_binding(item) is None:
                return False
            return self._upsert_worker_from_item(item)
        if event.name == "pane.agent_detected":
            item = _entity_payload(event.payload, "agent", "worker", "pane")
            return self._upsert_worker_from_item(item)
        if event.name == "pane.agent_status_changed":
            item = _entity_payload(event.payload, "agent", "worker", "pane")
            raw_status = _first_text(item, ("status", "agent_status", "state", "phase"))
            return self._upsert_worker_from_item(
                item,
                status=normalize_status(raw_status),
                update_binding=False,
            )
        if event.name == "pane.moved":
            item = _entity_payload(event.payload, "pane")
            return self._apply_pane_moved(item)
        if event.name in _CLOSED_EVENT_NAMES:
            item = _entity_payload(event.payload, "pane")
            return self._apply_pane_closed(item, reason=event.name.replace(".", "_"))
        if event.name == "pane.output_matched":
            return False
        return False

    def _apply_space_event(self, payload: Mapping[str, Any], *, status: str | None = None) -> bool:
        item = _entity_payload(payload, "workspace", "space")
        direct_name_hint = _first_text(item, ("label", "name", "title"))
        rename_hint = _first_text(item, ("new_name", "newName"))
        if _first_text(item, ("workspace_id", "space_id", "id", "slug", "name", "label", "title")) is None:
            return False
        name_hint = direct_name_hint or rename_hint
        if rename_hint and direct_name_hint is None:
            item["name"] = rename_hint
        if status is not None:
            item["status"] = status
        spaces = _spaces_from_payload([item] if item else [])
        if not spaces:
            return False
        space = spaces[0]
        existing = self._spaces.get(space.id)
        if existing is not None and status is None:
            meta = dict(existing.meta)
            meta.update(space.meta)
            space = Space(
                id=existing.id,
                name=space.name if name_hint else existing.name,
                status=space.status,
                meta=meta,
                updated_at=space.updated_at or utc_timestamp(),
                status_line=space.status_line or existing.status_line,
            )
        self._spaces[space.id] = space
        return True

    def _apply_worktree_event(self, payload: Mapping[str, Any], *, status: str | None = None) -> bool:
        item = _entity_payload(payload, "workspace", "space", "worktree")
        workspace_id = _first_text(item, ("workspace_id", "space_id"))
        if workspace_id is None or workspace_id not in self._spaces:
            return False
        item.setdefault("id", workspace_id)
        if status is not None:
            item["status"] = status
        return self._apply_space_event({"workspace": item}, status=status)

    def _event_worker_and_binding(
        self,
        item: Mapping[str, Any],
        *,
        status: str | None = None,
    ) -> tuple[Worker | None, WorkerBinding | None, WorkerBinding | None]:
        try:
            record = _worker_record_from_item(item, self.config)
            workers, bindings = _workers_and_bindings_from_records(
                self.config,
                [record],
                stored_bindings=self._current_bindings(),
            )
        except Exception:
            return None, None, self._match_binding(item)
        worker = workers[0] if workers else None
        binding = bindings[0] if bindings else None
        matched_binding = self._match_binding(item)
        if matched_binding is not None and worker is not None:
            worker = _worker_copy(worker, worker_id=matched_binding.worker_id)
        if worker is not None and status is not None:
            worker = _worker_copy(worker, status=status)
        return worker, binding, matched_binding

    def _upsert_worker_from_item(
        self,
        item: Mapping[str, Any],
        *,
        status: str | None = None,
        update_binding: bool = True,
    ) -> bool:
        if not item:
            return False
        if not _has_public_worker_identity(item) and self._match_binding(item) is None:
            return False
        worker, binding, matched_binding = self._event_worker_and_binding(item, status=status)
        if worker is None:
            return False
        existing = self._workers.get(worker.id)
        if existing is None and matched_binding is not None:
            existing = self._workers.get(matched_binding.worker_id)
        if matched_binding is not None:
            worker = _worker_copy(worker, worker_id=matched_binding.worker_id)
        worker = _merge_worker_update(existing, worker, status=status)
        self._workers[worker.id] = worker
        if update_binding and binding is not None:
            if matched_binding is not None and binding.private_fingerprint != matched_binding.private_fingerprint:
                binding = self._binding_with_worker(matched_binding, worker)
            else:
                binding = self._binding_with_worker(binding, worker)
            self._bindings[binding.private_fingerprint] = binding
            upsert_worker_bindings(self.db_path, [binding])
        return True

    def _binding_with_worker(self, binding: WorkerBinding, worker: Worker) -> WorkerBinding:
        return WorkerBinding(
            host_id=binding.host_id,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend=binding.backend,
            target_kind=binding.target_kind,
            target_value=binding.target_value,
            turn_target_kind=binding.turn_target_kind,
            turn_target_value=binding.turn_target_value,
            sendable=binding.sendable,
            reason=binding.reason,
            observed_at=utc_timestamp(),
            expires_at=None,
            private_fingerprint=binding.private_fingerprint,
        )

    def _match_binding(self, item: Mapping[str, Any], *, old_first: bool = False) -> WorkerBinding | None:
        pairs = _target_pairs_from_item(item, old_first=old_first)
        if not pairs:
            return None
        binding_by_target = {_binding_target(binding): binding for binding in self._bindings.values()}
        for pair in pairs:
            binding = binding_by_target.get(pair)
            if binding is not None:
                return binding
        return None

    def _apply_pane_moved(self, item: Mapping[str, Any]) -> bool:
        old_binding = self._match_binding(item, old_first=True)
        new_target = _new_move_target(item)
        if old_binding is None or new_target is None:
            return self._upsert_worker_from_item(item)
        worker = self._workers.get(old_binding.worker_id)
        observed_worker, _binding, _matched = self._event_worker_and_binding(item)
        if worker is None:
            worker = observed_worker
        elif observed_worker is not None:
            worker = _merge_worker_update(worker, _worker_copy(observed_worker, worker_id=worker.id))
        if worker is None:
            return False
        target_kind, target_value = new_target
        moved_binding = WorkerBinding(
            host_id=old_binding.host_id,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend=old_binding.backend,
            target_kind=target_kind,
            target_value=target_value,
            turn_target_kind=old_binding.turn_target_kind,
            turn_target_value=old_binding.turn_target_value,
            sendable=old_binding.sendable,
            reason=old_binding.reason,
            observed_at=utc_timestamp(),
            expires_at=None,
            private_fingerprint=old_binding.private_fingerprint,
        )
        self._workers[worker.id] = _worker_copy(worker, backend_target=moved_binding.backend_target())
        self._bindings[moved_binding.private_fingerprint] = moved_binding
        upsert_worker_bindings(self.db_path, [moved_binding])
        return True

    def _apply_pane_closed(self, item: Mapping[str, Any], *, reason: str) -> bool:
        binding = self._match_binding(item)
        worker: Worker | None = None
        if binding is not None:
            worker = self._workers.get(binding.worker_id)
        if worker is None:
            if binding is None and not _has_public_worker_identity(item):
                return False
            observed_worker, _event_binding, matched_binding = self._event_worker_and_binding(item, status="closed")
            if matched_binding is not None:
                binding = matched_binding
            worker = observed_worker
        if worker is None:
            return False
        closed = _closed_worker(worker)
        self._workers[closed.id] = closed
        if binding is not None:
            expire_worker_bindings(
                self.db_path,
                self.config.host_id,
                backend=BACKEND_NAME,
                private_fingerprints=[binding.private_fingerprint],
                now=utc_timestamp(),
                reason=reason,
            )
            self._bindings.pop(binding.private_fingerprint, None)
        else:
            expire_worker_bindings(
                self.db_path,
                self.config.host_id,
                backend=BACKEND_NAME,
                worker_id=closed.id,
                now=utc_timestamp(),
                reason=reason,
            )
        return True

    def _persist_current_state(self) -> Snapshot:
        spaces = list(self._spaces.values())
        workers = list(self._workers.values())
        outcome = "healthy_non_empty" if spaces or _observed_worker_count(workers) else "empty_healthy"
        health = herdr_backend_health(outcome, spaces=spaces, workers=workers)
        snapshot = project_from_observations(
            self.config,
            spaces=spaces,
            workers=workers,
            backend_health=[health],
        )
        save_snapshot(self.db_path, snapshot)
        self._health = HerdrEventBackendHealth(
            status=health.status,
            outcome=health.outcome,
            observed_at=health.observed_at or snapshot.updated_at,
            message=health.message,
        )
        return snapshot

    def _mark_unhealthy(self, outcome: str) -> Snapshot:
        with self._lock:
            health_state = self._health_for(outcome)
            self._health = health_state
            spaces = list(self._spaces.values())
            workers = list(self._workers.values())
            if not spaces and not workers:
                snapshot = latest_snapshot(self.db_path, self.config.host_id)
                if snapshot is not None:
                    spaces = list(snapshot.spaces)
                    workers = list(snapshot.workers)
            health = health_state.to_backend_health(spaces=spaces, workers=workers)
            snapshot = project_from_observations(
                self.config,
                spaces=spaces,
                workers=workers,
                backend_health=[health],
            )
            save_snapshot(self.db_path, snapshot)
            self._spaces = {space.id: space for space in snapshot.spaces}
            self._workers = {worker.id: worker for worker in snapshot.workers}
            return snapshot

    def _mark_unhealthy_safe(self, outcome: str) -> Snapshot | None:
        try:
            return self._mark_unhealthy(outcome)
        finally:
            self._ready.set()
