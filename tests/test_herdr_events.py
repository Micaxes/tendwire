"""Tests for the opt-in Herdr socket event backend."""

from __future__ import annotations

import json
import sqlite3
import socket
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from types import SimpleNamespace

import pytest

from tendwire.backends.herdr_events import DEFAULT_SUBSCRIBE_METHOD, HerdrEventBackend, HerdrEventBackendError, normalize_event
from tendwire.backends.herdr_socket import (
    HerdrSocketClient,
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)
from tendwire.backends.herdr_protocol import (
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAMES,
    build_events_subscribe_params,
)
from tendwire.config import Config
from tendwire.core.models import BackendHealth, Worker
from tendwire.core.projector import project_from_observations
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.store.sqlite import (
    init_store,
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "backend_target",
    "chat_id",
    "topic_id",
    "message_id",
    "connector",
    "argv",
    "args",
    "env",
    "environment",
    "stdin",
    "stdout",
    "stderr",
    "token",
    "tokens",
    "secret",
    "secrets",
    "raw_payload",
    "socket_path",
    "target_kind",
    "target_value",
    "private_fingerprint",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS}


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert (
                normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")



_NO_OP_TABLES = (
    "commands",
    "command_receipts",
    "turns",
    "attention_items",
    "connector_outbox",
    "connector_deliveries",
)


def _table_count(db_path: Path, host_id: str, table: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE host_id = ?", (host_id,)).fetchone()[0])


def _no_op_state(backend: HerdrEventBackend) -> tuple[str, tuple[tuple[Any, ...], ...], dict[str, int]]:
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    bindings = tuple(
        sorted(
            (
                binding.worker_id,
                binding.private_fingerprint,
                binding.target_kind,
                binding.target_value,
                binding.sendable,
                binding.reason,
                binding.expires_at,
            )
            for binding in list_worker_bindings(
                backend.db_path,
                backend.config.host_id,
                backend="herdr",
                include_expired=True,
            )
        )
    )
    counts = {table: _table_count(backend.db_path, backend.config.host_id, table) for table in _NO_OP_TABLES}
    return snapshot.to_json(), bindings, counts

class _SocketConnection:
    def __init__(self, conn: socket.socket, requests: list[dict[str, Any]]) -> None:
        self.conn = conn
        self.requests = requests
        self._buffer = bytearray()

    def read_request(self) -> dict[str, Any]:
        while b"\n" not in self._buffer:
            chunk = self.conn.recv(4096)
            if not chunk:
                raise ConnectionError("client disconnected before request")
            self._buffer.extend(chunk)
        index = self._buffer.index(b"\n")
        line = bytes(self._buffer[: index + 1])
        del self._buffer[: index + 1]
        request = json.loads(line.decode("utf-8"))
        self.requests.append(request)
        return request

    def send_json(self, payload: Mapping[str, Any]) -> None:
        self.conn.sendall(json.dumps(dict(payload), separators=(",", ":")).encode("utf-8") + b"\n")


class _FakeHerdrSocketServer:
    def __init__(self, tmp_path: Path, handler: Callable[[_SocketConnection], None]) -> None:
        self.path = tmp_path / f"herdr-events-{time.monotonic_ns()}.sock"
        self.handler = handler
        self.requests: list[dict[str, Any]] = []
        self.errors: list[BaseException] = []
        self._ready = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_FakeHerdrSocketServer":
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.path))
        listener.listen(1)
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self._ready.wait(1)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=1)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        if exc_type is None and self.errors:
            raise AssertionError(f"fake Herdr socket failed: {self.errors!r}")

    def _run(self) -> None:
        self._ready.set()
        try:
            assert self._listener is not None
            conn, _addr = self._listener.accept()
            with conn:
                self.handler(_SocketConnection(conn, self.requests))
        except OSError:
            pass
        except BaseException as exc:
            self.errors.append(exc)


class _StaticClient:
    def __init__(
        self,
        *,
        workspaces: Any | None = None,
        tabs: Any | None = None,
        panes: Any | None = None,
        agents: Any | None = None,
    ) -> None:
        self.workspaces = {"workspaces": list(workspaces or [])}
        self.tabs = {"tabs": list(tabs or [])}
        self.panes = {"panes": list(panes or [])}
        self.agents = {"agents": list(agents or [])}
        self.calls: list[str] = []

    def workspace_list(self) -> Any:
        self.calls.append("workspace.list")
        return self.workspaces

    def tab_list(self) -> Any:
        self.calls.append("tab.list")
        return self.tabs

    def pane_list(self) -> Any:
        self.calls.append("pane.list")
        return self.panes

    def agent_list(self) -> Any:
        self.calls.append("agent.list")
        return self.agents


def _config(tmp_path: Path, host_id: str = "events-host") -> Config:
    return Config(
        host_id=host_id,
        data_dir=tmp_path,
        db_path=tmp_path / f"{host_id}.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _backend(tmp_path: Path, host_id: str = "events-host", *, debounce_seconds: float = 0) -> HerdrEventBackend:
    config = _config(tmp_path, host_id)
    init_store(Path(config.db_path))
    return HerdrEventBackend(
        config,
        debounce_seconds=debounce_seconds,
        reconnect_delay_seconds=0,
    )


def _initial_pane_client() -> _StaticClient:
    return _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "pane-1",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "running",
            }
        ],
        agents=[],
    )


def test_startup_reconcile_uses_socket_client_persists_projection_and_private_bindings(tmp_path: Path) -> None:
    def handler(conn: _SocketConnection) -> None:
        results = {
            "workspace.list": {
                "workspaces": [
                    {
                        "id": "space-1",
                        "name": "Build",
                        "status": "active",
                        "pane_id": "private-pane",
                    }
                ]
            },
            "tab.list": {"tabs": [{"id": "tab-private", "workspace_id": "space-1"}]},
            "pane.list": {
                "panes": [
                    {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-private",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": "running",
                    }
                ]
            },
            "agent.list": {
                "agents": [
                    {
                        "agent_id": "agent-private",
                        "name": "Agent One",
                        "workspace_id": "space-1",
                        "status": "waiting",
                        "pane_id": "pane-1",
                    }
                ]
            },
        }
        for _index in range(4):
            request = conn.read_request()
            conn.send_json({"id": request["id"], "result": results[request["method"]]})

    config = _config(tmp_path, "socket-reconcile")
    init_store(Path(config.db_path))
    with _FakeHerdrSocketServer(tmp_path, handler) as server:
        backend = HerdrEventBackend(config, debounce_seconds=0)
        client = HerdrSocketClient(str(server.path), timeout=1)
        snapshot = backend.reconcile_once(client=client)
        client.close()

    assert [request["method"] for request in server.requests] == [
        "workspace.list",
        "tab.list",
        "pane.list",
        "agent.list",
    ]
    assert snapshot.backend_health[0].status == "healthy"
    assert "agent-private" in {worker.id for worker in snapshot.workers}
    assert list_worker_bindings(Path(config.db_path), config.host_id, backend="herdr")
    encoded = snapshot.to_json()
    assert "private-pane" not in encoded
    assert "terminal-private" not in encoded
    _assert_no_public_json_forbidden(json.loads(encoded))


@pytest.mark.parametrize("event_name", HERDR_OFFICIAL_EVENT_NAMES)
def test_normalize_event_accepts_each_official_event_name(event_name: str) -> None:
    event = normalize_event({"event": event_name, "payload": {}})

    assert event is not None
    assert event.name == event_name


@pytest.mark.parametrize(
    ("raw_name", "canonical_name"),
    [
        ("agent.status_changed", "pane.agent_status_changed"),
        ("agent_status_changed", "pane.agent_status_changed"),
        ("agent.detected", "pane.agent_detected"),
        ("pane.observed", "pane.created"),
        ("workspace.observed", "workspace.updated"),
        ("worktree.updated", "worktree.opened"),
        ("worktree.closed", "worktree.removed"),
    ],
)
def test_normalize_event_tolerates_legacy_inbound_aliases_only_after_receive(
    raw_name: str,
    canonical_name: str,
) -> None:
    event = normalize_event({"event": raw_name, "payload": {}})

    assert event is not None
    assert event.name == canonical_name


def test_backend_default_subscription_uses_official_shape_without_legacy_defaults(tmp_path: Path) -> None:
    config = _config(tmp_path, "default-subscribe")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)

    class SubscribeClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__()
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions.append((method, dict(params)))
            backend.stop_event.set()
            return SimpleNamespace(subscription_id="sub-default")

    client = SubscribeClient()
    backend.client_factory = lambda _config: client

    backend.run_forever()

    expected_params = build_events_subscribe_params(HERDR_OFFICIAL_EVENT_NAMES)
    assert DEFAULT_SUBSCRIBE_METHOD == HERDR_EVENTS_SUBSCRIBE_METHOD
    assert client.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected_params)]
    subscribed_names = {subscription["type"] for subscription in expected_params["subscriptions"]}
    assert {
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
    }.isdisjoint(subscribed_names)


def test_backend_rejects_non_official_subscribe_method(tmp_path: Path) -> None:
    config = _config(tmp_path, "custom-subscribe")
    init_store(Path(config.db_path))

    with pytest.raises(HerdrEventBackendError):
        HerdrEventBackend(config, subscribe_method="custom.subscribe")


def test_pane_agent_detected_official_event_updates_worker_and_private_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "agent-detected")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))

    backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {
                "agent": {
                    "agent_id": "agent-2",
                    "name": "Agent Two",
                    "workspace_id": "space-1",
                    "pane_id": "pane-2",
                    "status": "running",
                }
            },
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    assert snapshot is not None
    assert {worker.id for worker in snapshot.workers} == {"agent-2"}
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "agent-2"
    _assert_no_public_json_forbidden(json.loads(snapshot.to_json()))


def test_unknown_and_malformed_known_events_do_not_mutate_any_public_or_private_state(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "event-noop")
    backend.reconcile_once(client=_initial_pane_client())
    before = _no_op_state(backend)

    for envelope in (
        {"event": "unknown.future", "payload": {"pane_id": "pane-secret", "stdout": "secret"}},
        {"event": "workspace.created", "payload": {"pane_id": "pane-secret", "stdout": "secret"}},
        {"event": "workspace.renamed", "payload": {"new_name": "secret"}},
        {"event": "worktree.created", "payload": {"worktree_id": "worktree-secret", "stderr": "secret"}},
        {"event": "pane.created", "payload": {"labels": ["agent"], "argv": ["secret"]}},
        {"event": "pane.agent_detected", "payload": []},
        {"event": "pane.agent_status_changed", "payload": {"status": "failed", "stderr": "secret"}},
        {
            "event": "pane.output_matched",
            "payload": {
                "pane_id": "pane-secret",
                "terminal_id": "terminal-secret",
                "stdout": "secret",
                "stderr": "secret",
                "token": "secret",
            },
        },
    ):
        backend.queue_event_envelope(envelope)

    after = _no_op_state(backend)
    assert after == before
    _assert_no_public_json_forbidden(json.loads(after[0]))
    assert "pane-secret" not in after[0]
    assert "terminal-secret" not in after[0]
    assert "secret" not in after[0]


def test_worktree_events_only_update_existing_workspace_observations(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "worktree-adjacent")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None

    assert (
        backend.queue_event_envelope(
            {
                "event": "worktree.created",
                "payload": {"workspace_id": "new-space", "name": "Should Not Appear"},
            }
        )
        is True
    )
    unchanged = latest_snapshot(backend.db_path, backend.config.host_id)
    assert unchanged is not None
    assert [space.id for space in unchanged.spaces] == ["space-1"]
    assert unchanged.spaces[0].name == "Build"

    assert (
        backend.queue_event_envelope(
            {
                "event": "worktree.opened",
                "payload": {
                    "workspace_id": "space-1",
                    "name": "Build Worktree",
                    "status": "active",
                },
            }
        )
        is True
    )
    updated = latest_snapshot(backend.db_path, backend.config.host_id)
    assert updated is not None
    assert [space.id for space in updated.spaces] == ["space-1"]
    assert updated.spaces[0].name == "Build Worktree"
    _assert_no_public_json_forbidden(json.loads(updated.to_json()))

def test_run_forever_reconnects_and_resubscribes_after_event_disconnect(tmp_path: Path) -> None:
    config = _config(tmp_path, "reconnect-resubscribe")
    init_store(Path(config.db_path))

    class SequenceClient(_StaticClient):
        def __init__(
            self,
            label: str,
            events: list[Any],
            *,
            stop_on_subscribe: bool = False,
            pane_status: str = "running",
        ) -> None:
            super().__init__(
                workspaces=[{"id": "space-1", "name": "Build"}],
                panes=[
                    {
                        "pane_id": "pane-1",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": pane_status,
                    }
                ],
            )
            self.label = label
            self.events = list(events)
            self.stop_on_subscribe = stop_on_subscribe
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []
            self.read_calls = 0
            self.closed = False

        def connect(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions.append((method, dict(params)))
            if self.stop_on_subscribe:
                backend.stop_event.set()
            return SimpleNamespace(subscription_id=f"{self.label}-sub")

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            self.read_calls += 1
            if not self.events:
                backend.stop_event.set()
                raise HerdrSocketTimeoutError("idle")
            event = self.events.pop(0)
            if event == "timeout":
                raise HerdrSocketTimeoutError("idle")
            if event == "disconnect":
                raise HerdrSocketDisconnectedError("disconnect")
            return {"id": subscription_id, **event}

    first = SequenceClient(
        "first",
        [
            "timeout",
            {"event": "pane.agent_status_changed", "payload": {"agent": "Agent One", "status": "blocked"}},
            "disconnect",
        ],
    )
    second = SequenceClient("second", [], stop_on_subscribe=True, pane_status="blocked")
    clients = [first, second]
    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: clients.pop(0),
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )

    backend.run_forever()

    expected = build_events_subscribe_params(HERDR_OFFICIAL_EVENT_NAMES)
    assert first.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected)]
    assert second.subscriptions == [(HERDR_EVENTS_SUBSCRIBE_METHOD, expected)]
    assert first.read_calls == 3
    assert first.closed is True
    assert second.closed is True
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"


def test_start_stop_are_idempotent_and_bounded_for_idle_subscription(tmp_path: Path) -> None:
    config = Config(
        host_id="bounded-stop",
        data_dir=tmp_path,
        db_path=tmp_path / "bounded-stop.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.05,
    )
    init_store(Path(config.db_path))

    class IdleClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__()
            self.subscriptions = 0

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

        def subscribe(
            self,
            method: str,
            params: Mapping[str, Any],
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.subscriptions += 1
            return SimpleNamespace(subscription_id="idle-sub")

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            raise HerdrSocketTimeoutError("idle")

    client = IdleClient()
    backend = HerdrEventBackend(
        config,
        client_factory=lambda _config: client,
        debounce_seconds=0,
        reconnect_delay_seconds=0,
    )

    started = time.monotonic()
    backend.start(wait_for_reconcile=True, timeout_seconds=0.2)
    deadline = time.monotonic() + 0.5
    while client.subscriptions < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert backend.ready is True
    assert client.subscriptions >= 1

    backend.stop()
    backend.stop()

    assert backend.running is False
    assert time.monotonic() - started < 2.0


def test_agent_status_changed_updates_worker_once_for_duplicate_sequence(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "status-dedupe")
    backend.reconcile_once(client=_initial_pane_client())

    first = {
        "event": "pane.agent_status_changed",
        "server_id": "srv-1",
        "sequence": 10,
        "payload": {"agent": "Agent One", "status": "blocked"},
    }
    duplicate = {
        "event": "agent.status_changed",
        "server_id": "srv-1",
        "sequence": 10,
        "payload": {"agent": "Agent One", "status": "failed"},
    }

    assert backend.queue_event_envelope(first) is True
    assert backend.queue_event_envelope(duplicate) is False

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"


def test_pane_moved_preserves_public_worker_and_updates_private_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-moved")
    backend.reconcile_once(client=_initial_pane_client())
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before is not None
    worker_id = before.workers[0].id
    binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "old_pane_id": "pane-1",
                "pane_id": "pane-2",
                "agent": "Agent One",
                "workspace_id": "space-1",
            },
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    moved_binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert after is not None
    assert after.workers[0].id == worker_id
    assert moved_binding.private_fingerprint == binding.private_fingerprint
    assert moved_binding.target_kind == "pane_id"
    assert moved_binding.target_value == "pane-2"


def test_pane_closed_closes_worker_and_expires_matching_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-closed")
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope({"event": "pane.closed", "payload": {"pane_id": "pane-1"}})

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []
    expired = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
        include_expired=True,
    )
    assert expired[0].sendable is False
    assert expired[0].reason == "pane_closed"


def test_disconnect_degraded_state_preserves_workers_and_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "degraded")
    backend.reconcile_once(client=_initial_pane_client())
    binding_before = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    backend._mark_unhealthy("socket_disconnected")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    binding_after = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]
    assert snapshot is not None
    assert snapshot.workers[0].status == "active"
    assert snapshot.backend_health[0].status == "unavailable"
    assert binding_after.private_fingerprint == binding_before.private_fingerprint
    assert binding_after.sendable is True


def test_healthy_empty_reconnect_closes_missing_workers_and_expires_bindings(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "healthy-empty")
    backend.reconcile_once(client=_initial_pane_client())

    backend.reconcile_once(client=_StaticClient(workspaces=[], tabs=[], panes=[], agents=[]))

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "healthy"
    assert snapshot.backend_health[0].outcome == "empty_healthy"
    assert snapshot.workers[0].status == "closed"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr") == []


def test_debounce_batches_until_flush_and_shutdown_flushes(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "debounce", debounce_seconds=60)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {"event": "pane.agent_status_changed", "payload": {"agent": "Agent One", "status": "blocked"}}
    )
    pending = latest_snapshot(backend.db_path, backend.config.host_id)
    assert pending is not None
    assert pending.workers[0].status == "active"

    backend.stop()

    flushed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert flushed is not None
    assert flushed.workers[0].status == "blocked"


def test_idle_event_timeout_keeps_polling_without_marking_backend_unhealthy(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "idle-timeout")
    backend.reconcile_once(client=_initial_pane_client())

    class IdleThenEventClient:
        def __init__(self) -> None:
            self.calls = 0

        def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                raise HerdrSocketTimeoutError("idle")
            backend.stop_event.set()
            return {
                "id": subscription_id,
                "event": "pane.agent_status_changed",
                "payload": {"agent": "Agent One", "status": "blocked"},
            }

    client = IdleThenEventClient()
    backend._read_event_stream(client, "sub-1")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.workers[0].status == "blocked"
    assert snapshot.backend_health[0].status == "healthy"
    assert client.calls == 2


def test_mark_unhealthy_safe_sets_ready_even_when_persist_fails(tmp_path: Path, monkeypatch: Any) -> None:
    backend = _backend(tmp_path, "ready-on-error")

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("store unavailable")

    monkeypatch.setattr("tendwire.backends.herdr_events.save_snapshot", boom)

    try:
        backend._mark_unhealthy_safe("protocol_error")
    except RuntimeError:
        pass

    assert backend.ready is True


def test_protocol_error_health_is_degraded_and_specific(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "protocol-health")

    backend._mark_unhealthy("protocol_error")

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "protocol_error"


def test_daemon_starts_socket_backend_only_when_configured(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-socket.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(
        host_id="daemon-socket",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )
    calls: list[str] = []

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            calls.append(f"start:{wait_for_reconcile}")
            snapshot = project_from_observations(
                self.config,
                workers=[Worker(id="worker-1", name="Worker", status="active")],
                backend_health=[
                    BackendHealth(
                        name="herdr",
                        status="healthy",
                        outcome="healthy_non_empty",
                        counts={"workers": 1},
                    )
                ],
            )
            save_snapshot(Path(self.config.db_path), snapshot)

        def stop(self) -> None:
            calls.append("stop")

    def observe_cli(_config: Config) -> Any:
        raise AssertionError("CLI observation must not run in socket mode")

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            observe_initial_snapshot=observe_cli,
            event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event),
        ),
    )
    daemon.start()
    try:
        assert calls == ["start:True"]
        assert daemon.get_snapshot().workers[0].id == "worker-1"
        _assert_no_public_json_forbidden(daemon.get_health())
    finally:
        daemon.stop()

    assert "stop" in calls


def test_daemon_socket_fallback_uses_backend_health_when_snapshot_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-fallback.db"
    socket_path = tmp_path / "daemon-fallback.sock"
    config = Config(
        host_id="daemon-fallback",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )

    class FakeBackend:
        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config
            self.stop_event = stop_event
            self.health = HerdrEventBackend(config)._health_for("protocol_error")

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            return None

        def stop(self) -> None:
            return None

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda cfg, stop_event: FakeBackend(cfg, stop_event)),
    )
    daemon.start()
    try:
        snapshot = daemon.get_snapshot()
        assert snapshot.backend_health[0].status == "degraded"
        assert snapshot.backend_health[0].outcome == "protocol_error"
    finally:
        daemon.stop()
