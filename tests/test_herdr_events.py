"""Tests for the opt-in Herdr socket event backend."""

from __future__ import annotations

import json
import os
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
    HerdrEnvelopeError,
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
    assert "agent-private" not in {worker.id for worker in snapshot.workers}
    assert all("private" not in worker.id.lower() for worker in snapshot.workers)
    bindings = list_worker_bindings(Path(config.db_path), config.host_id, backend="herdr")
    assert bindings
    assert bindings[0].target_value == "agent-private"
    encoded = snapshot.to_json()
    assert "agent-private" not in encoded
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


def test_normalize_event_accepts_live_idless_data_payload_shape() -> None:
    event = normalize_event(
        {
            "event": "pane_agent_status_changed",
            "data": {"agent": "Agent One", "status": "blocked"},
        }
    )

    assert event is not None
    assert event.name == "pane.agent_status_changed"
    assert event.payload == {"agent": "Agent One", "status": "blocked"}


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


def test_backend_falls_back_to_private_pane_scoped_event_subscriptions(tmp_path: Path) -> None:
    config = _config(tmp_path, "pane-scoped-subscribe")
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0, reconnect_delay_seconds=0)

    class PaneScopedClient(_StaticClient):
        def __init__(self) -> None:
            super().__init__(
                workspaces=[{"id": "space-1", "name": "Build"}],
                panes=[
                    {
                        "pane_id": "pane-private",
                        "agent": "Agent One",
                        "workspace_id": "space-1",
                        "status": "running",
                    }
                ],
            )
            self.global_attempts = 0
            self.subscriptions: list[tuple[str, dict[str, Any]]] = []
            self.closed = 0
            self.connected = 0

        def connect(self) -> None:
            self.connected += 1

        def close(self) -> None:
            self.closed += 1

        def events_subscribe(
            self,
            event_names: Any,
            *,
            timeout: float | None = None,
            event_timeout: float | None = None,
        ) -> Any:
            self.global_attempts += 1
            raise HerdrEnvelopeError("Herdr envelope id must be a non-empty string")

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
            return SimpleNamespace(subscription_id="pane-scoped-sub")

    client = PaneScopedClient()
    backend.client_factory = lambda _config: client

    backend.run_forever()

    assert client.global_attempts == 1
    assert client.closed >= 1
    assert client.connected >= 1
    assert len(client.subscriptions) == 1
    method, params = client.subscriptions[0]
    assert method == HERDR_EVENTS_SUBSCRIBE_METHOD
    subscriptions = params["subscriptions"]
    fallback_names = set(HERDR_OFFICIAL_EVENT_NAMES) - {"pane.output_matched"}
    assert len(subscriptions) == len(fallback_names)
    assert {item["type"] for item in subscriptions} == fallback_names
    assert {item["pane_id"] for item in subscriptions} == {"pane-private"}


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


@pytest.mark.parametrize(
    "event_name",
    [
        "pane.created",
        "pane.focused",
        "pane.agent_detected",
        "pane.agent_status_changed",
    ],
)
@pytest.mark.parametrize("entity_name", ["agent", "worker"])
def test_supported_nested_agent_or_worker_canonical_fields_cannot_mint_continuity(
    tmp_path: Path,
    event_name: str,
    entity_name: str,
) -> None:
    backend = _backend(
        tmp_path,
        f"nested-no-mint-{event_name}-{entity_name}",
    )
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )
    entity = {
        "worker_id": f"public-{entity_name}",
        "agent_id": f"{entity_name}-target-secret",
        "name": "codex",
        "agent": "codex",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "status": "running",
        "agent_session": {
            "source": "compatibility-secret",
            "agent": "codex",
            "kind": "id",
            "value": "compatibility-session-secret",
        },
    }

    assert backend.queue_event_envelope(
        {
            "event": event_name,
            "payload": {entity_name: entity},
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert len(snapshot.workers) == 1
    worker = snapshot.workers[0]
    assert "stable_key" not in worker.meta
    assert "stable_key_version" not in worker.meta
    assert not backend.config.installation_key_path.exists()
    _assert_no_public_json_forbidden(json.loads(snapshot.to_json()))


def test_nested_compatibility_event_cannot_duplicate_authenticated_turn_owner(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "nested-turn-owner-conflict")
    session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "shared-session-secret",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[
                {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "running",
                }
            ],
            agents=[
                {
                    "worker_id": "public-old-owner",
                    "agent_id": "old-agent-target-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "running",
                }
            ],
        )
    )

    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {
                "agent": {
                    "worker_id": "public-compatibility-claimant",
                    "agent_id": "new-agent-target-secret",
                    "agent": "codex",
                    "agent_session": {
                        "source": "new-source-secret",
                        "agent": "codex",
                        "kind": "id",
                        "value": "shared-session-secret",
                    },
                    "status": "running",
                }
            },
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 1
    assert "stable_key" not in snapshot.workers[0].meta
    assert bindings[0].sendable is False
    assert bindings[0].reason == "ambiguous_pane_match"
    assert bindings[0].turn_target_kind is None
    assert bindings[0].turn_target_value is None


@pytest.mark.parametrize("entity_source", ["top_level", "pane"])
def test_official_pane_tuple_provenance_mints_continuity(
    tmp_path: Path,
    entity_source: str,
) -> None:
    backend = _backend(tmp_path, f"official-pane-{entity_source}")
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )
    pane = {
        "agent": "codex",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "official-terminal-secret",
        "status": "running",
        "agent_session": {
            "source": "official-source-secret",
            "agent": "codex",
            "kind": "id",
            "value": "official-session-secret",
        },
    }
    payload = pane if entity_source == "top_level" else {"pane": pane}

    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "payload": payload,
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    assert snapshot is not None
    assert len(snapshot.workers) == 1
    assert snapshot.workers[0].meta["stable_key"].startswith("wsk1_")
    assert snapshot.workers[0].meta["stable_key_version"] == 1
    assert backend.config.installation_key_path.exists()


@pytest.mark.parametrize(
    "event_name",
    ["pane.agent_detected", "pane.agent_status_changed"],
)
@pytest.mark.parametrize("key_failure", [False, True])
def test_official_event_id_churn_reuses_single_authenticated_pane_owner(
    tmp_path: Path,
    key_failure: bool,
    event_name: str,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-pane-owner-id-churn-{key_failure}-{event_name}",
    )
    old_session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    pane = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "old-terminal-secret",
        "agent": "codex",
        "agent_session": old_session,
        "status": "running",
    }
    agent = {
        "worker_id": "public-old-owner",
        "agent_id": "old-agent-target-secret",
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "old-terminal-secret",
        "agent": "codex",
        "agent_session": old_session,
        "status": "running",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane],
            agents=[agent],
        )
    )
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    before_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert before is not None
    assert len(before.workers) == len(before_bindings) == 1
    worker_id = before.workers[0].id
    stable_key = before.workers[0].meta["stable_key"]
    marker = backend.config.installation_key_marker_path.read_bytes()
    if key_failure:
        backend.config.installation_key_marker_path.unlink()

    event_payload = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "agent": {
            "worker_id": "public-new-owner",
            "agent_id": "new-agent-target-secret",
            "terminal_id": "new-terminal-secret",
            "agent": "codex",
            "agent_session": {
                "source": "new-source-secret",
                "agent": "codex",
                "kind": "id",
                "value": "new-session-secret",
            },
            "status": "working",
        },
    }
    assert backend.queue_event_envelope(
        {
            "event": event_name,
            "payload": event_payload,
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert after is not None
    assert len(after.workers) == len(bindings) == 1
    assert after.workers[0].id == worker_id
    if key_failure:
        assert after.workers == before.workers
        assert bindings == before_bindings
        assert after.backend_health[0].status == "degraded"
        assert after.backend_health[0].outcome == "continuity_unavailable"
        assert after.backend_health[0].counts == {"spaces": 1, "workers": 1}

        assert backend.queue_event_envelope(
            {
                "event": "workspace.updated",
                "event_id": f"unrelated-{event_name}",
                "payload": {
                    "workspace": {
                        "workspace_id": "wR9",
                        "name": "Build Renamed",
                    }
                },
            }
        )
        after_unrelated = latest_snapshot(backend.db_path, backend.config.host_id)
        assert after_unrelated is not None
        assert after_unrelated.backend_health[0].status == "degraded"
        assert after_unrelated.backend_health[0].outcome == "continuity_unavailable"

        after_cap = backend._mark_worker_cap_exceeded_locked(999)
        assert after_cap.backend_health[0].outcome == "continuity_unavailable"
        after_disconnect = backend._mark_unhealthy("socket_disconnected")
        assert after_disconnect.backend_health[0].outcome == "continuity_unavailable"

        backend.config.installation_key_marker_path.write_bytes(marker)
        os.chmod(backend.config.installation_key_marker_path, 0o600)
        assert backend.queue_event_envelope(
            {
                "event": event_name,
                "event_id": f"recovery-{event_name}",
                "payload": event_payload,
            }
        )
        after = latest_snapshot(backend.db_path, backend.config.host_id)
        bindings = list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        assert after is not None
        assert after.backend_health[0].status == "healthy"
        assert after.workers[0].meta["stable_key"] == stable_key
    else:
        assert after.workers[0].meta["stable_key"] == stable_key
    assert bindings[0].worker_id == worker_id
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "new-agent-target-secret"
    assert bindings[0].turn_target_kind == "codex_session_id"
    assert bindings[0].turn_target_value == "new-session-secret"
    _assert_no_public_json_forbidden(json.loads(after.to_json()))


@pytest.mark.parametrize("shared_owner", ["terminal_id", "agent_session"])
@pytest.mark.parametrize("key_failure", [False, True])
def test_incremental_shared_private_owner_fails_closed_across_canonical_panes(
    tmp_path: Path,
    shared_owner: str,
    key_failure: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-shared-{shared_owner}-owner-{key_failure}",
    )
    backend.reconcile_once(
        client=_StaticClient(workspaces=[{"id": "wR9", "name": "Build"}])
    )

    for suffix, pane_id in (("a", "wR9:pA"), ("b", "wR9:pB")):
        assert backend.queue_event_envelope(
            {
                "event": "pane.created",
                "payload": {
                    "workspace_id": "wR9",
                    "pane_id": pane_id,
                    "terminal_id": (
                        "shared-terminal-secret"
                        if shared_owner == "terminal_id"
                        else f"terminal-{suffix}-secret"
                    ),
                    "agent_id": f"agent-{suffix}-secret",
                    "agent": "codex",
                    "agent_session": {
                        "source": f"source-{suffix}-secret",
                        "agent": "codex",
                        "kind": "id",
                        "value": (
                            "shared-session-secret"
                            if shared_owner == "agent_session"
                            else f"session-{suffix}-secret"
                        ),
                    },
                    "status": "running",
                },
            }
        )
        if suffix == "a" and key_failure:
            backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "event_id": f"repeat-{shared_owner}-{key_failure}",
            "payload": {
                "workspace_id": "wR9",
                "pane_id": "wR9:pB",
                "terminal_id": (
                    "shared-terminal-secret"
                    if shared_owner == "terminal_id"
                    else "terminal-b-secret"
                ),
                "agent_id": "agent-b-secret",
                "agent": "codex",
                "agent_session": {
                    "source": "source-b-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": (
                        "shared-session-secret"
                        if shared_owner == "agent_session"
                        else "session-b-secret"
                    ),
                },
                "status": "running",
            },
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 1
    if key_failure:
        assert snapshot.workers[0].meta["stable_key"].startswith("wsk1_")
        assert snapshot.workers[0].meta["stable_key_version"] == 1
        assert snapshot.backend_health[0].status == "degraded"
        assert snapshot.backend_health[0].outcome == "continuity_unavailable"
        assert bindings[0].sendable is True
        assert bindings[0].reason is None
        assert bindings[0].turn_target_kind is not None
        assert bindings[0].turn_target_value is not None
    else:
        assert "stable_key" not in snapshot.workers[0].meta
        assert "stable_key_version" not in snapshot.workers[0].meta
        assert bindings[0].sendable is False
        assert bindings[0].reason == "ambiguous_pane_match"
        assert bindings[0].turn_target_kind is None
        assert bindings[0].turn_target_value is None


@pytest.mark.parametrize("complete_move", [False, True])
def test_move_into_owned_pane_fails_closed_for_both_owners(
    tmp_path: Path,
    complete_move: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-move-owned-destination-{complete_move}",
    )
    panes = [
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "agent": "Agent A",
            "status": "running",
        },
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pB",
            "agent": "Agent B",
            "status": "running",
        },
    ]
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=panes,
        )
    )

    payload: dict[str, Any] = {
        "previous_pane_id": "wR9:pB",
        "new_pane_id": "wR9:pA",
    }
    if complete_move:
        payload["pane"] = {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "agent": "Agent B",
            "status": "running",
        }
    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": payload,
        }
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert snapshot is not None
    assert len(snapshot.workers) == len(bindings) == 2
    assert all("stable_key" not in worker.meta for worker in snapshot.workers)
    assert all("stable_key_version" not in worker.meta for worker in snapshot.workers)
    assert all(binding.sendable is False for binding in bindings)
    assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
    assert all(binding.turn_target_kind is None for binding in bindings)
    assert all(binding.turn_target_value is None for binding in bindings)


def test_key_failure_precedes_move_conflict_mutation(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "event-move-conflict-key-failure")
    panes = [
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "agent": "Agent A",
            "status": "running",
        },
        {
            "workspace_id": "wR9",
            "pane_id": "wR9:pB",
            "agent": "Agent B",
            "status": "running",
        },
    ]
    before = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=panes,
        )
    )
    before_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    backend.config.installation_key_marker_path.unlink()

    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "previous_pane_id": "wR9:pB",
                "pane": {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "agent": "Agent B",
                    "status": "running",
                },
            },
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == before.workers
    assert after.spaces == before.spaces
    assert after.backend_health[0].status == "degraded"
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == before_bindings
    )


@pytest.mark.parametrize("key_failure", [False, True])
def test_authoritative_move_resolves_agent_targeted_source_by_previous_pane(
    tmp_path: Path,
    key_failure: bool,
) -> None:
    backend = _backend(
        tmp_path,
        f"event-move-agent-targeted-source-{key_failure}",
    )
    old_session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[
                {"id": "wR9", "name": "Source"},
                {"id": "wD2", "name": "Destination"},
            ],
            panes=[
                {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": old_session,
                    "status": "running",
                }
            ],
            agents=[
                {
                    "worker_id": "public-source-owner",
                    "agent_id": "old-agent-target-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "old-terminal-secret",
                    "agent": "codex",
                    "agent_session": old_session,
                    "status": "running",
                }
            ],
        )
    )
    before = latest_snapshot(backend.db_path, backend.config.host_id)
    before_binding = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )[0]
    assert before is not None
    worker_id = before.workers[0].id
    original_key = before.workers[0].meta["stable_key"]
    if key_failure:
        key_bytes = backend.config.installation_key_path.read_bytes()
        backend.config.installation_key_path.write_bytes(
            bytes(byte ^ 0xFF for byte in key_bytes)
        )

    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "previous_pane_id": "wR9:pA",
                "pane": {
                    "workspace_id": "wD2",
                    "pane_id": "wD2:p7",
                    "terminal_id": "new-terminal-secret",
                    "agent": "codex",
                    "agent_session": {
                        "source": "new-source-secret",
                        "agent": "codex",
                        "kind": "id",
                        "value": "new-session-secret",
                    },
                    "status": "running",
                },
            },
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert after is not None
    assert len(after.workers) == len(bindings) == 1
    assert after.workers[0].id == worker_id
    if key_failure:
        assert after.workers == before.workers
        assert after.workers[0].meta["stable_key"] == original_key
        assert bindings == [before_binding]
        assert after.backend_health[0].status == "degraded"
        assert after.backend_health[0].outcome == "continuity_unavailable"
    else:
        assert after.workers[0].meta["stable_key"] != original_key
        assert after.workers[0].space_id == "wD2"
        assert bindings[0].private_fingerprint == before_binding.private_fingerprint
        assert bindings[0].target_kind == "terminal_id"
        assert bindings[0].target_value == "new-terminal-secret"
        assert bindings[0].turn_target_kind == "codex_session_id"
        assert bindings[0].turn_target_value == "new-session-secret"


def test_reconcile_retains_authenticated_snapshot_until_installation_key_recovers(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "reconcile-key-recovery")
    client = _StaticClient(
        workspaces=[{"id": "wR9", "name": "Build"}],
        panes=[
            {
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "terminal_id": "terminal-secret",
                "agent": "codex",
                "status": "running",
            }
        ],
        agents=[
            {
                "worker_id": "public-worker",
                "agent_id": "agent-secret",
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "terminal_id": "terminal-secret",
                "agent": "codex",
                "status": "running",
            }
        ],
    )
    first = backend.reconcile_once(client=client)
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    stable_key = first.workers[0].meta["stable_key"]
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()

    degraded = backend.reconcile_once(client=client)

    assert degraded.workers == first.workers
    assert degraded.spaces == first.spaces
    assert degraded.backend_health[0].status == "degraded"
    assert degraded.backend_health[0].outcome == "continuity_unavailable"
    assert degraded.backend_health[0].counts == {"spaces": 1, "workers": 1}
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )

    previous_max_workers = backend.max_workers
    backend.max_workers = 1
    capped = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            agents=[
                {"worker_id": "agent-a", "agent": "Agent A"},
                {"worker_id": "agent-b", "agent": "Agent B"},
            ],
        )
    )
    assert capped.backend_health[0].outcome == "continuity_unavailable"
    backend.max_workers = previous_max_workers

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    recovered = backend.reconcile_once(client=client)

    assert recovered.backend_health[0].status == "healthy"
    assert recovered.workers[0].meta["stable_key"] == stable_key
    recovered_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )

    unmatched = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[],
            agents=[
                {
                    "worker_id": "public-worker",
                    "agent_id": "agent-secret",
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pA",
                    "terminal_id": "terminal-secret",
                    "agent": "codex",
                    "status": "running",
                }
            ],
        )
    )

    assert unmatched.workers == recovered.workers
    assert unmatched.spaces == recovered.spaces
    assert unmatched.backend_health[0].status == "degraded"
    assert unmatched.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == recovered_bindings
    )

    recovered_again = backend.reconcile_once(client=client)
    assert recovered_again.backend_health[0].status == "healthy"
    assert recovered_again.workers[0].meta["stable_key"] == stable_key


def test_incomplete_pane_event_does_not_clear_continuity_failure(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "incomplete-pane-key-recovery")
    pane = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-secret",
        "agent": "codex",
        "status": "running",
    }
    backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane],
        )
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {
            "event": "pane.focused",
            "payload": {"pane": pane},
        }
    )
    failed = latest_snapshot(backend.db_path, backend.config.host_id)
    assert failed is not None
    assert failed.backend_health[0].outcome == "continuity_unavailable"

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    assert backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "event_id": "incomplete-close-after-key-recovery",
            "payload": {"pane": {"pane_id": "wR9:pA"}},
        }
    )

    incomplete = latest_snapshot(backend.db_path, backend.config.host_id)
    assert incomplete is not None
    assert incomplete.backend_health[0].status == "degraded"
    assert incomplete.backend_health[0].outcome == "continuity_unavailable"


def test_over_cap_authenticated_retry_does_not_clear_continuity_failure(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "over-cap-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    backend.max_workers = 1
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "event_id": "over-cap-key-loss",
            "payload": {"pane": pane_b},
        }
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "event_id": "over-cap-key-retry",
            "payload": {"pane": pane_b},
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_conflicting_close_does_not_revalidate_continuity(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "conflicting-close-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "terminal_id": "terminal-b-secret",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {
            "event": "pane.focused",
            "event_id": "conflicting-close-key-loss",
            "payload": {"pane": pane_a},
        }
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    conflicting_close = {
        **pane_a,
        "terminal_id": pane_b["terminal_id"],
    }
    assert backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "event_id": "conflicting-close-retry",
            "payload": {"pane": conflicting_close},
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_conflicting_upsert_preserves_latched_authenticated_state(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "conflicting-upsert-key-recovery")
    pane_a = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "agent": "Agent A",
        "status": "running",
    }
    pane_b = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pB",
        "terminal_id": "terminal-b-secret",
        "agent": "Agent B",
        "status": "running",
    }
    first = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build"}],
            panes=[pane_a, pane_b],
        )
    )
    first_bindings = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    marker = backend.config.installation_key_marker_path.read_bytes()
    backend.config.installation_key_marker_path.unlink()
    assert backend.queue_event_envelope(
        {
            "event": "pane.focused",
            "event_id": "conflicting-upsert-key-loss",
            "payload": {"pane": pane_a},
        }
    )

    backend.config.installation_key_marker_path.write_bytes(marker)
    os.chmod(backend.config.installation_key_marker_path, 0o600)
    conflicting_pane = {
        **pane_a,
        "terminal_id": pane_b["terminal_id"],
        "status": "blocked",
    }
    assert backend.queue_event_envelope(
        {
            "event": "pane.focused",
            "event_id": "conflicting-upsert-retry",
            "payload": {"pane": conflicting_pane},
        }
    )

    after = latest_snapshot(backend.db_path, backend.config.host_id)
    assert after is not None
    assert after.workers == first.workers
    assert after.backend_health[0].status == "degraded"
    assert after.backend_health[0].outcome == "continuity_unavailable"
    assert (
        list_worker_bindings(
            backend.db_path,
            backend.config.host_id,
            backend="herdr",
        )
        == first_bindings
    )


def test_official_pane_event_generic_id_remains_private_binding_only(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-id-private")
    backend.reconcile_once(client=_StaticClient(workspaces=[{"id": "space-1", "name": "Build"}]))

    assert (
        backend.queue_event_envelope(
            {
                "event": "pane.created",
                "payload": {
                    "id": "pane-secret",
                    "agent": "Agent Two",
                    "workspace_id": "space-1",
                    "status": "running",
                },
            }
        )
        is True
    )

    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
    bindings = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    assert snapshot is not None
    public_json = snapshot.to_json()
    assert "pane-secret" not in public_json
    assert {worker.id for worker in snapshot.workers} == {"Agent Two"}
    assert bindings[0].target_kind == "pane_id"
    assert bindings[0].target_value == "pane-secret"
    _assert_no_public_json_forbidden(json.loads(public_json))


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

def test_pane_exited_closes_worker_and_expires_matching_binding(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "pane-exited")
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope({"event": "pane.exited", "payload": {"pane_id": "pane-1"}})

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
    assert expired[0].reason == "pane_exited"


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


def test_worker_cap_exceeded_preserves_previous_authoritative_snapshot(tmp_path: Path) -> None:
    config = Config(
        host_id="worker-cap",
        data_dir=tmp_path,
        db_path=tmp_path / "worker-cap.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    capped = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "space-1", "name": "Build"}],
            panes=[
                {"pane_id": "pane-1", "agent": "Agent One", "workspace_id": "space-1"},
                {"pane_id": "pane-2", "agent": "Agent Two", "workspace_id": "space-1"},
            ],
        )
    )
    latest = latest_snapshot(backend.db_path, backend.config.host_id)

    assert latest is not None
    assert capped.content_fingerprint == latest.content_fingerprint
    assert [worker.name for worker in capped.workers] == ["Agent One"]
    assert capped.backend_health[0].status == "degraded"
    assert capped.backend_health[0].outcome == "worker_cap_exceeded"
    assert list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
    _assert_no_public_json_forbidden(json.loads(capped.to_json()))


def test_output_excerpt_limit_bounds_public_worker_summary(tmp_path: Path) -> None:
    config = Config(
        host_id="output-excerpt",
        data_dir=tmp_path,
        db_path=tmp_path / "output-excerpt.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        output_excerpt_chars=12,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    long_summary = "x" * 40

    snapshot = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "space-1", "name": "Build"}],
            panes=[
                {
                    "pane_id": "pane-1",
                    "agent": "Agent One",
                    "workspace_id": "space-1",
                    "description": long_summary,
                }
            ],
        )
    )
    binding = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")[0]

    assert snapshot.workers[0].summary == "xxxxxxxxx..."
    assert len(snapshot.workers[0].summary or "") == 12
    assert latest_snapshot(backend.db_path, backend.config.host_id).workers[0].summary == "xxxxxxxxx..."
    assert binding.worker_fingerprint == snapshot.workers[0].fingerprint
    assert long_summary not in snapshot.to_json()


def test_incremental_event_over_worker_cap_is_ignored_with_degraded_health(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {
                "agent": {
                    "agent_id": "agent-2",
                    "name": "Agent Two",
                    "workspace_id": "space-1",
                    "pane_id": "pane-2",
                }
            },
        }
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert [worker.name for worker in snapshot.workers] == ["Agent One"]
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


def test_closed_worker_reactivation_over_worker_cap_is_ignored(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap-reactivate",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap-reactivate.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    backend.reconcile_once(client=_initial_pane_client())

    backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "payload": {"pane": {"pane_id": "pane-1", "agent": "Agent One", "workspace_id": "space-1"}},
        }
    )
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

    before_reactivation = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before_reactivation is not None
    assert {worker.name: worker.status for worker in before_reactivation.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }

    backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {
                "agent": {
                    "agent": "Agent One",
                    "workspace_id": "space-1",
                    "pane_id": "pane-3",
                    "status": "running",
                }
            },
        }
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert {worker.name: worker.status for worker in snapshot.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


def test_pane_moved_reactivation_over_worker_cap_is_ignored(tmp_path: Path) -> None:
    config = Config(
        host_id="event-worker-cap-moved-reactivate",
        data_dir=tmp_path,
        db_path=tmp_path / "event-worker-cap-moved-reactivate.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
        max_workers=1,
    )
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    first = backend.reconcile_once(client=_initial_pane_client())
    first_worker = first.workers[0]
    closed_worker = Worker(
        id=first_worker.id,
        name=first_worker.name,
        status="closed",
        space_id=first_worker.space_id,
        meta=first_worker.meta,
        last_seen_at=first_worker.last_seen_at,
        summary=first_worker.summary,
        backend_target=first_worker.backend_target,
    )
    backend._workers[closed_worker.id] = closed_worker
    save_snapshot(
        backend.db_path,
        project_from_observations(
            backend.config,
            spaces=first.spaces,
            workers=[closed_worker],
            backend_health=first.backend_health,
        ),
    )

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
    before_move = latest_snapshot(backend.db_path, backend.config.host_id)
    assert before_move is not None
    assert {worker.name: worker.status for worker in before_move.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }

    backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "old_pane_id": "pane-1",
                "pane_id": "pane-3",
                "agent": "Agent One",
                "workspace_id": "space-1",
                "status": "running",
            },
        }
    )
    snapshot = latest_snapshot(backend.db_path, backend.config.host_id)

    assert snapshot is not None
    assert {worker.name: worker.status for worker in snapshot.workers} == {
        "Agent One": "closed",
        "Agent Two": "active",
    }
    assert snapshot.backend_health[0].status == "degraded"
    assert snapshot.backend_health[0].outcome == "worker_cap_exceeded"


def test_periodic_reconcile_uses_config_and_zero_disables_it(tmp_path: Path) -> None:
    disabled_config = Config(
        host_id="periodic-disabled",
        data_dir=tmp_path,
        db_path=tmp_path / "periodic-disabled.db",
        herdr_backend="socket",
        reconcile_interval_seconds=0,
    )
    init_store(Path(disabled_config.db_path))
    disabled = HerdrEventBackend(disabled_config, debounce_seconds=0)
    disabled_client = _initial_pane_client()
    disabled._next_reconcile_monotonic = time.monotonic() - 1
    disabled._run_periodic_reconcile_if_due(disabled_client)

    enabled_config = Config(
        host_id="periodic-enabled",
        data_dir=tmp_path,
        db_path=tmp_path / "periodic-enabled.db",
        herdr_backend="socket",
        reconcile_interval_seconds=0.001,
    )
    init_store(Path(enabled_config.db_path))
    enabled = HerdrEventBackend(enabled_config, debounce_seconds=0)
    enabled_client = _initial_pane_client()
    enabled._next_reconcile_monotonic = time.monotonic() - 1
    enabled._run_periodic_reconcile_if_due(enabled_client)

    assert disabled_client.calls == []
    assert enabled_client.calls == ["workspace.list", "tab.list", "pane.list", "agent.list"]
    assert enabled.operational_status["last_reconcile_at"] is not None


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


def test_status_event_with_pane_id_only_updates_bound_worker_not_a_phantom(tmp_path: Path) -> None:
    """Regression: status events that only carry a pane id must resolve through
    the binding turn target instead of inserting a duplicate re-lettered worker
    that freezes the real worker's status (the 'stuck working icon' bug)."""
    backend = _backend(tmp_path, "phantom-host")
    client = _StaticClient(
        workspaces=[{"id": "space-1", "name": "Build", "status": "active"}],
        panes=[
            {
                "pane_id": "w1:p1",
                "terminal_id": "term-1",
                "agent": "claude",
                "workspace_id": "space-1",
                "agent_status": "working",
            },
            {
                "pane_id": "w1:p2",
                "terminal_id": "term-2",
                "agent": "claude",
                "workspace_id": "space-1",
                "agent_status": "working",
            },
        ],
        agents=[],
    )
    backend.reconcile_once(client=client)
    snapshot = latest_snapshot(Path(backend.db_path), backend.config.host_id)
    assert snapshot is not None
    ids_before = sorted(worker.id for worker in snapshot.workers)
    assert ids_before == ["claude-1", "claude-2"]

    event = normalize_event(
        {
            "event": "pane.agent_status_changed",
            "payload": {"pane": {"pane_id": "w1:p2", "agent": "claude", "status": "idle"}},
        }
    )
    assert event is not None
    assert backend._apply_event(event) is True
    backend._persist_projection_locked() if hasattr(backend, "_persist_projection_locked") else None
    workers = backend._workers
    assert sorted(workers) == ["claude-1", "claude-2"], f"phantom worker inserted: {sorted(workers)}"
    by_target = {}
    for binding in backend._bindings.values():
        by_target[binding.target_value] = binding.worker_id
    idle_worker_id = by_target["term-2"]
    assert workers[idle_worker_id].status in {"idle", "done"}


def test_pane_id_only_status_event_resolves_codex_binding_via_pane_terminal_map(tmp_path: Path) -> None:
    """Regression: codex bindings' turn target is a session id, so pane-id-only
    status events must resolve through the pane->terminal map remembered from
    reconcile instead of inserting a phantom bare 'codex' worker."""
    backend = _backend(tmp_path, "codex-phantom-host")
    client = _StaticClient(
        workspaces=[{"id": "wX8", "name": "projectx", "status": "active"}],
        panes=[
            {
                "pane_id": "wX8:p1",
                "terminal_id": "term-ctx",
                "agent": "codex",
                "agent_session": {"agent": "codex", "kind": "id", "value": "019f-session"},
                "workspace_id": "wX8",
                "agent_status": "working",
            }
        ],
        agents=[],
    )
    backend.reconcile_once(client=client)
    assert sorted(backend._workers) == ["codex"]
    binding = next(iter(backend._bindings.values()))
    assert binding.turn_target_kind == "codex_session_id"
    assert backend._pane_terminals == {"wX8:p1": "term-ctx"}

    event = normalize_event(
        {
            "event": "pane.agent_status_changed",
            "payload": {"pane_id": "wX8:p1", "workspace_id": "wX8", "agent_status": "idle"},
        }
    )
    assert event is not None
    assert backend._apply_event(event) is True
    assert sorted(backend._workers) == ["codex"], f"phantom inserted: {sorted(backend._workers)}"
    assert backend._workers["codex"].status in {"idle", "done"}
    updated_binding = next(iter(backend._bindings.values()))
    assert updated_binding.private_fingerprint == binding.private_fingerprint
    assert updated_binding.target_kind == binding.target_kind == "terminal_id"
    assert updated_binding.target_value == binding.target_value == "term-ctx"
    assert updated_binding.turn_target_kind == binding.turn_target_kind == "codex_session_id"
    assert updated_binding.turn_target_value == binding.turn_target_value == "019f-session"


def test_reconcile_drops_unbound_missing_workers_but_keeps_bound_closed(tmp_path: Path) -> None:
    backend = _backend(tmp_path, "phantom-aging-host")
    worker_bound = Worker(id="codex-1", name="codex", status="active", space_id="wX8")
    worker_phantom = Worker(id="codex", name="codex", status="working", space_id="wX8")
    merged = backend._workers_with_closed_missing(
        [worker_bound, worker_phantom],
        [],
        bound_worker_ids={"codex-1"},
    )
    ids = {worker.id: worker.status for worker in merged}
    assert "codex" not in ids, "unbound phantom must be dropped, not carried as closed"
    assert ids.get("codex-1") == "closed"


def test_nested_agent_pane_claim_cannot_poison_terminal_close_matching(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "pane-terminal-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert backend._pane_terminals == {"P1": "T1"}

    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {
                "agent": {
                    "worker_id": "nested-agent-claimant",
                    "pane_id": "Pfake",
                    "terminal_id": "T1",
                    "agent": "codex",
                    "status": "working",
                }
            },
        }
    )
    assert backend._pane_terminals == {"P1": "T1"}
    assert backend._pane_owners == {"P1": {worker.id}}

    assert backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "payload": {"pane_id": "Pfake"},
        }
    )

    current = backend._workers[worker.id]
    assert current.status == worker.status
    assert current.status != "closed"
    assert "Pfake" not in backend._pane_terminals
    assert backend._pane_owners == {"P1": {worker.id}}
    stored = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(stored) == 1
    assert stored[0].private_fingerprint == binding.private_fingerprint
    assert stored[0].sendable is True


def test_reconcile_maps_only_accepted_pane_info_rows(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "reconcile-pane-map-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                },
                {
                    "pane_id": "Pfake",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent_status": "working",
                },
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert backend._pane_terminals == {"P1": "T1"}
    assert backend._pane_owners == {"P1": {worker.id}}

    assert backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "payload": {"pane_id": "Pfake"},
        }
    )

    assert backend._workers[worker.id].status == worker.status
    assert backend._workers[worker.id].status != "closed"
    stored = list_worker_bindings(
        backend.db_path,
        backend.config.host_id,
        backend="herdr",
    )
    assert len(stored) == 1
    assert stored[0].private_fingerprint == binding.private_fingerprint


@pytest.mark.parametrize(
    ("source_field", "source_value"),
    [
        ("previous_pane_id", "P1"),
        ("previous_terminal_id", "T1"),
    ],
)
def test_accepted_move_removes_source_pane_terminal_alias_before_stale_close(
    tmp_path: Path,
    source_field: str,
    source_value: str,
) -> None:
    backend = _backend(tmp_path, "moved-pane-map-provenance")
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "W1", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "P1",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]

    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                source_field: source_value,
                "pane": {
                    "pane_id": "P2",
                    "terminal_id": "T1",
                    "workspace_id": "W1",
                    "agent": "codex",
                    "agent_status": "working",
                },
            },
        }
    )
    assert backend._pane_terminals == {"P2": "T1"}
    assert backend._pane_owners == {"P2": {worker.id}}

    assert backend.queue_event_envelope(
        {
            "event": "pane.closed",
            "payload": {"pane_id": "P1"},
        }
    )

    assert backend._workers[worker.id].status == worker.status
    assert backend._workers[worker.id].status != "closed"
    assert backend._pane_terminals == {"P2": "T1"}


def test_pane_only_status_preserves_unobserved_owner_aliases_for_later_conflict(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "status-owner-alias-provenance")
    session = {
        "source": "old-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "old-session-secret",
    }
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "wR9:pA",
                    "terminal_id": "shared-terminal-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "agent_status": "working",
                }
            ],
            agents=[
                {
                    "worker_id": "public-owner",
                    "agent_id": "old-agent-target-secret",
                    "pane_id": "wR9:pA",
                    "terminal_id": "shared-terminal-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "agent_status": "working",
                }
            ],
        )
    )
    worker = initial.workers[0]
    binding = next(iter(backend._bindings.values()))
    assert binding.target_kind == "agent_id"
    assert binding.turn_target_kind == "codex_session_id"

    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_status_changed",
            "payload": {
                "workspace_id": "wR9",
                "pane_id": "wR9:pA",
                "status": "idle",
            },
        }
    )
    status_binding = next(iter(backend._bindings.values()))
    assert status_binding.private_fingerprint == binding.private_fingerprint
    assert status_binding.target_kind == binding.target_kind
    assert status_binding.target_value == binding.target_value
    assert status_binding.turn_target_kind == binding.turn_target_kind
    assert status_binding.turn_target_value == binding.turn_target_value
    assert backend._terminal_owners == {
        "shared-terminal-secret": {worker.id},
    }
    assert backend._session_owners == {
        "old-session-secret": {worker.id},
    }

    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "payload": {
                "pane": {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pB",
                    "terminal_id": "shared-terminal-secret",
                    "agent_id": "new-agent-target-secret",
                    "agent": "codex",
                    "agent_session": {
                        "source": "new-source-secret",
                        "agent": "codex",
                        "kind": "id",
                        "value": "new-session-secret",
                    },
                    "status": "working",
                }
            },
        }
    )

    assert set(backend._workers) == {worker.id}
    conflicted = next(iter(backend._bindings.values()))
    assert conflicted.worker_id == worker.id
    assert conflicted.sendable is False
    assert conflicted.reason == "ambiguous_pane_match"
    assert "wR9:pB" not in backend._pane_terminals


def test_nested_compatibility_replay_cannot_rehabilitate_ambiguous_binding(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "compatibility-ambiguity-provenance")
    session = {
        "source": "shared-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "shared-session-secret",
    }
    original_agent = {
        "worker_id": "public-owner-a",
        "agent_id": "agent-target-a-secret",
        "pane_id": "wR9:pA",
        "terminal_id": "terminal-a-secret",
        "workspace_id": "wR9",
        "agent": "codex",
        "agent_session": session,
        "status": "working",
    }
    initial = backend.reconcile_once(
        client=_StaticClient(
            workspaces=[{"id": "wR9", "name": "Build", "status": "active"}],
            panes=[
                {
                    "pane_id": "wR9:pA",
                    "terminal_id": "terminal-a-secret",
                    "workspace_id": "wR9",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "working",
                }
            ],
            agents=[original_agent],
        )
    )
    worker = initial.workers[0]

    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "payload": {
                "pane": {
                    "workspace_id": "wR9",
                    "pane_id": "wR9:pB",
                    "terminal_id": "terminal-b-secret",
                    "agent_id": "agent-target-b-secret",
                    "agent": "codex",
                    "agent_session": session,
                    "status": "working",
                }
            },
        }
    )
    ambiguous = next(iter(backend._bindings.values()))
    assert ambiguous.worker_id == worker.id
    assert ambiguous.target_value == "agent-target-a-secret"
    assert ambiguous.sendable is False
    assert ambiguous.reason == "ambiguous_pane_match"
    assert ambiguous.turn_target_kind is None
    assert ambiguous.turn_target_value is None
    assert backend._workers[worker.id].backend_target == ambiguous.backend_target()

    assert backend.queue_event_envelope(
        {
            "event": "pane.agent_detected",
            "payload": {"agent": original_agent},
        }
    )

    replayed = next(iter(backend._bindings.values()))
    assert replayed.private_fingerprint == ambiguous.private_fingerprint
    assert replayed.target_kind == ambiguous.target_kind
    assert replayed.target_value == ambiguous.target_value
    assert replayed.sendable is False
    assert replayed.reason == "ambiguous_pane_match"
    assert replayed.turn_target_kind is None
    assert replayed.turn_target_value is None
    assert backend._workers[worker.id].backend_target == replayed.backend_target()
    assert backend._pane_terminals == {
        "wR9:pA": "terminal-a-secret",
    }
