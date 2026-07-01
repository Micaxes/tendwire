"""Tests for the PR7 Tendwire daemon skeleton and local JSON API."""

from __future__ import annotations

import io
import json
import sqlite3
import socket
import threading
import time
from pathlib import Path
from typing import Any

from tendwire.cli import main
from tendwire.config import Config
from tendwire.core.commands import STATUS_ACCEPTED, STATUS_INVALID_REQUEST, CommandEnvelope
from tendwire.core.models import (
    AttentionSignal,
    BackendHealth,
    Snapshot,
    SuggestedAction,
    Worker,
    WorkerBinding,
)
from tendwire.core.projector import project_from_raw
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.daemon_api import DaemonAPIClient, TendwireDaemonAPI, UnixSocketJSONServer
from tendwire.store.sqlite import (
    attention_payload_from_store,
    get_command_receipt,
    init_store,
    latest_snapshot,
    save_snapshot,
    upsert_worker_bindings,
)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "process_id",
    "pane_id",
    "terminal_id",
    "backend_target",
    "session_id",
    "private",
    "private_binding",
    "private_fingerprint",
    "route",
    "delivery",
    "connector",
    "command",
    "raw_command",
    "chat_id",
    "topic_id",
    "message_id",
    "token",
    "secret",
    "password",
    "credentials",
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


def _public_snapshot() -> Snapshot:
    return Snapshot(
        host_id="daemon-host",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="waiting",
                summary="approval required before continuing",
                meta={
                    "safe": "kept",
                    "tty": "sentinel-private-tty",
                    "pane_id": "sentinel-private-pane",
                    "connectorId": "sentinel-private-connector",
                    "authToken": "sentinel-private-token",
                },
                backend_target={
                    "kind": "agent_id",
                    "value": "sentinel-private-target",
                    "sendable": True,
                },
            )
        ],
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="approval required before continuing",
                source="worker:worker-1",
                updated_at="2026-01-01T00:00:00+00:00",
                suggested_actions=[
                    SuggestedAction(
                        action_id="approve",
                        label="Approve",
                        tendwire_action="approve",
                        params={"safe": "kept", "message_id": "sentinel-private-message"},
                    )
                ],
                meta={"needs_human": True, "space_id": "space-1", "private": "sentinel-private-meta"},
            )
        ],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at="2026-01-01T00:00:00+00:00",
                message="healthy",
                counts={"workers": 1},
            )
        ],
    )


def test_daemon_api_required_methods_are_public_safe() -> None:
    snapshot = _public_snapshot()
    calls: list[dict[str, Any]] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {
            "schema_version": 1,
            "status": "ok",
            "host_id": snapshot.host_id,
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        },
        submit_command=lambda params: calls.append(dict(params))
        or CommandEnvelope.error(
            None,
            {
                "code": STATUS_INVALID_REQUEST,
                "message": "bad command",
                "details": {"fields": ["$.tty"]},
            },
        ),
    )

    for method in ("ping", "health.get", "snapshot.get", "attention.list", "turn.list", "pending.list"):
        response = api.dispatch({"method": method})
        assert response["ok"] is True
        encoded = json.dumps(response)
        assert "sentinel-private" not in encoded
        _assert_no_public_json_forbidden(response)

    command_response = api.dispatch(
        {
            "method": "command.submit",
            "params": {
                "schema_version": 1,
                "action": "noop",
                "tty": "sentinel-private-tty",
            },
        }
    )
    assert command_response["ok"] is True
    assert command_response["result"]["ok"] is False
    assert calls[0]["tty"] == "sentinel-private-tty"
    assert "sentinel-private" not in json.dumps(command_response)
    _assert_no_public_json_forbidden(command_response)


def test_daemon_api_protocol_errors_do_not_echo_private_request_names() -> None:
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok", "host_id": "daemon-host"},
        submit_command=lambda params: CommandEnvelope.error(
            None,
            {
                "code": STATUS_INVALID_REQUEST,
                "message": "bad command",
                "details": {},
            },
        ),
    )

    unknown_field = api.dispatch(
        {
            "method": "ping",
            "telegram.bot.token": "sentinel-private-field",
            "backend.target": "sentinel-private-target",
        }
    )
    unknown_method = api.dispatch({"method": "telegram.bot.token"})
    unsafe_id = api.dispatch({"id": "telegram.bot.token", "method": "telegram.bot.token"})
    unsafe_object_id = api.dispatch(
        {
            "id": {"backend.target": "sentinel-private-id"},
            "method": "telegram.bot.token",
        }
    )
    unsafe_prefixed_ids = {
        private_id: api.dispatch({"id": private_id, "method": "telegram.bot.token"})
        for private_id in ("x-api_key", "my-api-key", "credentials", "my-credentials")
    }
    safe_id = api.dispatch({"id": "req-123_ok.1", "method": "ping"})

    unknown_field_encoded = json.dumps(unknown_field, sort_keys=True).lower()
    unknown_method_encoded = json.dumps(unknown_method, sort_keys=True).lower()
    unsafe_id_encoded = json.dumps(unsafe_id, sort_keys=True).lower()
    unsafe_object_id_encoded = json.dumps(unsafe_object_id, sort_keys=True).lower()

    assert unknown_field["ok"] is False
    assert unknown_field["error"]["message"] == "request contains unknown top-level fields"
    assert unknown_field["error"]["details"] == {"field_count": 2}
    assert "sentinel-private" not in unknown_field_encoded
    assert "telegram" not in unknown_field_encoded
    assert "bot.token" not in unknown_field_encoded
    assert "backend.target" not in unknown_field_encoded

    assert unknown_method["ok"] is False
    assert unknown_method["error"]["message"] == "unknown method"
    assert "telegram" not in unknown_method_encoded
    assert "bot.token" not in unknown_method_encoded
    assert unsafe_id["ok"] is False
    assert "id" not in unsafe_id
    assert "telegram" not in unsafe_id_encoded
    assert "bot.token" not in unsafe_id_encoded
    assert unsafe_object_id["ok"] is False
    assert "id" not in unsafe_object_id
    assert "sentinel-private" not in unsafe_object_id_encoded
    assert "backend.target" not in unsafe_object_id_encoded
    for private_id, response in unsafe_prefixed_ids.items():
        encoded = json.dumps(response, sort_keys=True).lower()
        assert response["ok"] is False
        assert "id" not in response
        assert private_id.lower() not in encoded
    assert safe_id["ok"] is True
    assert safe_id["id"] == "req-123_ok.1"
    _assert_no_public_json_forbidden(unknown_field)
    _assert_no_public_json_forbidden(unknown_method)
    _assert_no_public_json_forbidden(unsafe_id)
    _assert_no_public_json_forbidden(unsafe_object_id)
    for response in unsafe_prefixed_ids.values():
        _assert_no_public_json_forbidden(response)
    _assert_no_public_json_forbidden(safe_id)


def test_daemon_api_attention_list_uses_store_lifecycle_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "attention-api.db"
    config = Config(host_id="daemon-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "blocked",
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    save_snapshot(db_path, snapshot)
    daemon = TendwireDaemon(config)
    api = TendwireDaemonAPI(
        get_snapshot=daemon.get_snapshot,
        get_health=daemon.get_health,
        submit_command=daemon.submit_command,
        get_attention=daemon.get_attention,
    )

    response = api.dispatch({"method": "attention.list"})
    payload = response["result"]

    assert response["ok"] is True
    assert payload["host_id"] == "daemon-host"
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == snapshot.updated_at
    assert payload["attention"][0]["last_seen_at"] == snapshot.updated_at
    assert payload["attention"][0]["signal_count"] == 1
    assert attention_payload_from_store(db_path, "daemon-host") == payload
    assert "sentinel-private" not in json.dumps(response, sort_keys=True)
    _assert_no_public_json_forbidden(response)


def test_daemon_health_exposes_public_operational_status_without_private_values(tmp_path: Path) -> None:
    db_path = tmp_path / "health.db"
    config = Config(
        host_id="health-host",
        db_path=db_path,
        event_debounce_seconds=0.2,
        reconcile_interval_seconds=0,
        event_retention_days=3,
        output_excerpt_chars=80,
        max_workers=8,
        max_outbox_attempts=4,
        connector_claim_ttl_seconds=33,
    )
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "backend_target": {"pane_id": "sentinel-private-pane"},
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "health-host",
                "attention",
                "job-1",
                "queued",
                '{"safe":"kept"}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    health = TendwireDaemon(config).get_health()
    encoded = json.dumps(health)

    assert health["status"] == "ok"
    assert health["daemon"]["started_at"]
    assert health["store"]["counts"]["snapshots"] == 1
    assert health["store"]["outbox"]["pending"] == 1
    assert health["limits"] == {
        "event_debounce_seconds": 0.2,
        "reconcile_interval_seconds": 0,
        "event_retention_days": 3,
        "output_excerpt_chars": 80,
        "max_workers": 8,
        "max_outbox_attempts": 4,
        "outbox_claim_ttl_seconds": 33,
    }
    assert "health.db" not in encoded
    assert str(tmp_path) not in encoded
    assert "sentinel-private" not in encoded
    _assert_no_public_json_forbidden(health)


def test_daemon_starts_observes_persists_serves_and_removes_socket(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon.db"
    socket_path = tmp_path / "daemon.sock"
    config = Config(host_id="daemon-host", data_dir=tmp_path, db_path=db_path, socket_path=socket_path)

    def observe(config: Config) -> Snapshot:
        snapshot = project_from_raw(
            config,
            workers=[{"id": "worker-1", "name": "Worker One", "status": "active"}],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "observed_at": "2026-01-01T00:00:00+00:00",
                    "counts": {"workers": 1},
                }
            ],
        )
        save_snapshot(db_path, snapshot)
        return snapshot

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(observe_initial_snapshot=observe),
    )
    daemon.start()
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        assert latest_snapshot(db_path, "daemon-host") is not None
        ping = DaemonAPIClient(socket_path).request("ping")
        snapshot_response = DaemonAPIClient(socket_path).request("snapshot.get")
        health_response = DaemonAPIClient(socket_path).request("health.get")

        assert ping["ok"] is True
        assert ping["result"]["pong"] is True
        assert snapshot_response["result"]["host_id"] == "daemon-host"
        assert snapshot_response["result"]["workers"][0]["id"] == "worker-1"
        assert health_response["result"]["store"]["status"] == "healthy"
    finally:
        daemon.stop()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert not socket_path.exists()


def test_daemon_server_survives_client_disconnect_during_response(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon.sock"
    request_seen = threading.Event()
    allow_response = threading.Event()

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        if request.get("method") == "large.response":
            request_seen.set()
            allow_response.wait(timeout=2)
            return {"ok": True, "result": {"payload": "x" * 5_000_000}}
        return {"ok": True, "result": {"pong": True}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.05,
        client_timeout_seconds=2,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.connect(str(socket_path))
            conn.sendall(b'{"method":"large.response"}\n')
            assert request_seen.wait(timeout=2)
        allow_response.set()

        deadline = time.monotonic() + 2
        response: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                response = DaemonAPIClient(socket_path, timeout_seconds=1).request("ping")
                break
            except Exception:
                time.sleep(0.01)

        assert response is not None
        assert response["ok"] is True
        assert response["result"]["pong"] is True
        assert thread.is_alive()
    finally:
        server.close()
        thread.join(timeout=2)

    assert not thread.is_alive()


def test_daemon_command_submit_uses_existing_receipt_idempotency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "commands.db"
    socket_path = tmp_path / "commands.sock"
    config = Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )
    init_store(db_path)
    calls: list[dict[str, Any]] = []
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = WorkerBinding(
        host_id="cmd-host",
        worker_id="w-1",
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="private-binding",
    )

    class FakeHealth:
        def to_backend_health(self) -> BackendHealth:
            return BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at="2026-01-01T00:00:00+00:00",
                counts={"workers": 1},
            )

    class FakeEventBackend:
        health = FakeHealth()

        def __init__(self, config: Config, stop_event: threading.Event) -> None:
            self.config = config

        def start(self, *, wait_for_reconcile: bool = True) -> None:
            snapshot = Snapshot(
                host_id="cmd-host",
                updated_at="2026-01-01T00:00:00+00:00",
                workers=[worker],
                backend_health=[self.health.to_backend_health()],
            )
            save_snapshot(db_path, snapshot)
            upsert_worker_bindings(db_path, [binding])

        def stop(self) -> None:
            return None

    class FakeHerdrSocketClient:
        def connect(self) -> "FakeHerdrSocketClient":
            return self

        def agent_send(self, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
            calls.append(dict(params))
            return {"accepted": True}

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "tendwire.command_submission._default_socket_client_factory",
        lambda config: FakeHerdrSocketClient(),
    )

    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda config, stop_event: FakeEventBackend(config, stop_event)),
    )
    daemon.start()
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        client = DaemonAPIClient(socket_path)
        request = {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "req-1",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
        first = client.request("command.submit", request)
        second = client.request("command.submit", request)

        assert first["ok"] is True
        assert first["result"]["status"] == STATUS_ACCEPTED
        assert second["ok"] is True
        assert second["result"]["status"] == STATUS_ACCEPTED
        assert calls == [{"agent_id": "agent-private", "text": "hello"}]
        assert get_command_receipt(db_path, "cmd-host", "req-1", "send_instruction") is not None
        assert "agent-private" not in json.dumps(first)
        _assert_no_public_json_forbidden(first)
    finally:
        daemon.stop()
        thread.join(timeout=2)


def test_daemon_command_submit_rejects_blank_request_id_before_mutation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "invalid-request-id.db"
    config = Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=db_path,
        herdr_backend="socket",
    )
    init_store(db_path)
    calls: list[str] = []

    def guarded_socket_factory(config: Config) -> Any:
        calls.append("socket")
        raise AssertionError("invalid request_id must not construct Herdr socket client")

    monkeypatch.setattr(
        "tendwire.command_submission._default_socket_client_factory",
        guarded_socket_factory,
    )
    daemon = TendwireDaemon(config)
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "   \t",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }

    direct = daemon.submit_command(request)
    api = TendwireDaemonAPI(
        get_snapshot=_public_snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=daemon.submit_command,
    )
    response = api.dispatch({"method": "command.submit", "params": request})

    assert isinstance(direct, CommandEnvelope)
    assert direct.status == STATUS_INVALID_REQUEST
    assert response["ok"] is True
    assert response["result"]["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0

def test_cli_snapshot_falls_back_when_configured_socket_is_absent(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "absent.sock"

    def fake_state(config: Config) -> tuple[list[Any], list[Worker]]:
        return [], [Worker(id="fallback-worker", name="Fallback", status="active")]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", fake_state)

    code = main(
        [
            "--host-id",
            "fallback-host",
            "--socket-path",
            str(socket_path),
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["workers"][0]["id"] == "fallback-worker"


def test_cli_command_falls_back_when_configured_socket_is_stale(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "stale.sock"
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(socket_path))
    stale.close()

    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})))

    code = main(["--socket-path", str(socket_path), "command", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "noop"
