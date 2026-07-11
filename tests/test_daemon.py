"""Tests for the PR7 Tendwire daemon skeleton and local JSON API."""

from __future__ import annotations

import errno
import io
import json
import os
import socket
import sqlite3
import stat
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

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
from tendwire.daemon_api import (
    DaemonAPIClient,
    DaemonUnavailable,
    DaemonProtocolError,
    TendwireDaemonAPI,
    UnixSocketJSONServer,
    MAX_RESPONSE_BYTES,
)
from tendwire.local_state import LocalStateError, LocalStateErrorCode
from tendwire.store.sqlite import (
    SnapshotObservationContext,
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


def test_daemon_api_versions_turn_list_and_preserves_exact_content_page() -> None:
    turn_calls: list[int] = []
    page_calls: list[dict[str, Any]] = []
    page_text = "\n  " + ("α" * 20_000) + "  \r\n"
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        get_turns=lambda *, schema_version: turn_calls.append(schema_version)
        or {
            "schema_version": schema_version,
            "turns": [
                {
                    "id": "turn-public",
                    "assistant_final_text": "\n exact inline  ",
                    "content": {
                        "schema_version": 1,
                        "content_revision": "twrev1.public",
                        "known_incomplete": False,
                        "fields": {
                            "assistant_final_text": {
                                "availability": "complete",
                                "inline": True,
                            }
                        },
                    },
                }
            ],
        },
        get_turn_content=lambda params: page_calls.append(dict(params))
        or {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "turn_id": "turn-public",
            "content_revision": "twrev1.public",
            "field": "assistant_final_text",
            "availability": "complete",
            "segment_id": "twseg1.public",
            "index": 0,
            "count": 1,
            "text": page_text,
            "segment_char_length": len(page_text),
            "segment_byte_length": len(page_text.encode("utf-8")),
            "total_char_length": len(page_text),
            "total_byte_length": len(page_text.encode("utf-8")),
            "next_cursor": None,
        },
    )

    listed = api.dispatch({"method": "turn.list", "params": {"schema_version": 2}})
    page = api.dispatch(
        {
            "method": "turn.content.get",
            "params": {
                "schema_version": 1,
                "turn_id": "turn-public",
                "content_revision": "twrev1.public",
                "field": "assistant_final_text",
            },
        }
    )
    unsupported = api.dispatch({"method": "turn.list", "params": {"schema_version": 3}})

    assert listed["result"]["schema_version"] == 2
    assert listed["result"]["turns"][0]["assistant_final_text"] == "\n exact inline  "
    assert turn_calls == [2]
    assert page["result"]["text"] == page_text
    assert page_calls == [
        {
            "schema_version": 1,
            "turn_id": "turn-public",
            "content_revision": "twrev1.public",
            "field": "assistant_final_text",
        }
    ]
    assert unsupported["ok"] is False
    assert unsupported["error"]["code"] == "unsupported_schema"


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
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
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
                "observed_at": observed_at.isoformat(),
                "counts": {"workers": 1},
            }
        ],
        timestamp=observed_at,
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=observed_at.isoformat(),
        ),
    )
    escalated_at = observed_at + timedelta(seconds=1)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=[
                {
                    "id": "worker-1",
                    "name": "Worker One",
                    "status": "failed",
                    "meta": {"safe": "kept"},
                }
            ],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "observed_at": escalated_at.isoformat(),
                    "counts": {"workers": 1},
                }
            ],
            timestamp=escalated_at,
        ),
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at=escalated_at.isoformat(),
        ),
    )
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
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == observed_at.isoformat()
    assert payload["attention"][0]["last_seen_at"] == escalated_at.isoformat()
    assert payload["attention"][0]["signal_count"] == 2
    assert payload["attention"][0]["severity"] == "critical"
    assert not {
        "family_key",
        "generation",
        "first_missing_at",
        "missing_observation_count",
        "last_accepted_at",
        "last_observation_key",
        "max_notified_severity_rank",
    }.intersection(payload["attention"][0])
    assert attention_payload_from_store(db_path, "daemon-host") == payload
    assert "sentinel-private" not in json.dumps(response, sort_keys=True)
    _assert_no_public_json_forbidden(response)


def _blocked_worker(status: str) -> list[dict[str, Any]]:
    return [{"id": "worker-1", "name": "Worker One", "status": status}]


# Complete observations are the sole absence authority. Resolution requires
# two distinct misses and 120 seconds elapsed from the first accepted miss.
_HEALTHY_BACKEND = [
    {
        "name": "herdr",
        "status": "healthy",
        "outcome": "healthy_non_empty",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "counts": {"workers": 1},
    }
]


def _attention_outbox_count(db_path: Path, host_id: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE host_id = ? AND connector = 'attention'",
                (host_id,),
            ).fetchone()[0]
        )

def _complete_observation(observed_at: datetime) -> SnapshotObservationContext:
    return SnapshotObservationContext(
        authority="complete",
        observed_at=observed_at.isoformat(),
    )



def test_attention_positive_after_two_early_complete_misses_does_not_re_notify(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-flap.db"
    config = Config(host_id="flap-host", db_path=db_path)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=base,
        ),
        observation=_complete_observation(base),
    )
    assert _attention_outbox_count(db_path, "flap-host") == 1

    for offset in (30, 90):
        observed_at = base + timedelta(seconds=offset)
        save_snapshot(
            db_path,
            project_from_raw(
                config,
                workers=_blocked_worker("idle"),
                backend_health=_HEALTHY_BACKEND,
                timestamp=observed_at,
            ),
            observation=_complete_observation(observed_at),
        )
    payload = attention_payload_from_store(db_path, "flap-host")
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["lifecycle_status"] == "open"

    recurrence_at = base + timedelta(seconds=100)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=recurrence_at,
        ),
        observation=_complete_observation(recurrence_at),
    )
    assert _attention_outbox_count(db_path, "flap-host") == 1
    assert len(attention_payload_from_store(db_path, "flap-host")["attention"]) == 1
    with sqlite3.connect(str(db_path)) as conn:
        generation, missing_count = conn.execute(
            """
            SELECT generation, missing_observation_count
            FROM attention_lifecycles
            WHERE host_id = ?
            """,
            ("flap-host",),
        ).fetchone()
    assert (generation, missing_count) == (1, 0)


def test_attention_recurrence_after_two_complete_misses_re_notifies(tmp_path: Path) -> None:
    db_path = tmp_path / "attention-genuine-reopen.db"
    config = Config(host_id="reopen-host", db_path=db_path)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=base,
        ),
        observation=_complete_observation(base),
    )
    assert _attention_outbox_count(db_path, "reopen-host") == 1

    first_miss_at = base + timedelta(seconds=10)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("idle"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=first_miss_at,
        ),
        observation=_complete_observation(first_miss_at),
    )
    assert len(attention_payload_from_store(db_path, "reopen-host")["attention"]) == 1

    second_miss_at = first_miss_at + timedelta(seconds=120)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("idle"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=second_miss_at,
        ),
        observation=_complete_observation(second_miss_at),
    )
    assert attention_payload_from_store(db_path, "reopen-host")["attention"] == []

    recurrence_at = second_miss_at + timedelta(seconds=1)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=recurrence_at,
        ),
        observation=_complete_observation(recurrence_at),
    )
    assert _attention_outbox_count(db_path, "reopen-host") == 2
    assert len(attention_payload_from_store(db_path, "reopen-host")["attention"]) == 1
    with sqlite3.connect(str(db_path)) as conn:
        generation = conn.execute(
            "SELECT generation FROM attention_lifecycles WHERE host_id = ?",
            ("reopen-host",),
        ).fetchone()[0]
    assert generation == 2


def test_socket_daemon_synthesized_fallback_has_no_lifecycle_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "socket-fallback.db"
    config = Config(host_id="socket-fallback-host", db_path=db_path, herdr_backend="socket")
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=_blocked_worker("blocked"),
            backend_health=_HEALTHY_BACKEND,
            timestamp=base,
        ),
        observation=_complete_observation(base),
    )

    class _HealthyState:
        def to_backend_health(self) -> BackendHealth:
            return BackendHealth(
                name="herdr",
                status="healthy",
                outcome="empty_healthy",
                observed_at=(base + timedelta(seconds=300)).isoformat(),
            )

    class _Backend:
        health = _HealthyState()

        def start(self, *, wait_for_reconcile: bool) -> None:
            assert wait_for_reconcile is True

        def stop(self) -> None:
            pass

    backend = _Backend()
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda _config, _stop_event: backend),
    )
    monkeypatch.setattr("tendwire.store.sqlite.latest_snapshot", lambda _path, _host_id: None)

    fallback = daemon._start_socket_event_backend()

    assert fallback.attention == []
    assert len(attention_payload_from_store(db_path, config.host_id)["attention"]) == 1
    assert _attention_outbox_count(db_path, config.host_id) == 1
    with sqlite3.connect(str(db_path)) as conn:
        missing_count = conn.execute(
            "SELECT missing_observation_count FROM attention_lifecycles WHERE host_id = ?",
            (config.host_id,),
        ).fetchone()[0]
    assert missing_count == 0


@pytest.mark.parametrize(
    "observed_at",
    ["not-a-timestamp", "2026-01-01T00:00:00"],
)
def test_socket_daemon_fallback_drops_unordered_health_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    observed_at: str,
) -> None:
    config = Config(
        host_id="socket-fallback-invalid-time",
        db_path=tmp_path / "socket-fallback-invalid-time.db",
        herdr_backend="socket",
    )
    captured: list[SnapshotObservationContext] = []

    class _HealthState:
        def to_backend_health(self) -> BackendHealth:
            return BackendHealth(
                name="herdr",
                status="healthy",
                outcome="empty_healthy",
                observed_at=observed_at,
            )

    class _Backend:
        health = _HealthState()

        def start(self, *, wait_for_reconcile: bool) -> None:
            assert wait_for_reconcile is True

    def _capture_save(
        _db_path: Path,
        _snapshot: Snapshot,
        *,
        observation: SnapshotObservationContext,
    ) -> None:
        captured.append(observation)

    backend = _Backend()
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(event_backend_factory=lambda _config, _stop_event: backend),
    )
    monkeypatch.setattr("tendwire.store.sqlite.latest_snapshot", lambda _path, _host_id: None)
    monkeypatch.setattr("tendwire.store.sqlite.save_snapshot", _capture_save)

    daemon._start_socket_event_backend()

    assert len(captured) == 1
    assert captured[0].authority == "none"
    assert captured[0].observed_at is None


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


_UNIX_SOCKET_TEST = pytest.mark.skipif(
    os.name != "posix"
    or not sys.platform.startswith("linux")
    or not hasattr(socket, "AF_UNIX"),
    reason="Linux/POSIX Unix-socket lifecycle contract",
)


def _socket_mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _socket_identity(path: Path) -> tuple[int, int]:
    current = os.lstat(path)
    return (int(current.st_dev), int(current.st_ino))


def _bind_unix_listener(path: Path) -> socket.socket:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(os.fspath(path))
        listener.listen()
    except Exception:
        listener.close()
        raise
    return listener


def _assert_unix_socket_connects(path: Path) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.5)
        connection.connect(os.fspath(path))


def _assert_private_daemon_failure(
    error: BaseException,
    *paths: Path,
    forbidden: tuple[str, ...] = (),
) -> None:
    rendered = f"{error!s}\n{error!r}"
    for path in paths:
        assert os.fspath(path) not in rendered
    for value in forbidden:
        assert value not in rendered


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize(
    "existing_mode",
    [None, 0o777],
    ids=["creates-private-parent", "repairs-permissive-parent"],
)
def test_daemon_default_socket_parent_and_endpoint_are_private_under_umask_zero(
    tmp_path: Path,
    existing_mode: int | None,
) -> None:
    data_dir = tmp_path / "default-state"
    if existing_mode is not None:
        data_dir.mkdir()
        os.chmod(data_dir, existing_mode)
    socket_path = data_dir / "tendwire.sock"
    config = Config(
        host_id="daemon-host",
        data_dir=data_dir,
        db_path=data_dir / "daemon.db",
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=lambda _path: None,
            observe_initial_snapshot=lambda _config: _public_snapshot(),
        ),
    )

    try:
        previous_umask = os.umask(0)
        try:
            daemon.start()
        finally:
            os.umask(previous_umask)

        assert _socket_mode(data_dir) == 0o700
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        assert _socket_mode(socket_path) == 0o600
        _assert_unix_socket_connects(socket_path)
    finally:
        daemon.stop()

    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_daemon_startup_repairs_all_existing_state_before_empty_observation(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "startup-state"
    data_dir.mkdir()
    os.chmod(data_dir, 0o755)
    db_path = data_dir / "daemon.db"
    db_path.write_bytes(b"existing-database")
    os.chmod(db_path, 0o644)
    config = Config(host_id="daemon-host", data_dir=data_dir, db_path=db_path)
    identity_paths = (
        config.installation_key_path,
        config.installation_key_marker_path,
        config.installation_key_sentinel_path,
    )
    for path in identity_paths:
        path.write_bytes(b"existing-identity")
        os.chmod(path, 0o644)
    observations: list[Snapshot] = []

    def initialize_store(path: Path) -> None:
        assert path == db_path
        assert _socket_mode(data_dir) == 0o700
        assert _socket_mode(db_path) == 0o600
        assert all(_socket_mode(identity_path) == 0o600 for identity_path in identity_paths)

    def observe(_config: Config) -> Snapshot:
        snapshot = Snapshot(
            host_id="daemon-host",
            updated_at="2026-01-01T00:00:00+00:00",
        )
        observations.append(snapshot)
        return snapshot

    for _attempt in range(2):
        daemon = TendwireDaemon(
            config,
            hooks=DaemonHooks(
                init_store=initialize_store,
                observe_initial_snapshot=observe,
            ),
        )
        try:
            daemon.start()
            assert daemon.snapshot is not None
            assert daemon.snapshot.workers == []
            assert _socket_mode(data_dir) == 0o700
            assert _socket_mode(db_path) == 0o600
            assert all(
                _socket_mode(identity_path) == 0o600
                for identity_path in identity_paths
            )
        finally:
            daemon.stop()

    assert len(observations) == 2
    assert not os.path.lexists(data_dir / "tendwire.sock")


@_UNIX_SOCKET_TEST
def test_daemon_rejects_identity_defect_before_socket_or_hook_work(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "defective-startup-state"
    data_dir.mkdir()
    os.chmod(data_dir, 0o755)
    db_path = data_dir / "daemon.db"
    db_path.write_bytes(b"existing-database")
    os.chmod(db_path, 0o644)
    protected_target = data_dir / "protected-target"
    protected_target.write_bytes(b"unchanged")
    os.chmod(protected_target, 0o600)
    identity_path = data_dir / "installation.key"
    identity_path.symlink_to(protected_target)
    socket_path = data_dir / "tendwire.sock"
    hook_calls: list[str] = []

    def initialize_store(_path: Path) -> None:
        hook_calls.append("init_store")
        raise AssertionError("store hook must not run")

    def observe(_config: Config) -> Snapshot:
        hook_calls.append("observe")
        raise AssertionError("observation hook must not run")

    daemon = TendwireDaemon(
        Config(host_id="daemon-host", data_dir=data_dir, db_path=db_path),
        hooks=DaemonHooks(
            init_store=initialize_store,
            observe_initial_snapshot=observe,
        ),
    )

    with pytest.raises(LocalStateError) as caught:
        daemon.start()

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert hook_calls == []
    assert daemon.server is None
    assert not os.path.lexists(socket_path)
    assert _socket_mode(data_dir) == 0o755
    assert _socket_mode(db_path) == 0o644
    assert identity_path.is_symlink()
    assert protected_target.read_bytes() == b"unchanged"
    _assert_private_daemon_failure(
        caught.value,
        data_dir,
        db_path,
        identity_path,
        protected_target,
        socket_path,
    )


def test_one_shot_cli_repairs_existing_database_without_initializing_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "one-shot-state"
    db_path = data_dir / "one-shot.db"
    init_store(db_path)
    os.chmod(data_dir, 0o755)
    os.chmod(db_path, 0o644)
    identity_paths = (
        data_dir / "installation.key",
        data_dir / "installation.key.sha256",
        data_dir / "installation.key.initialized",
    )
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)

    exit_code = main(
        [
            "--host-id",
            "one-shot-host",
            "store",
            "status",
            "--db-path",
            str(db_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert _socket_mode(data_dir) == 0o700
    assert _socket_mode(db_path) == 0o600
    assert all(not path.exists() for path in identity_paths)


@_UNIX_SOCKET_TEST
def test_daemon_group_socket_and_client_use_exact_shared_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    target_gid = next(
        (group_id for group_id in os.getgroups() if group_id != os.getegid()),
        os.getegid(),
    )
    try:
        group_name = grp.getgrgid(target_gid).gr_name
    except KeyError:
        target_gid = os.getegid()
        group_name = grp.getgrgid(target_gid).gr_name
    os.chown(parent, -1, target_gid)
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path / "private-state",
        db_path=tmp_path / "daemon.db",
        socket_path=socket_path,
        socket_group=group_name,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=lambda _path: None,
            observe_initial_snapshot=lambda _config: _public_snapshot(),
        ),
    )
    thread: threading.Thread | None = None

    try:
        previous_umask = os.umask(0)
        try:
            daemon.start()
        finally:
            os.umask(previous_umask)
        thread = threading.Thread(target=daemon.serve_forever)
        thread.start()

        socket_owner = os.lstat(socket_path).st_uid
        with monkeypatch.context() as client_process:
            client_process.setattr(
                "tendwire.local_state.os.geteuid",
                lambda: socket_owner + 100_000,
            )
            response = DaemonAPIClient(
                socket_path,
                socket_group=group_name,
                timeout_seconds=1,
            ).request("ping")

        assert response["ok"] is True
        assert response["result"]["pong"] is True
        assert _socket_mode(socket_path) == 0o660
        assert os.lstat(socket_path).st_gid == target_gid
    finally:
        daemon.stop()
        if thread is not None:
            thread.join(timeout=2)

    assert thread is not None and not thread.is_alive()
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_group_chown_failure_rolls_back_bound_socket_without_leaking_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    supplementary = [
        group_id for group_id in os.getgroups() if group_id != os.getegid()
    ]
    if not supplementary:
        pytest.skip("no supplementary group available for chgrp failure coverage")
    target_gid = supplementary[0]
    try:
        group_name = grp.getgrgid(target_gid).gr_name
    except KeyError:
        pytest.skip("supplementary group has no local name")
    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chown(parent, -1, target_gid)
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    raw_error_path = os.fspath(socket_path)

    def fail_chown(*_args: Any, **_kwargs: Any) -> None:
        raise OSError(errno.EPERM, "sentinel chown failure", raw_error_path)

    monkeypatch.setattr("tendwire.local_state.os.chown", fail_chown)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    _assert_private_daemon_failure(
        caught.value,
        socket_path,
        forbidden=("sentinel chown failure",),
    )
    assert not os.path.lexists(socket_path)
    server.close()


@_UNIX_SOCKET_TEST
def test_explicit_private_socket_securely_creates_missing_parent(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "explicit-private-parent"
    socket_path = parent / "daemon.sock"
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        server.start()

        assert _socket_mode(parent) == 0o700
        assert _socket_mode(socket_path) == 0o600
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()

    assert parent.is_dir()
    assert _socket_mode(parent) == 0o700
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_explicit_private_socket_rejects_writable_parent_before_stale_cleanup(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "unsafe-explicit-parent"
    parent.mkdir()
    os.chmod(parent, 0o1777)
    socket_path = parent / "daemon.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_identity = _socket_identity(socket_path)
    stale_listener.close()
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
        _assert_private_daemon_failure(caught.value, parent, socket_path)
        assert _socket_mode(parent) == 0o1777
        assert _socket_identity(socket_path) == stale_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_post_bind_pin_failure_rolls_back_exact_bound_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pin-failure.sock"
    original_pin = daemon_api_module.pin_owned_socket_at

    def fail_post_bind_pin(parent_fd: int, leaf: str) -> Any:
        if os.path.lexists(socket_path):
            raise LocalStateError(
                LocalStateErrorCode.OPERATION_FAILED,
                "secure local-state operation failed",
            )
        return original_pin(parent_fd, leaf)

    monkeypatch.setattr(daemon_api_module, "pin_owned_socket_at", fail_post_bind_pin)
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    _assert_private_daemon_failure(caught.value, socket_path)
    assert not os.path.lexists(socket_path)
    server.close()


@_UNIX_SOCKET_TEST
def test_post_bind_pin_failure_never_unlinks_replacement_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pin-substitution.sock"
    original_pin = daemon_api_module.pin_owned_socket_at
    replacement_listener: socket.socket | None = None

    def substitute_before_pin_failure(parent_fd: int, leaf: str) -> Any:
        nonlocal replacement_listener
        if not os.path.lexists(socket_path):
            return original_pin(parent_fd, leaf)
        socket_path.unlink()
        replacement_listener = _bind_unix_listener(socket_path)
        raise LocalStateError(
            LocalStateErrorCode.OPERATION_FAILED,
            "secure local-state operation failed",
        )

    monkeypatch.setattr(
        daemon_api_module,
        "pin_owned_socket_at",
        substitute_before_pin_failure,
    )
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        replacement_identity = _socket_identity(socket_path)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        _assert_unix_socket_connects(socket_path)

        server.close()

        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_startup_cleanup_failure_preserves_primary_error_and_pending_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "pending-cleanup.sock"
    original_unlink = daemon_api_module.unlink_verified_socket_at
    unlink_calls = 0

    def fail_permissions(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("primary startup failure")

    def fail_unlink_once(parent_fd: int, leaf: str, expected: Any) -> None:
        nonlocal unlink_calls
        unlink_calls += 1
        if unlink_calls == 1:
            raise LocalStateError(
                LocalStateErrorCode.OPERATION_FAILED,
                "secure local-state operation failed",
            )
        original_unlink(parent_fd, leaf, expected)

    monkeypatch.setattr(
        daemon_api_module,
        "enforce_bound_socket_permissions_at",
        fail_permissions,
    )
    monkeypatch.setattr(
        daemon_api_module,
        "unlink_verified_socket_at",
        fail_unlink_once,
    )
    server = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})

    with pytest.raises(RuntimeError, match="primary startup failure"):
        server.start()

    assert os.path.lexists(socket_path)
    with pytest.raises(DaemonUnavailable, match="cleanup is pending"):
        server.start()
    server.close()
    assert unlink_calls == 2
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_daemon_rejects_group_sharing_on_implicit_private_parent_before_mutation(
    tmp_path: Path,
) -> None:
    import grp

    data_dir = tmp_path / "default-state"
    group_name = grp.getgrgid(os.getegid()).gr_name
    daemon = TendwireDaemon(
        Config(
            host_id="daemon-host",
            data_dir=data_dir,
            db_path=tmp_path / "daemon.db",
            socket_group=group_name,
        ),
        hooks=DaemonHooks(
            init_store=lambda _path: None,
            observe_initial_snapshot=lambda _config: _public_snapshot(),
        ),
    )

    with pytest.raises(DaemonUnavailable) as caught:
        daemon.start()

    assert not data_dir.exists()
    _assert_private_daemon_failure(caught.value, data_dir)


@_UNIX_SOCKET_TEST
def test_nonmember_socket_group_is_rejected_before_parent_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp
    from types import SimpleNamespace

    memberships = {os.getegid(), *os.getgroups()}
    nonmember_gid = max(memberships, default=0) + 100_000
    group_name = "tendwire-nonmember-group"
    original_getgrnam = grp.getgrnam

    def fake_getgrnam(name: str) -> object:
        if name == group_name:
            return SimpleNamespace(gr_gid=nonmember_gid)
        return original_getgrnam(name)

    monkeypatch.setattr(grp, "getgrnam", fake_getgrnam)
    missing_parent = tmp_path / "missing-shared-parent"
    server = UnixSocketJSONServer(
        missing_parent / "daemon.sock",
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    with pytest.raises(DaemonUnavailable) as caught:
        server.start()

    assert not missing_parent.exists()
    _assert_private_daemon_failure(caught.value, missing_parent)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_start_is_idempotent(tmp_path: Path) -> None:
    socket_path = tmp_path / "idempotent.sock"
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        server.start()
        first_identity = _socket_identity(socket_path)
        server.start()

        assert server.listening is True
        assert _socket_identity(socket_path) == first_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)

    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_concurrent_startup_cannot_unlink_socket_before_first_listener_is_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tendwire.daemon_api as daemon_api_module

    socket_path = tmp_path / "concurrent.sock"
    permission_started = threading.Event()
    allow_permission = threading.Event()
    first_call_lock = threading.Lock()
    first_call = True
    original_enforce = daemon_api_module.enforce_bound_socket_permissions_at

    def delayed_enforce(*args: Any, **kwargs: Any) -> Any:
        nonlocal first_call
        with first_call_lock:
            should_wait = first_call
            first_call = False
        if should_wait:
            permission_started.set()
            assert allow_permission.wait(timeout=2)
        return original_enforce(*args, **kwargs)

    monkeypatch.setattr(
        daemon_api_module,
        "enforce_bound_socket_permissions_at",
        delayed_enforce,
    )
    first = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})
    second = UnixSocketJSONServer(socket_path, lambda _request: {"ok": True})
    first_errors: list[Exception] = []
    second_errors: list[Exception] = []

    def start_server(
        server: UnixSocketJSONServer,
        errors: list[Exception],
    ) -> None:
        try:
            server.start()
        except Exception as exc:
            errors.append(exc)

    first_thread = threading.Thread(
        target=start_server,
        args=(first, first_errors),
    )
    second_thread = threading.Thread(
        target=start_server,
        args=(second, second_errors),
    )
    try:
        first_thread.start()
        assert permission_started.wait(timeout=2)
        bound_identity = _socket_identity(socket_path)
        second_thread.start()
        time.sleep(0.05)

        assert second_thread.is_alive()
        assert _socket_identity(socket_path) == bound_identity
        allow_permission.set()
        first_thread.join(timeout=2)
        second_thread.join(timeout=2)

        assert not first_thread.is_alive()
        assert not second_thread.is_alive()
        assert first_errors == []
        assert len(second_errors) == 1
        assert isinstance(second_errors[0], DaemonUnavailable)
        assert str(second_errors[0]) == "daemon socket is already active"
        assert first.listening is True
        assert _socket_identity(socket_path) == bound_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        allow_permission.set()
        first.close()
        second.close()
        first_thread.join(timeout=2)
        second_thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_replaces_owned_stale_socket_only_after_connection_refused(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "stale.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()

    with pytest.raises(OSError) as refused:
        _assert_unix_socket_connects(socket_path)
    assert refused.value.errno == errno.ECONNREFUSED

    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True, "result": {"pong": True}},
        socket_group=None,
        prepare_parent=False,
    )
    thread: threading.Thread | None = None
    try:
        server.start()
        thread = threading.Thread(target=server.serve_forever)
        thread.start()

        response = DaemonAPIClient(
            socket_path,
            socket_group=None,
            timeout_seconds=1,
        ).request("ping")
        assert response == {"ok": True, "result": {"pong": True}}
    finally:
        server.close()
        if thread is not None:
            thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert thread is not None
    assert not thread.is_alive()
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_client_treats_disconnect_after_request_delivery_as_uncertain_protocol(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "disconnect-after-request.sock"
    listener = _bind_unix_listener(socket_path)
    os.chmod(socket_path, 0o600)
    request_received = threading.Event()

    def receive_then_disconnect() -> None:
        connection, _address = listener.accept()
        with connection:
            frame = bytearray()
            while b"\n" not in frame:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                frame.extend(chunk)
            request_received.set()

    thread = threading.Thread(target=receive_then_disconnect)
    thread.start()
    try:
        with pytest.raises(DaemonProtocolError) as caught:
            DaemonAPIClient(socket_path, timeout_seconds=1).request("ping")

        assert request_received.wait(timeout=1)
        assert str(caught.value) == "empty daemon response"
        _assert_private_daemon_failure(caught.value, socket_path)
    finally:
        listener.close()
        thread.join(timeout=2)
        socket_path.unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
def test_unix_socket_server_rejects_and_preserves_active_listener(tmp_path: Path) -> None:
    socket_path = tmp_path / "active.sock"
    active_listener = _bind_unix_listener(socket_path)
    active_identity = _socket_identity(socket_path)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == active_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        active_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("entry_kind", ["regular-file", "symlink"])
def test_unix_socket_server_rejects_wrong_type_without_mutating_entry_or_target(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    protected_contents = b"sentinel-daemon-socket-target-contents"
    socket_path = tmp_path / "unsafe.sock"
    if entry_kind == "regular-file":
        protected_path = socket_path
        protected_path.write_bytes(protected_contents)
    else:
        protected_path = tmp_path / "protected-target"
        protected_path.write_bytes(protected_contents)
        socket_path.symlink_to(protected_path)
    original_identity = _socket_identity(socket_path)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(
            caught.value,
            socket_path,
            protected_path,
            forbidden=(protected_contents.decode("ascii"),),
        )
        assert _socket_identity(socket_path) == original_identity
        assert protected_path.read_bytes() == protected_contents
        if entry_kind == "symlink":
            assert socket_path.is_symlink()
    finally:
        server.close()


@_UNIX_SOCKET_TEST
def test_unix_socket_server_rejects_wrong_owner_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_path = tmp_path / "wrong-owner.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    original_identity = _socket_identity(socket_path)
    actual_euid = os.geteuid()
    monkeypatch.setattr("tendwire.local_state.os.geteuid", lambda: actual_euid + 1)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == original_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_preserves_stale_socket_when_probe_error_is_ambiguous(
    tmp_path: Path,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "ambiguous.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    stale_identity = _socket_identity(socket_path)
    original_connect = socket.socket.connect

    def ambiguous_connect(connection: socket.socket, address: Any) -> Any:
        if str(address).endswith(f"/{socket_path.name}"):
            raise OSError(
                errno.EACCES,
                "sentinel ambiguous socket probe",
                os.fspath(socket_path),
            )
        return original_connect(connection, address)

    monkeypatch.setattr(socket.socket, "connect", ambiguous_connect)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert _socket_identity(socket_path) == stale_identity
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_refuses_substitution_before_stale_unlink(
    tmp_path: Path,
    monkeypatch,
) -> None:
    socket_path = tmp_path / "stale-substitution.sock"
    stale_listener = _bind_unix_listener(socket_path)
    stale_listener.close()
    stale_identity = _socket_identity(socket_path)
    stale_fd = os.open(socket_path, os.O_PATH | os.O_NOFOLLOW)
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None

    def substitute_after_refusal(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener
        if not str(address).endswith(f"/{socket_path.name}"):
            return original_connect(connection, address)
        try:
            return original_connect(connection, address)
        except OSError as exc:
            if exc.errno != errno.ECONNREFUSED:
                raise
            socket_path.unlink()
            replacement_listener = _bind_unix_listener(socket_path)
            raise

    monkeypatch.setattr(socket.socket, "connect", substitute_after_refusal)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        replacement_identity = _socket_identity(socket_path)
        assert replacement_identity != stale_identity
        _assert_unix_socket_connects(socket_path)
        assert _socket_identity(socket_path) == replacement_identity
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        os.close(stale_fd)
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_unix_socket_server_close_preserves_substituted_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "close-substitution.sock"
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=None,
        prepare_parent=False,
    )
    replacement_listener: socket.socket | None = None

    try:
        server.start()
        original_identity = _socket_identity(socket_path)
        socket_path.unlink()
        replacement_listener = _bind_unix_listener(socket_path)
        replacement_identity = _socket_identity(socket_path)
        assert replacement_identity != original_identity

        server.close()

        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
def test_daemon_binds_socket_before_store_initialization_and_observation(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "ordered-cli.sock"
    calls: list[str] = []

    def assert_bound(stage: str) -> None:
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        _assert_unix_socket_connects(socket_path)
        calls.append(stage)

    def initialize_store(_db_path: Path) -> None:
        assert_bound("init_store")

    def observe(_config: Config) -> Snapshot:
        assert_bound("observe")
        return _public_snapshot()

    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / "ordered-cli.db",
        socket_path=socket_path,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=initialize_store,
            observe_initial_snapshot=observe,
        ),
    )

    try:
        daemon.start()
        assert calls == ["init_store", "observe"]
    finally:
        daemon.stop()

    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
def test_daemon_binds_socket_before_event_backend_factory_and_start(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "ordered-events.sock"
    db_path = tmp_path / "ordered-events.db"
    calls: list[str] = []

    def assert_bound(stage: str) -> None:
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        _assert_unix_socket_connects(socket_path)
        calls.append(stage)

    def initialize_store(path: Path) -> None:
        assert_bound("init_store")
        init_store(path)

    class RecordingEventBackend:
        def __init__(self) -> None:
            self.stopped = False

        def start(self, *, wait_for_reconcile: bool) -> None:
            assert wait_for_reconcile is True
            assert_bound("backend_start")
            save_snapshot(db_path, _public_snapshot())

        def stop(self) -> None:
            self.stopped = True

    backend = RecordingEventBackend()

    def event_backend_factory(_config: Config, _stop_event: threading.Event) -> Any:
        assert_bound("backend_factory")
        return backend

    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="socket",
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=initialize_store,
            event_backend_factory=event_backend_factory,
        ),
    )

    try:
        daemon.start()
        assert calls == ["init_store", "backend_factory", "backend_start"]
    finally:
        daemon.stop()

    assert backend.stopped is True
    assert not os.path.lexists(socket_path)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("failure_stage", ["init_store", "observe"])
def test_daemon_startup_failure_closes_prebound_socket(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    socket_path = tmp_path / f"{failure_stage}.sock"

    def assert_bound() -> None:
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        _assert_unix_socket_connects(socket_path)

    def initialize_store(_db_path: Path) -> None:
        assert_bound()
        if failure_stage == "init_store":
            raise RuntimeError("sentinel startup failure")

    def observe(_config: Config) -> Snapshot:
        assert_bound()
        if failure_stage == "observe":
            raise RuntimeError("sentinel startup failure")
        return _public_snapshot()

    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / f"{failure_stage}.db",
        socket_path=socket_path,
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=initialize_store,
            observe_initial_snapshot=observe,
        ),
    )

    try:
        with pytest.raises(RuntimeError, match="sentinel startup failure") as caught:
            daemon.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert not os.path.lexists(socket_path)
        with pytest.raises(RuntimeError, match="cannot start after shutdown"):
            daemon.start()
        assert not os.path.lexists(socket_path)
    finally:
        daemon.stop()


@_UNIX_SOCKET_TEST
def test_daemon_backend_start_failure_closes_socket_and_stops_started_backend(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "backend-failure.sock"

    def assert_bound() -> None:
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        _assert_unix_socket_connects(socket_path)

    class FailingEventBackend:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        def start(self, *, wait_for_reconcile: bool) -> None:
            assert wait_for_reconcile is True
            assert_bound()
            self.started = True
            raise RuntimeError("sentinel backend startup failure")

        def stop(self) -> None:
            self.stopped = True

    backend = FailingEventBackend()

    def event_backend_factory(_config: Config, _stop_event: threading.Event) -> Any:
        assert_bound()
        return backend

    config = Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / "backend-failure.db",
        socket_path=socket_path,
        herdr_backend="socket",
    )
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            init_store=lambda _path: assert_bound(),
            event_backend_factory=event_backend_factory,
        ),
    )

    try:
        with pytest.raises(RuntimeError, match="sentinel backend startup failure") as caught:
            daemon.start()

        _assert_private_daemon_failure(caught.value, socket_path)
        assert backend.started is True
        assert backend.stopped is True
        assert not os.path.lexists(socket_path)
    finally:
        daemon.stop()


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
        while not server.listening and time.monotonic() < deadline:
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


def test_daemon_bounds_oversized_response_and_keeps_serving(tmp_path: Path) -> None:
    socket_path = tmp_path / "bounded-response.sock"

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        if request.get("method") == "oversized":
            # Numeric JSON avoids making the guard test depend on text-redaction cost.
            return {
                "ok": True,
                "result": {"items": list(range(250_000))},
            }
        return {"ok": True, "result": {"pong": True}}

    server = UnixSocketJSONServer(
        socket_path,
        dispatch,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        oversized = DaemonAPIClient(socket_path, timeout_seconds=10).request("oversized")
        ping = DaemonAPIClient(socket_path, timeout_seconds=2).request("ping")

        assert oversized["ok"] is False
        assert oversized["error"]["code"] == "response_too_large"
        assert oversized["error"]["details"] == {"max_response_bytes": MAX_RESPONSE_BYTES}
        assert ping["ok"] is True
        assert ping["result"]["pong"] is True
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

        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
            calls.append({"method": method, "params": dict(params)})
            if method == "agent.get":
                return {"result": {"agent": {"pane_id": "pane-private"}}}
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
        assert calls == [
            {"method": "agent.get", "params": {"target": "agent-private"}},
            {"method": "pane.send_keys", "params": {"pane_id": "pane-private", "keys": ["ctrl+u"]}},
            {"method": "pane.send_keys", "params": {"pane_id": "pane-private", "keys": ["ctrl+a", "ctrl+k"]}},
            {"method": "pane.send_keys", "params": {"pane_id": "pane-private", "keys": ["ctrl+a", "backspace"]}},
            {"method": "pane.send_text", "params": {"pane_id": "pane-private", "text": "hello"}},
            {"method": "pane.send_keys", "params": {"pane_id": "pane-private", "keys": ["enter"]}},
        ]
        assert get_command_receipt(db_path, "cmd-host", "req-1", "send_instruction") is not None
        assert "agent-private" not in json.dumps(first)
        assert "pane-private" not in json.dumps(first)
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
    data_dir = tmp_path / "private-state"
    data_dir.mkdir(mode=0o700)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", os.fspath(data_dir))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)

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


def _current_socket_group() -> tuple[str, int]:
    import grp

    group_id = os.getegid()
    try:
        return grp.getgrgid(group_id).gr_name, group_id
    except KeyError:
        pytest.skip("effective group has no local name")


def _prepare_socket_test_parent(
    parent: Path,
    *,
    group_id: int | None,
) -> None:
    parent.mkdir(parents=True)
    if group_id is None:
        os.chmod(parent, 0o700)
    else:
        os.chown(parent, -1, group_id)
        os.chmod(parent, 0o710)


def _prepare_socket_test_endpoint(
    path: Path,
    *,
    group_id: int | None,
) -> socket.socket:
    listener = _bind_unix_listener(path)
    if group_id is None:
        os.chmod(path, 0o600)
    else:
        os.chown(path, -1, group_id)
        os.chmod(path, 0o660)
    return listener


def _read_request_frame(connection: socket.socket) -> bytes:
    frame = bytearray()
    while b"\n" not in frame:
        chunk = connection.recv(4096)
        if not chunk:
            break
        frame.extend(chunk)
    return bytes(frame)


def _configured_path_variant(
    path: Path,
    root: Path,
    variant: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    if variant == "absolute":
        return path
    monkeypatch.chdir(root)
    return path.relative_to(root)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize(
    "server_mode",
    ["default-private", "explicit-private", "group"],
)
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_server_rejects_intermediate_symlink_without_touching_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    server_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if server_mode == "group":
        group_name, group_id = _current_socket_group()
    target_parent = tmp_path / "protected-target" / "socket-parent"
    _prepare_socket_test_parent(target_parent, group_id=group_id)
    target_socket = target_parent / "daemon.sock"
    target_listener = _prepare_socket_test_endpoint(
        target_socket,
        group_id=group_id,
    )
    target_identity = _socket_identity(target_socket)
    target_parent_mode = _socket_mode(target_parent)
    configured_root = tmp_path / "configured-root"
    configured_root.mkdir()
    intermediate = configured_root / "intermediate"
    intermediate.symlink_to(target_parent.parent, target_is_directory=True)
    absolute_configured = intermediate / target_parent.name / target_socket.name
    configured = _configured_path_variant(
        absolute_configured,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    server = UnixSocketJSONServer(
        configured,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=server_mode == "default-private",
    )
    assert server.socket_path == configured

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            server.start()

        _assert_private_daemon_failure(
            caught.value,
            configured,
            target_parent,
            target_socket,
        )
        assert intermediate.is_symlink()
        assert _socket_mode(target_parent) == target_parent_mode
        assert _socket_identity(target_socket) == target_identity
        _assert_unix_socket_connects(target_socket)
    finally:
        server.close()
        target_listener.close()
        target_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("client_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_client_rejects_intermediate_symlink_without_touching_listener(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if client_mode == "group":
        group_name, group_id = _current_socket_group()
    target_parent = tmp_path / "ct" / "p"
    _prepare_socket_test_parent(target_parent, group_id=group_id)
    target_socket = target_parent / "s"
    target_listener = _prepare_socket_test_endpoint(
        target_socket,
        group_id=group_id,
    )
    target_identity = _socket_identity(target_socket)
    configured_root = tmp_path / "cc"
    configured_root.mkdir()
    intermediate = configured_root / "i"
    intermediate.symlink_to(target_parent.parent, target_is_directory=True)
    absolute_configured = intermediate / target_parent.name / target_socket.name
    configured = _configured_path_variant(
        absolute_configured,
        tmp_path,
        path_variant,
        monkeypatch,
    )

    client = DaemonAPIClient(
        configured,
        socket_group=group_name,
        timeout_seconds=0.2,
    )
    assert client.socket_path == configured

    try:
        with pytest.raises(DaemonUnavailable) as caught:
            client.request("ping")

        _assert_private_daemon_failure(
            caught.value,
            configured,
            target_parent,
            target_socket,
        )
        assert intermediate.is_symlink()
        assert _socket_identity(target_socket) == target_identity
        _assert_unix_socket_connects(target_socket)
    finally:
        target_listener.close()
        target_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_server_keeps_resolved_parent_pinned_when_ancestor_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    configured_parent = tmp_path / "server-configured-parent"
    _prepare_socket_test_parent(configured_parent, group_id=group_id)
    absolute_socket = configured_parent / "daemon.sock"
    configured_socket = _configured_path_variant(
        absolute_socket,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    pinned_parent = tmp_path / "server-pinned-parent"
    original_bind = socket.socket.bind
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None
    substituted = False

    def substitute_before_bind(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity, substituted
        if (
            not substituted
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{absolute_socket.name}")
        ):
            substituted = True
            configured_parent.rename(pinned_parent)
            _prepare_socket_test_parent(configured_parent, group_id=group_id)
            replacement_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            original_bind(replacement_listener, os.fspath(absolute_socket))
            replacement_listener.listen()
            if group_id is None:
                os.chmod(absolute_socket, 0o600)
            else:
                os.chown(absolute_socket, -1, group_id)
                os.chmod(absolute_socket, 0o660)
            replacement_identity = _socket_identity(absolute_socket)
        return original_bind(connection, address)

    monkeypatch.setattr(socket.socket, "bind", substitute_before_bind)
    server = UnixSocketJSONServer(
        configured_socket,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )

    try:
        server.start()
        pinned_socket = pinned_parent / absolute_socket.name
        assert substituted is True
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
        _assert_unix_socket_connects(pinned_socket)

        server.close()

        assert not os.path.lexists(pinned_socket)
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
    finally:
        server.close()
        if replacement_listener is not None:
            replacement_listener.close()
        absolute_socket.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
@pytest.mark.parametrize("path_variant", ["absolute", "relative"])
def test_socket_client_keeps_resolved_parent_pinned_when_ancestor_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
    path_variant: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    configured_parent = tmp_path / "client-configured-parent"
    _prepare_socket_test_parent(configured_parent, group_id=group_id)
    absolute_socket = configured_parent / "daemon.sock"
    original_listener = _prepare_socket_test_endpoint(
        absolute_socket,
        group_id=group_id,
    )
    configured_socket = _configured_path_variant(
        absolute_socket,
        tmp_path,
        path_variant,
        monkeypatch,
    )
    pinned_parent = tmp_path / "client-pinned-parent"
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None
    substituted = False

    def serve_original() -> None:
        connection, _address = original_listener.accept()
        with connection:
            _read_request_frame(connection)
            connection.sendall(b'{"ok":true,"result":{"source":"original"}}\n')

    def substitute_before_connect(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity, substituted
        if (
            not substituted
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{absolute_socket.name}")
        ):
            substituted = True
            configured_parent.rename(pinned_parent)
            _prepare_socket_test_parent(configured_parent, group_id=group_id)
            replacement_listener = _prepare_socket_test_endpoint(
                absolute_socket,
                group_id=group_id,
            )
            replacement_identity = _socket_identity(absolute_socket)
        return original_connect(connection, address)

    monkeypatch.setattr(socket.socket, "connect", substitute_before_connect)
    thread = threading.Thread(target=serve_original)
    thread.start()
    try:
        response = DaemonAPIClient(
            configured_socket,
            socket_group=group_name,
            timeout_seconds=1,
        ).request("ping")

        pinned_socket = pinned_parent / absolute_socket.name
        assert response == {"ok": True, "result": {"source": "original"}}
        assert substituted is True
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(absolute_socket) == replacement_identity
        _assert_unix_socket_connects(absolute_socket)
        _assert_unix_socket_connects(pinned_socket)
    finally:
        original_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        thread.join(timeout=2)
        absolute_socket.unlink(missing_ok=True)
        (pinned_parent / absolute_socket.name).unlink(missing_ok=True)

    assert not thread.is_alive()


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
def test_socket_client_rejects_leaf_replacement_after_anchored_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
) -> None:
    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    parent = tmp_path / "post-connect-parent"
    _prepare_socket_test_parent(parent, group_id=group_id)
    socket_path = parent / "daemon.sock"
    original_listener = _prepare_socket_test_endpoint(
        socket_path,
        group_id=group_id,
    )
    original_connect = socket.socket.connect
    replacement_listener: socket.socket | None = None
    replacement_identity: tuple[int, int] | None = None

    def replace_after_connect(connection: socket.socket, address: Any) -> Any:
        nonlocal replacement_listener, replacement_identity
        result = original_connect(connection, address)
        if (
            replacement_listener is None
            and str(address).startswith("/proc/self/fd/")
            and str(address).endswith(f"/{socket_path.name}")
        ):
            socket_path.unlink()
            replacement_listener = _prepare_socket_test_endpoint(
                socket_path,
                group_id=group_id,
            )
            replacement_identity = _socket_identity(socket_path)
        return result

    monkeypatch.setattr(socket.socket, "connect", replace_after_connect)
    try:
        with pytest.raises(DaemonUnavailable) as caught:
            DaemonAPIClient(
                socket_path,
                socket_group=group_name,
                timeout_seconds=1,
            ).request("ping")

        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        _assert_private_daemon_failure(caught.value, socket_path)
        assert replacement_listener is not None
        assert replacement_identity is not None
        assert _socket_identity(socket_path) == replacement_identity
        _assert_unix_socket_connects(socket_path)
    finally:
        original_listener.close()
        if replacement_listener is not None:
            replacement_listener.close()
        socket_path.unlink(missing_ok=True)


@_UNIX_SOCKET_TEST
@pytest.mark.parametrize("socket_mode", ["private", "group"])
def test_socket_startup_lock_contention_is_bounded_and_closes_parent_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_mode: str,
) -> None:
    import fcntl
    import tendwire.daemon_api as daemon_api_module

    group_name: str | None = None
    group_id: int | None = None
    if socket_mode == "group":
        group_name, group_id = _current_socket_group()
    parent = tmp_path / "locked"
    _prepare_socket_test_parent(parent, group_id=group_id)
    socket_path = parent / "daemon.sock"
    holder_fd = os.open(
        parent,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    fcntl.flock(holder_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    original_open = daemon_api_module.open_resolved_parent
    opened_parent_fds: list[int] = []

    def track_open_parent(*args: Any, **kwargs: Any) -> tuple[int, str]:
        parent_fd, leaf = original_open(*args, **kwargs)
        opened_parent_fds.append(parent_fd)
        return parent_fd, leaf

    monkeypatch.setattr(daemon_api_module, "open_resolved_parent", track_open_parent)
    monkeypatch.setattr(
        daemon_api_module,
        "_SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS",
        0.02,
    )
    monkeypatch.setattr(
        daemon_api_module,
        "_SOCKET_STARTUP_LOCK_RETRY_SECONDS",
        0.001,
    )
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        socket_group=group_name,
        prepare_parent=False,
    )
    errors: list[BaseException] = []

    def start_server() -> None:
        try:
            server.start()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=start_server)
    thread.start()
    thread.join(timeout=0.5)
    completed_while_contended = not thread.is_alive()
    fcntl.flock(holder_fd, fcntl.LOCK_UN)
    os.close(holder_fd)
    thread.join(timeout=1)
    try:
        assert completed_while_contended
        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], DaemonUnavailable)
        assert str(errors[0]) == "daemon socket startup lock timed out"
        assert errors[0].code is LocalStateErrorCode.OPERATION_FAILED
        _assert_private_daemon_failure(errors[0], parent, socket_path)
        assert opened_parent_fds
        with pytest.raises(OSError) as closed:
            os.fstat(opened_parent_fds[0])
        assert closed.value.errno == errno.EBADF
        assert not os.path.lexists(socket_path)
    finally:
        server.close()


@_UNIX_SOCKET_TEST
def test_socket_startup_lock_retries_interrupted_nonblocking_flock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    parent = tmp_path / "eintr"
    _prepare_socket_test_parent(parent, group_id=None)
    socket_path = parent / "daemon.sock"
    original_flock = fcntl.flock
    interrupted = False

    def interrupt_once(fd: int, operation: int) -> Any:
        nonlocal interrupted
        if operation & fcntl.LOCK_NB and not interrupted:
            interrupted = True
            raise OSError(errno.EINTR, "sentinel interrupted flock")
        return original_flock(fd, operation)

    monkeypatch.setattr(fcntl, "flock", interrupt_once)
    server = UnixSocketJSONServer(
        socket_path,
        lambda _request: {"ok": True},
        prepare_parent=False,
    )
    try:
        server.start()
        assert interrupted
        assert server.listening
        _assert_unix_socket_connects(socket_path)
    finally:
        server.close()

    assert not os.path.lexists(socket_path)
