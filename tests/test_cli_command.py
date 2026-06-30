"""Tests for the `tendwire command --json` CLI orchestration."""

from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tendwire.backends.herdr_cli import HerdrCommandObservation
from tendwire.cli import main
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
    CommandRequest,
)
from tendwire.core.models import Space, Worker, WorkerBinding
from tendwire.store.sqlite import (
    get_command_receipt,
    init_store,
    list_worker_bindings,
    upsert_worker_bindings,
)


def _fake_herdr_state(config: Any) -> tuple[list[Space], list[Worker]]:
    workers = [
        Worker(
            id="w-1",
            name="Alpha",
            status="active",
            space_id="s-1",
            backend_target={"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
        ),
        Worker(
            id="w-2",
            name="Beta",
            status="idle",
            space_id="s-1",
            backend_target={"kind": "agent_id", "value": "agent-2", "sendable": True, "reason": None},
        ),
    ]
    return [], workers


def _fake_herdr_command_observation(config: Any) -> HerdrCommandObservation:
    spaces, workers = _fake_herdr_state(config)
    return HerdrCommandObservation(
        spaces=spaces,
        workers=workers,
        status="healthy",
        outcome="healthy_non_empty",
    )


def _accepted_backend(calls: list[tuple[Any, Any]]):
    def send(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(
            ok=True,
            status=STATUS_ACCEPTED,
            action="send_instruction",
            result={"target": target},
        )

    return send


def test_cli_command_invalid_json(capsys, monkeypatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("not json"))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_noop_success(capsys, monkeypatch) -> None:
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("noop must not fetch Herdr state")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "noop"
    assert payload["schema_version"] == 1
    assert captured.err == ""
    assert calls == []


def test_cli_command_unknown_action_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "explode"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert captured.err == ""


def test_cli_command_read_snapshot_neutral_result(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["result"]["snapshot"]["schema_version"] == 2
    assert payload["result"]["snapshot"]["backend_health"][0]["status"] == "unavailable"
    assert payload["result"]["snapshot"]["backend_health"][0]["outcome"] == "missing_binary"
    assert captured.err == ""


def test_cli_command_send_instruction_dry_run_no_receipt(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "tendwire.cli.herdr_send_instruction",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backend called")),
    )
    db_path = tmp_path / "cmd.db"
    init_store(db_path)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == STATUS_DRY_RUN
    assert payload["dry_run"] is True
    # Dry-runs never create receipts.
    assert get_command_receipt(db_path, "cmd-host", "", "send_instruction") is None
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0


@pytest.mark.parametrize("mode", ["cli_socket_path", "env_socket_path", "env_backend_socket"])
def test_cli_command_socket_mode_mutation_unavailable_does_not_fallback(
    mode: str,
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def guarded_fetch(*args: Any, **kwargs: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("explicit daemon/socket mode must not fall back to Herdr observation")

    def guarded_send(*args: Any, **kwargs: Any) -> CommandEnvelope:
        calls.append("send")
        raise AssertionError("explicit daemon/socket mode must not send through Herdr CLI")

    monkeypatch.delenv("TENDWIRE_SOCKET_PATH", raising=False)
    monkeypatch.delenv("TENDWIRE_HERDR_BACKEND", raising=False)
    monkeypatch.delenv("TENDWIRE_DATA_DIR", raising=False)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded_send)

    socket_path = tmp_path / f"{mode}.sock"
    data_dir = tmp_path / "data"
    args = [
        "--host-id",
        "cmd-host",
        "--herdr-bin",
        "definitely-not-a-real-herdr-binary",
    ]
    forbidden_fragments = [str(socket_path)]
    if mode == "cli_socket_path":
        args.extend(["--socket-path", str(socket_path)])
    elif mode == "env_socket_path":
        monkeypatch.setenv("TENDWIRE_SOCKET_PATH", str(socket_path))
    elif mode == "env_backend_socket":
        monkeypatch.setenv("TENDWIRE_HERDR_BACKEND", "socket")
        monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
        forbidden_fragments.extend([str(data_dir), "tendwire.sock"])
    else:
        raise AssertionError(f"unexpected mode {mode}")

    db_path = tmp_path / f"{mode}.db"
    request_id = f"daemon-unavailable-{mode}"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": request_id,
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            *args,
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
    assert payload["request_id"] == request_id
    assert calls == []
    for fragment in forbidden_fragments:
        assert fragment not in serialized
    _assert_no_command_public_forbidden_fields(payload)

    receipt = get_command_receipt(db_path, "cmd-host", request_id, "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is False
    cached = json.loads(receipt["result_json"])
    assert cached["status"] == STATUS_BACKEND_UNAVAILABLE
    assert cached["request_id"] == request_id


@pytest.mark.parametrize(
    ("request_id", "include_request_id"),
    [
        (None, False),
        (None, True),
        ("", True),
        ("   \t", True),
    ],
)
def test_cli_command_send_instruction_non_dry_run_requires_request_id(
    capsys,
    monkeypatch,
    tmp_path: Path,
    request_id: Any,
    include_request_id: bool,
) -> None:
    calls: list[str] = []

    def guarded(*args: Any, **kwargs: Any) -> Any:
        calls.append("called")
        raise AssertionError("invalid request_id must stop before backend or store mutation")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", guarded)
    monkeypatch.setattr("tendwire.cli.reserve_command_receipt", guarded)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "action": "send_instruction",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }
    if include_request_id:
        payload["request_id"] = request_id
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    db_path = tmp_path / "invalid-request-id.db"

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    payload_out = json.loads(captured.out)
    assert payload_out["ok"] is False
    assert payload_out["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    assert not db_path.exists()

def test_cli_command_send_instruction_accepts_literal_false_for_mutation(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "literal-false.db"
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_command_observation",
        _fake_herdr_command_observation,
    )
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "literal-false",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == STATUS_ACCEPTED
    assert payload["dry_run"] is False
    assert len(calls) == 1


def test_cli_command_rehydrates_private_target_from_unexpired_stored_binding(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stored-binding.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="w-1",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-stored",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="stored-private",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="w-1", name="Alpha", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", targetless_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "stored-binding",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == STATUS_ACCEPTED
    assert len(calls) == 1
    assert calls[0][0]["backend_target"]["value"] == "agent-stored"
    assert "agent-stored" not in json.dumps(payload)
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_does_not_send_through_expired_stored_binding(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "expired-binding.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="w-1",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-expired",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="2026-01-02T00:00:00+00:00",
                private_fingerprint="expired-private",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="w-1", name="Alpha", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", targetless_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "expired-binding",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_BACKEND_UNSUPPORTED
    assert calls == []
    assert "agent-expired" not in json.dumps(payload)
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_duplicate_current_binding_is_ambiguous_and_not_expired(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "current-duplicates.db"
    calls: list[tuple[Any, Any]] = []

    def duplicate_observation(config: Any) -> HerdrCommandObservation:
        worker_a = Worker(
            id="dup-a",
            name="Duplicate A",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        )
        worker_b = Worker(
            id="dup-b",
            name="Duplicate B",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        )
        return HerdrCommandObservation(
            spaces=[],
            workers=[worker_a, worker_b],
            status="healthy",
            outcome="healthy_non_empty",
            bindings=[
                WorkerBinding(
                    host_id="cmd-host",
                    worker_id=worker_a.id,
                    worker_fingerprint=worker_a.fingerprint,
                    backend="herdr",
                    target_kind="agent_id",
                    target_value="same-agent",
                    sendable=False,
                    reason="duplicate_backend_target",
                    observed_at="2026-01-01T00:00:00+00:00",
                    private_fingerprint="colliding-private",
                ),
                WorkerBinding(
                    host_id="cmd-host",
                    worker_id=worker_b.id,
                    worker_fingerprint=worker_b.fingerprint,
                    backend="herdr",
                    target_kind="agent_id",
                    target_value="same-agent",
                    sendable=False,
                    reason="duplicate_backend_target",
                    observed_at="2026-01-01T00:00:00+00:00",
                    private_fingerprint="colliding-private",
                ),
            ],
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", duplicate_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "current-duplicate",
                    "dry_run": False,
                    "target": {"worker_id": "dup-a"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")

    assert code == 1
    assert payload["status"] == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []
    assert len(current) == 2
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert "colliding-private" not in {binding.private_fingerprint for binding in current}
    assert len({binding.private_fingerprint for binding in current}) == 2
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_stored_duplicate_binding_is_ambiguous_and_skips_backend(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stored-duplicate.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="dup-stored",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="same-agent",
                sendable=False,
                reason="duplicate_backend_target",
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="stored-duplicate",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="dup-stored", name="Duplicate", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", targetless_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "stored-duplicate",
                    "dry_run": False,
                    "target": {"worker_id": "dup-stored"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []
    assert "same-agent" not in json.dumps(payload)
    _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_cli_command_rejects_non_boolean_dry_run_before_backend(
    value: Any, capsys, monkeypatch
) -> None:
    calls: list[str] = []

    def guarded_observation(config: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("invalid dry_run must not fetch")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", guarded_observation)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "bad-dry-run",
                    "dry_run": value,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize("value", ["1", 1.0, True, False, None, [], {}, 2])
def test_cli_command_rejects_malformed_schema_version_before_pipeline(
    value: Any, capsys, monkeypatch
) -> None:
    calls: list[str] = []

    def guarded(*args: Any, **kwargs: Any) -> Any:
        calls.append("called")
        raise AssertionError("invalid schema_version must stop before pipeline work")

    monkeypatch.setattr("tendwire.cli.reserve_command_receipt", guarded)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", guarded)
    monkeypatch.setattr("tendwire.cli.project_from_observations", guarded)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": value,
                    "action": "send_instruction",
                    "request_id": "bad-schema",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_send_instruction_empty_target_rejects_before_fetch(capsys, monkeypatch) -> None:
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("empty target must reject before Herdr fetch")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "empty-target",
                    "dry_run": False,
                    "target": {},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []


def test_cli_command_duplicate_request_id_same_payload_returns_cached(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_command_observation",
        _fake_herdr_command_observation,
    )
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    db_path = tmp_path / "cmd.db"
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-1",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code1 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured1 = capsys.readouterr()
    assert code1 == 0
    result1 = json.loads(captured1.out)
    assert result1["status"] == STATUS_ACCEPTED

    receipt = get_command_receipt(db_path, "cmd-host", "dup-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is False
    assert receipt["completed_at"] is not None

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 0
    result2 = json.loads(captured2.out)
    assert result2["status"] == STATUS_ACCEPTED
    assert result2 == result1
    assert len(calls) == 1


def test_cli_command_duplicate_request_id_different_payload_rejects(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_command_observation",
        _fake_herdr_command_observation,
    )
    calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", _accepted_backend(calls))
    db_path = tmp_path / "cmd.db"
    payload1 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )
    payload2 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "world"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload1))
    code1 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    capsys.readouterr()
    assert code1 == 0

    monkeypatch.setattr("sys.stdin", io.StringIO(payload2))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_DUPLICATE_REQUEST
    assert len(calls) == 1


def test_cli_command_pending_receipt_rejects_without_retry(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    # Seed an uncertain receipt directly.
    from tendwire.store.sqlite import init_store, save_command_receipt

    pending_request = CommandRequest(
        action="send_instruction",
        request_id="uncertain-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    init_store(db_path)
    save_command_receipt(
        db_path,
        host_id="cmd-host",
        request_id="uncertain-1",
        action="send_instruction",
        payload_fingerprint=pending_request.payload_fingerprint(),
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        result_json=json.dumps(
            {
                "schema_version": 1,
                "action": "send_instruction",
                "request_id": "uncertain-1",
                "ok": False,
                "dry_run": False,
                "status": STATUS_REQUEST_STATE_UNCERTAIN,
                "result": None,
                "error": {"code": STATUS_REQUEST_STATE_UNCERTAIN, "message": "pending", "details": {}},
                "warnings": [],
            }
        ),
        uncertain=True,
    )

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "uncertain-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_REQUEST_STATE_UNCERTAIN


def test_cli_command_uncertain_receipt_changed_payload_is_duplicate_without_retry(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "cmd.db"
    from tendwire.store.sqlite import init_store, save_command_receipt

    original_request = CommandRequest(
        action="send_instruction",
        request_id="uncertain-changed",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    init_store(db_path)
    save_command_receipt(
        db_path,
        host_id="cmd-host",
        request_id="uncertain-changed",
        action="send_instruction",
        payload_fingerprint=original_request.payload_fingerprint(),
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        result_json=json.dumps(
            {
                "schema_version": 1,
                "action": "send_instruction",
                "request_id": "uncertain-changed",
                "ok": False,
                "dry_run": False,
                "status": STATUS_REQUEST_STATE_UNCERTAIN,
                "result": None,
                "error": {"code": STATUS_REQUEST_STATE_UNCERTAIN, "message": "pending", "details": {}},
                "warnings": [],
            }
        ),
        uncertain=True,
    )
    calls: list[str] = []

    def guarded_observation(config: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("changed duplicate receipt must not fetch Herdr")

    def guarded_send(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        calls.append("send")
        raise AssertionError("changed duplicate receipt must not send")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", guarded_observation)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded_send)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "uncertain-changed",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "world"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert code == 1
    assert result["status"] == STATUS_DUPLICATE_REQUEST
    assert calls == []


def test_cli_command_forbidden_field_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "noop",
                    "params": {"pane_id": "leaked"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_rejects_control_sequence_instruction_as_json_only(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello\x1b[31mworld"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["error"]["details"] == {"field": "instruction.text"}
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_backend_result_and_receipt_are_sanitized(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_command_observation",
        _fake_herdr_command_observation,
    )

    def leaky_backend(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        return CommandEnvelope(
            ok=False,
            status=STATUS_BACKEND_FAILED,
            action="send_instruction",
            result={
                "target": target,
                "pane_id": "p-1",
                "nested": {"argv": ["herdr"], "safe": "kept"},
            },
            error={
                "code": STATUS_BACKEND_FAILED,
                "message": "failed",
                "details": {"terminal_id": "t-1", "safe": "kept"},
            },
        )

    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", leaky_backend)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "sanitize-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_BACKEND_FAILED
    assert payload["result"]["nested"]["safe"] == "kept"
    assert payload["error"]["details"] == {"safe": "kept"}
    _assert_no_command_public_forbidden_fields(payload)

    receipt = get_command_receipt(db_path, "cmd-host", "sanitize-1", "send_instruction")
    assert receipt is not None
    cached = json.loads(receipt["result_json"])
    _assert_no_command_public_forbidden_fields(cached)
    assert captured.err == ""


_COMMAND_PUBLIC_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "pid",
    "tty",
    "pty",
    "tmux",
    "screen_session",
    "window_id",
    "tab_id",
    "argv",
    "shell",
    "command",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "token",
    "tokens",
    "connector",
    "connectors",
    "backend_target",
    "agent_session",
    "session_id",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private_fingerprint",
}


def _assert_no_command_public_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _COMMAND_PUBLIC_FORBIDDEN_KEYS, f"forbidden field {path}.{key}"
            _assert_no_command_public_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_command_public_forbidden_fields(item, f"{path}[{index}]")


def _fake_herdr_state_with_terminal(config: Any) -> tuple[list[Space], list[Worker]]:
    return [], [
        Worker(
            id="w-terminal",
            name="Terminal",
            status="active",
            space_id="s-1",
            meta={
                "pane_id": "p-1",
                "terminal_id": "t-1",
                "pid": 123,
                "tty": "/dev/pts/0",
                "pty": "pts",
                "tmux": "sess",
                "screen_session": "scr",
                "window_id": "win-1",
                "tab_id": "tab-1",
                "argv": ["bash"],
                "shell": "bash",
                "command": "python app.py",
                "route": "telegram",
                "routes": ["r1"],
                "delivery": {"id": 1},
                "deliveries": [{"id": 2}],
                "token": "secret",
                "tokens": ["t1"],
                "connector": {"x": 1},
                "connectors": [{"y": 2}],
                "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
                "agent_session": {"value": "sess-1"},
                "session_id": "session-1",
                "safe": "kept",
            },
            backend_target={"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
        )
    ]


def test_cli_command_read_snapshot_strips_command_public_terminal_fields(
    capsys, monkeypatch
) -> None:
    """Command-public read_snapshot strips terminal/connector identifiers while
    leaving the ordinary snapshot --json output unchanged.
    """
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state_with_terminal)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["action"] == "read_snapshot"
    assert "request_id" in payload
    meta = payload["result"]["snapshot"]["workers"][0]["meta"]
    for key in _COMMAND_PUBLIC_FORBIDDEN_KEYS:
        assert key not in meta, key
    assert meta["safe"] == "kept"

    # The standalone snapshot path is public too, so backend identifiers are absent there as well.
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 0
    snapshot = json.loads(captured2.out)
    assert snapshot["schema_version"] == 2
    snap_meta = snapshot["workers"][0]["meta"]
    for key in ("pane_id", "terminal_id", "backend_target", "agent_session", "session_id"):
        assert key not in snap_meta
    assert snap_meta["safe"] == "kept"


def test_cli_command_forbidden_field_rejects_before_backend_and_store(
    capsys, monkeypatch
) -> None:
    """A contract-invalid request must be rejected before any backend or store call."""
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before validation")

    def guarded_reserve_receipt(*args: Any, **kwargs: Any) -> Any:
        calls.append("reserve_receipt")
        raise AssertionError("reserve_command_receipt called before validation")

    def guarded_save_receipt(*args: Any, **kwargs: Any) -> None:
        calls.append("save_receipt")
        raise AssertionError("save_command_receipt called before validation")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.reserve_command_receipt", guarded_reserve_receipt)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded_save_receipt)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "rej-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                    "params": {"pane_id": "leaked"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["request_id"] == "rej-1"
    assert calls == []


def test_cli_command_raw_top_level_forbidden_rejects_before_pipeline(
    capsys, monkeypatch
) -> None:
    """Raw top-level forbidden fields reject before store, projection, or backend work."""
    calls: list[str] = []

    def guarded_reserve_receipt(*args: Any, **kwargs: Any) -> Any:
        calls.append("reserve_receipt")
        raise AssertionError("reserve_command_receipt called before raw validation")

    def guarded_save_receipt(*args: Any, **kwargs: Any) -> None:
        calls.append("save_receipt")
        raise AssertionError("save_command_receipt called before raw validation")

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before raw validation")

    def guarded_project(*args: Any, **kwargs: Any) -> Any:
        calls.append("project")
        raise AssertionError("project_from_observations called before raw validation")

    def guarded_execute(*args: Any, **kwargs: Any) -> Any:
        calls.append("execute")
        raise AssertionError("execute_command called before raw validation")

    def guarded_send(*args: Any, **kwargs: Any) -> Any:
        calls.append("backend")
        raise AssertionError("backend sender called before raw validation")

    monkeypatch.setattr("tendwire.cli.reserve_command_receipt", guarded_reserve_receipt)
    monkeypatch.setattr("tendwire.cli.save_command_receipt", guarded_save_receipt)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.project_from_observations", guarded_project)
    monkeypatch.setattr("tendwire.cli.execute_command", guarded_execute)
    monkeypatch.setattr("tendwire.cli.herdr_send_instruction", guarded_send)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "raw-rej",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                    "pane_id": "leaked",
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["request_id"] is None
    assert "pane_id" in str(payload["error"]["details"])
    assert calls == []


def test_cli_command_backend_unavailable_preserves_request_id(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    """Non-dry-run send_instruction preserves request_id in stdout and receipt."""
    db_path = tmp_path / "req.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="still-live",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-still-live",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="still-live-private",
            )
        ],
    )
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "req-visible",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
    assert payload["request_id"] == "req-visible"

    receipt = get_command_receipt(db_path, "cmd-host", "req-visible", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is False
    cached = json.loads(receipt["result_json"])
    assert cached["request_id"] == "req-visible"
    assert cached["status"] == STATUS_BACKEND_UNAVAILABLE
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")
    assert [binding.private_fingerprint for binding in current] == ["still-live-private"]


def test_cli_command_degraded_observation_returns_uncertain_not_not_found(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "degraded.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="still-live",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-still-live",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="still-live-private",
            )
        ],
    )

    def degraded_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[],
            status="degraded",
            outcome="malformed_json",
            message="Herdr agent observation is not healthy",
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", degraded_observation)
    monkeypatch.setattr(
        "tendwire.cli.herdr_send_instruction",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backend called")),
    )
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "degraded-1",
                    "dry_run": False,
                    "target": {"worker_id": "missing"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_REQUEST_STATE_UNCERTAIN
    receipt = get_command_receipt(db_path, "cmd-host", "degraded-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")
    assert [binding.private_fingerprint for binding in current] == ["still-live-private"]


def test_cli_command_healthy_empty_observation_can_return_not_found(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "empty.db"

    def empty_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[],
            status="healthy",
            outcome="empty_healthy",
        )

    monkeypatch.setattr("tendwire.cli.fetch_herdr_command_observation", empty_observation)
    monkeypatch.setattr(
        "tendwire.cli.herdr_send_instruction",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backend called")),
    )
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "empty-1",
                    "dry_run": False,
                    "target": {"worker_id": "missing"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_NOT_FOUND
    receipt = get_command_receipt(db_path, "cmd-host", "empty-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is False
