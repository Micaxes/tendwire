"""Tests for the Herdr CLI backend adapter contract."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from typing import Any

from tendwire.backends import herdr_cli, herdr_command
from tendwire.backends.herdr_cli import fetch_herdr_state
from tendwire.config import Config
from tendwire.core.commands import (
    STATUS_ACCEPTED,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_REQUEST_STATE_UNCERTAIN,
)
from tendwire.core.projector import project_from_observations


_FORBIDDEN_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "herdres_delivery",
    "pane_id",
    "terminal_id",
    "argv",
    "command",
    "shell",
    "connector",
    "connectors",
}


def _completed(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "workspace", "list", "--json"],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


def _respond(args: Sequence[str], responses: dict[tuple[str, ...], Any]) -> subprocess.CompletedProcess[str] | None:
    """Return a canned response for a herdr command tuple."""
    key = tuple(args)
    if key not in responses:
        return _completed("", returncode=1)
    response = responses[key]
    if isinstance(response, subprocess.CompletedProcess):
        return response
    if isinstance(response, str):
        return _completed(response)
    return _completed(json.dumps(response))


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_FIELDS, f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _send_completed(returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["herdr", "agent", "send", "worker-1", "hello"],
        returncode=returncode,
        stdout="",
        stderr="",
    )


def test_send_instruction_uses_agent_send_argv(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return _send_completed()

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command.subprocess, "run", fake_run)

    envelope = herdr_command.send_instruction(
        config,
        {"worker_id": "worker-1", "terminal_id": "ignored"},
        {"text": "hello"},
    )

    assert envelope.ok is True
    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {"target": {"worker_id": "worker-1"}}
    assert calls == [
        (
            ["herdr", "agent", "send", "worker-1", "hello"],
            {
                "capture_output": True,
                "text": True,
                "check": False,
                "timeout": herdr_command._HERDR_SEND_TIMEOUT_SECONDS,
            },
        )
    ]
    assert "shell" not in calls[0][1]


def test_send_instruction_maps_missing_binary_to_backend_unavailable(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="missing-herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: None)
    monkeypatch.setattr(
        herdr_command,
        "_run_agent_send",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    envelope = herdr_command.send_instruction(config, {"worker_id": "worker-1"}, {"text": "hello"})

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_nonzero_exit_to_backend_failed(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", lambda *args: _send_completed(returncode=2))

    envelope = herdr_command.send_instruction(config, {"worker_id": "worker-1"}, {"text": "hello"})

    assert envelope.ok is False
    assert envelope.status == STATUS_BACKEND_FAILED
    assert envelope.error["details"] == {"exit_code": 2}
    _assert_no_forbidden_fields(envelope.to_dict())


def test_send_instruction_maps_timeout_to_uncertain(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")

    def raise_timeout(*args: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["herdr", "agent", "send"], timeout=5.0)

    monkeypatch.setattr(herdr_command.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_command, "_run_agent_send", raise_timeout)

    envelope = herdr_command.send_instruction(config, {"worker_id": "worker-1"}, {"text": "hello"})

    assert envelope.ok is False
    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    _assert_no_forbidden_fields(envelope.to_dict())

def test_fetch_herdr_state_returns_empty_when_binary_missing() -> None:
    config = Config(host_id="testhost", herdr_bin="definitely-not-a-real-herdr-binary")
    spaces, workers = fetch_herdr_state(config)
    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_cli_failure(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed('{"workers":[{"id":"leaked"}]}', returncode=2),
    )

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_fetch_herdr_state_returns_empty_on_malformed_json(monkeypatch) -> None:
    config = Config(host_id="testhost", herdr_bin="herdr")
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _completed("not json"))

    spaces, workers = fetch_herdr_state(config)

    assert spaces == []
    assert workers == []


def test_sample_herdr_projection_is_neutral_and_fingerprinted(monkeypatch) -> None:
    config = Config(host_id="herdr-host", herdr_bin="herdr")
    sample_payload = {
        "spaces": [
            {
                "id": "space-1",
                "name": "Build",
                "status": "running",
                "status_line": "building package",
                "telegram": "forbidden",
                "chat_id": 111,
                "safe": "space-meta",
            }
        ],
        "workers": [
            {
                "id": "worker-1",
                "name": "Agent One",
                "status": "panic",
                "space_id": "space-1",
                "summary": "crashed",
                "topic_id": 222,
                "message_id": 333,
                "route": "telegram",
                "delivery": {"chat_id": 444},
                "safe": "worker-meta",
            }
        ],
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli,
        "_run_herdr",
        lambda args, cfg: _completed(json.dumps(sample_payload)),
    )

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    assert payload["schema_version"] == 2
    assert len(payload["content_fingerprint"]) == 24
    assert payload["spaces"][0]["status"] == "active"
    assert payload["spaces"][0]["fingerprint"]
    assert payload["spaces"][0]["meta"]["safe"] == "space-meta"
    assert payload["workers"][0]["status"] == "failed"
    assert payload["workers"][0]["fingerprint"]
    assert payload["workers"][0]["meta"]["raw_status"] == "panic"
    assert payload["workers"][0]["meta"]["safe"] == "worker-meta"
    assert payload["attention"][0]["source"] == "worker:worker-1"
    assert payload["attention"][0]["fingerprint"]
    _assert_no_forbidden_fields(payload)


def test_no_flag_workspace_and_agent_lists_preferred_without_json_calls(monkeypatch) -> None:
    """Herdr 0.7.0 no-flag envelopes are used before compatibility --json fallbacks."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Build",
                        "agent_status": "working",
                        "active_tab_id": "tab-1",
                    }
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Coder",
                        "workspace_id": "ws-1",
                        "agent_status": "done",
                        "cwd": "/home/dev",
                    }
                ]
            }
        },
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        if "--json" in args:
            raise AssertionError("--json fallback should not be called after valid no-flag output")
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-1"
    assert spaces[0].name == "Build"
    assert spaces[0].status == "active"
    assert spaces[0].meta.get("active_tab_id") == "tab-1"
    assert spaces[0].meta.get("raw_status") == "working"
    assert len(workers) == 1
    assert workers[0].id == "sess-1"
    assert workers[0].name == "Coder"
    assert workers[0].status == "closed"
    assert workers[0].space_id == "ws-1"
    assert workers[0].meta.get("cwd") == "/home/dev"
    assert workers[0].meta.get("raw_status") == "done"


def test_json_workspace_list_fallback_when_no_flag_fails(monkeypatch) -> None:
    """Compatibility --json workspace list is tried when no-flag output fails."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): _completed("usage", returncode=2),
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-json", "label": "Compat", "agent_status": "idle"}]
            }
        },
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls[:2] == [("workspace", "list"), ("workspace", "list", "--json")]
    assert len(spaces) == 1
    assert spaces[0].id == "ws-json"
    assert spaces[0].name == "Compat"
    assert workers == []


def test_json_agent_list_fallback_when_no_flag_is_malformed(monkeypatch) -> None:
    """Compatibility --json agent list is tried when no-flag output is malformed."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): _completed("not json"),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-json"},
                        "agent": "CompatAgent",
                        "workspace_id": "ws-1",
                    }
                ]
            }
        },
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Sequence[str], cfg: Config) -> subprocess.CompletedProcess[str] | None:
        calls.append(tuple(args))
        return _respond(args, responses)

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    spaces, workers = fetch_herdr_state(config)

    assert calls == [("workspace", "list"), ("agent", "list"), ("agent", "list", "--json")]
    assert spaces == []
    assert len(workers) == 1
    assert workers[0].id == "sess-json"
    assert workers[0].name == "CompatAgent"
    assert workers[0].space_id == "ws-1"


def test_result_envelopes_parse(monkeypatch) -> None:
    """result.workspaces, result.agents, and result.panes envelopes parse."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [{"workspace_id": "ws-result", "label": "ResultSpace", "agent_status": "idle"}]
            }
        },
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-result"}, "agent": "Agent"}]}
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(spaces) == 1
    assert spaces[0].id == "ws-result"
    assert len(workers) == 1
    assert workers[0].id == "sess-result"


def test_pane_fallback_only_when_agent_list_yields_none(monkeypatch) -> None:
    """Pane list fallback runs only when agents are empty and only for agent-bearing panes."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-agent", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-plain", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "pane-agent"
    assert workers[0].name == "Runner"


def test_pane_fallback_skipped_when_agents_present(monkeypatch) -> None:
    """Pane list is not used as a fallback when agent list already produced workers."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {"agents": [{"agent_session": {"value": "sess-1"}, "agent": "Agent"}]}
        },
        ("pane", "list"): {"result": {"panes": [{"pane_id": "sess-1", "agent": "Agent"}]}},
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "sess-1"


def test_agent_and_pane_duplicates_emit_one_worker(monkeypatch) -> None:
    """A worker described by both agent and pane payloads is deduplicated."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): _completed("", returncode=1),
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {
            "result": {
                "panes": [
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                    {"pane_id": "pane-1", "agent": "Runner", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 1
    assert workers[0].id == "pane-1"


def test_repeated_agent_names_with_distinct_ids_remain_distinct(monkeypatch) -> None:
    """Workers with the same display name but different session ids are kept."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): _completed("", returncode=1),
        ("workspace", "list"): _completed("", returncode=1),
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-a"}, "agent": "Coder", "workspace_id": "ws-1"},
                    {"agent_session": {"value": "sess-b"}, "agent": "Coder", "workspace_id": "ws-1"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert len(workers) == 2
    assert {w.id for w in workers} == {"sess-a", "sess-b"}
    assert workers[0].id < workers[1].id


def test_status_aliases_for_live_herdr(monkeypatch) -> None:
    """Working maps to active, done maps to closed, and raw_status is preserved."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "ws-1", "label": "Space", "agent_status": "working"},
                    {"workspace_id": "ws-2", "label": "Space2", "agent_status": "responding"},
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {"agent_session": {"value": "sess-1"}, "agent": "Agent", "workspace_id": "ws-1", "agent_status": "done"},
                    {"agent_session": {"value": "sess-2"}, "agent": "Agent", "workspace_id": "ws-2", "agent_status": "awaiting-input"},
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)

    assert spaces[0].status == "active"
    assert spaces[0].meta.get("raw_status") == "working"
    assert spaces[1].status == "waiting"
    assert spaces[1].meta.get("raw_status") == "responding"
    by_id = {w.id: w for w in workers}
    assert by_id["sess-1"].status == "closed"
    assert by_id["sess-1"].meta.get("raw_status") == "done"
    assert by_id["sess-2"].status == "waiting"
    assert by_id["sess-2"].meta.get("raw_status") == "awaiting-input"


def test_forbidden_connector_fields_stripped_in_herdr_070(monkeypatch) -> None:
    """Connector/delivery fields are stripped from live Herdr 0.7.0 payloads."""
    config = Config(host_id="testhost", herdr_bin="herdr")
    responses = {
        ("workspace", "list", "--json"): {
            "result": {
                "workspaces": [
                    {
                        "workspace_id": "ws-1",
                        "label": "Space",
                        "agent_status": "idle",
                        "telegram": "leaked",
                        "chat_id": 123,
                    }
                ]
            }
        },
        ("agent", "list", "--json"): {
            "result": {
                "agents": [
                    {
                        "agent_session": {"value": "sess-1"},
                        "agent": "Agent",
                        "workspace_id": "ws-1",
                        "agent_status": "active",
                        "route": "telegram",
                        "delivery": {"topic_id": 456},
                        "herdres_delivery": {"message_id": 789},
                    }
                ]
            }
        },
    }
    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", lambda args, cfg: _respond(args, responses))

    spaces, workers = fetch_herdr_state(config)
    snapshot = project_from_observations(config, spaces=spaces, workers=workers)
    payload = json.loads(snapshot.to_json())

    _assert_no_forbidden_fields(payload)
    assert "telegram" not in payload["spaces"][0]["meta"]
    assert "route" not in payload["workers"][0]["meta"]
