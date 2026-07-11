"""Tests for public turn and pending-interaction contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
import pytest

from tendwire.config import Config
from tendwire.core.models import (
    AttentionSignal,
    Snapshot,
    SuggestedAction,
    Worker,
    sanitize_canonical_turn_text,
)
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import (
    InteractionChoice,
    PendingInteraction,
    Turn,
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    build_turn_content_descriptor,
    build_turn_content_page,
    content_cursor,
    content_revision,
    content_segment_id,
    decode_content_cursor,
    payload_to_json,
    pending_from_snapshot,
    pending_payload_from_snapshot,
    turns_from_snapshot,
    turns_payload_from_snapshot,
    project_turn_content,
    segment_canonical_text,
)


_FORBIDDEN_FIELDS = {
    "telegram",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "thread_id",
    "thread_ids",
    "token",
    "tokens",
    "bot_token",
    "bot_tokens",
    "auth",
    "auth_token",
    "auth_tokens",
    "authorization",
    "authorization_header",
    "authorization_headers",
    "bearer_token",
    "bearer_tokens",
    "cookie",
    "cookies",
    "credential",
    "credentials",
    "delivery",
    "deliveries",
    "route",
    "routes",
    "connector",
    "connectors",
    "herdres_delivery",
    "command",
    "command_arg",
    "command_args",
    "command_argv",
    "command_argvs",
    "command_line",
    "command_lines",
    "command_payload",
    "command_text",
    "command_texts",
    "backend_target",
    "backend_targets",
    "terminal_id",
    "terminal_ids",
    "pane_id",
    "pane_ids",
    "tab_id",
    "tab_ids",
    "window_id",
    "window_ids",
    "tty",
    "pty",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "process",
    "tmux",
    "tmux_session",
    "tmux_sessions",
    "tmux_window",
    "tmux_windows",
    "tmux_pane",
    "tmux_panes",
    "screen",
    "screen_session",
    "screen_sessions",
    "screen_window",
    "screen_windows",
    "agent_session",
    "agent_sessions",
    "session_id",
    "session_ids",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private",
    "private_binding",
    "private_bindings",
    "private_fingerprint",
    "private_fingerprints",
    "argv",
    "args",
    "env",
    "raw_arg",
    "raw_args",
    "raw_argv",
    "raw_argvs",
    "stderr",
    "stdout",
    "stdin",
    "secret",
    "secrets",
    "password",
    "passwords",
    "api_keys",
    "api_key",
    "raw_command",
    "raw_command_line",
    "raw_command_lines",
    "raw_payload",
    "raw_control",
    "shell_command",
    "shell_commands",
    "terminal_control",
    "control_sequence",
    "escape_sequence",
    "ansi_escape",
}
_FORBIDDEN_FIELD_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_FIELDS}


def _is_forbidden_test_key(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_")
    return normalized in _FORBIDDEN_FIELDS or normalized.replace("_", "") in _FORBIDDEN_FIELD_COMPACT


def _assert_no_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not _is_forbidden_test_key(key), f"forbidden field {path}.{key}"
            _assert_no_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_forbidden_fields(item, f"{path}[{index}]")


def _assert_no_private_sentinels(value: Any) -> None:
    encoded = json.dumps(value, sort_keys=True)
    assert "sentinel-" not in encoded
    assert "private-" not in encoded


def test_turn_roundtrip_sanitizes_fields_and_ignores_volatile_timestamps() -> None:
    turn = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Summarizing work",
        started_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        completed_at=None,
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={
            "safe": {"nested": True, "pane_id": "pane-private"},
            "updated_at": "2026-01-01T00:00:02+00:00",
            "chat_id": 123,
            "token": "secret",
        },
    )
    same_logical_turn = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Summarizing work",
        started_at="2026-01-02T00:00:00+00:00",
        updated_at="2026-01-02T00:00:01+00:00",
        completed_at="2026-01-02T00:00:02+00:00",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": {"nested": True}, "updated_at": "2026-01-02T00:00:02+00:00"},
    )
    changed_summary = Turn(
        host_id="turn-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="running",
        kind="message",
        title="Worker One",
        summary="Different public summary",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": {"nested": True}},
    )

    payload = json.loads(turn.to_json())

    assert payload["schema_version"] == 1
    assert payload["status"] == "active"
    assert payload["kind"] == "message"
    assert payload["meta"] == {
        "safe": {"nested": True},
        "updated_at": "2026-01-01T00:00:02+00:00",
    }
    assert same_logical_turn.id == turn.id
    assert same_logical_turn.fingerprint == turn.fingerprint
    assert changed_summary.id == turn.id
    assert changed_summary.fingerprint != turn.fingerprint
    assert Turn.from_json(turn.to_json()).to_dict() == payload
    _assert_no_forbidden_fields(payload)


def test_pending_interaction_roundtrip_sanitizes_choices_and_ignores_timestamps() -> None:
    choice = InteractionChoice(
        label="Approve",
        value={"decision": "yes", "backend_target": "agent-private"},
        params={"safe": "kept", "route": "telegram", "terminal_id": "term-private"},
    )
    pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm",
        question="Delete generated files?",
        choices=[choice],
        status="pending",
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:01+00:00",
        expires_at="2026-01-01T00:05:00+00:00",
        meta={"source": "attention", "message_id": 99},
    )
    same_logical_pending = PendingInteraction(
        host_id="pending-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="confirm_destructive_action",
        question="Delete generated files?",
        choices=[choice],
        status="open",
        created_at="2026-01-02T00:00:00+00:00",
        updated_at="2026-01-02T00:00:01+00:00",
        expires_at="2026-01-02T00:05:00+00:00",
        meta={"source": "attention"},
    )

    payload = json.loads(pending.to_json())

    assert payload["schema_version"] == 1
    assert payload["kind"] == "confirm_destructive_action"
    assert payload["status"] == "open"
    assert payload["choices"] == [{"choice_id": choice.choice_id, "label": "Approve"}]
    assert "decision" not in json.dumps(payload)
    assert pending.id == same_logical_pending.id
    assert pending.fingerprint == same_logical_pending.fingerprint
    assert PendingInteraction.from_json(pending.to_json()).to_dict() == payload
    _assert_no_forbidden_fields(payload)


def test_turn_pending_and_choice_preserve_public_connector_text_before_fingerprints() -> None:
    dirty_turn = Turn(
        host_id="text-host",
        worker_id="worker-1",
        status="waiting",
        kind="message",
        title="outbox",
        summary="herdres queue",
        source="herdres",
        meta={
            "note": "outbox",
            "safe": "kept",
            "outbox": "herdres",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
    )
    clean_turn = Turn(
        host_id="text-host",
        worker_id="worker-1",
        status="waiting",
        kind="message",
        title="outbox",
        summary="herdres queue",
        source="herdres",
        meta={"note": "outbox", "safe": "kept", "nested": {"safe_nested": "kept"}},
    )
    dirty_choice = InteractionChoice(
        choice_id="telegram delivery",
        label="herdres action",
        value="outbox",
        description="herdres",
        params={
            "safe": "kept",
            "note": "outbox",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
    )
    clean_choice = InteractionChoice(
        choice_id="telegram delivery",
        label="herdres action",
        value="outbox",
        description="herdres",
        params={"safe": "kept", "note": "outbox", "nested": {"safe_nested": "kept"}},
    )
    dirty_pending = PendingInteraction(
        host_id="text-host",
        worker_id="worker-1",
        question="Review herdres outbox?",
        kind="approval",
        choices=[dirty_choice],
        meta={
            "source": "herdres",
            "safe": "kept",
            "note": "outbox",
            "outbox": "herdres",
            "telegram.delivery": "leaked route",
            "nested": {"safe_nested": "kept", "herdres.route": "leaked route"},
        },
    )
    clean_pending = PendingInteraction(
        host_id="text-host",
        worker_id="worker-1",
        question="Review herdres outbox?",
        kind="approval",
        choices=[clean_choice],
        meta={"source": "herdres", "safe": "kept", "note": "outbox", "nested": {"safe_nested": "kept"}},
    )

    turn_payload = dirty_turn.to_dict()
    pending_payload = dirty_pending.to_dict()
    encoded = json.dumps({"turn": turn_payload, "pending": pending_payload}, sort_keys=True).lower()

    assert turn_payload["title"] == "outbox"
    assert turn_payload["summary"] == "herdres queue"
    assert turn_payload["source"] == "herdres"
    assert turn_payload["meta"] == {"note": "outbox", "safe": "kept", "nested": {"safe_nested": "kept"}}
    assert dirty_turn.id == clean_turn.id
    assert dirty_turn.fingerprint == clean_turn.fingerprint
    assert dirty_choice.to_dict() == clean_choice.to_dict()
    assert pending_payload["question"] == "Review herdres outbox?"
    assert pending_payload["meta"] == {
        "source": "herdres",
        "safe": "kept",
        "note": "outbox",
        "nested": {"safe_nested": "kept"},
    }
    assert dirty_pending.id == clean_pending.id
    assert dirty_pending.fingerprint == clean_pending.fingerprint
    assert "telegram delivery" not in encoded
    assert "herdres" in encoded
    assert "outbox" in encoded
    assert "telegram.delivery" not in encoded
    assert "herdres.route" not in encoded
    assert "leaked route" not in encoded
    for forbidden in (
        "backend target",
        "pane id",
        "session id",
        "terminal id",
        "chat id",
        "message id",
        "bot token",
    ):
        assert forbidden not in encoded
    _assert_no_forbidden_fields(turn_payload)
    _assert_no_forbidden_fields(pending_payload)


def test_turn_pending_identity_and_worker_fingerprint_values_are_public_safe() -> None:
    dirty_turn = Turn(
        host_id="identity-host",
        worker_id="pane-private",
        worker_fingerprint="herdres private fingerprint",
        space_id="target-private",
        status="waiting",
        kind="task",
        source="worker:pane-private",
        origin_command_id="raw command private",
        meta={"safe": "kept"},
    )
    clean_turn = Turn(
        host_id="identity-host",
        worker_id=dirty_turn.worker_id,
        space_id=dirty_turn.space_id,
        status="waiting",
        kind="task",
        source="snapshot",
        meta={"safe": "kept"},
    )
    dirty_pending = PendingInteraction(
        host_id="identity-host",
        worker_id="pane-private",
        worker_fingerprint="backend target private fingerprint",
        space_id="target-private",
        kind="choice",
        question="Choose next action",
        meta={"source": "attention"},
    )
    clean_pending = PendingInteraction(
        host_id="identity-host",
        worker_id=dirty_pending.worker_id,
        space_id=dirty_pending.space_id,
        kind="choice",
        question="Choose next action",
        meta={"source": "attention"},
    )

    payload = {
        "turn": dirty_turn.to_dict(),
        "pending": dirty_pending.to_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert dirty_turn.worker_id.startswith("worker-")
    assert dirty_turn.space_id is not None
    assert dirty_turn.space_id.startswith("space-")
    assert dirty_turn.worker_fingerprint is None
    assert dirty_turn.origin_command_id is None
    assert dirty_turn.source == "snapshot"
    assert dirty_turn.id == clean_turn.id
    assert dirty_turn.fingerprint == clean_turn.fingerprint
    assert dirty_pending.worker_id.startswith("worker-")
    assert dirty_pending.space_id is not None
    assert dirty_pending.space_id.startswith("space-")
    assert dirty_pending.worker_fingerprint is None
    assert dirty_pending.id == clean_pending.id
    assert dirty_pending.fingerprint == clean_pending.fingerprint
    for forbidden in ("private", "herdres", "backend target", "pane-private", "target-private"):
        assert forbidden not in encoded
    _assert_no_forbidden_fields(payload)


def test_turn_pending_to_dict_resanitizes_mutable_public_maps() -> None:
    choice = InteractionChoice(
        label="Approve",
        value={"safe": "kept"},
        params={"safe_choice": "kept"},
    )
    turn = Turn(
        host_id="mutable-host",
        worker_id="worker-1",
        status="waiting",
        kind="task",
        meta={"safe_turn": "kept"},
    )
    pending = PendingInteraction(
        host_id="mutable-host",
        worker_id="worker-1",
        question="Choose next action",
        choices=[choice],
        meta={"safe_pending": "kept"},
    )

    choice.params["note"] = "herdres outbox"
    turn.meta["note"] = "herdres outbox"
    pending.meta["note"] = "herdres outbox"

    payload = {
        "choice": choice.to_dict(),
        "turn": turn.to_dict(),
        "pending": pending.to_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert payload["choice"] == {"choice_id": choice.choice_id, "label": "Approve"}
    assert payload["turn"]["meta"] == {"safe_turn": "kept", "note": "herdres outbox"}
    assert payload["pending"]["meta"] == {"safe_pending": "kept", "note": "herdres outbox"}
    assert "herdres outbox" in encoded
    _assert_no_forbidden_fields(payload)


def test_turn_pending_and_choice_strip_pr5_private_fields_before_fingerprints() -> None:
    dirty_meta = {
        "safe": "kept",
        "nested": {
            "safe_nested": "kept",
            "processId": "sentinel-nested-process",
            "terminal-id": "sentinel-nested-terminal",
        },
        "tty": "sentinel-tty",
        "pty": "sentinel-pty",
        "pid": "sentinel-pid",
        "process_id": "sentinel-process",
        "tmux": "sentinel-tmux",
        "screen_session": "sentinel-screen",
        "window_id": "sentinel-window",
        "tab_id": "sentinel-tab",
        "pane_id": "sentinel-pane",
        "terminal_id": "sentinel-terminal",
        "backend_target": "sentinel-backend",
        "session_id": "sentinel-session",
        "messageIds": "sentinel-message-ids",
        "terminalIds": "sentinel-terminal-ids",
        "terminal": "sentinel-terminal-object",
        "telegramMessageId": "sentinel-telegram-message",
        "routeId": "sentinel-route-id",
        "connectorId": "sentinel-connector-id",
        "tmuxPaneId": "sentinel-tmux-pane-id",
        "screenWindowId": "sentinel-screen-window-id",
        "agentSessionId": "sentinel-agent-session-id",
        "session": "sentinel-session-object",
        "privateFingerprints": "sentinel-private-fingerprints",
        "passwords": "sentinel-passwords",
        "privateBinding": "sentinel-private-binding",
        "authToken": "sentinel-auth",
    }
    clean_meta = {"safe": "kept", "nested": {"safe_nested": "kept"}}
    dirty_turn = Turn(
        host_id="pr5-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="waiting",
        kind="task",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta=dirty_meta,
    )
    clean_turn = Turn(
        host_id="pr5-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="waiting",
        kind="task",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta=clean_meta,
    )
    dirty_choice = InteractionChoice(
        label="Approve",
        value={
            "decision": "yes",
            "backendTarget": "sentinel-choice-backend",
            "nested": {"safe": "kept", "session-id": "sentinel-choice-session"},
        },
        description={"text": "safe description", "terminalId": "sentinel-description-terminal"},
        params={
            "safe": "kept",
            "tty": "sentinel-choice-tty",
            "nested": {"safe": "kept", "processId": "sentinel-choice-process"},
        },
    )
    clean_choice = InteractionChoice(
        label="Approve",
        value={"decision": "yes", "nested": {"safe": "kept"}},
        description={"text": "safe description"},
        params={"safe": "kept", "nested": {"safe": "kept"}},
    )
    dirty_pending = PendingInteraction(
        host_id="pr5-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="approval",
        question="Approve this action?",
        choices=[dirty_choice],
        meta={"source": "attention", **dirty_meta},
    )
    clean_pending = PendingInteraction(
        host_id="pr5-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="approval",
        question="Approve this action?",
        choices=[clean_choice],
        meta={"source": "attention", **clean_meta},
    )

    turn_payload = json.loads(dirty_turn.to_json())
    pending_payload = json.loads(dirty_pending.to_json())

    assert turn_payload["host_id"] == "pr5-host"
    assert turn_payload["worker_id"] == "worker-1"
    assert turn_payload["worker_fingerprint"] == "worker-fp"
    assert turn_payload["space_id"] == "space-1"
    assert turn_payload["source"] == "worker:worker-1"
    assert turn_payload["origin_command_id"] == "cmd-public"
    assert turn_payload["meta"] == clean_meta
    assert dirty_turn.id == clean_turn.id
    assert dirty_turn.fingerprint == clean_turn.fingerprint
    assert dirty_choice.choice_id == clean_choice.choice_id
    assert dirty_choice.to_dict() == {
        "choice_id": clean_choice.choice_id,
        "label": "Approve",
    }
    assert dirty_pending.id == clean_pending.id
    assert dirty_pending.fingerprint == clean_pending.fingerprint
    assert pending_payload["meta"] == {"source": "attention", **clean_meta}
    _assert_no_forbidden_fields(turn_payload)
    _assert_no_forbidden_fields(pending_payload)
    _assert_no_private_sentinels(turn_payload)
    _assert_no_private_sentinels(pending_payload)


def test_turn_pending_recompute_supplied_ids_and_filter_raw_command_choice_values() -> None:
    dirty_turn = Turn(
        id="sentinel-private-turn-id",
        fingerprint="sentinel-private-turn-fingerprint",
        host_id="id-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="waiting",
        kind="task",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": "kept", "commandLine": "sentinel-command-line"},
    )
    clean_turn = Turn(
        host_id="id-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        status="waiting",
        kind="task",
        source="worker:worker-1",
        origin_command_id="cmd-public",
        meta={"safe": "kept"},
    )
    dirty_choice = InteractionChoice(
        label="Run diagnostic",
        value="tendwire snapshot --json --token sentinel-choice-token",
        params={"safe": "kept", "rawCommandLine": "sentinel-choice-command-line"},
    )
    clean_choice = InteractionChoice(
        label="Run diagnostic",
        params={"safe": "kept"},
    )
    dirty_pending = PendingInteraction(
        id="sentinel-private-pending-id",
        fingerprint="sentinel-private-pending-fingerprint",
        host_id="id-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="choice",
        question="Choose next action",
        choices=[dirty_choice],
        meta={"source": "attention", "shellCommand": "sentinel-pending-shell-command"},
    )
    clean_pending = PendingInteraction(
        host_id="id-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fp",
        space_id="space-1",
        kind="choice",
        question="Choose next action",
        choices=[clean_choice],
        meta={"source": "attention"},
    )

    turn_payload = dirty_turn.to_dict()
    pending_payload = dirty_pending.to_dict()

    assert dirty_turn.id == clean_turn.id
    assert dirty_turn.fingerprint == clean_turn.fingerprint
    assert turn_payload["id"] == clean_turn.id
    assert turn_payload["fingerprint"] == clean_turn.fingerprint
    assert dirty_choice.choice_id == clean_choice.choice_id
    assert dirty_choice.to_dict() == clean_choice.to_dict()
    assert dirty_pending.id == clean_pending.id
    assert dirty_pending.fingerprint == clean_pending.fingerprint
    assert pending_payload["id"] == clean_pending.id
    assert pending_payload["fingerprint"] == clean_pending.fingerprint
    assert "value" not in pending_payload["choices"][0]
    _assert_no_forbidden_fields(turn_payload)
    _assert_no_forbidden_fields(pending_payload)
    _assert_no_private_sentinels(turn_payload)
    _assert_no_private_sentinels(pending_payload)


def test_turn_projection_from_snapshot_is_public_safe_and_timestamp_stable() -> None:
    config = Config(host_id="projection-host")
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "running",
                "space_id": "space-1",
                "last_seen_at": "2026-01-01T00:00:00+00:00",
                "summary": "Current work",
                "backend_target": {"value": "agent-private"},
                "meta": {
                    "origin_command_id": "cmd-public",
                    "pane_id": "pane-private",
                    "safe": "kept",
                },
            }
        ],
    )
    later_snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "running",
                "space_id": "space-1",
                "last_seen_at": "2026-01-02T00:00:00+00:00",
                "summary": "Current work",
                "meta": {"origin_command_id": "cmd-public", "safe": "kept"},
            }
        ],
    )

    turn = turns_from_snapshot(snapshot)[0]
    later_turn = turns_from_snapshot(later_snapshot)[0]
    payload = turn.to_dict()

    assert payload["host_id"] == "projection-host"
    assert payload["worker_id"] == "worker-1"
    assert payload["source"] == "worker:worker-1"
    assert payload["status"] == "active"
    assert payload["kind"] == "task"
    assert payload["title"] == "Worker One"
    assert payload["origin_command_id"] == "cmd-public"
    assert payload["meta"] == {
        "origin_command_id": "cmd-public",
        "raw_status": "running",
        "safe": "kept",
    }
    assert later_turn.id == turn.id
    assert later_turn.fingerprint == turn.fingerprint
    assert "agent-private" not in json.dumps(payload)
    assert "pane-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_pending_projection_is_conservative_and_uses_attention_signals() -> None:
    config = Config(host_id="pending-projection-host")
    generic_waiting = project_from_raw(
        config,
        workers=[
            {
                "id": "waiting",
                "name": "Waiting",
                "status": "waiting",
                "summary": "waiting for response",
            }
        ],
    )
    approval_snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "approval",
                "name": "Approval",
                "status": "pending",
                "space_id": "space-1",
                "summary": "human approval required before continuing",
                "meta": {"requires_approval": True, "backend_target": {"value": "agent-private"}},
            }
        ],
    )

    pending = pending_from_snapshot(approval_snapshot)

    assert pending_from_snapshot(generic_waiting) == []
    assert len(pending) == 1
    payload = pending[0].to_dict()
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "pending-projection-host"
    assert payload["worker_id"] == "approval"
    assert payload["space_id"] == "space-1"
    assert payload["kind"] == "approval"
    assert payload["status"] == "open"
    assert payload["choices"] == []
    assert "approval" in payload["question"]
    assert "agent-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_pending_projection_reuses_public_suggested_actions_as_choices() -> None:
    action = SuggestedAction(
        action_id="approve-action",
        label="Approve",
        tendwire_action="approve",
        params={
            "worker_id": "worker-1",
            "safe": "kept",
            "route": "telegram",
            "terminal_id": "term-private",
        },
    )
    signal = AttentionSignal(
        kind="worker_status",
        severity="warning",
        status="waiting",
        reason="Approval required before continuing",
        source="worker:worker-1",
        updated_at="2026-01-01T00:00:00+00:00",
        suggested_actions=[action],
        meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True, "chat_id": 123},
        host_id="choice-host",
    )
    snapshot = Snapshot(
        host_id="choice-host",
        updated_at="2026-01-01T00:00:01+00:00",
        workers=[Worker(id="worker-1", name="Worker One", status="waiting", space_id="space-1")],
        attention=[signal],
    )

    pending = pending_from_snapshot(snapshot)
    payload = pending[0].to_dict()

    assert len(pending) == 1
    assert payload["kind"] == "approval"
    assert payload["choices"] == [
        {
            "choice_id": pending[0].choices[0].choice_id,
            "label": "Approve",
        }
    ]
    assert payload["choices"][0]["choice_id"].startswith("choice-")
    assert "approve-action" not in json.dumps(payload)
    assert payload["meta"]["needs_human"] is True
    assert "term-private" not in json.dumps(payload)
    _assert_no_forbidden_fields(payload)


def test_pending_projection_omits_raw_command_suggested_action_material_before_fingerprints() -> None:
    def _snapshot(raw_command: str) -> Snapshot:
        return Snapshot(
            host_id="raw-command-choice-host",
            updated_at="2026-01-01T00:00:00+00:00",
            workers=[
                Worker(
                    id="worker-1",
                    name="Worker One",
                    status="waiting",
                    space_id="space-1",
                    summary="waiting for action",
                )
            ],
            attention=[
                AttentionSignal(
                    kind="worker_status",
                    severity="warning",
                    status="waiting",
                    reason="Choose next action",
                    source="worker:worker-1",
                    updated_at="2026-01-01T00:00:00+00:00",
                    suggested_actions=[
                        SuggestedAction(
                            label="Run diagnostic",
                            command=raw_command,
                            params={
                                "safe_choice": "kept",
                                "commandLine": "sentinel-action-command-line",
                            },
                        )
                    ],
                    meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True},
                    host_id="raw-command-choice-host",
                )
            ],
        )

    snapshot_a = _snapshot("tendwire snapshot --json --token sentinel-action-token-a")
    snapshot_b = _snapshot("tendwire snapshot --json --token sentinel-action-token-b")
    pending_a = pending_from_snapshot(snapshot_a)[0]
    pending_b = pending_from_snapshot(snapshot_b)[0]
    wrapper_a = pending_payload_from_snapshot(snapshot_a)
    wrapper_b = pending_payload_from_snapshot(snapshot_b)
    payload = pending_a.to_dict()

    assert pending_a.id == pending_b.id
    assert pending_a.fingerprint == pending_b.fingerprint
    assert wrapper_a["content_fingerprint"] == wrapper_b["content_fingerprint"]
    assert payload["kind"] == "choice"
    assert payload["choices"] == [
        {
            "choice_id": pending_b.choices[0].choice_id,
            "label": "Run diagnostic",
        }
    ]
    _assert_no_forbidden_fields(payload)
    _assert_no_forbidden_fields(wrapper_a)
    _assert_no_private_sentinels(payload)
    _assert_no_private_sentinels(wrapper_a)


def test_pending_projection_omits_command_alias_values_before_public_fingerprints() -> None:
    def _snapshot(raw_command: str, private_suffix: str) -> Snapshot:
        return Snapshot(
            host_id="command-alias-choice-host",
            updated_at="2026-01-01T00:00:00+00:00",
            workers=[
                Worker(
                    id="worker-1",
                    name="Worker One",
                    status="waiting",
                    space_id="space-1",
                    summary="waiting for action",
                )
            ],
            attention=[
                AttentionSignal(
                    kind="worker_status",
                    severity="warning",
                    status="waiting",
                    reason="Choose next action",
                    source="worker:worker-1",
                    updated_at="2026-01-01T00:00:00+00:00",
                    suggested_actions=[
                        {
                            "label": "Run diagnostic",
                            "command": raw_command,
                            "terminal_id": f"sentinel-terminal-{private_suffix}",
                            "backendTarget": f"sentinel-backend-{private_suffix}",
                            "session-id": f"sentinel-session-{private_suffix}",
                            "params": {
                                "safe_choice": "kept",
                                "commandLine": f"sentinel-command-line-{private_suffix}",
                                "token": f"sentinel-token-{private_suffix}",
                                "secret": f"sentinel-secret-{private_suffix}",
                            },
                        }
                    ],
                    meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True},
                    host_id="command-alias-choice-host",
                )
            ],
        )

    snapshot_a = _snapshot("sentinel-safe-looking-command-alias-a", "a")
    snapshot_b = _snapshot("sentinel-safe-looking-command-alias-b", "b")
    pending_a = pending_from_snapshot(snapshot_a)[0]
    pending_b = pending_from_snapshot(snapshot_b)[0]
    wrapper_a = pending_payload_from_snapshot(snapshot_a)
    wrapper_b = pending_payload_from_snapshot(snapshot_b)
    payload = pending_a.to_dict()

    assert pending_a.id == pending_b.id
    assert pending_a.fingerprint == pending_b.fingerprint
    assert wrapper_a["content_fingerprint"] == wrapper_b["content_fingerprint"]
    assert payload["kind"] == "choice"
    assert payload["choices"] == [
        {
            "choice_id": pending_b.choices[0].choice_id,
            "label": "Run diagnostic",
        }
    ]
    assert "value" not in wrapper_a["pending_interactions"][0]["choices"][0]
    _assert_no_forbidden_fields(payload)
    _assert_no_forbidden_fields(wrapper_a)
    _assert_no_private_sentinels(payload)
    _assert_no_private_sentinels(wrapper_a)


def test_pending_projection_keeps_safe_explicit_action_value_with_forbidden_command_alias() -> None:
    snapshot = Snapshot(
        host_id="explicit-action-choice-host",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="waiting",
                space_id="space-1",
                summary="approval required",
            )
        ],
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Approval required before continuing",
                source="worker:worker-1",
                updated_at="2026-01-01T00:00:00+00:00",
                suggested_actions=[
                    {
                        "action_id": "approve-action",
                        "label": "Approve",
                        "command": "sentinel-forbidden-command-alias",
                        "tendwire_action": "approve",
                        "terminal_id": "sentinel-terminal",
                        "backendTarget": "sentinel-backend",
                        "session-id": "sentinel-session",
                        "params": {
                            "safe_choice": "kept",
                            "commandLine": "sentinel-command-line",
                        },
                    }
                ],
                meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True},
                host_id="explicit-action-choice-host",
            )
        ],
    )

    pending = pending_from_snapshot(snapshot)[0]
    wrapper = pending_payload_from_snapshot(snapshot)
    payload = pending.to_dict()

    assert payload["kind"] == "approval"
    assert payload["choices"] == [
        {
            "choice_id": pending.choices[0].choice_id,
            "label": "Approve",
        }
    ]
    assert payload["choices"][0]["choice_id"].startswith("choice-")
    assert "approve-action" not in json.dumps(wrapper)
    assert "value" not in wrapper["pending_interactions"][0]["choices"][0]
    _assert_no_forbidden_fields(payload)
    _assert_no_forbidden_fields(wrapper)
    _assert_no_private_sentinels(payload)
    _assert_no_private_sentinels(wrapper)


def test_turn_pending_projectors_strip_pr5_metadata_and_keep_public_fingerprints_stable() -> None:
    dirty_worker_meta = {
        "origin_command_id": "cmd-public",
        "safe_worker": "kept",
        "tty": "sentinel-worker-tty",
        "pty": "sentinel-worker-pty",
        "pid": "sentinel-worker-pid",
        "process_id": "sentinel-worker-process",
        "tmux": "sentinel-worker-tmux",
        "screen_session": "sentinel-worker-screen",
        "window_id": "sentinel-worker-window",
        "tab_id": "sentinel-worker-tab",
        "pane_id": "sentinel-worker-pane",
        "terminal_id": "sentinel-worker-terminal",
        "backend_target": "sentinel-worker-backend",
        "session_id": "sentinel-worker-session",
        "messageIds": "sentinel-worker-message-ids",
        "terminalIds": "sentinel-worker-terminal-ids",
        "terminal": "sentinel-worker-terminal-object",
        "telegramMessageId": "sentinel-worker-telegram-message",
        "routeId": "sentinel-worker-route-id",
        "connectorId": "sentinel-worker-connector-id",
        "tmuxPaneId": "sentinel-worker-tmux-pane-id",
        "screenWindowId": "sentinel-worker-screen-window-id",
        "agentSessionId": "sentinel-worker-agent-session-id",
        "session": "sentinel-worker-session-object",
        "privateFingerprints": "sentinel-worker-private-fingerprints",
        "passwords": "sentinel-worker-passwords",
        "privateBindings": "sentinel-worker-private-bindings",
        "nested": {"safe": "kept", "backendTarget": "sentinel-worker-nested-backend"},
    }
    clean_worker_meta = {
        "origin_command_id": "cmd-public",
        "safe_worker": "kept",
        "nested": {"safe": "kept"},
    }
    dirty_signal_meta = {
        "workerId": "worker-1",
        "space-id": "space-1",
        "needs_human": True,
        "safe_attention": "kept",
        "processId": "sentinel-attention-process",
        "tmux-session": "sentinel-attention-tmux",
        "terminalid": "sentinel-attention-terminal",
        "backendTarget": "sentinel-attention-backend",
        "screenSession": "sentinel-attention-screen",
        "session-id": "sentinel-attention-session",
        "connector": "sentinel-attention-connector",
    }
    clean_signal_meta = {
        "worker_id": "worker-1",
        "space_id": "space-1",
        "needs_human": True,
        "safe_attention": "kept",
    }
    dirty_action_params = {
        "safe_choice": "kept",
        "route": "sentinel-choice-route",
        "terminal-id": "sentinel-choice-terminal",
        "processId": "sentinel-choice-process",
        "authToken": "sentinel-choice-auth",
    }
    clean_action_params = {"safe_choice": "kept"}

    def _snapshot(worker_meta: dict[str, Any], signal_meta: dict[str, Any], action_params: dict[str, Any]) -> Snapshot:
        return Snapshot(
            host_id="projector-pr5-host",
            updated_at="2026-01-01T00:00:00+00:00",
            workers=[
                Worker(
                    id="worker-1",
                    name="Worker One",
                    status="waiting",
                    space_id="space-1",
                    summary="human approval required",
                    meta=worker_meta,
                )
            ],
            attention=[
                AttentionSignal(
                    kind="worker_status",
                    severity="warning",
                    status="waiting",
                    reason="Approval required before continuing",
                    source="worker:worker-1",
                    updated_at="2026-01-01T00:00:00+00:00",
                    suggested_actions=[
                        SuggestedAction(
                            label="Approve",
                            tendwire_action="approve",
                            params=action_params,
                        )
                    ],
                    meta=signal_meta,
                    host_id="projector-pr5-host",
                )
            ],
            backend_health=[
                {
                    "name": "herdr",
                    "status": "healthy",
                    "outcome": "healthy_non_empty",
                    "counts": {"workers": 1},
                    "backendTarget": "sentinel-health-backend",
                }
            ],
        )

    dirty_snapshot = _snapshot(dirty_worker_meta, dirty_signal_meta, dirty_action_params)
    clean_snapshot = _snapshot(clean_worker_meta, clean_signal_meta, clean_action_params)
    dirty_turn = turns_from_snapshot(dirty_snapshot)[0]
    clean_turn = turns_from_snapshot(clean_snapshot)[0]
    dirty_pending = pending_from_snapshot(dirty_snapshot)[0]
    clean_pending = pending_from_snapshot(clean_snapshot)[0]
    turns_payload = turns_payload_from_snapshot(dirty_snapshot)
    clean_turns_payload = turns_payload_from_snapshot(clean_snapshot)
    pending_payload = pending_payload_from_snapshot(dirty_snapshot)
    clean_pending_payload = pending_payload_from_snapshot(clean_snapshot)

    assert dirty_turn.id == clean_turn.id
    assert dirty_turn.fingerprint == clean_turn.fingerprint
    assert dirty_pending.id == clean_pending.id
    assert dirty_pending.fingerprint == clean_pending.fingerprint
    assert turns_payload["content_fingerprint"] == clean_turns_payload["content_fingerprint"]
    assert pending_payload["content_fingerprint"] == clean_pending_payload["content_fingerprint"]
    assert dirty_turn.to_dict()["meta"] == clean_worker_meta
    assert dirty_pending.to_dict()["meta"]["safe_attention"] == "kept"
    assert "workerId" not in dirty_pending.to_dict()["meta"]
    assert "space-id" not in dirty_pending.to_dict()["meta"]
    assert dirty_pending.to_dict()["choices"] == [
        {"choice_id": dirty_pending.choices[0].choice_id, "label": "Approve"}
    ]
    assert turns_payload["host_id"] == "projector-pr5-host"
    assert turns_payload["turns"][0]["worker_id"] == "worker-1"
    assert turns_payload["turns"][0]["space_id"] == "space-1"
    assert turns_payload["turns"][0]["worker_fingerprint"] == clean_turn.worker_fingerprint
    assert turns_payload["turns"][0]["origin_command_id"] == "cmd-public"
    assert turns_payload["backend_health"][0]["name"] == "herdr"
    assert pending_payload["pending_interactions"][0]["worker_id"] == "worker-1"
    assert pending_payload["pending_interactions"][0]["space_id"] == "space-1"
    assert pending_payload["pending_interactions"][0]["worker_fingerprint"] == clean_pending.worker_fingerprint
    for payload in (
        dirty_turn.to_dict(),
        dirty_pending.to_dict(),
        turns_payload,
        pending_payload,
        json.loads(payload_to_json(turns_payload)),
        json.loads(payload_to_json(pending_payload)),
    ):
        _assert_no_forbidden_fields(payload)
        _assert_no_private_sentinels(payload)


def test_turn_and_pending_payload_fingerprints_ignore_wrapper_timestamps() -> None:
    config = Config(host_id="wrapper-host")
    timestamp_a = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp_b = datetime(2026, 1, 2, tzinfo=timezone.utc)
    health = {
        "name": "herdr",
        "status": "healthy",
        "outcome": "empty_healthy",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "counts": {"spaces": 0, "workers": 0},
    }
    health_later = {
        **health,
        "observed_at": "2026-01-02T00:00:00+00:00",
    }
    snapshot_a = project_from_raw(config, backend_health=[health], timestamp=timestamp_a)
    snapshot_b = project_from_raw(config, backend_health=[health_later], timestamp=timestamp_b)

    turns_payload_a = turns_payload_from_snapshot(snapshot_a)
    turns_payload_b = turns_payload_from_snapshot(snapshot_b)
    pending_payload_a = pending_payload_from_snapshot(snapshot_a)
    pending_payload_b = pending_payload_from_snapshot(snapshot_b)

    assert turns_payload_a["schema_version"] == 1
    assert turns_payload_a["turns"] == []
    assert pending_payload_a["pending_interactions"] == []
    assert turns_payload_a["content_fingerprint"] == turns_payload_b["content_fingerprint"]
    assert pending_payload_a["content_fingerprint"] == pending_payload_b["content_fingerprint"]
    assert json.loads(payload_to_json(turns_payload_a)) == turns_payload_a
    assert json.loads(payload_to_json(pending_payload_a)) == pending_payload_a
    _assert_no_forbidden_fields(turns_payload_a)
    _assert_no_forbidden_fields(pending_payload_a)


@pytest.mark.parametrize(
    ("length", "inline"),
    ((11_999, True), (12_000, True), (12_001, False)),
)
def test_canonical_turn_inline_boundaries_are_lossless(length: int, inline: bool) -> None:
    text = "x" * length
    turn = Turn(
        host_id="canonical-boundary-host",
        worker_id="worker-boundary",
        user_text=text,
        assistant_final_text=text,
        complete=True,
    )

    assert turn.user_text == text
    assert turn.assistant_final_text == text
    assert Turn.from_json(turn.to_json()).user_text == text
    projection = project_turn_content(turn.id, turn.user_text, turn.assistant_final_text)
    for field in ("user_text", "assistant_final_text"):
        descriptor = projection["content"]["fields"][field]
        assert descriptor["inline"] is inline
        assert descriptor["char_length"] == length
        assert descriptor["byte_length"] == length
        assert descriptor["availability"] == "complete"
    if inline:
        assert projection["user_text"] == text
        assert projection["assistant_final_text"] == text
        assert "user_preview" not in projection
        assert "assistant_final_preview" not in projection
    else:
        assert "user_text" not in projection
        assert "assistant_final_text" not in projection
        assert projection["user_preview"] == text[:1000]
        assert projection["assistant_final_preview"] == text[:1000]


@pytest.mark.parametrize("length", (3_999, 4_000, 4_001))
def test_assistant_stream_is_a_rolling_4000_code_point_projection(length: int) -> None:
    stream = ("x" + ("s" * 4_000)) if length == 4_001 else ("s" * length)
    turn = Turn(
        host_id="stream-boundary-host",
        worker_id=f"worker-stream-{length}",
        assistant_stream_text=stream,
    )
    assert turn.assistant_stream_text == stream[-4000:]
    assert len(turn.assistant_stream_text or "") == min(len(stream), 4000)
    assert "[truncated]" not in (turn.assistant_stream_text or "")


def test_canonical_turn_roundtrip_preserves_remaining_exact_code_points() -> None:
    raw = (
        " \r\n\r\n# He\u0301ading\r\n"
        "- first\r\n  - nested\r\n\r\n"
        "```python\r\nprint('public')\r\n```\r\n"
        "\u200bvisible\x00\r\n "
    )
    expected = (
        " \r\n\r\n# Héading\r\n"
        "- first\r\n  - nested\r\n\r\n"
        "```python\r\nprint('public')\r\n```\r\n"
        "visible\r\n "
    )
    canonical = sanitize_canonical_turn_text(raw)
    turn = Turn(
        host_id="canonical-fidelity-host",
        worker_id="worker-fidelity",
        user_text=raw,
        assistant_final_text=raw + ("z" * 20_000),
        complete=True,
    )

    assert canonical == expected
    assert turn.user_text == expected
    assert turn.assistant_final_text == expected + ("z" * 20_000)
    restored = Turn.from_json(turn.to_json())
    assert restored.user_text == turn.user_text
    assert restored.assistant_final_text == turn.assistant_final_text
    assert restored.fingerprint == turn.fingerprint


def test_large_multibyte_canonical_content_pages_reassemble_exactly() -> None:
    text = ("😀漢字e\u0301\r\n# heading\n- item\n```text\nx\n```\n" * 35_000)
    canonical = sanitize_canonical_turn_text(text)
    assert canonical is not None
    assert len(canonical.encode("utf-8")) > 1024 * 1024

    segments = segment_canonical_text(canonical)

    assert "".join(segment.text for segment in segments) == canonical
    assert sum(segment.char_length for segment in segments) == len(canonical)
    assert sum(segment.byte_length for segment in segments) == len(canonical.encode("utf-8"))
    assert all(
        segment.byte_length == len(segment.text.encode("utf-8"))
        and segment.byte_length <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        and segment.start_char == (segments[index - 1].end_char if index else 0)
        for index, segment in enumerate(segments)
    )
    assert segments[-1].end_char == len(canonical)
    assert segments == segment_canonical_text(canonical)

@pytest.mark.parametrize(
    ("byte_length", "expected_page_bytes"),
    (
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 1, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 1,)),
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES,)),
        (TURN_CONTENT_PAGE_MAX_UTF8_BYTES + 1, (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 3, 4)),
    ),
)
def test_multibyte_content_pages_honor_exact_utf8_byte_boundaries(
    byte_length: int,
    expected_page_bytes: tuple[int, ...],
) -> None:
    text = ("a" * (byte_length - 4)) + "😀"

    segments = segment_canonical_text(text)

    assert len(text.encode("utf-8")) == byte_length
    assert tuple(segment.byte_length for segment in segments) == expected_page_bytes
    assert "".join(segment.text for segment in segments) == text
    assert segments[-1].text.endswith("😀")



def test_content_identities_and_cursors_are_deterministic_and_revision_bound() -> None:
    turn_id = "turn-" + ("a" * 24)
    user_text = " prompt "
    final_text = "final😀" * 10_000
    revision = content_revision(
        turn_id,
        user_text,
        final_text,
        "complete",
        "complete",
    )
    same_revision = content_revision(
        turn_id,
        user_text,
        final_text,
        "complete",
        "complete",
    )
    changed_revision = content_revision(
        turn_id,
        user_text,
        final_text + "!",
        "complete",
        "complete",
    )
    cursor = content_cursor(
        revision,
        "assistant_final_text",
        1,
        start_char=6_144,
        start_byte=9_216,
    )

    assert revision == same_revision
    assert revision.startswith("twrev1.")
    assert changed_revision != revision
    assert content_segment_id(revision, "assistant_final_text", 1).startswith("twseg1.")
    assert content_segment_id(revision, "assistant_final_text", 1) == content_segment_id(
        revision,
        "assistant_final_text",
        1,
    )
    assert cursor.startswith("twcur1.")
    position = decode_content_cursor(
        cursor,
        revision=revision,
        field="assistant_final_text",
        count=3,
    )
    assert position.index == 1
    assert position.segment_id == content_segment_id(
        revision, "assistant_final_text", 1
    )
    assert position.start_char == 6_144
    assert position.start_byte == 9_216
    with pytest.raises(ValueError, match="invalid_cursor"):
        content_cursor(revision, "assistant_final_text", 1)
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=changed_revision,
            field="assistant_final_text",
            count=3,
        )
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor[:-1] + ("A" if cursor[-1] != "A" else "B"),
            revision=revision,
            field="assistant_final_text",
            count=3,
        )
    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="user_text",
            count=3,
        )

@pytest.mark.parametrize(
    "cursor",
    (
        "",
        "not-a-cursor",
        "twcur1.",
        "twcur1.!!!!",
        "twcur1.e30",
    ),
)
def test_content_cursor_rejects_malformed_encodings(cursor: str) -> None:
    revision = "twrev1." + ("d" * 43)

    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="assistant_final_text",
            count=2,
        )


def test_content_cursor_rejects_valid_integrity_at_out_of_range_index() -> None:
    revision = "twrev1." + ("e" * 43)
    cursor = content_cursor(
        revision,
        "assistant_final_text",
        2,
        start_char=20_000,
        start_byte=40_000,
    )

    with pytest.raises(ValueError, match="invalid_cursor"):
        decode_content_cursor(
            cursor,
            revision=revision,
            field="assistant_final_text",
            count=2,
        )


def test_v2_descriptor_and_page_payload_have_exact_lengths_and_cursor_progression() -> None:
    turn_id = "turn-" + ("b" * 24)
    user_text = "short"
    final_text = ("😀" * 20_000) + "\r\n "
    descriptor = build_turn_content_descriptor(turn_id, user_text, final_text)
    revision = descriptor.content_revision
    final_descriptor = descriptor.fields["assistant_final_text"]

    assert descriptor.schema_version == 1
    assert descriptor.known_incomplete is False
    assert descriptor.fields["user_text"].inline is True
    assert final_descriptor.inline is False
    assert final_descriptor.char_length == len(final_text)
    assert final_descriptor.byte_length == len(final_text.encode("utf-8"))
    assert final_descriptor.first_cursor == content_cursor(
        revision,
        "assistant_final_text",
        0,
    )
    first = build_turn_content_page(
        turn_id,
        revision,
        "assistant_final_text",
        final_text,
    )
    second = build_turn_content_page(
        turn_id,
        revision,
        "assistant_final_text",
        final_text,
        cursor=first["next_cursor"],
    )
    assert first["index"] == 0
    assert second["index"] == 1
    assert first["text"] + second["text"] == final_text
    assert first["segment_byte_length"] <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
    assert second["segment_byte_length"] <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
    assert first["total_char_length"] == second["total_char_length"] == len(final_text)
    assert first["total_byte_length"] == second["total_byte_length"] == len(
        final_text.encode("utf-8")
    )


def test_known_incomplete_projection_is_explicit_and_never_pageable_or_inline() -> None:
    turn_id = "turn-" + ("c" * 24)
    fragment = "legacy fragment\n[truncated]"

    projection = project_turn_content(
        turn_id,
        None,
        fragment,
        final_state="known_incomplete",
    )
    descriptor = projection["content"]
    final_descriptor = descriptor["fields"]["assistant_final_text"]
    complete_revision = content_revision(
        turn_id,
        None,
        fragment,
        "absent",
        "complete",
    )

    assert descriptor["known_incomplete"] is True
    assert final_descriptor == {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len(fragment),
        "byte_length": len(fragment.encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    assert "assistant_final_text" not in projection
    assert projection["assistant_final_preview"] == fragment
    assert descriptor["content_revision"] != complete_revision


def test_turn_list_v2_adds_content_descriptors_without_changing_v1_default() -> None:
    snapshot = Snapshot(
        host_id="turn-list-v2-host",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[Worker(id="worker-list-v2", name="List v2", status="active")],
    )

    legacy = turns_payload_from_snapshot(snapshot)
    version_two = turns_payload_from_snapshot(snapshot, schema_version=2)

    assert legacy["schema_version"] == 1
    assert "content" not in legacy["turns"][0]
    assert version_two["schema_version"] == 2
    assert version_two["turns"][0]["content"]["schema_version"] == 1
    assert version_two["turns"][0]["content"]["known_incomplete"] is False
    assert version_two["turns"][0]["content"]["fields"] == {
        "user_text": {
            "availability": "absent",
            "inline": False,
            "char_length": 0,
            "byte_length": 0,
            "page_count": 0,
            "first_cursor": None,
        },
        "assistant_final_text": {
            "availability": "absent",
            "inline": False,
            "char_length": 0,
            "byte_length": 0,
            "page_count": 0,
            "first_cursor": None,
        },
    }
    with pytest.raises(ValueError, match="unsupported_turn_schema_version"):
        turns_payload_from_snapshot(snapshot, schema_version=3)
