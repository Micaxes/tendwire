"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends import herdr_cli
from tendwire.cli import _build_parser, main, observe_public_snapshot
from tendwire.config import Config
from tendwire.core.models import AttentionSignal, Snapshot, Space, SuggestedAction, Worker
from tendwire.core.projector import project_from_raw
from tendwire.daemon_api import TendwireDaemonAPI, UnixSocketJSONServer
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    append_event,
    get_turn_content,
    init_store,
    latest_snapshot,
    list_worker_bindings,
    merge_turn_content,
    save_snapshot,
    turns_payload_from_store,
)


@pytest.fixture(autouse=True)
def _isolate_cli_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    private_home = tmp_path / "home"
    private_home.mkdir(mode=0o700)
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "tendwire-data"))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "tmux",
    "tmux_session",
    "tmux_sessions",
    "screen_session",
    "screen_sessions",
    "window_id",
    "window_ids",
    "tab_id",
    "tab_ids",
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "backend_target",
    "backend_targets",
    "session_id",
    "private",
    "private_binding",
    "private_bindings",
    "private_fingerprint",
    "private_fingerprints",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "connector",
    "connectors",
    "command",
    "command_args",
    "command_argv",
    "command_line",
    "command_payload",
    "command_text",
    "raw_args",
    "raw_argv",
    "raw_command",
    "raw_command_line",
    "shell_command",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "token",
    "tokens",
    "secret",
    "secrets",
    "password",
    "passwords",
    "credentials",
    "cookie",
    "auth_token",
    "auth_tokens",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {
    key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS
}


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


def test_cli_snapshot_json_prints_contract_json_only(capsys) -> None:
    code = main(
        [
            "--host-id",
            "cli-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-host"
    assert len(payload["content_fingerprint"]) == 24
    assert {"updated_at", "spaces", "workers", "attention", "backend_health"} <= set(payload)
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


def test_cli_socket_group_option_is_daemon_only_and_normalized(monkeypatch) -> None:
    captured: list[Config] = []

    def capture_daemon_config(config: Config) -> int:
        captured.append(config)
        return 0

    monkeypatch.delenv("TENDWIRE_SOCKET_GROUP", raising=False)
    monkeypatch.setattr("tendwire.cli.cmd_daemon", capture_daemon_config)

    snapshot_args = _build_parser().parse_args(["snapshot"])
    assert not hasattr(snapshot_args, "socket_group")
    assert main(["daemon", "--socket-group", "  daemon-clients  "]) == 0
    assert captured[0].socket_group == "daemon-clients"


def test_cli_turns_json_no_herdr_prints_public_empty_collection(capsys) -> None:
    code = main(
        [
            "--host-id",
            "turns-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "turns-host"
    assert payload["turns"] == []
    assert len(payload["content_fingerprint"]) == 24
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_turns_schema_v2_daemon_request_requires_no_content_fetch(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, dict(params or {})))
            return {
                "ok": True,
                "result": {
                    "schema_version": 2,
                    "host_id": "turns-host",
                    "turns": [
                        {
                            "id": "turn-public",
                            "assistant_final_text": "short final",
                            "content": {
                                "schema_version": 1,
                                "content_revision": "twrev1.public",
                                "known_incomplete": False,
                                "fields": {
                                    "assistant_final_text": {
                                        "availability": "complete",
                                        "inline": True,
                                        "char_length": 11,
                                        "byte_length": 11,
                                        "page_count": 1,
                                        "first_cursor": None,
                                    }
                                },
                            },
                        }
                    ],
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--schema-version",
            "2",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == 2
    assert payload["turns"][0]["assistant_final_text"] == "short final"
    assert payload["turns"][0]["content"]["fields"]["assistant_final_text"]["inline"] is True
    assert calls == [("turn.list", {"schema_version": 2})]


def test_cli_turns_v1_upgrade_required_is_json_and_nonzero(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "turn.list"
            assert params == {"schema_version": 1}
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": False,
                    "status": "upgrade_required",
                    "required_turn_schema_version": 2,
                    "error": {
                        "code": "upgrade_required",
                        "message": "turn content requires schema version 2",
                    },
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "upgrade_required"
    assert payload["required_turn_schema_version"] == 2
    assert payload["error"]["code"] == "upgrade_required"


def test_cli_turn_content_get_preserves_exact_page_and_params(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    page_text = "\n  " + ("界" * 20_000) + "\r\n  "
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, dict(params or {})))
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "turn_id": "turn-public",
                    "content_revision": "twrev1.public",
                    "field": "assistant_final_text",
                    "availability": "complete",
                    "segment_id": "twseg1.public",
                    "index": 1,
                    "count": 2,
                    "text": page_text,
                    "segment_char_length": len(page_text),
                    "segment_byte_length": len(page_text.encode("utf-8")),
                    "total_char_length": 40_000,
                    "total_byte_length": 120_000,
                    "next_cursor": None,
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            "turn-public",
            "--revision",
            "twrev1.public",
            "--field",
            "assistant_final_text",
            "--cursor",
            "twcur1.public",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["text"] == page_text
    assert calls == [
        (
            "turn.content.get",
            {
                "schema_version": 1,
                "turn_id": "turn-public",
                "content_revision": "twrev1.public",
                "field": "assistant_final_text",
                "cursor": "twcur1.public",
            },
        )
    ]


@pytest.mark.parametrize("with_db_path", [False, True])
@pytest.mark.parametrize(
    ("error_code", "details"),
    [
        ("internal_error", {"type": "RuntimeError"}),
        ("response_too_large", {"max_response_bytes": 1024 * 1024}),
    ],
)
def test_cli_turn_content_preserves_reachable_daemon_errors_without_store_fallback(
    tmp_path: Path,
    capsys,
    monkeypatch,
    with_db_path: bool,
    error_code: str,
    details: dict[str, Any],
) -> None:
    direct_calls: list[str] = []
    original_error = {
        "code": error_code,
        "message": f"daemon {error_code}",
        "details": details,
    }

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "turn.content.get"
            return {
                "schema_version": 1,
                "ok": False,
                "status": "error",
                "result": None,
                "error": original_error,
            }

    def forbidden_store_call(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        direct_calls.append("store")
        raise AssertionError("reachable daemon errors must not fall back to the store")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("tendwire.store.sqlite.init_store", forbidden_store_call)
    monkeypatch.setattr("tendwire.store.sqlite.get_turn_content", forbidden_store_call)
    argv = [
        "--host-id",
        "turns-host",
        "--socket-path",
        str(tmp_path / "daemon.sock"),
        "turn",
        "content",
        "get",
        "--json",
        "--turn-id",
        "turn-public",
        "--revision",
        "twrev1.public",
        "--field",
        "assistant_final_text",
    ]
    if with_db_path:
        argv += ["--db-path", str(tmp_path / "direct.db")]

    code = main(argv)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "error"
    assert payload["error"] == original_error
    assert direct_calls == []


def test_cli_long_content_pages_match_direct_store_and_daemon(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "long-content.db"
    socket_path = tmp_path / "long-content.sock"
    config = Config(host_id="long-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    canonical = (
        "# Exact heading\n\n"
        + ("safe-value αβγ\n- nested-looking item\n```text\ncode\n```\n" * 30_000)
    )[:1_100_000] + "終"
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "user_text": "short prompt",
            "assistant_final_text": canonical,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    listed = turns_payload_from_store(
        db_path,
        "long-host",
        snapshot=snapshot,
        schema_version=2,
    )
    turn = listed["turns"][0]
    revision = turn["content"]["content_revision"]
    descriptor = turn["content"]["fields"]["assistant_final_text"]
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", lambda _config: 0)

    v1_code = main(
        [
            "--host-id",
            "long-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--db-path",
            str(db_path),
            "--json",
        ]
    )
    v1_payload = json.loads(capsys.readouterr().out)
    v2_code = main(
        [
            "--host-id",
            "long-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--db-path",
            str(db_path),
            "--schema-version",
            "2",
            "--json",
        ]
    )
    v2_payload = json.loads(capsys.readouterr().out)

    assert v1_code == 1
    assert v1_payload["status"] == "upgrade_required"
    assert v1_payload["required_turn_schema_version"] == 2
    assert v2_code == 0
    assert v2_payload["schema_version"] == 2
    assert descriptor["inline"] is False
    assert descriptor["char_length"] == len(canonical)
    assert descriptor["byte_length"] == len(canonical.encode("utf-8"))
    assert descriptor["page_count"] > 1

    def fetch_pages(*, socket: Path | None, direct_db: Path | None) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            argv = ["--host-id", "long-host"]
            if socket is not None:
                argv += ["--socket-path", str(socket)]
            argv += [
                "turn",
                "content",
                "get",
                "--json",
                "--turn-id",
                turn["id"],
                "--revision",
                revision,
                "--field",
                "assistant_final_text",
            ]
            if direct_db is not None:
                argv += ["--db-path", str(direct_db)]
            if cursor is not None:
                argv += ["--cursor", cursor]
            assert main(argv) == 0
            captured = capsys.readouterr()
            assert captured.err == ""
            page = json.loads(captured.out)
            assert len(json.dumps(page, ensure_ascii=False).encode("utf-8")) < 1024 * 1024
            pages.append(page)
            next_cursor = page["next_cursor"]
            if next_cursor is None:
                return pages
            assert next_cursor not in {item.get("next_cursor") for item in pages[:-1]}
            cursor = next_cursor

    direct_pages = fetch_pages(socket=None, direct_db=db_path)
    bad_cursor_code = main(
        [
            "--host-id",
            "long-host",
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            turn["id"],
            "--revision",
            revision,
            "--field",
            "assistant_final_text",
            "--cursor",
            "twcur1.tampered",
            "--db-path",
            str(db_path),
        ]
    )
    bad_cursor_payload = json.loads(capsys.readouterr().out)
    assert bad_cursor_code == 1
    assert bad_cursor_payload["status"] == "invalid_cursor"

    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        get_turn_content=lambda params: get_turn_content(
            db_path,
            "long-host",
            turn_id=params["turn_id"],
            content_revision=params["content_revision"],
            field=params["field"],
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
        ),
    )
    server = UnixSocketJSONServer(
        socket_path,
        api.dispatch,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        daemon_pages = fetch_pages(socket=socket_path, direct_db=None)
        daemon_bad_code = main(
            [
                "--host-id",
                "long-host",
                "--socket-path",
                str(socket_path),
                "turn",
                "content",
                "get",
                "--json",
                "--turn-id",
                turn["id"],
                "--revision",
                revision,
                "--field",
                "assistant_final_text",
                "--cursor",
                "twcur1.tampered",
            ]
        )
        daemon_bad_payload = json.loads(capsys.readouterr().out)
        assert daemon_bad_code == 1
        assert daemon_bad_payload == bad_cursor_payload
    finally:
        server.close()
        thread.join(timeout=2)

    assert daemon_pages == direct_pages
    assert "".join(page["text"] for page in direct_pages) == canonical
    assert [page["index"] for page in direct_pages] == list(range(len(direct_pages)))
    assert all(page["count"] == len(direct_pages) for page in direct_pages)
    assert not thread.is_alive()


def test_cli_short_v1_compatibility_then_known_incomplete_refusal(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "content-compatibility.db"
    config = Config(host_id="compat-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "compat-host",
        "worker-1",
        {
            "user_text": "  short prompt\n",
            "assistant_final_text": "\n short final  ",
            "complete": True,
        },
    ) == 1
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", lambda _config: 0)
    common = [
        "--host-id",
        "compat-host",
        "--herdr-bin",
        "definitely-not-a-real-herdr-binary",
        "turns",
        "--db-path",
        str(db_path),
        "--json",
    ]

    short_v1_code = main(common)
    short_v1 = json.loads(capsys.readouterr().out)
    short_v2_code = main([*common, "--schema-version", "2"])
    short_v2 = json.loads(capsys.readouterr().out)
    short_turn = short_v2["turns"][0]

    assert short_v1_code == 0
    assert short_v1["schema_version"] == 1
    assert short_v1["turns"][0]["assistant_final_text"] == "\n short final  "
    assert short_v1["turns"][0]["user_text"] == "  short prompt\n"
    assert "content" not in short_v1["turns"][0]
    assert short_v2_code == 0
    assert short_turn["assistant_final_text"] == "\n short final  "
    assert short_turn["content"]["fields"]["assistant_final_text"]["inline"] is True

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET final_state = 'known_incomplete'
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            ("compat-host", short_turn["id"]),
        )

    incomplete_v1_code = main(common)
    incomplete_v1 = json.loads(capsys.readouterr().out)
    incomplete_v2_code = main([*common, "--schema-version", "2"])
    incomplete_v2 = json.loads(capsys.readouterr().out)
    revision = incomplete_v2["turns"][0]["content"]["content_revision"]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET content_revision = ?
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (revision, "compat-host", short_turn["id"]),
        )
    content_code = main(
        [
            "--host-id",
            "compat-host",
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            short_turn["id"],
            "--revision",
            revision,
            "--field",
            "assistant_final_text",
            "--db-path",
            str(db_path),
        ]
    )
    content_error = json.loads(capsys.readouterr().out)

    assert incomplete_v1_code == 1
    assert incomplete_v1["status"] == "upgrade_required"
    assert incomplete_v1["required_turn_schema_version"] == 2
    assert incomplete_v2_code == 0
    incomplete_field = incomplete_v2["turns"][0]["content"]["fields"]["assistant_final_text"]
    assert incomplete_field["availability"] == "known_incomplete"
    assert incomplete_field["inline"] is False
    assert "assistant_final_text" not in incomplete_v2["turns"][0]
    assert content_code == 1
    assert content_error["status"] == "content_known_incomplete"


def test_cli_pending_json_no_herdr_prints_public_empty_collection(capsys) -> None:
    code = main(
        [
            "--host-id",
            "pending-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "pending",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["schema_version"] == 1
    assert payload["host_id"] == "pending-host"
    assert payload["pending_interactions"] == []
    assert len(payload["content_fingerprint"]) == 24
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_store_hooks_print_json_only_and_support_dry_run(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "store-cli.db"
    init_store(db_path)
    append_event(
        db_path,
        "store-cli",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "store-cli",
        "public.event",
        {"safe": "kept"},
        observed_at="9999-01-09T00:00:00+00:00",
    )

    status_code = main(["--host-id", "store-cli", "store", "status", "--db-path", str(db_path)])
    status_captured = capsys.readouterr()
    status_payload = json.loads(status_captured.out)

    tail_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "events-tail",
            "--db-path",
            str(db_path),
            "--limit",
            "5",
        ]
    )
    tail_captured = capsys.readouterr()
    tail_payload = json.loads(tail_captured.out)

    cleanup_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "cleanup",
            "--db-path",
            str(db_path),
            "--retention-days",
            "7",
            "--dry-run",
        ]
    )
    cleanup_captured = capsys.readouterr()
    cleanup_payload = json.loads(cleanup_captured.out)

    missing_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "status",
            "--db-path",
            str(tmp_path / "missing.db"),
        ]
    )
    missing_captured = capsys.readouterr()
    missing_payload = json.loads(missing_captured.out)

    with sqlite3.connect(str(db_path)) as conn:
        event_count = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("store-cli",)).fetchone()[0]

    assert status_code == 0
    assert tail_code == 0
    assert cleanup_code == 0
    assert missing_code == 1
    assert status_captured.err == tail_captured.err == cleanup_captured.err == missing_captured.err == ""
    assert status_payload["counts"]["events"] == 2
    assert tail_payload["events"]
    assert "sentinel-private" not in json.dumps(tail_payload)
    assert "payload_json" not in json.dumps(tail_payload)
    assert cleanup_payload["dry_run"] is True
    assert cleanup_payload["retention"]["deleted"] == 1
    assert event_count == 2
    assert missing_payload["status"] == "store_unavailable"


def test_cli_turns_and_pending_project_from_snapshot_observation(capsys, monkeypatch) -> None:
    def _fake_herdr_state(config):
        return [
            Space(id="space-1", name="Space One", status="active"),
        ], [
            Worker(
                id="worker-1",
                name="Worker One",
                status="pending",
                space_id="space-1",
                summary="human approval required before continuing",
                meta={
                    "needs_human": True,
                    "safe": "kept",
                    "pane_id": "pane-private",
                    "tty": "sentinel-cli-tty",
                    "pty": "sentinel-cli-pty",
                    "pid": "sentinel-cli-pid",
                    "processId": "sentinel-cli-process",
                    "tmux-session": "sentinel-cli-tmux",
                    "screenSession": "sentinel-cli-screen",
                    "window_id": "sentinel-cli-window",
                    "tabId": "sentinel-cli-tab",
                    "terminalid": "sentinel-cli-terminal",
                    "backendTarget": "sentinel-cli-backend",
                    "session-id": "sentinel-cli-session",
                    "messageIds": "sentinel-cli-message-ids",
                    "terminalIds": "sentinel-cli-terminal-ids",
                    "terminal": "sentinel-cli-terminal-object",
                    "telegramMessageId": "sentinel-cli-telegram-message",
                    "routeId": "sentinel-cli-route-id",
                    "connectorId": "sentinel-cli-connector-id",
                    "tmuxPaneId": "sentinel-cli-tmux-pane-id",
                    "screenWindowId": "sentinel-cli-screen-window-id",
                    "agentSessionId": "sentinel-cli-agent-session-id",
                    "session": "sentinel-cli-session-object",
                    "privateFingerprints": "sentinel-cli-private-fingerprints",
                    "passwords": "sentinel-cli-passwords",
                    "privateBinding": "sentinel-cli-private-binding",
                    "authToken": "sentinel-cli-auth",
                },
                backend_target={"kind": "agent_id", "value": "agent-private", "sendable": True},
            )
        ]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    turns_code = main(["--host-id", "projection-cli", "--herdr-bin", "herdr", "turns", "--json"])
    turns_captured = capsys.readouterr()
    turns_payload = json.loads(turns_captured.out)
    pending_code = main(["--host-id", "projection-cli", "--herdr-bin", "herdr", "pending", "--json"])
    pending_captured = capsys.readouterr()
    pending_payload = json.loads(pending_captured.out)

    encoded_turns = json.dumps(turns_payload)
    encoded_pending = json.dumps(pending_payload)
    assert turns_code == 0
    assert pending_code == 0
    assert turns_captured.err == ""
    assert pending_captured.err == ""
    assert turns_payload["turns"][0]["worker_id"] == "worker-1"
    assert turns_payload["turns"][0]["status"] == "waiting"
    assert turns_payload["turns"][0]["kind"] == "task"
    assert pending_payload["pending_interactions"][0]["worker_id"] == "worker-1"
    assert pending_payload["pending_interactions"][0]["kind"] == "approval"
    assert pending_payload["pending_interactions"][0]["status"] == "open"
    assert "agent-private" not in encoded_turns
    assert "pane-private" not in encoded_turns
    assert "agent-private" not in encoded_pending
    assert "pane-private" not in encoded_pending
    assert "sentinel-cli-" not in encoded_turns
    assert "sentinel-cli-" not in encoded_pending
    _assert_no_public_json_forbidden(turns_payload)
    _assert_no_public_json_forbidden(pending_payload)


def test_cli_turns_and_pending_json_strip_raw_command_action_material(capsys, monkeypatch) -> None:
    def _fake_snapshot(config):
        return Snapshot(
            host_id=config.host_id,
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
                            command="sentinel-cli-safe-looking-command-alias",
                            params={
                                "safe_choice": "kept",
                                "commandLine": "sentinel-cli-command-line",
                                "terminal_id": "sentinel-cli-terminal",
                                "backendTarget": "sentinel-cli-backend",
                                "session-id": "sentinel-cli-session",
                                "token": "sentinel-cli-token",
                                "secret": "sentinel-cli-secret",
                            },
                        )
                    ],
                    meta={"worker_id": "worker-1", "space_id": "space-1", "needs_human": True},
                    host_id=config.host_id,
                )
            ],
        )

    monkeypatch.setattr("tendwire.cli._current_public_snapshot", _fake_snapshot)

    turns_code = main(["--host-id", "raw-command-cli", "turns", "--json"])
    turns_captured = capsys.readouterr()
    pending_code = main(["--host-id", "raw-command-cli", "pending", "--json"])
    pending_captured = capsys.readouterr()
    turns_payload = json.loads(turns_captured.out)
    pending_payload = json.loads(pending_captured.out)
    encoded_turns = json.dumps(turns_payload, sort_keys=True)
    encoded_pending = json.dumps(pending_payload, sort_keys=True)

    assert turns_code == 0
    assert pending_code == 0
    assert turns_captured.err == ""
    assert pending_captured.err == ""
    assert turns_payload["turns"][0]["worker_id"] == "worker-1"
    assert pending_payload["pending_interactions"][0]["choices"] == [
        {
            "choice_id": pending_payload["pending_interactions"][0]["choices"][0]["choice_id"],
            "label": "Action",
        }
    ]
    assert "sentinel-cli-" not in encoded_turns
    assert "sentinel-cli-" not in encoded_pending
    _assert_no_public_json_forbidden(turns_payload)
    _assert_no_public_json_forbidden(pending_payload)


def test_cli_snapshot_json_reports_healthy_empty_herdr(capsys, monkeypatch) -> None:
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-empty", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["spaces"] == []
    assert payload["workers"] == []
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "empty_healthy"
    assert payload["backend_health"][0]["counts"] == {"spaces": 0, "workers": 0}


def test_cli_snapshot_store_persists_printed_snapshot(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    code = main(
        [
            "--host-id",
            "cli-store",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    payload = json.loads(captured.out)
    assert captured.err == ""
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "cli-store"
    assert restored.content_fingerprint == payload["content_fingerprint"]


@pytest.mark.parametrize(
    ("outcome", "has_workers", "expected_authority"),
    [
        ("healthy_non_empty", True, "complete"),
        ("empty_healthy", False, "complete"),
        ("missing_binary", False, "none"),
        ("timeout", False, "none"),
        ("malformed_json", False, "none"),
        ("continuity_unavailable", True, "none"),
        ("unknown", False, "none"),
    ],
)
def test_cli_snapshot_persistence_passes_explicit_observation_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    has_workers: bool,
    expected_authority: str,
) -> None:
    db_path = tmp_path / f"{outcome}.db"
    init_store(db_path)
    config = Config(host_id=f"cli-{outcome}", db_path=db_path)
    observed_at = "2026-01-01T00:00:00+00:00"
    workers = [Worker(id="worker-1", name="Worker One", status="active")] if has_workers else []
    health = herdr_cli.herdr_backend_health(
        outcome,
        observed_at=observed_at,
        workers=workers,
    )
    observation = SimpleNamespace(
        spaces=[],
        workers=workers,
        bindings=[],
        backend_health=[health],
    )
    captured: list[SnapshotObservationContext] = []

    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_snapshot_observation",
        lambda _config, *, stored_bindings: observation,
    )

    def _capture_save(
        _db_path: Path,
        _snapshot: Snapshot,
        *,
        observation: SnapshotObservationContext,
    ) -> None:
        captured.append(observation)

    monkeypatch.setattr("tendwire.store.sqlite.save_snapshot", _capture_save)

    observe_public_snapshot(config, store_snapshot=True)

    assert len(captured) == 1
    assert captured[0].authority == expected_authority
    assert captured[0].observed_at == observed_at


def test_cli_legacy_observation_cannot_claim_complete_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "legacy-observation.db"
    init_store(db_path)
    config = Config(host_id="cli-legacy", db_path=db_path)
    worker = Worker(id="worker-1", name="Worker One", status="blocked")
    captured: list[SnapshotObservationContext] = []

    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_state",
        lambda _config, **_kwargs: ([], [worker]),
    )

    def _capture_save(
        _db_path: Path,
        _snapshot: Snapshot,
        *,
        observation: SnapshotObservationContext,
    ) -> None:
        captured.append(observation)

    monkeypatch.setattr("tendwire.store.sqlite.save_snapshot", _capture_save)

    observe_public_snapshot(config, store_snapshot=True)

    assert len(captured) == 1
    assert captured[0].authority == "none"


def test_cli_attention_json_reads_store_backed_lifecycle(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "attention.db"
    socket_path = tmp_path / "absent.sock"
    config = Config(host_id="cli-attention", db_path=db_path)
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
                    "terminalId": "sentinel-private-terminal",
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
        timestamp=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at="2026-01-01T00:00:00+00:00",
        ),
    )

    code = main(
        [
            "--host-id",
            "cli-attention",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["host_id"] == "cli-attention"
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert payload["attention"][0]["signal_count"] == 1
    assert len(payload["attention"]) == 1
    assert not {
        "family_key",
        "generation",
        "first_missing_at",
        "missing_observation_count",
        "last_accepted_at",
        "last_observation_key",
        "max_notified_severity_rank",
    }.intersection(payload["attention"][0])
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_attention_json_falls_back_to_snapshot_when_store_is_unavailable(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "missing.db"
    socket_path = tmp_path / "absent.sock"

    def _fake_herdr_state(config):
        return [], [
            Worker(
                id="worker-1",
                name="Worker One",
                status="blocked",
                meta={"pane_id": "sentinel-private-pane"},
            )
        ]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    code = main(
        [
            "--host-id",
            "cli-attention-fallback",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["status"] == "blocked"
    assert "first_seen_at" not in payload["attention"][0]
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_public_json_does_not_emit_connector_private_store_rows(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "connector-private.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                "queued",
                '{"safe":"kept"}',
                '{"chat_id":"sentinel-chat","route":"sentinel-route"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                1,
                "delivered",
                '{"ok":true}',
                '{"message_id":"sentinel-message","token":"sentinel-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )

    payloads: list[dict[str, Any]] = []

    snapshot_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
            "--store",
            "--db-path",
            str(db_path),
        ]
    )
    snapshot_captured = capsys.readouterr()
    payloads.append(json.loads(snapshot_captured.out))

    turns_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    turns_captured = capsys.readouterr()
    payloads.append(json.loads(turns_captured.out))

    pending_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "pending",
            "--json",
        ]
    )
    pending_captured = capsys.readouterr()
    payloads.append(json.loads(pending_captured.out))

    doctor_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "doctor",
            "--json",
        ]
    )
    doctor_captured = capsys.readouterr()
    payloads.append(json.loads(doctor_captured.out))

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    command_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    command_captured = capsys.readouterr()
    payloads.append(json.loads(command_captured.out))

    with sqlite3.connect(str(db_path)) as conn:
        private_counts = (
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_deliveries").fetchone()[0],
        )

    encoded = json.dumps(payloads, sort_keys=True)
    assert snapshot_code == 0
    assert turns_code == 0
    assert pending_code == 0
    assert doctor_code == 1
    assert command_code == 0
    assert private_counts == (1, 1)
    assert "sentinel-" not in encoded


def test_cli_snapshot_store_persists_private_bindings_outside_snapshot_payload(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bindings.db"
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "wA", "label": "Bindings"}
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "agent-secret",
                        "agent": "Worker",
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                        "terminal_id": "terminal-secret",
                        "agent": "Worker",
                    }
                ]
            }
        },
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(
        [
            "--host-id",
            "cli-bindings",
            "--herdr-bin",
            "herdr",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    bindings = list_worker_bindings(db_path, "cli-bindings", backend="herdr")

    assert code == 0
    assert len(bindings) == 1
    assert bindings[0].worker_id == "public-worker"
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "agent-secret"
    encoded = json.dumps(payload)
    assert "agent-secret" not in encoded
    assert "wA:p1" not in encoded
    assert "target_kind" not in encoded


def test_cli_module_invocation() -> None:
    """python -m tendwire.cli snapshot --json works."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tendwire.cli",
            "--host-id",
            "module-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "module-host"
    assert len(payload["content_fingerprint"]) == 24


def test_cli_snapshot_with_live_shaped_herdr_fixtures(capsys, monkeypatch) -> None:
    """CLI emits schema v2 JSON with non-empty spaces and workers from Herdr fixtures."""

    def _fake_run_herdr(args, cfg):
        if tuple(args) == ("workspace", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "workspaces": [
                            {
                                "workspace_id": "wA",
                                "label": "CLI Space",
                                "agent_status": "working",
                                "focused": True,
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("agent", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "agents": [
                            {
                                "agent_session": {"value": "sess-cli"},
                                "agent": "CLI Agent",
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "agent_status": "executing",
                                "cwd": "/tmp",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("pane", "list"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "panes": [
                            {
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "terminal_id": "terminal-cli",
                                "agent": "CLI Agent",
                                "agent_session": {"value": "sess-cli"},
                                "agent_status": "executing",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-live", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-live"
    assert len(payload["spaces"]) == 1
    assert payload["spaces"][0]["id"] == "wA"
    assert payload["spaces"][0]["status"] == "active"
    assert len(payload["workers"]) == 1
    assert payload["workers"][0]["id"] == "CLI Agent"
    assert payload["workers"][0]["status"] == "active"
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "healthy_non_empty"
    assert payload["backend_health"][0]["counts"] == {"spaces": 1, "workers": 1}
    assert "agent_session" not in json.dumps(payload)
    assert "sess-cli" not in json.dumps(payload)
