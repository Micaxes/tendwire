"""Backend-provided pending prompts: a REAL pane prompt (question + choices, captured by the herdres
pending hook through the turn adapter) flows adapter -> herdr_turns -> backend_pending -> pending.list,
superseding the worker's synthetic attention-derived row."""
from __future__ import annotations

import json
from pathlib import Path

from tendwire.backends.herdr_turns import _backend_pending_from_turn, _pop_backend_pending
from tendwire.config import Config
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import pending_payload_from_snapshot
from tendwire.daemon import TendwireDaemon
from tendwire.store.sqlite import (
    init_store,
    list_backend_pending,
    merge_backend_pending,
    prune_backend_pending,
    save_snapshot,
)


def _decision_turn() -> dict:
    return {
        "available": True,
        "complete": False,
        "awaiting_input": True,
        "user_text": "which db?",
        "pending_decision": {
            "decision_id": "toolu_123",
            "prompt": "Which database should we use?",
            "mode": "buttons",
            "options": [
                {"id": "1", "label": "Postgres", "send_text": "Postgres"},
                {"id": "2", "label": "SQLite", "send_text": "SQLite"},
                {"id": "custom", "label": "Tell me differently", "send_text": ""},
            ],
        },
    }


def test_extract_pending_decision():
    pending = _backend_pending_from_turn(_decision_turn())
    assert pending["question"] == "Which database should we use?"
    assert pending["kind"] == "question"
    assert [c["label"] for c in pending["choices"]] == ["Postgres", "SQLite", "Tell me differently"]
    # The machine-send payload (send_text) is not published; choices carry only id + label.
    assert all("value" not in c for c in pending["choices"])
    # decision_id (internal tool_use_id) is not published in public pending.
    assert "decision_id" not in pending["meta"]
    assert pending["meta"] == {"source": "backend"}


def test_extract_plan_approval_kind():
    turn = {"pending_decision": {"decision_id": "t", "prompt": "Approve this plan?",
                                 "options": [{"id": "approve", "label": "Approve", "send_text": "1"},
                                             {"id": "revise", "label": "Revise", "send_text": ""}]}}
    assert _backend_pending_from_turn(turn)["kind"] == "approval"


def test_extract_none_without_pending():
    assert _backend_pending_from_turn({"complete": True, "assistant_final_text": "done"}) is None


def test_pop_backend_pending_splits():
    content, pending = _pop_backend_pending({"user_text": "hi", "_backend_pending": {"question": "q"}})
    assert content == {"user_text": "hi"} and pending == {"question": "q"}
    content, pending = _pop_backend_pending({"_backend_pending": {"question": "q"}})
    assert content is None and pending == {"question": "q"}


def test_merge_and_prune_backend_pending(tmp_path: Path):
    db = tmp_path / "p.db"
    init_store(db)
    pending = {"question": "Q?", "kind": "question", "choices": [], "meta": {"source": "backend"}}
    assert merge_backend_pending(db, "h", "w1", pending) is True
    assert merge_backend_pending(db, "h", "w1", pending) is False       # unchanged -> no write
    assert list_backend_pending(db, "h") == {"w1": pending}
    assert merge_backend_pending(db, "h", "w1", None) is True           # answered -> pruned
    assert list_backend_pending(db, "h") == {}


# --- Regression tests for the PR#3 review fixes ---------------------------------------------

_SENTINELS = [
    "sk-live-SENTINELSECRET123ABC",             # sk- secret token
    "/run/user/1000/herdr/sock-abcdef123456",   # socket path
    "w4V:p1",                                    # pseudo pane id
    "/home/smith/.ssh/id_rsa",                  # absolute fs path
    "toolu_SENTINELDECISION01",                 # internal tool_use_id (dropped, not published)
    "AIzaSyD-ExampleKey1234567890abcdefghijk",  # google api key
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.SflKxwRJSMeKKF2QT4fwpMeJf",  # JWT
    "alice.jones@internal-corp.example",        # email / PII
    "10.4.2.9:5432",                            # internal ip:port
    "home/alice/.ssh/id_rsa",                  # home path without leading slash
]


def _leaky_decision_turn() -> dict:
    return {
        "pending_decision": {
            "decision_id": "toolu_SENTINELDECISION01",
            "prompt": (
                "Approve running against pane w4V:p1 at /home/smith/.ssh/id_rsa via "
                "/run/user/1000/herdr/sock-abcdef123456? Also rotate AIzaSyD-ExampleKey1234567890abcdefghijk "
                "and eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.SflKxwRJSMeKKF2QT4fwpMeJf; "
                "notify alice.jones@internal-corp.example on db 10.4.2.9:5432 at home/alice/.ssh/id_rsa"
            ),
            "options": [
                {"id": "approve", "label": "Use secret sk-live-SENTINELSECRET123ABC", "send_text": "sk-live-SENTINELSECRET123ABC"},
                {"id": "postgres", "label": "Postgres", "send_text": "Postgres"},
                {"id": "run", "label": "Deploy to 10.4.2.9:5432", "send_text": "tmux send-keys -t w4V:p1 'rm -rf /home/smith/.ssh'"},
            ],
        }
    }


def test_ingestion_redacts_private_data_from_pending():
    """Blocker regression: no private path/pane-id/secret/tool-id survives ingestion."""
    pending = _backend_pending_from_turn(_leaky_decision_turn())
    blob = json.dumps(pending)
    for sentinel in _SENTINELS:
        assert sentinel not in blob, f"private sentinel leaked into ingested pending: {sentinel!r}"
    # A benign label/value is preserved verbatim.
    labels = [c["label"] for c in pending["choices"]]
    assert "Postgres" in labels
    assert "[redacted]" in pending["question"]


def test_get_pending_public_json_has_no_private_leak(tmp_path: Path):
    """End-to-end: the sentinels must not reach the PUBLIC pending.list payload either."""
    db = tmp_path / "leak.db"
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "claude", "status": "blocked", "space_id": "s1"}])
    init_store(db)
    save_snapshot(db, snapshot)
    pending = _backend_pending_from_turn(_leaky_decision_turn())
    merge_backend_pending(db, "h", "worker-1", pending)

    public = json.dumps(TendwireDaemon(config).get_pending())
    for sentinel in _SENTINELS:
        assert sentinel not in public, f"private sentinel leaked into public pending.list: {sentinel!r}"
    # The benign choice survives; the raw-command choice value is dropped by _public_choice_value.
    assert "Postgres" in public


def test_get_pending_recomputes_content_fingerprint_and_shows_choices(tmp_path: Path):
    db = tmp_path / "fp.db"
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "claude", "status": "blocked", "space_id": "s1"}])
    init_store(db)
    save_snapshot(db, snapshot)
    baseline_fp = pending_payload_from_snapshot(snapshot)["content_fingerprint"]

    merge_backend_pending(db, "h", "worker-1", _backend_pending_from_turn(_decision_turn()))
    payload = TendwireDaemon(config).get_pending()

    # The overlaid list moves the change-token (was left stale before the fix).
    assert payload["content_fingerprint"] != baseline_fp
    interactions = [p for p in payload["pending_interactions"] if p["worker_id"] == "worker-1"]
    assert interactions and interactions[0]["question"] == "Which database should we use?"
    assert [c["label"] for c in interactions[0]["choices"]] == ["Postgres", "SQLite", "Tell me differently"]


def test_prune_backend_pending_reaps_orphaned_workers(tmp_path: Path):
    db = tmp_path / "orphan.db"
    init_store(db)
    live = {"question": "Q?", "kind": "question", "choices": [], "meta": {"source": "backend"}}
    merge_backend_pending(db, "h", "worker-live", live)
    merge_backend_pending(db, "h", "worker-gone", live)
    assert set(list_backend_pending(db, "h")) == {"worker-live", "worker-gone"}

    reaped = prune_backend_pending(db, "h", {"worker-live"})
    assert reaped == 1
    assert set(list_backend_pending(db, "h")) == {"worker-live"}
