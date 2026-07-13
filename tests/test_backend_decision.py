"""Tests for the remote-decision reconcile wiring: the backend_decision store, the session->worker
map, and the file-ingest round-trip that carries a structured decision to the connector."""
from __future__ import annotations

import json
from pathlib import Path

from tendwire.backends import decision_ingest
from tendwire.backends.herdr_cli import _WorkerRecord, _record_with_worker, _worker_record_from_item, session_worker_map
from tendwire.core.models import Worker
from tendwire.store.sqlite import (
    init_store,
    list_backend_decision,
    merge_backend_decision,
    prune_backend_decision,
)

HOST = "host-1"


def _db(tmp_path: Path) -> Path:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    return db_path


def _decision(question="Proceed?", options=None):
    return {"question": question, "kind": "single", "choices": options or [{"choice_id": "1", "label": "Yes"}],
            "meta": {"source": "backend", "decision": {"decision_ref": "d1", "kind": "single", "prompt": question,
                                                       "multi_select": False, "options": options or [{"ref": "1", "label": "Yes"}]}}}


# --- store --------------------------------------------------------------------
def test_merge_insert_then_idempotent(tmp_path):
    db = _db(tmp_path)
    assert merge_backend_decision(db, HOST, "claude-1", _decision()) is True   # inserted
    assert merge_backend_decision(db, HOST, "claude-1", _decision()) is False  # unchanged
    out = list_backend_decision(db, HOST)
    assert set(out) == {"claude-1"} and out["claude-1"]["question"] == "Proceed?"


def test_merge_none_prunes(tmp_path):
    db = _db(tmp_path)
    merge_backend_decision(db, HOST, "claude-1", _decision())
    assert merge_backend_decision(db, HOST, "claude-1", None) is True  # deleted a row
    assert list_backend_decision(db, HOST) == {}


def test_prune_reaps_dead_workers_only(tmp_path):
    db = _db(tmp_path)
    merge_backend_decision(db, HOST, "claude-1", _decision())
    merge_backend_decision(db, HOST, "claude-2", _decision())
    reaped = prune_backend_decision(db, HOST, live_worker_ids=["claude-1"])
    assert reaped == 1 and set(list_backend_decision(db, HOST)) == {"claude-1"}


def test_decision_and_pending_are_separate_tables(tmp_path):
    # backend_decision must not be clobbered by backend_pending writes for the same worker.
    from tendwire.store.sqlite import list_backend_pending, merge_backend_pending
    db = _db(tmp_path)
    merge_backend_decision(db, HOST, "claude-1", _decision(question="from-file"))
    merge_backend_pending(db, HOST, "claude-1", {"question": "from-scrape", "kind": "single", "choices": []})
    assert list_backend_decision(db, HOST)["claude-1"]["question"] == "from-file"
    assert list_backend_pending(db, HOST)["claude-1"]["question"] == "from-scrape"


# --- session -> worker map ----------------------------------------------------
def _rec(worker_id, session_value):
    return _WorkerRecord(worker=Worker(id=worker_id, name="claude", status="working", space_id="w1"),
                         private_fingerprint=f"fp-{worker_id}", session_value=session_value)


def test_session_worker_map_basic():
    m = session_worker_map([_rec("claude-1", "sess-a"), _rec("claude-2", "sess-b")])
    assert m == {"sess-a": "claude-1", "sess-b": "claude-2"}


def test_session_worker_map_skips_records_without_session():
    m = session_worker_map([_rec("claude-1", "sess-a"), _rec("claude-2", None)])
    assert m == {"sess-a": "claude-1"}


def test_record_with_worker_preserves_session_value():
    rec = _rec("claude-1", "sess-a")
    moved = _record_with_worker(rec, Worker(id="claude-1-1", name="claude", status="working", space_id="w1"))
    assert moved.session_value == "sess-a" and moved.worker.id == "claude-1-1"  # survives re-lettering


def test_worker_record_from_item_reads_session():
    item = {"agent": "claude", "agent_id": "a1", "agent_session": {"value": "sess-xyz"},
            "terminal_id": "term_9", "pane_id": "w1:p2", "workspace_id": "w1", "status": "working"}
    rec = _worker_record_from_item(item)
    assert rec.session_value == "sess-xyz"


# --- file -> store round-trip -------------------------------------------------
def test_ingest_file_to_store_roundtrip(tmp_path):
    db = _db(tmp_path)
    decisions_dir = tmp_path / "pending"
    decisions_dir.mkdir()
    (decisions_dir / "sess-a.json").write_text(json.dumps({
        "schema": decision_ingest.DECISION_SCHEMA, "decision_ref": "d1", "kind": "single",
        "prompt": "Deploy?", "options": [{"ref": "1", "label": "Yes"}, {"ref": "2", "label": "No"}],
        "session_id": "sess-a", "ts": 1_000_000.0,
    }), encoding="utf-8")
    # session map from a reconcile (session -> final worker id)
    per_worker = decision_ingest.decisions_by_worker({"sess-a": "claude-1"}, directory=decisions_dir, now=1_000_050.0)
    assert set(per_worker) == {"claude-1"}
    for worker_id, decision in per_worker.items():
        merge_backend_decision(db, HOST, worker_id, decision)
    stored = list_backend_decision(db, HOST)
    assert stored["claude-1"]["meta"]["decision"]["prompt"] == "Deploy?"
    assert [o["label"] for o in stored["claude-1"]["meta"]["decision"]["options"]] == ["Yes", "No"]
