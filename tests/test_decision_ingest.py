"""Tests for decision-file ingestion into backend_pending (remote-decision carry)."""
from __future__ import annotations

import json

from tendwire.backends import decision_ingest
from tendwire.core.turns import PendingInteraction


def _write(dir_, session_id, **overrides):
    doc = {
        "schema": decision_ingest.DECISION_SCHEMA,
        "decision_ref": "dref-1",
        "kind": "single",
        "prompt": "Which fruit?",
        "options": [{"ref": "1", "label": "Apple"}, {"ref": "2", "label": "Banana"}],
        "session_id": session_id,
        "ts": 1_000_000.0,
    }
    doc.update(overrides)
    (dir_ / f"{session_id}.json").write_text(json.dumps(doc), encoding="utf-8")
    return doc


def test_read_decision_files_valid(tmp_path):
    _write(tmp_path, "sessA")
    files = decision_ingest.read_decision_files(tmp_path, now=1_000_100.0)
    assert set(files) == {"sessA"} and files["sessA"]["prompt"] == "Which fruit?"


def test_read_decision_files_rejects_wrong_schema_and_missing_session(tmp_path):
    (tmp_path / "a.json").write_text(json.dumps({"schema": "other", "session_id": "x"}), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps({"schema": decision_ingest.DECISION_SCHEMA}), encoding="utf-8")
    assert decision_ingest.read_decision_files(tmp_path) == {}


def test_read_decision_files_drops_stale(tmp_path):
    _write(tmp_path, "old", ts=1.0)
    assert decision_ingest.read_decision_files(tmp_path, now=10**9, ttl_seconds=3600) == {}


def test_backend_pending_single_preserves_order_and_refs(tmp_path):
    doc = _write(tmp_path, "s")
    pending = decision_ingest.backend_pending_from_decision(doc)
    assert pending["kind"] == "single"
    assert [c["choice_id"] for c in pending["choices"]] == ["1", "2"]
    assert [o["label"] for o in pending["meta"]["decision"]["options"]] == ["Apple", "Banana"]
    assert pending["meta"]["decision"]["decision_ref"] == "dref-1"


def test_backend_pending_multi_and_plan_kinds(tmp_path):
    multi = decision_ingest.backend_pending_from_decision(
        {"schema": decision_ingest.DECISION_SCHEMA, "session_id": "s", "kind": "single",
         "multi_select": True, "prompt": "Pick colors", "options": [{"ref": "1", "label": "Red"}]}
    )
    assert multi["kind"] == "multi" and multi["meta"]["decision"]["multi_select"] is True
    plan = decision_ingest.backend_pending_from_decision(
        {"schema": decision_ingest.DECISION_SCHEMA, "session_id": "s", "kind": "plan",
         "prompt": "Approve?", "options": [{"ref": "1", "label": "Yes, proceed"}, {"ref": "2", "label": "Revise"}]}
    )
    assert plan["kind"] == "plan"


def test_backend_pending_redacts_private_patterns_in_text():
    pending = decision_ingest.backend_pending_from_decision(
        {"schema": decision_ingest.DECISION_SCHEMA, "session_id": "s",
         "prompt": "Deploy from /home/alice/.ssh/id_rsa ?", "options": [{"ref": "1", "label": "term_65641d70 yes"}]}
    )
    assert "/home/alice" not in pending["question"]
    assert "term_65641d70" not in pending["choices"][0]["label"]
    # ordinary words are preserved
    assert "Deploy" in pending["question"] and "yes" in pending["choices"][0]["label"]


def test_backend_pending_none_without_options():
    assert decision_ingest.backend_pending_from_decision(
        {"schema": decision_ingest.DECISION_SCHEMA, "session_id": "s", "prompt": "hi", "options": []}
    ) is None


def test_decisions_by_worker_joins_by_session(tmp_path):
    _write(tmp_path, "sessA")
    _write(tmp_path, "sessB", prompt="Other?")
    out = decision_ingest.decisions_by_worker(
        {"sessA": "worker-1", "sessZ": "worker-9"}, directory=tmp_path, now=1_000_050.0
    )
    assert set(out) == {"worker-1"} and out["worker-1"]["question"] == "Which fruit?"


def test_structured_decision_survives_pending_interaction_roundtrip(tmp_path):
    """The get_pending overlay builds a PendingInteraction from the backend row; the structured
    decision (ordered options + refs) must survive to_dict so the connector can render + answer."""
    doc = _write(tmp_path, "s")
    pending = decision_ingest.backend_pending_from_decision(doc)
    interaction = PendingInteraction.from_dict(
        {
            "host_id": "h",
            "worker_id": "w-1",
            "question": pending["question"],
            "kind": pending["kind"],
            "choices": pending["choices"],
            "status": "open",
            "meta": pending["meta"],
        }
    )
    result = interaction.to_dict()
    decision = result.get("meta", {}).get("decision")
    assert decision is not None, "meta.decision was stripped by PendingInteraction"
    assert [o["label"] for o in decision["options"]] == ["Apple", "Banana"]
    assert {c["choice_id"] for c in result["choices"]} == {"1", "2"}
