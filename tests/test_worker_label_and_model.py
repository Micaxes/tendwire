"""Pane label in public worker meta + model on turns (consumed by herdres for topic names and the
pinned status board)."""
from __future__ import annotations

from pathlib import Path

from tendwire.backends.herdr_cli import _worker_from_item
from tendwire.backends.herdr_events import HerdrEventBackend
from tendwire.backends.herdr_turns import _TURN_CONTENT_KEYS
from tendwire.config import Config
from tendwire.core.turns import Turn
from tendwire.core.projector import project_from_raw
from tendwire.store.sqlite import init_store, merge_turn_content, save_snapshot, turns_payload_from_store


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id="label-host",
        data_dir=tmp_path,
        db_path=tmp_path / "label-host.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _pane_item(label: str = "doro") -> dict:
    # Both shapes carry the same agent_session (as real herdr payloads do), so they share the
    # private identity fingerprint and collide in the reconcile dedup.
    return {
        "pane_id": "ws-1:p2Q",
        "terminal_id": "term-1",
        "workspace_id": "ws-1",
        "agent": "claude",
        "agent_session": {"kind": "id", "value": "sess-1"},
        "label": label,
        "cwd": "/root/temp",
        "agent_status": "idle",
    }


def _agent_item() -> dict:
    return {
        "name": "claude",
        "agent": "claude",
        "workspace_id": "ws-1",
        "status": "idle",
        "pane_id": "ws-1:p2Q",
        "terminal_id": "term-1",
        "agent_session": {"kind": "id", "value": "sess-1"},
    }


def test_pane_label_lands_in_public_worker_meta() -> None:
    worker = _worker_from_item(_pane_item())
    assert worker is not None
    assert worker.meta.get("label") == "doro"
    # name resolution unchanged: agent-first
    assert worker.name == "claude"


def test_reconcile_prefers_pane_shaped_record_with_label(tmp_path: Path) -> None:
    config = _config(tmp_path)
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    records = backend._records_from_reconcile_payloads(
        {"agents": [_agent_item()]},
        {"panes": [_pane_item()]},
    )
    # both shapes share the private fingerprint -> exactly one record, the pane one (carries the label)
    assert len(records) == 1
    assert records[0].worker.meta.get("label") == "doro"


def test_turn_model_round_trip_and_id_stability() -> None:
    base = {"host_id": "h", "worker_id": "w1", "kind": "turn", "source": "herdr",
            "user_text": "hi", "assistant_final_text": "done", "complete": True}
    plain = Turn.from_dict(base)
    with_model = Turn.from_dict({**base, "model": "claude-fable-5[1m]"})
    assert with_model.model == "claude-fable-5[1m]"
    assert with_model.to_dict()["model"] == "claude-fable-5[1m]"
    assert plain.id == with_model.id                    # model is content, NOT identity (no id re-mint)
    assert plain.fingerprint != with_model.fingerprint  # but the content fingerprint reflects it


def test_turn_content_keys_include_model() -> None:
    assert "model" in _TURN_CONTENT_KEYS


def test_merge_turn_content_persists_model(tmp_path: Path) -> None:
    db = tmp_path / "turns.db"
    config = Config(host_id="turn-host", db_path=db)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db)
    save_snapshot(db, snapshot)
    updated = merge_turn_content(
        db, "turn-host", "worker-1",
        {"user_text": "hi", "assistant_final_text": "done", "complete": True, "model": "claude-fable-5"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    payload = turns_payload_from_store(db, "turn-host", snapshot=snapshot)
    assert updated == 1
    assert payload["turns"][0].get("model") == "claude-fable-5"
