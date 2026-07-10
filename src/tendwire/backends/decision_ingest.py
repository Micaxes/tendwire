"""Ingest structured decision files into backend_pending rows.

A Claude Code pending hook writes one small JSON file per blocked session when the agent shows an
AskUserQuestion / ExitPlanMode prompt, in the neutral ``herdr-decision-v1`` shape::

    {"schema": "herdr-decision-v1", "decision_ref": "<opaque>", "kind": "single|multi|plan",
     "prompt": "<text>", "options": [{"ref": "1", "label": "Yes"}, ...],
     "multi_select": false, "session_id": "<claude session uuid>", "ts": 1712345678.0}

tendwire reads it at reconcile, joins it to a worker by ``session_id`` (the only place the Claude
session uuid is visible), redacts every human-authored string through the same prose-safe redactor
used for pending question / choice-label text, and writes it to ``backend_pending`` keyed by the
neutral ``worker_id``. The daemon's ``get_pending`` overlay then surfaces it to a connector as a REAL
pending interaction with ordered choices — so the connector never reads herdr or a local file, and
never needs to understand Claude Code's prompt format. This module is pure (no socket, no store): it
turns files + a session→worker map into the ``{worker_id: backend_pending_dict}`` the caller writes.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from ..core.turns import redact_private_prompt_text

DECISION_SCHEMA = "herdr-decision-v1"
_MAX_OPTIONS = 32
# A pending file older than this is treated as stale (the pane was likely answered and the hook's
# cleanup lost the race, or the session died mid-prompt). Mirrors the hook's own TTL intent.
_DEFAULT_TTL_SECONDS = 6 * 60 * 60


def decision_pending_dir() -> Path:
    base = os.environ.get("HERDR_DECISION_DIR") or os.environ.get("HERDRES_PENDING_DIR")
    return Path(base) if base else (Path.home() / ".local" / "share" / "herdr" / "pending")


def _read_decision_file(path: Path, *, now: float | None, ttl_seconds: float) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema") != DECISION_SCHEMA:
        return None
    if not str(data.get("session_id") or "").strip():
        return None
    if now is not None:
        try:
            ts = float(data.get("ts") or 0.0)
        except (TypeError, ValueError):
            ts = 0.0
        if ts and now - ts > ttl_seconds:
            return None
    return data


def read_decision_files(
    directory: Path | str | None = None,
    *,
    now: float | None = None,
    ttl_seconds: float = _DEFAULT_TTL_SECONDS,
) -> dict[str, dict[str, Any]]:
    """Return ``{session_id: raw_decision}`` for every valid, non-stale decision file."""
    directory = Path(directory) if directory is not None else decision_pending_dir()
    try:
        paths = sorted(directory.glob("*.json"))
    except OSError:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for path in paths:
        data = _read_decision_file(path, now=now, ttl_seconds=ttl_seconds)
        if data is not None:
            out[str(data["session_id"]).strip()] = data
    return out


def _redacted_options(raw_options: Any) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    if not isinstance(raw_options, list):
        return options
    for index, option in enumerate(raw_options[:_MAX_OPTIONS]):
        if not isinstance(option, Mapping):
            continue
        ref = str(option.get("ref") or index + 1).strip() or str(index + 1)
        label = redact_private_prompt_text(option.get("label"))
        if not label:
            continue
        options.append({"ref": ref, "label": label})
    return options


def backend_pending_from_decision(decision: Mapping[str, Any]) -> dict[str, Any] | None:
    """Build the neutral backend_pending dict (question + ordered choices + a structured decision in
    meta) from a raw decision file, redacting every human string. Returns None if unusable."""
    if not isinstance(decision, Mapping):
        return None
    kind = str(decision.get("kind") or "single").strip().lower()
    if kind not in {"single", "multi", "plan"}:
        kind = "single"
    prompt = redact_private_prompt_text(decision.get("prompt")) or "Input needed."
    options = _redacted_options(decision.get("options"))
    if not options:
        return None
    decision_ref = str(decision.get("decision_ref") or "").strip()
    multi_select = bool(decision.get("multi_select")) or kind == "multi"
    # choices: ordered, ref preserved as choice_id so the connector can map a label back to its
    # position/number when building the key sequence (get_pending sorts choices, but the ref carries
    # the ordinal, and meta.decision keeps the authoritative order too).
    choices = [{"choice_id": opt["ref"], "label": opt["label"]} for opt in options]
    structured = {
        "decision_ref": decision_ref,
        "kind": "multi" if multi_select else kind,
        "prompt": prompt,
        "multi_select": multi_select,
        "options": options,
    }
    return {
        "question": prompt,
        "kind": "multi" if multi_select else kind,
        "choices": choices,
        "meta": {"source": "backend", "decision": structured},
    }


def decisions_by_worker(
    session_to_worker: Mapping[str, str],
    *,
    directory: Path | str | None = None,
    now: float | None = None,
    ttl_seconds: float = _DEFAULT_TTL_SECONDS,
) -> dict[str, dict[str, Any]]:
    """Join decision files to workers by session_id and return ``{worker_id: backend_pending_dict}``.

    ``session_to_worker`` comes from the reconcile site, where the raw herdr pane items expose
    ``agent_session.value`` (the Claude session uuid) alongside the derived neutral worker id."""
    files = read_decision_files(directory, now=now, ttl_seconds=ttl_seconds)
    out: dict[str, dict[str, Any]] = {}
    for session_id, worker_id in session_to_worker.items():
        decision = files.get(str(session_id).strip())
        if decision is None:
            continue
        pending = backend_pending_from_decision(decision)
        if pending is not None:
            out[str(worker_id)] = pending
    return out
