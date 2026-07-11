"""Structured turn ingestion through Tendwire-owned private Herdr bindings."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import stable_fingerprint
from ..core.turns import InteractionChoice, is_internal_automation_turn_payload, redact_private_prompt_text
from ..store.sqlite import (
    list_worker_bindings,
    merge_backend_pending,
    merge_turn_content,
    prune_backend_pending,
)


_TURN_CONTENT_KEYS = (
    "user_text",
    "assistant_final_text",
    "assistant_stream_text",
    "model",
    "complete",
    "has_open_turn",
)
_CODEX_SESSION_TURN_KIND = "codex_session_id"
_OMP_SESSION_TURN_KIND = "omp_session_path"
_PANE_TURN_KIND = "pane_id"
_MAX_CODEX_STREAM_MESSAGES = 4
_OMP_TAIL_BYTES = 786432
_OMP_TOOL_SNIPPET_CHARS = 160
_PROMPTLESS_STATUS_FINAL_RE = re.compile(
    r"\b(?:"
    r"standing by|"
    r"waiting(?: quietly)?|"
    r"wait for|"
    r"no new state|"
    r"repeat tick|"
    r"startup line|"
    r"initial state|"
    r"current state|"
    r"monitor(?: reports|ing)?|"
    r"review in progress|"
    r"gate (?:phase|verdict|held|reached)|"
    r"not yet (?:reached|materialized)|"
    r"handoff|"
    r"phase"
    r")\b",
    re.IGNORECASE,
)


@dataclass
class _OmpSessionState:
    offset: int = 0
    file_id: tuple[int, int] | None = None
    prompt_id: str = ""
    user_text: str = ""
    stream_parts: list[str] = field(default_factory=list)
    final_text: str = ""
    tool_count: int = 0
    project_root: Path | None = None


@dataclass(frozen=True)
class _PublicOmpToolProgress:
    """Progress assembled only from constants and proven-safe relative paths."""

    action: str
    subject: str | None = None

    def render(self, step: int) -> str:
        body = f"{self.action}: {self.subject}" if self.subject else self.action
        return f"step {step} · {body}"




_OMP_SESSION_CACHE: dict[str, _OmpSessionState] = {}
_OMP_SESSION_CACHE_LOCK = threading.RLock()


def _extract_turn_payload(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    result = value.get("result")
    if isinstance(result, Mapping) and isinstance(result.get("turn"), Mapping):
        return result["turn"]
    if isinstance(value.get("turn"), Mapping):
        return value["turn"]
    return value


_PENDING_MAX_CHOICES = 12
_PENDING_TEXT_MAX = 2000


def _backend_pending_from_turn(turn: Mapping[str, Any]) -> dict[str, Any] | None:
    """Build public pending display text without backend identifiers or action data."""
    decision = turn.get("pending_decision")
    if isinstance(decision, Mapping):
        question = redact_private_prompt_text(
            decision.get("prompt"),
            max_chars=_PENDING_TEXT_MAX,
        )
        options = decision.get("options") if isinstance(decision.get("options"), list) else []
        choices: list[dict[str, str]] = []
        for ordinal, option in enumerate(options[:_PENDING_MAX_CHOICES]):
            if not isinstance(option, Mapping):
                continue
            label = redact_private_prompt_text(
                option.get("label"),
                max_chars=_PENDING_TEXT_MAX,
            )
            if not label:
                continue
            label = InteractionChoice(label=label).label
            choices.append(
                {
                    "choice_id": f"choice-{stable_fingerprint({'question': question, 'ordinal': ordinal, 'label': label})}",
                    "label": label,
                }
            )
        if not question and not choices:
            return None
        option_ids = {
            str(option.get("id") or "")
            for option in options
            if isinstance(option, Mapping)
        }
        return {
            "question": question or "Input needed",
            "kind": "approval" if "approve" in option_ids else "question",
            "choices": choices,
            "meta": {"source": "backend"},
        }
    interaction = turn.get("pending_interaction")
    if isinstance(interaction, Mapping):
        questions = (
            interaction.get("questions")
            if isinstance(interaction.get("questions"), list)
            else []
        )
        parts: list[str] = []
        for item in questions[:4]:
            if not isinstance(item, Mapping):
                continue
            part = redact_private_prompt_text(item.get("question"))
            if part:
                parts.append(part)
        question = " / ".join(parts)[:_PENDING_TEXT_MAX] or "Input needed (multi-question form)"
        return {
            "question": question,
            "kind": "review",
            "choices": [],
            "meta": {"source": "backend"},
        }
    return None


def _read_private_turn(config: Config, pane_id: str) -> Mapping[str, Any] | None:
    try:
        completed = subprocess.run(
            [
                config.herdr_bin,
                "pane",
                "turn",
                pane_id,
                "--last",
                "--format",
                "json",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=config.herdr_timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError, ValueError):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    turn = _extract_turn_payload(payload)
    if not isinstance(turn, Mapping) or turn.get("available") is False:
        return None
    open_user_text = turn.get("open_user_text")
    open_turn_id = str(turn.get("open_turn_id") or "").strip()
    if turn.get("has_open_turn") and (open_user_text or open_turn_id):
        # An in-progress turn is reported alongside the last completed one via
        # open_* fields. Emit it as its own open turn keyed by the stable prompt
        # id so the connector streams a live "working" card that later edits
        # into the final once this same id completes.
        return _open_turn_content(open_user_text, turn.get("assistant_stream_text"), open_turn_id)

    content = {key: turn.get(key) for key in _TURN_CONTENT_KEYS if key in turn}
    pending = _backend_pending_from_turn(turn)
    if pending is not None:
        content["_backend_pending"] = pending
    # Prefer the stable prompt-scoped id so a turn keeps one identity from
    # open through complete; fall back to turn_id for backends without it.
    source_turn_id = str(turn.get("source_turn_id") or turn.get("turn_id") or "").strip()
    if source_turn_id:
        content["source_turn_id"] = source_turn_id[:160]
    if _is_internal_turn_content(content):
        return None
    # Never clobber stored text with an empty value: notification-triggered
    # turns legitimately carry no prompt, but the previous real prompt and
    # stream must survive the merge.
    for key in ("user_text", "assistant_final_text", "assistant_stream_text"):
        if key in content and not (content.get(key) or "").strip():
            content.pop(key)
    if not any(value not in (None, "", False) for value in content.values()):
        return None
    return content


def _pop_backend_pending(content: Mapping[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Split the reserved _backend_pending key off a turn-content read. Returns (content, pending);
    content becomes None when nothing else remains."""
    if content is None:
        return None, None
    data = dict(content)
    pending = data.pop("_backend_pending", None)
    if not any(value not in (None, "", False) for value in data.values()):
        data = None
    return data, pending if isinstance(pending, dict) else None


def _open_turn_content(
    open_user_text: Any,
    stream_text: Any,
    open_turn_id: str,
) -> Mapping[str, Any] | None:
    if isinstance(open_user_text, str) and _is_internal_user_text(open_user_text):
        return None
    content: dict[str, Any] = {
        "assistant_final_text": None,
        "complete": False,
        "has_open_turn": True,
    }
    if isinstance(open_user_text, str) and open_user_text.strip():
        content["user_text"] = open_user_text
    if isinstance(stream_text, str) and stream_text.strip():
        content["assistant_stream_text"] = stream_text
    if open_turn_id:
        content["source_turn_id"] = open_turn_id[:160]
    if _is_internal_turn_content(content):
        return None
    if not (content.get("user_text") or content.get("assistant_stream_text")):
        return None
    return content


def _codex_home() -> Path:
    raw = os.environ.get("CODEX_HOME")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".codex"


def _safe_codex_session_id(session_id: str) -> str:
    clean = str(session_id or "").strip()
    if not clean or "/" in clean or "\\" in clean or clean in {".", ".."}:
        return ""
    return clean


def _find_codex_session_file(session_id: str) -> Path | None:
    clean = _safe_codex_session_id(session_id)
    if not clean:
        return None
    root = _codex_home() / "sessions"
    if not root.exists():
        return None
    matches = [
        path
        for path in root.rglob(f"*{clean}*.jsonl")
        if path.is_file()
    ]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _payload_turn_id(payload: Mapping[str, Any]) -> str:
    raw = payload.get("turn_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    metadata = payload.get("internal_chat_message_metadata_passthrough")
    if isinstance(metadata, Mapping):
        raw = metadata.get("turn_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _message_text(payload: Mapping[str, Any]) -> str:
    content = payload.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, Mapping):
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
        return "\n".join(parts)
    text = payload.get("message")
    if isinstance(text, str):
        return text
    return ""


_INTERNAL_USER_TEXT_PREFIXES = (
    "<subagent_notification>",
    "<environment_context>",
    "<local-command-caveat>",
    "<local-command-stdout>",
    "<local-command-stderr>",
    "<command-name>",
    "<command-message>",
    "<system-reminder>",
    "Caveat: The messages below were generated by the user while running local commands.",
)

def _is_internal_user_text(text: str) -> bool:
    clean = text.lstrip().replace("\r\n", "\n")
    return clean.startswith(_INTERNAL_USER_TEXT_PREFIXES) or is_internal_automation_turn_payload(
        {"user_text": clean}
    )


def _is_internal_turn_content(content: Mapping[str, Any]) -> bool:
    user_text = content.get("user_text")
    if isinstance(user_text, str) and _is_internal_user_text(user_text):
        return True
    if _is_promptless_status_final(content):
        return True
    return is_internal_automation_turn_payload(content)


def _is_promptless_status_final(content: Mapping[str, Any]) -> bool:
    if str(content.get("user_text") or "").strip():
        return False
    if content.get("complete") is False or content.get("has_open_turn") is True:
        return False
    final_text = str(content.get("assistant_final_text") or "").strip()
    if not final_text or len(final_text) > 800:
        return False
    return bool(_PROMPTLESS_STATUS_FINAL_RE.search(final_text))


def _append_unique_recent(items: list[str], text: str) -> None:
    clean = text.strip()
    if not clean:
        return
    if clean in items:
        items.remove(clean)
    items.append(clean)
    if len(items) > _MAX_CODEX_STREAM_MESSAGES:
        del items[: len(items) - _MAX_CODEX_STREAM_MESSAGES]


def _read_codex_session_turn(session_id: str) -> Mapping[str, Any] | None:
    session_file = _find_codex_session_file(session_id)
    if session_file is None:
        return None
    active_turn_id = ""
    last_content_turn_id = ""
    user_text_by_turn: dict[str, str] = {}
    stream_by_turn: dict[str, list[str]] = {}
    final_by_turn: dict[str, str] = {}
    complete_by_turn: dict[str, bool] = {}
    try:
        lines = session_file.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        try:
            item = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(item, Mapping):
            continue
        payload = item.get("payload")
        if not isinstance(payload, Mapping):
            continue
        event_type = str(payload.get("type") or "")
        if item.get("type") == "event_msg" and event_type == "task_started":
            turn_id = _payload_turn_id(payload)
            if turn_id:
                active_turn_id = turn_id
                complete_by_turn[turn_id] = False
            continue
        if item.get("type") == "event_msg" and event_type == "task_complete":
            turn_id = _payload_turn_id(payload) or active_turn_id
            final_text = str(payload.get("last_agent_message") or "")
            if turn_id and final_text:
                final_by_turn[turn_id] = final_text
                complete_by_turn[turn_id] = True
                last_content_turn_id = turn_id
            continue
        if item.get("type") != "response_item" or event_type != "message":
            continue
        role = str(payload.get("role") or "")
        turn_id = _payload_turn_id(payload) or active_turn_id
        if not turn_id:
            continue
        text = _message_text(payload)
        if not text:
            continue
        if role == "user":
            if _is_internal_user_text(text):
                continue
            user_text_by_turn[turn_id] = text
            complete_by_turn.setdefault(turn_id, False)
            last_content_turn_id = turn_id
            continue
        if role != "assistant":
            continue
        phase = str(payload.get("phase") or "")
        if phase == "commentary":
            _append_unique_recent(stream_by_turn.setdefault(turn_id, []), text)
            complete_by_turn.setdefault(turn_id, False)
            last_content_turn_id = turn_id
        else:
            final_by_turn[turn_id] = text
            complete_by_turn[turn_id] = True
            last_content_turn_id = turn_id
    turn_id = active_turn_id or last_content_turn_id
    if not turn_id:
        return None
    user_text = user_text_by_turn.get(turn_id)
    stream_text = "\n\n".join(stream_by_turn.get(turn_id, [])) or None
    final_text = final_by_turn.get(turn_id)
    has_final = bool(final_text)
    content = {
        "user_text": user_text,
        "assistant_stream_text": None if has_final else stream_text,
        "assistant_final_text": final_text,
        "complete": bool(complete_by_turn.get(turn_id)) if has_final else False,
        "has_open_turn": not has_final,
        "source_turn_id": turn_id[:160],
    }
    if _is_internal_turn_content(content):
        return None
    if not any(value not in (None, "", False) for value in content.values()):
        return None
    return content


def _omp_sessions_root() -> Path:
    raw = os.environ.get("OMP_SESSIONS_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".omp" / "agent" / "sessions"


def _safe_omp_session_path(value: str) -> Path | None:
    candidate = Path(str(value or "")).expanduser()
    root = _omp_sessions_root()
    try:
        candidate.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return None
    if candidate.suffix != ".jsonl" or not candidate.is_file():
        return None
    return candidate


def _omp_valid_git_directory(path: Path) -> bool:
    """Require bounded, parseable Git HEAD evidence inside a git directory."""
    try:
        if not path.is_dir():
            return False
        with open(path / "HEAD", encoding="ascii") as handle:
            head_text = handle.read(257)
    except (OSError, UnicodeError):
        return False
    if len(head_text) > 256:
        return False
    lines = head_text.splitlines()
    if len(lines) != 1:
        return False
    head = lines[0]
    if re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", head):
        return True
    if not head.startswith("ref: refs/"):
        return False
    reference = head[len("ref: ") :]
    return bool(reference) and all(
        part not in {"", ".", ".."} for part in reference.split("/")
    ) and all(character.isalnum() or character in "._-/" for character in reference)


def _omp_project_root(session_file: Path) -> Path | None:
    """Return the nearest repository root proven by the session cwd."""
    try:
        with open(session_file, encoding="utf-8", errors="replace") as handle:
            for _index in range(32):
                line = handle.readline()
                if not line or handle.tell() > 65536:
                    break
                try:
                    entry = json.loads(line)
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(entry, Mapping) or entry.get("type") != "session":
                    continue
                cwd = entry.get("cwd")
                if type(cwd) is not str or not cwd.strip():
                    return None
                resolved_cwd = Path(cwd).expanduser().resolve(strict=True)
                if not resolved_cwd.is_dir():
                    return None
                for root in (resolved_cwd, *resolved_cwd.parents):
                    git_marker = root / ".git"
                    if git_marker.is_dir():
                        return root if _omp_valid_git_directory(git_marker) else None
                    if not git_marker.exists():
                        continue
                    if not git_marker.is_file():
                        return None
                    with open(git_marker, encoding="utf-8") as marker_handle:
                        marker_text = marker_handle.read(4097)
                    if len(marker_text) > 4096:
                        return None
                    marker_lines = marker_text.splitlines()
                    if len(marker_lines) != 1 or not marker_lines[0].startswith("gitdir:"):
                        return None
                    reference = marker_lines[0][len("gitdir:") :].strip()
                    if not reference:
                        return None
                    git_dir = Path(reference)
                    if not git_dir.is_absolute():
                        git_dir = git_marker.parent / git_dir
                    resolved_git_dir = git_dir.resolve(strict=True)
                    return root if _omp_valid_git_directory(resolved_git_dir) else None
                return None
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return None
    return None


def _omp_file_id(stat_result: os.stat_result) -> tuple[int, int]:
    return (int(stat_result.st_dev), int(stat_result.st_ino))


def _read_omp_jsonl_lines(
    session_file: Path,
    *,
    start_offset: int,
    drop_first_partial: bool,
) -> tuple[list[str], int]:
    try:
        with open(session_file, "rb") as handle:
            handle.seek(start_offset)
            blob = handle.read()
    except OSError:
        return [], start_offset
    if not blob:
        return [], start_offset

    offset = start_offset
    segments = blob.splitlines(keepends=True)
    if drop_first_partial and segments:
        offset += len(segments[0])
        segments = segments[1:]

    lines: list[str] = []
    for index, segment in enumerate(segments):
        line_bytes = segment.rstrip(b"\r\n")
        if not line_bytes:
            offset += len(segment)
            continue
        text = line_bytes.decode("utf-8", "replace")
        has_line_end = segment.endswith(b"\n") or segment.endswith(b"\r")
        if not has_line_end and index == len(segments) - 1:
            try:
                json.loads(text)
            except (TypeError, json.JSONDecodeError):
                break
        lines.append(text)
        offset += len(segment)
    return lines, offset


def _omp_message_entry_from_line(line: str) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
    try:
        entry = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(entry, Mapping) or entry.get("type") != "message":
        return None
    message = entry.get("message")
    if not isinstance(message, Mapping):
        return None
    return entry, message


def _is_omp_user_message(message: Mapping[str, Any]) -> bool:
    return str(message.get("role") or "") == "user" and str(message.get("attribution") or "") == "user"


def _last_omp_user_line_index(lines: list[str]) -> int | None:
    found: int | None = None
    for index, line in enumerate(lines):
        parsed = _omp_message_entry_from_line(line)
        if parsed is None:
            continue
        _entry, message = parsed
        text = _omp_message_text(message)
        if _is_omp_user_message(message) and text and not _is_internal_user_text(text):
            found = index
    return found


def _omp_thinking_snippet(message: Mapping[str, Any]) -> str:
    """Compact progress line from an omp thinking block: its bold headline,
    falling back to a trimmed first line."""
    content = message.get("content")
    if not isinstance(content, list):
        return ""
    for item in content:
        if not isinstance(item, Mapping) or item.get("type") != "thinking":
            continue
        text = str(item.get("thinking") or "").strip()
        if not text:
            continue
        first = text.splitlines()[0].strip()
        if first.startswith("**") and first.endswith("**") and len(first) > 4:
            return first.strip("*").strip()
        return first[:120]
    return ""


_OMP_FILE_ACTIONS = {
    "read": "read",
    "read_file": "read",
    "write": "write",
    "write_file": "write",
    "edit": "edit",
    "edit_file": "edit",
    "apply_patch": "edit",
}
_OMP_ACTIONS = {
    "grep": "search",
    "search": "search",
    "ast_grep": "search",
    "glob": "list files",
    "list_files": "list files",
    "browser": "browse",
    "web_search": "search web",
    "task": "delegate",
    "agent": "delegate",
    "lsp": "inspect code",
}
_OMP_PRIVATE_PATH_SEGMENTS = frozenset(
    {
        ".git",
        ".netrc",
        ".npmrc",
        ".pypirc",
        ".ssh",
        "auth.json",
        "credential",
        "credentials",
        "credentials.json",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
        "secret",
        "secrets",
        "secrets.json",
        "token",
        "tokens",
    }
)


def _omp_path_segment_is_private(value: str) -> bool:
    lowered = value.lower()
    return (
        lowered in _OMP_PRIVATE_PATH_SEGMENTS
        or lowered.startswith(".env")
        or lowered.endswith(".key")
    )
_OMP_SHELL_TOOLS = frozenset({"bash", "shell", "sh", "exec", "execute", "run"})


def _omp_tool_name(item: Mapping[str, Any]) -> str:
    raw = item.get("name")
    if raw is None:
        raw = item.get("toolName")
    if raw is None:
        raw = item.get("tool")
    if type(raw) is not str:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")


def _omp_tool_arguments(item: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("arguments", "input", "args"):
        value = item.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _omp_repo_relative_path(
    arguments: Mapping[str, Any],
    project_root: Path | None,
) -> str | None:
    if project_root is None:
        return None
    raw: Any = None
    for key in ("path", "file_path", "filepath", "file"):
        if key in arguments:
            raw = arguments.get(key)
            break
    if type(raw) is not str:
        return None
    text = raw.strip()
    if not text or text.startswith("~") or any(ord(character) < 32 for character in text):
        return None
    try:
        root = project_root.resolve(strict=True)
        candidate = Path(text)
        if ".." in candidate.parts:
            return None
        resolved = candidate.resolve(strict=False) if candidate.is_absolute() else (root / candidate).resolve(strict=False)
        relative = resolved.relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return None
    if any(_omp_path_segment_is_private(part) for part in relative.parts):
        return None
    public_path = relative.as_posix()
    if not public_path or public_path == "." or len(public_path) > 120:
        return None
    allowed = frozenset("._-+@/ ")
    if any(not (character.isascii() and (character.isalnum() or character in allowed)) for character in public_path):
        return None
    return public_path


def _omp_shell_progress(arguments: Mapping[str, Any]) -> _PublicOmpToolProgress:
    command = arguments.get("command")
    if command is None:
        command = arguments.get("cmd")
    if type(command) is not str or any(character in command for character in "\r\n;&|`$<>"):
        return _PublicOmpToolProgress("run command")
    try:
        tokens = shlex.split(command)
    except ValueError:
        return _PublicOmpToolProgress("run command")
    if not tokens:
        return _PublicOmpToolProgress("run command")
    if tokens[:2] == ["git", "status"] and all(
        token in {"--short", "--porcelain", "--branch", "-s", "-sb"}
        for token in tokens[2:]
    ):
        return _PublicOmpToolProgress("git status")
    if tokens[0] == "pytest" or tokens[:3] in (["python", "-m", "pytest"], ["python3", "-m", "pytest"]):
        return _PublicOmpToolProgress("test", "pytest")
    if tokens[:3] == ["uv", "run", "pytest"]:
        return _PublicOmpToolProgress("test", "pytest")
    if len(tokens) >= 2 and tokens[0] in {"bun", "cargo", "go", "npm"} and tokens[1] == "test":
        return _PublicOmpToolProgress("test", tokens[0])
    if len(tokens) >= 2 and tokens[0] in {"cargo", "go"} and tokens[1] == "build":
        return _PublicOmpToolProgress("build", tokens[0])
    if tokens[:3] == ["npm", "run", "build"]:
        return _PublicOmpToolProgress("build", "npm")
    if tokens[0] == "make":
        return _PublicOmpToolProgress("build", "make")
    if tokens[:3] in (["python", "-m", "build"], ["python3", "-m", "build"]):
        return _PublicOmpToolProgress("build", "python")
    return _PublicOmpToolProgress("run command")


def _omp_public_tool_progress(
    item: Mapping[str, Any],
    project_root: Path | None,
) -> _PublicOmpToolProgress:
    name = _omp_tool_name(item)
    arguments = _omp_tool_arguments(item)
    if name in _OMP_FILE_ACTIONS:
        action = _OMP_FILE_ACTIONS[name]
        subject = _omp_repo_relative_path(arguments, project_root)
        return _PublicOmpToolProgress(action, subject) if subject else _PublicOmpToolProgress(f"{action} file")
    if name in _OMP_SHELL_TOOLS:
        return _omp_shell_progress(arguments)
    return _PublicOmpToolProgress(_OMP_ACTIONS.get(name, "tool"))


def _omp_tool_snippet(
    item: Mapping[str, Any],
    step: int,
    project_root: Path | None = None,
) -> str:
    return _omp_public_tool_progress(item, project_root).render(step)


def _apply_omp_progress_message(state: _OmpSessionState, message: Mapping[str, Any]) -> None:
    text = _omp_message_text(message)
    if text:
        _append_unique_recent(state.stream_parts, text)

    content = message.get("content")
    if not isinstance(content, list):
        return
    for item in content:
        if not isinstance(item, Mapping):
            continue
        kind = str(item.get("type") or "")
        if kind == "thinking":
            snippet = _omp_thinking_snippet({"content": [item]})
            if snippet:
                _append_unique_recent(state.stream_parts, snippet)
            continue
        if kind == "toolCall":
            state.tool_count += 1
            snippet = _omp_tool_snippet(item, state.tool_count, state.project_root)
            if snippet:
                _append_unique_recent(state.stream_parts, snippet)


def _omp_message_text(message: Mapping[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(item.get("text") or "")
            for item in content
            if isinstance(item, Mapping)
            and item.get("type") == "text"
            and str(item.get("text") or "").strip()
        )
    return ""


def _apply_omp_lines_to_state(state: _OmpSessionState, lines: list[str]) -> None:
    for line in lines:
        parsed = _omp_message_entry_from_line(line)
        if parsed is None:
            continue
        entry, message = parsed
        role = str(message.get("role") or "")
        text = _omp_message_text(message)
        if role == "user":
            if not _is_omp_user_message(message):
                continue
            if not text or _is_internal_user_text(text):
                continue
            state.prompt_id = str(entry.get("id") or "")
            state.user_text = text
            state.stream_parts = []
            state.final_text = ""
            state.tool_count = 0
            continue
        if role != "assistant" or not state.prompt_id:
            continue
        if str(message.get("stopReason") or "") == "stop" and text:
            state.final_text = text
            state.stream_parts = []
            continue
        state.final_text = ""
        _apply_omp_progress_message(state, message)


def _read_omp_state_from_recent(
    session_file: Path,
    size: int,
    file_id: tuple[int, int],
    project_root: Path | None,
) -> _OmpSessionState:
    state = _OmpSessionState(file_id=file_id, project_root=project_root)
    if size <= 0:
        return state

    window = min(size, max(1, _OMP_TAIL_BYTES))
    selected_lines: list[str] = []
    selected_offset = size
    while True:
        start = max(0, size - window)
        lines, next_offset = _read_omp_jsonl_lines(
            session_file,
            start_offset=start,
            drop_first_partial=start > 0,
        )
        user_index = _last_omp_user_line_index(lines)
        if user_index is not None:
            selected_lines = lines[user_index:]
            selected_offset = next_offset
            break
        if start == 0:
            selected_lines = lines
            selected_offset = next_offset
            break
        window = min(size, window * 2)

    _apply_omp_lines_to_state(state, selected_lines)
    state.offset = selected_offset
    return state


def _omp_state_to_content(state: _OmpSessionState) -> Mapping[str, Any] | None:
    if not state.prompt_id:
        return None
    has_final = bool(state.final_text)
    content: dict[str, Any] = {
        "user_text": state.user_text or None,
        "assistant_stream_text": None if has_final else ("\n\n".join(state.stream_parts) or None),
        "assistant_final_text": state.final_text or None,
        "complete": has_final,
        "has_open_turn": not has_final,
        "source_turn_id": state.prompt_id[:160],
    }
    if _is_internal_turn_content(content):
        return None
    if not (content.get("user_text") or content.get("assistant_stream_text") or content.get("assistant_final_text")):
        return None
    return content


def _read_omp_session_turn(path_value: str) -> Mapping[str, Any] | None:
    """Parse an oh-my-pi native session into the current public turn.

    Entries are ``{"type": "message", "id": ..., "message": {"role", "content",
    "stopReason", "attribution"}}``; ``attribution == "user"`` marks real human
    prompts, ``stopReason == "stop"`` marks a turn-final assistant message, and
    ``toolUse`` marks intermediate steps whose text and compact tool calls
    stream as progress.
    """
    session_file = _safe_omp_session_path(path_value)
    if session_file is None:
        return None
    try:
        stat_result = session_file.stat()
    except OSError:
        return None
    size = int(stat_result.st_size)
    file_id = _omp_file_id(stat_result)
    cache_key = str(session_file.resolve())
    project_root = _omp_project_root(session_file)

    with _OMP_SESSION_CACHE_LOCK:
        state = _OMP_SESSION_CACHE.get(cache_key)
        if state is None or state.file_id != file_id or size < state.offset:
            state = _read_omp_state_from_recent(session_file, size, file_id, project_root)
        else:
            lines, next_offset = _read_omp_jsonl_lines(
                session_file,
                start_offset=state.offset,
                drop_first_partial=False,
            )
            _apply_omp_lines_to_state(state, lines)
            state.offset = next_offset
            state.file_id = file_id
            state.project_root = project_root
        _OMP_SESSION_CACHE[cache_key] = state
        return _omp_state_to_content(state)


def _read_turn_for_binding(config: Config, binding: Any) -> Mapping[str, Any] | None:
    target_kind = str(getattr(binding, "turn_target_kind", "") or "")
    target_value = str(getattr(binding, "turn_target_value", "") or "")
    if not target_value:
        return None
    if target_kind == _CODEX_SESSION_TURN_KIND:
        return _read_codex_session_turn(target_value)
    if target_kind == _OMP_SESSION_TURN_KIND:
        return _read_omp_session_turn(target_value)
    if target_kind == _PANE_TURN_KIND:
        return _read_private_turn(config, target_value)
    return None


def refresh_structured_turn_content(config: Config) -> dict[str, Any]:
    """Refresh public turn text from private turn targets, if a turn-capable Herdr bin exists."""
    if config.db_path is None:
        return {"ok": False, "status": "store_unavailable", "updated": 0, "attempted": 0}
    bindings = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    turn_bindings = [
        binding
        for binding in bindings
        if binding.turn_target_kind in {_CODEX_SESSION_TURN_KIND, _OMP_SESSION_TURN_KIND, _PANE_TURN_KIND}
        and binding.turn_target_value
    ]
    updated = 0
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(turn_bindings)))) as pool:
        futures = {
            pool.submit(_read_turn_for_binding, config, binding): binding
            for binding in turn_bindings
        }
        for future in as_completed(futures):
            binding = futures[future]
            try:
                content = future.result()
            except Exception:
                content = None
            if content is None:
                continue
            content, pending = _pop_backend_pending(content)
            if binding.turn_target_kind == _PANE_TURN_KIND:
                # Presence-based: a successful pane read either carries the live pending prompt
                # (upsert) or it doesn't (the prompt was answered/dismissed -> prune the row).
                try:
                    merge_backend_pending(config.db_path, config.host_id, binding.worker_id, pending)
                except Exception:
                    # A pending-sync failure must not abort the whole refresh cycle.
                    pass
            if content is None:
                continue
            updated += merge_turn_content(
                config.db_path,
                config.host_id,
                binding.worker_id,
                content,
            )
    # Reap backend_pending rows for workers that no longer have a live binding (pane closed /
    # worker gone): presence-sync above only prunes workers still being polled, so a prompt open
    # at the moment a worker disappears would otherwise linger in pending.list forever.
    try:
        prune_backend_pending(
            config.db_path,
            config.host_id,
            {binding.worker_id for binding in bindings},
        )
    except Exception:
        pass
    return {"ok": True, "status": "ok", "updated": updated, "attempted": len(turn_bindings)}
