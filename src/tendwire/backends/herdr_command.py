"""Narrow mutating command adapter for Herdr.

Only the high-level ``herdr agent send <target> <text>`` API is used here.
This module must not fall back to pane control, key sending, shell commands,
PTY control, signals, paste buffers, raw argv, or client-provided backend
parameters.
"""

from __future__ import annotations

from dataclasses import dataclass

import shutil
import subprocess
from typing import Any

from ..config import Config
from ..core.commands import (
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    error_value,
    sanitize_command_result,
)
from ..core.models import _string_value

_BACKEND_TARGET_KINDS = frozenset(
    {"agent_id", "terminal_id", "pane_id", "agent", "name", "label"}
)


@dataclass(frozen=True)
class _HerdrCommandResult:
    """Internal backend outcome, never a public command receipt envelope."""

    ok: bool
    status: str
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "result": sanitize_command_result(self.result),
            "error": sanitize_command_result(self.error),
        }


def _run_agent_send(
    config: Config,
    target_value: str,
    instruction_text: str,
) -> subprocess.CompletedProcess[str]:
    """Run the single allowed Herdr send surface with an argv list."""
    return subprocess.run(
        [config.herdr_bin, "agent", "send", target_value, instruction_text],
        capture_output=True,
        text=True,
        check=False,
        timeout=config.herdr_timeout_seconds,
    )


def _backend_error(
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> _HerdrCommandResult:
    return _HerdrCommandResult(
        ok=False,
        status=status,
        error=error_value(status, message, details=details),
    )


def send_instruction(
    config: Config,
    target: dict[str, Any],
    instruction: dict[str, Any],
) -> _HerdrCommandResult:
    """Send instruction text to the backend-resolved private Herdr target."""
    backend_target = target.get("backend_target")
    target_value = ""
    target_kind = ""
    target_reason = ""
    if isinstance(backend_target, dict):
        target_value = _string_value(backend_target.get("value"))
        target_kind = _string_value(backend_target.get("kind"))
        target_reason = _string_value(backend_target.get("reason"))
    public_worker_id = _string_value(target.get("worker_id"))
    instruction_text = instruction.get("text")

    if not isinstance(instruction_text, str) or not instruction_text:
        return _backend_error(
            STATUS_BACKEND_FAILED,
            "instruction text is missing after validation",
        )

    try:
        if shutil.which(config.herdr_bin) is None:
            return _backend_error(
                STATUS_BACKEND_UNAVAILABLE,
                "Herdr binary is unavailable",
            )
    except (OSError, TypeError, ValueError):
        return _backend_error(
            STATUS_BACKEND_UNAVAILABLE,
            "Herdr binary is unavailable",
        )

    if (
        not isinstance(backend_target, dict)
        or backend_target.get("sendable") is not True
        or target_kind not in _BACKEND_TARGET_KINDS
        or not target_value
    ):
        if target_reason in {"duplicate_backend_target", "not_unique"}:
            return _backend_error(
                STATUS_AMBIGUOUS_BACKEND_TARGET,
                "resolved target is ambiguous for backend send",
            )
        return _backend_error(
            STATUS_BACKEND_UNSUPPORTED,
            "resolved target has no backend-owned sendable target",
        )

    try:
        completed = _run_agent_send(config, target_value, instruction_text)
    except subprocess.TimeoutExpired:
        return _backend_error(
            STATUS_REQUEST_STATE_UNCERTAIN,
            "Herdr agent send timed out after starting",
            details={"timeout_seconds": config.herdr_timeout_seconds},
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError):
        return _backend_error(
            STATUS_BACKEND_UNAVAILABLE,
            "Herdr agent send could not be launched",
        )

    if completed.returncode == 0:
        return _HerdrCommandResult(
            ok=True,
            status=STATUS_ACCEPTED,
            result={"target": {"worker_id": public_worker_id}},
        )

    return _backend_error(
        STATUS_BACKEND_FAILED,
        "Herdr agent send exited non-zero",
        details={"exit_code": int(completed.returncode)},
    )
