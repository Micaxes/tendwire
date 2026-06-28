"""Narrow mutating command adapter for Herdr.

Only the high-level ``herdr agent send <target> <text>`` API is used here.
This module must not fall back to pane control, key sending, shell commands,
PTY control, signals, paste buffers, raw argv, or client-provided backend
parameters.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Any

from ..config import Config
from ..core.commands import (
    STATUS_ACCEPTED,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
    error_value,
)
from ..core.models import _string_value


_HERDR_SEND_TIMEOUT_SECONDS = 5.0


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


def _backend_error(status: str, message: str, details: dict[str, Any] | None = None) -> CommandEnvelope:
    return CommandEnvelope(
        ok=False,
        status=status,
        action="send_instruction",
        request_id=None,
        dry_run=False,
        result=None,
        error=error_value(status, message, details=details),
    )


def send_instruction(
    config: Config,
    target: dict[str, Any],
    instruction: dict[str, Any],
) -> CommandEnvelope:
    """Send instruction text to the backend-resolved private Herdr target."""
    backend_target = target.get("backend_target")
    target_value = ""
    if isinstance(backend_target, dict):
        target_value = _string_value(backend_target.get("value"))
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

    if not target_value:
        return _backend_error(
            STATUS_BACKEND_FAILED,
            "resolved target is missing a backend target",
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
        return CommandEnvelope(
            ok=True,
            status=STATUS_ACCEPTED,
            action="send_instruction",
            request_id=None,
            dry_run=False,
            result={"target": {"worker_id": public_worker_id}},
            error=None,
        )

    return _backend_error(
        STATUS_BACKEND_FAILED,
        "Herdr agent send exited non-zero",
        details={"exit_code": int(completed.returncode)},
    )
