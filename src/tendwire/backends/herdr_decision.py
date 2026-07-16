"""Translate semantic Claude decisions into private Herdr pane input.

Calibration assumptions are deliberately confined to this backend module, and
every mapping below was LIVE-VERIFIED against Claude Code 2.1.211 on a real
pane (2026-07-16):

* Single-choice and plan rows carry 1-based decimal shortcuts, and typing the
  ordinal alone SELECTS AND SUBMITS the row instantly — no Enter follows. A
  trailing Enter would leak into whatever UI appears next, so none is sent.
* The single-choice write-in row ("Type something", at position N + 1) does
  NOT respond to its digit. It is reached by pressing Down exactly N times
  from the initial cursor on row 1; the focused row is itself a text input,
  so Tendwire then sends the write-in prose and submits with Enter.
* Multi-select digits toggle their row ABSOLUTELY without moving the cursor,
  Right switches to the Submit tab (a review screen whose default focus is
  "Submit answers"), and Enter there submits the selection set.
* Every driven ordinal must stay a single keystroke, so decisions expose at
  most 9 real options (PENDING_DECISION_MAX_OPTIONS in herdr_turns).
* Herdr's private ``pane.send_keys`` accepts decimal character keys plus
  ``Down``, ``Up``, ``Right``, and ``Enter``; write-in prose uses
  ``pane.send_input`` so Herdr owns terminal text encoding and appends the
  final Enter atomically.

These steps are internal calibration data. They are never accepted from a
connector and there is intentionally no public raw-key command action.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


MULTI_SELECT_CALIBRATION = {
    "submit_tab": "Right",
    "submit": "Enter",
}


@dataclass(frozen=True)
class HerdrDecisionStep:
    """One private, already-calibrated Herdr pane operation."""

    operation: Literal["keys", "text", "input"]
    keys: tuple[str, ...] = ()
    text: str | None = None

    def __post_init__(self) -> None:
        if self.operation == "keys":
            if not self.keys or self.text is not None:
                raise ValueError("key calibration step requires only keys")
        elif self.operation == "text":
            raise ValueError("text calibration steps are no longer produced")
        elif self.operation == "input":
            if not isinstance(self.text, str) or not self.text or self.keys != ("Enter",):
                raise ValueError("input calibration step requires text plus Enter")
        else:
            raise ValueError("unsupported decision calibration operation")


def _digit_keys(value: int | str) -> tuple[str, ...]:
    text = str(value)
    if not text.isdigit() or int(text) < 1:
        raise ValueError("decision ordinal must be a positive decimal")
    return tuple(text)


def calibrate_decision_steps(
    *,
    kind: Literal["single", "multi", "plan"],
    option_count: int,
    option_refs: tuple[str, ...] = (),
    text: str | None = None,
) -> tuple[HerdrDecisionStep, ...]:
    """Return private pane operations for one validated semantic selection."""
    if (
        kind not in {"single", "multi", "plan"}
        or not isinstance(option_count, int)
        or isinstance(option_count, bool)
        or option_count < 1
    ):
        raise ValueError("invalid decision calibration context")
    if text is not None:
        if kind != "single" or option_refs or not isinstance(text, str) or not text:
            raise ValueError("invalid decision write-in calibration")
        # The write-in row ignores digits; reach it with Down x N from row 1,
        # where the focused row is itself the text input.
        return (
            HerdrDecisionStep("keys", keys=("Down",) * option_count),
            HerdrDecisionStep("input", keys=("Enter",), text=text),
        )
    if not option_refs or len(option_refs) != len(set(option_refs)):
        raise ValueError("decision option refs must be nonempty and unique")
    ordinals: list[int] = []
    for ref in option_refs:
        if not isinstance(ref, str) or not ref.isdigit():
            raise ValueError("decision option ref must be a decimal ordinal")
        ordinal = int(ref)
        if not 1 <= ordinal <= option_count:
            raise ValueError("decision option ref is out of range")
        ordinals.append(ordinal)
    if kind in {"single", "plan"}:
        if len(ordinals) != 1:
            raise ValueError("single and plan decisions require one option")
        # The digit alone selects AND submits; a trailing Enter would leak into
        # the next UI (composer, or worse, a modal).
        return (HerdrDecisionStep("keys", keys=_digit_keys(ordinals[0])),)

    # Digits toggle rows absolutely (cursor-independent); Right reaches the
    # Submit tab whose default focus is "Submit answers"; Enter submits.
    steps: list[HerdrDecisionStep] = [
        HerdrDecisionStep("keys", keys=_digit_keys(ordinal))
        for ordinal in sorted(ordinals)
    ]
    steps.append(
        HerdrDecisionStep(
            "keys",
            keys=(
                MULTI_SELECT_CALIBRATION["submit_tab"],
                MULTI_SELECT_CALIBRATION["submit"],
            ),
        )
    )
    return tuple(steps)
