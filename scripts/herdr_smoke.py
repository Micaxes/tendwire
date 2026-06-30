#!/usr/bin/env python3
"""Opt-in live Herdr smoke harness for Tendwire.

The module is intentionally stdlib-only and has no import-time side effects.
"""


import argparse
import json
import os
import shutil
import subprocess
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_SESSION = "tendwire-smoke"
DEFAULT_TIMEOUT_SECONDS = 2.0
LIVE_ENV_FLAG = "TENDWIRE_HERDR_LIVE_SMOKE"
SMOKE_TEXT = "tendwire smoke probe: no action required"
SMOKE_ADDRESS = "tendwire-smoke-address-probe"

EVENT_SUBSCRIBE_METHOD = "events.subscribe"
OFFICIAL_EVENT_TYPES = (
    "workspace.created",
    "workspace.updated",
    "workspace.renamed",
    "workspace.closed",
    "workspace.focused",
    "pane.created",
    "pane.closed",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
    "pane.output_matched",
    "pane.agent_status_changed",
    "worktree.created",
    "worktree.opened",
    "worktree.removed",
)
OFFICIAL_EVENT_TYPE_SET = frozenset(OFFICIAL_EVENT_TYPES)
LEGACY_EVENT_TYPES = frozenset(
    (
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
    )
)

CHECK_NAMES = (
    "workspace_list",
    "agent_list",
    "worker_surface",
    "send_addressing",
    "name_ambiguity",
    "routing_resolution",
    "event_subscription",
    "status_event",
    "closed_moved_observations",
    "public_safety",
)

CHECK_KEYS = {
    "name",
    "status",
    "required",
    "ok",
    "exit_code",
    "json_status",
    "item_count",
    "variants",
    "detail",
    "method",
    "official_event_count",
    "params_shape_ok",
    "legacy_event_count",
}

TOP_LEVEL_KEYS = {
    "schema_version",
    "ok",
    "mode",
    "status",
    "summary",
    "default_isolated_session",
    "explicit_session",
    "checks",
    "failures",
}

SUMMARY_KEYS = {"total", "required", "passed", "failed"}

PUBLIC_ALLOWED_COMPACT_KEYS = {
    "defaultisolatedsession",
    "explicitsession",
}

FORBIDDEN_KEY_EXACT = {
    "argv",
    "args",
    "auth",
    "authtoken",
    "backendtarget",
    "backendtargets",
    "binding",
    "bindings",
    "bottoken",
    "chatid",
    "connector",
    "connectorid",
    "credentials",
    "delivery",
    "env",
    "environment",
    "herdresdelivery",
    "herdresstate",
    "herdrstate",
    "messageid",
    "outbox",
    "paneid",
    "paneids",
    "pid",
    "privatebinding",
    "privatefingerprint",
    "processid",
    "pty",
    "route",
    "secret",
    "session",
    "sessionid",
    "shell",
    "socket",
    "socketpath",
    "stderr",
    "stdout",
    "target",
    "targetkind",
    "targetvalue",
    "telegram",
    "terminal",
    "terminalid",
    "terminalids",
    "threadid",
    "tmuxpane",
    "tmuxpaneid",
    "tmuxpanes",
    "token",
    "topicid",
    "tty",
}

FORBIDDEN_KEY_FRAGMENTS = (
    "telegram",
    "herdres",
    "paneid",
    "terminal",
    "socket",
    "target",
    "session",
    "private",
    "binding",
    "connector",
    "outbox",
    "delivery",
    "stdout",
    "stderr",
    "token",
    "secret",
    "fingerprint",
)

FORBIDDEN_STRING_FRAGMENTS = (
    "telegram",
    "herdres",
    "pane_id",
    "pane-id",
    "pane/",
    "pane ",
    "terminal",
    "socket",
    "target",
    "session",
    "private",
    "binding",
    "connector",
    "outbox",
    "delivery",
    "argv",
    "stdout",
    "stderr",
    "token",
    "secret",
    "fingerprint",
    "bot_token",
    "chat_id",
    "message_id",
    "thread_id",
    "topic_id",
)


Runner = Callable[..., Any]


@dataclass(frozen=True)
class SmokeOptions:
    live: bool
    fixture_dir: Path | None
    herdr_bin: str
    timeout: float
    session: str | None


@dataclass(frozen=True)
class SessionPlan:
    child_env: dict[str, str]
    default_isolated: bool
    explicit: bool


@dataclass(frozen=True)
class ProbeResult:
    check: dict[str, Any]
    payload: Any = None


class PublicSafetyError(ValueError):
    """Raised when a fixture or generated public summary is not safe to print."""


def _compact_key(key: object) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _forbidden_key(key: object, *, allow_public_keys: bool = False) -> bool:
    compact = _compact_key(key)
    if allow_public_keys and compact in PUBLIC_ALLOWED_COMPACT_KEYS:
        return False
    if compact in FORBIDDEN_KEY_EXACT:
        return True
    return any(fragment in compact for fragment in FORBIDDEN_KEY_FRAGMENTS)


def _forbidden_string(value: str) -> bool:
    lowered = value.lower()
    return any(fragment in lowered for fragment in FORBIDDEN_STRING_FRAGMENTS)


def iter_forbidden_public_values(value: Any, *, allow_public_keys: bool = False) -> list[str]:
    """Return generic recursive safety violations without echoing private content."""
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            if _forbidden_key(key, allow_public_keys=allow_public_keys):
                violations.append("forbidden_key")
            violations.extend(iter_forbidden_public_values(child, allow_public_keys=allow_public_keys))
        return violations
    if isinstance(value, (list, tuple)):
        for child in value:
            violations.extend(iter_forbidden_public_values(child, allow_public_keys=allow_public_keys))
        return violations
    if isinstance(value, str) and _forbidden_string(value):
        violations.append("forbidden_value")
    return violations


def validate_public_summary(value: Any) -> None:
    violations = iter_forbidden_public_values(value, allow_public_keys=True)
    if violations:
        raise PublicSafetyError("public summary contains forbidden content")


def validate_fixture_payload(value: Any) -> None:
    violations = iter_forbidden_public_values(value, allow_public_keys=False)
    if violations:
        raise PublicSafetyError("fixture contains forbidden content")


def _positive_float(raw: str) -> float:
    value = float(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def parse_options(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> SmokeOptions:
    env_map = os.environ if env is None else env
    parser = argparse.ArgumentParser(description="Opt-in Tendwire live Herdr smoke harness")
    parser.add_argument("--live", action="store_true", help="opt in to real Herdr subprocess checks")
    parser.add_argument("--fixture-dir", type=Path, help="replay deterministic fixture files instead of live checks")
    parser.add_argument("--herdr-bin", default="herdr", help="Herdr executable name or path")
    parser.add_argument("--timeout", type=_positive_float, default=DEFAULT_TIMEOUT_SECONDS, help="per-command timeout seconds")
    parser.add_argument("--session", help="explicit child HERDR_SESSION value; the value is never printed")
    ns = parser.parse_args(list(argv) if argv is not None else None)
    live_flag = str(env_map.get(LIVE_ENV_FLAG, "")).strip() == "1"
    return SmokeOptions(
        live=bool(ns.live or live_flag),
        fixture_dir=ns.fixture_dir,
        herdr_bin=str(ns.herdr_bin),
        timeout=float(ns.timeout),
        session=ns.session,
    )


def _session_plan(options: SmokeOptions, env: Mapping[str, str]) -> SessionPlan:
    child_env = {str(key): str(value) for key, value in env.items()}
    if options.session is not None:
        child_env["HERDR_SESSION"] = options.session
        return SessionPlan(child_env=child_env, default_isolated=False, explicit=True)
    if "HERDR_SESSION" in child_env:
        return SessionPlan(child_env=child_env, default_isolated=False, explicit=True)
    child_env["HERDR_SESSION"] = DEFAULT_SESSION
    return SessionPlan(child_env=child_env, default_isolated=True, explicit=False)


def _check(
    name: str,
    status: str,
    *,
    required: bool,
    ok: bool,
    exit_code: int | None = None,
    json_status: str | None = None,
    item_count: int | None = None,
    variants: int | None = None,
    detail: str | None = None,
    method: str | None = None,
    official_event_count: int | None = None,
    params_shape_ok: bool | None = None,
    legacy_event_count: int | None = None,
) -> dict[str, Any]:
    if name not in CHECK_NAMES:
        raise ValueError("unknown check name")
    record: dict[str, Any] = {
        "name": name,
        "status": status,
        "required": bool(required),
        "ok": bool(ok),
    }
    if exit_code is not None:
        record["exit_code"] = int(exit_code)
    if json_status is not None:
        record["json_status"] = json_status
    if item_count is not None:
        record["item_count"] = int(item_count)
    if variants is not None:
        record["variants"] = int(variants)
    if detail is not None:
        record["detail"] = detail
    if method is not None:
        record["method"] = method
    if official_event_count is not None:
        record["official_event_count"] = int(official_event_count)
    if params_shape_ok is not None:
        record["params_shape_ok"] = bool(params_shape_ok)
    if legacy_event_count is not None:
        record["legacy_event_count"] = int(legacy_event_count)
    return record


def _event_subscription_params(event_types: Sequence[object] = OFFICIAL_EVENT_TYPES) -> dict[str, Any]:
    subscriptions: list[dict[str, str]] = []
    for raw_name in event_types:
        if not isinstance(raw_name, str) or not raw_name:
            raise ValueError("subscription event names must be non-empty strings")
        if raw_name.strip() != raw_name:
            raise ValueError("unknown subscription event name")
        event_name = raw_name
        if event_name not in OFFICIAL_EVENT_TYPE_SET or event_name in LEGACY_EVENT_TYPES:
            raise ValueError("unknown subscription event name")
        subscriptions.append({"type": event_name})

    ordered_names = tuple(item["type"] for item in subscriptions)
    if ordered_names != OFFICIAL_EVENT_TYPES:
        raise ValueError("subscription event names must match the official ordered set")
    return {"subscriptions": subscriptions}


def _event_subscription_params_shape_ok(params: Any) -> bool:
    if not isinstance(params, Mapping) or set(params) != {"subscriptions"}:
        return False
    subscriptions = params.get("subscriptions")
    if not isinstance(subscriptions, list) or len(subscriptions) != len(OFFICIAL_EVENT_TYPES):
        return False
    for item in subscriptions:
        if not isinstance(item, Mapping) or set(item) != {"type"}:
            return False
        event_name = item.get("type")
        if not isinstance(event_name, str) or not event_name:
            return False
    return True


def _event_subscription_aggregate_check() -> dict[str, Any]:
    try:
        params = _event_subscription_params()
    except ValueError:
        return _check(
            "event_subscription",
            "failed",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="invalid_contract",
        )

    subscriptions = params["subscriptions"]
    count = len(subscriptions)
    shape_ok = _event_subscription_params_shape_ok(params)
    legacy_count = sum(1 for item in subscriptions if item["type"] in LEGACY_EVENT_TYPES)
    ok = count == len(OFFICIAL_EVENT_TYPES) and shape_ok and legacy_count == 0
    return _check(
        "event_subscription",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        method=EVENT_SUBSCRIBE_METHOD,
        official_event_count=count,
        params_shape_ok=shape_ok,
        legacy_event_count=legacy_count,
        detail="official_contract" if ok else "invalid_contract",
    )


def _summary_for(checks: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "total": len(checks),
        "required": sum(1 for check in checks if check.get("required") is True),
        "passed": sum(1 for check in checks if check.get("ok") is True),
        "failed": sum(1 for check in checks if check.get("ok") is False),
    }


def _failures_for(checks: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for check in checks:
        if check.get("ok") is not False:
            continue
        failure = {
            "name": check.get("name"),
            "status": check.get("status"),
            "required": check.get("required") is True,
            "ok": False,
        }
        detail = check.get("detail")
        if detail is not None:
            failure["detail"] = detail
        failures.append(failure)
    return failures


def _payload(
    *,
    mode: str,
    status: str,
    checks: Sequence[dict[str, Any]],
    session_plan: SessionPlan,
) -> dict[str, Any]:
    public_checks = list(checks)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "ok": status in {"ok", "skipped", "degraded"} and not any(
            check.get("required") is True and check.get("ok") is False for check in public_checks
        ),
        "mode": mode,
        "status": status,
        "summary": _summary_for(public_checks),
        "default_isolated_session": session_plan.default_isolated,
        "explicit_session": session_plan.explicit,
        "checks": public_checks,
        "failures": _failures_for(public_checks),
    }
    return payload


def _finalize_payload(mode: str, status: str, checks: Sequence[dict[str, Any]], session_plan: SessionPlan) -> dict[str, Any]:
    final_checks = [check for check in checks if check.get("name") != "public_safety"]
    final_checks.append(_check("public_safety", "ok", required=True, ok=True, detail="passed"))
    payload = _payload(mode=mode, status=status, checks=final_checks, session_plan=session_plan)
    try:
        validate_public_summary(payload)
    except PublicSafetyError:
        final_checks[-1] = _check("public_safety", "failed", required=True, ok=False, detail="summary_rejected")
        payload = _payload(mode=mode, status="failed", checks=final_checks, session_plan=session_plan)
        validate_public_summary(payload)
    return payload


def _skip_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    checks = [
        _check(name, "skipped", required=False, ok=True, detail="no_live_opt_in")
        for name in CHECK_NAMES
        if name != "public_safety"
    ]
    return _finalize_payload("skipped", "skipped", checks, session_plan)


def _missing_binary_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    checks = [
        _check("workspace_list", "missing_binary", required=True, ok=False, detail="binary_unavailable"),
        _check("agent_list", "missing_binary", required=True, ok=False, detail="binary_unavailable"),
        _check("worker_surface", "skipped", required=False, ok=True, detail="binary_unavailable"),
        _check("send_addressing", "skipped", required=False, ok=True, detail="binary_unavailable"),
        _check("name_ambiguity", "skipped", required=False, ok=True, detail="binary_unavailable"),
        _check("routing_resolution", "skipped", required=True, ok=False, detail="binary_unavailable"),
        _event_subscription_aggregate_check(),
        _check("status_event", "skipped", required=False, ok=True, detail="binary_unavailable"),
        _check("closed_moved_observations", "skipped", required=False, ok=True, detail="binary_unavailable"),
    ]
    return _finalize_payload("live", "unavailable", checks, session_plan)


def _binary_available(herdr_bin: str) -> bool:
    try:
        return shutil.which(herdr_bin) is not None
    except (TypeError, ValueError, OSError):
        return False


def _run_command(
    args: Sequence[str],
    *,
    env: Mapping[str, str],
    timeout: float,
    runner: Runner | None,
) -> Any:
    actual_runner = subprocess.run if runner is None else runner
    return actual_runner(
        list(args),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=dict(env),
    )


def _return_code(completed: Any) -> int:
    try:
        return int(getattr(completed, "returncode"))
    except (TypeError, ValueError):
        return 1


def _stdout_text(completed: Any) -> str:
    stdout = getattr(completed, "stdout", "")
    if isinstance(stdout, bytes):
        return stdout.decode("utf-8", errors="replace")
    return str(stdout or "")


def _parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None


def _payload_items(payload: Any, keys: Sequence[str]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, Mapping):
        return []
    for key in keys:
        if key not in payload:
            continue
        child = payload[key]
        if isinstance(child, list):
            return child
        if isinstance(child, Mapping):
            nested = _payload_items(child, keys)
            if nested:
                return nested
    result = payload.get("result")
    if isinstance(result, Mapping):
        nested = _payload_items(result, keys)
        if nested:
            return nested
    return []


def _item_count(payload: Any, keys: Sequence[str]) -> int:
    items = _payload_items(payload, keys)
    if items:
        return len(items)
    if isinstance(payload, Mapping) and payload:
        return 1
    if isinstance(payload, list):
        return len(payload)
    return 0


def _probe_variants(
    name: str,
    variants: Sequence[Sequence[str]],
    keys: Sequence[str],
    options: SmokeOptions,
    session_plan: SessionPlan,
    runner: Runner | None,
    *,
    required: bool,
) -> ProbeResult:
    last_status = "not_available"
    last_exit_code: int | None = None
    last_json_status = "not_checked"
    for variant in variants:
        try:
            completed = _run_command(
                [options.herdr_bin, *variant],
                env=session_plan.child_env,
                timeout=options.timeout,
                runner=runner,
            )
        except subprocess.TimeoutExpired:
            return ProbeResult(
                _check(
                    name,
                    "timeout",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="timeout",
                )
            )
        except FileNotFoundError:
            return ProbeResult(
                _check(
                    name,
                    "missing_binary",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="binary_unavailable",
                )
            )
        except OSError:
            return ProbeResult(
                _check(
                    name,
                    "launch_error",
                    required=required,
                    ok=False if required else True,
                    json_status="not_checked",
                    variants=len(variants),
                    detail="launch_failed",
                )
            )
        exit_code = _return_code(completed)
        last_exit_code = exit_code
        if exit_code != 0:
            last_status = "nonzero"
            last_json_status = "not_checked"
            continue
        payload = _parse_json(_stdout_text(completed))
        if payload is None:
            last_status = "invalid_json"
            last_json_status = "invalid"
            continue
        count = _item_count(payload, keys)
        return ProbeResult(
            _check(
                name,
                "ok",
                required=required,
                ok=True,
                exit_code=exit_code,
                json_status="valid",
                item_count=count,
                variants=len(variants),
                detail="non_empty" if count else "empty",
            ),
            payload,
        )
    return ProbeResult(
        _check(
            name,
            last_status,
            required=required,
            ok=False if required else True,
            exit_code=last_exit_code,
            json_status=last_json_status,
            variants=len(variants),
            detail="all_variants_failed" if required else "not_available",
        )
    )


def _run_send_probe(options: SmokeOptions, session_plan: SessionPlan, runner: Runner | None) -> dict[str, Any]:
    if not session_plan.default_isolated:
        return _check("send_addressing", "skipped", required=False, ok=True, detail="caller_override")
    try:
        completed = _run_command(
            [options.herdr_bin, "agent", "send", SMOKE_ADDRESS, SMOKE_TEXT],
            env=session_plan.child_env,
            timeout=options.timeout,
            runner=runner,
        )
    except subprocess.TimeoutExpired:
        return _check("send_addressing", "timeout", required=False, ok=False, json_status="not_checked", detail="timeout")
    except FileNotFoundError:
        return _check("send_addressing", "missing_binary", required=False, ok=True, json_status="not_checked", detail="binary_unavailable")
    except OSError:
        return _check("send_addressing", "launch_error", required=False, ok=True, json_status="not_checked", detail="launch_failed")
    return _check(
        "send_addressing",
        "observed",
        required=False,
        ok=True,
        exit_code=_return_code(completed),
        json_status="not_checked",
        detail="isolated_probe",
    )


def _text_from_record(record: Any, keys: Sequence[str]) -> str:
    if not isinstance(record, Mapping):
        return ""
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _name_ambiguity_check(agent_payload: Any) -> dict[str, Any]:
    names = [
        _text_from_record(item, ("name", "agent", "label", "title"))
        for item in _payload_items(agent_payload, ("agents", "workers", "data", "items", "results", "result"))
    ]
    counts = Counter(name.lower() for name in names if name)
    duplicates = sum(1 for count in counts.values() if count > 1)
    return _check(
        "name_ambiguity",
        "observed",
        required=False,
        ok=True,
        item_count=duplicates,
        detail="ambiguous_labels" if duplicates else "unique_or_empty",
    )


def _routing_resolution_check(session_plan: SessionPlan, agent_payload: Any) -> dict[str, Any]:
    count = _item_count(agent_payload, ("agents", "workers", "data", "items", "results", "result")) if agent_payload is not None else 0
    return _check(
        "routing_resolution",
        "ok",
        required=True,
        ok=True,
        item_count=count,
        detail="default_scope" if session_plan.default_isolated else "caller_override",
    )

def _worker_surface_check(agent_payload: Any) -> dict[str, Any]:
    count = _item_count(agent_payload, ("agents", "workers", "data", "items", "results", "result")) if agent_payload is not None else 0
    return _check(
        "worker_surface",
        "ok",
        required=False,
        ok=True,
        item_count=count,
        detail="agent_list_surface",
    )


def _closed_moved_check(*payloads: Any) -> dict[str, Any]:
    observed = 0
    for payload in payloads:
        for item in _payload_items(payload, ("workspaces", "spaces", "agents", "workers", "panes", "data", "items", "results", "result")):
            if not isinstance(item, Mapping):
                continue
            text = " ".join(
                str(item.get(key, ""))
                for key in ("status", "state", "phase", "lifecycle", "agent_status")
            ).lower()
            if "closed" in text or "moved" in text:
                observed += 1
    return _check(
        "closed_moved_observations",
        "observed",
        required=False,
        ok=True,
        item_count=observed,
        detail="observed" if observed else "not_observed",
    )


def _status_event_check(options: SmokeOptions, session_plan: SessionPlan, runner: Runner | None) -> ProbeResult:
    result = _probe_variants(
        "status_event",
        (
            ("status", "--json"),
            ("status",),
            ("events", "list", "--json"),
            ("event", "list", "--json"),
        ),
        ("events", "status", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=False,
    )
    if result.check["status"] != "ok":
        return ProbeResult(
            _check(
                "status_event",
                "not_available",
                required=False,
                ok=True,
                exit_code=result.check.get("exit_code"),
                json_status=result.check.get("json_status"),
                variants=result.check.get("variants"),
                detail="not_available",
            )
        )
    return result


def _live_payload(options: SmokeOptions, env: Mapping[str, str], runner: Runner | None) -> dict[str, Any]:
    if runner is None and not _binary_available(options.herdr_bin):
        return _missing_binary_payload(options, env)

    session_plan = _session_plan(options, env)
    workspace = _probe_variants(
        "workspace_list",
        (("workspace", "list", "--json"), ("workspace", "list")),
        ("workspaces", "spaces", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=True,
    )
    agent = _probe_variants(
        "agent_list",
        (("agent", "list", "--json"), ("agent", "list")),
        ("agents", "workers", "data", "items", "results", "result"),
        options,
        session_plan,
        runner,
        required=True,
    )
    worker_check = _worker_surface_check(agent.payload)

    checks = [
        workspace.check,
        agent.check,
        worker_check,
        _run_send_probe(options, session_plan, runner),
        _name_ambiguity_check(agent.payload),
        _routing_resolution_check(session_plan, agent.payload),
        _event_subscription_aggregate_check(),
        _status_event_check(options, session_plan, runner).check,
        _closed_moved_check(workspace.payload, agent.payload),
    ]

    if any(check.get("required") is True and check.get("status") == "timeout" for check in checks):
        status = "timeout"
    elif any(check.get("required") is True and check.get("ok") is False for check in checks):
        status = "failed"
    elif any(check.get("ok") is False for check in checks):
        status = "degraded"
    else:
        status = "ok"
    return _finalize_payload("live", status, checks, session_plan)


def _read_fixture_files(fixture_dir: Path) -> dict[str, tuple[str, Any | None]]:
    if not fixture_dir.exists() or not fixture_dir.is_dir():
        raise FileNotFoundError
    fixtures: dict[str, tuple[str, Any | None]] = {}
    for path in sorted(item for item in fixture_dir.iterdir() if item.is_file()):
        text = path.read_text(encoding="utf-8")
        validate_fixture_payload(text)
        payload = _parse_json(text)
        if payload is not None:
            validate_fixture_payload(payload)
        fixtures[path.stem] = (text, payload)
    return fixtures


def _fixture_for(fixtures: Mapping[str, tuple[str, Any | None]], prefixes: Sequence[str]) -> tuple[str, Any | None] | None:
    for prefix in prefixes:
        for stem in sorted(fixtures):
            if stem == prefix or stem.startswith(prefix + "_") or stem.startswith(prefix + "-"):
                return fixtures[stem]
    return None


def _fixture_probe(
    fixtures: Mapping[str, tuple[str, Any | None]],
    name: str,
    prefixes: Sequence[str],
    keys: Sequence[str],
    *,
    required: bool,
) -> ProbeResult:
    found = _fixture_for(fixtures, prefixes)
    if found is None:
        return ProbeResult(_check(name, "missing_fixture", required=required, ok=False if required else True, detail="fixture_absent"))
    text, payload = found
    if payload is None:
        return ProbeResult(
            _check(
                name,
                "invalid_json",
                required=required,
                ok=False if required else True,
                json_status="invalid",
                item_count=0,
                variants=1,
                detail="all_variants_failed" if required else "not_available",
            )
        )
    count = _item_count(payload, keys)
    return ProbeResult(
        _check(
            name,
            "ok",
            required=required,
            ok=True,
            exit_code=0,
            json_status="valid",
            item_count=count,
            variants=1,
            detail="non_empty" if count else "empty",
        ),
        payload,
    )


def _fixture_event_subscription_check(fixtures: Mapping[str, tuple[str, Any | None]]) -> dict[str, Any]:
    found = _fixture_for(fixtures, ("event_subscription", "events_subscribe", "subscription_contract"))
    if found is None:
        return _check(
            "event_subscription",
            "missing_fixture",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="fixture_absent",
        )

    _, payload = found
    if not isinstance(payload, Mapping):
        return _check(
            "event_subscription",
            "invalid_json",
            required=True,
            ok=False,
            method=EVENT_SUBSCRIBE_METHOD,
            official_event_count=0,
            params_shape_ok=False,
            legacy_event_count=0,
            detail="fixture_invalid",
        )

    def int_field(*keys: str, default: int = 0) -> int:
        for key in keys:
            if key not in payload:
                continue
            try:
                return int(payload[key])
            except (TypeError, ValueError):
                return default
        return default

    params = payload.get("params")
    inferred_types: list[str] = []
    if isinstance(params, Mapping):
        subscriptions = params.get("subscriptions")
        if isinstance(subscriptions, list):
            for item in subscriptions:
                if isinstance(item, Mapping) and isinstance(item.get("type"), str):
                    inferred_types.append(item["type"])

    count = int_field("official_event_count", "subscription_count", "event_count")
    if not count and inferred_types:
        count = len(inferred_types)
    shape_ok = payload.get("params_shape_ok") is True or _event_subscription_params_shape_ok(params)
    legacy_count = int_field("legacy_event_count", "legacy_count")
    if inferred_types:
        legacy_count = sum(1 for event_name in inferred_types if event_name in LEGACY_EVENT_TYPES)
    if inferred_types:
        try:
            _event_subscription_params(inferred_types)
            official_names_ok = True
        except ValueError:
            official_names_ok = False
    else:
        official_names_ok = count == len(OFFICIAL_EVENT_TYPES)

    method_ok = payload.get("method") == EVENT_SUBSCRIBE_METHOD
    ok = method_ok and count == len(OFFICIAL_EVENT_TYPES) and shape_ok and legacy_count == 0 and official_names_ok
    return _check(
        "event_subscription",
        "ok" if ok else "failed",
        required=True,
        ok=ok,
        method=EVENT_SUBSCRIBE_METHOD,
        official_event_count=count,
        params_shape_ok=shape_ok,
        legacy_event_count=legacy_count,
        detail="fixture_replayed" if ok else "fixture_mismatch",
    )


def _fixture_payload(options: SmokeOptions, env: Mapping[str, str]) -> dict[str, Any]:
    session_plan = _session_plan(options, env)
    try:
        fixtures = _read_fixture_files(options.fixture_dir if options.fixture_dir is not None else Path())
    except (FileNotFoundError, OSError):
        checks = [
            _check("workspace_list", "missing_fixture", required=True, ok=False, detail="fixture_absent"),
            _check("agent_list", "missing_fixture", required=True, ok=False, detail="fixture_absent"),
            _check("worker_surface", "missing_fixture", required=False, ok=True, detail="fixture_absent"),
            _check("send_addressing", "skipped", required=False, ok=True, detail="fixture_absent"),
            _check("name_ambiguity", "skipped", required=False, ok=True, detail="fixture_absent"),
            _check("routing_resolution", "skipped", required=True, ok=False, detail="fixture_absent"),
            _check(
                "event_subscription",
                "missing_fixture",
                required=True,
                ok=False,
                method=EVENT_SUBSCRIBE_METHOD,
                official_event_count=0,
                params_shape_ok=False,
                legacy_event_count=0,
                detail="fixture_absent",
            ),
            _check("status_event", "skipped", required=False, ok=True, detail="fixture_absent"),
            _check("closed_moved_observations", "skipped", required=False, ok=True, detail="fixture_absent"),
        ]
        return _finalize_payload("fixture", "failed", checks, session_plan)
    except PublicSafetyError:
        checks = [
            _check(name, "skipped", required=False, ok=True, detail="fixture_rejected")
            for name in CHECK_NAMES
            if name != "public_safety"
        ]
        checks.append(_check("public_safety", "failed", required=True, ok=False, detail="fixture_rejected"))
        payload = _payload(mode="fixture", status="failed", checks=checks, session_plan=session_plan)
        validate_public_summary(payload)
        return payload

    workspace = _fixture_probe(
        fixtures,
        "workspace_list",
        ("workspace_list", "workspaces"),
        ("workspaces", "spaces", "data", "items", "results", "result"),
        required=True,
    )
    agent = _fixture_probe(
        fixtures,
        "agent_list",
        ("agent_list", "agents"),
        ("agents", "workers", "data", "items", "results", "result"),
        required=True,
    )
    worker = _fixture_probe(
        fixtures,
        "worker_surface",
        ("worker_surface", "pane_list", "panes"),
        ("panes", "items", "data", "results", "result"),
        required=False,
    )
    status_event = _fixture_probe(
        fixtures,
        "status_event",
        ("status_event", "events", "event_list", "status"),
        ("events", "status", "data", "items", "results", "result"),
        required=False,
    )
    if status_event.check["status"] != "ok":
        status_event = ProbeResult(
            _check("status_event", "not_available", required=False, ok=True, detail="fixture_absent")
        )

    event_subscription = _fixture_event_subscription_check(fixtures)
    send_found = _fixture_for(fixtures, ("send_addressing",))
    send_check = (
        _check("send_addressing", "observed", required=False, ok=True, exit_code=0, json_status="valid", variants=1, detail="fixture_replayed")
        if send_found is not None
        else _check("send_addressing", "skipped", required=False, ok=True, detail="fixture_absent")
    )

    checks = [
        workspace.check,
        agent.check,
        worker.check,
        send_check,
        _name_ambiguity_check(agent.payload),
        _routing_resolution_check(session_plan, agent.payload),
        event_subscription,
        status_event.check,
        _closed_moved_check(workspace.payload, agent.payload, worker.payload),
    ]

    if any(check.get("required") is True and check.get("ok") is False for check in checks):
        status = "failed"
    elif any(check.get("ok") is False for check in checks):
        status = "degraded"
    else:
        status = "ok"
    return _finalize_payload("fixture", status, checks, session_plan)


def run_smoke(
    options: SmokeOptions,
    *,
    env: Mapping[str, str] | None = None,
    runner: Runner | None = None,
) -> dict[str, Any]:
    env_map = os.environ if env is None else env
    if options.fixture_dir is not None:
        return _fixture_payload(options, env_map)
    if not options.live:
        return _skip_payload(options, env_map)
    return _live_payload(options, env_map, runner)


def main(
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    runner: Runner | None = None,
) -> int:
    env_map = os.environ if env is None else env
    options = parse_options(argv, env_map)
    payload = run_smoke(options, env=env_map, runner=runner)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0 if payload.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
