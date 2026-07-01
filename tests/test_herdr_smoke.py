import importlib.util
import json
import types
from pathlib import Path


import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMOKE_SCRIPT = PROJECT_ROOT / "scripts" / "herdr_smoke.py"
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures" / "herdr" / "live_smoke"
OK_FIXTURES = FIXTURE_ROOT / "ok"
NEGATIVE_FIXTURES = FIXTURE_ROOT / "negative_private"

REQUIRED_CHECKS = {
    "create_attach",
    "observe",
    "send_addressing",
    "target_validation",
    "event_subscription",
    "status_agent_status_changed",
    "pane_moved_binding_update",
    "close_exited",
    "degraded_backend_preserves_workers",
    "public_safety",
}

# The public schema deliberately contains scenario names such as
# target_validation and pane_moved_binding_update. The banned list therefore
# names concrete private surfaces rather than the neutral words "target" or
# "binding" by themselves.
FORBIDDEN_PUBLIC_TERMS = (
    "telegram",
    "herdres",
    "raw pane",
    "pane_id",
    "pane-id",
    "terminal_id",
    "terminal",
    "socket",
    "backend_target",
    "target_kind",
    "target_value",
    "private_binding",
    "private_fingerprint",
    "connector",
    "outbox",
    "delivery",
    "argv",
    "env",
    "stdout",
    "stderr",
    "token",
    "secret",
    "fingerprint",
)

PRIVATE_MARKERS = (
    "explicit-smoke",
    "caller-smoke",
    "tendwire-smoke",
    "do-not-leak-token",
    "do-not-leak-secret",
    "actual-private-session",
    "socket:///tmp/forbidden.sock",
    "private fingerprint abc123",
    "pane-secret",
    "agent-secret",
)


def _load_smoke_module():
    spec = importlib.util.spec_from_file_location("tendwire_herdr_smoke_under_test", SMOKE_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def smoke_module():
    return _load_smoke_module()


def _patch_which(monkeypatch, module, result):
    if not hasattr(module, "shutil"):
        monkeypatch.setattr(module, "shutil", types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(module.shutil, "which", lambda _name: result, raising=False)


def _run_main(module, argv, capsys, *, env=None, runner=None):
    try:
        return_code = module.main(argv, env={} if env is None else env, runner=runner)
    except SystemExit as exc:
        return_code = exc.code

    captured = capsys.readouterr()
    assert captured.err == ""
    public_text = captured.out.strip()
    assert public_text, "smoke harness must print one JSON summary"
    data = json.loads(public_text)
    assert isinstance(data, dict)
    module.validate_public_summary(data)
    return return_code, public_text, data


def _all_strings(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from _all_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _all_strings(child)
    elif isinstance(value, (str, int, float, bool)) or value is None:
        yield str(value)


def _summary_text(data):
    return " ".join(_all_strings(data)).lower()


def _assert_public_json_safe(public_text, *extra_absent):
    lowered = public_text.lower()
    for term in FORBIDDEN_PUBLIC_TERMS:
        assert term not in lowered
    for marker in extra_absent:
        assert marker.lower() not in lowered


def _is_skip(data):
    text = _summary_text(data)
    return "skip" in text or "skipped" in text


def _is_failure_or_skip(data):
    text = _summary_text(data)
    return "fail" in text or "failed" in text or "error" in text or _is_skip(data)


def _check_records(data):
    checks = data.get("checks", [])
    if isinstance(checks, dict):
        records = []
        for name, record in checks.items():
            if isinstance(record, dict):
                records.append({"name": name, **record})
            else:
                records.append({"name": name, "status": record})
        return records
    assert isinstance(checks, list), "checks must be a list or object"
    return [record for record in checks if isinstance(record, dict)]


def _check_names(data):
    return {record.get("name") for record in _check_records(data)}


def _check_by_name(data, name):
    for record in _check_records(data):
        if record.get("name") == name:
            return record
    raise AssertionError(f"missing check {name}")


class ExplodingRunner:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise AssertionError("runner must not be called")


class RecordingRunner:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        argv = args[0] if args else kwargs.get("args") or kwargs.get("argv")
        assert isinstance(argv, list), "Herdr commands must be argv lists, not shell strings"
        assert all(isinstance(part, str) for part in argv)
        assert kwargs.get("shell") is not True

        child_env = kwargs.get("env")
        if child_env is None:
            for value in args[1:]:
                if isinstance(value, dict):
                    child_env = value
                    break
        assert isinstance(child_env, dict), "runner must receive an explicit child environment"

        self.calls.append({"argv": list(argv), "env": dict(child_env), "kwargs": dict(kwargs)})
        return types.SimpleNamespace(returncode=0, stdout=self._stdout_for(argv), stderr="")

    def _stdout_for(self, argv):
        joined = " ".join(part.lower() for part in argv)
        if "status" in joined and "server" in joined:
            return "status: running\n"
        if argv[3:5] == ["agent", "start"]:
            return json.dumps(
                {
                    "id": "cli:agent:start",
                    "result": {
                        "type": "agent_started",
                        "agent": {
                            "name": "tendwire-smoke-address-probe",
                            "pane_id": "private-pane-id",
                        },
                    },
                }
            )
        if argv[3:5] == ["pane", "close"]:
            return json.dumps({"id": "cli:pane:close", "result": {"type": "ok"}})
        if "workspace" in joined and "list" in joined:
            return json.dumps({"status": "ok", "items": [{"label": "smoke-space"}], "count": 1})
        if "agent" in joined and "list" in joined:
            return json.dumps({"status": "ok", "items": [{"name": "smoke-worker"}], "count": 1})
        if "status" in joined or "event" in joined:
            return (OK_FIXTURES / "status_agent_status_changed.json").read_text()
        if "send" in joined or "address" in joined:
            return (OK_FIXTURES / "send_addressing.json").read_text()
        return json.dumps({"status": "ok", "items": [{"name": "generic-worker"}], "count": 1})


class StoppedSessionRunner(RecordingRunner):
    def _stdout_for(self, argv):
        joined = " ".join(part.lower() for part in argv)
        if "status" in joined and "server" in joined:
            return "status: not running\nsocket: /private/path\n"
        raise AssertionError("stopped smoke scope must fail before observe/send commands")


class StoppedUnderscoreSessionRunner(RecordingRunner):
    def _stdout_for(self, argv):
        joined = " ".join(part.lower() for part in argv)
        if "status" in joined and "server" in joined:
            return json.dumps({"status": "not_running"})
        raise AssertionError("stopped smoke scope must fail before observe/send commands")


class NonzeroSendRunner(RecordingRunner):
    def __call__(self, *args, **kwargs):
        result = super().__call__(*args, **kwargs)
        argv = self.calls[-1]["argv"]
        if argv[3:5] == ["agent", "send"]:
            result.returncode = 1
            result.stdout = ""
            result.stderr = "private send failure"
        return result


class ZeroAcceptedSendRunner(RecordingRunner):
    def __call__(self, *args, **kwargs):
        result = super().__call__(*args, **kwargs)
        argv = self.calls[-1]["argv"]
        if argv[3:5] == ["agent", "send"]:
            result.returncode = 0
            result.stdout = json.dumps({"status": "ok", "accepted_count": 0})
            result.stderr = ""
        return result


def test_no_live_opt_in_skips_without_subprocess_calls(smoke_module, monkeypatch, capsys):
    runner = ExplodingRunner()
    _patch_which(monkeypatch, smoke_module, "/unused/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, [], capsys, env={}, runner=runner)

    assert return_code in (0, None)
    assert runner.calls == []
    assert _is_skip(data)
    assert data.get("mode") != "live"
    assert REQUIRED_CHECKS <= _check_names(data)
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


@pytest.mark.parametrize(
    ("argv", "env", "expected_session", "expected_default", "expected_explicit", "expect_send"),
    [
        (["--live"], {}, "tendwire-smoke", True, False, True),
        (["--live"], {"HERDR_SESSION": "tendwire-smoke"}, "tendwire-smoke", False, True, True),
        (["--live", "--session", "explicit-smoke"], {}, "explicit-smoke", False, True, False),
        (["--live"], {"HERDR_SESSION": "caller-smoke"}, "caller-smoke", False, True, False),
    ],
)
def test_live_session_selection_and_argv_construction(
    smoke_module,
    monkeypatch,
    capsys,
    argv,
    env,
    expected_session,
    expected_default,
    expected_explicit,
    expect_send,
):
    runner = RecordingRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, argv, capsys, env=env, runner=runner)

    assert return_code in (0, None)
    assert runner.calls, "live opt-in must execute Herdr checks through the injected runner"
    for call in runner.calls:
        assert call["env"].get("HERDR_SESSION") == expected_session
        assert isinstance(call["argv"], list)
        assert call["kwargs"].get("shell") is not True
        assert call["argv"][1:3] == ["--session", expected_session]
    send_calls = [call for call in runner.calls if call["argv"][3:5] == ["agent", "send"]]
    assert bool(send_calls) is expect_send
    start_calls = [call for call in runner.calls if call["argv"][3:5] == ["agent", "start"]]
    close_calls = [call for call in runner.calls if call["argv"][3:5] == ["pane", "close"]]
    assert bool(start_calls) is expect_send
    assert bool(close_calls) is expect_send
    assert data.get("default_isolated_session") is expected_default
    assert data.get("explicit_session") is expected_explicit
    assert REQUIRED_CHECKS <= _check_names(data)
    assert _check_by_name(data, "observe")["observed"] is True
    create = _check_by_name(data, "create_attach")
    if expect_send:
        assert create["status"] == "ok"
        assert create["detail"] == "live_created"
        assert "limitation" not in create
    else:
        assert create["limitation"] == "caller_override"
    assert _check_by_name(data, "pane_moved_binding_update")["limitation"] == "live_skipped_unreliable"
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_environment_variable_opts_into_live_mode(smoke_module, monkeypatch, capsys):
    runner = RecordingRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(
        smoke_module,
        [],
        capsys,
        env={"TENDWIRE_HERDR_LIVE_SMOKE": "1"},
        runner=runner,
    )

    assert return_code in (0, None)
    assert runner.calls
    for call in runner.calls:
        assert call["env"].get("HERDR_SESSION") == "tendwire-smoke"
        assert call["argv"][1:3] == ["--session", "tendwire-smoke"]
    assert data.get("mode") == "live"
    assert _check_by_name(data, "create_attach")["detail"] == "live_created"
    assert _check_by_name(data, "send_addressing")["send_attempts"] == 1
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_live_stopped_selected_scope_fails_before_observe_or_send(smoke_module, monkeypatch, capsys):
    runner = StoppedSessionRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={}, runner=runner)

    assert return_code not in (0, None)
    assert data.get("ok") is False
    assert data.get("status") == "unavailable"
    assert len(runner.calls) == 1
    assert runner.calls[0]["argv"][1:5] == ["--session", "tendwire-smoke", "status", "server"]
    observe = _check_by_name(data, "observe")
    assert observe["ok"] is False
    assert observe["workspace_count"] == 0
    assert observe["worker_count"] == 0
    send = _check_by_name(data, "send_addressing")
    assert send["status"] == "skipped"
    assert send["send_attempts"] == 0
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_live_not_running_machine_status_fails_before_observe_or_send(smoke_module, monkeypatch, capsys):
    runner = StoppedUnderscoreSessionRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={}, runner=runner)

    assert return_code not in (0, None)
    assert data.get("ok") is False
    assert data.get("status") == "unavailable"
    assert len(runner.calls) == 1
    assert runner.calls[0]["argv"][1:5] == ["--session", "tendwire-smoke", "status", "server"]
    observe = _check_by_name(data, "observe")
    assert observe["ok"] is False
    assert observe["status"] == "unavailable"
    send = _check_by_name(data, "send_addressing")
    assert send["status"] == "skipped"
    assert send["send_attempts"] == 0
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_live_nonzero_send_is_not_ok(smoke_module, monkeypatch, capsys):
    runner = NonzeroSendRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={}, runner=runner)

    assert return_code not in (0, None)
    assert data.get("ok") is False
    assert data.get("status") == "failed"
    send = _check_by_name(data, "send_addressing")
    assert send["ok"] is False
    assert send["status"] == "nonzero"
    assert send["exit_code"] == 1
    assert send["accepted_count"] == 0
    send_calls = [call for call in runner.calls if call["argv"][3:5] == ["agent", "send"]]
    close_calls = [call for call in runner.calls if call["argv"][3:5] == ["pane", "close"]]
    assert len(send_calls) == 1
    assert len(close_calls) == 1
    assert send_calls[0]["argv"][1:3] == ["--session", "tendwire-smoke"]
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_live_zero_accepted_send_is_not_ok(smoke_module, monkeypatch, capsys):
    runner = ZeroAcceptedSendRunner()
    _patch_which(monkeypatch, smoke_module, "/fake/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={}, runner=runner)

    assert return_code not in (0, None)
    assert data.get("ok") is False
    assert data.get("status") == "failed"
    send = _check_by_name(data, "send_addressing")
    assert send["ok"] is False
    assert send["status"] == "zero_accepted"
    assert send["exit_code"] == 0
    assert send["json_status"] == "valid"
    assert send["accepted_count"] == 0
    send_calls = [call for call in runner.calls if call["argv"][3:5] == ["agent", "send"]]
    close_calls = [call for call in runner.calls if call["argv"][3:5] == ["pane", "close"]]
    assert len(send_calls) == 1
    assert len(close_calls) == 1
    assert send_calls[0]["argv"][1:3] == ["--session", "tendwire-smoke"]
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_send_ok_envelope_counts_as_one_accepted(smoke_module):
    accepted_count, json_status = smoke_module._send_accepted_count(
        json.dumps({"id": "cli:agent:send", "result": {"type": "ok"}})
    )

    assert accepted_count == 1
    assert json_status == "valid"


def test_fixture_replay_is_deterministic_and_public_safe(smoke_module, monkeypatch, capsys):
    runner = ExplodingRunner()
    _patch_which(monkeypatch, smoke_module, None)

    return_code, public_text, data = _run_main(
        smoke_module,
        ["--fixture-dir", str(OK_FIXTURES)],
        capsys,
        env={},
        runner=runner,
    )

    assert return_code in (0, None)
    assert runner.calls == []
    assert data.get("ok") is True
    assert data.get("mode") == "fixture"
    assert REQUIRED_CHECKS <= _check_names(data)
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)
    assert _check_by_name(data, "create_attach")["created_count"] == 1
    assert _check_by_name(data, "observe")["worker_count"] == 2
    assert _check_by_name(data, "send_addressing")["send_attempts"] == 1
    assert _check_by_name(data, "target_validation")["rejected_send_attempts"] == 0
    assert _check_by_name(data, "status_agent_status_changed")["changed_count"] == 1
    assert _check_by_name(data, "pane_moved_binding_update")["preserved"] is True
    assert _check_by_name(data, "close_exited")["exited_count"] == 1
    assert _check_by_name(data, "degraded_backend_preserves_workers")["preserved"] is True
    event_check = _check_by_name(data, "event_subscription")
    assert event_check.get("method") == "events.subscribe"
    assert event_check.get("official_event_count") == len(smoke_module.OFFICIAL_EVENT_TYPES)
    assert event_check.get("params_shape_ok") is True
    assert event_check.get("legacy_event_count") == 0
    assert "subscriptions" not in event_check
    assert "subscriptions" not in public_text
    for raw_name in (
        "workspace.created",
        "pane.created",
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
    ):
        assert raw_name not in public_text


def test_deterministic_target_validation_sends_only_valid_case(smoke_module):
    calls = []

    check = smoke_module._deterministic_target_validation_check(calls.append)

    assert check["ok"] is True
    assert check["valid_cases"] == 1
    assert check["invalid_cases"] == 2
    assert check["ambiguous_cases"] == 1
    assert check["send_attempts"] == 1
    assert check["rejected_send_attempts"] == 0
    assert calls == ["valid"]


def test_deterministic_event_backend_covers_move_close_exited_and_degraded(smoke_module):
    checks = smoke_module._deterministic_event_backend_checks()

    assert checks["status_agent_status_changed"]["changed_count"] == 1
    assert checks["pane_moved_binding_update"]["preserved"] is True
    assert checks["pane_moved_binding_update"]["worker_count_before"] == checks["pane_moved_binding_update"]["worker_count_after"]
    assert checks["close_exited"]["closed_count"] == 1
    assert checks["close_exited"]["exited_count"] == 1
    assert checks["degraded_backend_preserves_workers"]["preserved"] is True
    assert checks["degraded_backend_preserves_workers"]["worker_count_before"] == checks["degraded_backend_preserves_workers"]["worker_count_after"]


def test_event_subscription_builder_rejects_unknown_and_legacy_names(smoke_module):
    params = smoke_module._event_subscription_params()
    assert list(params) == ["subscriptions"]
    assert len(params["subscriptions"]) == len(smoke_module.OFFICIAL_EVENT_TYPES)
    assert smoke_module._event_subscription_params_shape_ok(params) is True

    for bad_name in (
        "",
        " workspace.created ",
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "worktree.updated",
        123,
    ):
        names = list(smoke_module.OFFICIAL_EVENT_TYPES)
        names[0] = bad_name
        with pytest.raises(ValueError):
            smoke_module._event_subscription_params(names)


def test_negative_fixture_rejects_recursive_forbidden_data(smoke_module, monkeypatch, capsys):
    runner = ExplodingRunner()
    _patch_which(monkeypatch, smoke_module, None)

    return_code, public_text, data = _run_main(
        smoke_module,
        ["--fixture-dir", str(NEGATIVE_FIXTURES)],
        capsys,
        env={},
        runner=runner,
    )

    assert return_code not in (0, None) or data.get("ok") is False
    assert runner.calls == []
    assert _is_failure_or_skip(data)
    assert any(word in _summary_text(data) for word in ("forbidden", "unsafe", "rejected"))
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


def test_public_safety_rejects_forbidden_keys_values_and_allows_neutral_record_names(smoke_module):
    for value in (
        {"pane_id": "hidden"},
        {"nested": [{"backend_target": "hidden"}]},
        {"detail": "socket:///tmp/forbidden.sock"},
        {"detail": "actual target value"},
        {"detail": "private fingerprint abc123"},
        {"stdout": "hidden"},
    ):
        with pytest.raises(smoke_module.PublicSafetyError):
            smoke_module.validate_public_summary(value)

    smoke_module.validate_public_summary(
        {
            "checks": [
                {"name": "target_validation", "status": "ok", "required": True, "ok": True},
                {"name": "pane_moved_binding_update", "status": "ok", "required": True, "ok": True},
            ]
        }
    )


def test_missing_herdr_binary_reports_clear_skip_without_runner(smoke_module, monkeypatch, capsys):
    _patch_which(monkeypatch, smoke_module, None)

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={})

    assert return_code not in (0, None) or data.get("ok") is False
    assert _is_failure_or_skip(data)
    assert any(word in _summary_text(data) for word in ("missing", "not found", "unavailable", "requires"))
    assert REQUIRED_CHECKS <= _check_names(data)
    assert _check_by_name(data, "observe")["ok"] is False
    assert _check_by_name(data, "target_validation")["ok"] is True
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)
