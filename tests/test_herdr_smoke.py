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
    "workspace_list",
    "agent_list",
    "worker_surface",
    "send_addressing",
    "name_ambiguity",
    "routing_resolution",
    "status_event",
    "event_subscription",
    "closed_moved_observations",
    "public_safety",
}

# The public schema deliberately contains boolean fields named
# default_isolated_session and explicit_session; concrete session values are
# asserted separately instead of banning that substring wholesale.
FORBIDDEN_PUBLIC_TERMS = (
    "telegram",
    "herdres",
    "raw pane",
    "terminal",
    "socket",
    "target",
    "private",
    "binding",
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
        if "workspace" in joined and "list" in joined:
            return (OK_FIXTURES / "workspace_list.json").read_text()
        if "agent" in joined and "list" in joined:
            return (OK_FIXTURES / "agent_list.json").read_text()
        if "send" in joined or "address" in joined:
            return (OK_FIXTURES / "send_addressing.json").read_text()
        if "ambig" in joined:
            return (OK_FIXTURES / "name_ambiguity.json").read_text()
        if "route" in joined or "resolve" in joined:
            return (OK_FIXTURES / "routing_resolution.json").read_text()
        if "status" in joined or "event" in joined:
            return (OK_FIXTURES / "status_event.json").read_text()
        if "closed" in joined or "moved" in joined:
            return (OK_FIXTURES / "closed_moved_observations.json").read_text()
        if "surface" in joined or "pane" in joined or "worker" in joined:
            return (OK_FIXTURES / "worker_surface.json").read_text()
        return json.dumps({"status": "ok", "items": [{"name": "generic-worker"}], "count": 1})


def test_no_live_opt_in_skips_without_subprocess_calls(smoke_module, monkeypatch, capsys):
    runner = ExplodingRunner()
    _patch_which(monkeypatch, smoke_module, "/unused/bin/herdr")

    return_code, public_text, data = _run_main(smoke_module, [], capsys, env={}, runner=runner)

    assert return_code in (0, None)
    assert runner.calls == []
    assert _is_skip(data)
    assert data.get("mode") != "live"
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


@pytest.mark.parametrize(
    ("argv", "env", "expected_session", "expected_default", "expected_explicit"),
    [
        (["--live"], {}, "tendwire-smoke", True, False),
        (["--live", "--session", "explicit-smoke"], {}, "explicit-smoke", False, True),
        (["--live"], {"HERDR_SESSION": "caller-smoke"}, "caller-smoke", False, True),
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
    assert not any(call["argv"][1:3] == ["pane", "list"] for call in runner.calls)
    assert data.get("default_isolated_session") is expected_default
    assert data.get("explicit_session") is expected_explicit
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
    assert data.get("mode") == "live"
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)


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


def test_missing_herdr_binary_reports_clear_skip_without_runner(smoke_module, monkeypatch, capsys):
    _patch_which(monkeypatch, smoke_module, None)

    return_code, public_text, data = _run_main(smoke_module, ["--live"], capsys, env={})

    assert return_code not in (0, None) or data.get("ok") is False
    assert _is_failure_or_skip(data)
    assert any(word in _summary_text(data) for word in ("missing", "not found", "unavailable", "requires"))
    _assert_public_json_safe(public_text, *PRIVATE_MARKERS)
