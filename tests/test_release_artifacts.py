from __future__ import annotations

import importlib.util
import tomllib
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "release_artifacts", ROOT / "scripts/release_artifacts.py"
)
assert SPEC is not None and SPEC.loader is not None
release_artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_artifacts)


def test_rc_metadata_has_one_version_source_and_python_313_support() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    assert project["dynamic"] == ["version"]
    assert "version" not in project
    assert data["tool"]["hatch"]["version"]["path"] == "src/tendwire/_version.py"
    assert release_artifacts._package_version() == "0.1.0rc3"
    assert project["requires-python"] == ">=3.13"
    assert "Programming Language :: Python :: 3.13" in project["classifiers"]
    assert not any("Python :: 3.1" in item and not item.endswith("3.13") for item in project["classifiers"])


def test_ci_uses_one_cancellable_bounded_python_job() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "python-version: \"3.13\"" in workflow
    assert "matrix:" not in workflow
    assert "cancel-in-progress: true" in workflow
    assert "timeout-minutes: 30" in workflow
    assert workflow.count("runs-on:") == 1
    assert workflow.count("repository: luminexord/herdres") == 1
    assert "TENDWIRE_BENCHMARK_HERDRES_ROOT:" in workflow
    assert "permissions:\n  contents: read" in workflow
    assert "python -m pytest -q" in workflow
    assert "scripts/release_artifacts.py artifacts dist" in workflow


def test_secret_scanner_detects_runtime_assembled_provider_shapes() -> None:
    token = ("123456789:" + "A" * 35).encode()
    with pytest.raises(release_artifacts.ReleaseCheckError, match="telegram_bot_token"):
        release_artifacts._scan_secret("fixture", token)
    key = ("sk-" + "x" * 24).encode()
    with pytest.raises(release_artifacts.ReleaseCheckError, match="openai_key"):
        release_artifacts._scan_secret("fixture", key)


def test_sdist_declares_release_and_script_assets() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    includes = set(data["tool"]["hatch"]["build"]["targets"]["sdist"]["include"])
    assert {"/RELEASE.md", "/scripts", "/docs/evidence"} <= includes
    assert release_artifacts.REQUIRED_SDIST <= {
        ".env.example",
        "INSTALL.md",
        "LICENSE",
        "README.md",
        "RELEASE.md",
        "SECURITY.md",
        "pyproject.toml",
        "scripts/herdr_smoke.py",
        "scripts/release_artifacts.py",
    }


def test_clean_install_doctor_accepts_only_documented_health_states() -> None:
    assert release_artifacts.INSTALL_SMOKE_DOCTOR_STATUSES == {
        "ok",
        "degraded",
        "unavailable",
    }
