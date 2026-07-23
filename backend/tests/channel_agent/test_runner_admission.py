from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


RUNNER_PATH = Path(__file__).resolve().parents[2] / "channel_agent_runner.py"
REJECTION_TEXT = "Go ChannelOps runner is the production owner"


def load_runner_module():
    spec = importlib.util.spec_from_file_location("channel_agent_runner_admission", RUNNER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"DEPLOY_MODE": ""},
        {"DEPLOY_MODE": "   "},
        {"DEPLOY_MODE": "shared"},
        {"DEPLOY_MODE": " SHARED "},
        {"DEPLOY_MODE": "production"},
        {"DEPLOY_MODE": " Production "},
    ],
)
def test_python_runner_rejects_missing_blank_and_production_owner_modes(
    env: dict[str, str],
) -> None:
    module = load_runner_module()

    with pytest.raises(RuntimeError, match=REJECTION_TEXT):
        module.assert_python_channelops_runner_admission(env)


@pytest.mark.parametrize("env", [{"DEPLOY_MODE": "local"}, {"DEPLOY_MODE": " TEST "}])
def test_python_runner_keeps_explicit_local_and_test_modes_available(
    env: dict[str, str],
) -> None:
    module = load_runner_module()

    module.assert_python_channelops_runner_admission(env)


def test_rejected_python_runner_does_not_import_runtime_runner() -> None:
    script = f"""
import builtins
import runpy

runner_path = {json.dumps(str(RUNNER_PATH))}
original_import = builtins.__import__

def guarded_import(name, *args, **kwargs):
    if name == "app.channel_agent.runner":
        raise AssertionError("runtime runner import attempted")
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
module = runpy.run_path(runner_path, run_name="channel_agent_runner_admission_sentinel")
try:
    module["assert_python_channelops_runner_admission"]({{"DEPLOY_MODE": "shared"}})
except RuntimeError as error:
    assert {json.dumps(REJECTION_TEXT)} in str(error)
else:
    raise AssertionError("shared mode was admitted")
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("deploy_mode", [None, "", "   ", "shared", "production"])
def test_rejected_main_modes_fail_before_every_app_import(
    deploy_mode: str | None,
) -> None:
    script = f"""
import builtins
import os
import runpy

runner_path = {json.dumps(str(RUNNER_PATH))}
deploy_mode = {deploy_mode!r}
if deploy_mode is None:
    os.environ.pop("DEPLOY_MODE", None)
else:
    os.environ["DEPLOY_MODE"] = deploy_mode
original_import = builtins.__import__

def guarded_import(name, *args, **kwargs):
    if name == "app" or name.startswith("app."):
        raise AssertionError(f"app import attempted: {{name}}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
try:
    runpy.run_path(runner_path, run_name="__main__")
except RuntimeError as error:
    assert {json.dumps(REJECTION_TEXT)} in str(error)
else:
    raise AssertionError("rejected main mode was admitted")
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("deploy_mode", ["local", " TEST "])
def test_explicit_local_and_test_main_reach_runtime_import(deploy_mode: str) -> None:
    script = f"""
import builtins
import os
import runpy

runner_path = {json.dumps(str(RUNNER_PATH))}
os.environ["DEPLOY_MODE"] = {json.dumps(deploy_mode)}
original_import = builtins.__import__

class RuntimeImportReached(Exception):
    pass

def guarded_import(name, *args, **kwargs):
    if name == "app" or name.startswith("app."):
        raise RuntimeImportReached(name)
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
try:
    runpy.run_path(runner_path, run_name="__main__")
except RuntimeImportReached:
    pass
else:
    raise AssertionError("explicit local/test mode did not reach runtime import")
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
