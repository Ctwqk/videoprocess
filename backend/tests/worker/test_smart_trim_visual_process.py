from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker.handlers import smart_trim
from worker.handlers.base import CancelledError
from worker.handlers.smart_trim import SmartTrimConfig, SmartTrimHandler


@pytest.fixture
def local_handler(monkeypatch, tmp_path):
    handler = SmartTrimHandler()
    monkeypatch.setattr(smart_trim, "settings", SimpleNamespace(
        vision_embedding_url="", vision_embedding_model_path="/offline/model",
    ))
    frame = tmp_path / "frame.jpg"
    frame.write_bytes(b"image fixture")
    monkeypatch.setattr(handler, "_extract_frames", AsyncMock(return_value=[(1.0, frame)]))
    return handler, frame


def child_command(handler, monkeypatch, tmp_path, *, output=None, exit_code=0, wait=False):
    pid_path = tmp_path / "child.pid"
    code = (
        "import json, os, pathlib, sys, time; "
        "request=json.load(sys.stdin); "
        "assert request['model_path']=='/offline/model'; "
        "assert request['texts']==['blue']; "
        "assert len(request['image_paths'])==1; "
        f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid())); "
        + ("time.sleep(30); " if wait else "")
        + f"print({json.dumps(output or {'similarities': [[0.8]]})!r}, flush=True); "
        + f"sys.exit({exit_code})"
    )
    monkeypatch.setattr(handler, "_local_scoring_command", lambda: [
        sys.executable, "-c", code,
    ], raising=False)
    return pid_path


def assert_reaped(pid_path: Path):
    pid = int(pid_path.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def wait_started(pid_path: Path):
    async def poll():
        while not pid_path.exists():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), 3.0)


async def test_local_visual_provider_runs_real_child_and_reaps_it(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path)
    windows, warnings = await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert warnings == []
    assert len(windows) == 1
    assert windows[0].visual_score == pytest.approx(0.8)
    assert (windows[0].start, windows[0].end) == (0.5, 1.5)
    assert_reaped(pid_path)
    assert handler._proc is None


async def test_remote_provider_has_priority_over_local_model(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    smart_trim.settings.vision_embedding_url = "http://remote.test"
    monkeypatch.setattr(handler, "_score_frames", AsyncMock(return_value=[(1.0, 0.7)]))
    pid_path = child_command(handler, monkeypatch, tmp_path)
    windows, warnings = await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert warnings == []
    assert windows[0].visual_score == 0.7
    assert not pid_path.exists()


async def test_remote_failure_does_not_switch_to_local_model(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    smart_trim.settings.vision_embedding_url = "http://remote.test"
    monkeypatch.setattr(handler, "_score_frames", AsyncMock(side_effect=RuntimeError("provider failed")))
    pid_path = child_command(handler, monkeypatch, tmp_path)
    windows, warnings = await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert windows == []
    assert "provider failed" in warnings[0]
    assert not pid_path.exists()


@pytest.mark.parametrize("exit_code,output,expected", [
    (7, None, "visual scoring"),
    (0, {"similarities": [[True]]}, "similarity matrix"),
    (0, {"similarities": []}, "similarity matrix"),
])
async def test_local_invalid_result_warns_without_windows(
    local_handler, monkeypatch, tmp_path, exit_code, output, expected,
):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path, output=output, exit_code=exit_code)
    windows, warnings = await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert windows == []
    assert expected in warnings[0]
    assert_reaped(pid_path)
    assert handler._proc is None


async def test_local_timeout_kills_and_reaps_child(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    monkeypatch.setattr(smart_trim, "LOCAL_VISUAL_TIMEOUT_SECONDS", 0.3, raising=False)
    pid_path = child_command(handler, monkeypatch, tmp_path, wait=True)
    windows, warnings = await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert windows == []
    assert "timed out" in warnings[0]
    assert_reaped(pid_path)
    assert handler._proc is None


@pytest.mark.parametrize("cancel_kind", ["task", "handler"])
async def test_local_cancellation_propagates_after_child_reaped(
    local_handler, monkeypatch, tmp_path, cancel_kind,
):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path, wait=True)
    task = asyncio.create_task(handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue")))
    try:
        await wait_started(pid_path)
        if cancel_kind == "task":
            task.cancel()
            exception = asyncio.CancelledError
        else:
            handler.cancel()
            exception = CancelledError
        with pytest.raises(exception):
            await asyncio.wait_for(task, 3.0)
        assert_reaped(pid_path)
        assert handler._proc is None
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_handler_never_starts_visual_child(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path)
    handler.cancel()
    with pytest.raises(CancelledError):
        await handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue"))
    assert not pid_path.exists()


async def test_cancellation_during_spawn_still_owns_and_reaps_child(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path, wait=True)
    original_spawn = asyncio.create_subprocess_exec
    created = asyncio.Event()
    release = asyncio.Event()

    async def delayed_spawn(*args, **kwargs):
        proc = await original_spawn(*args, **kwargs)
        created.set()
        await release.wait()
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_spawn)
    task = asyncio.create_task(handler._visual_windows("source.mp4", 5.0, SmartTrimConfig(prompt="blue")))
    try:
        await asyncio.wait_for(created.wait(), 3.0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3.0)
        assert handler._proc is None
        if pid_path.exists():
            assert_reaped(pid_path)
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_low_local_score_fails_without_rendering_placeholder(local_handler, monkeypatch, tmp_path):
    handler, _ = local_handler
    pid_path = child_command(handler, monkeypatch, tmp_path, output={"similarities": [[0.1]]})
    monkeypatch.setattr(handler, "run_ffprobe", AsyncMock(return_value={
        "format": {"duration": "5"}, "streams": [{"codec_type": "video"}],
    }))
    monkeypatch.setattr(handler, "run_ffmpeg", AsyncMock(side_effect=AssertionError("must not render")))
    output = tmp_path / "output.mp4"
    with pytest.raises(RuntimeError, match="no matching video segment"):
        await handler.execute({"prompt": "blue", "use_asr": False}, {"input": "source.mp4"}, str(output))
    assert not output.exists()
    assert_reaped(pid_path)
