from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_go_dockerfiles_use_go_mod_toolchain_version():
    go_mod = (ROOT / "go.mod").read_text(encoding="utf-8")
    match = re.search(r"^go\s+(\d+\.\d+)\s*$", go_mod, re.MULTILINE)
    assert match, "go.mod must declare a Go toolchain version"
    go_version = match.group(1)

    dockerfiles = [
        ROOT / "backend" / "Dockerfile.api-go",
        ROOT / "backend" / "Dockerfile.channelops-runner-go",
        ROOT / "backend" / "Dockerfile.ffmpeg-worker-go",
    ]
    for dockerfile in dockerfiles:
        text = dockerfile.read_text(encoding="utf-8")
        assert f"FROM golang:{go_version}-bookworm AS build" in text


def test_channelops_go_runner_exposes_queue_and_metrics_envs():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    docs = (ROOT / "docs" / "channelops-go-live-runner.md").read_text(encoding="utf-8")

    for env_name in [
        "CHANNELOPS_QUEUE_MAX_ATTEMPTS",
        "CHANNELOPS_METRICS_MAX_POLLS",
        "CHANNELOPS_METRICS_POLL_DELAY_MINUTES",
    ]:
        assert env_name in compose
        assert f"`{env_name}`" in docs
