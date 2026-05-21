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
