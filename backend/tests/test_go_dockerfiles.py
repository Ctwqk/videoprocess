from __future__ import annotations

import re
import shlex
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_api_image_layout_imports_with_inventory_disabled_and_shared_secret_reader(tmp_path):
    backend = ROOT / "backend"
    image = tmp_path / "image"
    workdir = image / "app"
    workdir.mkdir(parents=True)
    dockerfile = (backend / "Dockerfile.api").read_text(encoding="utf-8")
    assert "WORKDIR /app" in dockerfile
    for line in dockerfile.splitlines():
        if not line.startswith("COPY "):
            continue
        _, *sources, destination = shlex.split(line)
        target = image / destination.lstrip("/") if destination.startswith("/") else workdir / destination
        for source in sources:
            path = backend / source
            if path.is_dir():
                shutil.copytree(path, target, dirs_exist_ok=True)
            else:
                output = target / path.name if target.is_dir() or destination.endswith("/") else target
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, output)
    secret = tmp_path / "observer-url"
    secret.write_text("redis://fixture-reader:fixture-secret@localhost:6379/0\n", encoding="utf-8")
    secret.chmod(0o400)
    script = """
import sys
from pathlib import Path
sys.path[:0] = [sys.argv[1], sys.argv[2]]
from app.main import app
from app.services import owned_seed_inventory as inventory
from worker import secret_config
assert app.title == "VideoProcess API"
assert inventory.settings.owned_seed_inventory_enabled is False
assert inventory.settings.owned_history_redis_url_file is None
assert Path(inventory.__file__).is_relative_to(sys.argv[1])
assert Path(secret_config.__file__).is_relative_to(sys.argv[1])
assert inventory.secret_config is secret_config
inventory.settings.owned_history_redis_url_file = sys.argv[3]
assert inventory._history_redis_url() == "redis://fixture-reader:fixture-secret@localhost:6379/0"
"""
    # -S excludes editable-install hooks; only staged source and dependencies enter.
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-B", "-c", script, str(workdir),
         sysconfig.get_path("purelib"), str(secret)],
        cwd=workdir, env={}, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert {path.name for path in (workdir / "worker").iterdir()} == {"__init__.py", "secret_config.py"}


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


def test_worker_release_identity_does_not_invalidate_dependency_layers():
    go_worker = (ROOT / "backend" / "Dockerfile.ffmpeg-worker-go").read_text(encoding="utf-8")
    build, runtime = re.split(r"(?m)^FROM ", go_worker)[1:]
    assert build.index("RUN go mod download") < build.index("ARG VP_BUILD_COMMIT")
    assert runtime.index("RUN apt-get update") < runtime.index("ARG VP_BUILD_COMMIT")

    python_worker = (ROOT / "backend" / "Dockerfile.worker").read_text(encoding="utf-8")
    identity = python_worker.index("ARG VP_BUILD_COMMIT")
    for dependency in (
        "RUN apt-get update", "RUN pip install", "RUN python /tmp/vp-visual-embedding-model.py",
    ):
        assert python_worker.rindex(dependency) < identity


def test_worker_dependency_cache_preserves_checked_release_identity():
    go_worker = (ROOT / "backend" / "Dockerfile.ffmpeg-worker-go").read_text(encoding="utf-8")
    build, runtime = re.split(r"(?m)^FROM ", go_worker)[1:]
    python_worker = (ROOT / "backend" / "Dockerfile.worker").read_text(encoding="utf-8")
    check = "RUN printf '%s\\n' \"$VP_BUILD_COMMIT\" | grep -Eq '^[0-9a-f]{40}$'"
    for stage in (build, runtime, python_worker):
        assert stage.index("ARG VP_BUILD_COMMIT") < stage.index(check)
    assert build.index(check) < build.index("RUN CGO_ENABLED=0 go build")
    assert "-X main.buildCommit=$VP_BUILD_COMMIT" in build
    for stage in (runtime, python_worker):
        assert stage.index(check) < stage.index("LABEL org.opencontainers.image.revision=$VP_BUILD_COMMIT")
    assert runtime.index(check) < runtime.index("COPY --from=build")
    assert python_worker.index(check) < python_worker.index("/usr/local/share/videoprocess/worker-build-commit")
    assert "chmod 0444 /usr/local/share/videoprocess/worker-build-commit" in python_worker
    assert "USER videoprocess-worker" in python_worker
    assert 'CMD ["/opt/venv/bin/python", "-I", "-m", "worker.main"]' in python_worker


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
