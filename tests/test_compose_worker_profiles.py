from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml


COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"
BACKEND_ROOT = COMPOSE_FILE.parent / "backend"
COMPOSE_DEFAULT = re.compile(r"^\$\{[A-Z0-9_]+:-(.*)\}$")


def _resolved_default(value: str) -> str:
    match = COMPOSE_DEFAULT.fullmatch(value)
    return match.group(1) if match else value


def test_python_ffmpeg_worker_is_local_profile_only() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    service = compose["services"]["ffmpeg-worker"]

    assert "local-python-worker" in service.get("profiles", [])
    assert service["environment"]["DEPLOY_MODE"] == "${DEPLOY_MODE:-local}"


def test_python_worker_defaults_pass_worker_admission() -> None:
    compose = yaml.safe_load(COMPOSE_FILE.read_text())
    sys.path.insert(0, str(BACKEND_ROOT))
    try:
        from app.services.worker_admission import validate_worker_admission
    finally:
        sys.path.remove(str(BACKEND_ROOT))

    for service_name in ("ffmpeg-worker", "vision-worker"):
        environment = compose["services"][service_name]["environment"]
        worker_env = {
            key: _resolved_default(str(value)) for key, value in environment.items()
        }
        decision = validate_worker_admission(worker_env)

        assert decision.allowed is True, f"{service_name}: {decision.reasons}"
