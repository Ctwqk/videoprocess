from __future__ import annotations

import importlib
import json

import pytest


def _module():
    return importlib.import_module(
        "app.services.worker_marker_control_role_cli"
    )


def test_generation_role_names_are_deterministic_bounded_and_separate() -> None:
    module = _module()

    names = module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control"
    )

    assert names.stable == {
        "readiness": "vp_marker_readiness_runtime",
        "janitor": "vp_marker_janitor_runtime",
        "repair": "vp_marker_repair_runtime",
    }
    assert set(names.versioned) == {"readiness", "janitor", "repair"}
    assert len(set(names.versioned.values())) == 3
    assert all(
        role.startswith(f"vp_marker_{purpose}_")
        and len(role.encode("utf-8")) <= 63
        for purpose, role in names.versioned.items()
    )
    assert names == module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control"
    )
    assert names != module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control-next"
    )


@pytest.mark.parametrize(
    "generation",
    (
        "",
        "-leading",
        "UPPERCASE",
        "contains_underscore",
        "a" * 64,
        "../escape",
    ),
)
def test_cli_rejects_invalid_generation_without_reading_credentials(
    generation: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    module = _module()
    monkeypatch.setenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        str(tmp_path / "does-not-exist"),
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://must-not-be-read:secret@production.invalid/db",
    )

    result = module.main(
        [
            "provision",
            "--generation",
            generation,
            "--state-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "reason_code": "marker_control_invalid_arguments",
        "status": "error",
    }
    assert "secret" not in captured.out


def test_cli_requires_the_owner_url_file_environment_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    module = _module()
    monkeypatch.delenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        raising=False,
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://must-not-be-read:secret@production.invalid/db",
    )

    result = module.main(
        [
            "provision",
            "--generation",
            "release-1",
            "--state-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert result == 3
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "reason_code": "marker_control_owner_url_file_invalid",
        "status": "error",
    }
    assert "secret" not in captured.out


def test_credential_paths_are_generation_scoped_and_independent(
    tmp_path,
) -> None:
    module = _module()

    paths = module.credential_paths(tmp_path, "release-1")

    assert paths == {
        "readiness": (
            tmp_path
            / "release-1"
            / "worker-marker-readiness-database-url"
        ),
        "janitor": (
            tmp_path / "release-1" / "worker-marker-janitor-database-url"
        ),
        "repair": (
            tmp_path / "release-1" / "worker-marker-repair-database-url"
        ),
    }
    assert len(set(paths.values())) == 3
