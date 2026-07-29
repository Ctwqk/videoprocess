from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest

from app.services import worker_registration_operator_cli as operator_cli


def _request(token: str = "admission-secret") -> dict[str, object]:
    return {
        "version": 1,
        "service_name": "vp-ffmpeg-worker-go-swarm",
        "generation": 41,
        "worker_type": "ffmpeg_go",
        "worker_host": "colima-127",
        "capabilities": ["media_cpu"],
        "release_commit": "0123456789abcdef0123456789abcdef01234567",
        "image_identity": "vp-ffmpeg-worker-go:deploy-0123456789ab",
        "database_principal": "vp_worker_0123456789abcdef",
        "redis_stream": "vp:tasks:ffmpeg_go",
        "redis_group": "ffmpeg_go-workers",
        "endpoint_bindings": {
            "database": {
                "driver": "postgresql",
                "host": "10.0.0.150",
                "port": 5435,
                "database": "videoprocess",
            },
            "redis": {
                "scheme": "redis",
                "host": "10.0.0.150",
                "port": 6380,
                "database": 0,
            },
            "storage": {
                "backend": "minio",
                "host": "10.0.0.150",
                "port": 9000,
                "bucket": "videoprocess",
            },
        },
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "issued_by": "vp-deploy-controller",
    }


def test_request_file_requires_exact_mode_and_schema(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(_request()))
    request_path.chmod(0o600)

    request = operator_cli.load_upsert_request(request_path)

    assert request.service_name == "vp-ffmpeg-worker-go-swarm"
    assert request.capabilities == ("media_cpu",)
    assert request.token_sha256 == _request()["token_sha256"]

    request_path.chmod(0o644)
    with pytest.raises(operator_cli.OperatorRequestError):
        operator_cli.load_upsert_request(request_path)

    request_path.chmod(0o600)
    changed = _request()
    changed["unknown"] = "field"
    request_path.write_text(json.dumps(changed))
    with pytest.raises(operator_cli.OperatorRequestError):
        operator_cli.load_upsert_request(request_path)


@pytest.mark.parametrize(
    ("service_name", "worker_type", "worker_host", "capabilities", "stream", "group"),
    [
        (
            "vp-ffmpeg-worker-gpu-swarm",
            "ffmpeg",
            "150-gpu",
            ["media_gpu"],
            "vp:tasks:ffmpeg",
            "ffmpeg-workers",
        ),
        (
            "vp-vision-worker-swarm",
            "vision",
            "150-vision",
            ["vision_gpu"],
            "vp:tasks:vision",
            "vision-workers",
        ),
        (
            "vp-youtube-publisher-swarm",
            "youtube_publisher",
            "150-publisher",
            ["youtube_publisher"],
            "vp:tasks:youtube_publisher",
            "youtube_publisher-workers",
        ),
    ],
)
def test_request_file_preserves_stable_manager_consumer_identities(
    tmp_path: Path,
    service_name: str,
    worker_type: str,
    worker_host: str,
    capabilities: list[str],
    stream: str,
    group: str,
) -> None:
    request = _request()
    request.update(
        service_name=service_name,
        worker_type=worker_type,
        worker_host=worker_host,
        capabilities=capabilities,
        redis_stream=stream,
        redis_group=group,
    )
    request["image_identity"] = "vp-ffmpeg-worker-python:deploy-0123456789ab"
    request_path = tmp_path / f"{service_name}.json"
    request_path.write_text(json.dumps(request))
    request_path.chmod(0o600)

    claims = operator_cli.load_upsert_request(request_path)

    assert claims.worker_host == worker_host


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capabilities", ["media_cpu", "media_cpu"]),
        ("capabilities", ["media_gpu", "media_cpu"]),
        ("generation", True),
        ("release_commit", "0123456789ab"),
        ("image_identity", "vp-worker:latest"),
        ("token_sha256", "secret"),
        ("worker_host", "colima-swarmbridged"),
    ],
)
def test_request_file_rejects_noncanonical_claims(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    request = _request()
    request[field] = value
    request_path = tmp_path / f"{field}.json"
    request_path.write_text(json.dumps(request))
    request_path.chmod(0o600)

    with pytest.raises(operator_cli.OperatorRequestError):
        operator_cli.load_upsert_request(request_path)


def test_request_file_rejects_physical_node_as_manager_consumer_identity(
    tmp_path: Path,
) -> None:
    request = _request()
    request.update(
        service_name="vp-vision-worker-swarm",
        worker_type="vision",
        worker_host="ccttww-lap",
        capabilities=["vision_gpu"],
        image_identity="vp-ffmpeg-worker-python:deploy-0123456789ab",
        redis_stream="vp:tasks:vision",
        redis_group="vision-workers",
    )
    request_path = tmp_path / "physical-host.json"
    request_path.write_text(json.dumps(request))
    request_path.chmod(0o600)

    with pytest.raises(operator_cli.OperatorRequestError):
        operator_cli.load_upsert_request(request_path)


async def test_upsert_uses_only_schema_qualified_function_and_redacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "operator-url"
    owner.write_text(
        "postgresql://operator:database-secret@"
        "127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(_request()))
    request_path.chmod(0o600)
    calls: list[tuple[str, tuple[object, ...]]] = []

    class FakeConnection:
        async def fetchval(
            self,
            query: str,
            *arguments: object,
        ) -> object:
            calls.append((query, arguments))
            return "d859cfc4-2f64-467c-81e0-a24aa5da03cf"

        async def close(self) -> None:
            return None

    async def fake_connect(url: str) -> FakeConnection:
        assert "database-secret" in url
        return FakeConnection()

    monkeypatch.setattr(operator_cli.asyncpg, "connect", fake_connect)
    monkeypatch.setenv(operator_cli.DATABASE_URL_FILE_ENV, str(owner))

    result = await operator_cli.run(
        ["upsert", "--request-file", str(request_path)]
    )

    assert result == 0
    assert len(calls) == 1
    sql, arguments = calls[0]
    assert "public.vp_worker_grant_upsert" in sql
    assert not any(
        token in sql.lower()
        for token in ("insert ", "update ", "delete ", "worker_admission_grants")
    )
    assert _request()["token_sha256"] in arguments
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert "admission-secret" not in output
    assert str(_request()["token_sha256"]) not in output
    assert json.loads(output) == {
        "code": "worker_grant_upserted",
        "grant_id": "d859cfc4-2f64-467c-81e0-a24aa5da03cf",
        "status": "ok",
    }


@pytest.mark.parametrize(
    ("arguments", "expected_query", "expected_arguments", "success_code"),
    [
        (
            [
                "activate",
                "--service-name",
                "vp-ffmpeg-worker-go-swarm",
                "--generation",
                "41",
            ],
            "SELECT public.vp_worker_grant_activate($1, $2)",
            ("vp-ffmpeg-worker-go-swarm", 41),
            "worker_grant_activated",
        ),
        (
            [
                "revoke-grant",
                "--service-name",
                "vp-ffmpeg-worker-go-swarm",
                "--generation",
                "41",
                "--reason",
                "operator retirement",
            ],
            "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
            (
                "vp-ffmpeg-worker-go-swarm",
                41,
                "operator retirement",
            ),
            "worker_grant_revoked",
        ),
        (
            [
                "revoke-registration",
                "--service-name",
                "vp-ffmpeg-worker-go-swarm",
                "--registration-id",
                "d859cfc4-2f64-467c-81e0-a24aa5da03cf",
                "--reason",
                "lease retirement",
            ],
            (
                "SELECT public.vp_worker_registration_revoke("
                "$1, $2::uuid, $3)"
            ),
            (
                "vp-ffmpeg-worker-go-swarm",
                uuid.UUID("d859cfc4-2f64-467c-81e0-a24aa5da03cf"),
                "lease retirement",
            ),
            "worker_registration_revoked",
        ),
        (
            [
                "expire-registration",
                "--service-name",
                "vp-ffmpeg-worker-go-swarm",
                "--registration-id",
                "d859cfc4-2f64-467c-81e0-a24aa5da03cf",
            ],
            (
                "SELECT public.vp_worker_registration_expire("
                "$1, $2::uuid)"
            ),
            (
                "vp-ffmpeg-worker-go-swarm",
                uuid.UUID("d859cfc4-2f64-467c-81e0-a24aa5da03cf"),
            ),
            "worker_registration_expired",
        ),
    ],
)
async def test_operator_mutations_use_one_exact_function_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
    expected_query: str,
    expected_arguments: tuple[object, ...],
    success_code: str,
) -> None:
    url_file = tmp_path / "operator-url"
    url_file.write_text(
        "postgresql://operator:database-secret@"
        "127.0.0.1:5432/videoprocess\n"
    )
    url_file.chmod(0o400)
    calls: list[tuple[str, tuple[object, ...]]] = []

    class FakeConnection:
        async def fetchval(
            self,
            query: str,
            *query_arguments: object,
        ) -> object:
            calls.append((query, query_arguments))
            return "d859cfc4-2f64-467c-81e0-a24aa5da03cf"

        async def close(self) -> None:
            return None

    async def fake_connect(url: str) -> FakeConnection:
        assert "database-secret" in url
        return FakeConnection()

    monkeypatch.setattr(operator_cli.asyncpg, "connect", fake_connect)
    monkeypatch.setenv(
        operator_cli.DATABASE_URL_FILE_ENV,
        str(url_file),
    )

    assert await operator_cli.run(arguments) == 0

    assert calls == [(expected_query, expected_arguments)]
    assert not any(
        token in expected_query.lower()
        for token in (
            "insert ",
            "update ",
            "delete ",
            "worker_admission_grants",
            "worker_registrations",
        )
    )
    output = capsys.readouterr().out
    assert "database-secret" not in output
    payload = json.loads(output)
    assert payload["code"] == success_code
    if success_code == "worker_grant_activated":
        assert payload["grant_id"] == (
            "d859cfc4-2f64-467c-81e0-a24aa5da03cf"
        )
    else:
        assert set(payload) == {"code", "status"}


async def test_operator_failure_is_stable_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "operator-url"
    url_file.write_text(
        "postgresql://operator:database-secret@127.0.0.1/videoprocess\n"
    )
    url_file.chmod(0o400)

    async def failing_connect(url: str) -> object:
        raise OSError(f"could not connect with {url}")

    monkeypatch.setattr(operator_cli.asyncpg, "connect", failing_connect)
    monkeypatch.setenv(operator_cli.DATABASE_URL_FILE_ENV, str(url_file))

    result = await operator_cli.run(
        [
            "activate",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "41",
        ]
    )

    assert result == 4
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert json.loads(output) == {
        "code": "worker_operator_operation_failed",
        "status": "error",
    }
