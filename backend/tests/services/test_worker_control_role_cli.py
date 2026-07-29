from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from app.services import worker_control_role_cli as control_cli
from app.services import worker_role_cli_common as role_common


def test_control_roles_are_independent_and_generation_scoped() -> None:
    names = control_cli.role_names_for_generation("release-41")
    replacement = control_cli.role_names_for_generation("release-42")

    assert names.stable == {
        "operator": "vp_worker_operator_runtime",
        "orchestrator": "vp_orchestrator_control_runtime",
        "staging_janitor": "vp_staging_janitor_runtime",
    }
    assert set(names.versioned) == set(names.stable)
    assert len(set(names.versioned.values())) == 3
    assert names.versioned != replacement.versioned
    assert all(len(name.encode()) <= 63 for name in names.versioned.values())


def test_control_function_allowlists_are_exact() -> None:
    assert set(control_cli.ROLE_FUNCTIONS["operator"]) == {
        "vp_worker_grant_upsert("
        "text,bigint,text,text,jsonb,text,text,text,text,text,jsonb,text,text)",
        "vp_worker_grant_activate(text,bigint)",
        "vp_worker_grant_revoke(text,bigint,text)",
        "vp_worker_registration_revoke(text,uuid,text)",
        "vp_worker_registration_expire(text,uuid)",
    }
    assert set(control_cli.ROLE_FUNCTIONS["orchestrator"]) == {
        "vp_observe_worker_lease(uuid,bigint)",
        "vp_observe_worker_task_delivery("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
        "text,text,text,text,uuid)",
        "vp_observe_worker_event_emission("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
        "text,text,text,text)",
        "vp_acknowledge_proven_worker_task_dispatch(uuid)",
        "vp_authorize_cancelled_worker_task_ack(uuid)",
        "vp_require_cancelled_worker_task_ack("
        "uuid,text,text,text,text,uuid)",
        "vp_acknowledge_cancelled_worker_task("
        "uuid,text,text,text,text,uuid)",
        "vp_recover_registered_worker_node(uuid,uuid)",
        "vp_resolve_worker_event_authority_for_job_deletion(uuid)",
    }
    assert set(control_cli.ROLE_FUNCTIONS["staging_janitor"]) == {
        "vp_begin_staging_janitor_run(uuid,text,integer)",
        "vp_finish_staging_janitor_run(uuid,jsonb,boolean)",
    }


def test_orchestrator_column_allowlists_cover_real_receipt_transaction() -> None:
    assert set(control_cli.ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS) == {
        "worker_task_dispatches",
        "worker_task_delivery_attestations",
        "worker_event_emissions",
        "registered_worker_event_receipts",
        "registered_worker_event_deliveries",
    }
    assert control_cli.ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS[
        "worker_task_dispatches"
    ] == (
        "id",
        "origin_receipt_id",
        "dispatch_key",
        "job_id",
        "node_execution_id",
        "redis_stream",
        "consumer_group",
        "payload_sha256",
        "payload_json",
        "delivery_state",
        "delivery_attempted_at",
        "delivery_error",
        "redis_message_id",
        "resolution_state",
        "acknowledged_at",
        "cancelled_at",
        "created_at",
        "delivered_at",
    )
    assert control_cli.ORCHESTRATOR_ENTITY_COLUMNS["runtime_schedules"] == (
        "service_name",
        "state",
        "guarded_job_id",
        "updated_at",
        "updated_by",
    )
    assert set(control_cli.ORCHESTRATOR_ENTITY_COLUMNS) >= {
        "channel_profiles",
        "production_tasks",
        "runtime_schedules",
        "jobs",
        "node_executions",
        "artifacts",
        "intermediate_artifact_cache",
    }
    assert control_cli.ORCHESTRATOR_INSERT_COLUMNS["runtime_schedules"] == (
        "service_name",
        "state",
        "updated_by",
    )
    assert {
        "applied_at",
        "acknowledged_at",
        "source_task_acknowledged_at",
    } <= set(
        control_cli.ORCHESTRATOR_INSERT_COLUMNS[
            "registered_worker_event_receipts"
        ]
    )
    assert "acknowledged_at" in control_cli.ORCHESTRATOR_INSERT_COLUMNS[
        "registered_worker_event_deliveries"
    ]
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS["artifacts"] == ("kind",)
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS[
        "worker_event_emissions"
    ] == ("emission_state", "resolved_at")
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS[
        "worker_task_delivery_attestations"
    ] == ("ack_state", "acknowledged_at")
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS["channel_profiles"] == (
        "updated_at",
    )
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS["production_tasks"] == (
        "updated_at",
    )
    assert control_cli.ORCHESTRATOR_UPDATE_COLUMNS["runtime_schedules"] == (
        "updated_at",
    )


async def test_control_provision_writes_independent_mode_0400_urls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner"
    owner.write_text(
        "postgresql://owner:secret@127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)
    state_dir = tmp_path / "state"

    async def fake_provision(
        owner_url: str,
        generation: str,
        target: Path,
        names: control_cli.ControlRoleNames,
    ) -> None:
        assert owner_url.endswith("/videoprocess")
        control_cli.write_generation_credentials(
            target,
            generation,
            {
                purpose: (
                    f"postgresql://{name}:{purpose}-secret@"
                    "127.0.0.1:5432/videoprocess"
                )
                for purpose, name in names.versioned.items()
            },
        )

    monkeypatch.setattr(control_cli, "_provision", fake_provision)
    monkeypatch.setenv(control_cli.OWNER_URL_FILE_ENV, str(owner))

    result = await control_cli.run(
        [
            "provision",
            "--generation",
            "release-41",
            "--state-dir",
            str(state_dir),
        ]
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "-secret" not in output
    assert json.loads(output)["code"] == "worker_control_roles_provisioned"
    paths = control_cli.credential_paths(state_dir, "release-41")
    assert set(paths) == {
        "operator",
        "orchestrator",
        "staging_janitor",
    }
    assert len({path.read_text() for path in paths.values()}) == 3
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o400
        for path in paths.values()
    )


async def test_control_cli_sanitizes_shared_role_helper_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner"
    owner.write_text(
        "postgresql://owner:database-secret@"
        "127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)

    async def failing_provision(*arguments: object) -> None:
        raise role_common.WorkerRoleCommonError(
            "internal database-secret detail"
        )

    monkeypatch.setattr(control_cli, "_provision", failing_provision)
    monkeypatch.setenv(control_cli.OWNER_URL_FILE_ENV, str(owner))

    result = await control_cli.run(
        [
            "provision",
            "--generation",
            "release-41",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert result == 4
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert json.loads(output) == {
        "code": "worker_control_role_operation_failed",
        "status": "error",
    }


@pytest.mark.parametrize(
    "generation",
    ["", "../release", "Release 1", "a" * 64],
)
def test_control_generation_rejects_paths_and_unsafe_names(
    generation: str,
) -> None:
    with pytest.raises(control_cli.ControlRoleArgumentError):
        control_cli.role_names_for_generation(generation)
