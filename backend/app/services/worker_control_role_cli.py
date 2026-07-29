from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]

from app.services.worker_role_cli_common import (
    PinnedPrivateDirectory,
    PrivateDirectorySnapshot,
    WorkerRoleCommonError,
    acquire_database_acl_dcl_lock,
    acquire_role_lifecycle_lock,
    acquire_stable_role_authority_locks,
    asyncpg_url,
    capture_private_directory,
    close_pinned_private_directories,
    create_login_role,
    drop_login_roles,
    ensure_stable_role,
    grant_columns,
    grant_functions,
    load_database_url_file,
    pin_private_directory,
    quote_identifier,
    quarantine_login_roles,
    read_secure_file,
    remove_secure_files,
    reset_public_privileges,
    role_database_url,
    verify_role_database_url,
    verify_private_directory,
    verify_pinned_private_directory,
    write_secure_files,
)


OWNER_URL_FILE_ENV = "WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE"
GENERATION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
STABLE_ROLES = {
    "operator": "vp_worker_operator_runtime",
    "orchestrator": "vp_orchestrator_control_runtime",
    "staging_janitor": "vp_staging_janitor_runtime",
}
CREDENTIAL_FILENAMES = {
    "operator": "worker-registration-operator-database-url",
    "orchestrator": "worker-orchestrator-database-url",
    "staging_janitor": "vp-staging-janitor-database-url",
}
ROLE_FUNCTIONS = {
    "operator": (
        "vp_worker_grant_upsert("
        "text,bigint,text,text,jsonb,text,text,text,text,text,jsonb,text,text)",
        "vp_worker_grant_activate(text,bigint)",
        "vp_worker_grant_revoke(text,bigint,text)",
        "vp_worker_registration_revoke(text,uuid,text)",
        "vp_worker_registration_expire(text,uuid)",
    ),
    "orchestrator": (
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
    ),
    "staging_janitor": (
        "vp_begin_staging_janitor_run(uuid,text,integer)",
        "vp_finish_staging_janitor_run(uuid,jsonb,boolean)",
    ),
}
ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS: Mapping[
    str,
    tuple[str, ...],
] = {
    "worker_task_dispatches": (
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
    ),
    "worker_task_delivery_attestations": (
        "id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "dispatch_key",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "ack_state",
        "acknowledged_at",
        "ack_event_emission_id",
        "attested_at",
    ),
    "worker_event_emissions": (
        "id",
        "source_task_attestation_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "payload_json",
        "event_type",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "emission_state",
        "prepared_at",
        "emitted_at",
        "resolved_at",
    ),
    "registered_worker_event_receipts": (
        "id",
        "source_task_attestation_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "payload_json",
        "event_type",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "source_task_stream",
        "source_task_group",
        "source_task_message_id",
        "application_state",
        "ack_state",
        "source_task_ack_state",
        "accepted_at",
        "applied_at",
        "acknowledged_at",
        "source_task_acknowledged_at",
    ),
    "registered_worker_event_deliveries": (
        "id",
        "source_task_attestation_id",
        "receipt_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "resolution_state",
        "reason_code",
        "ack_state",
        "accepted_at",
        "acknowledged_at",
    ),
}
ORCHESTRATOR_INSERT_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "runtime_schedules": (
        "service_name",
        "state",
        "updated_by",
    ),
    "worker_task_dispatches": (
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
    ),
    "registered_worker_event_receipts": (
        "id",
        "source_task_attestation_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "payload_json",
        "event_type",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "source_task_stream",
        "source_task_group",
        "source_task_message_id",
        "application_state",
        "ack_state",
        "source_task_ack_state",
        "applied_at",
        "acknowledged_at",
        "source_task_acknowledged_at",
    ),
    "registered_worker_event_deliveries": (
        "id",
        "source_task_attestation_id",
        "receipt_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "resolution_state",
        "reason_code",
        "ack_state",
        "acknowledged_at",
    ),
    "artifacts": (
        "id",
        "job_id",
        "node_execution_id",
        "kind",
        "filename",
        "mime_type",
        "file_size",
        "storage_backend",
        "storage_path",
        "media_info",
    ),
    "intermediate_artifact_cache": (
        "id",
        "cache_key",
        "node_type",
        "node_config_hash",
        "input_signature_hash",
        "output_artifact_id",
        "storage_backend",
        "storage_path",
        "filename",
        "mime_type",
        "file_size",
        "media_info",
        "hit_count",
        "metadata_json",
    ),
}
ORCHESTRATOR_UPDATE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "channel_profiles": ("updated_at",),
    "production_tasks": ("updated_at",),
    "runtime_schedules": ("updated_at",),
    "worker_task_dispatches": (
        "delivery_state",
        "delivery_attempted_at",
        "delivery_error",
        "redis_message_id",
        "delivered_at",
    ),
    "worker_task_delivery_attestations": (
        "ack_state",
        "acknowledged_at",
    ),
    "registered_worker_event_receipts": (
        "application_state",
        "applied_at",
        "ack_state",
        "acknowledged_at",
        "source_task_ack_state",
        "source_task_acknowledged_at",
    ),
    "registered_worker_event_deliveries": (
        "ack_state",
        "acknowledged_at",
    ),
    "worker_event_emissions": (
        "emission_state",
        "resolved_at",
    ),
    "artifacts": ("kind",),
    "jobs": (
        "status",
        "completed_at",
        "error_message",
    ),
    "node_executions": (
        "status",
        "progress",
        "queued_at",
        "completed_at",
        "error_message",
        "error_trace",
        "retry_count",
        "input_artifact_ids",
        "output_artifact_id",
    ),
    "intermediate_artifact_cache": (
        "node_type",
        "node_config_hash",
        "input_signature_hash",
        "output_artifact_id",
        "storage_backend",
        "storage_path",
        "filename",
        "mime_type",
        "file_size",
        "media_info",
        "last_used_at",
        "hit_count",
        "metadata_json",
    ),
}
ORCHESTRATOR_ENTITY_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "channel_profiles": (
        "operator_id",
        "name",
        "positioning",
        "language",
        "default_aspect_ratio",
        "risk_policy_json",
        "content_mix_policy_json",
        "cadence_policy_json",
        "alert_policy_json",
        "enabled",
        "dry_run",
        "halted_at",
        "halt_reason",
        "intake_paused_at",
        "intake_pause_reason",
        "config_version",
        "tick_interval_minutes",
        "id",
        "created_at",
        "updated_at",
    ),
    "production_tasks": (
        "task_group_id",
        "channel_profile_id",
        "topic_lane_id",
        "lane_format_id",
        "target_account_id",
        "manual_seed_id",
        "discovery_signal_id",
        "source",
        "title_seed",
        "prompt",
        "rationale_json",
        "score_breakdown_json",
        "portfolio_bucket",
        "source_platforms_json",
        "material_library_ids_json",
        "uses_external_assets",
        "approval_mode",
        "agent_approval_evidence_json",
        "human_review_evidence_json",
        "autoflow_plan_id",
        "autoflow_run_id",
        "pipeline_id",
        "job_id",
        "scheduled_at",
        "priority",
        "state",
        "state_updated_at",
        "failure_reason",
        "failure_category",
        "retry_count",
        "blocked_by_guard",
        "channel_config_version_snapshot",
        "channel_config_snapshot_json",
        "transition_history_json",
        "id",
        "created_at",
        "updated_at",
    ),
    "runtime_schedules": (
        "service_name",
        "state",
        "guarded_job_id",
        "updated_at",
        "updated_by",
    ),
    "jobs": (
        "id",
        "pipeline_id",
        "pipeline_snapshot",
        "status",
        "execution_plan",
        "submitted_at",
        "started_at",
        "completed_at",
        "error_message",
        "submitted_by",
        "parent_job_id",
        "retry_count",
        "orchestrator_owner",
    ),
    "node_executions": (
        "id",
        "job_id",
        "node_id",
        "node_type",
        "node_label",
        "node_config",
        "status",
        "progress",
        "worker_id",
        "queued_at",
        "started_at",
        "completed_at",
        "error_message",
        "error_trace",
        "retry_count",
        "input_artifact_ids",
        "output_artifact_id",
        "worker_registration_id",
        "worker_lease_epoch",
    ),
    "artifacts": (
        "id",
        "job_id",
        "node_execution_id",
        "kind",
        "filename",
        "mime_type",
        "file_size",
        "storage_backend",
        "storage_path",
        "media_info",
        "created_at",
    ),
    "intermediate_artifact_cache": (
        "id",
        "cache_key",
        "node_type",
        "node_config_hash",
        "input_signature_hash",
        "output_artifact_id",
        "storage_backend",
        "storage_path",
        "filename",
        "mime_type",
        "file_size",
        "media_info",
        "created_at",
        "last_used_at",
        "hit_count",
        "metadata_json",
    ),
}


class ControlRoleError(RuntimeError):
    pass


class ControlRoleArgumentError(ControlRoleError):
    pass


class ControlStateTreeError(ControlRoleError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ControlRoleArgumentError(message)


@dataclass(frozen=True)
class ControlRoleNames:
    stable: Mapping[str, str]
    versioned: Mapping[str, str]


class PinnedControlMembers(dict[str, set[str]]):
    def __init__(
        self,
        members: Mapping[str, set[str]],
        pins: Sequence[PinnedPrivateDirectory],
    ) -> None:
        super().__init__(members)
        self.pins = tuple(pins)


def role_names_for_generation(generation: str) -> ControlRoleNames:
    if (
        not isinstance(generation, str)
        or not GENERATION_PATTERN.fullmatch(generation)
    ):
        raise ControlRoleArgumentError("invalid control generation")
    suffix = hashlib.sha256(generation.encode()).hexdigest()[:16]
    versioned = {
        purpose: f"vp_{purpose}_{suffix}"
        for purpose in STABLE_ROLES
    }
    if any(len(name.encode()) > 63 for name in versioned.values()):
        raise ControlRoleArgumentError("invalid control generation")
    return ControlRoleNames(
        stable=dict(STABLE_ROLES),
        versioned=versioned,
    )


def credential_paths(
    state_dir: Path,
    generation: str,
) -> dict[str, Path]:
    role_names_for_generation(generation)
    return {
        purpose: state_dir / generation / filename
        for purpose, filename in CREDENTIAL_FILENAMES.items()
    }


def write_generation_credentials(
    state_dir: Path,
    generation: str,
    role_urls: Mapping[str, str],
) -> None:
    role_names_for_generation(generation)
    if set(role_urls) != set(CREDENTIAL_FILENAMES):
        raise ControlRoleError("control credentials incomplete")
    try:
        write_secure_files(
            state_dir,
            (generation,),
            {
                CREDENTIAL_FILENAMES[purpose]: f"{role_urls[purpose]}\n"
                for purpose in CREDENTIAL_FILENAMES
            },
            file_mode=0o400,
        )
    except WorkerRoleCommonError as exc:
        raise ControlRoleError("control credential write failed") from exc


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        state_dir = Path(args.state_dir)
        if not state_dir.is_absolute():
            raise ControlRoleArgumentError("state dir must be absolute")
        names = role_names_for_generation(args.generation)
    except (argparse.ArgumentError, ControlRoleArgumentError):
        _emit("error", "worker_control_invalid_arguments")
        return 2

    try:
        owner_url = load_database_url_file(OWNER_URL_FILE_ENV)
    except WorkerRoleCommonError:
        _emit("error", "worker_control_owner_url_invalid")
        return 3

    try:
        if args.command == "provision":
            await _provision(
                owner_url,
                args.generation,
                state_dir,
                names,
            )
            code = "worker_control_roles_provisioned"
        else:
            await _revoke(
                owner_url,
                args.generation,
                state_dir,
                names,
            )
            code = "worker_control_roles_revoked"
    except (
        asyncpg.PostgresError,
        OSError,
        ControlRoleError,
        WorkerRoleCommonError,
    ):
        _emit("error", "worker_control_role_operation_failed")
        return 4
    _emit("ok", code, roles=dict(names.versioned))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-control-role",
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("provision", "revoke"):
        command_parser = subparsers.add_parser(
            command,
            exit_on_error=False,
        )
        command_parser.add_argument("--generation", required=True)
        command_parser.add_argument("--state-dir", required=True)
    return parser


async def _provision(
    owner_url: str,
    generation: str,
    state_dir: Path,
    names: ControlRoleNames,
) -> None:
    paths = credential_paths(state_dir, generation)
    connection = await asyncpg.connect(asyncpg_url(owner_url))
    created = False
    fresh = False
    operation_pins: list[PinnedPrivateDirectory] = []
    try:
        await acquire_database_acl_dcl_lock(connection)
        await acquire_stable_role_authority_locks(
            connection,
            tuple(names.stable.values()),
        )
        await acquire_role_lifecycle_lock(
            connection,
            f"control:{generation}",
        )
        try:
            tree_pins = _control_generation_tree_pins(
                state_dir,
                generation,
            )
            operation_pins.extend(tree_pins)
        except ControlStateTreeError as exc:
            await quarantine_login_roles(
                connection,
                tuple(names.versioned.values()),
            )
            raise ControlRoleError(
                "control state directory invalid"
            ) from exc
        state_presence = {
            purpose: path.exists()
            for purpose, path in paths.items()
        }
        existing_role_count = int(
            await connection.fetchval(
                """
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_roles
                WHERE rolname = ANY($1::text[])
                """,
                list(names.versioned.values()),
            )
        )
        fresh = not any(state_presence.values()) and existing_role_count == 0
        if not fresh and (
            not all(state_presence.values())
            or existing_role_count != len(names.versioned)
        ):
            await _quarantine_control_generation(
                connection,
                owner_url,
                state_dir,
                generation,
                names,
            )
            raise ControlRoleError("control generation state incomplete")
        if not fresh:
            try:
                role_urls = await _validate_existing_generation(
                    connection,
                    owner_url,
                    paths,
                    names,
                )
                _verify_pinned_directories(operation_pins)
            except (
                asyncpg.PostgresError,
                OSError,
                ControlRoleError,
                WorkerRoleCommonError,
            ) as exc:
                tree_error: ControlStateTreeError | None = None
                try:
                    _verify_pinned_directories(operation_pins)
                except ControlStateTreeError as detected_tree_error:
                    tree_error = detected_tree_error
                if tree_error is not None or isinstance(
                    exc,
                    ControlStateTreeError,
                ):
                    await quarantine_login_roles(
                        connection,
                        tuple(names.versioned.values()),
                    )
                else:
                    await _quarantine_control_generation(
                        connection,
                        owner_url,
                        state_dir,
                        generation,
                        names,
                    )
                raise ControlRoleError(
                    "control generation state invalid"
                ) from (tree_error or exc)
            try:
                authorized_members = await _authorized_control_members(
                    connection,
                    owner_url,
                    state_dir,
                )
            except ControlStateTreeError as exc:
                await quarantine_login_roles(
                    connection,
                    tuple(names.versioned.values()),
                )
                raise ControlRoleError(
                    "control state directory invalid"
                ) from exc
            member_pins = tuple(getattr(authorized_members, "pins", ()))
            authority_pins = (*operation_pins, *member_pins)
            try:
                _state_tree_test_hook("before_dcl")
                _verify_pinned_directories(authority_pins)
                try:
                    async with connection.transaction():
                        _verify_pinned_directories(authority_pins)
                        for purpose, stable_role in names.stable.items():
                            await ensure_stable_role(
                                connection,
                                stable_role,
                                setting_prefix="worker_control",
                                authorized_members=tuple(
                                    sorted(
                                        {
                                            *authorized_members[purpose],
                                            names.versioned[purpose],
                                        }
                                    )
                                ),
                            )
                        await _set_control_privileges(connection, names)
                        _state_tree_test_hook("after_dcl")
                        _verify_pinned_directories(authority_pins)
                except BaseException as exc:
                    try:
                        _verify_pinned_directories(authority_pins)
                    except ControlStateTreeError as tree_exc:
                        raise tree_exc from exc
                    raise
                _verify_pinned_directories(authority_pins)
                for purpose, database_url in role_urls.items():
                    try:
                        await verify_role_database_url(
                            owner_url,
                            database_url,
                            names.versioned[purpose],
                        )
                    except BaseException as exc:
                        try:
                            _verify_pinned_directories(authority_pins)
                        except ControlStateTreeError as tree_exc:
                            raise tree_exc from exc
                        raise
                    _verify_pinned_directories(authority_pins)
            except ControlStateTreeError as exc:
                await quarantine_login_roles(
                    connection,
                    tuple(names.versioned.values()),
                )
                raise ControlRoleError(
                    "control state directory invalid"
                ) from exc
            finally:
                close_pinned_private_directories(member_pins)
            return

        passwords = {
            purpose: secrets.token_urlsafe(32)
            for purpose in STABLE_ROLES
        }
        if len(set(passwords.values())) != len(passwords):
            raise ControlRoleError("credential generation failed")
        authorized_members = await _authorized_control_members(
            connection,
            owner_url,
            state_dir,
        )
        member_pins = tuple(getattr(authorized_members, "pins", ()))
        authority_pins = (*operation_pins, *member_pins)
        try:
            _state_tree_test_hook("before_dcl")
            _verify_pinned_directories(authority_pins)
            async with connection.transaction():
                _verify_pinned_directories(authority_pins)
                for purpose, stable_role in names.stable.items():
                    await ensure_stable_role(
                        connection,
                        stable_role,
                        setting_prefix="worker_control",
                        authorized_members=tuple(
                            sorted(
                                {
                                    *authorized_members[purpose],
                                    names.versioned[purpose],
                                }
                            )
                        ),
                    )
                await _set_control_privileges(connection, names)
                for purpose, versioned_role in names.versioned.items():
                    await create_login_role(
                        connection,
                        versioned_role,
                        passwords[purpose],
                        setting_prefix="worker_control",
                        stable_role=names.stable[purpose],
                    )
                _state_tree_test_hook("after_dcl")
                _verify_pinned_directories(authority_pins)
            created = True
            role_urls = {
                purpose: role_database_url(
                    owner_url,
                    names.versioned[purpose],
                    passwords[purpose],
                )
                for purpose in STABLE_ROLES
            }
            _verify_pinned_directories(authority_pins)
            for purpose, database_url in role_urls.items():
                await verify_role_database_url(
                    owner_url,
                    database_url,
                    names.versioned[purpose],
                )
                _verify_pinned_directories(authority_pins)
        finally:
            close_pinned_private_directories(member_pins)
        write_generation_credentials(
            state_dir,
            generation,
            role_urls,
        )
    except BaseException:
        if created:
            try:
                await drop_login_roles(
                    connection,
                    tuple(names.versioned.values()),
                )
                remove_secure_files(
                    state_dir,
                    (generation,),
                    tuple(CREDENTIAL_FILENAMES.values()),
                )
            except (asyncpg.PostgresError, OSError, WorkerRoleCommonError):
                pass
        raise
    finally:
        close_pinned_private_directories(operation_pins)
        await connection.close()


async def _quarantine_control_generation(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    generation: str,
    names: ControlRoleNames,
) -> None:
    await quarantine_login_roles(
        connection,
        tuple(names.versioned.values()),
    )
    try:
        authorized_members = await _authorized_control_members(
            connection,
            owner_url,
            state_dir,
            excluded_generation=generation,
        )
    except ControlStateTreeError:
        return
    member_pins = tuple(getattr(authorized_members, "pins", ()))
    try:
        _verify_pinned_directories(member_pins)
        async with connection.transaction():
            _verify_pinned_directories(member_pins)
            for purpose, stable_role in names.stable.items():
                await ensure_stable_role(
                    connection,
                    stable_role,
                    setting_prefix="worker_control",
                    authorized_members=tuple(
                        sorted(authorized_members[purpose])
                    ),
                )
            _verify_pinned_directories(member_pins)
    finally:
        close_pinned_private_directories(member_pins)


async def _set_control_privileges(
    connection: asyncpg.Connection,
    names: ControlRoleNames,
) -> None:
    for purpose, role_name in names.stable.items():
        await reset_public_privileges(connection, role_name)
        await grant_functions(
            connection,
            role_name,
            ROLE_FUNCTIONS[purpose],
        )

    operator = quote_identifier(names.stable["operator"])
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public "
        f"FROM {operator}"
    )

    janitor = names.stable["staging_janitor"]
    await grant_columns(
        connection,
        janitor,
        "SELECT",
        "artifacts",
        ("storage_path",),
    )
    await grant_columns(
        connection,
        janitor,
        "SELECT",
        "intermediate_artifact_cache",
        ("storage_path",),
    )

    orchestrator = names.stable["orchestrator"]
    for (
        table_name,
        columns,
    ) in ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS.items():
        await grant_columns(
            connection,
            orchestrator,
            "SELECT",
            table_name,
            columns,
        )
    for table_name, columns in ORCHESTRATOR_ENTITY_COLUMNS.items():
        await grant_columns(
            connection,
            orchestrator,
            "SELECT",
            table_name,
            columns,
        )
    for table_name, columns in ORCHESTRATOR_INSERT_COLUMNS.items():
        await grant_columns(
            connection,
            orchestrator,
            "INSERT",
            table_name,
            columns,
        )
    for table_name, columns in ORCHESTRATOR_UPDATE_COLUMNS.items():
        await grant_columns(
            connection,
            orchestrator,
            "UPDATE",
            table_name,
            columns,
        )


async def _validate_existing_generation(
    connection: asyncpg.Connection,
    owner_url: str,
    paths: Mapping[str, Path],
    names: ControlRoleNames,
) -> dict[str, str]:
    if not all(path.exists() for path in paths.values()):
        raise ControlRoleError("control credentials incomplete")
    role_urls: dict[str, str] = {}
    for purpose, path in paths.items():
        try:
            database_url = read_secure_file(
                path,
                required_mode=0o400,
            ).strip()
        except (WorkerRoleCommonError, ValueError, TypeError) as exc:
            raise ControlRoleError(
                "control credentials invalid"
            ) from exc
        if (
            not await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                """,
                names.versioned[purpose],
            )
        ):
            raise ControlRoleError("control credentials invalid")
        await verify_role_database_url(
            owner_url,
            database_url,
            names.versioned[purpose],
        )
        role_urls[purpose] = database_url
    return role_urls


async def _authorized_control_members(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    *,
    excluded_generation: str | None = None,
) -> PinnedControlMembers:
    pins: list[PinnedPrivateDirectory] = []
    try:
        return await _scan_authorized_control_members(
            connection,
            owner_url,
            state_dir,
            pins,
            excluded_generation=excluded_generation,
        )
    except BaseException:
        close_pinned_private_directories(pins)
        raise


async def _scan_authorized_control_members(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    pins: list[PinnedPrivateDirectory],
    *,
    excluded_generation: str | None = None,
) -> PinnedControlMembers:
    authorized: dict[str, set[str]] = {
        purpose: set() for purpose in STABLE_ROLES
    }
    root_pin = _pin_optional_private_directory(state_dir)
    if root_pin is None:
        return PinnedControlMembers(authorized, ())
    pins.append(root_pin)
    try:
        with os.scandir(state_dir) as entries:
            generation_names = sorted(
                entry.name
                for entry in entries
                if entry.is_dir(follow_symlinks=False)
            )
        _verify_pinned_directories(pins)
    except BaseException:
        close_pinned_private_directories(pins)
        raise
    for generation in generation_names:
        if not GENERATION_PATTERN.fullmatch(generation):
            continue
        if generation == excluded_generation:
            continue
        generation_dir = state_dir / generation
        names = role_names_for_generation(generation)
        generation_pin: PinnedPrivateDirectory | None = None
        try:
            generation_pin = _pin_required_private_directory(
                generation_dir
            )
            await _validate_existing_generation(
                connection,
                owner_url,
                credential_paths(state_dir, generation),
                names,
            )
            verify_pinned_private_directory(generation_pin)
            _verify_pinned_directories(pins)
        except (
            asyncpg.PostgresError,
            OSError,
            ControlRoleError,
            WorkerRoleCommonError,
        ):
            if generation_pin is not None:
                generation_pin.close()
            try:
                await quarantine_login_roles(
                    connection,
                    tuple(names.versioned.values()),
                )
            except BaseException:
                close_pinned_private_directories(pins)
                raise
            continue
        pins.append(generation_pin)
        for purpose, role_name in names.versioned.items():
            authorized[purpose].add(role_name)
    try:
        _verify_pinned_directories(pins)
    except BaseException:
        close_pinned_private_directories(pins)
        raise
    return PinnedControlMembers(authorized, pins)


def _capture_optional_private_directory(
    path: Path,
) -> PrivateDirectorySnapshot | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ControlStateTreeError(
            "control state directory invalid"
        ) from exc
    return _capture_required_private_directory(path)


def _pin_optional_private_directory(
    path: Path,
) -> PinnedPrivateDirectory | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ControlStateTreeError(
            "control state directory invalid"
        ) from exc
    return _pin_required_private_directory(path)


def _pin_required_private_directory(
    path: Path,
) -> PinnedPrivateDirectory:
    try:
        return pin_private_directory(path)
    except WorkerRoleCommonError as exc:
        raise ControlStateTreeError(
            "control state directory invalid"
        ) from exc


def _capture_required_private_directory(
    path: Path,
) -> PrivateDirectorySnapshot:
    try:
        return capture_private_directory(path)
    except WorkerRoleCommonError as exc:
        raise ControlStateTreeError(
            "control state directory invalid"
        ) from exc


def _verify_directory_snapshot(snapshot: PrivateDirectorySnapshot) -> None:
    try:
        verify_private_directory(snapshot)
    except WorkerRoleCommonError as exc:
        raise ControlStateTreeError(
            "control state directory invalid"
        ) from exc


def _verify_pinned_directories(
    pins: Sequence[PinnedPrivateDirectory],
) -> None:
    for pin in pins:
        try:
            verify_pinned_private_directory(pin)
        except WorkerRoleCommonError as exc:
            raise ControlStateTreeError(
                "control state directory invalid"
            ) from exc


def _state_tree_test_hook(phase: str) -> None:
    del phase


def _control_generation_tree_pins(
    state_dir: Path,
    generation: str,
) -> tuple[PinnedPrivateDirectory, ...]:
    pins: list[PinnedPrivateDirectory] = []
    try:
        root_pin = _pin_optional_private_directory(state_dir)
        if root_pin is None:
            return ()
        pins.append(root_pin)
        generation_pin = _pin_optional_private_directory(
            state_dir / generation
        )
        if generation_pin is not None:
            pins.append(generation_pin)
        _verify_pinned_directories(pins)
        return tuple(pins)
    except BaseException:
        close_pinned_private_directories(pins)
        raise


def _require_private_state_directory(path: Path) -> None:
    _capture_required_private_directory(path)


async def _revoke(
    owner_url: str,
    generation: str,
    state_dir: Path,
    names: ControlRoleNames,
) -> None:
    connection = await asyncpg.connect(asyncpg_url(owner_url))
    operation_pins: list[PinnedPrivateDirectory] = []
    try:
        await acquire_database_acl_dcl_lock(connection)
        await acquire_stable_role_authority_locks(
            connection,
            tuple(names.stable.values()),
        )
        await acquire_role_lifecycle_lock(
            connection,
            f"control:{generation}",
        )
        try:
            tree_pins = _control_generation_tree_pins(
                state_dir,
                generation,
            )
            operation_pins.extend(tree_pins)
        except ControlStateTreeError as exc:
            await quarantine_login_roles(
                connection,
                tuple(names.versioned.values()),
            )
            raise ControlRoleError(
                "control state directory invalid"
            ) from exc
        await drop_login_roles(
            connection,
            tuple(names.versioned.values()),
            state_guard=lambda: _verify_pinned_directories(
                operation_pins
            ),
        )
        close_pinned_private_directories(operation_pins)
        operation_pins.clear()
        remove_secure_files(
            state_dir,
            (generation,),
            tuple(CREDENTIAL_FILENAMES.values()),
        )
        authorized_members = await _authorized_control_members(
            connection,
            owner_url,
            state_dir,
        )
        member_pins = tuple(getattr(authorized_members, "pins", ()))
        try:
            _verify_pinned_directories(member_pins)
            async with connection.transaction():
                _verify_pinned_directories(member_pins)
                for purpose, stable_role in names.stable.items():
                    await ensure_stable_role(
                        connection,
                        stable_role,
                        setting_prefix="worker_control",
                        authorized_members=tuple(
                            sorted(authorized_members[purpose])
                        ),
                    )
                _verify_pinned_directories(member_pins)
        finally:
            close_pinned_private_directories(member_pins)
    except (asyncpg.PostgresError, WorkerRoleCommonError) as exc:
        raise ControlRoleError("control credential removal failed") from exc
    finally:
        close_pinned_private_directories(operation_pins)
        await connection.close()


def _emit(status: str, code: str, **fields: object) -> None:
    payload = {"code": code, "status": status, **fields}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
