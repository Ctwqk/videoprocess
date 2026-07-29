"""add durable production worker registration leases

Revision ID: 034_worker_registrations
Revises: 033_legacy_worker_event_resolutions
Create Date: 2026-07-26 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "034_worker_registrations"
down_revision: Union[str, None] = "033_legacy_worker_event_resolutions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


REGISTER_SIGNATURE = (
    "public.vp_worker_register(text,bigint,text,text,uuid,integer,text,jsonb,"
    "text,text,text,text,jsonb,text,text,text,text,text)"
)
HEARTBEAT_SIGNATURE = (
    "public.vp_worker_heartbeat(uuid,text,uuid,bigint,text)"
)
RELEASE_SIGNATURE = (
    "public.vp_worker_release(uuid,text,uuid,bigint,text,text)"
)
REQUIRE_SIGNATURE = "public.vp_require_worker_lease(uuid,bigint)"
OBSERVER_SIGNATURE = "public.vp_observe_worker_lease(uuid,bigint)"
ATTEST_TASK_SIGNATURE = (
    "public.vp_attest_worker_task_delivery("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
    "text,text,text,text,uuid)"
)
OBSERVE_TASK_SIGNATURE = (
    "public.vp_observe_worker_task_delivery("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
    "text,text,text,text,uuid)"
)
MARGIN_SIGNATURE = (
    "public.vp_require_worker_lease_margin(uuid,bigint,integer)"
)
TASK_ACK_SIGNATURE = (
    "public.vp_require_worker_task_ack_receipt("
    "uuid,bigint,text,timestamp with time zone,text,text,text,text,uuid)"
)
TASK_ACKNOWLEDGED_SIGNATURE = (
    "public.vp_acknowledge_worker_task_delivery("
    "uuid,uuid,bigint,text,timestamp with time zone,"
    "text,text,text,text,uuid)"
)
TASK_ACK_AUTHORIZE_SIGNATURE = (
    "public.vp_authorize_worker_task_ack("
    "uuid,uuid,bigint,text,timestamp with time zone)"
)
PROVEN_TASK_ACKNOWLEDGE_SIGNATURE = (
    "public.vp_acknowledge_proven_worker_task_dispatch(uuid)"
)
CLAIM_WORKER_NODE_SIGNATURE = (
    "public.vp_claim_worker_node("
    "uuid,bigint,text,uuid,uuid,text,text,text,text,uuid)"
)
REQUIRE_WORKER_NODE_CLAIM_SIGNATURE = (
    "public.vp_require_worker_node_claim("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid)"
)
PERSIST_WORKER_ARTIFACT_SIGNATURE = (
    "public.vp_persist_worker_artifact("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
    "text,text,bigint,text,text,jsonb)"
)
PREPARE_EVENT_EMISSION_SIGNATURE = (
    "public.vp_prepare_worker_event_emission("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
    "text,text,text,jsonb,text)"
)
MARK_EVENT_EMITTED_SIGNATURE = (
    "public.vp_mark_worker_event_emitted(uuid,uuid,bigint,text)"
)
LIST_PREPARED_EVENT_EMISSIONS_SIGNATURE = (
    "public.vp_list_worker_prepared_event_emissions(uuid,bigint,integer)"
)
LOAD_PREPARED_EVENT_EMISSION_SIGNATURE = (
    "public.vp_load_worker_prepared_event_emission(uuid,uuid,bigint)"
)
OBSERVE_EVENT_EMISSION_SIGNATURE = (
    "public.vp_observe_worker_event_emission("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
    "text,text,text,text)"
)
RESERVE_YOUTUBE_UPLOAD_SIGNATURE = (
    "public.vp_reserve_worker_youtube_upload("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
    "text,text,text)"
)
TRANSITION_YOUTUBE_UPLOAD_SIGNATURE = (
    "public.vp_transition_worker_youtube_upload("
    "uuid,bigint,text,timestamp with time zone,uuid,text,text,"
    "text,text,jsonb,text)"
)
RECOVER_REGISTERED_NODE_SIGNATURE = (
    "public.vp_recover_registered_worker_node(uuid,uuid)"
)
CANCEL_TASK_AUTHORIZE_SIGNATURE = (
    "public.vp_authorize_cancelled_worker_task_ack(uuid)"
)
CANCEL_TASK_ACKNOWLEDGE_SIGNATURE = (
    "public.vp_acknowledge_cancelled_worker_task(uuid,text,text,text,text,uuid)"
)
CANCEL_TASK_REQUIRE_SIGNATURE = (
    "public.vp_require_cancelled_worker_task_ack(uuid,text,text,text,text,uuid)"
)
CLEANUP_EVENT_AUTHORITY_SIGNATURE = (
    "public.vp_resolve_worker_event_authority_for_job_deletion(uuid)"
)
ENDPOINT_FINGERPRINTS_SIGNATURE = (
    "public.vp_worker_endpoint_fingerprints(jsonb)"
)
GRANT_UPSERT_SIGNATURE = (
    "public.vp_worker_grant_upsert(text,bigint,text,text,jsonb,text,text,text,"
    "text,text,jsonb,text,text)"
)
GRANT_ACTIVATE_SIGNATURE = (
    "public.vp_worker_grant_activate(text,bigint)"
)
GRANT_REVOKE_SIGNATURE = (
    "public.vp_worker_grant_revoke(text,bigint,text)"
)
REGISTRATION_REVOKE_SIGNATURE = (
    "public.vp_worker_registration_revoke(text,uuid,text)"
)
REGISTRATION_EXPIRE_SIGNATURE = (
    "public.vp_worker_registration_expire(text,uuid)"
)
BEGIN_STAGING_JANITOR_SIGNATURE = (
    "public.vp_begin_staging_janitor_run(uuid,text,integer)"
)
FINISH_STAGING_JANITOR_SIGNATURE = (
    "public.vp_finish_staging_janitor_run(uuid,jsonb,boolean)"
)
STAGING_JANITOR_READINESS_SIGNATURE = (
    "public.vp_staging_janitor_readiness(integer,integer)"
)
LIST_REDIS_MARKER_EXPECTATIONS_SIGNATURE = (
    "public.vp_list_worker_redis_marker_expectations(text,integer)"
)
BEGIN_REDIS_CONTINUITY_SIGNATURE = (
    "public.vp_begin_worker_redis_continuity_check(uuid,integer)"
)
FINISH_REDIS_CONTINUITY_SIGNATURE = (
    "public.vp_finish_worker_redis_continuity_check("
    "uuid,text,text,text,bigint,bigint)"
)
RECORD_REDIS_MARKER_OBSERVATION_SIGNATURE = (
    "public.vp_record_worker_redis_marker_observation("
    "uuid,text,uuid,text,text)"
)
REQUIRE_REDIS_CONTINUITY_SIGNATURE = (
    "public.vp_require_worker_redis_continuity(integer)"
)
CLAIM_REDIS_MARKER_CLEANUP_SIGNATURE = (
    "public.vp_claim_worker_redis_marker_cleanup(uuid,integer,integer)"
)
FINISH_REDIS_MARKER_CLEANUP_SIGNATURE = (
    "public.vp_finish_worker_redis_marker_cleanup(uuid,uuid,text,text)"
)
LOAD_REDIS_MARKER_REPAIR_SIGNATURE = (
    "public.vp_load_worker_redis_marker_repair(text,uuid)"
)
PROMOTE_OBSERVED_EVENT_EMISSION_SIGNATURE = (
    "public.vp_promote_observed_worker_event_emission(uuid,text,text)"
)
ORCHESTRATOR_CONTROL_ROLE = "vp_orchestrator_control_runtime"


def upgrade() -> None:
    _converge_public_security_defaults()
    op.create_table(
        "worker_admission_grants",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("service_name", sa.String(length=255), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("worker_type", sa.String(length=64), nullable=False),
        sa.Column("worker_host", sa.String(length=255), nullable=False),
        sa.Column(
            "capabilities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("release_commit", sa.String(length=40), nullable=False),
        sa.Column("image_identity", sa.String(length=255), nullable=False),
        sa.Column(
            "database_principal",
            sa.String(length=63),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("redis_group", sa.String(length=255), nullable=False),
        sa.Column(
            "endpoint_bindings_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("token_sha256", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column(
            "issued_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("issued_by", sa.String(length=255), nullable=False),
        sa.Column(
            "activated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoke_reason", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(length(trim(service_name)) > 0) IS TRUE",
            name="ck_worker_admission_grant_service_nonempty",
        ),
        sa.CheckConstraint(
            "(generation > 0) IS TRUE",
            name="ck_worker_admission_grant_generation_positive",
        ),
        sa.CheckConstraint(
            "(length(trim(worker_type)) > 0) IS TRUE",
            name="ck_worker_admission_grant_worker_type_nonempty",
        ),
        sa.CheckConstraint(
            "(length(trim(worker_host)) > 0) IS TRUE",
            name="ck_worker_admission_grant_worker_host_nonempty",
        ),
        sa.CheckConstraint(
            "(jsonb_typeof(capabilities_json) = 'array' "
            "AND jsonb_array_length(capabilities_json) > 0) IS TRUE",
            name="ck_worker_admission_grant_capabilities_array",
        ),
        sa.CheckConstraint(
            "(release_commit ~ '^[0-9a-f]{40}$') IS TRUE",
            name="ck_worker_admission_grant_release_commit",
        ),
        sa.CheckConstraint(
            "(image_identity ~ ("
            "'^[A-Za-z0-9][-A-Za-z0-9._/]*' "
            "|| chr(58) || 'deploy-[0-9a-f]{12}$')) IS TRUE",
            name="ck_worker_admission_grant_image_identity",
        ),
        sa.CheckConstraint(
            "(length(trim(database_principal)) > 0) IS TRUE",
            name="ck_worker_admission_grant_database_principal",
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0) IS TRUE",
            name="ck_worker_admission_grant_redis_stream",
        ),
        sa.CheckConstraint(
            "(length(trim(redis_group)) > 0) IS TRUE",
            name="ck_worker_admission_grant_redis_group",
        ),
        sa.CheckConstraint(
            "(jsonb_typeof(endpoint_bindings_json) = 'object') IS TRUE",
            name="ck_worker_admission_grant_endpoint_bindings",
        ),
        sa.CheckConstraint(
            "(token_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_admission_grant_token_sha256",
        ),
        sa.CheckConstraint(
            "(state IN ('pending', 'active', 'revoked')) IS TRUE",
            name="ck_worker_admission_grant_state",
        ),
        sa.CheckConstraint(
            "((state = 'pending' AND activated_at IS NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'active' AND activated_at IS NOT NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'revoked' AND revoked_at IS NOT NULL "
            "AND revoke_reason IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0)) IS TRUE",
            name="ck_worker_admission_grant_lifecycle",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "service_name",
            "generation",
            name="uq_worker_admission_grants_service_generation",
        ),
        sa.UniqueConstraint(
            "token_sha256",
            name="uq_worker_admission_grants_token_sha256",
        ),
    )
    op.create_index(
        "uq_worker_admission_grants_active_service",
        "worker_admission_grants",
        ["service_name"],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )

    op.create_table(
        "worker_registrations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "grant_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("service_name", sa.String(length=255), nullable=False),
        sa.Column("worker_type", sa.String(length=64), nullable=False),
        sa.Column("worker_host", sa.String(length=255), nullable=False),
        sa.Column(
            "capabilities_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "worker_instance_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("worker_slot", sa.Integer(), nullable=False),
        sa.Column("redis_consumer_id", sa.String(length=255), nullable=False),
        sa.Column("image_identity", sa.String(length=255), nullable=False),
        sa.Column(
            "database_principal",
            sa.String(length=63),
            nullable=False,
        ),
        sa.Column("database_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("redis_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("storage_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("lease_secret_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "heartbeat_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoke_reason", sa.String(length=255), nullable=True),
        sa.Column(
            "superseded_by",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(length(trim(service_name)) > 0) IS TRUE",
            name="ck_worker_registration_service_nonempty",
        ),
        sa.CheckConstraint(
            "(length(trim(worker_type)) > 0) IS TRUE",
            name="ck_worker_registration_worker_type_nonempty",
        ),
        sa.CheckConstraint(
            "(length(trim(worker_host)) > 0) IS TRUE",
            name="ck_worker_registration_worker_host_nonempty",
        ),
        sa.CheckConstraint(
            "(jsonb_typeof(capabilities_json) = 'array' "
            "AND jsonb_array_length(capabilities_json) > 0) IS TRUE",
            name="ck_worker_registration_capabilities_array",
        ),
        sa.CheckConstraint(
            "(worker_slot > 0) IS TRUE",
            name="ck_worker_registration_slot_positive",
        ),
        sa.CheckConstraint(
            "(length(trim(redis_consumer_id)) > 0) IS TRUE",
            name="ck_worker_registration_consumer_nonempty",
        ),
        sa.CheckConstraint(
            "(image_identity ~ ("
            "'^[A-Za-z0-9][-A-Za-z0-9._/]*' "
            "|| chr(58) || 'deploy-[0-9a-f]{12}$')) IS TRUE",
            name="ck_worker_registration_image_identity",
        ),
        sa.CheckConstraint(
            "(length(trim(database_principal)) > 0) IS TRUE",
            name="ck_worker_registration_database_principal",
        ),
        sa.CheckConstraint(
            "(database_fingerprint ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_registration_database_fingerprint",
        ),
        sa.CheckConstraint(
            "(redis_fingerprint ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_registration_redis_fingerprint",
        ),
        sa.CheckConstraint(
            "(storage_fingerprint ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_registration_storage_fingerprint",
        ),
        sa.CheckConstraint(
            "(lease_epoch > 0) IS TRUE",
            name="ck_worker_registration_epoch_positive",
        ),
        sa.CheckConstraint(
            "(lease_secret_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_registration_lease_secret_sha256",
        ),
        sa.CheckConstraint(
            "(status IN ('active', 'revoked', 'expired')) IS TRUE",
            name="ck_worker_registration_status",
        ),
        sa.CheckConstraint(
            "(heartbeat_at >= registered_at) IS TRUE",
            name="ck_worker_registration_heartbeat_order",
        ),
        sa.CheckConstraint(
            "(lease_expires_at > heartbeat_at) IS TRUE",
            name="ck_worker_registration_expiry_order",
        ),
        sa.CheckConstraint(
            "((status = 'revoked' AND revoked_at IS NOT NULL "
            "AND revoke_reason IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0) "
            "OR (status IN ('active', 'expired') "
            "AND revoked_at IS NULL AND revoke_reason IS NULL)) IS TRUE",
            name="ck_worker_registration_revocation_state",
        ),
        sa.CheckConstraint(
            "(superseded_by IS NULL "
            "OR (status = 'revoked' AND superseded_by IS NOT NULL "
            "AND superseded_by <> id)) IS TRUE",
            name="ck_worker_registration_supersession",
        ),
        sa.ForeignKeyConstraint(
            ["grant_id"],
            ["worker_admission_grants.id"],
            name="fk_worker_registrations_grant_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["superseded_by"],
            ["worker_registrations.id"],
            name="fk_worker_registrations_superseded_by",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "service_name",
            "lease_epoch",
            name="uq_worker_registrations_service_epoch",
        ),
    )
    op.create_index(
        "ix_worker_registrations_grant_id",
        "worker_registrations",
        ["grant_id"],
        unique=False,
    )
    op.create_index(
        "ix_worker_registrations_lease_expires_at",
        "worker_registrations",
        ["lease_expires_at"],
        unique=False,
    )
    op.create_index(
        "uq_worker_registrations_active_service",
        "worker_registrations",
        ["service_name"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.add_column(
        "node_executions",
        sa.Column(
            "worker_registration_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.add_column(
        "node_executions",
        sa.Column("worker_lease_epoch", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_node_executions_worker_registration_id",
        "node_executions",
        "worker_registrations",
        ["worker_registration_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_node_execution_worker_lease_binding",
        "node_executions",
        "((worker_registration_id IS NULL "
        "AND worker_lease_epoch IS NULL) "
        "OR (worker_registration_id IS NOT NULL "
        "AND worker_lease_epoch IS NOT NULL "
        "AND worker_lease_epoch > 0)) IS TRUE",
    )
    op.create_index(
        "ix_node_executions_worker_registration_id",
        "node_executions",
        ["worker_registration_id"],
        unique=False,
    )

    _create_registered_event_receipt_tables()
    _create_worker_redis_marker_tables()
    _create_staging_janitor_status()
    _upgrade_intermediate_artifact_cache()
    _create_receipt_immutability_trigger()
    _create_endpoint_fingerprints_function()
    _create_operator_functions()
    _create_register_function()
    _create_heartbeat_function()
    _create_release_function()
    _create_require_function()
    _create_observer_function()
    _create_task_delivery_functions()
    _create_worker_node_claim_function()
    _create_worker_node_authority_functions()
    _create_worker_event_emission_functions()
    _create_worker_youtube_upload_functions()
    _create_registered_node_recovery_function()
    _create_margin_function()
    _create_task_ack_function()
    _create_proven_task_acknowledge_function()
    _create_cancelled_task_functions()
    _create_event_authority_cleanup_function()
    _create_worker_redis_marker_functions()
    for signature in (
        REGISTER_SIGNATURE,
        HEARTBEAT_SIGNATURE,
        RELEASE_SIGNATURE,
        REQUIRE_SIGNATURE,
        OBSERVER_SIGNATURE,
        ATTEST_TASK_SIGNATURE,
        OBSERVE_TASK_SIGNATURE,
        MARGIN_SIGNATURE,
        TASK_ACK_SIGNATURE,
        TASK_ACKNOWLEDGED_SIGNATURE,
        TASK_ACK_AUTHORIZE_SIGNATURE,
        PROVEN_TASK_ACKNOWLEDGE_SIGNATURE,
        CLAIM_WORKER_NODE_SIGNATURE,
        REQUIRE_WORKER_NODE_CLAIM_SIGNATURE,
        PERSIST_WORKER_ARTIFACT_SIGNATURE,
        PREPARE_EVENT_EMISSION_SIGNATURE,
        MARK_EVENT_EMITTED_SIGNATURE,
        LIST_PREPARED_EVENT_EMISSIONS_SIGNATURE,
        LOAD_PREPARED_EVENT_EMISSION_SIGNATURE,
        OBSERVE_EVENT_EMISSION_SIGNATURE,
        RESERVE_YOUTUBE_UPLOAD_SIGNATURE,
        TRANSITION_YOUTUBE_UPLOAD_SIGNATURE,
        RECOVER_REGISTERED_NODE_SIGNATURE,
        CANCEL_TASK_AUTHORIZE_SIGNATURE,
        CANCEL_TASK_REQUIRE_SIGNATURE,
        CANCEL_TASK_ACKNOWLEDGE_SIGNATURE,
        CLEANUP_EVENT_AUTHORITY_SIGNATURE,
        ENDPOINT_FINGERPRINTS_SIGNATURE,
        GRANT_UPSERT_SIGNATURE,
        GRANT_ACTIVATE_SIGNATURE,
        GRANT_REVOKE_SIGNATURE,
        REGISTRATION_REVOKE_SIGNATURE,
        REGISTRATION_EXPIRE_SIGNATURE,
        BEGIN_STAGING_JANITOR_SIGNATURE,
        FINISH_STAGING_JANITOR_SIGNATURE,
        STAGING_JANITOR_READINESS_SIGNATURE,
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")


def _converge_public_security_defaults() -> None:
    op.execute(
        """
DO $database_acl$
BEGIN
    EXECUTE pg_catalog.format(
        'REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',
        pg_catalog.current_database()
    );
END
$database_acl$
"""
    )
    op.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC"
    )


def _create_worker_redis_marker_tables() -> None:
    op.create_table(
        "worker_redis_marker_cleanup_authorizations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("marker_kind", sa.String(length=32), nullable=False),
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("marker_key", sa.String(length=255), nullable=False),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column(
            "expected_message_id",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "authorization_state",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "authorized_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "claimed_by_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "claim_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("result_code", sa.String(length=64), nullable=True),
        sa.CheckConstraint(
            "(marker_kind IN ('event_emission', 'task_dispatch')) IS TRUE",
            name="ck_worker_redis_marker_cleanup_kind",
        ),
        sa.CheckConstraint(
            "(length(trim(marker_key)) > 0 "
            "AND length(trim(redis_stream)) > 0 "
            "AND length(trim(expected_message_id)) > 0) IS TRUE",
            name="ck_worker_redis_marker_cleanup_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_redis_marker_cleanup_sha256",
        ),
        sa.CheckConstraint(
            "(authorization_state IN "
            "('pending', 'claimed', 'deleted', 'absent', 'conflict')) "
            "IS TRUE",
            name="ck_worker_redis_marker_cleanup_state",
        ),
        sa.CheckConstraint(
            "((authorization_state = 'pending' "
            "AND claimed_by_run_id IS NULL AND claim_expires_at IS NULL "
            "AND finished_at IS NULL AND result_code IS NULL) "
            "OR (authorization_state = 'claimed' "
            "AND claimed_by_run_id IS NOT NULL "
            "AND claim_expires_at IS NOT NULL "
            "AND finished_at IS NULL AND result_code IS NULL) "
            "OR (authorization_state IN "
            "('deleted', 'absent', 'conflict') "
            "AND claimed_by_run_id IS NOT NULL "
            "AND claim_expires_at IS NOT NULL "
            "AND finished_at IS NOT NULL "
            "AND result_code IS NOT NULL)) IS TRUE",
            name="ck_worker_redis_marker_cleanup_claim",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "marker_kind",
            "source_id",
            name="uq_worker_redis_marker_cleanup_source",
        ),
        sa.UniqueConstraint(
            "marker_key",
            name="uq_worker_redis_marker_cleanup_key",
        ),
    )
    op.create_index(
        "ix_worker_redis_marker_cleanup_claim",
        "worker_redis_marker_cleanup_authorizations",
        ["authorization_state", "authorized_at", "id"],
        unique=False,
    )

    op.create_table(
        "worker_redis_continuity_status",
        sa.Column(
            "singleton",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column(
            "run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=False),
        sa.Column("redis_run_id", sa.String(length=255), nullable=True),
        sa.Column(
            "expected_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "checked_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(singleton) IS TRUE",
            name="ck_worker_redis_continuity_singleton",
        ),
        sa.CheckConstraint(
            "(state IN ('running', 'ready', 'error')) IS TRUE",
            name="ck_worker_redis_continuity_state",
        ),
        sa.CheckConstraint(
            "(length(trim(reason_code)) > 0 "
            "AND expected_count >= 0 "
            "AND checked_count >= 0 "
            "AND checked_count <= expected_count "
            "AND lease_expires_at = "
            "started_at + interval '300 seconds') IS TRUE",
            name="ck_worker_redis_continuity_counts",
        ),
        sa.CheckConstraint(
            "((state = 'running' "
            "AND reason_code = 'continuity_check_running' "
            "AND redis_run_id IS NULL "
            "AND checked_count = 0 "
            "AND finished_at IS NULL) "
            "OR (state = 'ready' "
            "AND reason_code = 'ready' "
            "AND length(trim(redis_run_id)) > 0 "
            "AND expected_count = checked_count "
            "AND finished_at IS NOT NULL) "
            "OR (state = 'error' "
            "AND finished_at IS NOT NULL)) IS TRUE",
            name="ck_worker_redis_continuity_result",
        ),
        sa.PrimaryKeyConstraint("singleton"),
    )

    op.create_table(
        "worker_redis_continuity_expectations",
        sa.Column(
            "run_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("marker_kind", sa.String(length=32), nullable=False),
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("marker_key", sa.String(length=255), nullable=False),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column(
            "expected_message_id",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_state", sa.String(length=64), nullable=False),
        sa.Column(
            "absence_allowed",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column(
            "observed_message_id",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "observed_payload_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column("observed_by", sa.String(length=63), nullable=True),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(marker_kind IN ('event_emission', 'task_dispatch')) IS TRUE",
            name="ck_worker_redis_continuity_expectation_kind",
        ),
        sa.CheckConstraint(
            "(length(trim(marker_key)) > 0 "
            "AND length(trim(redis_stream)) > 0 "
            "AND length(trim(source_state)) > 0) IS TRUE",
            name="ck_worker_redis_continuity_expectation_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_redis_continuity_expectation_sha256",
        ),
        sa.CheckConstraint(
            "((observed_message_id IS NULL "
            "AND observed_payload_sha256 IS NULL "
            "AND observed_by IS NULL AND observed_at IS NULL) "
            "OR (length(trim(observed_message_id)) > 0 "
            "AND observed_payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND length(trim(observed_by)) > 0 "
            "AND observed_at IS NOT NULL)) IS TRUE",
            name="ck_worker_redis_continuity_expectation_observation",
        ),
        sa.PrimaryKeyConstraint("run_id", "marker_key"),
        sa.UniqueConstraint(
            "run_id",
            "marker_kind",
            "source_id",
            name="uq_worker_redis_continuity_expectation_source",
        ),
    )
    op.create_index(
        "ix_worker_redis_continuity_expectation_page",
        "worker_redis_continuity_expectations",
        ["run_id", "marker_key"],
        unique=False,
    )

    op.create_table(
        "worker_redis_marker_repair_audits",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "source_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("result_code", sa.String(length=64), nullable=False),
        sa.Column("principal", sa.String(length=63), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(action IN ('restore_marker', 'promote_prepared')) IS TRUE",
            name="ck_worker_redis_marker_repair_action",
        ),
        sa.CheckConstraint(
            "(result_code IN ('authorized', 'restored', 'promoted')) "
            "IS TRUE",
            name="ck_worker_redis_marker_repair_result",
        ),
        sa.CheckConstraint(
            "(length(trim(principal)) > 0) IS TRUE",
            name="ck_worker_redis_marker_repair_principal",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for table_name in (
        "worker_redis_marker_cleanup_authorizations",
        "worker_redis_continuity_status",
        "worker_redis_continuity_expectations",
        "worker_redis_marker_repair_audits",
    ):
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM PUBLIC")

    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_redis_marker_cleanup_immutability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_proof_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF ROW(
        NEW.id,
        NEW.marker_kind,
        NEW.source_id,
        NEW.marker_key,
        NEW.redis_stream,
        NEW.expected_message_id,
        NEW.payload_sha256,
        NEW.authorized_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.marker_kind,
        OLD.source_id,
        OLD.marker_key,
        OLD.redis_stream,
        OLD.expected_message_id,
        OLD.payload_sha256,
        OLD.authorized_at
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_proof_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF NOT (
        (
            OLD.authorization_state = 'pending'
            AND NEW.authorization_state = 'claimed'
        )
        OR (
            OLD.authorization_state = 'claimed'
            AND NEW.authorization_state = 'claimed'
            AND (
                OLD.claimed_by_run_id = NEW.claimed_by_run_id
                OR OLD.claim_expires_at <= pg_catalog.clock_timestamp()
            )
        )
        OR (
            OLD.authorization_state = 'claimed'
            AND NEW.authorization_state IN ('deleted', 'absent', 'conflict')
            AND NEW.claimed_by_run_id = OLD.claimed_by_run_id
            AND NEW.claim_expires_at = OLD.claim_expires_at
        )
        OR (
            OLD.authorization_state IN ('deleted', 'absent')
            AND NEW.authorization_state = 'pending'
            AND NEW.claimed_by_run_id IS NULL
            AND NEW.claim_expires_at IS NULL
            AND NEW.finished_at IS NULL
            AND NEW.result_code IS NULL
        )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_state_transition_invalid',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_redis_marker_cleanup_immutability
BEFORE UPDATE OR DELETE
ON public.worker_redis_marker_cleanup_authorizations
FOR EACH ROW
EXECUTE FUNCTION
    public.vp_enforce_worker_redis_marker_cleanup_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_redis_marker_cleanup_immutability() "
        "FROM PUBLIC"
    )
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_redis_marker_repair_audit_append_only()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION USING
        MESSAGE = 'marker_repair_audit_append_only',
        ERRCODE = 'P0001';
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_redis_marker_repair_audit_append_only
BEFORE UPDATE OR DELETE
ON public.worker_redis_marker_repair_audits
FOR EACH ROW
EXECUTE FUNCTION
    public.vp_enforce_worker_redis_marker_repair_audit_append_only()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_redis_marker_repair_audit_append_only() "
        "FROM PUBLIC"
    )


def _create_staging_janitor_status() -> None:
    op.create_table(
        "staging_janitor_status",
        sa.Column(
            "singleton",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column(
            "active_run_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "active_runner_id",
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column(
            "active_started_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_success_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_failure_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_result_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "consecutive_failures",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "singleton",
            name="ck_staging_janitor_status_singleton",
        ),
        sa.CheckConstraint(
            "((active_run_id IS NULL "
            "AND active_runner_id IS NULL "
            "AND active_started_at IS NULL) "
            "OR (active_run_id IS NOT NULL "
            "AND active_runner_id IS NOT NULL "
            "AND active_started_at IS NOT NULL)) IS TRUE",
            name="ck_staging_janitor_status_active_run",
        ),
        sa.CheckConstraint(
            "(active_runner_id IS NULL "
            "OR length(trim(active_runner_id)) > 0) IS TRUE",
            name="ck_staging_janitor_status_runner",
        ),
        sa.CheckConstraint(
            "consecutive_failures >= 0",
            name="ck_staging_janitor_status_failures",
        ),
        sa.PrimaryKeyConstraint("singleton"),
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_begin_staging_janitor_run(
    p_run_id uuid,
    p_runner_id text,
    p_stale_run_seconds integer
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_status public.staging_janitor_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_recovered boolean := false;
BEGIN
    IF p_run_id IS NULL
       OR p_runner_id IS NULL
       OR length(trim(p_runner_id)) = 0
       OR length(p_runner_id) > 255
       OR p_runner_id IS DISTINCT FROM trim(p_runner_id)
       OR p_stale_run_seconds IS NULL
       OR p_stale_run_seconds < 300
       OR p_stale_run_seconds > 3600
    THEN
        RAISE EXCEPTION USING MESSAGE = 'janitor_status_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-staging-object-janitor',
            0
        )
    );
    INSERT INTO public.staging_janitor_status (singleton)
    VALUES (true)
    ON CONFLICT (singleton) DO NOTHING;
    SELECT status.*
    INTO v_status
    FROM public.staging_janitor_status AS status
    WHERE status.singleton
    FOR UPDATE;

    IF v_status.active_run_id IS NOT NULL
       AND v_status.active_started_at
           > v_now - pg_catalog.make_interval(
               secs => p_stale_run_seconds
           )
    THEN
        RETURN 'overlap';
    END IF;
    IF v_status.active_run_id IS NOT NULL THEN
        v_recovered := true;
    END IF;
    UPDATE public.staging_janitor_status
    SET active_run_id = p_run_id,
        active_runner_id = p_runner_id,
        active_started_at = v_now,
        last_failure_at = CASE
            WHEN v_recovered THEN v_now
            ELSE last_failure_at
        END,
        consecutive_failures = CASE
            WHEN v_recovered THEN consecutive_failures + 1
            ELSE consecutive_failures
        END,
        updated_at = v_now
    WHERE singleton;
    IF v_recovered THEN
        RETURN 'recovered_stale';
    END IF;
    RETURN 'started';
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_finish_staging_janitor_run(
    p_run_id uuid,
    p_result_json jsonb,
    p_succeeded boolean
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_status public.staging_janitor_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_effective_success boolean;
BEGIN
    IF p_run_id IS NULL
       OR p_result_json IS NULL
       OR p_succeeded IS NULL
       OR pg_catalog.jsonb_typeof(p_result_json) IS DISTINCT FROM 'object'
       OR NOT p_result_json ?& ARRAY[
           'schema_version', 'grace_seconds', 'scanned', 'deleted',
           'protected', 'too_young', 'invalid', 'errors'
       ]
       OR (
           SELECT count(*)
           FROM pg_catalog.jsonb_object_keys(p_result_json)
       ) <> 8
       OR p_result_json->>'schema_version' <> '1'
       OR p_result_json->>'grace_seconds' <> '86400'
       OR COALESCE(
           p_result_json->>'scanned' !~ '^[0-9]+$',
           true
       )
       OR COALESCE(
           p_result_json->>'deleted' !~ '^[0-9]+$',
           true
       )
       OR COALESCE(
           p_result_json->>'protected' !~ '^[0-9]+$',
           true
       )
       OR COALESCE(
           p_result_json->>'too_young' !~ '^[0-9]+$',
           true
       )
       OR COALESCE(
           p_result_json->>'invalid' !~ '^[0-9]+$',
           true
       )
       OR COALESCE(
           p_result_json->>'errors' !~ '^[0-9]+$',
           true
       )
    THEN
        RAISE EXCEPTION USING MESSAGE = 'janitor_status_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT status.*
    INTO v_status
    FROM public.staging_janitor_status AS status
    WHERE status.singleton
    FOR UPDATE;
    IF NOT FOUND
       OR v_status.active_run_id IS DISTINCT FROM p_run_id
    THEN
        RAISE EXCEPTION USING MESSAGE = 'janitor_run_mismatch', ERRCODE = 'P0001';
    END IF;

    v_effective_success := (
        p_succeeded
        AND (p_result_json->>'errors')::integer = 0
    );
    UPDATE public.staging_janitor_status
    SET active_run_id = NULL,
        active_runner_id = NULL,
        active_started_at = NULL,
        last_finished_at = v_now,
        last_success_at = CASE
            WHEN v_effective_success THEN v_now
            ELSE last_success_at
        END,
        last_failure_at = CASE
            WHEN v_effective_success THEN last_failure_at
            ELSE v_now
        END,
        last_result_json = p_result_json,
        consecutive_failures = CASE
            WHEN v_effective_success THEN 0
            ELSE consecutive_failures + 1
        END,
        updated_at = v_now
    WHERE singleton;
    RETURN v_effective_success;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_staging_janitor_readiness(
    p_max_age_seconds integer,
    p_stale_run_seconds integer
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_status public.staging_janitor_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_max_age_seconds IS NULL
       OR p_max_age_seconds < 300
       OR p_max_age_seconds > 3600
       OR p_stale_run_seconds IS NULL
       OR p_stale_run_seconds < 300
       OR p_stale_run_seconds > 3600
    THEN
        RAISE EXCEPTION USING MESSAGE = 'janitor_status_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT status.*
    INTO v_status
    FROM public.staging_janitor_status AS status
    WHERE status.singleton;
    IF FOUND
       AND v_status.active_run_id IS NOT NULL
       AND v_status.active_started_at
           <= v_now - pg_catalog.make_interval(
               secs => p_stale_run_seconds
           )
    THEN
        RETURN 'active_stale';
    END IF;
    IF NOT FOUND OR v_status.last_success_at IS NULL THEN
        RETURN 'missing';
    END IF;
    IF v_status.last_failure_at IS NOT NULL
       AND v_status.last_failure_at > v_status.last_success_at
    THEN
        RETURN 'latest_error';
    END IF;
    IF v_status.last_success_at
       < v_now - pg_catalog.make_interval(
           secs => p_max_age_seconds
       )
    THEN
        RETURN 'stale_success';
    END IF;
    IF v_status.last_result_json->>'schema_version' <> '1'
       OR v_status.last_result_json->>'grace_seconds' <> '86400'
       OR v_status.last_result_json->>'errors' <> '0'
    THEN
        RETURN 'latest_error';
    END IF;
    RETURN 'ready';
END;
$function$
"""
    )


def _upgrade_intermediate_artifact_cache() -> None:
    for column in (
        sa.Column("storage_backend", sa.String(length=50), nullable=True),
        sa.Column("storage_path", sa.String(length=1024), nullable=True),
        sa.Column("filename", sa.String(length=512), nullable=True),
        sa.Column("mime_type", sa.String(length=100), nullable=True),
        sa.Column("file_size", sa.BigInteger(), nullable=True),
        sa.Column(
            "media_info",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    ):
        op.add_column("intermediate_artifact_cache", column)
    op.execute(
        """
UPDATE public.intermediate_artifact_cache AS cache
SET storage_backend = artifact.storage_backend,
    storage_path = artifact.storage_path,
    filename = artifact.filename,
    mime_type = artifact.mime_type,
    file_size = artifact.file_size,
    media_info = artifact.media_info::jsonb
FROM public.artifacts AS artifact
WHERE artifact.id = cache.output_artifact_id
"""
    )
    op.drop_constraint(
        "intermediate_artifact_cache_output_artifact_id_fkey",
        "intermediate_artifact_cache",
        type_="foreignkey",
    )
    op.alter_column(
        "intermediate_artifact_cache",
        "output_artifact_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )
    op.create_foreign_key(
        "intermediate_artifact_cache_output_artifact_id_fkey",
        "intermediate_artifact_cache",
        "artifacts",
        ["output_artifact_id"],
        ["id"],
        ondelete="SET NULL",
    )


def _create_registered_event_receipt_tables() -> None:
    op.create_table(
        "worker_task_dispatches",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "origin_receipt_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "dispatch_key",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "node_execution_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "delivery_state",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "delivery_attempted_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("delivery_error", sa.String(length=255), nullable=True),
        sa.Column("redis_message_id", sa.String(length=64), nullable=True),
        sa.Column(
            "resolution_state",
            sa.String(length=24),
            server_default=sa.text("'unresolved'"),
            nullable=False,
        ),
        sa.Column(
            "acknowledged_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "cancelled_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "delivered_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0) IS TRUE",
            name="ck_worker_task_dispatch_redis_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_task_dispatch_sha256",
        ),
        sa.CheckConstraint(
            "(delivery_state IN "
            "('pending', 'attempting', 'delivered', 'uncertain', "
            "'cancelled')) IS TRUE",
            name="ck_worker_task_dispatch_state",
        ),
        sa.CheckConstraint(
            "((delivery_state = 'pending' "
            "AND delivery_attempted_at IS NULL "
            "AND redis_message_id IS NULL AND delivered_at IS NULL) "
            "OR (delivery_state IN ('attempting', 'uncertain') "
            "AND delivery_attempted_at IS NOT NULL "
            "AND redis_message_id IS NULL AND delivered_at IS NULL) "
            "OR (delivery_state = 'delivered' "
            "AND delivery_attempted_at IS NOT NULL "
            "AND redis_message_id IS NOT NULL "
            "AND delivered_at IS NOT NULL) "
            "OR (delivery_state = 'cancelled' "
            "AND redis_message_id IS NULL "
            "AND delivered_at IS NULL)) IS TRUE",
            name="ck_worker_task_dispatch_delivery",
        ),
        sa.CheckConstraint(
            "(resolution_state IN "
            "('unresolved', 'cancel_authorized', 'acknowledged', "
            "'cancelled')) IS TRUE",
            name="ck_worker_task_dispatch_resolution_state",
        ),
        sa.CheckConstraint(
            "((resolution_state IN ('unresolved', 'cancel_authorized') "
            "AND acknowledged_at IS NULL AND cancelled_at IS NULL) "
            "OR (resolution_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL AND cancelled_at IS NULL) "
            "OR (resolution_state = 'cancelled' "
            "AND acknowledged_at IS NULL "
            "AND cancelled_at IS NOT NULL)) IS TRUE",
            name="ck_worker_task_dispatch_resolution_time",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_worker_task_dispatches_job_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["node_execution_id"],
            ["node_executions.id"],
            name="fk_worker_task_dispatches_node_execution_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "dispatch_key",
            name="uq_worker_task_dispatch_key",
        ),
        sa.UniqueConstraint(
            "origin_receipt_id",
            "node_execution_id",
            name="uq_worker_task_dispatch_receipt_node",
        ),
    )
    op.create_index(
        "ix_worker_task_dispatches_origin_receipt_id",
        "worker_task_dispatches",
        ["origin_receipt_id"],
        unique=False,
    )
    op.create_index(
        "uq_worker_task_dispatch_unresolved_initial_node",
        "worker_task_dispatches",
        ["node_execution_id"],
        unique=True,
        postgresql_where=sa.text(
            "origin_receipt_id IS NULL "
            "AND resolution_state IN ('unresolved', 'cancel_authorized')"
        ),
    )
    op.create_index(
        "ix_worker_task_dispatches_pending",
        "worker_task_dispatches",
        ["delivery_state", "created_at"],
        unique=False,
    )

    op.create_table(
        "worker_task_delivery_attestations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "dispatch_key",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "node_execution_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "worker_registration_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("worker_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column(
            "worker_started_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "ack_state",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "acknowledged_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "ack_event_emission_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "attested_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0 "
            "AND length(trim(worker_id)) > 0) IS TRUE",
            name="ck_worker_task_delivery_attestation_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_task_delivery_attestation_sha256",
        ),
        sa.CheckConstraint(
            "(worker_lease_epoch > 0) IS TRUE",
            name="ck_worker_task_delivery_attestation_epoch",
        ),
        sa.CheckConstraint(
            "(ack_state IN "
            "('pending', 'authorized', 'acknowledged')) IS TRUE",
            name="ck_worker_task_delivery_attestation_ack_state",
        ),
        sa.CheckConstraint(
            "((ack_state IN ('pending', 'authorized') "
            "AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL)) IS TRUE",
            name="ck_worker_task_delivery_attestation_ack_time",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_worker_task_delivery_attestations_job_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["node_execution_id"],
            ["node_executions.id"],
            name="fk_worker_task_delivery_attestations_node_execution_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_registration_id"],
            ["worker_registrations.id"],
            name="fk_worker_task_delivery_attestations_registration_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_worker_task_delivery_attestation_identity",
        ),
        sa.UniqueConstraint(
            "node_execution_id",
            "worker_registration_id",
            "worker_lease_epoch",
            "worker_id",
            "worker_started_at",
            name="uq_worker_task_delivery_attestation_claim",
        ),
    )
    op.create_index(
        "ix_worker_task_delivery_attestations_registration_id",
        "worker_task_delivery_attestations",
        ["worker_registration_id"],
        unique=False,
    )

    op.create_table(
        "worker_event_emissions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "source_task_attestation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=True),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "node_execution_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "worker_registration_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("worker_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column(
            "worker_started_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "emission_state",
            sa.String(length=16),
            server_default=sa.text("'prepared'"),
            nullable=False,
        ),
        sa.Column(
            "prepared_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "emitted_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "resolved_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(worker_id)) > 0) IS TRUE",
            name="ck_worker_event_emission_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_worker_event_emission_sha256",
        ),
        sa.CheckConstraint(
            "(event_type IN ('node_completed', 'node_failed')) IS TRUE",
            name="ck_worker_event_emission_event_type",
        ),
        sa.CheckConstraint(
            "(worker_lease_epoch > 0) IS TRUE",
            name="ck_worker_event_emission_epoch",
        ),
        sa.CheckConstraint(
            "(emission_state IN ('prepared', 'emitted', 'resolved')) IS TRUE",
            name="ck_worker_event_emission_state",
        ),
        sa.CheckConstraint(
            "((emission_state = 'prepared' "
            "AND message_id IS NULL AND emitted_at IS NULL "
            "AND resolved_at IS NULL) "
            "OR (emission_state = 'emitted' "
            "AND message_id IS NOT NULL AND emitted_at IS NOT NULL "
            "AND resolved_at IS NULL) "
            "OR (emission_state = 'resolved' "
            "AND message_id IS NOT NULL AND emitted_at IS NOT NULL "
            "AND resolved_at IS NOT NULL)) IS TRUE",
            name="ck_worker_event_emission_times",
        ),
        sa.ForeignKeyConstraint(
            ["source_task_attestation_id"],
            ["worker_task_delivery_attestations.id"],
            name="fk_worker_event_emissions_attestation_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_worker_event_emissions_job_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["node_execution_id"],
            ["node_executions.id"],
            name="fk_worker_event_emissions_node_execution_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_registration_id"],
            ["worker_registrations.id"],
            name="fk_worker_event_emissions_registration_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_task_attestation_id",
            name="uq_worker_event_emission_attestation",
        ),
    )
    op.create_index(
        "uq_worker_event_emission_redis_identity",
        "worker_event_emissions",
        ["redis_stream", "consumer_group", "message_id"],
        unique=True,
        postgresql_where=sa.text("message_id IS NOT NULL"),
    )
    op.create_index(
        "ix_worker_event_emissions_job_id",
        "worker_event_emissions",
        ["job_id"],
        unique=False,
    )
    op.create_table(
        "registered_worker_event_receipts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "source_task_attestation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "payload_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "node_execution_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "worker_registration_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("worker_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column(
            "worker_started_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "source_task_stream",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "source_task_group",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "source_task_message_id",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "application_state",
            sa.String(length=16),
            server_default=sa.text("'accepted'"),
            nullable=False,
        ),
        sa.Column(
            "ack_state",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "source_task_ack_state",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "accepted_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "applied_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "acknowledged_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "source_task_acknowledged_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0) IS TRUE",
            name="ck_registered_worker_event_receipt_redis_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_registered_worker_event_receipt_sha256",
        ),
        sa.CheckConstraint(
            "(event_type IN ('node_completed', 'node_failed')) IS TRUE",
            name="ck_registered_worker_event_receipt_event_type",
        ),
        sa.CheckConstraint(
            "(worker_lease_epoch > 0) IS TRUE",
            name="ck_registered_worker_event_receipt_epoch",
        ),
        sa.CheckConstraint(
            "(length(trim(worker_id)) > 0 "
            "AND length(trim(source_task_stream)) > 0 "
            "AND length(trim(source_task_group)) > 0 "
            "AND length(trim(source_task_message_id)) > 0) IS TRUE",
            name="ck_registered_worker_event_receipt_claim_identity",
        ),
        sa.CheckConstraint(
            "(application_state IN ('accepted', 'applied')) IS TRUE",
            name="ck_registered_worker_event_receipt_application_state",
        ),
        sa.CheckConstraint(
            "(ack_state IN ('pending', 'acknowledged')) IS TRUE",
            name="ck_registered_worker_event_receipt_ack_state",
        ),
        sa.CheckConstraint(
            "(source_task_ack_state IN "
            "('pending', 'acknowledged')) IS TRUE",
            name="ck_registered_worker_event_receipt_task_ack_state",
        ),
        sa.CheckConstraint(
            "((application_state = 'accepted' AND applied_at IS NULL) "
            "OR (application_state = 'applied' "
            "AND applied_at IS NOT NULL)) IS TRUE",
            name="ck_registered_worker_event_receipt_application_time",
        ),
        sa.CheckConstraint(
            "((ack_state = 'pending' AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND application_state = 'applied' "
            "AND acknowledged_at IS NOT NULL)) IS TRUE",
            name="ck_registered_worker_event_receipt_ack_time",
        ),
        sa.CheckConstraint(
            "((source_task_ack_state = 'pending' "
            "AND source_task_acknowledged_at IS NULL) "
            "OR (source_task_ack_state = 'acknowledged' "
            "AND application_state = 'applied' "
            "AND source_task_acknowledged_at IS NOT NULL)) IS TRUE",
            name="ck_registered_worker_event_receipt_task_ack_time",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_registered_worker_event_receipts_job_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["node_execution_id"],
            ["node_executions.id"],
            name="fk_registered_worker_event_receipts_node_execution_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_task_attestation_id"],
            ["worker_task_delivery_attestations.id"],
            name="fk_registered_worker_event_receipts_attestation_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["worker_registration_id"],
            ["worker_registrations.id"],
            name="fk_registered_worker_event_receipts_registration_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_task_attestation_id",
            name="uq_registered_worker_event_receipt_attestation",
        ),
        sa.UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_registered_worker_event_receipt_identity",
        ),
        sa.UniqueConstraint(
            "source_task_stream",
            "source_task_group",
            "source_task_message_id",
            name="uq_registered_worker_event_receipt_source_task",
        ),
    )
    op.create_index(
        "ix_registered_worker_event_receipts_node_execution_id",
        "registered_worker_event_receipts",
        ["node_execution_id"],
        unique=False,
    )
    op.create_index(
        "ix_registered_worker_event_receipts_worker_registration_id",
        "registered_worker_event_receipts",
        ["worker_registration_id"],
        unique=False,
    )
    op.create_index(
        "ix_registered_worker_event_receipts_source_task",
        "registered_worker_event_receipts",
        [
            "source_task_stream",
            "source_task_group",
            "source_task_message_id",
        ],
        unique=False,
    )

    op.create_table(
        "registered_worker_event_deliveries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "source_task_attestation_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "receipt_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("resolution_state", sa.String(length=16), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column(
            "ack_state",
            sa.String(length=16),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "accepted_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "acknowledged_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "(length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0) IS TRUE",
            name="ck_registered_worker_event_delivery_redis_identity",
        ),
        sa.CheckConstraint(
            "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE",
            name="ck_registered_worker_event_delivery_sha256",
        ),
        sa.CheckConstraint(
            "((resolution_state = 'accepted' "
            "AND receipt_id IS NOT NULL AND reason_code IS NULL) "
            "OR (resolution_state = 'quarantined' "
            "AND reason_code IS NOT NULL "
            "AND length(trim(reason_code)) > 0)) IS TRUE",
            name="ck_registered_worker_event_delivery_resolution",
        ),
        sa.CheckConstraint(
            "(ack_state IN ('pending', 'acknowledged')) IS TRUE",
            name="ck_registered_worker_event_delivery_ack_state",
        ),
        sa.CheckConstraint(
            "((ack_state = 'pending' AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL)) IS TRUE",
            name="ck_registered_worker_event_delivery_ack_time",
        ),
        sa.ForeignKeyConstraint(
            ["source_task_attestation_id"],
            ["worker_task_delivery_attestations.id"],
            name="fk_registered_worker_event_deliveries_attestation_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["receipt_id"],
            ["registered_worker_event_receipts.id"],
            name="fk_registered_worker_event_deliveries_receipt_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_registered_worker_event_delivery_identity",
        ),
    )
    op.create_index(
        "ix_registered_worker_event_deliveries_receipt_id",
        "registered_worker_event_deliveries",
        ["receipt_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_worker_task_dispatches_origin_receipt_id",
        "worker_task_dispatches",
        "registered_worker_event_receipts",
        ["origin_receipt_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def _create_receipt_immutability_trigger() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_event_receipt_immutability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF ROW(
        NEW.id,
        NEW.source_task_attestation_id,
        NEW.redis_stream,
        NEW.consumer_group,
        NEW.message_id,
        NEW.payload_sha256,
        NEW.payload_json,
        NEW.event_type,
        NEW.job_id,
        NEW.node_execution_id,
        NEW.worker_registration_id,
        NEW.worker_lease_epoch,
        NEW.worker_id,
        NEW.worker_started_at,
        NEW.source_task_stream,
        NEW.source_task_group,
        NEW.source_task_message_id,
        NEW.accepted_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.source_task_attestation_id,
        OLD.redis_stream,
        OLD.consumer_group,
        OLD.message_id,
        OLD.payload_sha256,
        OLD.payload_json,
        OLD.event_type,
        OLD.job_id,
        OLD.node_execution_id,
        OLD.worker_registration_id,
        OLD.worker_lease_epoch,
        OLD.worker_id,
        OLD.worker_started_at,
        OLD.source_task_stream,
        OLD.source_task_group,
        OLD.source_task_message_id,
        OLD.accepted_at
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'receipt_facts_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.application_state = 'applied'
       AND (
           NEW.application_state IS DISTINCT FROM OLD.application_state
           OR NEW.applied_at IS DISTINCT FROM OLD.applied_at
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'receipt_application_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.ack_state = 'acknowledged'
       AND (
           NEW.ack_state IS DISTINCT FROM OLD.ack_state
           OR NEW.acknowledged_at IS DISTINCT FROM OLD.acknowledged_at
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'receipt_ack_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.source_task_ack_state = 'acknowledged'
       AND (
           NEW.source_task_ack_state
               IS DISTINCT FROM OLD.source_task_ack_state
           OR NEW.source_task_acknowledged_at
               IS DISTINCT FROM OLD.source_task_acknowledged_at
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'receipt_task_ack_immutable',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_event_receipt_immutability
BEFORE UPDATE ON public.registered_worker_event_receipts
FOR EACH ROW
EXECUTE FUNCTION public.vp_enforce_worker_event_receipt_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_event_receipt_immutability() FROM PUBLIC"
    )
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_task_attestation_immutability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF ROW(
        NEW.id,
        NEW.redis_stream,
        NEW.consumer_group,
        NEW.message_id,
        NEW.payload_sha256,
        NEW.dispatch_key,
        NEW.job_id,
        NEW.node_execution_id,
        NEW.worker_registration_id,
        NEW.worker_lease_epoch,
        NEW.worker_id,
        NEW.worker_started_at,
        NEW.attested_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.redis_stream,
        OLD.consumer_group,
        OLD.message_id,
        OLD.payload_sha256,
        OLD.dispatch_key,
        OLD.job_id,
        OLD.node_execution_id,
        OLD.worker_registration_id,
        OLD.worker_lease_epoch,
        OLD.worker_id,
        OLD.worker_started_at,
        OLD.attested_at
    )
       OR OLD.ack_state = 'acknowledged'
       OR (
            OLD.ack_state = 'pending'
            AND NEW.ack_state NOT IN ('authorized', 'acknowledged')
       )
       OR (
            OLD.ack_state = 'authorized'
            AND NEW.ack_state <> 'acknowledged'
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF NEW.ack_state = 'authorized'
       AND (
           NEW.ack_event_emission_id IS NULL
           OR NOT EXISTS (
                SELECT 1
                FROM public.worker_registrations AS registration
                WHERE registration.id = NEW.worker_registration_id
                  AND registration.lease_epoch = NEW.worker_lease_epoch
                  AND registration.database_principal = session_user
                  AND registration.status = 'active'
                  AND registration.lease_expires_at
                        > pg_catalog.clock_timestamp()
           )
           OR NOT EXISTS (
                SELECT 1
                FROM public.worker_event_emissions AS emission
                WHERE emission.id = NEW.ack_event_emission_id
                  AND emission.source_task_attestation_id = NEW.id
                  AND emission.emission_state IN ('emitted', 'resolved')
           )
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_ack_authority_missing',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.ack_state = 'pending'
       AND NEW.ack_state = 'acknowledged'
       AND NOT EXISTS (
            SELECT 1
            FROM public.registered_worker_event_receipts AS receipt
            WHERE receipt.source_task_attestation_id = NEW.id
              AND receipt.application_state = 'applied'
       )
       AND (
            EXISTS (
                SELECT 1
                FROM public.worker_event_emissions AS emission
                WHERE emission.source_task_attestation_id = NEW.id
            )
            OR NOT EXISTS (
                SELECT 1
                FROM public.worker_task_dispatches AS dispatch
                WHERE dispatch.dispatch_key = NEW.dispatch_key
                  AND dispatch.redis_stream = NEW.redis_stream
                  AND dispatch.consumer_group = NEW.consumer_group
                  AND dispatch.redis_message_id = NEW.message_id
                  AND dispatch.payload_sha256 = NEW.payload_sha256
                  AND dispatch.resolution_state IN (
                      'cancel_authorized',
                      'acknowledged'
                  )
            )
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_ack_authority_missing',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_task_attestation_immutability
BEFORE UPDATE ON public.worker_task_delivery_attestations
FOR EACH ROW
EXECUTE FUNCTION public.vp_enforce_worker_task_attestation_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_task_attestation_immutability() FROM PUBLIC"
    )
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_event_emission_immutability()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF ROW(
        NEW.id,
        NEW.source_task_attestation_id,
        NEW.redis_stream,
        NEW.consumer_group,
        NEW.payload_sha256,
        NEW.payload_json,
        NEW.event_type,
        NEW.job_id,
        NEW.node_execution_id,
        NEW.worker_registration_id,
        NEW.worker_lease_epoch,
        NEW.worker_id,
        NEW.worker_started_at,
        NEW.prepared_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.source_task_attestation_id,
        OLD.redis_stream,
        OLD.consumer_group,
        OLD.payload_sha256,
        OLD.payload_json,
        OLD.event_type,
        OLD.job_id,
        OLD.node_execution_id,
        OLD.worker_registration_id,
        OLD.worker_lease_epoch,
        OLD.worker_id,
        OLD.worker_started_at,
        OLD.prepared_at
    )
       OR (
           OLD.emission_state = 'prepared'
           AND NEW.emission_state NOT IN ('emitted')
       )
       OR (
           OLD.emission_state = 'emitted'
           AND (
               NEW.emission_state NOT IN ('emitted', 'resolved')
               OR NEW.message_id IS DISTINCT FROM OLD.message_id
               OR NEW.emitted_at IS DISTINCT FROM OLD.emitted_at
           )
       )
       OR OLD.emission_state = 'resolved'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_emission_immutable',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_event_emission_immutability
BEFORE UPDATE ON public.worker_event_emissions
FOR EACH ROW
EXECUTE FUNCTION public.vp_enforce_worker_event_emission_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_event_emission_immutability() FROM PUBLIC"
    )
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_event_delivery_immutability()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    IF ROW(
        NEW.id,
        NEW.source_task_attestation_id,
        NEW.receipt_id,
        NEW.redis_stream,
        NEW.consumer_group,
        NEW.message_id,
        NEW.payload_sha256,
        NEW.resolution_state,
        NEW.reason_code,
        NEW.accepted_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.source_task_attestation_id,
        OLD.receipt_id,
        OLD.redis_stream,
        OLD.consumer_group,
        OLD.message_id,
        OLD.payload_sha256,
        OLD.resolution_state,
        OLD.reason_code,
        OLD.accepted_at
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_delivery_facts_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.ack_state = 'acknowledged'
       AND ROW(NEW.ack_state, NEW.acknowledged_at)
           IS DISTINCT FROM ROW(OLD.ack_state, OLD.acknowledged_at)
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_delivery_ack_immutable',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_event_delivery_immutability
BEFORE UPDATE ON public.registered_worker_event_deliveries
FOR EACH ROW
EXECUTE FUNCTION public.vp_enforce_worker_event_delivery_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_event_delivery_immutability() FROM PUBLIC"
    )
    op.execute(
        """
CREATE FUNCTION public.vp_enforce_worker_task_dispatch_immutability()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    IF ROW(
        NEW.id,
        NEW.origin_receipt_id,
        NEW.dispatch_key,
        NEW.job_id,
        NEW.node_execution_id,
        NEW.redis_stream,
        NEW.consumer_group,
        NEW.payload_sha256,
        NEW.payload_json,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.origin_receipt_id,
        OLD.dispatch_key,
        OLD.job_id,
        OLD.node_execution_id,
        OLD.redis_stream,
        OLD.consumer_group,
        OLD.payload_sha256,
        OLD.payload_json,
        OLD.created_at
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_facts_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.delivery_state IN ('delivered', 'uncertain', 'cancelled')
       AND ROW(
           NEW.delivery_state,
           NEW.delivery_attempted_at,
           NEW.delivery_error,
           NEW.redis_message_id,
           NEW.delivered_at
       )
           IS DISTINCT FROM ROW(
               OLD.delivery_state,
               OLD.delivery_attempted_at,
               OLD.delivery_error,
               OLD.redis_message_id,
               OLD.delivered_at
           )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_delivery_immutable',
            ERRCODE = 'P0001';
    END IF;
    IF OLD.resolution_state IN ('acknowledged', 'cancelled')
       AND ROW(
           NEW.resolution_state,
           NEW.acknowledged_at,
           NEW.cancelled_at
       ) IS DISTINCT FROM ROW(
           OLD.resolution_state,
           OLD.acknowledged_at,
           OLD.cancelled_at
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_resolution_immutable',
            ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$function$
"""
    )
    op.execute(
        """
CREATE TRIGGER trg_worker_task_dispatch_immutability
BEFORE UPDATE ON public.worker_task_dispatches
FOR EACH ROW
EXECUTE FUNCTION public.vp_enforce_worker_task_dispatch_immutability()
"""
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.vp_enforce_worker_task_dispatch_immutability() FROM PUBLIC"
    )


def _create_endpoint_fingerprints_function() -> None:
    op.execute(
        r"""
CREATE FUNCTION public.vp_worker_endpoint_fingerprints(
    p_endpoint_bindings_json jsonb
)
RETURNS TABLE(
    database_fingerprint text,
    redis_fingerprint text,
    storage_fingerprint text
)
LANGUAGE plpgsql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_database_binding jsonb;
    v_redis_binding jsonb;
    v_storage_binding jsonb;
    v_database_host text;
    v_redis_host text;
    v_storage_host text;
    v_database_canonical text;
    v_redis_canonical text;
    v_storage_canonical text;
    v_dns_pattern text :=
        '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?'
        '(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$';
    v_ipv4_pattern text :=
        '^((0|[1-9][0-9]?|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}'
        '(0|[1-9][0-9]?|1[0-9]{2}|2[0-4][0-9]|25[0-5])$';
BEGIN
    IF p_endpoint_bindings_json IS NULL
       OR pg_catalog.jsonb_typeof(p_endpoint_bindings_json)
          IS DISTINCT FROM 'object'
       OR (
           SELECT pg_catalog.count(*)
           FROM pg_catalog.jsonb_object_keys(p_endpoint_bindings_json)
       ) <> 3
       OR NOT (
           p_endpoint_bindings_json
           ?& ARRAY['database', 'redis', 'storage']
       )
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    v_database_binding := p_endpoint_bindings_json -> 'database';
    v_redis_binding := p_endpoint_bindings_json -> 'redis';
    v_storage_binding := p_endpoint_bindings_json -> 'storage';
    v_database_host := v_database_binding ->> 'host';
    v_redis_host := v_redis_binding ->> 'host';
    v_storage_host := v_storage_binding ->> 'host';

    IF pg_catalog.jsonb_typeof(v_database_binding)
           IS DISTINCT FROM 'object'
       OR (
           SELECT pg_catalog.count(*)
           FROM pg_catalog.jsonb_object_keys(v_database_binding)
       ) <> 4
       OR NOT (
           v_database_binding
           ?& ARRAY['database', 'driver', 'host', 'port']
       )
       OR pg_catalog.jsonb_typeof(v_database_binding -> 'database')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(v_database_binding -> 'driver')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(v_database_binding -> 'host')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(v_database_binding -> 'port')
          IS DISTINCT FROM 'number'
       OR v_database_binding ->> 'driver' IS DISTINCT FROM 'postgresql'
       OR v_database_binding ->> 'database'
          !~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$'
       OR pg_catalog.length(v_database_host) NOT BETWEEN 1 AND 253
       OR v_database_host IS DISTINCT FROM pg_catalog.lower(v_database_host)
       OR v_database_host !~ v_dns_pattern
       OR (
           v_database_host ~ '^[0-9.]+$'
           AND v_database_host !~ v_ipv4_pattern
       )
       OR v_database_host = 'localhost'
       OR v_database_host ~ '^127\.'
       OR v_database_host = '0.0.0.0'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    IF (v_database_binding ->> 'port')::numeric
           IS DISTINCT FROM pg_catalog.trunc(
               (v_database_binding ->> 'port')::numeric
           )
       OR (v_database_binding ->> 'port')::numeric
          NOT BETWEEN 1 AND 65535
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    IF pg_catalog.jsonb_typeof(v_redis_binding)
           IS DISTINCT FROM 'object'
       OR (
           SELECT pg_catalog.count(*)
           FROM pg_catalog.jsonb_object_keys(v_redis_binding)
       ) <> 4
       OR NOT (
           v_redis_binding ?& ARRAY['database', 'host', 'port', 'scheme']
       )
       OR pg_catalog.jsonb_typeof(v_redis_binding -> 'database')
          IS DISTINCT FROM 'number'
       OR pg_catalog.jsonb_typeof(v_redis_binding -> 'host')
          IS DISTINCT FROM 'string'
       OR pg_catalog.jsonb_typeof(v_redis_binding -> 'port')
          IS DISTINCT FROM 'number'
       OR pg_catalog.jsonb_typeof(v_redis_binding -> 'scheme')
          IS DISTINCT FROM 'string'
       OR v_redis_binding ->> 'scheme' NOT IN ('redis', 'rediss')
       OR pg_catalog.length(v_redis_host) NOT BETWEEN 1 AND 253
       OR v_redis_host IS DISTINCT FROM pg_catalog.lower(v_redis_host)
       OR v_redis_host !~ v_dns_pattern
       OR (
           v_redis_host ~ '^[0-9.]+$'
           AND v_redis_host !~ v_ipv4_pattern
       )
       OR v_redis_host = 'localhost'
       OR v_redis_host ~ '^127\.'
       OR v_redis_host = '0.0.0.0'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    IF (v_redis_binding ->> 'port')::numeric
           IS DISTINCT FROM pg_catalog.trunc(
               (v_redis_binding ->> 'port')::numeric
           )
       OR (v_redis_binding ->> 'database')::numeric
           IS DISTINCT FROM pg_catalog.trunc(
               (v_redis_binding ->> 'database')::numeric
           )
       OR (v_redis_binding ->> 'port')::numeric
          NOT BETWEEN 1 AND 65535
       OR (v_redis_binding ->> 'database')::numeric
          NOT BETWEEN 0 AND 2147483647
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    IF pg_catalog.jsonb_typeof(v_storage_binding)
           IS DISTINCT FROM 'object'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    IF v_storage_binding = '{"backend":"not_applicable"}'::jsonb THEN
        v_storage_canonical := '{"backend":"not_applicable"}';
    ELSIF (
           SELECT pg_catalog.count(*)
           FROM pg_catalog.jsonb_object_keys(v_storage_binding)
       ) = 4
       AND v_storage_binding ?& ARRAY['backend', 'bucket', 'host', 'port']
       AND pg_catalog.jsonb_typeof(v_storage_binding -> 'backend') = 'string'
       AND pg_catalog.jsonb_typeof(v_storage_binding -> 'bucket') = 'string'
       AND pg_catalog.jsonb_typeof(v_storage_binding -> 'host') = 'string'
       AND pg_catalog.jsonb_typeof(v_storage_binding -> 'port') = 'number'
       AND v_storage_binding ->> 'backend' = 'minio'
       AND v_storage_binding ->> 'bucket'
           ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$'
       AND pg_catalog.length(v_storage_host) BETWEEN 1 AND 253
       AND v_storage_host = pg_catalog.lower(v_storage_host)
       AND v_storage_host ~ v_dns_pattern
       AND (
           v_storage_host !~ '^[0-9.]+$'
           OR v_storage_host ~ v_ipv4_pattern
       )
       AND v_storage_host <> 'localhost'
       AND v_storage_host !~ '^127\.'
       AND v_storage_host <> '0.0.0.0'
    THEN
        IF (v_storage_binding ->> 'port')::numeric
               IS DISTINCT FROM pg_catalog.trunc(
                   (v_storage_binding ->> 'port')::numeric
               )
           OR (v_storage_binding ->> 'port')::numeric
              NOT BETWEEN 1 AND 65535
        THEN
            RAISE EXCEPTION USING
                MESSAGE = 'claim_mismatch',
                ERRCODE = 'P0001';
        END IF;
        v_storage_canonical := pg_catalog.format(
            '{"backend":"minio","bucket":%s,"host":%s,"port":%s}',
            pg_catalog.to_jsonb(v_storage_binding ->> 'bucket')::text,
            pg_catalog.to_jsonb(v_storage_host)::text,
            ((v_storage_binding ->> 'port')::numeric::bigint)::text
        );
    ELSE
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    v_database_canonical := pg_catalog.format(
        '{"database":%s,"driver":"postgresql","host":%s,"port":%s}',
        pg_catalog.to_jsonb(v_database_binding ->> 'database')::text,
        pg_catalog.to_jsonb(v_database_host)::text,
        ((v_database_binding ->> 'port')::numeric::bigint)::text
    );
    v_redis_canonical := pg_catalog.format(
        '{"database":%s,"host":%s,"port":%s,"scheme":%s}',
        ((v_redis_binding ->> 'database')::numeric::bigint)::text,
        pg_catalog.to_jsonb(v_redis_host)::text,
        ((v_redis_binding ->> 'port')::numeric::bigint)::text,
        pg_catalog.to_jsonb(v_redis_binding ->> 'scheme')::text
    );
    RETURN QUERY SELECT
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(v_database_canonical, 'UTF8')
            ),
            'hex'
        ),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(v_redis_canonical, 'UTF8')
            ),
            'hex'
        ),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(v_storage_canonical, 'UTF8')
            ),
            'hex'
        );
END;
$function$
"""
    )


def _operator_active_registration_lock_sql() -> str:
    return """
    -- Global order: service-exclusive, registration-exclusive, then rows.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-service:' || p_service_name,
            0
        )
    );
    SELECT registration.id
    INTO v_registration_id
    FROM public.worker_registrations AS registration
    WHERE registration.service_name = p_service_name
      AND registration.status = 'active';
    IF v_registration_id IS NOT NULL THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
                'vp-worker-registration:' || v_registration_id::text,
                0
            )
        );
    END IF;
    SELECT registration.id
    INTO v_locked_registration_id
    FROM public.worker_registrations AS registration
    WHERE registration.service_name = p_service_name
      AND registration.status = 'active'
    FOR UPDATE;
    IF v_locked_registration_id IS DISTINCT FROM v_registration_id THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
"""


def _create_operator_functions() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_worker_grant_upsert(
    p_service_name text,
    p_generation bigint,
    p_worker_type text,
    p_worker_host text,
    p_capabilities_json jsonb,
    p_release_commit text,
    p_image_identity text,
    p_database_principal text,
    p_redis_stream text,
    p_redis_group text,
    p_endpoint_bindings_json jsonb,
    p_token_sha256 text,
    p_issued_by text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration_id uuid;
    v_locked_registration_id uuid;
    v_existing_state text;
    v_grant_id uuid;
    v_now timestamptz;
BEGIN
    IF p_service_name IS NULL
       OR p_generation IS NULL
       OR p_worker_type IS NULL
       OR p_worker_host IS NULL
       OR p_capabilities_json IS NULL
       OR p_release_commit IS NULL
       OR p_image_identity IS NULL
       OR p_database_principal IS NULL
       OR p_redis_stream IS NULL
       OR p_redis_group IS NULL
       OR p_endpoint_bindings_json IS NULL
       OR p_token_sha256 IS NULL
       OR p_issued_by IS NULL
       OR pg_catalog.length(p_service_name) NOT BETWEEN 1 AND 255
       OR p_service_name IS DISTINCT FROM pg_catalog.btrim(p_service_name)
       OR p_generation <= 0
       OR pg_catalog.length(p_worker_type) NOT BETWEEN 1 AND 64
       OR p_worker_type IS DISTINCT FROM pg_catalog.btrim(p_worker_type)
       OR pg_catalog.length(p_worker_host) NOT BETWEEN 1 AND 255
       OR p_worker_host IS DISTINCT FROM pg_catalog.btrim(p_worker_host)
       OR pg_catalog.jsonb_typeof(p_capabilities_json)
          IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_array_length(p_capabilities_json) = 0
       OR p_release_commit !~ '^[0-9a-f]{40}$'
       OR p_image_identity !~ (
           '^[A-Za-z0-9][-A-Za-z0-9._/]*'
           || pg_catalog.chr(58)
           || 'deploy-[0-9a-f]{12}$'
       )
       OR p_database_principal
          !~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'
       OR pg_catalog.length(p_redis_stream) NOT BETWEEN 1 AND 255
       OR p_redis_stream IS DISTINCT FROM pg_catalog.btrim(p_redis_stream)
       OR pg_catalog.length(p_redis_group) NOT BETWEEN 1 AND 255
       OR p_redis_group IS DISTINCT FROM pg_catalog.btrim(p_redis_group)
       OR p_token_sha256 !~ '^[0-9a-f]{64}$'
       OR pg_catalog.length(p_issued_by) NOT BETWEEN 1 AND 255
       OR p_issued_by IS DISTINCT FROM pg_catalog.btrim(p_issued_by)
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.vp_worker_endpoint_fingerprints(
        p_endpoint_bindings_json
    );
""" + _principal_guard_sql() + _operator_active_registration_lock_sql() + """
    SELECT grant_row.state
    INTO v_existing_state
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.service_name = p_service_name
      AND grant_row.generation = p_generation
    FOR UPDATE;
    IF FOUND AND v_existing_state IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION USING MESSAGE = 'grant_disabled', ERRCODE = 'P0001';
    END IF;
    v_now := pg_catalog.clock_timestamp();
    BEGIN
        INSERT INTO public.worker_admission_grants (
            service_name,
            generation,
            worker_type,
            worker_host,
            capabilities_json,
            release_commit,
            image_identity,
            database_principal,
            redis_stream,
            redis_group,
            endpoint_bindings_json,
            token_sha256,
            state,
            issued_at,
            issued_by,
            created_at,
            updated_at
        ) VALUES (
            p_service_name,
            p_generation,
            p_worker_type,
            p_worker_host,
            p_capabilities_json,
            p_release_commit,
            p_image_identity,
            p_database_principal,
            p_redis_stream,
            p_redis_group,
            p_endpoint_bindings_json,
            p_token_sha256,
            'pending',
            v_now,
            p_issued_by,
            v_now,
            v_now
        )
        ON CONFLICT (service_name, generation) DO UPDATE
        SET worker_type = EXCLUDED.worker_type,
            worker_host = EXCLUDED.worker_host,
            capabilities_json = EXCLUDED.capabilities_json,
            release_commit = EXCLUDED.release_commit,
            image_identity = EXCLUDED.image_identity,
            database_principal = EXCLUDED.database_principal,
            redis_stream = EXCLUDED.redis_stream,
            redis_group = EXCLUDED.redis_group,
            endpoint_bindings_json = EXCLUDED.endpoint_bindings_json,
            token_sha256 = EXCLUDED.token_sha256,
            issued_at = EXCLUDED.issued_at,
            issued_by = EXCLUDED.issued_by,
            updated_at = v_now
        RETURNING id INTO v_grant_id;
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION USING
                MESSAGE = 'claim_mismatch',
                ERRCODE = 'P0001';
    END;
    RETURN v_grant_id;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_worker_grant_activate(
    p_service_name text,
    p_generation bigint
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration_id uuid;
    v_locked_registration_id uuid;
    v_grant public.worker_admission_grants%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_service_name IS NULL OR p_generation IS NULL OR p_generation <= 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
""" + _principal_guard_sql() + _operator_active_registration_lock_sql() + """
    SELECT grant_row.*
    INTO v_grant
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.service_name = p_service_name
      AND grant_row.generation = p_generation
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'grant_missing', ERRCODE = 'P0001';
    END IF;
    IF v_grant.state = 'active' THEN
        RETURN v_grant.id;
    END IF;
    IF v_grant.state IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION USING MESSAGE = 'grant_disabled', ERRCODE = 'P0001';
    END IF;
    PERFORM grant_row.id
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.service_name = p_service_name
      AND grant_row.state = 'active'
    ORDER BY grant_row.id
    FOR UPDATE;
    v_now := pg_catalog.clock_timestamp();
    UPDATE public.worker_admission_grants AS grant_row
    SET state = 'revoked',
        revoked_at = v_now,
        revoke_reason = 'superseded',
        updated_at = v_now
    WHERE grant_row.service_name = p_service_name
      AND grant_row.state = 'active';
    UPDATE public.worker_admission_grants AS grant_row
    SET state = 'active',
        activated_at = v_now,
        updated_at = v_now
    WHERE grant_row.id = v_grant.id;
    RETURN v_grant.id;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_worker_grant_revoke(
    p_service_name text,
    p_generation bigint,
    p_reason text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration_id uuid;
    v_locked_registration_id uuid;
    v_grant public.worker_admission_grants%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_service_name IS NULL
       OR p_generation IS NULL
       OR p_reason IS NULL
       OR p_generation <= 0
       OR pg_catalog.length(p_reason) NOT BETWEEN 1 AND 255
       OR p_reason IS DISTINCT FROM pg_catalog.btrim(p_reason)
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
""" + _principal_guard_sql() + _operator_active_registration_lock_sql() + """
    SELECT grant_row.*
    INTO v_grant
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.service_name = p_service_name
      AND grant_row.generation = p_generation
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'grant_missing', ERRCODE = 'P0001';
    END IF;
    IF v_grant.state = 'revoked' THEN
        RETURN TRUE;
    END IF;
    v_now := pg_catalog.clock_timestamp();
    UPDATE public.worker_admission_grants AS grant_row
    SET state = 'revoked',
        revoked_at = v_now,
        revoke_reason = p_reason,
        updated_at = v_now
    WHERE grant_row.id = v_grant.id;
    IF v_registration_id IS NOT NULL THEN
        UPDATE public.worker_registrations AS registration
        SET status = 'revoked',
            revoked_at = v_now,
            revoke_reason = p_reason
        WHERE registration.id = v_registration_id
          AND registration.grant_id = v_grant.id
          AND registration.status = 'active';
    END IF;
    RETURN TRUE;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_worker_registration_revoke(
    p_service_name text,
    p_registration_id uuid,
    p_reason text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_service_name IS NULL
       OR p_registration_id IS NULL
       OR p_reason IS NULL
       OR pg_catalog.length(p_reason) NOT BETWEEN 1 AND 255
       OR p_reason IS DISTINCT FROM pg_catalog.btrim(p_reason)
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
""" + _principal_guard_sql() + """
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-service:' || p_service_name,
            0
        )
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_registration.service_name IS DISTINCT FROM p_service_name
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.status IN ('revoked', 'expired') THEN
        RETURN TRUE;
    END IF;
    v_now := pg_catalog.clock_timestamp();
    UPDATE public.worker_registrations AS registration
    SET status = 'revoked',
        revoked_at = v_now,
        revoke_reason = p_reason
    WHERE registration.id = p_registration_id;
    RETURN TRUE;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_worker_registration_expire(
    p_service_name text,
    p_registration_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_service_name IS NULL OR p_registration_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
""" + _principal_guard_sql() + """
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-service:' || p_service_name,
            0
        )
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_registration.service_name IS DISTINCT FROM p_service_name
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.status = 'expired' THEN
        RETURN TRUE;
    END IF;
    IF v_registration.status IS DISTINCT FROM 'active'
       OR v_registration.lease_expires_at > v_now
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    UPDATE public.worker_registrations AS registration
    SET status = 'expired'
    WHERE registration.id = p_registration_id;
    RETURN TRUE;
END;
$function$
"""
    )


def _create_register_function() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_worker_register(
    p_service_name text,
    p_generation bigint,
    p_worker_type text,
    p_worker_host text,
    p_worker_instance_id uuid,
    p_worker_slot integer,
    p_redis_consumer_id text,
    p_capabilities_json jsonb,
    p_release_commit text,
    p_image_identity text,
    p_redis_stream text,
    p_redis_group text,
    p_endpoint_bindings_json jsonb,
    p_database_fingerprint text,
    p_redis_fingerprint text,
    p_storage_fingerprint text,
    p_token_sha256 text,
    p_lease_secret_sha256 text
)
RETURNS TABLE(
    registration_id uuid,
    grant_id uuid,
    lease_epoch bigint,
    lease_expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_grant public.worker_admission_grants%ROWTYPE;
    v_registration_id uuid := gen_random_uuid();
    v_superseded_id uuid;
    v_locked_superseded_id uuid;
    v_epoch bigint;
    v_now timestamptz;
    v_expires_at timestamptz;
    v_expected_database_fingerprint text;
    v_expected_redis_fingerprint text;
    v_expected_storage_fingerprint text;
BEGIN
    IF p_token_sha256 IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'token_invalid', ERRCODE = 'P0001';
    END IF;
    IF p_service_name IS NULL
       OR p_generation IS NULL
       OR p_worker_type IS NULL
       OR p_worker_host IS NULL
       OR p_worker_instance_id IS NULL
       OR p_worker_slot IS NULL
       OR p_redis_consumer_id IS NULL
       OR p_capabilities_json IS NULL
       OR p_release_commit IS NULL
       OR p_image_identity IS NULL
       OR p_redis_stream IS NULL
       OR p_redis_group IS NULL
       OR p_endpoint_bindings_json IS NULL
       OR p_database_fingerprint IS NULL
       OR p_redis_fingerprint IS NULL
       OR p_storage_fingerprint IS NULL
       OR p_lease_secret_sha256 IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

""" + _principal_guard_sql() + """
    -- Global order: service-exclusive, registration-exclusive, then rows.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-service:' || p_service_name,
            0
        )
    );

    SELECT old_registration.id
    INTO v_superseded_id
    FROM public.worker_registrations AS old_registration
    WHERE old_registration.service_name = p_service_name
      AND old_registration.status = 'active';
    IF v_superseded_id IS NOT NULL THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
                'vp-worker-registration:' || v_superseded_id::text,
                0
            )
        );
    END IF;
    SELECT old_registration.id
    INTO v_locked_superseded_id
    FROM public.worker_registrations AS old_registration
    WHERE old_registration.service_name = p_service_name
      AND old_registration.status = 'active'
    FOR UPDATE;
    IF v_locked_superseded_id IS DISTINCT FROM v_superseded_id THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;

    SELECT grant_row.*
    INTO v_grant
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.service_name = p_service_name
      AND grant_row.generation = p_generation
    FOR UPDATE;

    IF NOT FOUND THEN
        IF EXISTS (
            SELECT 1
            FROM public.worker_admission_grants AS any_grant
            WHERE any_grant.service_name = p_service_name
        ) THEN
            RAISE EXCEPTION USING
                MESSAGE = 'claim_mismatch',
                ERRCODE = 'P0001';
        END IF;
        RAISE EXCEPTION USING
            MESSAGE = 'grant_missing',
            ERRCODE = 'P0001';
    END IF;

    IF v_grant.state <> 'active' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'grant_disabled',
            ERRCODE = 'P0001';
    END IF;
    IF v_grant.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_grant.token_sha256 IS DISTINCT FROM p_token_sha256 THEN
        RAISE EXCEPTION USING
            MESSAGE = 'token_invalid',
            ERRCODE = 'P0001';
    END IF;
    IF v_grant.worker_type IS DISTINCT FROM p_worker_type
       OR v_grant.worker_host IS DISTINCT FROM p_worker_host
       OR v_grant.capabilities_json IS DISTINCT FROM p_capabilities_json
       OR v_grant.release_commit IS DISTINCT FROM p_release_commit
       OR v_grant.image_identity IS DISTINCT FROM p_image_identity
       OR v_grant.redis_stream IS DISTINCT FROM p_redis_stream
       OR v_grant.redis_group IS DISTINCT FROM p_redis_group
       OR v_grant.endpoint_bindings_json
          IS DISTINCT FROM p_endpoint_bindings_json
       OR p_worker_slot <= 0
       OR length(trim(p_redis_consumer_id)) = 0
       OR p_database_fingerprint !~ '^[0-9a-f]{64}$'
       OR p_redis_fingerprint !~ '^[0-9a-f]{64}$'
       OR p_storage_fingerprint !~ '^[0-9a-f]{64}$'
       OR p_lease_secret_sha256 !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'claim_mismatch',
            ERRCODE = 'P0001';
    END IF;

    SELECT fingerprints.database_fingerprint,
           fingerprints.redis_fingerprint,
           fingerprints.storage_fingerprint
    INTO v_expected_database_fingerprint,
         v_expected_redis_fingerprint,
         v_expected_storage_fingerprint
    FROM public.vp_worker_endpoint_fingerprints(
        p_endpoint_bindings_json
    ) AS fingerprints;
    IF v_expected_database_fingerprint
           IS DISTINCT FROM p_database_fingerprint
       OR v_expected_redis_fingerprint
           IS DISTINCT FROM p_redis_fingerprint
       OR v_expected_storage_fingerprint
           IS DISTINCT FROM p_storage_fingerprint
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    v_now := pg_catalog.clock_timestamp();

    UPDATE public.worker_registrations AS old_registration
    SET status = 'revoked',
        revoked_at = v_now,
        revoke_reason = 'superseded'
    WHERE old_registration.service_name = p_service_name
      AND old_registration.status = 'active'
    RETURNING old_registration.id INTO v_superseded_id;

    SELECT COALESCE(MAX(existing.lease_epoch), 0) + 1
    INTO v_epoch
    FROM public.worker_registrations AS existing
    WHERE existing.service_name = p_service_name;

    v_expires_at := v_now + interval '180 seconds';
    INSERT INTO public.worker_registrations (
        id,
        grant_id,
        service_name,
        worker_type,
        worker_host,
        capabilities_json,
        worker_instance_id,
        worker_slot,
        redis_consumer_id,
        image_identity,
        database_principal,
        database_fingerprint,
        redis_fingerprint,
        storage_fingerprint,
        lease_epoch,
        lease_secret_sha256,
        status,
        registered_at,
        heartbeat_at,
        lease_expires_at
    ) VALUES (
        v_registration_id,
        v_grant.id,
        p_service_name,
        p_worker_type,
        p_worker_host,
        p_capabilities_json,
        p_worker_instance_id,
        p_worker_slot,
        p_redis_consumer_id,
        p_image_identity,
        v_principal,
        p_database_fingerprint,
        p_redis_fingerprint,
        p_storage_fingerprint,
        v_epoch,
        p_lease_secret_sha256,
        'active',
        v_now,
        v_now,
        v_expires_at
    );

    IF v_superseded_id IS NOT NULL THEN
        UPDATE public.worker_registrations AS old_registration
        SET superseded_by = v_registration_id
        WHERE old_registration.id = v_superseded_id;
    END IF;

    RETURN QUERY
    SELECT v_registration_id, v_grant.id, v_epoch, v_expires_at;
END;
$function$
"""
    )


def _principal_guard_sql() -> str:
    return """
    WITH RECURSIVE assumable_roles(role_oid) AS (
        SELECT role.oid
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = v_principal
        UNION
        SELECT membership.roleid
        FROM pg_catalog.pg_auth_members AS membership
        JOIN assumable_roles AS member_role
          ON member_role.role_oid = membership.member
        WHERE membership.set_option
    )
    SELECT EXISTS (
        SELECT 1
        FROM assumable_roles
        JOIN pg_catalog.pg_roles AS role
          ON role.oid = assumable_roles.role_oid
        WHERE role.rolsuper
           OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname IN (
                  'worker_admission_grants',
                  'worker_registrations',
                  'worker_redis_marker_cleanup_authorizations',
                  'worker_redis_continuity_status',
                  'worker_redis_continuity_expectations',
                  'worker_redis_marker_repair_audits'
              )
              AND relation.relowner = role.oid
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_admission_grants',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_registrations',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_redis_marker_cleanup_authorizations',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_redis_continuity_status',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_redis_continuity_expectations',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_table_privilege(
               role.oid,
               'public.worker_redis_marker_repair_audits',
               'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_admission_grants',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_registrations',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_redis_marker_cleanup_authorizations',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_redis_continuity_status',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_redis_continuity_expectations',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
           OR pg_catalog.has_any_column_privilege(
               role.oid,
               'public.worker_redis_marker_repair_audits',
               'SELECT,INSERT,UPDATE,REFERENCES'
           )
    )
    INTO v_privileged
    ;

    IF COALESCE(v_privileged, true) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_privileged',
            ERRCODE = 'P0001';
    END IF;
"""


def _marker_control_principal_guard_sql(expected_role: str) -> str:
    return (
        _principal_guard_sql()
        + f"""
    v_expected_role := pg_catalog.to_regrole('{expected_role}');
    IF v_expected_role IS NULL
       OR NOT COALESCE(
            pg_catalog.pg_has_role(
                v_principal,
                v_expected_role,
                'MEMBER'
            ),
            false
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_control_principal_unauthorized',
            ERRCODE = 'P0001';
    END IF;
"""
    )


def _create_heartbeat_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_worker_heartbeat(
    p_registration_id uuid,
    p_service_name text,
    p_worker_instance_id uuid,
    p_lease_epoch bigint,
    p_lease_secret_sha256 text
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
    v_expires_at timestamptz;
BEGIN
    IF p_registration_id IS NULL
       OR p_service_name IS NULL
       OR p_worker_instance_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_lease_secret_sha256 IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    -- Shared with require so heartbeats continue through irreversible work.
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.service_name IS DISTINCT FROM p_service_name
       OR v_registration.worker_instance_id
          IS DISTINCT FROM p_worker_instance_id
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
       OR v_registration.lease_secret_sha256
          IS DISTINCT FROM p_lease_secret_sha256
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_expired',
            ERRCODE = 'P0001';
    END IF;

    v_expires_at := v_now + interval '180 seconds';
    UPDATE public.worker_registrations AS registration
    SET heartbeat_at = v_now,
        lease_expires_at = v_expires_at
    WHERE registration.id = p_registration_id;
    RETURN v_expires_at;
END;
$function$
"""
    )


def _create_release_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_worker_release(
    p_registration_id uuid,
    p_service_name text,
    p_worker_instance_id uuid,
    p_lease_epoch bigint,
    p_lease_secret_sha256 text,
    p_reason text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL
       OR p_service_name IS NULL
       OR p_worker_instance_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_lease_secret_sha256 IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF p_reason IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    -- Release mutates only after all shared registration fences drain.
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.service_name IS DISTINCT FROM p_service_name
       OR v_registration.worker_instance_id
          IS DISTINCT FROM p_worker_instance_id
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.lease_secret_sha256
          IS DISTINCT FROM p_lease_secret_sha256
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.status IS DISTINCT FROM 'active' THEN
        RETURN true;
    END IF;
    IF length(trim(p_reason)) = 0 OR length(p_reason) > 255 THEN
        RAISE EXCEPTION USING
            MESSAGE = 'claim_mismatch',
            ERRCODE = 'P0001';
    END IF;

    UPDATE public.worker_registrations AS registration
    SET status = 'revoked',
        revoked_at = v_now,
        revoke_reason = p_reason
    WHERE registration.id = p_registration_id;
    RETURN true;
END;
$function$
"""
    )


def _create_require_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_require_worker_lease(
    p_registration_id uuid,
    p_lease_epoch bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL OR p_lease_epoch IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    -- The caller's transaction holds this fence across its durable operation.
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_expired',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )


def _create_observer_function() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_observe_worker_lease(
    p_registration_id uuid,
    p_lease_epoch bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL OR p_lease_epoch IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_expired',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )


def _create_task_delivery_functions() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_attest_worker_task_delivery(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
    v_grant_stream text;
    v_grant_group text;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
       OR p_job_id IS NULL
       OR p_node_execution_id IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_dispatch_key IS NULL
       OR length(trim(p_worker_id)) = 0
       OR length(trim(p_redis_stream)) = 0
       OR length(trim(p_consumer_group)) = 0
       OR length(trim(p_message_id)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_expired', ERRCODE = 'P0001';
    END IF;

    SELECT grant_row.redis_stream, grant_row.redis_group
    INTO v_grant_stream, v_grant_group
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.id = v_registration.grant_id;
    IF NOT FOUND
       OR v_grant_stream IS DISTINCT FROM p_redis_stream
       OR v_grant_group IS DISTINCT FROM p_consumer_group
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
      AND node.status::text = 'RUNNING'
      AND node.worker_id = p_worker_id
      AND node.started_at = p_worker_started_at
      AND node.worker_registration_id = p_registration_id
      AND node.worker_lease_epoch = p_lease_epoch
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'node_claim_mismatch',
            ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = p_dispatch_key
      AND dispatch.job_id = p_job_id
      AND dispatch.node_execution_id = p_node_execution_id
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.redis_message_id = p_message_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;

    INSERT INTO public.worker_task_delivery_attestations (
        redis_stream,
        consumer_group,
        message_id,
        payload_sha256,
        dispatch_key,
        job_id,
        node_execution_id,
        worker_registration_id,
        worker_lease_epoch,
        worker_id,
        worker_started_at,
        attested_at
    ) VALUES (
        p_redis_stream,
        p_consumer_group,
        p_message_id,
        p_payload_sha256,
        p_dispatch_key,
        p_job_id,
        p_node_execution_id,
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        v_now
    )
    ON CONFLICT DO NOTHING;

    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
    FOR SHARE;

    IF NOT FOUND
       OR v_attestation.payload_sha256 IS DISTINCT FROM p_payload_sha256
       OR v_attestation.dispatch_key IS DISTINCT FROM p_dispatch_key
       OR v_attestation.job_id IS DISTINCT FROM p_job_id
       OR v_attestation.node_execution_id
            IS DISTINCT FROM p_node_execution_id
       OR v_attestation.worker_registration_id
            IS DISTINCT FROM p_registration_id
       OR v_attestation.worker_lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_attestation.worker_id IS DISTINCT FROM p_worker_id
       OR v_attestation.worker_started_at
            IS DISTINCT FROM p_worker_started_at
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    RETURN v_attestation.id;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_observe_worker_task_delivery(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_registration public.worker_registrations%ROWTYPE;
    v_attestation_id uuid;
    v_grant_stream text;
    v_grant_group text;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
       OR p_job_id IS NULL
       OR p_node_execution_id IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{64}$'
       OR p_dispatch_key IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_expired', ERRCODE = 'P0001';
    END IF;

    SELECT grant_row.redis_stream, grant_row.redis_group
    INTO v_grant_stream, v_grant_group
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.id = v_registration.grant_id;
    IF NOT FOUND
       OR v_grant_stream IS DISTINCT FROM p_redis_stream
       OR v_grant_group IS DISTINCT FROM p_consumer_group
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;

    SELECT attestation.id
    INTO v_attestation_id
    FROM public.worker_task_delivery_attestations AS attestation
    JOIN public.worker_task_dispatches AS dispatch
      ON dispatch.dispatch_key = attestation.dispatch_key
    WHERE attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.job_id = p_job_id
      AND attestation.node_execution_id = p_node_execution_id
      AND attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
      AND attestation.payload_sha256 = p_payload_sha256
      AND attestation.dispatch_key = p_dispatch_key
      AND dispatch.job_id = p_job_id
      AND dispatch.node_execution_id = p_node_execution_id
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.redis_message_id = p_message_id
    FOR SHARE OF attestation, dispatch;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_missing',
            ERRCODE = 'P0001';
    END IF;
    RETURN v_attestation_id;
END;
$function$
"""
    )


def _create_worker_node_claim_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_claim_worker_node(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS TABLE(worker_started_at timestamptz, attestation_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_dispatch public.worker_task_dispatches%ROWTYPE;
    v_grant_stream text;
    v_grant_group text;
    v_started_at timestamptz;
    v_attestation_id uuid;
    v_task_id uuid;
    v_channel_id uuid;
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_job_id IS NULL
       OR p_node_execution_id IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_dispatch_key IS NULL
       OR length(trim(p_worker_id)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}

    SELECT task.id, task.channel_profile_id
    INTO v_task_id, v_channel_id
    FROM public.production_tasks AS task
    WHERE task.job_id = p_job_id
    ORDER BY task.id
    LIMIT 1;
    IF v_task_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM public.production_tasks AS task
        WHERE task.job_id = p_job_id AND task.id <> v_task_id
    ) THEN
        RAISE EXCEPTION USING MESSAGE = 'production_task_authority_changed', ERRCODE = 'P0001';
    END IF;
    IF v_channel_id IS NOT NULL THEN
        PERFORM 1
        FROM public.channel_profiles AS channel
        WHERE channel.id = v_channel_id
          AND channel.enabled
          AND channel.halted_at IS NULL
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING MESSAGE = 'channel_authority_changed', ERRCODE = 'P0001';
        END IF;
    END IF;
    PERFORM 1
    FROM public.runtime_schedules AS schedule
    WHERE schedule.service_name = 'videoprocess'
      AND schedule.state IN ('OPEN', 'DRAINING')
      AND (
          schedule.guarded_job_id IS NULL
          OR schedule.guarded_job_id = p_job_id
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'schedule_authority_changed', ERRCODE = 'P0001';
    END IF;
    IF v_task_id IS NOT NULL THEN
        PERFORM 1
        FROM public.production_tasks AS task
        WHERE task.id = v_task_id
          AND task.job_id = p_job_id
          AND task.channel_profile_id = v_channel_id
          AND task.state = 'producing'
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING MESSAGE = 'production_task_authority_changed', ERRCODE = 'P0001';
        END IF;
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.id = p_job_id
      AND job.status::text = 'RUNNING'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'job_authority_changed', ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
      AND node.status::text = 'QUEUED'
      AND node.worker_id IS NULL
      AND node.worker_registration_id IS NULL
      AND node.worker_lease_epoch IS NULL
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_started_at := pg_catalog.clock_timestamp();
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR SHARE;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
       OR v_registration.lease_expires_at <= v_started_at
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;

    SELECT grant_row.redis_stream, grant_row.redis_group
    INTO v_grant_stream, v_grant_group
    FROM public.worker_admission_grants AS grant_row
    WHERE grant_row.id = v_registration.grant_id;
    IF NOT FOUND
       OR v_grant_stream IS DISTINCT FROM p_redis_stream
       OR v_grant_group IS DISTINCT FROM p_consumer_group
    THEN
        RAISE EXCEPTION USING MESSAGE = 'task_dispatch_mismatch', ERRCODE = 'P0001';
    END IF;

    SELECT dispatch.*
    INTO v_dispatch
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = p_dispatch_key
      AND dispatch.job_id = p_job_id
      AND dispatch.node_execution_id = p_node_execution_id
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.redis_message_id = p_message_id
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'unresolved'
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'task_dispatch_mismatch', ERRCODE = 'P0001';
    END IF;

    UPDATE public.node_executions AS node
    SET status = 'RUNNING',
        started_at = v_started_at,
        worker_id = p_worker_id,
        worker_registration_id = p_registration_id,
        worker_lease_epoch = p_lease_epoch
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
      AND node.status::text = 'QUEUED'
      AND node.worker_id IS NULL
      AND node.worker_registration_id IS NULL
      AND node.worker_lease_epoch IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;

    INSERT INTO public.worker_task_delivery_attestations (
        redis_stream,
        consumer_group,
        message_id,
        payload_sha256,
        dispatch_key,
        job_id,
        node_execution_id,
        worker_registration_id,
        worker_lease_epoch,
        worker_id,
        worker_started_at,
        attested_at
    ) VALUES (
        p_redis_stream,
        p_consumer_group,
        p_message_id,
        p_payload_sha256,
        p_dispatch_key,
        p_job_id,
        p_node_execution_id,
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        v_started_at,
        v_started_at
    )
    RETURNING id INTO v_attestation_id;

    RETURN QUERY SELECT v_started_at, v_attestation_id;
END;
$function$
"""
    )


def _create_worker_node_authority_functions() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_require_worker_node_claim(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_task_id uuid;
    v_channel_id uuid;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
       OR p_job_id IS NULL
       OR p_node_execution_id IS NULL
       OR length(trim(p_worker_id)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}

    SELECT task.id, task.channel_profile_id
    INTO v_task_id, v_channel_id
    FROM public.production_tasks AS task
    WHERE task.job_id = p_job_id
    ORDER BY task.id
    LIMIT 1;
    IF v_task_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM public.production_tasks AS task
        WHERE task.job_id = p_job_id
          AND task.id <> v_task_id
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'production_task_authority_changed',
            ERRCODE = 'P0001';
    END IF;
    IF v_channel_id IS NOT NULL THEN
        PERFORM 1
        FROM public.channel_profiles AS channel
        WHERE channel.id = v_channel_id
          AND channel.enabled
          AND channel.halted_at IS NULL
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                MESSAGE = 'channel_authority_changed',
                ERRCODE = 'P0001';
        END IF;
    END IF;

    PERFORM 1
    FROM public.runtime_schedules AS schedule
    WHERE schedule.service_name = 'videoprocess'
      AND schedule.state IN ('OPEN', 'DRAINING')
      AND (
          schedule.guarded_job_id IS NULL
          OR schedule.guarded_job_id = p_job_id
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'schedule_authority_changed',
            ERRCODE = 'P0001';
    END IF;
    IF v_task_id IS NOT NULL THEN
        PERFORM 1
        FROM public.production_tasks AS task
        WHERE task.id = v_task_id
          AND task.job_id = p_job_id
          AND task.channel_profile_id = v_channel_id
          AND task.state = 'producing'
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                MESSAGE = 'production_task_authority_changed',
                ERRCODE = 'P0001';
        END IF;
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.id = p_job_id
      AND job.status::text = 'RUNNING'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'job_authority_changed',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
      AND node.status::text = 'RUNNING'
      AND node.worker_id = p_worker_id
      AND node.started_at = p_worker_started_at
      AND node.worker_registration_id = p_registration_id
      AND node.worker_lease_epoch = p_lease_epoch
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'node_claim_mismatch',
            ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
       OR v_registration.lease_expires_at <= v_now
       OR v_registration.database_principal IS DISTINCT FROM v_principal
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_persist_worker_artifact(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_filename text,
    p_mime_type text,
    p_file_size bigint,
    p_storage_backend text,
    p_storage_path text,
    p_media_info jsonb
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_artifact_id uuid;
BEGIN
    IF p_filename IS NULL
       OR p_file_size IS NULL
       OR p_file_size < 0
       OR p_storage_backend IS NULL
       OR p_storage_path IS NULL
       OR length(trim(p_filename)) = 0
       OR length(trim(p_storage_backend)) = 0
       OR length(trim(p_storage_path)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'artifact_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        p_job_id,
        p_node_execution_id
    );
    INSERT INTO public.artifacts (
        job_id,
        node_execution_id,
        kind,
        filename,
        mime_type,
        file_size,
        storage_backend,
        storage_path,
        media_info
    ) VALUES (
        p_job_id,
        p_node_execution_id,
        'INTERMEDIATE'::public.artifact_kind,
        p_filename,
        p_mime_type,
        p_file_size,
        p_storage_backend,
        p_storage_path,
        p_media_info
    )
    RETURNING id INTO v_artifact_id;
    RETURN v_artifact_id;
END;
$function$
"""
    )


def _create_worker_event_emission_functions() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_prepare_worker_event_emission(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_attestation_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_payload_sha256 text,
    p_payload_json jsonb,
    p_event_type text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_emission public.worker_event_emissions%ROWTYPE;
BEGIN
    IF p_attestation_id IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_payload_json IS NULL
       OR p_event_type NOT IN ('node_completed', 'node_failed')
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        p_job_id,
        p_node_execution_id
    );
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    JOIN public.worker_task_dispatches AS dispatch
      ON dispatch.dispatch_key = attestation.dispatch_key
    WHERE attestation.id = p_attestation_id
      AND attestation.job_id = p_job_id
      AND attestation.node_execution_id = p_node_execution_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.ack_state = 'pending'
      AND dispatch.job_id = p_job_id
      AND dispatch.node_execution_id = p_node_execution_id
      AND dispatch.redis_stream = attestation.redis_stream
      AND dispatch.consumer_group = attestation.consumer_group
      AND dispatch.redis_message_id = attestation.message_id
      AND dispatch.payload_sha256 = attestation.payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'unresolved'
    FOR UPDATE OF attestation, dispatch;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    INSERT INTO public.worker_event_emissions (
        source_task_attestation_id,
        redis_stream,
        consumer_group,
        payload_sha256,
        payload_json,
        event_type,
        job_id,
        node_execution_id,
        worker_registration_id,
        worker_lease_epoch,
        worker_id,
        worker_started_at
    ) VALUES (
        p_attestation_id,
        p_redis_stream,
        p_consumer_group,
        p_payload_sha256,
        p_payload_json,
        p_event_type,
        p_job_id,
        p_node_execution_id,
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at
    )
    ON CONFLICT (source_task_attestation_id) DO NOTHING;
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = p_attestation_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_emission.redis_stream IS DISTINCT FROM p_redis_stream
       OR v_emission.consumer_group IS DISTINCT FROM p_consumer_group
       OR v_emission.payload_sha256 IS DISTINCT FROM p_payload_sha256
       OR v_emission.payload_json IS DISTINCT FROM p_payload_json
       OR v_emission.event_type IS DISTINCT FROM p_event_type
       OR v_emission.job_id IS DISTINCT FROM p_job_id
       OR v_emission.node_execution_id IS DISTINCT FROM p_node_execution_id
       OR v_emission.worker_registration_id IS DISTINCT FROM p_registration_id
       OR v_emission.worker_lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_emission.worker_id IS DISTINCT FROM p_worker_id
       OR v_emission.worker_started_at IS DISTINCT FROM p_worker_started_at
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_emission_mismatch',
            ERRCODE = 'P0001';
    END IF;
    RETURN v_emission.id;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_mark_worker_event_emitted(
    p_emission_id uuid,
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_message_id text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_emission public.worker_event_emissions%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_emission_id IS NULL
       OR p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_message_id IS NULL
       OR length(trim(p_message_id)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id;
    IF NOT FOUND
       OR v_emission.worker_registration_id IS DISTINCT FROM p_registration_id
       OR v_emission.worker_lease_epoch IS DISTINCT FROM p_lease_epoch
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        v_emission.worker_id,
        v_emission.worker_started_at,
        v_emission.job_id,
        v_emission.node_execution_id
    );
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id
    FOR UPDATE;
    IF v_emission.emission_state = 'prepared' THEN
        UPDATE public.worker_event_emissions
        SET message_id = p_message_id,
            emission_state = 'emitted',
            emitted_at = v_now
        WHERE id = p_emission_id;
    ELSIF v_emission.message_id IS DISTINCT FROM p_message_id
          OR v_emission.emission_state NOT IN ('emitted', 'resolved')
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_list_worker_prepared_event_emissions(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_limit integer
)
RETURNS TABLE(emission_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_limit IS NULL
       OR p_limit < 1
       OR p_limit > 100
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
       OR v_registration.lease_expires_at <= v_now
       OR v_registration.database_principal IS DISTINCT FROM v_principal
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;

    RETURN QUERY
    SELECT emission.id
    FROM public.worker_event_emissions AS emission
    JOIN public.jobs AS job
      ON job.id = emission.job_id
    JOIN public.node_executions AS node
      ON node.id = emission.node_execution_id
     AND node.job_id = emission.job_id
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = emission.source_task_attestation_id
    JOIN public.worker_task_dispatches AS dispatch
      ON dispatch.dispatch_key = attestation.dispatch_key
    WHERE emission.worker_registration_id = p_registration_id
      AND emission.worker_lease_epoch = p_lease_epoch
      AND emission.emission_state = 'prepared'
      AND emission.message_id IS NULL
      AND job.status::text = 'RUNNING'
      AND node.status::text = 'RUNNING'
      AND node.worker_registration_id = p_registration_id
      AND node.worker_lease_epoch = p_lease_epoch
      AND node.worker_id = emission.worker_id
      AND node.started_at = emission.worker_started_at
      AND attestation.job_id = emission.job_id
      AND attestation.node_execution_id = emission.node_execution_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = emission.worker_id
      AND attestation.worker_started_at = emission.worker_started_at
      AND attestation.ack_state = 'pending'
      AND dispatch.job_id = emission.job_id
      AND dispatch.node_execution_id = emission.node_execution_id
      AND dispatch.redis_stream = attestation.redis_stream
      AND dispatch.consumer_group = attestation.consumer_group
      AND dispatch.redis_message_id = attestation.message_id
      AND dispatch.payload_sha256 = attestation.payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'unresolved'
    ORDER BY emission.prepared_at, emission.id
    LIMIT p_limit;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_load_worker_prepared_event_emission(
    p_emission_id uuid,
    p_registration_id uuid,
    p_lease_epoch bigint
)
RETURNS TABLE(
    emission_id uuid,
    source_task_attestation_id uuid,
    job_id uuid,
    node_execution_id uuid,
    worker_id text,
    worker_started_at timestamptz,
    redis_stream text,
    consumer_group text,
    payload_sha256 text,
    payload_json jsonb,
    event_type text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_emission public.worker_event_emissions%ROWTYPE;
BEGIN
    IF p_emission_id IS NULL
       OR p_registration_id IS NULL
       OR p_lease_epoch IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id;
    IF NOT FOUND
       OR v_emission.worker_registration_id IS DISTINCT FROM p_registration_id
       OR v_emission.worker_lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_emission.emission_state IS DISTINCT FROM 'prepared'
       OR v_emission.message_id IS NOT NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;

    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        v_emission.worker_id,
        v_emission.worker_started_at,
        v_emission.job_id,
        v_emission.node_execution_id
    );
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = v_emission.source_task_attestation_id
      AND attestation.job_id = v_emission.job_id
      AND attestation.node_execution_id = v_emission.node_execution_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = v_emission.worker_id
      AND attestation.worker_started_at = v_emission.worker_started_at
      AND attestation.ack_state = 'pending'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'task_delivery_attestation_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id
      AND emission.emission_state = 'prepared'
      AND emission.message_id IS NULL
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.dispatch_key = dispatch.dispatch_key
    WHERE attestation.id = v_emission.source_task_attestation_id
      AND dispatch.job_id = v_emission.job_id
      AND dispatch.node_execution_id = v_emission.node_execution_id
      AND dispatch.redis_stream = attestation.redis_stream
      AND dispatch.consumer_group = attestation.consumer_group
      AND dispatch.redis_message_id = attestation.message_id
      AND dispatch.payload_sha256 = attestation.payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'unresolved'
    FOR UPDATE OF dispatch;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'task_dispatch_mismatch', ERRCODE = 'P0001';
    END IF;

    RETURN QUERY SELECT
        v_emission.id,
        v_emission.source_task_attestation_id,
        v_emission.job_id,
        v_emission.node_execution_id,
        v_emission.worker_id::text,
        v_emission.worker_started_at,
        v_emission.redis_stream::text,
        v_emission.consumer_group::text,
        v_emission.payload_sha256::text,
        v_emission.payload_json,
        v_emission.event_type::text;
END;
$function$
"""
    )
    op.execute(
        """
CREATE FUNCTION public.vp_observe_worker_event_emission(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_attestation_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_emission public.worker_event_emissions%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_attestation_id IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM 1 FROM public.jobs AS job
    WHERE job.id = p_job_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'job_authority_changed', ERRCODE = 'P0001';
    END IF;
    PERFORM 1 FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id AND node.job_id = p_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    PERFORM 1 FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
      AND registration.lease_epoch = p_lease_epoch
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.job_id = p_job_id
      AND attestation.node_execution_id = p_node_execution_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'task_delivery_attestation_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = p_attestation_id
      AND emission.redis_stream = p_redis_stream
      AND emission.consumer_group = p_consumer_group
      AND emission.payload_sha256 = p_payload_sha256
      AND emission.job_id = p_job_id
      AND emission.node_execution_id = p_node_execution_id
      AND emission.worker_registration_id = p_registration_id
      AND emission.worker_lease_epoch = p_lease_epoch
      AND emission.worker_id = p_worker_id
      AND emission.worker_started_at = p_worker_started_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_missing', ERRCODE = 'P0001';
    END IF;
    IF v_emission.emission_state = 'prepared' THEN
        UPDATE public.worker_event_emissions
        SET message_id = p_message_id,
            emission_state = 'emitted',
            emitted_at = v_now
        WHERE id = v_emission.id;
    ELSIF v_emission.message_id IS DISTINCT FROM p_message_id THEN
        RAISE EXCEPTION USING MESSAGE = 'event_emission_mismatch', ERRCODE = 'P0001';
    END IF;
    RETURN p_attestation_id;
END;
$function$
"""
    )


def _create_worker_youtube_upload_functions() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_reserve_worker_youtube_upload(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_job_id uuid,
    p_node_execution_id uuid,
    p_input_artifact_id uuid,
    p_content_sha256 text,
    p_title text,
    p_privacy text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_operation_id uuid;
    v_production_task_id uuid;
BEGIN
    IF p_input_artifact_id IS NULL
       OR p_content_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_title IS NULL
       OR length(trim(p_title)) = 0
       OR p_privacy NOT IN ('private', 'unlisted')
    THEN
        RAISE EXCEPTION USING MESSAGE = 'upload_operation_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id, p_lease_epoch, p_worker_id,
        p_worker_started_at, p_job_id, p_node_execution_id
    );
    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id
      AND node.node_type = 'youtube_upload'
      AND node.input_artifact_ids = ARRAY[p_input_artifact_id]::uuid[];
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'upload_operation_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.artifacts AS artifact
    WHERE artifact.id = p_input_artifact_id
      AND artifact.job_id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'artifact_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT task.id
    INTO v_production_task_id
    FROM public.production_tasks AS task
    WHERE task.job_id = p_job_id
    ORDER BY task.id
    LIMIT 1;
    INSERT INTO public.youtube_upload_operations (
        production_task_id,
        job_id,
        node_execution_id,
        input_artifact_id,
        content_sha256,
        title,
        privacy,
        status,
        receipt_json
    ) VALUES (
        v_production_task_id,
        p_job_id,
        p_node_execution_id,
        p_input_artifact_id,
        p_content_sha256,
        p_title,
        p_privacy,
        'reserved',
        '{{}}'::jsonb
    )
    ON CONFLICT (node_execution_id) DO NOTHING;
    SELECT operation.id
    INTO v_operation_id
    FROM public.youtube_upload_operations AS operation
    WHERE operation.node_execution_id = p_node_execution_id
      AND operation.job_id = p_job_id
      AND operation.input_artifact_id = p_input_artifact_id
      AND operation.content_sha256 = p_content_sha256
      AND operation.title = p_title
      AND operation.privacy = p_privacy
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'upload_operation_mismatch', ERRCODE = 'P0001';
    END IF;
    RETURN v_operation_id;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_transition_worker_youtube_upload(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_operation_id uuid,
    p_expected_status text,
    p_transition text,
    p_manager_task_id text,
    p_platform_video_id text,
    p_receipt_json jsonb,
    p_error_message text
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_operation public.youtube_upload_operations%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
{_principal_guard_sql()}
    SELECT operation.*
    INTO v_operation
    FROM public.youtube_upload_operations AS operation
    WHERE operation.id = p_operation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'upload_operation_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id, p_lease_epoch, p_worker_id,
        p_worker_started_at, v_operation.job_id, v_operation.node_execution_id
    );
    SELECT operation.*
    INTO v_operation
    FROM public.youtube_upload_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF v_operation.status IS DISTINCT FROM p_expected_status THEN
        RAISE EXCEPTION USING MESSAGE = 'upload_transition_mismatch', ERRCODE = 'P0001';
    END IF;
    IF p_transition = 'attempting'
       AND p_expected_status = 'reserved'
       AND v_operation.request_attempted_at IS NULL
    THEN
        UPDATE public.youtube_upload_operations
        SET request_attempted_at = v_now, updated_at = v_now
        WHERE id = p_operation_id;
    ELSIF p_transition = 'fence'
          AND p_expected_status = 'reserved'
          AND v_operation.request_attempted_at IS NOT NULL
          AND v_operation.manager_task_id IS NULL
          AND p_manager_task_id IS NULL
          AND p_platform_video_id IS NULL
          AND p_receipt_json IS NULL
          AND p_error_message IS NULL
    THEN
        NULL;
    ELSIF p_transition = 'submitted'
          AND p_expected_status = 'reserved'
          AND p_manager_task_id IS NOT NULL
    THEN
        UPDATE public.youtube_upload_operations
        SET status = 'submitted',
            manager_task_id = p_manager_task_id,
            request_attempted_at = COALESCE(request_attempted_at, v_now),
            error_message = NULL,
            updated_at = v_now
        WHERE id = p_operation_id;
    ELSIF p_transition = 'succeeded'
          AND p_expected_status = 'submitted'
          AND p_platform_video_id IS NOT NULL
          AND p_receipt_json IS NOT NULL
    THEN
        UPDATE public.youtube_upload_operations
        SET status = 'succeeded',
            platform_video_id = p_platform_video_id,
            receipt_json = p_receipt_json,
            completed_at = v_now,
            error_message = NULL,
            updated_at = v_now
        WHERE id = p_operation_id;
    ELSIF p_transition IN ('failed', 'uncertain')
          AND p_expected_status IN ('reserved', 'submitted')
          AND p_error_message IS NOT NULL
    THEN
        UPDATE public.youtube_upload_operations
        SET status = p_transition,
            error_message = p_error_message,
            updated_at = v_now
        WHERE id = p_operation_id;
    ELSE
        RAISE EXCEPTION USING MESSAGE = 'upload_transition_mismatch', ERRCODE = 'P0001';
    END IF;
    RETURN p_operation_id;
END;
$function$
"""
    )


def _create_registered_node_recovery_function() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_recover_registered_worker_node(
    p_job_id uuid,
    p_node_execution_id uuid
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_registration_id uuid;
    v_lease_epoch bigint;
    v_registration public.worker_registrations%ROWTYPE;
    v_job_status text;
    v_node_status text;
    v_now timestamptz;
BEGIN
    IF p_job_id IS NULL OR p_node_execution_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT job.status::text
    INTO v_job_status
    FROM public.jobs AS job
    WHERE job.id = p_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'job_authority_changed', ERRCODE = 'P0001';
    END IF;

    SELECT
        node.worker_registration_id,
        node.worker_lease_epoch,
        node.status::text
    INTO v_registration_id, v_lease_epoch, v_node_status
    FROM public.node_executions AS node
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;
    IF v_job_status <> 'RUNNING'
       OR v_node_status NOT IN ('QUEUED', 'RUNNING')
    THEN
        RETURN 'terminal';
    END IF;
    IF v_registration_id IS NULL OR v_lease_epoch IS NULL THEN
        RETURN 'not_registered';
    END IF;

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = v_registration_id
    FOR UPDATE;
    v_now := pg_catalog.clock_timestamp();
    IF FOUND
       AND v_registration.lease_epoch = v_lease_epoch
       AND v_registration.status = 'active'
       AND v_registration.lease_expires_at > v_now
    THEN
        RETURN 'live';
    END IF;

    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.node_execution_id = p_node_execution_id
      AND dispatch.resolution_state IN ('unresolved', 'cancel_authorized')
    ORDER BY dispatch.id
    FOR UPDATE;
    IF FOUND THEN
        RETURN 'held_unresolved';
    END IF;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    WHERE emission.node_execution_id = p_node_execution_id
      AND emission.emission_state IN ('prepared', 'emitted')
    ORDER BY emission.id
    FOR UPDATE;
    IF FOUND THEN
        RETURN 'held_unresolved_event';
    END IF;

    UPDATE public.node_executions AS node
    SET status = 'PENDING',
        worker_id = NULL,
        worker_registration_id = NULL,
        worker_lease_epoch = NULL,
        queued_at = NULL,
        started_at = NULL,
        completed_at = NULL,
        progress = 0,
        error_message = NULL,
        error_trace = NULL,
        input_artifact_ids = ARRAY[]::uuid[],
        output_artifact_id = NULL,
        retry_count = node.retry_count
    WHERE node.id = p_node_execution_id
      AND node.job_id = p_job_id
      AND node.worker_registration_id = v_registration_id
      AND node.worker_lease_epoch = v_lease_epoch;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;
    RETURN 'recovered';
END;
$function$
"""
    )


def _create_margin_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_require_worker_lease_margin(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_minimum_margin_seconds integer
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_minimum_margin_seconds IS NULL
       OR p_minimum_margin_seconds <= 0
       OR p_minimum_margin_seconds > 3600
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;

    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at
       < v_now + pg_catalog.make_interval(
            secs => p_minimum_margin_seconds
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_margin_insufficient',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )


def _create_task_ack_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_require_worker_task_ack_receipt(
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
BEGIN
    IF p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_dispatch_key IS NULL
       OR length(trim(p_worker_id)) = 0
       OR length(trim(p_redis_stream)) = 0
       OR length(trim(p_consumer_group)) = 0
       OR length(trim(p_message_id)) = 0
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
      AND attestation.payload_sha256 = p_payload_sha256
      AND attestation.dispatch_key = p_dispatch_key;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_receipt_missing',
            ERRCODE = 'P0001';
    END IF;
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        v_attestation.job_id,
        v_attestation.node_execution_id
    );

    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = v_attestation.id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
      AND attestation.payload_sha256 = p_payload_sha256
      AND attestation.dispatch_key = p_dispatch_key
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_receipt_missing',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = v_attestation.id
    ORDER BY emission.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.source_task_attestation_id = v_attestation.id
    ORDER BY receipt.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    WHERE delivery.source_task_attestation_id = v_attestation.id
    ORDER BY delivery.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = p_dispatch_key
      AND dispatch.job_id = v_attestation.job_id
      AND dispatch.node_execution_id = v_attestation.node_execution_id
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.redis_message_id = p_message_id
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.id = v_attestation.id
          AND (
              (
                  attestation.ack_state = 'authorized'
                  AND attestation.ack_event_emission_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM public.worker_event_emissions AS emission
                      WHERE emission.id = attestation.ack_event_emission_id
                        AND emission.source_task_attestation_id = attestation.id
                        AND emission.emission_state IN ('emitted', 'resolved')
                  )
              )
              OR EXISTS (
                  SELECT 1
                  FROM public.registered_worker_event_receipts AS receipt
                  WHERE receipt.source_task_attestation_id = attestation.id
                    AND receipt.application_state = 'applied'
              )
          )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_receipt_missing',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_authorize_worker_task_ack(
    p_attestation_id uuid,
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
    v_emission public.worker_event_emissions%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_attestation_id IS NULL
       OR p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        v_attestation.job_id,
        v_attestation.node_execution_id
    );
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
       OR v_registration.status IS DISTINCT FROM 'active'
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_expired', ERRCODE = 'P0001';
    END IF;

    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = p_attestation_id
      AND emission.worker_registration_id = p_registration_id
      AND emission.worker_lease_epoch = p_lease_epoch
      AND emission.worker_id = p_worker_id
      AND emission.worker_started_at = p_worker_started_at
      AND emission.emission_state IN ('emitted', 'resolved')
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_emission_missing',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.source_task_attestation_id = p_attestation_id
    ORDER BY receipt.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    WHERE delivery.source_task_attestation_id = p_attestation_id
    ORDER BY delivery.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = v_attestation.dispatch_key
      AND dispatch.job_id = v_attestation.job_id
      AND dispatch.node_execution_id = v_attestation.node_execution_id
      AND dispatch.redis_stream = v_attestation.redis_stream
      AND dispatch.consumer_group = v_attestation.consumer_group
      AND dispatch.redis_message_id = v_attestation.message_id
      AND dispatch.payload_sha256 = v_attestation.payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;
    UPDATE public.worker_task_delivery_attestations AS attestation
    SET ack_state = 'authorized',
        ack_event_emission_id = v_emission.id
    WHERE attestation.id = p_attestation_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.ack_state = 'pending';
    IF NOT FOUND AND NOT EXISTS (
        SELECT 1
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.id = p_attestation_id
          AND attestation.worker_registration_id = p_registration_id
          AND attestation.worker_lease_epoch = p_lease_epoch
          AND attestation.worker_id = p_worker_id
          AND attestation.worker_started_at = p_worker_started_at
          AND attestation.ack_state IN ('authorized', 'acknowledged')
          AND attestation.ack_event_emission_id = v_emission.id
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_acknowledge_worker_task_delivery(
    p_attestation_id uuid,
    p_registration_id uuid,
    p_lease_epoch bigint,
    p_worker_id text,
    p_worker_started_at timestamptz,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_attestation_id IS NULL
       OR p_registration_id IS NULL
       OR p_lease_epoch IS NULL
       OR p_worker_id IS NULL
       OR p_worker_started_at IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_dispatch_key IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
      AND attestation.payload_sha256 = p_payload_sha256
      AND attestation.dispatch_key = p_dispatch_key;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM public.vp_require_worker_node_claim(
        p_registration_id,
        p_lease_epoch,
        p_worker_id,
        p_worker_started_at,
        v_attestation.job_id,
        v_attestation.node_execution_id
    );
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || p_registration_id::text,
            0
        )
    );
    v_now := pg_catalog.clock_timestamp();

    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id;
    IF NOT FOUND
       OR v_registration.lease_epoch IS DISTINCT FROM p_lease_epoch
    THEN
        RAISE EXCEPTION USING MESSAGE = 'lease_fenced', ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal IS DISTINCT FROM v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;

    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.worker_registration_id = p_registration_id
      AND attestation.worker_lease_epoch = p_lease_epoch
      AND attestation.worker_id = p_worker_id
      AND attestation.worker_started_at = p_worker_started_at
      AND attestation.redis_stream = p_redis_stream
      AND attestation.consumer_group = p_consumer_group
      AND attestation.message_id = p_message_id
      AND attestation.payload_sha256 = p_payload_sha256
      AND attestation.dispatch_key = p_dispatch_key
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = p_attestation_id
    ORDER BY emission.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.source_task_attestation_id = p_attestation_id
    ORDER BY receipt.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    WHERE delivery.source_task_attestation_id = p_attestation_id
    ORDER BY delivery.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = p_dispatch_key
      AND dispatch.job_id = v_attestation.job_id
      AND dispatch.node_execution_id = v_attestation.node_execution_id
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.redis_message_id = p_message_id
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF NOT (
        (
            v_attestation.ack_state = 'authorized'
            AND v_attestation.ack_event_emission_id IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM public.worker_event_emissions AS emission
                WHERE emission.id = v_attestation.ack_event_emission_id
                  AND emission.source_task_attestation_id = p_attestation_id
                  AND emission.emission_state IN ('emitted', 'resolved')
            )
        )
        OR EXISTS (
            SELECT 1
            FROM public.registered_worker_event_receipts AS receipt
            WHERE receipt.source_task_attestation_id = p_attestation_id
              AND receipt.application_state = 'applied'
        )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_ack_authority_missing',
            ERRCODE = 'P0001';
    END IF;

    UPDATE public.worker_task_delivery_attestations
    SET ack_state = 'acknowledged',
        acknowledged_at = v_now
    WHERE id = p_attestation_id
      AND ack_state IN ('pending', 'authorized');

    UPDATE public.worker_task_dispatches AS dispatch
    SET resolution_state = 'acknowledged',
        acknowledged_at = COALESCE(dispatch.acknowledged_at, v_now)
    WHERE dispatch.dispatch_key = p_dispatch_key
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.redis_message_id = p_message_id
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      );
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )


def _create_proven_task_acknowledge_function() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_acknowledge_proven_worker_task_dispatch(
    p_attestation_id uuid
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_job_id uuid;
    v_node_execution_id uuid;
    v_registration_id uuid;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_attestation_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;

    SELECT
        attestation.job_id,
        attestation.node_execution_id,
        attestation.worker_registration_id
    INTO v_job_id, v_node_execution_id, v_registration_id
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.id = v_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'job_authority_changed',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = v_node_execution_id
      AND node.job_id = v_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'node_claim_mismatch',
            ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:' || v_registration_id::text,
            0
        )
    );

    SELECT attestation.*
    INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = p_attestation_id
      AND attestation.job_id = v_job_id
      AND attestation.node_execution_id = v_node_execution_id
      AND attestation.worker_registration_id = v_registration_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_delivery_attestation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = p_attestation_id
    ORDER BY emission.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.source_task_attestation_id = p_attestation_id
    ORDER BY receipt.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    WHERE delivery.source_task_attestation_id = p_attestation_id
    ORDER BY delivery.id
    FOR UPDATE;
    IF NOT (
        (
            v_attestation.ack_state IN ('authorized', 'acknowledged')
            AND v_attestation.ack_event_emission_id IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM public.worker_event_emissions AS emission
                WHERE emission.id = v_attestation.ack_event_emission_id
                  AND emission.source_task_attestation_id = p_attestation_id
                  AND emission.emission_state IN ('emitted', 'resolved')
            )
        )
        OR EXISTS (
            SELECT 1
            FROM public.registered_worker_event_receipts AS receipt
            WHERE receipt.source_task_attestation_id = p_attestation_id
              AND receipt.application_state = 'applied'
        )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_ack_authority_missing',
            ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = v_attestation.dispatch_key
      AND dispatch.redis_stream = v_attestation.redis_stream
      AND dispatch.consumer_group = v_attestation.consumer_group
      AND dispatch.redis_message_id = v_attestation.message_id
      AND dispatch.payload_sha256 = v_attestation.payload_sha256
      AND dispatch.job_id = v_attestation.job_id
      AND dispatch.node_execution_id = v_attestation.node_execution_id
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'task_dispatch_mismatch',
            ERRCODE = 'P0001';
    END IF;

    UPDATE public.worker_task_delivery_attestations
    SET ack_state = 'acknowledged',
        acknowledged_at = COALESCE(acknowledged_at, v_now)
    WHERE id = p_attestation_id
      AND ack_state IN ('pending', 'authorized');

    UPDATE public.worker_task_dispatches
    SET resolution_state = 'acknowledged',
        acknowledged_at = COALESCE(acknowledged_at, v_now)
    WHERE dispatch_key = v_attestation.dispatch_key
      AND redis_stream = v_attestation.redis_stream
      AND consumer_group = v_attestation.consumer_group
      AND redis_message_id = v_attestation.message_id
      AND payload_sha256 = v_attestation.payload_sha256
      AND job_id = v_attestation.job_id
      AND node_execution_id = v_attestation.node_execution_id
      AND delivery_state = 'delivered'
      AND resolution_state IN (
          'unresolved',
          'cancel_authorized',
          'acknowledged'
      );
    RETURN (
        SELECT attestation.acknowledged_at
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.id = p_attestation_id
    );
END;
$function$
"""
    )


def _create_cancelled_task_functions() -> None:
    op.execute(
        """
CREATE FUNCTION public.vp_authorize_cancelled_worker_task_ack(
    p_dispatch_id uuid
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_job_id uuid;
    v_node_execution_id uuid;
    v_dispatch_key uuid;
    v_registration_id uuid;
    v_state text;
    v_resolution text;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_dispatch_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT
        dispatch.job_id,
        dispatch.node_execution_id,
        dispatch.dispatch_key
    INTO v_job_id, v_node_execution_id, v_dispatch_key
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.id = p_dispatch_id;
    IF NOT FOUND THEN
        RETURN 'missing';
    END IF;
    PERFORM 1 FROM public.jobs AS job
    WHERE job.id = v_job_id AND job.status::text = 'CANCELLED'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'job_authority_changed', ERRCODE = 'P0001';
    END IF;
    PERFORM 1 FROM public.node_executions AS node
    WHERE node.id = v_node_execution_id
      AND node.job_id = v_job_id
      AND node.status::text = 'CANCELLED'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;
    FOR v_registration_id IN
        SELECT DISTINCT attestation.worker_registration_id
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.dispatch_key = v_dispatch_key
        ORDER BY attestation.worker_registration_id
    LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock_shared(
            pg_catalog.hashtextextended(
                'vp-worker-registration:' || v_registration_id::text,
                0
            )
        );
        PERFORM 1
        FROM public.worker_registrations AS registration
        WHERE registration.id = v_registration_id
        FOR SHARE;
    END LOOP;
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.dispatch_key = v_dispatch_key
    ORDER BY attestation.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = emission.source_task_attestation_id
    WHERE attestation.dispatch_key = v_dispatch_key
    ORDER BY emission.id
    FOR UPDATE OF emission;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = receipt.source_task_attestation_id
    WHERE attestation.dispatch_key = v_dispatch_key
    ORDER BY receipt.id
    FOR UPDATE OF receipt;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = delivery.source_task_attestation_id
    WHERE attestation.dispatch_key = v_dispatch_key
    ORDER BY delivery.id
    FOR UPDATE OF delivery;
    IF EXISTS (
        SELECT 1
        FROM public.worker_event_emissions AS emission
        JOIN public.worker_task_delivery_attestations AS attestation
          ON attestation.id = emission.source_task_attestation_id
        WHERE attestation.dispatch_key = v_dispatch_key
    ) THEN
        RETURN 'held_event';
    END IF;
    SELECT dispatch.delivery_state, dispatch.resolution_state
    INTO v_state, v_resolution
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.id = p_dispatch_id
    FOR UPDATE;

    IF v_state = 'pending' AND v_resolution = 'unresolved' THEN
        UPDATE public.worker_task_dispatches
        SET delivery_state = 'cancelled',
            resolution_state = 'cancelled',
            cancelled_at = v_now
        WHERE id = p_dispatch_id;
        RETURN 'cancelled';
    END IF;
    IF v_state = 'delivered'
       AND v_resolution IN ('unresolved', 'cancel_authorized')
    THEN
        UPDATE public.worker_task_dispatches
        SET resolution_state = 'cancel_authorized'
        WHERE id = p_dispatch_id
          AND resolution_state = 'unresolved';
        RETURN 'cancel_authorized';
    END IF;
    IF v_resolution IN ('acknowledged', 'cancelled') THEN
        RETURN v_resolution;
    END IF;
    RETURN 'held';
END;
$function$
"""
    )
    for function_name in (
        "vp_require_cancelled_worker_task_ack",
        "vp_acknowledge_cancelled_worker_task",
    ):
        returns_void = function_name.startswith("vp_acknowledge")
        mutation_sql = (
            """
    UPDATE public.worker_task_dispatches
    SET resolution_state = 'acknowledged',
        acknowledged_at = COALESCE(acknowledged_at, v_now)
    WHERE id = p_dispatch_id
      AND resolution_state = 'cancel_authorized';
    UPDATE public.worker_task_delivery_attestations
    SET ack_state = 'acknowledged',
        acknowledged_at = COALESCE(acknowledged_at, v_now)
    WHERE dispatch_key = p_dispatch_key
      AND redis_stream = p_redis_stream
      AND consumer_group = p_consumer_group
      AND message_id = p_message_id
      AND payload_sha256 = p_payload_sha256
      AND ack_state IN ('pending', 'authorized');
"""
            if returns_void
            else ""
        )
        op.execute(
            f"""
CREATE FUNCTION public.{function_name}(
    p_dispatch_id uuid,
    p_redis_stream text,
    p_consumer_group text,
    p_message_id text,
    p_payload_sha256 text,
    p_dispatch_key uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_job_id uuid;
    v_node_execution_id uuid;
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_registration_id uuid;
    v_actual_dispatch_key uuid;
BEGIN
    IF p_dispatch_id IS NULL
       OR p_redis_stream IS NULL
       OR p_consumer_group IS NULL
       OR p_message_id IS NULL
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
       OR p_dispatch_key IS NULL
    THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
    SELECT
        dispatch.job_id,
        dispatch.node_execution_id,
        dispatch.dispatch_key
    INTO v_job_id, v_node_execution_id, v_actual_dispatch_key
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.id = p_dispatch_id;
    IF NOT FOUND OR v_actual_dispatch_key IS DISTINCT FROM p_dispatch_key THEN
        RAISE EXCEPTION USING MESSAGE = 'task_dispatch_mismatch', ERRCODE = 'P0001';
    END IF;
    PERFORM 1 FROM public.jobs AS job
    WHERE job.id = v_job_id AND job.status::text = 'CANCELLED'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'job_authority_changed', ERRCODE = 'P0001';
    END IF;
    PERFORM 1 FROM public.node_executions AS node
    WHERE node.id = v_node_execution_id
      AND node.job_id = v_job_id
      AND node.status::text = 'CANCELLED'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'node_claim_mismatch', ERRCODE = 'P0001';
    END IF;
    FOR v_registration_id IN
        SELECT DISTINCT attestation.worker_registration_id
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.dispatch_key = p_dispatch_key
        ORDER BY attestation.worker_registration_id
    LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock_shared(
            pg_catalog.hashtextextended(
                'vp-worker-registration:' || v_registration_id::text,
                0
            )
        );
        PERFORM 1
        FROM public.worker_registrations AS registration
        WHERE registration.id = v_registration_id
        FOR SHARE;
    END LOOP;
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.dispatch_key = p_dispatch_key
    ORDER BY attestation.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = emission.source_task_attestation_id
    WHERE attestation.dispatch_key = p_dispatch_key
    ORDER BY emission.id
    FOR UPDATE OF emission;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = receipt.source_task_attestation_id
    WHERE attestation.dispatch_key = p_dispatch_key
    ORDER BY receipt.id
    FOR UPDATE OF receipt;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = delivery.source_task_attestation_id
    WHERE attestation.dispatch_key = p_dispatch_key
    ORDER BY delivery.id
    FOR UPDATE OF delivery;
    IF EXISTS (
        SELECT 1
        FROM public.worker_event_emissions AS emission
        JOIN public.worker_task_delivery_attestations AS attestation
          ON attestation.id = emission.source_task_attestation_id
        WHERE attestation.dispatch_key = p_dispatch_key
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'event_emission_unresolved',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.id = p_dispatch_id
      AND dispatch.dispatch_key = p_dispatch_key
      AND dispatch.redis_stream = p_redis_stream
      AND dispatch.consumer_group = p_consumer_group
      AND dispatch.redis_message_id = p_message_id
      AND dispatch.payload_sha256 = p_payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'cancel_authorized'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'task_dispatch_mismatch', ERRCODE = 'P0001';
    END IF;
{mutation_sql}
END;
$function$
"""
        )


def _create_event_authority_cleanup_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_resolve_worker_event_authority_for_job_deletion(
    p_job_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_registration_id uuid;
BEGIN
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'claim_mismatch', ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql(ORCHESTRATOR_CONTROL_ROLE)}

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.id = p_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.jobs AS job
        WHERE job.id = p_job_id
          AND job.status::text NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_event_authority_unresolved',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.job_id = p_job_id
    ORDER BY node.id
    FOR UPDATE;

    FOR v_registration_id IN
        SELECT DISTINCT attestation.worker_registration_id
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.job_id = p_job_id
        ORDER BY attestation.worker_registration_id
    LOOP
        PERFORM pg_catalog.pg_advisory_xact_lock_shared(
            pg_catalog.hashtextextended(
                'vp-worker-registration:' || v_registration_id::text,
                0
            )
        );
        PERFORM 1
        FROM public.worker_registrations AS registration
        WHERE registration.id = v_registration_id
        FOR SHARE;
    END LOOP;

    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.job_id = p_job_id
    ORDER BY attestation.id
    FOR UPDATE;
    PERFORM 1
    FROM public.worker_event_emissions AS emission
    WHERE emission.job_id = p_job_id
    ORDER BY emission.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.job_id = p_job_id
    ORDER BY receipt.id
    FOR UPDATE;
    PERFORM 1
    FROM public.registered_worker_event_deliveries AS delivery
    JOIN public.worker_task_delivery_attestations AS attestation
      ON attestation.id = delivery.source_task_attestation_id
    WHERE attestation.job_id = p_job_id
    ORDER BY delivery.id
    FOR UPDATE OF delivery;
    PERFORM 1
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.job_id = p_job_id
    ORDER BY dispatch.id
    FOR UPDATE;

    IF EXISTS (
        SELECT 1
        FROM public.worker_task_delivery_attestations AS attestation
        WHERE attestation.job_id = p_job_id
          AND attestation.ack_state <> 'acknowledged'
    )
       OR EXISTS (
        SELECT 1
        FROM public.worker_event_emissions AS emission
        WHERE emission.job_id = p_job_id
          AND emission.emission_state <> 'resolved'
    )
       OR EXISTS (
        SELECT 1
        FROM public.registered_worker_event_receipts AS receipt
        WHERE receipt.job_id = p_job_id
          AND (
              receipt.application_state <> 'applied'
              OR receipt.ack_state <> 'acknowledged'
          )
    )
       OR EXISTS (
        SELECT 1
        FROM public.registered_worker_event_deliveries AS delivery
        JOIN public.worker_task_delivery_attestations AS attestation
          ON attestation.id = delivery.source_task_attestation_id
        WHERE attestation.job_id = p_job_id
          AND delivery.ack_state <> 'acknowledged'
    )
       OR EXISTS (
        SELECT 1
        FROM public.worker_task_dispatches AS dispatch
        WHERE dispatch.job_id = p_job_id
          AND NOT (
              dispatch.resolution_state = 'acknowledged'
              OR (
                  dispatch.delivery_state = 'cancelled'
                  AND dispatch.resolution_state = 'cancelled'
              )
          )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_event_authority_unresolved',
            ERRCODE = 'P0001';
    END IF;

    -- Cleanup authorization is always last in the global authority order.
    PERFORM 1
    FROM public.worker_redis_marker_cleanup_authorizations AS cleanup
    WHERE (
        cleanup.marker_kind = 'event_emission'
        AND cleanup.source_id IN (
            SELECT emission.id
            FROM public.worker_event_emissions AS emission
            WHERE emission.job_id = p_job_id
        )
    )
       OR (
        cleanup.marker_kind = 'task_dispatch'
        AND cleanup.source_id IN (
            SELECT dispatch.id
            FROM public.worker_task_dispatches AS dispatch
            WHERE dispatch.job_id = p_job_id
              AND dispatch.delivery_state = 'delivered'
              AND dispatch.resolution_state = 'acknowledged'
        )
    )
    ORDER BY cleanup.marker_key
    FOR UPDATE;

    INSERT INTO public.worker_redis_marker_cleanup_authorizations (
        marker_kind,
        source_id,
        marker_key,
        redis_stream,
        expected_message_id,
        payload_sha256,
        authorization_state,
        authorized_at
    )
    SELECT
        'event_emission',
        emission.id,
        'vp:worker-event-emission:' || emission.id::text,
        emission.redis_stream,
        emission.message_id,
        emission.payload_sha256,
        'pending',
        pg_catalog.clock_timestamp()
    FROM public.worker_event_emissions AS emission
    WHERE emission.job_id = p_job_id
      AND emission.emission_state = 'resolved'
    ORDER BY 'vp:worker-event-emission:' || emission.id::text
    ON CONFLICT DO NOTHING;

    INSERT INTO public.worker_redis_marker_cleanup_authorizations (
        marker_kind,
        source_id,
        marker_key,
        redis_stream,
        expected_message_id,
        payload_sha256,
        authorization_state,
        authorized_at
    )
    SELECT
        'task_dispatch',
        dispatch.id,
        'vp:worker-task-dispatch:' || dispatch.dispatch_key::text,
        dispatch.redis_stream,
        dispatch.redis_message_id,
        dispatch.payload_sha256,
        'pending',
        pg_catalog.clock_timestamp()
    FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.job_id = p_job_id
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'acknowledged'
    ORDER BY 'vp:worker-task-dispatch:' || dispatch.dispatch_key::text
    ON CONFLICT DO NOTHING;

    IF EXISTS (
        SELECT 1
        FROM public.worker_event_emissions AS emission
        LEFT JOIN public.worker_redis_marker_cleanup_authorizations
            AS cleanup
          ON cleanup.marker_kind = 'event_emission'
         AND cleanup.source_id = emission.id
        WHERE emission.job_id = p_job_id
          AND (
              cleanup.id IS NULL
              OR cleanup.marker_key IS DISTINCT FROM
                    'vp:worker-event-emission:' || emission.id::text
              OR cleanup.redis_stream
                    IS DISTINCT FROM emission.redis_stream
              OR cleanup.expected_message_id
                    IS DISTINCT FROM emission.message_id
              OR cleanup.payload_sha256
                    IS DISTINCT FROM emission.payload_sha256
              OR cleanup.authorized_at IS NULL
          )
    )
       OR EXISTS (
        SELECT 1
        FROM public.worker_task_dispatches AS dispatch
        LEFT JOIN public.worker_redis_marker_cleanup_authorizations
            AS cleanup
          ON cleanup.marker_kind = 'task_dispatch'
         AND cleanup.source_id = dispatch.id
        WHERE dispatch.job_id = p_job_id
          AND dispatch.delivery_state = 'delivered'
          AND dispatch.resolution_state = 'acknowledged'
          AND (
              cleanup.id IS NULL
              OR cleanup.marker_key IS DISTINCT FROM
                    'vp:worker-task-dispatch:' || dispatch.dispatch_key::text
              OR cleanup.redis_stream
                    IS DISTINCT FROM dispatch.redis_stream
              OR cleanup.expected_message_id
                    IS DISTINCT FROM dispatch.redis_message_id
              OR cleanup.payload_sha256
                    IS DISTINCT FROM dispatch.payload_sha256
              OR cleanup.authorized_at IS NULL
          )
    )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;

    DELETE FROM public.registered_worker_event_deliveries AS delivery
    USING public.worker_task_delivery_attestations AS attestation
    WHERE delivery.source_task_attestation_id = attestation.id
      AND attestation.job_id = p_job_id;

    DELETE FROM public.worker_task_dispatches
    WHERE job_id = p_job_id;

    DELETE FROM public.registered_worker_event_receipts
    WHERE job_id = p_job_id;

    DELETE FROM public.worker_event_emissions
    WHERE job_id = p_job_id;

    DELETE FROM public.worker_task_delivery_attestations
    WHERE job_id = p_job_id;
END;
$function$
"""
    )


def _create_worker_redis_marker_functions() -> None:
    op.execute(
        f"""
CREATE FUNCTION public.vp_list_worker_redis_marker_expectations(
    p_after_key text,
    p_limit integer
)
RETURNS TABLE(
    marker_kind text,
    source_id uuid,
    marker_key text,
    redis_stream text,
    expected_message_id text,
    payload_sha256 text,
    source_state text,
    absence_allowed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_after_key IS NULL
       OR p_limit IS NULL
       OR p_limit < 1
       OR p_limit > 500
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_expectation_request_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_readiness_runtime")}
    v_now := pg_catalog.clock_timestamp();
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton;
    IF NOT FOUND OR v_status.state <> 'running' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_status.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_expired',
            ERRCODE = 'P0001';
    END IF;
    RETURN QUERY
    SELECT
        expectation.marker_kind::text,
        expectation.source_id,
        expectation.marker_key::text,
        expectation.redis_stream::text,
        expectation.expected_message_id::text,
        expectation.payload_sha256::text,
        expectation.source_state::text,
        expectation.absence_allowed
    FROM public.worker_redis_continuity_expectations AS expectation
    WHERE expectation.run_id = v_status.run_id
      AND expectation.marker_key > p_after_key
    ORDER BY expectation.marker_key
    LIMIT p_limit;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_begin_worker_redis_continuity_check(
    p_run_id uuid,
    p_stale_run_seconds integer
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
    v_expected_count bigint;
BEGIN
    IF p_run_id IS NULL
       OR p_stale_run_seconds IS DISTINCT FROM 300
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_request_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_readiness_runtime")}
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            'vp-worker-redis-continuity',
            0
        )
    );
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton
    FOR UPDATE;
    IF FOUND
       AND v_status.state = 'running'
       AND v_status.lease_expires_at > v_now
    THEN
        RETURN 'overlap';
    END IF;
    DELETE FROM public.worker_redis_continuity_expectations;
    INSERT INTO public.worker_redis_continuity_expectations (
        run_id,
        marker_kind,
        source_id,
        marker_key,
        redis_stream,
        expected_message_id,
        payload_sha256,
        source_state,
        absence_allowed
    )
    WITH candidates AS (
        SELECT
            2 AS priority,
            'event_emission'::text AS marker_kind,
            emission.id AS source_id,
            (
                'vp:worker-event-emission:' || emission.id::text
            )::text AS marker_key,
            emission.redis_stream::text AS redis_stream,
            emission.message_id::text AS expected_message_id,
            emission.payload_sha256::text AS payload_sha256,
            emission.emission_state::text AS source_state,
            (
                emission.emission_state = 'prepared'
                AND emission.message_id IS NULL
            ) AS absence_allowed
        FROM public.worker_event_emissions AS emission
        UNION ALL
        SELECT
            2,
            'task_dispatch'::text,
            dispatch.id,
            (
                'vp:worker-task-dispatch:'
                || dispatch.dispatch_key::text
            )::text,
            dispatch.redis_stream::text,
            dispatch.redis_message_id::text,
            dispatch.payload_sha256::text,
            (
                'delivery:' || dispatch.delivery_state
                || '/resolution:' || dispatch.resolution_state
            )::text,
            (
                dispatch.redis_message_id IS NULL
                AND dispatch.delivery_state IN ('pending', 'cancelled')
            )
        FROM public.worker_task_dispatches AS dispatch
        UNION ALL
        SELECT
            1,
            cleanup.marker_kind::text,
            cleanup.source_id,
            cleanup.marker_key::text,
            cleanup.redis_stream::text,
            cleanup.expected_message_id::text,
            cleanup.payload_sha256::text,
            cleanup.authorization_state::text,
            cleanup.authorization_state IN ('deleted', 'absent')
        FROM public.worker_redis_marker_cleanup_authorizations
            AS cleanup
    ),
    snapshot AS (
        SELECT DISTINCT ON (candidate.marker_key)
            candidate.marker_kind,
            candidate.source_id,
            candidate.marker_key,
            candidate.redis_stream,
            candidate.expected_message_id,
            candidate.payload_sha256,
            candidate.source_state,
            candidate.absence_allowed
        FROM candidates AS candidate
        ORDER BY candidate.marker_key, candidate.priority
    )
    SELECT
        p_run_id,
        snapshot.marker_kind,
        snapshot.source_id,
        snapshot.marker_key,
        snapshot.redis_stream,
        snapshot.expected_message_id,
        snapshot.payload_sha256,
        snapshot.source_state,
        snapshot.absence_allowed
    FROM snapshot
    ORDER BY snapshot.marker_key;
    GET DIAGNOSTICS v_expected_count = ROW_COUNT;
    IF v_expected_count > 100000 THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_expectation_limit_exceeded',
            ERRCODE = 'P0001';
    END IF;
    INSERT INTO public.worker_redis_continuity_status (
        singleton,
        run_id,
        state,
        reason_code,
        redis_run_id,
        expected_count,
        checked_count,
        started_at,
        lease_expires_at,
        finished_at
    ) VALUES (
        true,
        p_run_id,
        'running',
        'continuity_check_running',
        NULL,
        v_expected_count,
        0,
        v_now,
        v_now + interval '300 seconds',
        NULL
    )
    ON CONFLICT (singleton) DO UPDATE
    SET run_id = EXCLUDED.run_id,
        state = EXCLUDED.state,
        reason_code = EXCLUDED.reason_code,
        redis_run_id = EXCLUDED.redis_run_id,
        expected_count = EXCLUDED.expected_count,
        checked_count = EXCLUDED.checked_count,
        started_at = EXCLUDED.started_at,
        lease_expires_at = EXCLUDED.lease_expires_at,
        finished_at = EXCLUDED.finished_at;
    RETURN 'begun';
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_finish_worker_redis_continuity_check(
    p_run_id uuid,
    p_result text,
    p_reason_code text,
    p_redis_run_id text,
    p_expected_count bigint,
    p_checked_count bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_run_id IS NULL
       OR p_result NOT IN ('ready', 'error')
       OR p_reason_code IS NULL
       OR p_reason_code !~ '^[a-z][a-z0-9_]{{0,63}}$'
       OR p_expected_count IS NULL
       OR p_expected_count < 0
       OR p_checked_count IS NULL
       OR p_checked_count < 0
       OR (
            p_result = 'ready'
            AND (
                p_reason_code <> 'ready'
                OR p_redis_run_id IS NULL
                OR length(p_redis_run_id) NOT BETWEEN 1 AND 255
                OR p_redis_run_id IS DISTINCT FROM trim(p_redis_run_id)
                OR p_checked_count <> p_expected_count
            )
       )
       OR (
            p_result = 'error'
            AND p_reason_code = 'ready'
       )
       OR (
            p_redis_run_id IS NOT NULL
            AND (
                length(p_redis_run_id) NOT BETWEEN 1 AND 255
                OR p_redis_run_id IS DISTINCT FROM trim(p_redis_run_id)
            )
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_result_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_readiness_runtime")}
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton
    FOR UPDATE;
    IF NOT FOUND
       OR v_status.run_id IS DISTINCT FROM p_run_id
       OR v_status.state <> 'running'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_mismatch',
            ERRCODE = 'P0001';
    END IF;
    v_now := pg_catalog.clock_timestamp();
    IF (
            p_result = 'ready'
            AND p_expected_count IS DISTINCT FROM v_status.expected_count
       )
       OR (
            p_result = 'error'
            AND p_checked_count > v_status.expected_count
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_result_invalid',
            ERRCODE = 'P0001';
    END IF;
    IF p_result = 'ready'
       AND EXISTS (
            SELECT 1
            FROM public.worker_redis_continuity_expectations AS expectation
            JOIN public.worker_redis_marker_cleanup_authorizations AS cleanup
              ON cleanup.marker_kind = expectation.marker_kind
             AND cleanup.source_id = expectation.source_id
             AND cleanup.marker_key = expectation.marker_key
             AND cleanup.redis_stream = expectation.redis_stream
             AND cleanup.expected_message_id =
                    expectation.expected_message_id
             AND cleanup.payload_sha256 = expectation.payload_sha256
            WHERE expectation.run_id = p_run_id
              AND expectation.observed_message_id IS NOT NULL
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_result_invalid',
            ERRCODE = 'P0001';
    END IF;
    IF p_result = 'ready' AND v_status.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_expired',
            ERRCODE = 'P0001';
    END IF;
    UPDATE public.worker_redis_continuity_status
    SET state = p_result,
        reason_code = p_reason_code,
        redis_run_id = p_redis_run_id,
        expected_count = v_status.expected_count,
        checked_count = p_checked_count,
        finished_at = v_now
    WHERE singleton;
    RETURN p_result = 'ready';
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_record_worker_redis_marker_observation(
    p_run_id uuid,
    p_marker_kind text,
    p_source_id uuid,
    p_message_id text,
    p_payload_sha256 text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_source_exists boolean;
    v_cleanup_exists boolean;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_expectation public.worker_redis_continuity_expectations%ROWTYPE;
    v_cleanup
        public.worker_redis_marker_cleanup_authorizations%ROWTYPE;
    v_authorization public.worker_redis_marker_repair_audits%ROWTYPE;
    v_restore_authorized boolean := false;
    v_now timestamptz;
BEGIN
    IF p_run_id IS NULL
       OR p_marker_kind NOT IN ('event_emission', 'task_dispatch')
       OR p_source_id IS NULL
       OR p_message_id IS NULL
       OR length(p_message_id) NOT BETWEEN 3 AND 64
       OR p_message_id !~ '^[0-9]+-[0-9]+$'
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_observation_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_readiness_runtime")}
    IF p_marker_kind = 'event_emission' THEN
        PERFORM 1
        FROM public.worker_event_emissions AS emission
        WHERE emission.id = p_source_id
        FOR SHARE;
    ELSE
        PERFORM 1
        FROM public.worker_task_dispatches AS dispatch
        WHERE dispatch.id = p_source_id
        FOR SHARE;
    END IF;
    v_source_exists := FOUND;
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton
    FOR SHARE;
    IF NOT FOUND
       OR v_status.run_id IS DISTINCT FROM p_run_id
       OR v_status.state <> 'running'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_mismatch',
            ERRCODE = 'P0001';
    END IF;
    SELECT expectation.*
    INTO v_expectation
    FROM public.worker_redis_continuity_expectations AS expectation
    WHERE expectation.run_id = p_run_id
      AND expectation.marker_kind = p_marker_kind
      AND expectation.source_id = p_source_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_observation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    SELECT cleanup.*
    INTO v_cleanup
    FROM public.worker_redis_marker_cleanup_authorizations AS cleanup
    WHERE cleanup.marker_kind = p_marker_kind
      AND cleanup.source_id = p_source_id
    FOR UPDATE;
    v_cleanup_exists := FOUND;
    v_now := pg_catalog.clock_timestamp();
    IF v_status.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_run_expired',
            ERRCODE = 'P0001';
    END IF;
    IF v_expectation.payload_sha256
            IS DISTINCT FROM p_payload_sha256
       OR (
            v_expectation.expected_message_id IS NOT NULL
            AND v_expectation.expected_message_id
                IS DISTINCT FROM p_message_id
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_observation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_cleanup_exists THEN
        IF v_cleanup.marker_key IS DISTINCT FROM v_expectation.marker_key
           OR v_cleanup.redis_stream
                IS DISTINCT FROM v_expectation.redis_stream
           OR v_cleanup.expected_message_id
                IS DISTINCT FROM v_expectation.expected_message_id
           OR v_cleanup.payload_sha256
                IS DISTINCT FROM v_expectation.payload_sha256
           OR v_cleanup.authorization_state = 'conflict'
        THEN
            RAISE EXCEPTION USING
                MESSAGE = 'marker_observation_not_authorized',
                ERRCODE = 'P0001';
        END IF;
        IF v_expectation.observed_message_id IS NULL THEN
            UPDATE public.worker_redis_continuity_expectations
            SET observed_message_id = p_message_id,
                observed_payload_sha256 = p_payload_sha256,
                observed_by = v_principal,
                observed_at = v_now
            WHERE run_id = p_run_id
              AND marker_key = v_expectation.marker_key;
        ELSIF v_expectation.observed_message_id
                    IS DISTINCT FROM p_message_id
              OR v_expectation.observed_payload_sha256
                    IS DISTINCT FROM p_payload_sha256
              OR v_expectation.observed_by IS DISTINCT FROM v_principal
        THEN
            RAISE EXCEPTION USING
                MESSAGE = 'marker_observation_mismatch',
                ERRCODE = 'P0001';
        END IF;
        IF v_cleanup.authorization_state IN ('deleted', 'absent') THEN
            UPDATE public.worker_redis_marker_cleanup_authorizations
            SET authorization_state = 'pending',
                claimed_by_run_id = NULL,
                claim_expires_at = NULL,
                finished_at = NULL,
                result_code = NULL
            WHERE id = v_cleanup.id;
        END IF;
        RETURN true;
    END IF;
    IF NOT v_source_exists THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_observation_not_authorized',
            ERRCODE = 'P0001';
    END IF;
    IF v_expectation.expected_message_id IS NULL THEN
        IF v_expectation.marker_kind <> 'event_emission'
           OR v_expectation.source_state <> 'prepared'
           OR NOT v_expectation.absence_allowed
        THEN
            RAISE EXCEPTION USING
                MESSAGE = 'marker_observation_not_authorized',
                ERRCODE = 'P0001';
        END IF;
    ELSE
        SELECT audit.*
        INTO v_authorization
        FROM public.worker_redis_marker_repair_audits AS audit
        WHERE audit.source_id = p_source_id
          AND audit.action = 'restore_marker'
          AND audit.result_code = 'authorized'
          AND audit.created_at
                > v_now - interval '300 seconds'
          AND NOT EXISTS (
                SELECT 1
                FROM public.worker_redis_marker_repair_audits AS completed
                WHERE completed.source_id = p_source_id
                  AND completed.action = 'restore_marker'
                  AND completed.result_code = 'restored'
                  AND completed.created_at >= audit.created_at
          )
        ORDER BY audit.created_at DESC, audit.id DESC
        LIMIT 1;
        v_restore_authorized := FOUND;
    END IF;
    IF v_expectation.observed_message_id IS NULL THEN
        UPDATE public.worker_redis_continuity_expectations
        SET observed_message_id = p_message_id,
            observed_payload_sha256 = p_payload_sha256,
            observed_by = v_principal,
            observed_at = v_now
        WHERE run_id = p_run_id
          AND marker_key = v_expectation.marker_key;
    ELSIF v_expectation.observed_message_id
                IS DISTINCT FROM p_message_id
          OR v_expectation.observed_payload_sha256
                IS DISTINCT FROM p_payload_sha256
          OR v_expectation.observed_by IS DISTINCT FROM v_principal
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_observation_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_restore_authorized THEN
        INSERT INTO public.worker_redis_marker_repair_audits (
            source_id,
            action,
            result_code,
            principal,
            created_at
        ) VALUES (
            p_source_id,
            'restore_marker',
            'restored',
            v_authorization.principal,
            v_now
        );
    END IF;
    RETURN true;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_require_worker_redis_continuity(
    p_max_age_seconds integer
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_max_age_seconds IS NULL
       OR p_max_age_seconds < 1
       OR p_max_age_seconds > 300
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_request_invalid',
            ERRCODE = 'P0001';
    END IF;
{_principal_guard_sql()}
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_missing',
            ERRCODE = 'P0001';
    END IF;
    IF v_status.state = 'running' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_running',
            ERRCODE = 'P0001';
    END IF;
    IF v_status.state <> 'ready' THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_error',
            ERRCODE = 'P0001';
    END IF;
    IF v_status.finished_at IS NULL
       OR v_status.finished_at
            < v_now - pg_catalog.make_interval(
                secs => p_max_age_seconds
            )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'worker_redis_continuity_stale',
            ERRCODE = 'P0001';
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_claim_worker_redis_marker_cleanup(
    p_run_id uuid,
    p_limit integer,
    p_lease_seconds integer
)
RETURNS TABLE(
    id uuid,
    marker_kind text,
    source_id uuid,
    marker_key text,
    redis_stream text,
    expected_message_id text,
    payload_sha256 text,
    claim_expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_now timestamptz := pg_catalog.clock_timestamp();
BEGIN
    IF p_run_id IS NULL
       OR p_limit IS NULL
       OR p_limit < 1
       OR p_limit > 100
       OR p_lease_seconds IS DISTINCT FROM 300
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_claim_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_janitor_runtime")}
    RETURN QUERY
    WITH candidates AS (
        SELECT cleanup.id
        FROM public.worker_redis_marker_cleanup_authorizations
            AS cleanup
        WHERE cleanup.authorization_state = 'pending'
           OR (
                cleanup.authorization_state = 'claimed'
                AND (
                    cleanup.claimed_by_run_id = p_run_id
                    OR cleanup.claim_expires_at <= v_now
                )
           )
        ORDER BY cleanup.authorized_at, cleanup.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    ),
    claimed AS (
        UPDATE public.worker_redis_marker_cleanup_authorizations
            AS cleanup
        SET authorization_state = 'claimed',
            claimed_by_run_id = p_run_id,
            claim_expires_at = v_now
                + pg_catalog.make_interval(secs => p_lease_seconds),
            finished_at = NULL,
            result_code = NULL
        FROM candidates
        WHERE cleanup.id = candidates.id
        RETURNING cleanup.*
    )
    SELECT
        claimed.id,
        claimed.marker_kind::text,
        claimed.source_id,
        claimed.marker_key::text,
        claimed.redis_stream::text,
        claimed.expected_message_id::text,
        claimed.payload_sha256::text,
        claimed.claim_expires_at
    FROM claimed
    ORDER BY claimed.authorized_at, claimed.id;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_finish_worker_redis_marker_cleanup(
    p_authorization_id uuid,
    p_run_id uuid,
    p_result text,
    p_result_code text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_authorization
        public.worker_redis_marker_cleanup_authorizations%ROWTYPE;
    v_now timestamptz;
BEGIN
    IF p_authorization_id IS NULL
       OR p_run_id IS NULL
       OR p_result NOT IN ('deleted', 'absent', 'conflict')
       OR p_result_code IS NULL
       OR p_result_code !~ '^[a-z][a-z0-9_]{{0,63}}$'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_result_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_janitor_runtime")}
    SELECT cleanup.*
    INTO v_authorization
    FROM public.worker_redis_marker_cleanup_authorizations
        AS cleanup
    WHERE cleanup.id = p_authorization_id
    FOR UPDATE;
    v_now := pg_catalog.clock_timestamp();
    IF NOT FOUND
       OR v_authorization.authorization_state <> 'claimed'
       OR v_authorization.claimed_by_run_id IS DISTINCT FROM p_run_id
       OR v_authorization.claim_expires_at <= v_now
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_cleanup_claim_mismatch',
            ERRCODE = 'P0001';
    END IF;
    UPDATE public.worker_redis_marker_cleanup_authorizations
    SET authorization_state = p_result,
        finished_at = v_now,
        result_code = p_result_code
    WHERE id = p_authorization_id;
    RETURN true;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_load_worker_redis_marker_repair(
    p_action text,
    p_source_id uuid
)
RETURNS TABLE(
    marker_kind text,
    source_id uuid,
    marker_key text,
    redis_stream text,
    expected_message_id text,
    payload_sha256 text,
    source_state text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_source_exists boolean;
BEGIN
    IF p_action NOT IN (
        'audit',
        'restore_marker',
        'authorize_restore_marker',
        'promote_prepared'
    )
       OR (p_action <> 'audit' AND p_source_id IS NULL)
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_request_invalid',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_repair_runtime")}
    IF p_action = 'promote_prepared' THEN
        RETURN QUERY
        SELECT
            'event_emission'::text,
            emission.id,
            (
                'vp:worker-event-emission:' || emission.id::text
            )::text,
            emission.redis_stream::text,
            emission.message_id::text,
            emission.payload_sha256::text,
            emission.emission_state::text
        FROM public.worker_event_emissions AS emission
        WHERE emission.id = p_source_id
          AND emission.emission_state = 'prepared'
          AND emission.message_id IS NULL;
    ELSIF p_action = 'audit' THEN
        RETURN QUERY
        WITH candidates AS (
            SELECT
                1 AS priority,
                'event_emission'::text AS marker_kind,
                emission.id AS source_id,
                (
                    'vp:worker-event-emission:' || emission.id::text
                )::text AS marker_key,
                emission.redis_stream::text AS redis_stream,
                emission.message_id::text AS expected_message_id,
                emission.payload_sha256::text AS payload_sha256,
                emission.emission_state::text AS source_state
            FROM public.worker_event_emissions AS emission
            WHERE (
                emission.id = p_source_id
                OR p_source_id IS NULL
              )
            UNION ALL
            SELECT
                2,
                'task_dispatch'::text,
                dispatch.id,
                (
                    'vp:worker-task-dispatch:'
                    || dispatch.dispatch_key::text
                )::text,
                dispatch.redis_stream::text,
                dispatch.redis_message_id::text,
                dispatch.payload_sha256::text,
                (
                    'delivery:' || dispatch.delivery_state
                    || '/resolution:' || dispatch.resolution_state
                )::text
            FROM public.worker_task_dispatches AS dispatch
            WHERE (
                dispatch.id = p_source_id
                OR p_source_id IS NULL
              )
            UNION ALL
            SELECT
                3,
                cleanup.marker_kind::text,
                cleanup.source_id,
                cleanup.marker_key::text,
                cleanup.redis_stream::text,
                cleanup.expected_message_id::text,
                cleanup.payload_sha256::text,
                cleanup.authorization_state::text
            FROM public.worker_redis_marker_cleanup_authorizations
                AS cleanup
            WHERE (
                cleanup.source_id = p_source_id
                OR p_source_id IS NULL
              )
        )
        SELECT
            candidate.marker_kind,
            candidate.source_id,
            candidate.marker_key,
            candidate.redis_stream,
            candidate.expected_message_id,
            candidate.payload_sha256,
            candidate.source_state
        FROM candidates AS candidate
        ORDER BY candidate.marker_key, candidate.priority, candidate.source_id
        LIMIT 100;
    ELSE
        PERFORM 1
        FROM public.worker_event_emissions AS emission
        WHERE emission.id = p_source_id
        FOR SHARE;
        v_source_exists := FOUND;
        IF NOT v_source_exists THEN
            PERFORM 1
            FROM public.worker_task_dispatches AS dispatch
            WHERE dispatch.id = p_source_id
            FOR SHARE;
            v_source_exists := FOUND;
        END IF;
        IF NOT v_source_exists
           OR EXISTS (
                SELECT 1
                FROM public.worker_redis_marker_cleanup_authorizations
                    AS cleanup
                WHERE cleanup.source_id = p_source_id
           )
        THEN
            RAISE EXCEPTION USING
                MESSAGE = 'marker_repair_evidence_missing',
                ERRCODE = 'P0001';
        END IF;
        RETURN QUERY
        WITH candidates AS (
            SELECT
                1 AS priority,
                'event_emission'::text AS marker_kind,
                emission.id AS source_id,
                (
                    'vp:worker-event-emission:' || emission.id::text
                )::text AS marker_key,
                emission.redis_stream::text AS redis_stream,
                emission.message_id::text AS expected_message_id,
                emission.payload_sha256::text AS payload_sha256,
                emission.emission_state::text AS source_state
            FROM public.worker_event_emissions AS emission
            WHERE emission.id = p_source_id
              AND emission.message_id IS NOT NULL
            UNION ALL
            SELECT
                2,
                'task_dispatch'::text,
                dispatch.id,
                (
                    'vp:worker-task-dispatch:'
                    || dispatch.dispatch_key::text
                )::text,
                dispatch.redis_stream::text,
                dispatch.redis_message_id::text,
                dispatch.payload_sha256::text,
                (
                    'delivery:' || dispatch.delivery_state
                    || '/resolution:' || dispatch.resolution_state
                )::text
            FROM public.worker_task_dispatches AS dispatch
            WHERE dispatch.id = p_source_id
              AND dispatch.redis_message_id IS NOT NULL
        )
        SELECT
            candidate.marker_kind,
            candidate.source_id,
            candidate.marker_key,
            candidate.redis_stream,
            candidate.expected_message_id,
            candidate.payload_sha256,
            candidate.source_state
        FROM candidates AS candidate
        ORDER BY candidate.priority
        LIMIT 1;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_evidence_missing',
            ERRCODE = 'P0001';
    END IF;
    IF p_action = 'authorize_restore_marker' THEN
        INSERT INTO public.worker_redis_marker_repair_audits (
            source_id,
            action,
            result_code,
            principal,
            created_at
        ) VALUES (
            p_source_id,
            'restore_marker',
            'authorized',
            v_principal,
            pg_catalog.clock_timestamp()
        );
    END IF;
END;
$function$
"""
    )
    op.execute(
        f"""
CREATE FUNCTION public.vp_promote_observed_worker_event_emission(
    p_emission_id uuid,
    p_message_id text,
    p_payload_sha256 text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role oid;
    v_emission public.worker_event_emissions%ROWTYPE;
    v_registration public.worker_registrations%ROWTYPE;
    v_status public.worker_redis_continuity_status%ROWTYPE;
    v_observation public.worker_redis_continuity_expectations%ROWTYPE;
    v_observation_exists boolean;
    v_now timestamptz;
BEGIN
    IF p_emission_id IS NULL
       OR p_message_id IS NULL
       OR length(p_message_id) NOT BETWEEN 3 AND 64
       OR p_message_id !~ '^[0-9]+-[0-9]+$'
       OR p_payload_sha256 !~ '^[0-9a-f]{{64}}$'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
{_marker_control_principal_guard_sql("vp_marker_repair_runtime")}
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_evidence_missing',
            ERRCODE = 'P0001';
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.id = v_emission.job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.node_executions AS node
    WHERE node.id = v_emission.node_execution_id
      AND node.job_id = v_emission.job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
        pg_catalog.hashtextextended(
            'vp-worker-registration:'
            || v_emission.worker_registration_id::text,
            0
        )
    );
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = v_emission.worker_registration_id
      AND registration.lease_epoch = v_emission.worker_lease_epoch
      AND registration.status IN ('active', 'expired')
    FOR SHARE;
    IF NOT FOUND
       OR EXISTS (
            SELECT 1
            FROM public.worker_registrations AS replacement
            WHERE replacement.service_name = v_registration.service_name
              AND replacement.id <> v_registration.id
              AND replacement.lease_epoch > v_registration.lease_epoch
       )
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_registration_replaced',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = v_emission.source_task_attestation_id
      AND attestation.job_id = v_emission.job_id
      AND attestation.node_execution_id = v_emission.node_execution_id
      AND attestation.worker_registration_id =
            v_emission.worker_registration_id
      AND attestation.worker_lease_epoch = v_emission.worker_lease_epoch
      AND attestation.ack_state = 'acknowledged'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
    SELECT emission.*
    INTO v_emission
    FROM public.worker_event_emissions AS emission
    WHERE emission.id = p_emission_id
    FOR UPDATE;
    IF v_emission.emission_state <> 'prepared'
       OR v_emission.message_id IS NOT NULL
       OR v_emission.payload_sha256 IS DISTINCT FROM p_payload_sha256
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
    PERFORM 1
    FROM public.worker_task_delivery_attestations AS attestation
    JOIN public.worker_task_dispatches AS dispatch
      ON dispatch.dispatch_key = attestation.dispatch_key
    WHERE attestation.id = v_emission.source_task_attestation_id
      AND dispatch.job_id = v_emission.job_id
      AND dispatch.node_execution_id = v_emission.node_execution_id
      AND dispatch.redis_stream = attestation.redis_stream
      AND dispatch.consumer_group = attestation.consumer_group
      AND dispatch.redis_message_id = attestation.message_id
      AND dispatch.payload_sha256 = attestation.payload_sha256
      AND dispatch.delivery_state = 'delivered'
      AND dispatch.resolution_state = 'acknowledged'
    FOR UPDATE OF dispatch;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_proof_mismatch',
            ERRCODE = 'P0001';
    END IF;
    SELECT status.*
    INTO v_status
    FROM public.worker_redis_continuity_status AS status
    WHERE status.singleton
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_observation_missing',
            ERRCODE = 'P0001';
    END IF;
    SELECT expectation.*
    INTO v_observation
    FROM public.worker_redis_continuity_expectations AS expectation
    WHERE expectation.marker_kind = 'event_emission'
      AND expectation.source_id = p_emission_id
      AND expectation.run_id = v_status.run_id
      AND expectation.source_state = 'prepared'
      AND expectation.expected_message_id IS NULL
      AND expectation.observed_message_id = p_message_id
      AND expectation.observed_payload_sha256 = p_payload_sha256
    FOR SHARE OF expectation;
    v_observation_exists := FOUND;
    v_now := pg_catalog.clock_timestamp();
    IF NOT v_observation_exists
       OR v_status.lease_expires_at <= v_now
       OR v_observation.observed_at
            <= v_now - interval '300 seconds'
       OR v_observation.observed_at > v_status.lease_expires_at
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'marker_repair_observation_missing',
            ERRCODE = 'P0001';
    END IF;
    UPDATE public.worker_event_emissions
    SET message_id = p_message_id,
        emission_state = 'emitted',
        emitted_at = v_now
    WHERE id = p_emission_id;
    INSERT INTO public.worker_redis_marker_repair_audits (
        source_id,
        action,
        result_code,
        principal,
        created_at
    ) VALUES (
        p_emission_id,
        'promote_prepared',
        'promoted',
        v_principal,
        v_now
    );
    RETURN true;
END;
$function$
"""
    )
    for signature in (
        LIST_REDIS_MARKER_EXPECTATIONS_SIGNATURE,
        BEGIN_REDIS_CONTINUITY_SIGNATURE,
        FINISH_REDIS_CONTINUITY_SIGNATURE,
        RECORD_REDIS_MARKER_OBSERVATION_SIGNATURE,
        REQUIRE_REDIS_CONTINUITY_SIGNATURE,
        CLAIM_REDIS_MARKER_CLEANUP_SIGNATURE,
        FINISH_REDIS_MARKER_CLEANUP_SIGNATURE,
        LOAD_REDIS_MARKER_REPAIR_SIGNATURE,
        PROMOTE_OBSERVED_EVENT_EMISSION_SIGNATURE,
    ):
        op.execute(
            f"REVOKE EXECUTE ON FUNCTION {signature} FROM PUBLIC"
        )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS "
        "trg_worker_redis_marker_repair_audit_append_only "
        "ON public.worker_redis_marker_repair_audits"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS "
        "trg_worker_redis_marker_cleanup_immutability "
        "ON public.worker_redis_marker_cleanup_authorizations"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_worker_event_emission_immutability "
        "ON public.worker_event_emissions"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_worker_task_dispatch_immutability "
        "ON public.worker_task_dispatches"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_worker_event_delivery_immutability "
        "ON public.registered_worker_event_deliveries"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_worker_task_attestation_immutability "
        "ON public.worker_task_delivery_attestations"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_worker_event_receipt_immutability "
        "ON public.registered_worker_event_receipts"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS "
        "public.vp_enforce_worker_event_receipt_immutability()"
    )
    for trigger_function in (
        "public."
        "vp_enforce_worker_redis_marker_repair_audit_append_only()",
        "public.vp_enforce_worker_redis_marker_cleanup_immutability()",
        "public.vp_enforce_worker_task_dispatch_immutability()",
        "public.vp_enforce_worker_event_delivery_immutability()",
        "public.vp_enforce_worker_task_attestation_immutability()",
        "public.vp_enforce_worker_event_emission_immutability()",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {trigger_function}")
    for signature in (
        PROMOTE_OBSERVED_EVENT_EMISSION_SIGNATURE,
        LOAD_REDIS_MARKER_REPAIR_SIGNATURE,
        FINISH_REDIS_MARKER_CLEANUP_SIGNATURE,
        CLAIM_REDIS_MARKER_CLEANUP_SIGNATURE,
        REQUIRE_REDIS_CONTINUITY_SIGNATURE,
        RECORD_REDIS_MARKER_OBSERVATION_SIGNATURE,
        FINISH_REDIS_CONTINUITY_SIGNATURE,
        BEGIN_REDIS_CONTINUITY_SIGNATURE,
        LIST_REDIS_MARKER_EXPECTATIONS_SIGNATURE,
        STAGING_JANITOR_READINESS_SIGNATURE,
        FINISH_STAGING_JANITOR_SIGNATURE,
        BEGIN_STAGING_JANITOR_SIGNATURE,
        CLEANUP_EVENT_AUTHORITY_SIGNATURE,
        CANCEL_TASK_ACKNOWLEDGE_SIGNATURE,
        CANCEL_TASK_REQUIRE_SIGNATURE,
        CANCEL_TASK_AUTHORIZE_SIGNATURE,
        PROVEN_TASK_ACKNOWLEDGE_SIGNATURE,
        RECOVER_REGISTERED_NODE_SIGNATURE,
        TRANSITION_YOUTUBE_UPLOAD_SIGNATURE,
        RESERVE_YOUTUBE_UPLOAD_SIGNATURE,
        OBSERVE_EVENT_EMISSION_SIGNATURE,
        LOAD_PREPARED_EVENT_EMISSION_SIGNATURE,
        LIST_PREPARED_EVENT_EMISSIONS_SIGNATURE,
        MARK_EVENT_EMITTED_SIGNATURE,
        PREPARE_EVENT_EMISSION_SIGNATURE,
        PERSIST_WORKER_ARTIFACT_SIGNATURE,
        REQUIRE_WORKER_NODE_CLAIM_SIGNATURE,
        CLAIM_WORKER_NODE_SIGNATURE,
        TASK_ACK_AUTHORIZE_SIGNATURE,
        TASK_ACKNOWLEDGED_SIGNATURE,
        TASK_ACK_SIGNATURE,
        MARGIN_SIGNATURE,
        OBSERVE_TASK_SIGNATURE,
        ATTEST_TASK_SIGNATURE,
        OBSERVER_SIGNATURE,
        REQUIRE_SIGNATURE,
        RELEASE_SIGNATURE,
        HEARTBEAT_SIGNATURE,
        REGISTER_SIGNATURE,
        REGISTRATION_EXPIRE_SIGNATURE,
        REGISTRATION_REVOKE_SIGNATURE,
        GRANT_REVOKE_SIGNATURE,
        GRANT_ACTIVATE_SIGNATURE,
        GRANT_UPSERT_SIGNATURE,
        ENDPOINT_FINGERPRINTS_SIGNATURE,
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")

    op.drop_table("worker_redis_marker_repair_audits")
    op.drop_index(
        "ix_worker_redis_continuity_expectation_page",
        table_name="worker_redis_continuity_expectations",
    )
    op.drop_table("worker_redis_continuity_expectations")
    op.drop_table("worker_redis_continuity_status")
    op.drop_index(
        "ix_worker_redis_marker_cleanup_claim",
        table_name="worker_redis_marker_cleanup_authorizations",
    )
    op.drop_table("worker_redis_marker_cleanup_authorizations")
    op.drop_table("staging_janitor_status")
    op.drop_index(
        "ix_registered_worker_event_deliveries_receipt_id",
        table_name="registered_worker_event_deliveries",
    )
    op.drop_table("registered_worker_event_deliveries")
    op.drop_constraint(
        "fk_worker_task_dispatches_origin_receipt_id",
        "worker_task_dispatches",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_registered_worker_event_receipts_source_task",
        table_name="registered_worker_event_receipts",
    )
    op.drop_index(
        "ix_registered_worker_event_receipts_worker_registration_id",
        table_name="registered_worker_event_receipts",
    )
    op.drop_index(
        "ix_registered_worker_event_receipts_node_execution_id",
        table_name="registered_worker_event_receipts",
    )
    op.drop_table("registered_worker_event_receipts")
    op.drop_index(
        "ix_worker_event_emissions_job_id",
        table_name="worker_event_emissions",
    )
    op.drop_index(
        "uq_worker_event_emission_redis_identity",
        table_name="worker_event_emissions",
    )
    op.drop_table("worker_event_emissions")
    op.drop_index(
        "ix_worker_task_delivery_attestations_registration_id",
        table_name="worker_task_delivery_attestations",
    )
    op.drop_table("worker_task_delivery_attestations")
    op.drop_index(
        "ix_worker_task_dispatches_pending",
        table_name="worker_task_dispatches",
    )
    op.drop_index(
        "uq_worker_task_dispatch_unresolved_initial_node",
        table_name="worker_task_dispatches",
    )
    op.drop_index(
        "ix_worker_task_dispatches_origin_receipt_id",
        table_name="worker_task_dispatches",
    )
    op.drop_table("worker_task_dispatches")
    _downgrade_intermediate_artifact_cache()

    op.drop_index(
        "ix_node_executions_worker_registration_id",
        table_name="node_executions",
    )
    op.drop_constraint(
        "ck_node_execution_worker_lease_binding",
        "node_executions",
        type_="check",
    )
    op.drop_constraint(
        "fk_node_executions_worker_registration_id",
        "node_executions",
        type_="foreignkey",
    )
    op.drop_column("node_executions", "worker_lease_epoch")
    op.drop_column("node_executions", "worker_registration_id")

    op.drop_index(
        "uq_worker_registrations_active_service",
        table_name="worker_registrations",
    )
    op.drop_index(
        "ix_worker_registrations_lease_expires_at",
        table_name="worker_registrations",
    )
    op.drop_index(
        "ix_worker_registrations_grant_id",
        table_name="worker_registrations",
    )
    op.drop_table("worker_registrations")
    op.drop_index(
        "uq_worker_admission_grants_active_service",
        table_name="worker_admission_grants",
    )
    op.drop_table("worker_admission_grants")


def _downgrade_intermediate_artifact_cache() -> None:
    op.execute(
        """
DELETE FROM public.intermediate_artifact_cache
WHERE output_artifact_id IS NULL
"""
    )
    op.drop_constraint(
        "intermediate_artifact_cache_output_artifact_id_fkey",
        "intermediate_artifact_cache",
        type_="foreignkey",
    )
    op.alter_column(
        "intermediate_artifact_cache",
        "output_artifact_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
    op.create_foreign_key(
        "intermediate_artifact_cache_output_artifact_id_fkey",
        "intermediate_artifact_cache",
        "artifacts",
        ["output_artifact_id"],
        ["id"],
        ondelete="CASCADE",
    )
    for column_name in (
        "media_info",
        "file_size",
        "mime_type",
        "filename",
        "storage_path",
        "storage_backend",
    ):
        op.drop_column("intermediate_artifact_cache", column_name)
