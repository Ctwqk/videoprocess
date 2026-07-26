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
    "vp_worker_register(text,bigint,text,text,uuid,integer,text,jsonb,"
    "text,text,text,text,jsonb,text,text,text,text,text)"
)
HEARTBEAT_SIGNATURE = (
    "vp_worker_heartbeat(uuid,text,uuid,bigint,text)"
)
RELEASE_SIGNATURE = (
    "vp_worker_release(uuid,text,uuid,bigint,text,text)"
)
REQUIRE_SIGNATURE = "vp_require_worker_lease(uuid,bigint)"


def upgrade() -> None:
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
            "length(trim(service_name)) > 0",
            name="ck_worker_admission_grant_service_nonempty",
        ),
        sa.CheckConstraint(
            "generation > 0",
            name="ck_worker_admission_grant_generation_positive",
        ),
        sa.CheckConstraint(
            "length(trim(worker_type)) > 0",
            name="ck_worker_admission_grant_worker_type_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(worker_host)) > 0",
            name="ck_worker_admission_grant_worker_host_nonempty",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(capabilities_json) = 'array' "
            "AND jsonb_array_length(capabilities_json) > 0",
            name="ck_worker_admission_grant_capabilities_array",
        ),
        sa.CheckConstraint(
            "release_commit ~ '^[0-9a-f]{40}$'",
            name="ck_worker_admission_grant_release_commit",
        ),
        sa.CheckConstraint(
            "image_identity ~ ("
            "'^[A-Za-z0-9][-A-Za-z0-9._/]*' "
            "|| chr(58) || 'deploy-[0-9a-f]{12}$')",
            name="ck_worker_admission_grant_image_identity",
        ),
        sa.CheckConstraint(
            "length(trim(database_principal)) > 0",
            name="ck_worker_admission_grant_database_principal",
        ),
        sa.CheckConstraint(
            "length(trim(redis_stream)) > 0",
            name="ck_worker_admission_grant_redis_stream",
        ),
        sa.CheckConstraint(
            "length(trim(redis_group)) > 0",
            name="ck_worker_admission_grant_redis_group",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(endpoint_bindings_json) = 'object'",
            name="ck_worker_admission_grant_endpoint_bindings",
        ),
        sa.CheckConstraint(
            "token_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_worker_admission_grant_token_sha256",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'active', 'revoked')",
            name="ck_worker_admission_grant_state",
        ),
        sa.CheckConstraint(
            "(state = 'pending' AND activated_at IS NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'active' AND activated_at IS NOT NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'revoked' AND revoked_at IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0)",
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
            "length(trim(service_name)) > 0",
            name="ck_worker_registration_service_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(worker_type)) > 0",
            name="ck_worker_registration_worker_type_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(worker_host)) > 0",
            name="ck_worker_registration_worker_host_nonempty",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(capabilities_json) = 'array' "
            "AND jsonb_array_length(capabilities_json) > 0",
            name="ck_worker_registration_capabilities_array",
        ),
        sa.CheckConstraint(
            "worker_slot > 0",
            name="ck_worker_registration_slot_positive",
        ),
        sa.CheckConstraint(
            "length(trim(redis_consumer_id)) > 0",
            name="ck_worker_registration_consumer_nonempty",
        ),
        sa.CheckConstraint(
            "image_identity ~ ("
            "'^[A-Za-z0-9][-A-Za-z0-9._/]*' "
            "|| chr(58) || 'deploy-[0-9a-f]{12}$')",
            name="ck_worker_registration_image_identity",
        ),
        sa.CheckConstraint(
            "length(trim(database_principal)) > 0",
            name="ck_worker_registration_database_principal",
        ),
        sa.CheckConstraint(
            "database_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_worker_registration_database_fingerprint",
        ),
        sa.CheckConstraint(
            "redis_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_worker_registration_redis_fingerprint",
        ),
        sa.CheckConstraint(
            "storage_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_worker_registration_storage_fingerprint",
        ),
        sa.CheckConstraint(
            "lease_epoch > 0",
            name="ck_worker_registration_epoch_positive",
        ),
        sa.CheckConstraint(
            "lease_secret_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_worker_registration_lease_secret_sha256",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'revoked', 'expired')",
            name="ck_worker_registration_status",
        ),
        sa.CheckConstraint(
            "heartbeat_at >= registered_at",
            name="ck_worker_registration_heartbeat_order",
        ),
        sa.CheckConstraint(
            "lease_expires_at > heartbeat_at",
            name="ck_worker_registration_expiry_order",
        ),
        sa.CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0) "
            "OR (status IN ('active', 'expired') "
            "AND revoked_at IS NULL AND revoke_reason IS NULL)",
            name="ck_worker_registration_revocation_state",
        ),
        sa.CheckConstraint(
            "superseded_by IS NULL "
            "OR (status = 'revoked' AND superseded_by <> id)",
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
        "(worker_registration_id IS NULL AND worker_lease_epoch IS NULL) "
        "OR (worker_registration_id IS NOT NULL AND worker_lease_epoch > 0)",
    )
    op.create_index(
        "ix_node_executions_worker_registration_id",
        "node_executions",
        ["worker_registration_id"],
        unique=False,
    )

    _create_register_function()
    _create_heartbeat_function()
    _create_release_function()
    _create_require_function()
    for signature in (
        REGISTER_SIGNATURE,
        HEARTBEAT_SIGNATURE,
        RELEASE_SIGNATURE,
        REQUIRE_SIGNATURE,
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")


def _create_register_function() -> None:
    op.execute(
        """
CREATE FUNCTION vp_worker_register(
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
SET search_path = pg_catalog, public
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_grant public.worker_admission_grants%ROWTYPE;
    v_registration_id uuid := gen_random_uuid();
    v_superseded_id uuid;
    v_epoch bigint;
    v_now timestamptz := clock_timestamp();
    v_expires_at timestamptz;
BEGIN
    SELECT
        role.rolsuper
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname IN (
                  'worker_admission_grants',
                  'worker_registrations'
              )
              AND relation.relowner = role.oid
        )
        OR pg_catalog.has_table_privilege(
            v_principal,
            'public.worker_admission_grants',
            'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
        )
        OR pg_catalog.has_table_privilege(
            v_principal,
            'public.worker_registrations',
            'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
        )
    INTO v_privileged
    FROM pg_catalog.pg_roles AS role
    WHERE role.rolname = v_principal;

    IF COALESCE(v_privileged, true) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_privileged',
            ERRCODE = 'P0001';
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
    IF v_grant.database_principal <> v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_grant.token_sha256 <> p_token_sha256 THEN
        RAISE EXCEPTION USING
            MESSAGE = 'token_invalid',
            ERRCODE = 'P0001';
    END IF;
    IF v_grant.worker_type <> p_worker_type
       OR v_grant.worker_host <> p_worker_host
       OR v_grant.capabilities_json <> p_capabilities_json
       OR v_grant.release_commit <> p_release_commit
       OR v_grant.image_identity <> p_image_identity
       OR v_grant.redis_stream <> p_redis_stream
       OR v_grant.redis_group <> p_redis_group
       OR v_grant.endpoint_bindings_json <> p_endpoint_bindings_json
       OR p_worker_slot <= 0
       OR p_worker_instance_id IS NULL
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
    SELECT
        role.rolsuper
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname IN (
                  'worker_admission_grants',
                  'worker_registrations'
              )
              AND relation.relowner = role.oid
        )
        OR pg_catalog.has_table_privilege(
            v_principal,
            'public.worker_admission_grants',
            'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
        )
        OR pg_catalog.has_table_privilege(
            v_principal,
            'public.worker_registrations',
            'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'
        )
    INTO v_privileged
    FROM pg_catalog.pg_roles AS role
    WHERE role.rolname = v_principal;

    IF COALESCE(v_privileged, true) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_privileged',
            ERRCODE = 'P0001';
    END IF;
"""


def _create_heartbeat_function() -> None:
    op.execute(
        f"""
CREATE FUNCTION vp_worker_heartbeat(
    p_registration_id uuid,
    p_service_name text,
    p_worker_instance_id uuid,
    p_lease_epoch bigint,
    p_lease_secret_sha256 text
)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_expires_at timestamptz;
BEGIN
{_principal_guard_sql()}
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_registration.service_name <> p_service_name
       OR v_registration.worker_instance_id <> p_worker_instance_id
       OR v_registration.lease_epoch <> p_lease_epoch
       OR v_registration.status <> 'active'
       OR v_registration.lease_secret_sha256 <> p_lease_secret_sha256
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal <> v_principal THEN
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
CREATE FUNCTION vp_worker_release(
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
SET search_path = pg_catalog, public
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
{_principal_guard_sql()}
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_registration.service_name <> p_service_name
       OR v_registration.worker_instance_id <> p_worker_instance_id
       OR v_registration.lease_epoch <> p_lease_epoch
       OR v_registration.lease_secret_sha256 <> p_lease_secret_sha256
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal <> v_principal THEN
        RAISE EXCEPTION USING
            MESSAGE = 'database_principal_mismatch',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.status <> 'active' THEN
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
CREATE FUNCTION vp_require_worker_lease(
    p_registration_id uuid,
    p_lease_epoch bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_registration public.worker_registrations%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
{_principal_guard_sql()}
    SELECT registration.*
    INTO v_registration
    FROM public.worker_registrations AS registration
    WHERE registration.id = p_registration_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_registration.lease_epoch <> p_lease_epoch
       OR v_registration.status <> 'active'
    THEN
        RAISE EXCEPTION USING
            MESSAGE = 'lease_fenced',
            ERRCODE = 'P0001';
    END IF;
    IF v_registration.database_principal <> v_principal THEN
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


def downgrade() -> None:
    for signature in (
        REQUIRE_SIGNATURE,
        RELEASE_SIGNATURE,
        HEARTBEAT_SIGNATURE,
        REGISTER_SIGNATURE,
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {signature}")

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
