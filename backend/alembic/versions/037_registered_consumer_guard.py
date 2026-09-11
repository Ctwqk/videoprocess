"""Restricted, non-mutating registered-consumer reconciliation guard.

Revision ID: 037_registered_consumer_guard
Revises: 036_worker_session_signal

Local branch ordering only. Parent must renumber after ACK/native migrations.
"""

from __future__ import annotations

import runpy
from pathlib import Path

from alembic import op


revision = "037_registered_consumer_guard"
down_revision = "036_worker_session_signal"
branch_labels = None
depends_on = None
SIGNATURE = "public.vp_registered_consumer_reconcile_guard(text,uuid[],uuid[])"


def guard_sql() -> str:
    previous = runpy.run_path(
        str(Path(__file__).with_name("034_worker_registrations.py"))
    )
    principal_guard = previous["_principal_guard_sql"]()
    return (
        """
CREATE FUNCTION public.vp_registered_consumer_reconcile_guard(
    p_generation text, p_current uuid[], p_predecessor uuid[]
) RETURNS TABLE (
    observed_at timestamptz, registration_id uuid, grant_id uuid,
    worker_instance_id uuid, superseded_by uuid, registered_at timestamptz,
    lease_expires_at timestamptz, registration_revoked_at timestamptz,
    grant_activated_at timestamptz, grant_revoked_at timestamptz,
    registration_facts jsonb, grant_facts jsonb
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role regrole;
    v_schedule record;
    v_services text[] := ARRAY[
        'vp-ffmpeg-worker-go-swarm', 'vp-ffmpeg-worker-gpu-swarm',
        'vp-vision-worker-swarm', 'vp-youtube-publisher-swarm'
    ];
    v_actual_services text[];
    v_ids uuid[];
    v_count integer;
    v_now timestamptz;
BEGIN
    IF p_generation IS NULL OR p_generation !~ '^[a-z0-9][a-z0-9-]{0,62}$'
       OR v_principal IS DISTINCT FROM 'vp_operator_' || substr(
           encode(sha256(convert_to(p_generation, 'UTF8')), 'hex'), 1, 16)
    THEN RAISE EXCEPTION 'registered_reconcile_principal_invalid'; END IF;
    v_expected_role := pg_catalog.to_regrole('vp_worker_operator_runtime');
    IF v_expected_role IS NULL OR NOT COALESCE(
        pg_catalog.pg_has_role(v_principal, v_expected_role, 'USAGE'), false
    ) THEN RAISE EXCEPTION 'registered_reconcile_principal_invalid'; END IF;
    WITH RECURSIVE assumable_roles(role_oid) AS (
        SELECT role.oid FROM pg_catalog.pg_roles AS role WHERE role.rolname = v_principal
        UNION
        SELECT membership.roleid FROM pg_catalog.pg_auth_members AS membership
        JOIN assumable_roles AS member_role ON member_role.role_oid = membership.member
        WHERE membership.set_option
    )
    SELECT EXISTS (
        SELECT 1 FROM assumable_roles
        JOIN pg_catalog.pg_roles AS role ON role.oid = assumable_roles.role_oid
        WHERE role.rolsuper OR role.rolcreaterole OR role.rolcreatedb
           OR role.rolreplication OR role.rolbypassrls
    ) INTO v_privileged;
    IF COALESCE(v_privileged, true)
    THEN RAISE EXCEPTION 'registered_reconcile_principal_invalid'; END IF;
"""
        + principal_guard
        + """
    IF p_current IS NULL OR p_predecessor IS NULL
       OR cardinality(p_current) <> 4 OR array_ndims(p_current) <> 1
       OR cardinality(p_predecessor) > 4
       OR (cardinality(p_predecessor) > 0 AND array_ndims(p_predecessor) <> 1)
       OR array_position(p_current, NULL) IS NOT NULL
       OR array_position(p_predecessor, NULL) IS NOT NULL
    THEN RAISE EXCEPTION 'registered_reconcile_pins_invalid'; END IF;
    v_ids := p_current || p_predecessor;
    IF (SELECT count(DISTINCT item) FROM pg_catalog.unnest(v_ids) AS item)
       <> cardinality(v_ids)
    THEN RAISE EXCEPTION 'registered_reconcile_pins_invalid'; END IF;

    SELECT schedule.state, schedule.guarded_job_id INTO v_schedule
    FROM public.runtime_schedules AS schedule
    WHERE schedule.service_name = 'videoprocess' FOR SHARE;
    IF NOT FOUND OR v_schedule.state::text IS DISTINCT FROM 'CLOSED'
       OR v_schedule.guarded_job_id IS NOT NULL
    THEN RAISE EXCEPTION 'registered_reconcile_schedule_unsafe'; END IF;

    PERFORM registration.id
    FROM public.worker_registrations AS registration
    JOIN public.worker_admission_grants AS grant_row ON grant_row.id = registration.grant_id
    WHERE registration.id = ANY(v_ids)
    ORDER BY registration.service_name, registration.lease_epoch
    FOR SHARE OF registration, grant_row;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    IF v_count <> cardinality(v_ids)
    THEN RAISE EXCEPTION 'registered_reconcile_pins_missing'; END IF;

    SELECT array_agg(registration.service_name ORDER BY registration.service_name)
    INTO v_actual_services FROM public.worker_registrations AS registration
    WHERE registration.id = ANY(p_current);
    IF v_actual_services IS DISTINCT FROM v_services
       OR EXISTS (SELECT 1 FROM public.worker_registrations AS registration
                  WHERE registration.id = ANY(p_predecessor)
                    AND (NOT registration.service_name = ANY(v_services)
                         OR NOT registration.superseded_by = ANY(p_current)
                         OR registration.superseded_by IS NULL))
       OR EXISTS (SELECT 1 FROM public.worker_registrations AS registration
                  WHERE registration.service_name = ANY(v_services)
                    AND registration.status = 'active' AND NOT registration.id = ANY(p_current))
       OR EXISTS (SELECT 1 FROM public.worker_admission_grants AS grant_row
                  WHERE grant_row.service_name = ANY(v_services) AND grant_row.state = 'active'
                    AND NOT EXISTS (SELECT 1 FROM public.worker_registrations AS registration
                                    WHERE registration.id = ANY(p_current)
                                      AND registration.grant_id = grant_row.id))
    THEN RAISE EXCEPTION 'registered_reconcile_inventory_changed'; END IF;

    IF EXISTS (SELECT 1 FROM public.jobs WHERE status::text IN ('VALIDATING','PLANNING','RUNNING'))
       OR EXISTS (SELECT 1 FROM public.node_executions WHERE status::text IN ('QUEUED','RUNNING'))
       OR EXISTS (SELECT 1 FROM public.worker_task_dispatches
                  WHERE resolution_state NOT IN ('acknowledged','cancelled'))
       OR EXISTS (SELECT 1 FROM public.worker_task_delivery_attestations
                  WHERE ack_state <> 'acknowledged')
       OR EXISTS (SELECT 1 FROM public.worker_event_emissions WHERE emission_state <> 'resolved')
       OR EXISTS (SELECT 1 FROM public.registered_worker_event_receipts
                  WHERE application_state <> 'applied' OR ack_state <> 'acknowledged'
                    OR source_task_ack_state <> 'acknowledged')
       OR EXISTS (SELECT 1 FROM public.registered_worker_event_deliveries
                  WHERE ack_state <> 'acknowledged')
       OR EXISTS (SELECT 1 FROM public.youtube_upload_operations
                  WHERE status NOT IN ('succeeded','failed'))
       OR EXISTS (SELECT 1 FROM public.publication_promotion_operations WHERE status <> 'finalized')
    THEN RAISE EXCEPTION 'registered_reconcile_work_active'; END IF;

    v_now := pg_catalog.clock_timestamp();
    RETURN QUERY SELECT v_now, registration.id, grant_row.id,
        registration.worker_instance_id, registration.superseded_by,
        registration.registered_at, registration.lease_expires_at, registration.revoked_at,
        grant_row.activated_at, grant_row.revoked_at,
        jsonb_build_object(
            'service_name', registration.service_name, 'worker_type', registration.worker_type,
            'worker_host', registration.worker_host, 'capabilities_json', registration.capabilities_json,
            'image_identity', registration.image_identity, 'database_principal', registration.database_principal,
            'worker_slot', registration.worker_slot, 'redis_consumer_id', registration.redis_consumer_id,
            'lease_epoch', registration.lease_epoch, 'database_fingerprint', registration.database_fingerprint,
            'redis_fingerprint', registration.redis_fingerprint, 'storage_fingerprint', registration.storage_fingerprint,
            'status', registration.status, 'revoke_reason', registration.revoke_reason
        ),
        jsonb_build_object(
            'service_name', grant_row.service_name, 'worker_type', grant_row.worker_type,
            'worker_host', grant_row.worker_host, 'capabilities_json', grant_row.capabilities_json,
            'image_identity', grant_row.image_identity, 'database_principal', grant_row.database_principal,
            'generation', grant_row.generation, 'release_commit', grant_row.release_commit,
            'redis_stream', grant_row.redis_stream, 'redis_group', grant_row.redis_group,
            'endpoint_bindings_json', grant_row.endpoint_bindings_json,
            'state', grant_row.state, 'revoke_reason', grant_row.revoke_reason
        )
    FROM public.worker_registrations AS registration
    JOIN public.worker_admission_grants AS grant_row ON grant_row.id = registration.grant_id
    WHERE registration.id = ANY(v_ids) ORDER BY registration.service_name, registration.lease_epoch;
END;
$function$;
"""
    )


def upgrade() -> None:
    op.execute(guard_sql())
    op.execute(f"REVOKE ALL ON FUNCTION {SIGNATURE} FROM PUBLIC")
    # Fresh installs can create the stable role later through the existing CLI.
    op.execute(f"""
DO $acl$ DECLARE v_grantee record; BEGIN
    FOR v_grantee IN
        SELECT DISTINCT role.rolname FROM pg_catalog.pg_proc AS function
        CROSS JOIN LATERAL pg_catalog.aclexplode(function.proacl) AS acl
        JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
        WHERE function.oid = '{SIGNATURE}'::regprocedure AND acl.grantee <> function.proowner
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION {SIGNATURE} FROM %I', v_grantee.rolname);
    END LOOP;
    IF pg_catalog.to_regrole('vp_worker_operator_runtime') IS NOT NULL THEN
        GRANT EXECUTE ON FUNCTION {SIGNATURE} TO vp_worker_operator_runtime;
    END IF;
END $acl$;
""")


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {SIGNATURE}")
