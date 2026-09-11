"""Release a proven original worker claim during accepted first-retry application."""

from pathlib import Path
import runpy

from alembic import op


revision = "037_registered_retry_release"
down_revision = "036_worker_session_signal"
branch_labels = None
depends_on = None


def _release_sql() -> str:
    previous = runpy.run_path(
        str(Path(__file__).with_name("034_worker_registrations.py"))
    )
    guard = previous["_marker_control_principal_guard_sql"](
        "vp_orchestrator_control_runtime"
    )
    return """
CREATE FUNCTION public.vp_release_registered_retry_claim(p_receipt_id uuid)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_principal text := session_user;
    v_privileged boolean;
    v_expected_role regrole;
    v_receipt public.registered_worker_event_receipts%ROWTYPE;
    v_attestation public.worker_task_delivery_attestations%ROWTYPE;
    v_original public.worker_task_dispatches%ROWTYPE;
    v_retry public.worker_task_dispatches%ROWTYPE;
    v_job_id uuid;
    v_node_id uuid;
    v_task_id uuid;
    v_channel_id uuid;
    v_count bigint;
BEGIN
$PRINCIPAL_GUARD$
    SELECT receipt.job_id, receipt.node_execution_id INTO v_job_id, v_node_id
    FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.id = p_receipt_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_receipt_mismatch';
    END IF;

    -- Same quarantine-compatible authority order as registered claiming.
    SELECT count(*) INTO v_count FROM public.production_tasks WHERE job_id = v_job_id;
    IF v_count > 1 THEN
        RAISE EXCEPTION 'task_authority_changed';
    END IF;
    SELECT task.id, task.channel_profile_id INTO v_task_id, v_channel_id
    FROM public.production_tasks AS task WHERE task.job_id = v_job_id;
    IF v_task_id IS NOT NULL THEN
        PERFORM 1 FROM public.channel_profiles
        WHERE id = v_channel_id AND enabled IS TRUE AND halted_at IS NULL FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'channel_authority_changed';
        END IF;
    END IF;
    PERFORM 1 FROM public.runtime_schedules
    WHERE service_name = 'videoprocess' AND state IN ('OPEN', 'DRAINING')
      AND (guarded_job_id IS NULL OR guarded_job_id = v_job_id) FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'schedule_authority_changed';
    END IF;
    IF v_task_id IS NOT NULL THEN
        PERFORM 1 FROM public.production_tasks
        WHERE id = v_task_id AND channel_profile_id = v_channel_id
          AND job_id = v_job_id AND state = 'producing' FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'task_authority_changed';
        END IF;
    END IF;
    PERFORM 1 FROM public.jobs WHERE id = v_job_id AND status = 'RUNNING' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job_authority_changed';
    END IF;
    PERFORM 1 FROM public.node_executions
    WHERE id = v_node_id AND job_id = v_job_id AND status = 'QUEUED'
      AND retry_count = 1 AND queued_at IS NOT NULL FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_claim_mismatch';
    END IF;
    SELECT receipt.* INTO v_receipt FROM public.registered_worker_event_receipts AS receipt
    WHERE receipt.id = p_receipt_id AND receipt.job_id = v_job_id
      AND receipt.node_execution_id = v_node_id
      AND receipt.application_state = 'accepted' AND receipt.applied_at IS NULL
      AND receipt.event_type = 'node_failed' AND receipt.ack_state = 'pending'
      AND receipt.source_task_ack_state = 'pending'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_receipt_mismatch';
    END IF;
    PERFORM 1 FROM public.node_executions
    WHERE id = v_node_id AND worker_id = v_receipt.worker_id
      AND worker_registration_id = v_receipt.worker_registration_id
      AND worker_lease_epoch = v_receipt.worker_lease_epoch
      AND started_at = v_receipt.worker_started_at;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_claim_mismatch';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended(
        'vp-worker-registration:' || v_receipt.worker_registration_id::text, 0));
    SELECT attestation.* INTO v_attestation
    FROM public.worker_task_delivery_attestations AS attestation
    WHERE attestation.id = v_receipt.source_task_attestation_id
      AND attestation.job_id = v_job_id AND attestation.node_execution_id = v_node_id
      AND attestation.worker_registration_id = v_receipt.worker_registration_id
      AND attestation.worker_lease_epoch = v_receipt.worker_lease_epoch
      AND attestation.worker_id = v_receipt.worker_id
      AND attestation.worker_started_at = v_receipt.worker_started_at
      AND attestation.redis_stream = v_receipt.source_task_stream
      AND attestation.consumer_group = v_receipt.source_task_group
      AND attestation.message_id = v_receipt.source_task_message_id FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_attestation_mismatch';
    END IF;
    PERFORM public.vp_observe_worker_task_delivery(
        v_receipt.worker_registration_id, v_receipt.worker_lease_epoch,
        v_receipt.worker_id, v_receipt.worker_started_at, v_job_id, v_node_id,
        v_receipt.source_task_stream, v_receipt.source_task_group,
        v_receipt.source_task_message_id, v_attestation.payload_sha256, v_attestation.dispatch_key);
    SELECT dispatch.* INTO v_original FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.dispatch_key = v_attestation.dispatch_key
      AND dispatch.resolution_state IN ('unresolved', 'acknowledged') FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_dispatch_mismatch';
    END IF;
    PERFORM 1 FROM public.worker_event_emissions AS emission
    WHERE emission.source_task_attestation_id = v_attestation.id
      AND emission.job_id = v_job_id AND emission.node_execution_id = v_node_id
      AND emission.worker_registration_id = v_receipt.worker_registration_id
      AND emission.worker_lease_epoch = v_receipt.worker_lease_epoch
      AND emission.worker_id = v_receipt.worker_id
      AND emission.worker_started_at = v_receipt.worker_started_at
      AND emission.event_type = v_receipt.event_type
      AND emission.redis_stream = v_receipt.redis_stream
      AND emission.consumer_group = v_receipt.consumer_group
      AND emission.message_id = v_receipt.message_id
      AND emission.payload_sha256 = v_receipt.payload_sha256
      AND emission.payload_json = v_receipt.payload_json
      AND emission.emission_state = 'emitted' AND emission.emitted_at IS NOT NULL
      AND emission.resolved_at IS NULL FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_emission_mismatch';
    END IF;
    IF (
        v_receipt.payload_json->>'event' = 'node_failed'
        AND v_receipt.payload_json->>'job_id' = v_job_id::text
        AND v_receipt.payload_json->>'node_execution_id' = v_node_id::text
        AND v_receipt.payload_json->>'worker_id' = v_receipt.worker_id
        AND v_receipt.payload_json->>'worker_registration_id' = v_receipt.worker_registration_id::text
        AND v_receipt.payload_json->>'worker_lease_epoch' = v_receipt.worker_lease_epoch::text
        AND (v_receipt.payload_json->>'started_at')::timestamptz = v_receipt.worker_started_at
        AND v_receipt.payload_json->>'task_stream' = v_receipt.source_task_stream
        AND v_receipt.payload_json->>'task_group' = v_receipt.source_task_group
        AND v_receipt.payload_json->>'task_message_id' = v_receipt.source_task_message_id
        AND v_receipt.payload_json->>'task_payload_sha256' = v_attestation.payload_sha256
        AND v_receipt.payload_json->>'task_dispatch_key' = v_attestation.dispatch_key::text
    ) IS NOT TRUE THEN
        RAISE EXCEPTION 'retry_payload_mismatch';
    END IF;
    PERFORM 1 FROM public.registered_worker_event_deliveries AS delivery
    WHERE delivery.receipt_id = p_receipt_id AND delivery.source_task_attestation_id = v_attestation.id
      AND delivery.redis_stream = v_receipt.redis_stream
      AND delivery.consumer_group = v_receipt.consumer_group
      AND delivery.message_id = v_receipt.message_id
      AND delivery.payload_sha256 = v_receipt.payload_sha256
      AND delivery.resolution_state = 'accepted' AND delivery.reason_code IS NULL
      AND delivery.ack_state = 'pending' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_delivery_mismatch';
    END IF;

    SELECT count(*) INTO v_count FROM public.worker_task_dispatches
    WHERE origin_receipt_id = p_receipt_id;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'retry_dispatch_mismatch';
    END IF;
    SELECT dispatch.* INTO v_retry FROM public.worker_task_dispatches AS dispatch
    WHERE dispatch.origin_receipt_id = p_receipt_id
      AND dispatch.id <> v_original.id AND dispatch.job_id = v_job_id
      AND dispatch.node_execution_id = v_node_id
      AND dispatch.redis_stream = v_original.redis_stream
      AND dispatch.consumer_group = v_original.consumer_group
      AND dispatch.delivery_state = 'pending' AND dispatch.resolution_state = 'unresolved'
      AND dispatch.delivery_attempted_at IS NULL AND dispatch.delivery_error IS NULL
      AND dispatch.redis_message_id IS NULL AND dispatch.delivered_at IS NULL
      AND dispatch.acknowledged_at IS NULL AND dispatch.cancelled_at IS NULL
      AND dispatch.created_at >= v_receipt.accepted_at
      AND dispatch.payload_json->>'job_id' = v_job_id::text
      AND dispatch.payload_json->>'node_execution_id' = v_node_id::text
      AND dispatch.payload_json->>'dispatch_key' = dispatch.dispatch_key::text FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_dispatch_mismatch';
    END IF;
    IF EXISTS (SELECT 1 FROM public.worker_task_delivery_attestations
               WHERE dispatch_key = v_retry.dispatch_key)
       OR EXISTS (SELECT 1 FROM public.worker_task_dispatches
                  WHERE node_execution_id = v_node_id AND id NOT IN (v_original.id, v_retry.id)
                    AND resolution_state IN ('unresolved', 'cancel_authorized')) THEN
        RAISE EXCEPTION 'retry_dispatch_conflict';
    END IF;
    UPDATE public.node_executions
    SET worker_id = NULL, worker_registration_id = NULL, worker_lease_epoch = NULL, started_at = NULL
    WHERE id = v_node_id AND job_id = v_job_id AND status = 'QUEUED' AND retry_count = 1
      AND worker_id = v_receipt.worker_id AND worker_registration_id = v_receipt.worker_registration_id
      AND worker_lease_epoch = v_receipt.worker_lease_epoch AND started_at = v_receipt.worker_started_at;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'retry_claim_mismatch';
    END IF;
    RETURN v_node_id;
END;
$function$
""".replace("$PRINCIPAL_GUARD$", guard)


def upgrade() -> None:
    op.execute(_release_sql())
    op.execute(
        "REVOKE ALL ON FUNCTION public.vp_release_registered_retry_claim(uuid) FROM PUBLIC"
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION public.vp_release_registered_retry_claim(uuid)")
