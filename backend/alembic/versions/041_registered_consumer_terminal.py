"""Prove cancelled preuploads quiescent for consumer metadata only.

Revision ID: 041_registered_consumer_terminal
Revises: 039_registered_consumer_guard (parent reparents after A2 integration)
"""

from __future__ import annotations

import runpy
from pathlib import Path

from alembic import op


revision = "041_registered_consumer_terminal"
down_revision = "039_registered_consumer_guard"
branch_labels = None
depends_on = None

SIGNATURE = "public.vp_registered_consumer_reconcile_guard(text,uuid[],uuid[])"
OLD_PREDICATE = (
    "OR EXISTS (SELECT 1 FROM public.youtube_upload_operations\n"
    "                  WHERE status NOT IN ('succeeded','failed'))"
)
NEW_PREDICATE = "OR NOT public.vp_registered_consumer_uploads_quiescent()"
HELPERS = (
    "public.vp_registered_consumer_ascii_json(text)",
    "public.vp_registered_consumer_payload_sha256(jsonb)",
    "public.vp_registered_consumer_terminal_upload(uuid,timestamptz)",
    "public.vp_registered_consumer_uploads_quiescent()",
)


def _bodies(*, downgrade: bool = False) -> tuple[str, str]:
    previous = runpy.run_path(
        str(Path(__file__).with_name("039_registered_consumer_guard.py"))
    )
    sql = previous["guard_sql"]()
    expected = sql.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    if expected.count(OLD_PREDICATE) != 1:
        raise RuntimeError("registered_reconcile_definition_changed")
    changed = expected.replace(OLD_PREDICATE, NEW_PREDICATE)
    return (changed, expected) if downgrade else (expected, changed)


def replace_definition(definition: str, source: str, *, downgrade: bool = False) -> str:
    before, after = _bodies(downgrade=downgrade)
    if source != before or definition.count(source) != 1:
        raise RuntimeError("registered_reconcile_definition_changed")
    return definition.replace(source, after, 1)


def _replacement(*, downgrade: bool = False) -> str:
    before, after = _bodies(downgrade=downgrade)
    for tag in ("$terminal_before$", "$terminal_after$", "$terminal_replace$"):
        if tag in before or tag in after:
            raise RuntimeError("registered_reconcile_definition_changed")
    # Catalog checks run on the server even when Alembic renders an offline script.
    return f"""
DO $terminal_replace$
DECLARE
    v_before text := $terminal_before${before}$terminal_before$;
    v_after text := $terminal_after${after}$terminal_after$;
    v_definition text; v_source text; v_security boolean; v_config text[];
BEGIN
    SELECT pg_catalog.pg_get_functiondef(p.oid), p.prosrc, p.prosecdef, p.proconfig
    INTO v_definition, v_source, v_security, v_config
    FROM pg_catalog.pg_proc p
    WHERE p.oid = pg_catalog.to_regprocedure('{SIGNATURE}');
    IF NOT FOUND OR v_source IS DISTINCT FROM v_before OR v_security IS NOT TRUE
        OR v_config IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
        OR (length(v_definition) - length(replace(v_definition, v_before, '')))
            IS DISTINCT FROM length(v_before)
    THEN RAISE EXCEPTION 'registered_reconcile_definition_changed'; END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END;
$terminal_replace$;
"""


def helper_sql() -> str:
    return "\n".join(helper_statements())


def helper_statements() -> tuple[str, ...]:
    return (
        r"""
CREATE FUNCTION public.vp_registered_consumer_ascii_json(p_text text) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path = pg_catalog AS $ascii$
DECLARE v_result text := ''; v_char text; v_code integer;
BEGIN
    -- Python's Redis payload contract is sorted, compact, ensure_ascii JSON.
    FOR v_char IN SELECT regexp_split_to_table(to_json(p_text)::text, '') LOOP
        v_code := ascii(v_char);
        IF v_code < 127 THEN v_result := v_result || v_char;
        ELSIF v_code <= 65535 THEN
            v_result := v_result || chr(92) || 'u' || lpad(to_hex(v_code), 4, '0');
        ELSE
            v_result := v_result || chr(92) || 'u' || to_hex(55296 + (v_code - 65536) / 1024)
                || chr(92) || 'u' || to_hex(56320 + (v_code - 65536) % 1024);
        END IF;
    END LOOP;
    RETURN v_result;
END;
$ascii$;
""",
        r"""
CREATE FUNCTION public.vp_registered_consumer_payload_sha256(p_payload jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path = pg_catalog AS $hash$
DECLARE v_text text;
BEGIN
    IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object' THEN RETURN NULL; END IF;
    IF EXISTS (SELECT 1 FROM jsonb_each(p_payload) e WHERE jsonb_typeof(e.value) <> 'string')
    THEN RETURN NULL; END IF;
    SELECT '{' || COALESCE(string_agg(public.vp_registered_consumer_ascii_json(e.key) || ':' ||
        public.vp_registered_consumer_ascii_json(e.value #>> '{}'), ',' ORDER BY e.key COLLATE "C"), '') || '}'
    INTO v_text FROM jsonb_each(p_payload) e;
    RETURN encode(sha256(convert_to(v_text, 'UTF8')), 'hex');
END;
$hash$;
""",
        r"""
CREATE FUNCTION public.vp_registered_consumer_terminal_upload(p_id uuid, p_now timestamptz)
RETURNS boolean LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $proof$
DECLARE
    o public.youtube_upload_operations%ROWTYPE; t public.production_tasks%ROWTYPE;
    j public.jobs%ROWTYPE; c public.channel_profiles%ROWTYPE; a public.publishing_accounts%ROWTYPE;
    n public.node_executions%ROWTYPE; art public.artifacts%ROWTYPE; asset public.assets%ROWTYPE;
    d public.worker_task_dispatches%ROWTYPE; att public.worker_task_delivery_attestations%ROWTYPE;
    e public.worker_event_emissions%ROWTYPE; r public.registered_worker_event_receipts%ROWTYPE;
    delivery public.registered_worker_event_deliveries%ROWTYPE;
    reg public.worker_registrations%ROWTYPE; g public.worker_admission_grants%ROWTYPE;
    origin public.registered_worker_event_receipts%ROWTYPE; q public.channel_ops_queue_items%ROWTYPE;
    v_nodes uuid[]; v_keys uuid[]; v_atts uuid[]; v_receipts uuid[]; v_emissions uuid[]; v_queues uuid[];
    v_spec jsonb; v_config jsonb; v_inputs jsonb; v_pipeline jsonb; v_stream text;
    v_cancel timestamptz; v_count bigint; v_att_count bigint;
BEGIN
    SELECT * INTO o FROM public.youtube_upload_operations WHERE id = p_id;
    IF NOT FOUND OR NOT COALESCE(o.status = 'reserved' AND o.privacy = 'unlisted'
        AND o.request_attempted_at IS NULL AND o.manager_task_id IS NULL AND o.platform_video_id IS NULL
        AND o.completed_at IS NULL AND o.error_message IS NULL AND o.receipt_json::jsonb = '{}'::jsonb
        AND o.content_sha256 ~ '^[0-9a-f]{64}$', false) THEN RETURN false; END IF;
    SELECT * INTO t FROM public.production_tasks WHERE id = o.production_task_id;
    IF NOT FOUND THEN RETURN false; END IF;
    SELECT * INTO j FROM public.jobs WHERE id = o.job_id;
    IF NOT FOUND THEN RETURN false; END IF;
    SELECT * INTO c FROM public.channel_profiles WHERE id = t.channel_profile_id;
    IF NOT FOUND THEN RETURN false; END IF;
    SELECT * INTO a FROM public.publishing_accounts WHERE id = t.target_account_id;
    IF NOT FOUND THEN RETURN false; END IF;
    v_cancel := j.completed_at AT TIME ZONE 'UTC';
    IF NOT COALESCE(t.job_id = j.id AND t.state = 'held' AND j.status::text = 'CANCELLED'
        AND a.channel_profile_id = c.id AND a.platform = 'youtube'
        AND c.halted_at <= v_cancel AND c.intake_paused_at <= v_cancel AND v_cancel <= p_now
        AND t.state_updated_at <= p_now, false) THEN RETURN false; END IF;
    IF (SELECT count(*) FROM public.production_tasks WHERE job_id = j.id) <> 1
        OR (SELECT count(*) FROM public.youtube_upload_operations WHERE job_id = j.id) <> 1
        OR EXISTS (SELECT 1 FROM public.jobs WHERE parent_job_id = j.id OR id = j.parent_job_id)
        OR EXISTS (SELECT 1 FROM public.publication_records WHERE production_task_id = t.id)
        OR EXISTS (SELECT 1 FROM public.publication_promotion_operations WHERE production_task_id = t.id)
    THEN RETURN false; END IF;
    SELECT COALESCE(array_agg(id), '{}'::uuid[]) INTO v_nodes FROM public.node_executions WHERE job_id = j.id;
    IF NOT o.node_execution_id = ANY(v_nodes) THEN RETURN false; END IF;
    SELECT * INTO n FROM public.node_executions WHERE id = o.node_execution_id;
    SELECT * INTO art FROM public.artifacts WHERE id = o.input_artifact_id;
    IF NOT FOUND OR NOT COALESCE(n.node_type = 'youtube_upload' AND n.status::text = 'CANCELLED'
        AND n.worker_id IS NULL AND n.output_artifact_id IS NULL
        AND n.completed_at AT TIME ZONE 'UTC' = v_cancel
        AND n.input_artifact_ids = ARRAY[o.input_artifact_id] AND art.job_id = j.id
        AND art.node_execution_id = ANY(v_nodes), false) THEN RETURN false; END IF;
    v_pipeline := j.pipeline_snapshot::jsonb;
    IF jsonb_typeof(v_pipeline->'nodes') IS DISTINCT FROM 'array'
        OR jsonb_typeof(v_pipeline->'edges') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
    IF jsonb_array_length(v_pipeline->'nodes') <> cardinality(v_nodes)
        OR (SELECT count(DISTINCT value->>'id') FROM jsonb_array_elements(v_pipeline->'nodes')) <> cardinality(v_nodes)
        OR (SELECT count(DISTINCT node_id) FROM public.node_executions WHERE id = ANY(v_nodes)) <> cardinality(v_nodes)
        OR (SELECT count(DISTINCT value->>'id') FROM jsonb_array_elements(v_pipeline->'edges')) <> jsonb_array_length(v_pipeline->'edges')
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_pipeline->'edges') edge WHERE
            NOT EXISTS (SELECT 1 FROM public.node_executions x WHERE x.id = ANY(v_nodes) AND x.node_id = edge->>'source')
            OR NOT EXISTS (SELECT 1 FROM public.node_executions x WHERE x.id = ANY(v_nodes) AND x.node_id = edge->>'target'))
    THEN RETURN false; END IF;
    IF EXISTS (WITH RECURSIVE paths(src, dst) AS (
        SELECT value->>'source', value->>'target' FROM jsonb_array_elements(v_pipeline->'edges')
        UNION SELECT paths.src, edge->>'target' FROM paths
        JOIN jsonb_array_elements(v_pipeline->'edges') edge ON edge->>'source' = paths.dst
    ) SELECT 1 FROM paths WHERE src = dst) THEN RETURN false; END IF;

    SELECT COALESCE(array_agg(dispatch_key), '{}'::uuid[]) INTO v_keys
    FROM public.worker_task_dispatches WHERE job_id = j.id OR node_execution_id = ANY(v_nodes)
        OR payload_json->>'job_id' = j.id::text
        OR payload_json->>'node_execution_id' IN (SELECT id::text FROM unnest(v_nodes) id);
    SELECT COALESCE(array_agg(id), '{}'::uuid[]) INTO v_atts
    FROM public.worker_task_delivery_attestations WHERE job_id = j.id OR node_execution_id = ANY(v_nodes) OR dispatch_key = ANY(v_keys);
    SELECT COALESCE(array_agg(id), '{}'::uuid[]) INTO v_receipts FROM public.registered_worker_event_receipts
    WHERE job_id = j.id OR node_execution_id = ANY(v_nodes) OR source_task_attestation_id = ANY(v_atts)
        OR payload_json->>'task_dispatch_key' IN (SELECT k::text FROM unnest(v_keys) k);
    SELECT COALESCE(array_agg(id), '{}'::uuid[]) INTO v_emissions FROM public.worker_event_emissions
    WHERE job_id = j.id OR node_execution_id = ANY(v_nodes) OR source_task_attestation_id = ANY(v_atts)
        OR payload_json->>'task_dispatch_key' IN (SELECT k::text FROM unnest(v_keys) k);
    -- Reject cross-boundary edges in either direction instead of hiding them in joins.
    IF EXISTS (SELECT 1 FROM public.worker_task_dispatches x WHERE
        (x.origin_receipt_id = ANY(v_receipts) OR x.dispatch_key IN
            (SELECT dispatch_key FROM public.worker_task_delivery_attestations WHERE id = ANY(v_atts)))
        AND NOT (x.job_id = j.id AND x.node_execution_id = ANY(v_nodes) AND x.dispatch_key = ANY(v_keys)))
        OR EXISTS (SELECT 1 FROM public.worker_event_emissions x WHERE x.id = ANY(v_emissions)
            AND NOT (x.job_id = j.id AND x.node_execution_id = ANY(v_nodes) AND x.source_task_attestation_id = ANY(v_atts)))
        OR EXISTS (SELECT 1 FROM public.registered_worker_event_receipts x WHERE x.id = ANY(v_receipts)
            AND NOT (x.job_id = j.id AND x.node_execution_id = ANY(v_nodes) AND x.source_task_attestation_id = ANY(v_atts)))
        OR EXISTS (SELECT 1 FROM public.legacy_worker_event_resolutions x WHERE x.job_id = j.id
            OR x.node_execution_id = ANY(v_nodes) OR EXISTS (SELECT 1 FROM public.worker_event_emissions y
                WHERE y.id = ANY(v_emissions) AND (x.redis_stream,x.consumer_group,x.message_id) = (y.redis_stream,y.consumer_group,y.message_id)))
        OR EXISTS (SELECT 1 FROM public.worker_redis_marker_cleanup_authorizations x WHERE x.source_id = ANY(v_emissions)
            OR x.source_id IN (SELECT id FROM public.worker_task_dispatches WHERE dispatch_key = ANY(v_keys)))
        OR EXISTS (SELECT 1 FROM public.worker_redis_marker_repair_audits x WHERE x.source_id = ANY(v_emissions)
            OR x.source_id IN (SELECT id FROM public.worker_task_dispatches WHERE dispatch_key = ANY(v_keys)))
    THEN RETURN false; END IF;

    SELECT COALESCE(array_agg(id), '{}'::uuid[]) INTO v_queues FROM public.channel_ops_queue_items
    WHERE channel_profile_id = c.id OR payload_json->>'production_task_id' = t.id::text OR payload_json->>'job_id' = j.id::text;
    IF EXISTS (SELECT 1 FROM public.channel_ops_queue_items WHERE parent_queue_item_id = ANY(v_queues) AND NOT id = ANY(v_queues))
    THEN RETURN false; END IF;
    FOR q IN SELECT * FROM public.channel_ops_queue_items WHERE id = ANY(v_queues) LOOP
        IF NOT COALESCE(q.channel_profile_id = c.id AND q.locked_at IS NULL AND q.locked_by IS NULL
            AND (q.parent_queue_item_id IS NULL OR q.parent_queue_item_id = ANY(v_queues))
            AND ((q.status = 'succeeded' AND q.last_error IS NULL AND q.dead_letter_at IS NULL)
                OR (q.status = 'dead_lettered' AND q.last_error = j.error_message
                    AND q.dead_letter_at <= p_now)), false) THEN RETURN false; END IF;
    END LOOP;

    FOR n IN SELECT * FROM public.node_executions WHERE id = ANY(v_nodes) LOOP
        SELECT value INTO STRICT v_spec FROM jsonb_array_elements(v_pipeline->'nodes') WHERE value->>'id' = n.node_id;
        v_config := COALESCE(v_spec->'data'->'config', '{}'::jsonb);
        IF v_spec->'data'->>'asset_id' IS NOT NULL THEN
            v_config := v_config || jsonb_build_object('asset_id', v_spec->'data'->>'asset_id');
        END IF;
        IF NOT COALESCE(n.job_id = j.id AND n.node_type = v_spec->>'type' AND n.node_config::jsonb = v_config
            AND n.status::text IN ('SUCCEEDED','CANCELLED') AND n.completed_at AT TIME ZONE 'UTC' <= v_cancel
            AND n.input_artifact_ids IS NOT NULL, false) THEN RETURN false; END IF;
        IF n.status::text = 'CANCELLED' AND NOT COALESCE(n.worker_id IS NULL AND n.output_artifact_id IS NULL
            AND n.completed_at AT TIME ZONE 'UTC' = v_cancel, false) THEN RETURN false; END IF;
        IF n.status::text = 'SUCCEEDED' THEN
            SELECT * INTO art FROM public.artifacts WHERE id = n.output_artifact_id;
            IF NOT FOUND OR NOT COALESCE(art.job_id = j.id AND art.node_execution_id = n.id
                AND n.started_at <= n.completed_at AT TIME ZONE 'UTC', false) THEN RETURN false; END IF;
        END IF;
        IF EXISTS (SELECT 1 FROM unnest(n.input_artifact_ids) AS input_id(value) WHERE NOT EXISTS
            (SELECT 1 FROM public.artifacts x WHERE x.id = input_id.value AND x.job_id = j.id AND x.node_execution_id = ANY(v_nodes)))
        THEN RETURN false; END IF;
        IF n.node_type = 'source' THEN
            SELECT * INTO asset FROM public.assets WHERE id = (n.node_config->>'asset_id')::uuid;
            IF NOT FOUND OR NOT COALESCE(n.status::text = 'SUCCEEDED' AND cardinality(n.input_artifact_ids) = 0
                AND n.worker_id IS NULL AND n.worker_registration_id IS NULL AND n.worker_lease_epoch IS NULL
                AND (art.filename,art.mime_type,art.file_size,art.storage_backend,art.storage_path)
                    IS NOT DISTINCT FROM (asset.filename,asset.mime_type,asset.file_size,asset.storage_backend,asset.storage_path)
                AND art.media_info->>'source_asset_id' = asset.id::text AND art.media_info->>'asset_id' = asset.id::text,
                false) OR EXISTS (SELECT 1 FROM public.worker_task_dispatches WHERE node_execution_id = n.id)
            THEN RETURN false; END IF;
        ELSE
            SELECT count(*) INTO v_count FROM public.worker_task_dispatches WHERE node_execution_id = n.id AND job_id = j.id;
            IF (n.id <> o.node_execution_id AND v_count <> 1)
                OR (n.id = o.node_execution_id AND (v_count NOT IN (1,2)
                    OR (SELECT count(*) FROM public.worker_task_delivery_attestations WHERE node_execution_id = n.id) <> 1))
            THEN RETURN false; END IF;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM public.artifacts x WHERE (x.job_id = j.id OR x.node_execution_id = ANY(v_nodes))
        AND NOT (x.job_id = j.id AND x.node_execution_id = ANY(v_nodes)
            AND EXISTS (SELECT 1 FROM public.node_executions n2 WHERE n2.id = x.node_execution_id AND n2.output_artifact_id = x.id)))
    THEN RETURN false; END IF;

    FOR d IN SELECT * FROM public.worker_task_dispatches WHERE dispatch_key = ANY(v_keys) LOOP
        SELECT * INTO n FROM public.node_executions WHERE id = d.node_execution_id;
        v_stream := CASE
            WHEN n.node_type IN ('transcode','trim','concat_timeline','concat_vertical_timeline','concat_many',
                'concat_horizontal','concat_vertical','export','bgm','watermark','replace_audio','vertical_crop','title_overlay','montage_assembler') THEN 'ffmpeg_go'
            WHEN n.node_type = 'smart_trim' THEN 'vision'
            WHEN n.node_type = 'youtube_upload' THEN 'youtube_publisher'
            ELSE NULL END;
        SELECT COALESCE(jsonb_object_agg(edge->>'targetHandle', x.output_artifact_id), '{}'::jsonb), count(*)
        INTO v_inputs, v_count FROM jsonb_array_elements(v_pipeline->'edges') edge
        JOIN public.node_executions x ON x.id = ANY(v_nodes) AND x.node_id = edge->>'source'
        WHERE edge->>'target' = n.node_id;
        IF v_count <> (SELECT count(*) FROM jsonb_object_keys(v_inputs)) THEN RETURN false; END IF;
        IF NOT COALESCE(d.job_id = j.id AND n.id = ANY(v_nodes) AND d.redis_stream = 'vp:tasks:' || v_stream
            AND d.consumer_group = v_stream || '-workers' AND d.delivery_error IS NULL
            AND d.created_at <= p_now AND d.payload_json->>'job_id' = j.id::text
            AND d.payload_json->>'node_execution_id' = n.id::text AND d.payload_json->>'node_id' = n.node_id
            AND d.payload_json->>'node_type' = n.node_type AND d.payload_json->>'dispatch_key' = d.dispatch_key::text
            AND (d.payload_json->>'config')::jsonb = n.node_config::jsonb
            AND (d.payload_json->>'input_artifacts')::jsonb = v_inputs
            AND (SELECT array_agg(value::uuid ORDER BY value::uuid) FROM jsonb_each_text(v_inputs))
                IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM unnest(n.input_artifact_ids) id)
            AND public.vp_registered_consumer_payload_sha256(d.payload_json::jsonb) = d.payload_sha256, false)
        THEN RETURN false; END IF;
        IF d.origin_receipt_id IS NOT NULL THEN
            SELECT * INTO origin FROM public.registered_worker_event_receipts WHERE id = d.origin_receipt_id;
            IF NOT FOUND OR NOT COALESCE(origin.id = ANY(v_receipts) AND origin.job_id = j.id
                AND origin.application_state = 'applied' AND origin.accepted_at <= d.created_at
                AND ((origin.event_type = 'node_failed' AND origin.node_execution_id = n.id)
                    OR (origin.event_type = 'node_completed' AND EXISTS (
                        SELECT 1 FROM jsonb_array_elements(v_pipeline->'edges') edge
                        JOIN public.node_executions x ON x.node_id = edge->>'source' AND x.id = origin.node_execution_id
                        WHERE edge->>'target' = n.node_id))), false) THEN RETURN false; END IF;
        END IF;
        SELECT count(*) INTO v_att_count FROM public.worker_task_delivery_attestations WHERE dispatch_key = d.dispatch_key;
        IF d.resolution_state = 'cancelled' THEN
            IF NOT COALESCE(v_att_count = 0 AND n.status::text = 'CANCELLED'
                AND d.delivery_state IN ('pending','cancelled') AND d.delivery_attempted_at IS NULL
                AND d.redis_message_id IS NULL AND d.delivered_at IS NULL AND d.acknowledged_at IS NULL
                AND v_cancel <= d.cancelled_at AND d.cancelled_at <= p_now
                AND n.worker_registration_id IS NULL AND n.worker_lease_epoch IS NULL
                AND n.worker_id IS NULL AND n.started_at IS NULL, false) THEN RETURN false; END IF;
        ELSE
            IF NOT COALESCE(d.delivery_state = 'delivered' AND d.resolution_state = 'acknowledged'
                AND d.cancelled_at IS NULL AND d.redis_message_id ~ '^[0-9]+-[0-9]+$'
                AND d.created_at <= d.delivery_attempted_at AND d.delivery_attempted_at <= d.delivered_at
                AND d.delivered_at <= d.acknowledged_at AND d.acknowledged_at <= p_now, false)
            THEN RETURN false; END IF;
            IF v_att_count = 0 THEN
                SELECT * INTO origin FROM public.registered_worker_event_receipts WHERE id = d.origin_receipt_id;
                IF NOT FOUND OR NOT COALESCE(n.id = o.node_execution_id AND n.status::text = 'CANCELLED'
                    AND origin.id = ANY(v_receipts) AND origin.event_type = 'node_failed' AND origin.node_execution_id = n.id
                    AND origin.application_state = 'applied' AND origin.ack_state = 'acknowledged'
                    AND origin.worker_registration_id = n.worker_registration_id AND origin.worker_lease_epoch = n.worker_lease_epoch
                    AND origin.worker_started_at = n.started_at AND d.acknowledged_at >= v_cancel
                    AND origin.payload_json->>'task_dispatch_key' <> d.dispatch_key::text, false)
                    OR EXISTS (SELECT 1 FROM public.worker_event_emissions WHERE payload_json->>'task_dispatch_key' = d.dispatch_key::text)
                THEN RETURN false; END IF;
            ELSIF v_att_count <> 1 THEN RETURN false;
            END IF;
        END IF;
    END LOOP;

    FOR att IN SELECT * FROM public.worker_task_delivery_attestations WHERE id = ANY(v_atts) LOOP
        SELECT * INTO STRICT d FROM public.worker_task_dispatches WHERE dispatch_key = att.dispatch_key;
        SELECT * INTO STRICT n FROM public.node_executions WHERE id = att.node_execution_id;
        SELECT * INTO STRICT e FROM public.worker_event_emissions WHERE source_task_attestation_id = att.id;
        SELECT * INTO STRICT r FROM public.registered_worker_event_receipts WHERE source_task_attestation_id = att.id;
        SELECT * INTO STRICT reg FROM public.worker_registrations WHERE id = att.worker_registration_id;
        SELECT * INTO STRICT g FROM public.worker_admission_grants WHERE id = reg.grant_id;
        IF NOT COALESCE(att.job_id = j.id AND att.node_execution_id = d.node_execution_id AND att.dispatch_key = ANY(v_keys)
            AND (att.redis_stream,att.consumer_group,att.message_id,att.payload_sha256)
                = (d.redis_stream,d.consumer_group,d.redis_message_id,d.payload_sha256)
            AND att.ack_state = 'acknowledged' AND att.acknowledged_at = d.acknowledged_at
            AND att.worker_started_at <= att.attested_at AND att.attested_at <= att.acknowledged_at
            AND att.attested_at >= d.delivered_at
            AND (att.worker_registration_id,att.worker_lease_epoch,att.worker_started_at)
                = (n.worker_registration_id,n.worker_lease_epoch,n.started_at)
            AND reg.redis_consumer_id = att.worker_id AND att.worker_lease_epoch > 0 AND att.worker_lease_epoch <= reg.lease_epoch
            AND g.activated_at <= reg.registered_at AND reg.registered_at <= att.worker_started_at
            AND (reg.service_name,reg.worker_type,reg.worker_host,reg.capabilities_json,reg.image_identity,reg.database_principal)
                IS NOT DISTINCT FROM (g.service_name,g.worker_type,g.worker_host,g.capabilities_json,g.image_identity,g.database_principal)
            AND g.redis_stream = d.redis_stream AND g.redis_group = d.consumer_group
            AND reg.database_fingerprint ~ '^[0-9a-f]{64}$' AND reg.redis_fingerprint ~ '^[0-9a-f]{64}$'
            AND reg.storage_fingerprint ~ '^[0-9a-f]{64}$'
            AND (e.job_id,e.node_execution_id,e.worker_registration_id,e.worker_lease_epoch,e.worker_id,e.worker_started_at)
                = (att.job_id,att.node_execution_id,att.worker_registration_id,att.worker_lease_epoch,att.worker_id,att.worker_started_at)
            AND (r.job_id,r.node_execution_id,r.worker_registration_id,r.worker_lease_epoch,r.worker_id,r.worker_started_at)
                = (att.job_id,att.node_execution_id,att.worker_registration_id,att.worker_lease_epoch,att.worker_id,att.worker_started_at)
            AND e.emission_state = 'resolved' AND att.attested_at <= e.prepared_at
            AND e.prepared_at <= e.emitted_at AND e.emitted_at <= e.resolved_at AND e.resolved_at <= p_now
            AND r.application_state = 'applied' AND r.ack_state = 'acknowledged' AND r.source_task_ack_state = 'acknowledged'
            AND r.accepted_at <= r.applied_at AND r.applied_at <= r.acknowledged_at AND r.acknowledged_at <= p_now
            AND r.source_task_acknowledged_at = att.acknowledged_at
            AND ((att.ack_event_emission_id = e.id) OR (att.ack_event_emission_id IS NULL AND r.applied_at <= att.acknowledged_at))
            AND (e.redis_stream,e.consumer_group,e.message_id,e.payload_sha256,e.payload_json,e.event_type)
                IS NOT DISTINCT FROM (r.redis_stream,r.consumer_group,r.message_id,r.payload_sha256,r.payload_json,r.event_type)
            AND r.redis_stream = 'vp:events' AND r.consumer_group = 'orchestrator' AND r.message_id ~ '^[0-9]+-[0-9]+$'
            AND (r.source_task_stream,r.source_task_group,r.source_task_message_id) = (d.redis_stream,d.consumer_group,d.redis_message_id)
            AND public.vp_registered_consumer_payload_sha256(r.payload_json::jsonb) = r.payload_sha256
            AND r.payload_json->>'event' = r.event_type AND r.payload_json->>'job_id' = j.id::text
            AND r.payload_json->>'node_execution_id' = n.id::text AND r.payload_json->>'worker_id' = att.worker_id
            AND r.payload_json->>'worker_registration_id' = att.worker_registration_id::text
            AND r.payload_json->>'worker_lease_epoch' = att.worker_lease_epoch::text
            AND (r.payload_json->>'started_at')::timestamptz = att.worker_started_at
            AND r.payload_json->>'task_stream' = d.redis_stream AND r.payload_json->>'task_group' = d.consumer_group
            AND r.payload_json->>'task_message_id' = d.redis_message_id AND r.payload_json->>'task_payload_sha256' = d.payload_sha256
            AND r.payload_json->>'task_dispatch_key' = d.dispatch_key::text
            AND ((r.event_type = 'node_completed' AND n.status::text = 'SUCCEEDED' AND n.worker_id = att.worker_id
                    AND r.payload_json->>'output_artifact_id' = n.output_artifact_id::text)
                OR (r.event_type = 'node_failed' AND n.id = o.node_execution_id AND n.status::text = 'CANCELLED' AND n.worker_id IS NULL)), false)
        THEN RETURN false; END IF;
        IF NOT EXISTS (SELECT 1 FROM public.registered_worker_event_deliveries x WHERE x.receipt_id = r.id
            AND x.source_task_attestation_id = att.id AND x.message_id = r.message_id) THEN RETURN false; END IF;
    END LOOP;
    FOR delivery IN SELECT * FROM public.registered_worker_event_deliveries x
        WHERE x.source_task_attestation_id = ANY(v_atts) OR x.receipt_id = ANY(v_receipts)
            OR EXISTS (SELECT 1 FROM public.worker_event_emissions y WHERE y.id = ANY(v_emissions)
                AND (x.redis_stream,x.consumer_group,x.message_id) = (y.redis_stream,y.consumer_group,y.message_id)) LOOP
        SELECT * INTO STRICT r FROM public.registered_worker_event_receipts WHERE id = delivery.receipt_id;
        IF NOT COALESCE(r.id = ANY(v_receipts) AND delivery.source_task_attestation_id = r.source_task_attestation_id
            AND delivery.resolution_state = 'accepted' AND delivery.reason_code IS NULL AND delivery.ack_state = 'acknowledged'
            AND delivery.accepted_at <= delivery.acknowledged_at AND delivery.acknowledged_at <= p_now
            AND delivery.redis_stream = r.redis_stream AND delivery.consumer_group = r.consumer_group
            AND delivery.message_id ~ '^[0-9]+-[0-9]+$' AND delivery.payload_sha256 = r.payload_sha256, false)
        THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
EXCEPTION WHEN data_exception OR no_data_found OR too_many_rows THEN RETURN false;
END;
$proof$;
""",
        r"""
CREATE FUNCTION public.vp_registered_consumer_uploads_quiescent() RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $bounded$
DECLARE v_table text; v_rows bigint; v_bytes bigint; v_total bigint := 2; v_id uuid;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.youtube_upload_operations WHERE status NOT IN ('succeeded','failed'))
    THEN RETURN true; END IF;
    -- Fixed proof inputs only. A sentinel and serialized byte count reject overflow;
    -- no table truncation, mutable catalog, unbounded returned snapshot or row locks.
    FOREACH v_table IN ARRAY ARRAY[
        'youtube_upload_operations','production_tasks','jobs','node_executions','artifacts','assets',
        'channel_profiles','publishing_accounts','publication_records','publication_promotion_operations',
        'worker_task_dispatches','worker_task_delivery_attestations','worker_event_emissions',
        'registered_worker_event_receipts','registered_worker_event_deliveries','worker_registrations',
        'worker_admission_grants','channel_ops_queue_items','legacy_worker_event_resolutions',
        'worker_redis_marker_cleanup_authorizations','worker_redis_marker_repair_audits'
    ] LOOP
        EXECUTE format('SELECT count(*), COALESCE(sum(octet_length(convert_to(to_jsonb(r)::text, ''UTF8'')) + 1),0) '
            || 'FROM (SELECT * FROM public.%I LIMIT 4097) r', v_table) INTO v_rows, v_bytes;
        v_total := v_total + v_bytes + octet_length(v_table) + 6;
        IF v_rows > 4096 OR v_total > 16777216 THEN RETURN false; END IF;
    END LOOP;
    FOR v_id IN SELECT id FROM public.youtube_upload_operations
        WHERE status NOT IN ('succeeded','failed') ORDER BY id LOOP
        IF public.vp_registered_consumer_terminal_upload(v_id, statement_timestamp()) IS NOT TRUE
        THEN RETURN false; END IF;
    END LOOP;
    RETURN true;
END;
$bounded$;
""",
    )


def _revoke_helpers() -> None:
    for signature in HELPERS:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        op.execute(f"""
DO $acl$ DECLARE r record; BEGIN
    FOR r IN SELECT DISTINCT role.rolname FROM pg_catalog.pg_proc p
        CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) acl
        JOIN pg_catalog.pg_roles role ON role.oid = acl.grantee
        WHERE p.oid = '{signature}'::regprocedure AND acl.grantee <> p.proowner
    LOOP EXECUTE format('REVOKE ALL ON FUNCTION {signature} FROM %I', r.rolname); END LOOP;
END $acl$;
""")


def upgrade() -> None:
    replacement = _replacement()
    for statement in helper_statements():
        op.execute(statement)
    _revoke_helpers()
    op.execute(replacement)


def downgrade() -> None:
    op.execute(_replacement(downgrade=True))
    for signature in reversed(HELPERS):
        op.execute(f"DROP FUNCTION {signature}")
