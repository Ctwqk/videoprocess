"""Fence native retirement writers and retain approved manifest-referenced facts."""
from alembic import op
import json
import sqlalchemy as sa


revision = "040_owned_history_seal"
down_revision = "039_registered_consumer_guard"
branch_labels = None
depends_on = None

# Read the installed definitions, including the merged ACK fixes. No old RPC body
# is copied, renamed, re-granted, or replaced by an alternative protocol.
ENTRY_JOBS = {name: "p_job_id" for name in (
    "vp_attest_worker_task_delivery", "vp_claim_worker_node", "vp_require_worker_node_claim",
    "vp_persist_worker_artifact", "vp_prepare_worker_event_emission", "vp_reserve_worker_youtube_upload",
    "vp_recover_registered_worker_node", "vp_resolve_worker_event_authority_for_job_deletion",
)}
ENTRY_JOBS.update({
    "vp_transition_worker_youtube_upload": "(SELECT job_id FROM public.youtube_upload_operations WHERE id = p_operation_id)",
    "vp_mark_worker_event_emitted": "(SELECT job_id FROM public.worker_event_emissions WHERE id = p_emission_id)",
    "vp_promote_observed_worker_event_emission": "(SELECT job_id FROM public.worker_event_emissions WHERE id = p_emission_id)",
    "vp_require_worker_task_ack_receipt": "(SELECT job_id FROM public.worker_task_dispatches WHERE dispatch_key = p_dispatch_key)",
    "vp_authorize_worker_task_ack": "(SELECT job_id FROM public.worker_task_delivery_attestations WHERE id = p_attestation_id)",
    "vp_acknowledge_worker_task_delivery": "(SELECT job_id FROM public.worker_task_delivery_attestations WHERE id = p_attestation_id)",
    "vp_acknowledge_proven_worker_task_dispatch": "(SELECT job_id FROM public.worker_task_delivery_attestations WHERE id = p_attestation_id)",
    "vp_authorize_cancelled_worker_task_ack": "(SELECT job_id FROM public.worker_task_dispatches WHERE id = p_dispatch_id)",
    "vp_require_cancelled_worker_task_ack": "(SELECT job_id FROM public.worker_task_dispatches WHERE id = p_dispatch_id)",
    "vp_acknowledge_cancelled_worker_task": "(SELECT job_id FROM public.worker_task_dispatches WHERE id = p_dispatch_id)",
    "vp_release_registered_retry_claim": "(SELECT job_id FROM public.registered_worker_event_receipts WHERE id = p_receipt_id)",
    "vp_claim_worker_redis_marker_cleanup": "NULL::uuid",
})

TABLES = (
    "channel_profiles", "publishing_accounts", "production_tasks", "jobs", "node_executions",
    "youtube_upload_operations", "assets", "artifacts", "manual_seeds", "publication_records",
    "publication_promotion_operations", "worker_task_dispatches", "worker_task_delivery_attestations",
    "worker_event_emissions", "registered_worker_event_receipts", "registered_worker_event_deliveries",
    "worker_registrations", "worker_admission_grants", "legacy_worker_event_resolutions", "channel_ops_queue_items",
    "worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits", "runtime_schedules",
)


def _fence_installed_functions(*, remove: bool = False) -> None:
    mapping = str(sa.literal(json.dumps(ENTRY_JOBS)).compile(compile_kwargs={"literal_binds": True}))
    op.execute("""DO $patch$
DECLARE v_name text; v_expression text; v_source text; v_definition text; v_changed text;
        v_injection text; v_count integer; v_pos integer;
BEGIN
    FOR v_name, v_expression IN SELECT key, value FROM jsonb_each_text($MAPPING$::jsonb) LOOP
        SELECT count(*) INTO v_count FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = v_name;
        IF v_count <> 1 THEN RAISE EXCEPTION 'owned_history_function_inventory_changed'; END IF;
        SELECT p.prosrc, pg_catalog.pg_get_functiondef(p.oid) INTO v_source, v_definition
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = v_name;
        v_pos := strpos(v_source, E'\\nBEGIN\\n');
        IF v_pos = 0 OR length(v_source) = 0 OR
           length(v_definition) - length(replace(v_definition, v_source, '')) <> length(v_source) THEN
            RAISE EXCEPTION 'owned_history_function_definition_changed';
        END IF;
        v_injection := E'    -- owned_history_entry_040\\n    PERFORM public.vp_owned_history_job_entry(' || v_expression || E');\\n';
        IF $REMOVE$ THEN
            IF length(v_source) - length(replace(v_source, v_injection, '')) <> length(v_injection) THEN
                RAISE EXCEPTION 'owned_history_function_definition_changed';
            END IF;
            v_changed := replace(v_source, v_injection, '');
        ELSE
            IF strpos(v_source, 'owned_history_entry_040') > 0 THEN
                RAISE EXCEPTION 'owned_history_function_definition_changed';
            END IF;
            v_changed := overlay(v_source placing E'\\nBEGIN\\n' || v_injection from v_pos for 7);
        END IF;
        EXECUTE replace(v_definition, v_source, v_changed);
    END LOOP;
END $patch$;""".replace("$MAPPING$", mapping).replace("$REMOVE$", "TRUE" if remove else "FALSE"))


ENTRY_SQL = """
CREATE FUNCTION public.vp_owned_history_job_entry(p_job_id uuid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $f$
DECLARE v_channel uuid; v_count bigint; v_references jsonb; v_fresh_references jsonb;
BEGIN
    SELECT COALESCE(jsonb_agg(jsonb_build_array(id, channel_profile_id) ORDER BY id), '[]'::jsonb)
    INTO v_references FROM public.production_tasks WHERE job_id = p_job_id;
    SELECT count(*) INTO v_count FROM public.production_tasks WHERE job_id = p_job_id;
    IF v_count > 1 THEN
        RAISE EXCEPTION USING MESSAGE = 'task_authority_changed', ERRCODE = 'P0001';
    END IF;
    FOR v_channel IN SELECT DISTINCT channel_profile_id FROM public.production_tasks
                     WHERE job_id = p_job_id ORDER BY channel_profile_id LOOP
        PERFORM 1 FROM public.channel_profiles WHERE id = v_channel FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING MESSAGE = 'channel_authority_changed', ERRCODE = 'P0001';
        END IF;
    END LOOP;
    PERFORM 1 FROM public.runtime_schedules WHERE service_name = 'videoprocess' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING MESSAGE = 'schedule_authority_changed', ERRCODE = 'P0001';
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_array(id, channel_profile_id) ORDER BY id), '[]'::jsonb)
    INTO v_fresh_references FROM public.production_tasks WHERE job_id = p_job_id;
    IF v_references IS DISTINCT FROM v_fresh_references THEN
        RAISE EXCEPTION USING MESSAGE = 'task_authority_changed', ERRCODE = 'P0001';
    END IF;
    IF EXISTS (SELECT 1 FROM public.owned_seed_inventories
        WHERE approved_at IS NOT NULL AND manifest_json::jsonb->'legacy_history'->'retired_unassigned_preupload'->>'job_id' = p_job_id::text) THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_history_sealed', ERRCODE = 'P0001';
    END IF;
END;
$f$;
"""


GUARD_SQL = """
CREATE FUNCTION public.vp_owned_history_seal_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $f$
DECLARE
    v_before jsonb; v_after jsonb; v_row jsonb; v_history jsonb; v_cert jsonb; v_binding jsonb;
    v_retained text; v_pinned boolean; v_allowed text[] := ARRAY[]::text[];
BEGIN
    v_before := CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END;
    v_after := CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END;
    IF TG_OP = 'UPDATE' AND v_before IS NOT DISTINCT FROM v_after THEN RETURN NEW; END IF;
    -- This trigger is a backstop, not a late-lock serialization mechanism.
    -- Covered native writers acquire the channel/schedule fence at entry.
    FOR v_history IN SELECT manifest_json::jsonb->'legacy_history' FROM public.owned_seed_inventories
                     WHERE approved_at IS NOT NULL AND manifest_json::jsonb->>'version' = '2' LOOP
        FOR v_binding IN SELECT value FROM jsonb_array_elements(v_history->'bindings') LOOP
            IF TG_TABLE_NAME = 'publishing_accounts' AND
               (v_before->>'id' = v_binding->>'legacy_account_id' OR v_after->>'id' = v_binding->>'legacy_account_id') AND
               (TG_OP <> 'UPDATE' OR
                v_before - ARRAY['account_label','last_token_check_status','last_token_check_at','updated_at'] IS DISTINCT FROM
                v_after - ARRAY['account_label','last_token_check_status','last_token_check_at','updated_at']) THEN
                RAISE EXCEPTION USING MESSAGE = 'owned_history_sealed', ERRCODE = 'P0001';
            END IF;
            IF TG_TABLE_NAME = 'channel_profiles' AND
               (v_before->>'id' = v_binding->>'legacy_channel_profile_id' OR v_after->>'id' = v_binding->>'legacy_channel_profile_id') AND
               (TG_OP <> 'UPDATE' OR
                (v_before->'enabled',v_before->'dry_run',v_before->'halted_at',v_before->'halt_reason',v_before->'intake_paused_at',v_before->'intake_pause_reason')
                IS DISTINCT FROM
                (v_after->'enabled',v_after->'dry_run',v_after->'halted_at',v_after->'halt_reason',v_after->'intake_paused_at',v_after->'intake_pause_reason')) THEN
                RAISE EXCEPTION USING MESSAGE = 'owned_history_sealed', ERRCODE = 'P0001';
            END IF;
            IF TG_TABLE_NAME IN ('production_tasks','manual_seeds') AND
               (v_before->>'target_account_id' = v_binding->>'legacy_account_id' OR v_after->>'target_account_id' = v_binding->>'legacy_account_id') AND
               (TG_OP <> 'UPDATE' OR
                (v_before->'id',v_before->'target_account_id',v_before->'channel_profile_id',v_before->'job_id') IS DISTINCT FROM
                (v_after->'id',v_after->'target_account_id',v_after->'channel_profile_id',v_after->'job_id')) THEN
                RAISE EXCEPTION USING MESSAGE = 'owned_history_sealed', ERRCODE = 'P0001';
            END IF;
        END LOOP;
        v_cert := v_history->'retired_unassigned_preupload';
        IF v_cert IS NULL OR v_cert = 'null'::jsonb THEN CONTINUE; END IF;
        v_retained := CASE TG_TABLE_NAME
            WHEN 'channel_profiles' THEN 'channel' WHEN 'publishing_accounts' THEN 'account'
            WHEN 'production_tasks' THEN 'task' WHEN 'jobs' THEN 'job' WHEN 'node_executions' THEN 'upload_node'
            WHEN 'youtube_upload_operations' THEN 'operation' WHEN 'manual_seeds' THEN 'manual_seed' ELSE NULL END;
        FOR v_row IN SELECT value FROM jsonb_array_elements(jsonb_build_array(v_before, v_after)) LOOP
            IF v_row = 'null'::jsonb THEN CONTINUE; END IF;
            v_pinned := COALESCE(v_row->>'id' = v_cert->'retained_facts'->v_retained->>'id', FALSE)
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(v_cert->'terminal_graph'->TG_TABLE_NAME, '[]'::jsonb)) r
                           WHERE r->>'id' = v_row->>'id')
                OR (TG_TABLE_NAME = 'assets' AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(v_cert->'retained_facts'->'source_assets') s WHERE s->'asset'->>'id' = v_row->>'id'))
                OR COALESCE(v_row->>'job_id' = v_cert->>'job_id', FALSE)
                OR COALESCE(v_row->>'production_task_id' = v_cert->>'task_id', FALSE)
                OR (TG_TABLE_NAME = 'jobs' AND COALESCE(v_row->>'parent_job_id' = v_cert->>'job_id', FALSE))
                OR (TG_TABLE_NAME = 'runtime_schedules' AND COALESCE(v_row->>'guarded_job_id' = v_cert->>'job_id', FALSE))
                OR (TG_TABLE_NAME IN ('production_tasks','manual_seeds','publishing_accounts','channel_ops_queue_items') AND
                    COALESCE(v_row->>'channel_profile_id' = v_cert->>'legacy_channel_profile_id', FALSE))
                OR (TG_TABLE_NAME IN ('production_tasks','manual_seeds') AND
                    COALESCE(v_row->>'target_account_id' = v_cert->>'legacy_account_id', FALSE))
                OR COALESCE(v_row->'payload_json'->>'production_task_id' = v_cert->>'task_id', FALSE)
                OR COALESCE(v_row->'payload_json'->>'job_id' = v_cert->>'job_id', FALSE)
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'node_executions') n
                           WHERE n->>'id' = v_row->>'node_execution_id')
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'worker_task_dispatches') d
                           WHERE d->>'dispatch_key' IN (v_row->>'dispatch_key', v_row->'payload_json'->>'task_dispatch_key')
                              OR d->>'id' = v_row->>'source_id')
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'worker_task_delivery_attestations') a
                           WHERE a->>'id' = v_row->>'source_task_attestation_id')
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'registered_worker_event_receipts') r
                           WHERE r->>'id' IN (v_row->>'receipt_id', v_row->>'origin_receipt_id'))
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'worker_event_emissions') e
                           WHERE e->>'id' = v_row->>'source_id'
                              OR (e->>'redis_stream',e->>'consumer_group',e->>'message_id') =
                                 (v_row->>'redis_stream',v_row->>'consumer_group',v_row->>'message_id'))
                OR (TG_TABLE_NAME = 'channel_ops_queue_items' AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(v_cert->'terminal_graph'->'channel_ops_queue_items') q
                    WHERE q->>'id' = v_row->>'parent_queue_item_id'));
            IF NOT v_pinned THEN CONTINUE; END IF;
            v_allowed := CASE TG_TABLE_NAME
                WHEN 'worker_registrations' THEN ARRAY['heartbeat_at','lease_expires_at','status','revoked_at','revoke_reason','superseded_by']
                WHEN 'worker_admission_grants' THEN ARRAY['state','revoked_at','revoke_reason','updated_at']
                ELSE ARRAY[]::text[] END;
            IF TG_OP <> 'UPDATE' OR v_before - v_allowed IS DISTINCT FROM v_after - v_allowed THEN
                RAISE EXCEPTION USING MESSAGE = 'owned_history_sealed', ERRCODE = 'P0001';
            END IF;
        END LOOP;
    END LOOP;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$f$;
"""


def upgrade() -> None:
    op.execute(ENTRY_SQL)
    op.execute("REVOKE ALL ON FUNCTION public.vp_owned_history_job_entry(uuid) FROM PUBLIC")
    op.execute(GUARD_SQL)
    op.execute("REVOKE ALL ON FUNCTION public.vp_owned_history_seal_guard() FROM PUBLIC")
    for table in TABLES:
        op.execute(f"CREATE TRIGGER owned_history_seal_{table} BEFORE INSERT OR UPDATE OR DELETE ON public.{table} "
                   "FOR EACH ROW EXECUTE FUNCTION public.vp_owned_history_seal_guard()")
    _fence_installed_functions()


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM public.owned_seed_inventories
            WHERE approved_at IS NOT NULL AND manifest_json::jsonb->>'version' = '2') THEN
            RAISE EXCEPTION 'owned_inventory_history_requires_preservation';
        END IF;
    END $$;""")
    _fence_installed_functions(remove=True)
    for table in TABLES:
        op.execute(f"DROP TRIGGER owned_history_seal_{table} ON public.{table}")
    op.execute("DROP FUNCTION public.vp_owned_history_seal_guard()")
    op.execute("DROP FUNCTION public.vp_owned_history_job_entry(uuid)")
