"""Retain truthful terminal legacy upload facts after queue retention gaps.

Revision ID: 046_owned_retained_terminal
Revises: 045_registered_consumer_history
"""
from __future__ import annotations

from pathlib import Path
import runpy

from alembic import op

revision = "046_owned_retained_terminal"
down_revision = "045_registered_consumer_history"
branch_labels = None
depends_on = None

HISTORY_SIGNATURE = "public.vp_owned_producer_history(json,uuid,text,timestamp with time zone)"
FACT_SIGNATURE = "public.vp_owned_retained_terminal_facts(json,json)"
CHECK_SIGNATURE = "public.vp_owned_producer_retained_terminal(json,json,json,timestamp with time zone)"
SEAL_SIGNATURE = "public.vp_owned_retained_terminal_seal_guard()"
TABLE_FIELDS = {
    "production_tasks": "task", "jobs": "job", "node_executions": "nodes",
    "artifacts": "artifacts", "youtube_upload_operations": "operations",
    "publication_records": "publications", "channel_ops_queue_items": "queues",
    "publication_metric_schedules": "metrics", "feedback_snapshots": "feedback",
    "publication_promotion_operations": "promotion_operations",
}
SEAL_TABLES = (*TABLE_FIELDS, "owned_seed_inventory_items", "worker_task_dispatches",
    "worker_task_delivery_attestations", "worker_event_emissions",
    "registered_worker_event_receipts", "registered_worker_event_deliveries")


def once(source, before, after):
    if source.count(before) != 1:
        raise RuntimeError("owned_retained_definition_changed")
    return source.replace(before, after, 1)


def original():
    return runpy.run_path(str(Path(__file__).with_name("042_owned_producer_fence.py")))


def facts_sql():
    predicates = {
        "operations": ("youtube_upload_operations", "r->>'production_task_id' = p_task->>'id'"),
        "nodes": ("node_executions", "r->>'job_id' = p_task->>'job_id'"),
        "artifacts": ("artifacts", "r->>'job_id' = p_task->>'job_id' AND r->>'id' IN (SELECT n->>'output_artifact_id' FROM json_array_elements(p_rows->'node_executions') n WHERE n->>'job_id' = p_task->>'job_id')"),
        "publications": ("publication_records", "r->>'production_task_id' = p_task->>'id'"),
        "metrics": ("publication_metric_schedules", "r->>'publication_id' = v_pub->>'id'"),
        "feedback": ("feedback_snapshots", "r->>'publication_id' = v_pub->>'id'"),
        "promotion_operations": ("publication_promotion_operations", "r->>'production_task_id' = p_task->>'id' OR r->>'publication_id' = v_pub->>'id'"),
    }
    fields = ["'task', p_task", "'job', public.vp_owned_producer_find(p_rows,'jobs',p_task->>'job_id')", "'queues', v_queues"]
    fields += [f"'{key}', (SELECT COALESCE(json_agg(r ORDER BY r->>'id'),'[]') FROM json_array_elements(p_rows->'{table}') r WHERE {predicate})"
               for key, (table, predicate) in predicates.items()]
    return """
CREATE FUNCTION public.vp_owned_retained_terminal_facts(p_rows json,p_task json) RETURNS json
LANGUAGE plpgsql SET search_path = pg_catalog AS $facts$
DECLARE v_pub json; v_queues json;
BEGIN
    IF (SELECT count(*) FROM json_array_elements(p_rows->'publication_records') p WHERE p->>'production_task_id'=p_task->>'id') <> 1
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    SELECT p INTO v_pub FROM json_array_elements(p_rows->'publication_records') p WHERE p->>'production_task_id'=p_task->>'id';
    WITH RECURSIVE queues AS (
        SELECT value row_data FROM json_array_elements(p_rows->'channel_ops_queue_items')
    ), related(id) AS (
        SELECT row_data->>'id' FROM queues WHERE row_data->'payload_json'->>'production_task_id'=p_task->>'id'
            OR row_data->'payload_json'->>'publication_id'=v_pub->>'id' OR row_data->'payload_json'->>'metric_schedule_id' IN (
                SELECT m->>'id' FROM json_array_elements(p_rows->'publication_metric_schedules') m WHERE m->>'publication_id'=v_pub->>'id')
        UNION
        SELECT child.row_data->>'id' FROM queues child JOIN queues ancestor ON child.row_data->>'parent_queue_item_id'=ancestor.row_data->>'id'
            OR child.row_data->>'id'=ancestor.row_data->>'parent_queue_item_id' JOIN related r ON ancestor.row_data->>'id'=r.id
    ) SELECT COALESCE(json_agg(row_data ORDER BY row_data->>'id'),'[]') INTO v_queues FROM queues WHERE row_data->>'id' IN (SELECT id FROM related);
    RETURN json_build_object(""" + ",\n".join(fields) + """);
END;
$facts$;
"""


def worker_graph_cte(*, live=False):
    tables = ("worker_task_dispatches", "worker_task_delivery_attestations", "worker_event_emissions", "registered_worker_event_receipts")
    def source(table):
        return (f"SELECT to_jsonb(t) r FROM public.{table} t" if live else
                f"SELECT value::jsonb r FROM json_array_elements(p_rows->'{table}')")
    union = " UNION ALL ".join(f"SELECT '{table}'::text kind,r FROM ({source(table)}) t" for table in tables)
    if live:
        union += " UNION SELECT TG_TABLE_NAME,after_row WHERE TG_TABLE_NAME=ANY(ARRAY['worker_task_dispatches','worker_task_delivery_attestations','worker_event_emissions','registered_worker_event_receipts']) AND after_row IS NOT NULL"
    job = "job_id" if live else "j->>'id'"
    nodes = "facts->'nodes'" if live else "(facts->'nodes')::jsonb"
    delivery = source("registered_worker_event_deliveries")
    if live:
        delivery += " UNION SELECT after_row WHERE TG_TABLE_NAME='registered_worker_event_deliveries' AND after_row IS NOT NULL"
    return f"""WITH RECURSIVE all_workers(kind,r) AS ({union}), linked(kind,r) AS (
        SELECT kind,r FROM all_workers WHERE r->>'job_id'={job}
            OR r->>'node_execution_id' IN (SELECT graph_node->>'id' FROM jsonb_array_elements({nodes}) graph_node)
        UNION
        SELECT c.kind,c.r FROM all_workers c JOIN linked p ON
            c.r->>'source_task_attestation_id'=CASE WHEN p.kind='worker_task_delivery_attestations' THEN p.r->>'id'
                WHEN p.kind IN ('worker_event_emissions','registered_worker_event_receipts') THEN p.r->>'source_task_attestation_id' END
            OR p.kind='worker_task_dispatches' AND (c.r->>'dispatch_key'=p.r->>'dispatch_key' OR c.r->'payload_json'->>'task_dispatch_key'=p.r->>'dispatch_key')
            OR (c.r->>'origin_receipt_id'=CASE WHEN p.kind='registered_worker_event_receipts' THEN p.r->>'id' WHEN p.kind='worker_task_dispatches' THEN p.r->>'origin_receipt_id' END)
            OR (c.r->>'id'=CASE WHEN p.kind='registered_worker_event_receipts' THEN p.r->>'id' WHEN p.kind='worker_task_dispatches' THEN p.r->>'origin_receipt_id' END)
    ), worker_graph(kind,r) AS (
        SELECT kind,r FROM linked
        UNION ALL SELECT 'registered_worker_event_deliveries',d.r FROM ({delivery}) d WHERE EXISTS (
            SELECT 1 FROM linked p WHERE d.r->>'source_task_attestation_id'=CASE WHEN p.kind='worker_task_delivery_attestations' THEN p.r->>'id'
                WHEN p.kind IN ('worker_event_emissions','registered_worker_event_receipts') THEN p.r->>'source_task_attestation_id' END
            OR d.r->>'receipt_id'=CASE WHEN p.kind='registered_worker_event_receipts' THEN p.r->>'id' WHEN p.kind='worker_task_dispatches' THEN p.r->>'origin_receipt_id' END
            OR p.kind IN ('worker_event_emissions','registered_worker_event_receipts') AND
                (d.r->>'redis_stream',d.r->>'consumer_group',d.r->>'message_id')=(p.r->>'redis_stream',p.r->>'consumer_group',p.r->>'message_id')))
"""


def check_sql():
    # Reuse the reviewed successful upload/job/node/receipt/publication predicates,
    # not the normal queue-chain acceptance. That helper itself stays untouched.
    prefix = original()["NORMAL_SQL"].split("BEGIN\n", 1)[1].split("    WITH RECURSIVE queues AS (", 1)[0]
    return """
CREATE FUNCTION public.vp_owned_producer_retained_terminal(p_rows json,p_cert json,p_binding json,p_now timestamptz) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $retained$
DECLARE p_task json; facts json; part json; row_data json; channel json; o jsonb; j jsonb; n jsonb; a jsonb; pub jsonb;
        promotion json; automatic json; m json; feedback json; v_start timestamptz; v_due timestamptz;
        v_grace timestamptz; v_observed timestamptz; v_stage text; v_hours integer; v_grace_hours integer;
        v_name text; v_ids jsonb; v_sorted jsonb; v_count integer;
BEGIN
    PERFORM public.vp_owned_producer_canonical(p_cert);
    IF NOT COALESCE((SELECT array_agg(key ORDER BY key) FROM json_each(p_cert)) =
        ARRAY['approval_reference','classification','legacy_account_id','legacy_channel_profile_id','observed_at','operation_id','publication_id','retained_facts','retained_facts_sha256','server_subject','task_id','version']
        AND (p_cert->'version')::text = '1' AND p_cert->>'classification'='retained_terminal_upload_queue_gap'
        AND octet_length(public.vp_owned_producer_canonical(p_cert)) <= 1048576
        AND p_cert->>'retained_facts_sha256' ~ '^[0-9a-f]{64}$'
        AND p_cert->>'observed_at' ~ '(Z|[+]00:00)$'
        AND json_typeof(p_cert->'server_subject')='string' AND length(btrim(p_cert->>'server_subject'))>0
        AND length(p_cert->>'server_subject')<=512
        AND json_typeof(p_cert->'approval_reference')='string' AND length(btrim(p_cert->>'approval_reference'))>0
        AND length(p_cert->>'approval_reference')<=512, FALSE)
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    FOREACH v_name IN ARRAY ARRAY['operation_id','task_id','publication_id','legacy_account_id','legacy_channel_profile_id'] LOOP
        IF NOT COALESCE(p_cert->>v_name ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',FALSE)
        THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    END LOOP;
    v_observed := (p_cert->>'observed_at')::timestamptz;
    IF NOT COALESCE(v_observed <= p_now AND p_binding->>'legacy_account_id'=p_cert->>'legacy_account_id'
        AND p_binding->>'legacy_channel_profile_id'=p_cert->>'legacy_channel_profile_id'
        AND p_binding::jsonb->'qualified_operation_ids' ? (p_cert->>'operation_id')
        AND (p_binding->'qualification'->>'observed_at')::timestamptz=v_observed
        AND p_binding->'qualification'->>'server_subject'=p_cert->>'server_subject'
        AND p_binding->'qualification'->>'approval_reference'=p_cert->>'approval_reference',FALSE)
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    -- Freeze the observation epoch; later wall time cannot age an invalid certificate into validity.
    p_now := v_observed;
    p_task := public.vp_owned_producer_find(p_rows,'production_tasks',p_cert->>'task_id');
    channel := public.vp_owned_producer_find(p_rows,'channel_profiles',p_cert->>'legacy_channel_profile_id');
    IF NOT COALESCE(json_typeof(channel->'enabled')='boolean' AND json_typeof(channel->'dry_run')='boolean'
        AND channel::jsonb ? 'halted_at' AND channel::jsonb ? 'intake_paused_at'
        AND (channel->>'halted_at' IS NULL OR (channel->>'halted_at')::timestamptz<=p_now)
        AND (channel->>'intake_paused_at' IS NULL OR (channel->>'intake_paused_at')::timestamptz<=p_now)
        AND ((channel->'enabled')::text='false' OR (channel->'dry_run')::text='true'
        OR channel->>'halted_at' IS NOT NULL OR channel->>'intake_paused_at' IS NOT NULL)
        AND public.vp_owned_producer_find(p_rows,'publishing_accounts',p_cert->>'legacy_account_id')->>'channel_profile_id'=channel->>'id',FALSE)
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    IF NOT COALESCE(p_task->>'target_account_id'=p_cert->>'legacy_account_id'
        AND p_task->>'channel_profile_id'=p_cert->>'legacy_channel_profile_id'
        AND p_task->>'state'='measured' AND (p_task->'retry_count')::text='0' AND COALESCE(p_task->>'failure_category','')=''
        AND NOT (COALESCE(p_task::jsonb->'agent_approval_evidence_json','{}') ? 'owned_inventory')
        AND NOT (COALESCE(p_task::jsonb->'channel_config_snapshot_json','{}') ? 'owned_inventory'),FALSE)
        OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'owned_seed_inventory_items') i WHERE i->>'production_task_id'=p_task->>'id')
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
""" + prefix + """
    facts := public.vp_owned_retained_terminal_facts(p_rows,p_task);
    IF (SELECT array_agg(key ORDER BY key) FROM json_each(p_cert->'retained_facts')) IS DISTINCT FROM
        ARRAY['artifacts','feedback','job','metrics','nodes','operations','promotion_operations','publications','queues','task']
        OR public.vp_owned_producer_hash(facts) IS DISTINCT FROM p_cert->>'retained_facts_sha256'
        OR public.vp_owned_producer_canonical(facts) IS DISTINCT FROM public.vp_owned_producer_canonical(p_cert->'retained_facts')
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    FOR v_name,part IN SELECT key,value FROM json_each(facts) WHERE key NOT IN ('task','job') LOOP
        IF json_typeof(part) <> 'array' OR json_array_length(part)>128
        THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
        SELECT jsonb_agg(r->>'id' ORDER BY ordinal),jsonb_agg(DISTINCT r->>'id' ORDER BY r->>'id') INTO v_ids,v_sorted
        FROM json_array_elements(part) WITH ORDINALITY a(r,ordinal);
        IF v_ids IS DISTINCT FROM v_sorted THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    END LOOP;
    IF NOT COALESCE(o->>'id'=p_cert->>'operation_id' AND pub->>'id'=p_cert->>'publication_id'
        AND (o->>'completed_at')::timestamptz <= p_now-INTERVAL '30 days'
        AND (o->>'completed_at')::timestamptz <= (pub->>'uploaded_at')::timestamptz
        AND (pub->>'uploaded_at')::timestamptz <= v_start
        AND json_array_length(facts->'nodes') BETWEEN 1 AND 128
        AND json_array_length(facts->'promotion_operations')=1 AND json_array_length(facts->'queues')=1,FALSE)
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    promotion := facts->'promotion_operations'->0; automatic := facts->'queues'->0;
    IF NOT COALESCE(promotion->>'production_task_id'=p_task->>'id' AND promotion->>'publication_id'=pub->>'id'
        AND promotion->>'platform_video_id'=o->>'platform_video_id' AND promotion->>'status'='finalized'
        AND promotion->>'error_message' IS NULL AND promotion->>'target_privacy'='unlisted'
        AND promotion->>'observed_privacy'='unlisted' AND promotion->>'observed_publish_status' IN ('uploaded','scheduled')
        AND (promotion->>'scheduled_at')::timestamptz=v_start
        AND promotion->>'attempt_key'='channelops-promotion:' || (promotion->>'id')
        AND (o->>'completed_at')::timestamptz <= (promotion->>'request_attempted_at')::timestamptz
        AND (promotion->>'request_attempted_at')::timestamptz <= (promotion->>'confirmed_at')::timestamptz
        AND (promotion->>'confirmed_at')::timestamptz <= (promotion->>'completed_at')::timestamptz
        AND (promotion->>'completed_at')::timestamptz <= p_now
        AND json_typeof(promotion->'decision_json')='object' AND promotion->'decision_json'->>'verdict'='allow'
        AND promotion::jsonb->'decision_json'->'decision_id' NOT IN ('null','false','0','""','[]','{}')
        AND promotion::jsonb->'decision_json'->'rules_version' NOT IN ('null','false','0','""','[]','{}')
        AND json_typeof(promotion->'evidence_json')='object' AND promotion::jsonb->'evidence_json'<>'{}'::jsonb
        AND promotion->>'queue_item_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',FALSE)
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    v_due := (pub->>'uploaded_at')::timestamptz+INTERVAL '1 hour';
    IF NOT COALESCE(automatic->>'kind'='promote_publication' AND automatic->>'status'='cancelled'
        AND automatic->>'last_error'='replaced_by_immediate_unlisted_canary_promotion'
        AND (automatic->'attempt_count')::text='0' AND automatic->>'locked_at' IS NULL AND automatic->>'locked_by' IS NULL
        AND automatic->>'channel_profile_id'=p_task->>'channel_profile_id'
        AND automatic->>'id'<>promotion->>'queue_item_id' AND (automatic->>'run_after')::timestamptz=v_due
        AND automatic->>'idempotency_key'='promote_publication:' || (pub->>'id') || ':unlisted:' || to_char(v_due AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
        AND automatic->'payload_json'->>'publication_id'=pub->>'id' AND automatic->'payload_json'->>'target_visibility'='unlisted'
        AND automatic->'payload_json'->>'scheduled_at'=to_char(v_due AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
        AND (pub->>'uploaded_at')::timestamptz <= (automatic->>'dead_letter_at')::timestamptz
        AND (automatic->>'dead_letter_at')::timestamptz <= (promotion->>'request_attempted_at')::timestamptz
        AND automatic->>'parent_queue_item_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND automatic->>'parent_queue_item_id' NOT IN (automatic->>'id',promotion->>'queue_item_id'),FALSE)
        OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'channel_ops_queue_items') q
            WHERE q->>'id' IN (automatic->>'parent_queue_item_id',promotion->>'queue_item_id')
            OR q->>'channel_profile_id'=p_task->>'channel_profile_id' AND q->>'status' NOT IN ('succeeded','cancelled')
            OR q->>'id'<>automatic->>'id' AND q->'payload_json'->>'job_id'=j->>'id')
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    IF json_array_length(facts->'metrics')<>5 OR json_array_length(facts->'feedback')<>6
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    FOR v_stage,v_hours,v_grace_hours IN VALUES ('1h',1,3),('6h',6,12),('24h',24,30),('72h',72,84),('7d',168,192) LOOP
        IF (SELECT count(*) FROM json_array_elements(facts->'metrics') r WHERE r->>'snapshot_stage'=v_stage)<>1
            OR (SELECT count(*) FROM json_array_elements(facts->'feedback') r WHERE r->>'snapshot_stage'=v_stage)<>1
        THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
        SELECT r INTO m FROM json_array_elements(facts->'metrics') r WHERE r->>'snapshot_stage'=v_stage;
        SELECT r INTO feedback FROM json_array_elements(facts->'feedback') r WHERE r->>'snapshot_stage'=v_stage;
        v_due:=v_start+make_interval(hours=>v_hours); v_grace:=v_start+make_interval(hours=>v_grace_hours);
        IF NOT COALESCE(m->>'publication_id'=pub->>'id' AND (m->>'effective_start_at')::timestamptz=v_start
            AND (m->>'due_at')::timestamptz=v_due AND (m->>'grace_until')::timestamptz=v_grace
            AND m->>'status'='succeeded' AND (m->'attempt_count')::text='1' AND m->>'last_error_code' IS NULL
            AND (m->>'completed_at')::timestamptz=(m->>'last_attempt_at')::timestamptz
            AND (m->>'completed_at')::timestamptz BETWEEN v_due AND v_grace
            AND (m->>'completed_at')::timestamptz <= p_now
            AND feedback->>'publication_id'=pub->>'id'
            AND (feedback->>'collected_at')::timestamptz BETWEEN v_due AND (m->>'completed_at')::timestamptz,FALSE)
        THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    END LOOP;
    IF (SELECT count(*) FROM json_array_elements(facts->'feedback') f WHERE f->>'snapshot_stage'='immediate'
        AND f->>'publication_id'=pub->>'id' AND (f->>'collected_at')::timestamptz BETWEEN (o->>'completed_at')::timestamptz AND p_now)<>1
        OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'jobs') child WHERE child->>'parent_job_id'=j->>'id')
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    IF EXISTS (SELECT 1 FROM json_array_elements(facts->'nodes') graph_node WHERE graph_node->>'worker_registration_id' IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM json_array_elements(p_rows->'worker_registrations') r
            JOIN json_array_elements(p_rows->'worker_admission_grants') g ON g->>'id'=r->>'grant_id'
            WHERE r->>'id'=graph_node->>'worker_registration_id'))
    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    FOR v_name,row_data IN """ + worker_graph_cte() + """ SELECT kind,r::json FROM worker_graph LOOP
        IF NOT COALESCE(CASE v_name
            WHEN 'worker_task_dispatches' THEN row_data->>'resolution_state' IN ('acknowledged','cancelled')
            WHEN 'worker_task_delivery_attestations' THEN row_data->>'ack_state'='acknowledged'
            WHEN 'worker_event_emissions' THEN row_data->>'emission_state'='resolved'
            WHEN 'registered_worker_event_receipts' THEN row_data->>'application_state'='applied' AND row_data->>'ack_state'='acknowledged' AND row_data->>'source_task_ack_state'='acknowledged'
            ELSE row_data->>'ack_state'='acknowledged' END,FALSE)
            OR row_data->>'worker_registration_id' IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM json_array_elements(p_rows->'worker_registrations') r
                JOIN json_array_elements(p_rows->'worker_admission_grants') g ON g->>'id'=r->>'grant_id'
                WHERE r->>'id'=row_data->>'worker_registration_id')
        THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
    END LOOP;
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow OR invalid_text_representation THEN
    RAISE EXCEPTION 'owned_history_retained_terminal_invalid';
END;
$retained$;
"""


def history_bodies():
    before = original()["HISTORY_SQL"].split("AS $history$",1)[1].split("$history$;",1)[0]
    after = once(before,"item json; entry json; hashes jsonb := '[]'; v_fields text[];",
                 "item json; entry json; hashes jsonb := '[]'; v_fields text[]; retained jsonb := '{}'; retained_cert json;")
    anchor = "            candidate := manifest->'legacy_history'->'retired_unassigned_preupload';"
    insertion = """            IF (manifest->'legacy_history'->'version')::text NOT IN ('1','2') OR manifest->'legacy_history'->>'version' IS NULL
                THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
            IF manifest->'legacy_history'->>'version'='2' THEN
                IF (SELECT array_agg(key ORDER BY key) FROM json_each(manifest->'legacy_history')) IS DISTINCT FROM
                    ARRAY['bindings','retained_terminal_uploads','retired_unassigned_preupload','version']
                    OR json_typeof(manifest->'legacy_history'->'retained_terminal_uploads') IS DISTINCT FROM 'array'
                    OR json_array_length(manifest->'legacy_history'->'retained_terminal_uploads') NOT BETWEEN 1 AND 128
                THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
                IF (SELECT jsonb_agg(c->>'operation_id' ORDER BY ordinal) FROM json_array_elements(manifest->'legacy_history'->'retained_terminal_uploads') WITH ORDINALITY a(c,ordinal))
                    IS DISTINCT FROM (SELECT jsonb_agg(DISTINCT c->>'operation_id' ORDER BY c->>'operation_id') FROM json_array_elements(manifest->'legacy_history'->'retained_terminal_uploads') c)
                THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
                FOR retained_cert IN SELECT value FROM json_array_elements(manifest->'legacy_history'->'retained_terminal_uploads') LOOP
                    IF NOT COALESCE((retained_cert->>'observed_at')::timestamptz <= (i->>'approved_at')::timestamptz,FALSE)
                        OR retained ? (retained_cert->>'operation_id') AND retained->(retained_cert->>'operation_id') IS DISTINCT FROM retained_cert::jsonb
                        OR NOT EXISTS (SELECT 1 FROM json_array_elements(manifest->'legacy_history'->'bindings') local_binding
                            WHERE local_binding->>'legacy_account_id'=retained_cert->>'legacy_account_id'
                                AND local_binding->>'legacy_channel_profile_id'=retained_cert->>'legacy_channel_profile_id'
                                AND local_binding::jsonb->'qualified_operation_ids' ? (retained_cert->>'operation_id'))
                    THEN RAISE EXCEPTION 'owned_history_retained_terminal_invalid'; END IF;
                    retained := retained || jsonb_build_object(retained_cert->>'operation_id',retained_cert);
                END LOOP;
            ELSIF manifest::jsonb->'legacy_history' ? 'retained_terminal_uploads' THEN
                RAISE EXCEPTION 'owned_history_retained_terminal_invalid';
            END IF;
"""
    after = once(after,anchor,insertion+anchor)
    anchor = "    IF cert IS NOT NULL THEN\n"
    after = once(after,anchor,"""    FOR retained_cert IN SELECT value::json FROM jsonb_each(retained) LOOP
        PERFORM public.vp_owned_producer_retained_terminal(p_rows,retained_cert,
            (bindings->(retained_cert->>'legacy_account_id'))::json,p_now);
    END LOOP;
"""+anchor)
    anchor = "        PERFORM public.vp_owned_producer_normal(p_rows, task, p_now);"
    after = once(after,anchor,"""        IF EXISTS (SELECT 1 FROM jsonb_each(retained) r WHERE r.value->>'task_id'=task->>'id') THEN CONTINUE; END IF;
"""+anchor)
    return before, after


def replacement(before,after,signature=HISTORY_SIGNATURE):
    return f"""DO $replace_retained$
DECLARE v_definition text; v_source text; v_security boolean; v_config text[];
BEGIN
    SELECT pg_get_functiondef(oid),prosrc,prosecdef,proconfig INTO v_definition,v_source,v_security,v_config
    FROM pg_proc WHERE oid=to_regprocedure('{signature}');
    IF NOT FOUND OR v_source IS DISTINCT FROM $before_retained${before}$before_retained$
        OR v_security IS NOT FALSE OR NOT COALESCE('search_path=pg_catalog'=ANY(v_config),FALSE)
        OR length(v_definition)-length(replace(v_definition,v_source,'')) IS DISTINCT FROM length(v_source)
    THEN RAISE EXCEPTION 'owned_retained_definition_changed'; END IF;
    EXECUTE replace(v_definition,v_source,$after_retained${after}$after_retained$);
END;
$replace_retained$;"""


def seal_sql():
    mapping = "CASE TG_TABLE_NAME " + " ".join(f"WHEN '{table}' THEN '{field}'" for table,field in TABLE_FIELDS.items()) + " END"
    return """
CREATE FUNCTION public.vp_owned_retained_terminal_seal_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $seal$
DECLARE before_row jsonb; after_row jsonb; row_data jsonb; cert jsonb; facts jsonb; selected jsonb;
        field_name text; job_id text; task_id text; publication_id text;
BEGIN
    before_row:=CASE WHEN TG_OP='INSERT' THEN NULL ELSE to_jsonb(OLD) END;
    after_row:=CASE WHEN TG_OP='DELETE' THEN NULL ELSE to_jsonb(NEW) END;
    IF TG_OP='UPDATE' AND before_row IS NOT DISTINCT FROM after_row THEN RETURN NEW; END IF;
    FOR cert IN SELECT c.value FROM public.owned_seed_inventories i
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(i.manifest_json::jsonb->'legacy_history'->'retained_terminal_uploads','[]')) c
        WHERE i.approved_at IS NOT NULL AND i.manifest_json::jsonb->>'version'='2'
          AND i.manifest_json::jsonb->'legacy_history'->>'version'='2' LOOP
        facts:=cert->'retained_facts'; job_id:=facts->'job'->>'id'; task_id:=cert->>'task_id'; publication_id:=cert->>'publication_id';
        IF TG_TABLE_NAME=ANY(ARRAY['worker_task_dispatches','worker_task_delivery_attestations','worker_event_emissions','registered_worker_event_receipts','registered_worker_event_deliveries'])
            AND EXISTS ("""+worker_graph_cte(live=True)+""" SELECT 1 FROM worker_graph g WHERE g.kind=TG_TABLE_NAME AND g.r->>'id' IN (before_row->>'id',after_row->>'id'))
        THEN RAISE EXCEPTION 'owned_history_sealed'; END IF;
        field_name:="""+mapping+""";
        selected:=CASE WHEN field_name IN ('task','job') THEN jsonb_build_array(facts->field_name)
            ELSE COALESCE(facts->field_name,'[]') END;
        FOR row_data IN SELECT value FROM jsonb_array_elements(jsonb_build_array(before_row,after_row)) LOOP
            IF row_data='null'::jsonb THEN CONTINUE; END IF;
            IF EXISTS (SELECT 1 FROM jsonb_array_elements(selected) r WHERE r->>'id'=row_data->>'id')
                OR row_data->>'job_id'=job_id OR row_data->>'production_task_id'=task_id
                OR row_data->>'publication_id'=publication_id
                OR TG_TABLE_NAME='jobs' AND row_data->>'parent_job_id'=job_id
                OR TG_TABLE_NAME='production_tasks' AND (row_data->>'target_account_id'=cert->>'legacy_account_id' OR row_data->>'channel_profile_id'=cert->>'legacy_channel_profile_id')
                OR TG_TABLE_NAME='channel_ops_queue_items' AND (
                    (row_data->>'channel_profile_id'=cert->>'legacy_channel_profile_id' AND TG_OP<>'DELETE'
                        AND after_row->>'status' NOT IN ('succeeded','cancelled'))
                    OR row_data->'payload_json'->>'production_task_id'=task_id OR row_data->'payload_json'->>'job_id'=job_id
                    OR row_data->'payload_json'->>'publication_id'=publication_id
                    OR EXISTS (SELECT 1 FROM jsonb_array_elements(facts->'metrics') m WHERE m->>'id'=row_data->'payload_json'->>'metric_schedule_id')
                    OR EXISTS (SELECT 1 FROM jsonb_array_elements(facts->'queues') q WHERE q->>'id'=row_data->>'parent_queue_item_id' OR q->>'parent_queue_item_id'=row_data->>'id')
                    OR EXISTS (SELECT 1 FROM jsonb_array_elements(facts->'promotion_operations') p WHERE p->>'queue_item_id'=row_data->>'id'))
                OR EXISTS (SELECT 1 FROM jsonb_array_elements(facts->'nodes') n WHERE n->>'id'=row_data->>'node_execution_id')
            THEN RAISE EXCEPTION 'owned_history_sealed'; END IF;
        END LOOP;
    END LOOP;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$seal$;
"""


def upgrade():
    for signature, sql in ((FACT_SIGNATURE,facts_sql()),(CHECK_SIGNATURE,check_sql()),(SEAL_SIGNATURE,seal_sql())):
        op.execute(sql)
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
    op.execute(replacement(*history_bodies()))
    for table in SEAL_TABLES:
        op.execute(f"CREATE TRIGGER owned_retained_seal_{table} BEFORE INSERT OR UPDATE OR DELETE ON public.{table} FOR EACH ROW EXECUTE FUNCTION public.vp_owned_retained_terminal_seal_guard()")


def downgrade():
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM public.owned_seed_inventories WHERE approved_at IS NOT NULL
            AND jsonb_array_length(COALESCE(manifest_json::jsonb->'legacy_history'->'retained_terminal_uploads','[]'))>0)
        THEN RAISE EXCEPTION 'owned_inventory_history_requires_preservation'; END IF;
    END $$;""")
    before,after=history_bodies()
    op.execute(replacement(after,before))
    for table in reversed(SEAL_TABLES):
        op.execute(f"DROP TRIGGER owned_retained_seal_{table} ON public.{table}")
    for signature in (SEAL_SIGNATURE,CHECK_SIGNATURE,FACT_SIGNATURE):
        op.execute(f"DROP FUNCTION {signature}")
