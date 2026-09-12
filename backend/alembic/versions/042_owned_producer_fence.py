"""Retain owned producer authority at the existing registered upload boundary."""
from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa


revision = "042_owned_producer_fence"
down_revision = "041_registered_consumer_terminal"
branch_labels = None
depends_on = None


CANONICAL_SQL = r"""
CREATE FUNCTION public.vp_owned_producer_canonical(p_value json) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path = pg_catalog SET extra_float_digits = 3 AS $canonical$
DECLARE v_kind text := json_typeof(p_value); v_text text; v_float double precision;
        v_exponent integer; v_mantissa text;
BEGIN
    IF v_kind = 'object' THEN
        IF (SELECT count(*) <> count(DISTINCT key) FROM json_each(p_value)) THEN
            RAISE EXCEPTION 'owned_history_invalid_json';
        END IF;
        SELECT '{' || COALESCE(string_agg(public.vp_registered_consumer_ascii_json(key) || ':' ||
            public.vp_owned_producer_canonical(value), ',' ORDER BY key COLLATE "C"), '') || '}'
        INTO v_text FROM json_each(p_value);
    ELSIF v_kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(public.vp_owned_producer_canonical(value), ',' ORDER BY ordinal), '') || ']'
        INTO v_text FROM json_array_elements(p_value) WITH ORDINALITY a(value, ordinal);
    ELSIF v_kind = 'string' THEN
        v_text := public.vp_registered_consumer_ascii_json(p_value #>> '{}');
    ELSIF v_kind = 'number' THEN
        v_text := p_value::text;
        IF v_text ~ '[.eE]' THEN
            v_float := v_text::double precision;
            IF v_float::text IN ('Infinity','-Infinity','NaN') THEN RAISE EXCEPTION 'owned_history_invalid_json'; END IF;
            v_text := v_float::text;
            IF abs(v_float) >= 0.0001 AND abs(v_float) < 1e16 THEN
                v_text := v_text::numeric::text;
                IF strpos(v_text, '.') = 0 THEN v_text := v_text || '.0'; END IF;
            ELSIF v_float = 0 THEN
                v_text := CASE WHEN left(p_value::text, 1) = '-' THEN '-0.0' ELSE '0.0' END;
            ELSE
                -- PostgreSQL and Python use shortest round-trip float8 digits;
                -- Python additionally fixes the exponent threshold and width.
                IF strpos(v_text, 'e') = 0 THEN
                    v_exponent := length(split_part(trim(leading '-' FROM v_text), '.', 1)) - 1;
                    v_mantissa := replace(v_text, '.', '');
                    IF left(v_mantissa, 1) = '-' THEN
                        v_mantissa := '-' || substr(v_mantissa, 2, 1) || '.' || substr(v_mantissa, 3);
                    ELSE v_mantissa := left(v_mantissa, 1) || '.' || substr(v_mantissa, 2); END IF;
                    v_mantissa := rtrim(rtrim(v_mantissa, '0'), '.');
                ELSE
                    v_mantissa := split_part(v_text, 'e', 1);
                    v_exponent := split_part(v_text, 'e', 2)::integer;
                END IF;
                v_text := v_mantissa || 'e' || CASE WHEN v_exponent < 0 THEN '-' ELSE '+' END
                    || lpad(abs(v_exponent)::text, greatest(2, length(abs(v_exponent)::text)), '0');
            END IF;
        ELSE v_text := v_text::numeric::text; END IF;
    ELSIF v_kind IN ('boolean','null') THEN v_text := p_value::text;
    ELSE RAISE EXCEPTION 'owned_history_invalid_json'; END IF;
    IF octet_length(v_text) > 16777216 THEN RAISE EXCEPTION 'owned_history_too_large'; END IF;
    RETURN v_text;
END;
$canonical$;
"""

HASH_SQL = """
CREATE FUNCTION public.vp_owned_producer_hash(p_value json) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $hash$
    SELECT encode(sha256(convert_to(public.vp_owned_producer_canonical(p_value), 'UTF8')), 'hex')
$hash$;
"""

FIELDS_SQL = """
CREATE FUNCTION public.vp_owned_producer_fields(p_value json, p_fields text[]) RETURNS json
LANGUAGE sql IMMUTABLE STRICT SET search_path = pg_catalog AS $fields$
    SELECT COALESCE(json_object_agg(key, value ORDER BY key COLLATE "C"), '{}'::json)
    FROM json_each(p_value) WHERE key = ANY(p_fields)
$fields$;
"""

# The same complete bounded evidence sets as the A1 reader. No secret hashes enter
# the snapshot; native retirement facts, including aliases and orphans, do.
TABLES = (
    "owned_seed_inventories", "owned_seed_inventory_items", "youtube_upload_operations", "production_tasks",
    "publishing_accounts", "channel_profiles", "jobs", "node_executions", "artifacts", "assets", "manual_seeds",
    "publication_records", "publication_metric_schedules", "feedback_snapshots", "channel_ops_queue_items",
    "worker_task_dispatches", "worker_task_delivery_attestations", "worker_event_emissions",
    "registered_worker_event_receipts", "registered_worker_event_deliveries", "worker_registrations",
    "worker_admission_grants", "legacy_worker_event_resolutions", "runtime_schedules",
    "publication_promotion_operations", "worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits",
)


def rows_sql():
    queries = []
    for table in TABLES:
        key = "service_name" if table == "runtime_schedules" else "id"
        value = "row_to_json(r)"
        if table in {"worker_registrations", "worker_admission_grants"}:
            value = "(SELECT json_object_agg(key, value) FROM json_each(row_to_json(r)) WHERE key NOT IN ('lease_secret_sha256','token_sha256'))"
        queries.append(f"'{table}', (SELECT COALESCE(json_agg({value}), '[]'::json) FROM "
                       f"(SELECT * FROM public.{table} ORDER BY {key} LIMIT 4097) r)")
    return """
CREATE FUNCTION public.vp_owned_producer_rows() RETURNS json
LANGUAGE plpgsql SET search_path = pg_catalog SET timezone = 'UTC' AS $rows$
DECLARE v_rows json;
BEGIN
    -- A1 freezes PostgreSQL's JSON directly, without ORM timestamp/float decoding.
    SELECT json_build_object(""" + ",\n".join(queries) + """) INTO v_rows;
    IF EXISTS (SELECT 1 FROM json_each(v_rows) WHERE json_array_length(value) > 4096) THEN
        RAISE EXCEPTION 'owned_history_incomplete';
    END IF;
    PERFORM public.vp_owned_producer_canonical(v_rows);
    RETURN v_rows;
END;
$rows$;
"""


FIND_SQL = """
CREATE FUNCTION public.vp_owned_producer_find(p_rows json, p_table text, p_id text) RETURNS json
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $find$
DECLARE v_row json;
BEGIN
    IF (SELECT count(*) FROM json_array_elements(p_rows->p_table) WHERE value->>'id' = p_id) <> 1 THEN
        RAISE EXCEPTION 'owned_history_orphan';
    END IF;
    SELECT value INTO v_row FROM json_array_elements(p_rows->p_table) WHERE value->>'id' = p_id;
    RETURN v_row;
END;
$find$;
"""

PDS_SQL = """
CREATE FUNCTION public.vp_owned_producer_pds(p_value jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $pds$
    SELECT COALESCE(jsonb_typeof(p_value) = 'object' AND p_value->>'verdict' = 'allow'
        AND jsonb_typeof(p_value->'decision_id') = 'string' AND length(trim(p_value->>'decision_id')) > 0
        AND jsonb_typeof(p_value->'rules_version') = 'string' AND length(trim(p_value->>'rules_version')) > 0
        AND jsonb_typeof(p_value->'evaluated_rules') = 'array'
        AND jsonb_array_length(p_value->'evaluated_rules') > 0
        AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_value->'evaluated_rules') r
                        WHERE jsonb_typeof(r) <> 'string' OR length(trim(r #>> '{}')) = 0)
        AND jsonb_typeof(p_value->'metadata') = 'object'
        AND NOT EXISTS (SELECT 1 FROM jsonb_each(p_value->'metadata') m
            WHERE m.key IN ('disabled','dev','dev_allow_all','noop','fallback','degraded','fail_policy')
              AND m.value NOT IN ('null','false','0','""','[]','{}'))
        AND COALESCE(p_value->'metadata'->>'warning', '') NOT IN
            ('pds_disabled','pds_unavailable','pds_parse_failed','dev_allow_all','noop','degraded','pds_degraded'), FALSE)
$pds$;
"""

_RESERVE = "    INSERT INTO public.youtube_upload_operations ("
_ATTEMPT = "        UPDATE public.youtube_upload_operations\n        SET request_attempted_at = v_now, updated_at = v_now"
_FENCE = "    THEN\n        NULL;\n    ELSIF p_transition = 'submitted'"
_ENTRY = "    IF EXISTS (SELECT 1 FROM public.owned_seed_inventories\n        WHERE approved_at IS NOT NULL AND manifest_json::jsonb->'legacy_history'"
_CHECK_OPERATION = """        PERFORM public.vp_owned_producer_upload(v_operation.job_id, v_operation.node_execution_id,
            v_operation.input_artifact_id, v_operation.content_sha256, v_operation.title, v_operation.privacy);
"""
PATCHES = {
    "vp_reserve_worker_youtube_upload": [(_RESERVE, """    -- owned_producer_reserve_042
    IF NOT EXISTS (SELECT 1 FROM public.youtube_upload_operations WHERE node_execution_id = p_node_execution_id
                   AND (status <> 'reserved' OR request_attempted_at IS NOT NULL)) THEN
        PERFORM public.vp_owned_producer_upload(p_job_id, p_node_execution_id, p_input_artifact_id,
            p_content_sha256, p_title, p_privacy);
    END IF;
""" + _RESERVE)],
    "vp_transition_worker_youtube_upload": [
        (_ATTEMPT, _CHECK_OPERATION + "        v_now := pg_catalog.clock_timestamp();\n" + _ATTEMPT),
        (_FENCE, "    THEN\n" + _CHECK_OPERATION + "    ELSIF p_transition = 'submitted'"),
    ],
    "vp_owned_history_job_entry": [(_ENTRY, """    -- Lock-only entry: settlement does not require a live producer approval.
    PERFORM pg_advisory_xact_lock(('x' || substr(encode(sha256(convert_to('owned-inventory:' || a.platform_account_id,
        'UTF8')), 'hex'), 1, 16))::bit(64)::bigint)
    FROM public.production_tasks t JOIN public.publishing_accounts a ON a.id = t.target_account_id
    WHERE t.job_id = p_job_id AND a.platform_account_id ~ '^UC[A-Za-z0-9_-]{22}$'
    ORDER BY a.platform_account_id;
    PERFORM 1 FROM public.owned_seed_inventories i WHERE i.platform_channel_id IN (
        SELECT a.platform_account_id FROM public.production_tasks t
        JOIN public.publishing_accounts a ON a.id = t.target_account_id WHERE t.job_id = p_job_id)
    ORDER BY i.id FOR UPDATE;
""" + _ENTRY)],
}


def patch_sql(*, remove=False):
    patches = {name: [(after, before) if remove else (before, after) for before, after in changes]
               for name, changes in PATCHES.items()}
    mapping = str(sa.literal(json.dumps(patches)).compile(compile_kwargs={"literal_binds": True}))
    return """DO $patch$
DECLARE v_name text; v_changes jsonb; v_change jsonb; v_before text; v_after text;
        v_source text; v_definition text; v_changed text; v_count integer;
BEGIN
    FOR v_name, v_changes IN SELECT key, value FROM jsonb_each($MAPPING$::jsonb) LOOP
        SELECT count(*) INTO v_count FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = v_name AND p.prosecdef;
        IF v_count <> 1 THEN RAISE EXCEPTION 'owned_producer_definition_changed'; END IF;
        SELECT p.prosrc, pg_catalog.pg_get_functiondef(p.oid) INTO v_source, v_definition
        FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = v_name AND p.proconfig = ARRAY['search_path=pg_catalog']::text[];
        IF v_source IS NULL OR length(v_definition) - length(replace(v_definition, v_source, '')) <> length(v_source)
        THEN RAISE EXCEPTION 'owned_producer_definition_changed'; END IF;
        v_changed := v_source;
        FOR v_change IN SELECT value FROM jsonb_array_elements(v_changes) LOOP
            v_before := v_change->>0; v_after := v_change->>1;
            IF length(v_changed) - length(replace(v_changed, v_before, '')) <> length(v_before)
            THEN RAISE EXCEPTION 'owned_producer_definition_changed'; END IF;
            v_changed := replace(v_changed, v_before, v_after);
        END LOOP;
        EXECUTE replace(v_definition, v_source, v_changed);
    END LOOP;
END $patch$;""".replace("$MAPPING$", mapping)


RETIRED_SQL = """
CREATE FUNCTION public.vp_owned_producer_retired(p_rows json, p_cert json, p_now timestamptz) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $retired$
DECLARE v_key text; v_table text; v_before json; v_current json; v_fields text[]; v_source json;
BEGIN
    IF p_cert IS NULL OR p_cert::jsonb = 'null'::jsonb THEN RETURN; END IF;
    IF p_cert->>'classification' IS DISTINCT FROM 'retired_unassigned_preupload'
        OR (p_cert->>'observed_at')::timestamptz > p_now
        OR NOT public.vp_registered_consumer_terminal_upload((p_cert->>'operation_id')::uuid, p_now)
    THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
    -- This is a recheck of an existing immutable approved certificate, not a
    -- metadata-terminal proof promoted into a new retirement authority.
    FOR v_key, v_table IN SELECT * FROM (VALUES ('operation','youtube_upload_operations'),
        ('task','production_tasks'), ('job','jobs'), ('upload_node','node_executions'),
        ('account','publishing_accounts'), ('channel','channel_profiles'), ('manual_seed','manual_seeds')) m LOOP
        v_before := p_cert->'retained_facts'->v_key;
        v_current := public.vp_owned_producer_find(p_rows, v_table, v_before->>'id');
        IF public.vp_owned_producer_hash(v_current) IS DISTINCT FROM public.vp_owned_producer_hash(v_before)
        THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
    END LOOP;
    IF p_cert->'retained_facts'->'account'->>'platform_account_id' IS DISTINCT FROM ''
        OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'production_tasks') t
            WHERE t->>'id' <> p_cert->>'task_id' AND (t->>'target_account_id' = p_cert->>'legacy_account_id'
                OR t->>'channel_profile_id' = p_cert->>'legacy_channel_profile_id' OR t->>'job_id' = p_cert->>'job_id'))
        OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'runtime_schedules') s WHERE s->>'guarded_job_id' = p_cert->>'job_id')
    THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
    FOR v_table, v_before IN SELECT key, value FROM json_each(p_cert->'terminal_graph') LOOP
        FOR v_source IN SELECT value FROM json_array_elements(v_before) LOOP
            v_current := public.vp_owned_producer_find(p_rows, v_table, v_source->>'id');
            SELECT array_agg(key ORDER BY key) INTO v_fields FROM json_each(v_source)
            WHERE NOT (v_table = 'worker_registrations' AND key IN
                ('heartbeat_at','lease_expires_at','status','revoked_at','revoke_reason','superseded_by'))
              AND NOT (v_table = 'worker_admission_grants' AND key IN ('state','revoked_at','revoke_reason','updated_at'));
            IF public.vp_owned_producer_hash(public.vp_owned_producer_fields(v_current, v_fields)) IS DISTINCT FROM
               public.vp_owned_producer_hash(public.vp_owned_producer_fields(v_source, v_fields))
            THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
        END LOOP;
    END LOOP;
    FOR v_source IN SELECT value FROM json_array_elements(p_cert->'retained_facts'->'source_assets') LOOP
        v_current := public.vp_owned_producer_find(p_rows, 'assets', v_source->'asset'->>'id');
        IF public.vp_owned_producer_hash(v_current) IS DISTINCT FROM public.vp_owned_producer_hash(v_source->'asset')
        THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
    END LOOP;
END;
$retired$;
"""

QUEUE_SQL = """
CREATE FUNCTION public.vp_owned_producer_queue(p_q jsonb, p_states text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $queue$
    SELECT COALESCE(p_q->>'status' = ANY(p_states) AND p_q->>'last_error' IS NULL
        AND p_q->>'dead_letter_at' IS NULL AND p_q->>'attempt_count' IN ('0','1')
        AND CASE p_q->>'status'
            WHEN 'queued' THEN p_q->>'attempt_count' = '0' AND p_q->>'locked_at' IS NULL AND p_q->>'locked_by' IS NULL
            WHEN 'running' THEN p_q->>'attempt_count' = '1' AND p_q->>'locked_at' IS NOT NULL AND length(p_q->>'locked_by') > 0
            WHEN 'succeeded' THEN p_q->>'attempt_count' = '1' AND p_q->>'locked_at' IS NULL AND p_q->>'locked_by' IS NULL
            ELSE FALSE END, FALSE)
$queue$;
"""

NORMAL_SQL = """
CREATE FUNCTION public.vp_owned_producer_normal(p_rows json, p_task json, p_now timestamptz) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $normal$
DECLARE o jsonb; j jsonb; n jsonb; a jsonb; pub jsonb; q jsonb; parent jsonb; replacement jsonb;
        automatic jsonb; m jsonb; f jsonb; chain jsonb; previous jsonb; v_queues jsonb;
        v_start timestamptz; v_due timestamptz; v_grace timestamptz; v_stage text; v_hours integer;
        v_grace_hours integer; v_index integer; v_last integer; v_done boolean; v_count integer; v_item boolean;
BEGIN
    IF NOT COALESCE(p_task->>'retry_count' = '0' AND p_task->>'failure_reason' IS NULL
        AND p_task->>'blocked_by_guard' IS NULL AND p_task->>'state' IN
        ('selected','planning','producing','scheduled','uploaded_private','measured'), FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_task_failed'; END IF;
    IF (SELECT count(*) FROM json_array_elements(p_rows->'youtube_upload_operations') op_row
        WHERE op_row->>'production_task_id' = p_task->>'id') <> 1 THEN RAISE EXCEPTION 'owned_inventory_outstanding'; END IF;
    SELECT value::jsonb INTO o FROM json_array_elements(p_rows->'youtube_upload_operations')
    WHERE value->>'production_task_id' = p_task->>'id';
    IF NOT COALESCE(o->>'job_id' = p_task->>'job_id' AND o->>'status' = 'succeeded'
        AND o->>'error_message' IS NULL AND o->>'content_sha256' ~ '^[0-9a-f]{64}$'
        AND o->>'manager_task_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND o->>'platform_video_id' ~ '^[A-Za-z0-9_-]{11}$'
        AND (o->>'request_attempted_at')::timestamptz <= (o->>'completed_at')::timestamptz
        AND (o->>'completed_at')::timestamptz <= p_now
        AND o->>'privacy' IN ('private','unlisted') AND o->'receipt_json'->>'privacy' = o->>'privacy'
        AND o->'receipt_json'->>'video_id' = o->>'platform_video_id' AND o->'receipt_json'->>'title' = o->>'title'
        AND o->'receipt_json'->>'url' = 'https://www.youtube.com/watch?v=' || (o->>'platform_video_id')
        AND (SELECT array_agg(key ORDER BY key) FROM jsonb_each(o->'receipt_json')) =
            ARRAY['privacy','quota_estimate','tags','title','url','video_id']::text[], FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_operation_unresolved'; END IF;
    j := public.vp_owned_producer_find(p_rows, 'jobs', o->>'job_id')::jsonb;
    IF NOT COALESCE(j->>'status' = 'SUCCEEDED' AND j->>'error_message' IS NULL
        AND (o->>'completed_at')::timestamptz <= (j->>'completed_at')::timestamptz
        AND (j->>'completed_at')::timestamptz <= p_now, FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_job_receipt'; END IF;
    v_count := 0;
    FOR n IN SELECT value::jsonb FROM json_array_elements(p_rows->'node_executions') WHERE value->>'job_id' = j->>'id' LOOP
        IF n->>'status' IS DISTINCT FROM 'SUCCEEDED' OR n->>'error_message' IS NOT NULL
        THEN RAISE EXCEPTION 'owned_inventory_node_outcome'; END IF;
        IF n->>'node_type' = 'youtube_upload' THEN
            v_count := v_count + 1;
            a := public.vp_owned_producer_find(p_rows, 'artifacts', n->>'output_artifact_id')::jsonb;
            IF NOT COALESCE(n->>'id' = o->>'node_execution_id'
                AND n->'input_artifact_ids' = jsonb_build_array(o->>'input_artifact_id')
                AND (o->>'completed_at')::timestamptz <= (n->>'completed_at')::timestamptz
                AND (n->>'completed_at')::timestamptz <= (j->>'completed_at')::timestamptz
                AND a->>'job_id' = j->>'id' AND a->>'node_execution_id' = n->>'id'
                AND a->'media_info'->'youtube' = o->'receipt_json', FALSE)
            THEN RAISE EXCEPTION 'owned_inventory_node_receipt'; END IF;
        END IF;
    END LOOP;
    IF v_count <> 1 THEN RAISE EXCEPTION 'owned_inventory_upload_node_count'; END IF;
    IF (SELECT count(*) FROM json_array_elements(p_rows->'publication_records') p WHERE p->>'production_task_id' = p_task->>'id') <> 1
    THEN RAISE EXCEPTION 'owned_inventory_publication_count'; END IF;
    SELECT value::jsonb INTO pub FROM json_array_elements(p_rows->'publication_records') WHERE value->>'production_task_id' = p_task->>'id';
    v_start := (pub->>'scheduled_publish_at')::timestamptz;
    IF NOT COALESCE(pub->>'account_id' = p_task->>'target_account_id' AND pub->>'platform' = 'youtube'
        AND pub->>'platform_content_id' = o->>'platform_video_id' AND pub->>'desired_privacy' = 'unlisted'
        AND pub->>'current_privacy' = 'unlisted' AND pub->>'public_at' IS NULL AND pub->>'publish_status' IN ('uploaded','scheduled')
        AND (o->>'completed_at')::timestamptz <= v_start AND v_start <= p_now, FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_publication_identity'; END IF;
    WITH RECURSIVE queues AS (
        SELECT value::jsonb row_data FROM json_array_elements(p_rows->'channel_ops_queue_items')
    ), related(id) AS (
        SELECT row_data->>'id' FROM queues WHERE row_data->'payload_json'->>'production_task_id' = p_task->>'id'
            OR row_data->'payload_json'->>'publication_id' = pub->>'id' OR row_data->'payload_json'->>'metric_schedule_id' IN (
                SELECT value->>'id' FROM json_array_elements(p_rows->'publication_metric_schedules') WHERE value->>'publication_id' = pub->>'id')
        UNION
        SELECT child.row_data->>'id' FROM queues child JOIN queues ancestor ON child.row_data->>'parent_queue_item_id' = ancestor.row_data->>'id'
            OR child.row_data->>'id' = ancestor.row_data->>'parent_queue_item_id' JOIN related related_row ON ancestor.row_data->>'id' = related_row.id
    ) SELECT COALESCE(jsonb_agg(row_data ORDER BY row_data->>'id'), '[]') INTO v_queues FROM queues WHERE row_data->>'id' IN (SELECT id FROM related);
    IF (SELECT count(*) FROM jsonb_array_elements(v_queues) queue_row WHERE queue_row->>'kind' = 'reconcile_publication') <> 1
    THEN RAISE EXCEPTION 'owned_inventory_reconciliation'; END IF;
    SELECT value INTO q FROM jsonb_array_elements(v_queues) WHERE value->>'kind' = 'reconcile_publication';
    parent := public.vp_owned_producer_find(p_rows, 'channel_ops_queue_items', q->>'parent_queue_item_id')::jsonb;
    IF NOT COALESCE(public.vp_owned_producer_queue(q, ARRAY['succeeded'])
        AND (q->>'run_after')::timestamptz = v_start + INTERVAL '30 minutes'
        AND q->>'idempotency_key' = 'reconcile_publication:' || (pub->>'id') || ':' || to_char(v_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        AND q->'payload_json'->>'publication_id' = pub->>'id' AND q->>'channel_profile_id' = p_task->>'channel_profile_id'
        AND public.vp_owned_producer_queue(parent, ARRAY['succeeded']) AND parent->>'kind' = 'promote_publication'
        AND parent->>'channel_profile_id' = q->>'channel_profile_id' AND parent->'payload_json'->>'publication_id' = pub->>'id'
        AND parent->'payload_json'->>'target_visibility' = 'unlisted', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_reconciliation'; END IF;
    replacement := parent;
    SELECT EXISTS (SELECT 1 FROM json_array_elements(p_rows->'owned_seed_inventory_items') i
                   WHERE i->>'production_task_id' = p_task->>'id') INTO v_item;
    -- Only A1's exact historical settled automatic-to-manual replacement is clean.
    FOR automatic IN SELECT value FROM jsonb_array_elements(v_queues) WHERE value->>'status' = 'cancelled' LOOP
        v_due := (pub->>'uploaded_at')::timestamptz + INTERVAL '1 hour';
        parent := public.vp_owned_producer_find(p_rows, 'channel_ops_queue_items', automatic->>'parent_queue_item_id')::jsonb;
        IF v_item OR NOT COALESCE(automatic->>'kind' = 'promote_publication'
            AND automatic->>'last_error' = 'replaced_by_immediate_unlisted_canary_promotion'
            AND automatic->>'attempt_count' = '0' AND automatic->>'locked_at' IS NULL AND automatic->>'locked_by' IS NULL
            AND (automatic->>'run_after')::timestamptz = v_due
            AND (pub->>'uploaded_at')::timestamptz <= (automatic->>'dead_letter_at')::timestamptz
            AND (automatic->>'dead_letter_at')::timestamptz <= (replacement->>'run_after')::timestamptz
            AND (replacement->>'run_after')::timestamptz <= v_start
            AND automatic->>'channel_profile_id' = p_task->>'channel_profile_id'
            AND automatic->'payload_json'->>'publication_id' = pub->>'id'
            AND automatic->'payload_json'->>'target_visibility' = 'unlisted'
            AND automatic->'payload_json'->>'scheduled_at' = to_char(v_due AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            AND automatic->>'idempotency_key' = 'promote_publication:' || (pub->>'id') || ':unlisted:' || to_char(v_due AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            AND replacement->>'parent_queue_item_id' IS NULL
            AND replacement->>'idempotency_key' = 'promote_publication:' || (pub->>'id') || ':unlisted:manual'
            AND replacement->'payload_json'->>'channel_profile_id' = p_task->>'channel_profile_id'
            AND replacement->'payload_json'->>'scheduled_at' IS NULL
            AND public.vp_owned_producer_queue(parent, ARRAY['succeeded']) AND parent->>'kind' = 'publish_task'
            AND parent->>'channel_profile_id' = p_task->>'channel_profile_id'
            AND parent->'payload_json'->>'production_task_id' = p_task->>'id'
            AND (SELECT count(*) FROM jsonb_array_elements(v_queues) queue_row WHERE queue_row->>'kind' = 'promote_publication') = 2, FALSE)
        THEN RAISE EXCEPTION 'owned_inventory_queue_failed'; END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_queues) queue_row WHERE queue_row->>'status' <> 'cancelled' AND
        (NOT public.vp_owned_producer_queue(queue_row, ARRAY['queued','running','succeeded'])
         OR queue_row->>'channel_profile_id' IS DISTINCT FROM p_task->>'channel_profile_id'))
    THEN RAISE EXCEPTION 'owned_inventory_queue_failed'; END IF;
    IF (SELECT count(*) FROM json_array_elements(p_rows->'publication_metric_schedules') metric_row WHERE metric_row->>'publication_id' = pub->>'id') <> 5
    THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
    FOR v_stage, v_hours, v_grace_hours IN VALUES ('1h',1,3),('6h',6,12),('24h',24,30),('72h',72,84),('7d',168,192) LOOP
        IF (SELECT count(*) FROM json_array_elements(p_rows->'publication_metric_schedules') metric_row
            WHERE metric_row->>'publication_id' = pub->>'id' AND metric_row->>'snapshot_stage' = v_stage) <> 1
        THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
        SELECT value::jsonb INTO m FROM json_array_elements(p_rows->'publication_metric_schedules')
        WHERE value->>'publication_id' = pub->>'id' AND value->>'snapshot_stage' = v_stage;
        v_due := v_start + make_interval(hours => v_hours); v_grace := v_start + make_interval(hours => v_grace_hours);
        v_done := m->>'status' = 'succeeded'; v_last := (m->>'attempt_count')::integer - v_done::integer;
        IF NOT COALESCE(m->>'status' IN ('pending','succeeded') AND v_last BETWEEN 0 AND 1024
            AND (m->>'effective_start_at')::timestamptz = v_start AND (m->>'due_at')::timestamptz = v_due
            AND (m->>'grace_until')::timestamptz = v_grace
            AND (m->>'last_error_code') IS NOT DISTINCT FROM CASE WHEN v_done OR m->>'attempt_count' = '0' THEN NULL ELSE 'metrics_unavailable' END
            AND CASE WHEN m->>'attempt_count' = '0' THEN m->>'last_attempt_at' IS NULL ELSE
                (m->>'last_attempt_at')::timestamptz BETWEEN v_due AND least(p_now, v_grace) END, FALSE)
        THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
        SELECT COALESCE(jsonb_agg(queue_row ORDER BY (queue_row->'payload_json'->>'metrics_poll_count')::integer), '[]') INTO chain
        FROM jsonb_array_elements(v_queues) queue_row WHERE queue_row->>'kind' = 'collect_metrics' AND queue_row->'payload_json'->>'metric_schedule_id' = m->>'id';
        IF jsonb_array_length(chain) <> v_last + 1 THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
        FOR v_index IN 0..v_last LOOP
            q := chain->v_index;
            parent := public.vp_owned_producer_find(p_rows, 'channel_ops_queue_items', q->>'parent_queue_item_id')::jsonb;
            IF NOT COALESCE(q->'payload_json'->>'metrics_poll_count' = v_index::text
                AND q->'payload_json'->>'publication_id' = pub->>'id' AND q->'payload_json'->>'snapshot_stage' = v_stage
                AND q->>'channel_profile_id' = p_task->>'channel_profile_id'
                AND q->>'idempotency_key' = 'collect_metrics:' || (pub->>'id') || ':stage:' || v_stage || ':attempt:' || v_index::text
                AND (q->>'run_after')::timestamptz <= v_grace
                AND public.vp_owned_producer_queue(q, ARRAY['queued','running','succeeded'])
                AND CASE WHEN v_index = 0 THEN (q->>'run_after')::timestamptz = v_due
                    AND parent->>'kind' = 'promote_publication' AND public.vp_owned_producer_queue(parent, ARRAY['succeeded'])
                    AND parent->>'channel_profile_id' = p_task->>'channel_profile_id'
                    AND parent->'payload_json'->>'publication_id' = pub->>'id' AND parent->'payload_json'->>'target_visibility' = 'unlisted'
                ELSE parent = previous AND (q->>'run_after')::timestamptz > (previous->>'run_after')::timestamptz END
                AND CASE WHEN q->>'status' = 'succeeded' THEN v_index < v_last OR v_done
                    ELSE v_index = v_last AND NOT v_done END, FALSE)
            THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
            previous := q;
        END LOOP;
        SELECT count(*) INTO v_count FROM json_array_elements(p_rows->'feedback_snapshots') feedback_row
        WHERE feedback_row->>'publication_id' = pub->>'id' AND feedback_row->>'snapshot_stage' = v_stage;
        IF v_done THEN
            IF NOT COALESCE(v_count = 1 AND (m->>'completed_at')::timestamptz = (m->>'last_attempt_at')::timestamptz
                AND (m->>'completed_at')::timestamptz BETWEEN v_due AND least(p_now, v_grace)
                AND (m->>'completed_at')::timestamptz >= (q->>'run_after')::timestamptz, FALSE)
            THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
        ELSE
            IF v_count <> 0 OR m->>'completed_at' IS NOT NULL OR p_now >= v_due
                OR (m->>'attempt_count')::integer > 0 AND (q->>'run_after')::timestamptz <= (m->>'last_attempt_at')::timestamptz
            THEN RAISE EXCEPTION 'owned_inventory_metrics_pending'; END IF;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM json_array_elements(p_rows->'feedback_snapshots') feedback_row WHERE feedback_row->>'publication_id' = pub->>'id'
               AND feedback_row->>'snapshot_stage' NOT IN ('1h','6h','24h','72h','7d'))
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(v_queues) queue_row WHERE queue_row->>'kind' = 'collect_metrics'
            AND NOT EXISTS (SELECT 1 FROM json_array_elements(p_rows->'publication_metric_schedules') metric_row
                WHERE metric_row->>'publication_id' = pub->>'id' AND metric_row->>'id' = queue_row->'payload_json'->>'metric_schedule_id'))
    THEN RAISE EXCEPTION 'owned_inventory_metrics'; END IF;
    IF p_now - (o->>'request_attempted_at')::timestamptz < INTERVAL '24 hours'
        OR p_now - (o->>'completed_at')::timestamptz < INTERVAL '24 hours'
    THEN RAISE EXCEPTION 'owned_inventory_cooldown'; END IF;
END;
$normal$;
"""

HISTORY_SQL = """
CREATE FUNCTION public.vp_owned_producer_history(p_rows json, p_task_id uuid, p_uc text, p_now timestamptz) RETURNS jsonb
LANGUAGE plpgsql SET search_path = pg_catalog AS $history$
DECLARE i json; manifest json; b json; fact json; cert json; candidate json; account json; task json; channel json;
        operation json; bindings jsonb := '{}'; members text[] := '{}'; v_members text[]; v_ids jsonb;
        item json; entry json; hashes jsonb := '[]'; v_fields text[];
BEGIN
    FOR i IN SELECT value FROM json_array_elements(p_rows->'owned_seed_inventories') WHERE value->>'approved_at' IS NOT NULL LOOP
        manifest := i->'manifest_json';
        IF NOT COALESCE(i->>'state' IN ('approved','held','exhausted','expired','revoked')
            AND (i->>'approved_at')::timestamptz <= p_now AND length(i->>'approved_by') > 0 AND length(i->>'approval_reference') > 0
            AND manifest->>'version' IN ('1','2') AND manifest->>'inventory_id' = i->>'id'
            AND manifest->>'platform_channel_id' = i->>'platform_channel_id'
            AND manifest->>'target_account_id' = i->>'target_account_id' AND manifest->>'channel_profile_id' = i->>'channel_profile_id'
            AND public.vp_owned_producer_hash(manifest) = i->>'manifest_sha256', FALSE)
        THEN RAISE EXCEPTION 'owned_history_authority_invalid'; END IF;
        IF manifest->>'version' = '2' THEN
            candidate := manifest->'legacy_history'->'retired_unassigned_preupload';
            IF candidate IS NOT NULL AND candidate::jsonb <> 'null'::jsonb THEN
                IF (candidate->>'observed_at')::timestamptz > (i->>'approved_at')::timestamptz
                    OR cert IS NOT NULL AND public.vp_owned_producer_hash(cert) IS DISTINCT FROM public.vp_owned_producer_hash(candidate)
                THEN RAISE EXCEPTION 'owned_history_authority_conflict'; END IF;
                cert := candidate;
            END IF;
            FOR b IN SELECT value FROM json_array_elements(manifest->'legacy_history'->'bindings') LOOP
                IF NOT COALESCE(b->>'use' = 'history_only' AND b->>'platform' = 'youtube'
                    AND b->>'canonical_platform_channel_id' ~ '^UC[A-Za-z0-9_-]{22}$'
                    AND (b->'qualification'->>'observed_at')::timestamptz <= (i->>'approved_at')::timestamptz
                    AND public.vp_owned_producer_hash(b->'qualification'->'sanitized_facts') = b->'qualification'->>'facts_sha256', FALSE)
                    OR bindings ? (b->>'legacy_account_id') AND bindings->(b->>'legacy_account_id') IS DISTINCT FROM b::jsonb
                THEN RAISE EXCEPTION 'owned_history_authority_conflict'; END IF;
                bindings := bindings || jsonb_build_object(b->>'legacy_account_id', b::jsonb);
            END LOOP;
        END IF;
    END LOOP;
    FOR b IN SELECT value::json FROM jsonb_each(bindings) LOOP
        account := public.vp_owned_producer_find(p_rows, 'publishing_accounts', b->>'legacy_account_id');
        IF NOT COALESCE(account->>'channel_profile_id' = b->>'legacy_channel_profile_id'
            AND COALESCE(NULLIF(account->>'platform',''), 'youtube') = 'youtube'
            AND account->>'platform_account_id' IN ('', b->>'canonical_platform_channel_id')
            AND public.vp_owned_producer_hash(public.vp_owned_producer_fields(account,
                ARRAY['id','channel_profile_id','platform','platform_account_id','credential_ref','platform_specific_config_json']))
                = b->>'account_descriptor_sha256', FALSE)
        THEN RAISE EXCEPTION 'owned_history_binding_changed'; END IF;
        SELECT COALESCE(jsonb_agg(o->>'id' ORDER BY o->>'id'), '[]') INTO v_ids
        FROM json_array_elements(p_rows->'youtube_upload_operations') o WHERE o->>'production_task_id' IN (
            SELECT t->>'id' FROM json_array_elements(p_rows->'production_tasks') t WHERE t->>'target_account_id' = b->>'legacy_account_id');
        IF v_ids IS DISTINCT FROM b::jsonb->'qualified_operation_ids'
            OR EXISTS (SELECT 1 FROM json_array_elements(p_rows->'production_tasks') t
                WHERE t->>'target_account_id' = b->>'legacy_account_id' AND NOT EXISTS (
                    SELECT 1 FROM json_array_elements(p_rows->'youtube_upload_operations') o WHERE o->>'production_task_id' = t->>'id'))
        THEN RAISE EXCEPTION 'owned_history_membership_changed'; END IF;
        SELECT COALESCE(jsonb_agg(f->>'operation_id' ORDER BY f->>'operation_id'), '[]') INTO v_ids
        FROM json_array_elements(b->'qualification'->'sanitized_facts') f;
        IF v_ids IS DISTINCT FROM b::jsonb->'qualified_operation_ids' THEN RAISE EXCEPTION 'owned_history_qualification_changed'; END IF;
        FOR fact IN SELECT value FROM json_array_elements(b->'qualification'->'sanitized_facts') LOOP
            operation := public.vp_owned_producer_find(p_rows, 'youtube_upload_operations', fact->>'operation_id');
            IF NOT COALESCE(operation->>'status' = 'succeeded' AND operation->>'manager_task_id' = fact->>'manager_task_id'
                AND operation->>'platform_video_id' = fact->>'platform_video_id'
                AND fact->>'actual_platform_channel_id' = b->>'canonical_platform_channel_id'
                AND (operation->>'completed_at')::timestamptz <= (fact->>'observed_at')::timestamptz
                AND (fact->>'observed_at')::timestamptz <= p_now
                AND public.vp_owned_producer_hash(operation) = fact->>'operation_sha256'
                AND public.vp_owned_producer_hash(operation->'receipt_json') = fact->>'receipt_sha256', FALSE)
            THEN RAISE EXCEPTION 'owned_history_qualification_changed'; END IF;
        END LOOP;
    END LOOP;
    IF cert IS NOT NULL THEN
        IF bindings ? (cert->>'legacy_account_id') OR NOT COALESCE(
            public.vp_owned_producer_hash(cert->'terminal_graph') = cert->>'terminal_graph_sha256'
            AND public.vp_owned_producer_hash(cert->'retained_facts'->'task'->'transition_history_json') = cert->>'transition_sha256', FALSE)
        THEN RAISE EXCEPTION 'owned_history_retired_changed'; END IF;
        PERFORM public.vp_owned_producer_retired(p_rows, cert, p_now);
        SELECT COALESCE(jsonb_agg(s->>'content_sha256'), '[]') INTO hashes
        FROM json_array_elements(cert->'retained_facts'->'source_assets') s;
        hashes := hashes || jsonb_build_array(cert->'retained_facts'->'operation'->>'content_sha256');
    END IF;
    -- Complete global classification precedes the caller's current-task exclusion.
    FOR operation IN SELECT value FROM json_array_elements(p_rows->'youtube_upload_operations') LOOP
        task := public.vp_owned_producer_find(p_rows, 'production_tasks', operation->>'production_task_id');
        account := public.vp_owned_producer_find(p_rows, 'publishing_accounts', task->>'target_account_id');
        channel := public.vp_owned_producer_find(p_rows, 'channel_profiles', task->>'channel_profile_id');
        IF account->>'channel_profile_id' IS DISTINCT FROM channel->>'id' THEN RAISE EXCEPTION 'owned_history_orphan'; END IF;
        IF cert IS NOT NULL AND operation->>'id' = cert->>'operation_id' THEN CONTINUE; END IF;
        IF bindings ? (account->>'id') THEN CONTINUE; END IF;
        IF NOT COALESCE(COALESCE(NULLIF(account->>'platform',''), 'youtube') = 'youtube'
            AND account->>'platform_account_id' ~ '^UC[A-Za-z0-9_-]{22}$', FALSE)
        THEN RAISE EXCEPTION 'owned_history_unclassified'; END IF;
    END LOOP;
    SELECT COALESCE(array_agg(a->>'id' ORDER BY a->>'id'), '{}') INTO members FROM json_array_elements(p_rows->'publishing_accounts') a
    WHERE COALESCE(NULLIF(a->>'platform',''), 'youtube') = 'youtube' AND a->>'platform_account_id' = p_uc;
    SELECT COALESCE(array_agg(key ORDER BY key), '{}') INTO v_members FROM jsonb_each(bindings)
    WHERE value->>'canonical_platform_channel_id' = p_uc;
    members := members || v_members;
    FOR item IN SELECT value FROM json_array_elements(p_rows->'owned_seed_inventory_items')
                WHERE value->>'platform_channel_id' = p_uc AND value->>'production_task_id' IS NOT NULL LOOP
        task := public.vp_owned_producer_find(p_rows, 'production_tasks', item->>'production_task_id');
        i := public.vp_owned_producer_find(p_rows, 'owned_seed_inventories', item->>'inventory_id');
        SELECT value INTO entry FROM json_array_elements(i->'manifest_json'->'entries') WHERE value->>'id' = item->>'id';
        IF NOT COALESCE(i->>'approved_at' IS NOT NULL AND i->>'platform_channel_id' = p_uc
            AND i->>'channel_profile_id' = task->>'channel_profile_id' AND i->>'target_account_id' = task->>'target_account_id'
            AND entry->>'manual_seed_id' = item->>'manual_seed_id' AND entry->>'asset_id' = item->>'asset_id'
            AND entry->>'content_sha256' = item->>'content_sha256' AND task->>'manual_seed_id' = item->>'manual_seed_id', FALSE)
        THEN RAISE EXCEPTION 'owned_history_item_authority'; END IF;
    END LOOP;
    FOR task IN SELECT value FROM json_array_elements(p_rows->'production_tasks') LOOP
        -- Equivalent to A1: t.id = p_task_id skips prior effects, never classification.
        IF task->>'id' = p_task_id::text THEN CONTINUE; END IF;
        IF NOT task->>'target_account_id' = ANY(members) AND NOT EXISTS (
            SELECT 1 FROM json_array_elements(p_rows->'owned_seed_inventory_items') item_row
            WHERE item_row->>'production_task_id' = task->>'id' AND item_row->>'platform_channel_id' = p_uc) THEN CONTINUE; END IF;
        IF task->>'state' IN ('held','failed','rejected') AND NOT EXISTS (
            SELECT 1 FROM json_array_elements(p_rows->'youtube_upload_operations') o WHERE o->>'production_task_id' = task->>'id')
            AND NOT EXISTS (SELECT 1 FROM json_array_elements(p_rows->'owned_seed_inventory_items') item_row WHERE item_row->>'production_task_id' = task->>'id')
        THEN CONTINUE; END IF;
        PERFORM public.vp_owned_producer_normal(p_rows, task, p_now);
    END LOOP;
    RETURN jsonb_build_object('bindings', bindings, 'retired', cert, 'retired_hashes', hashes, 'members', members);
    -- Both request_attempted_at and completed_at floors live in the normal-history
    -- helper and are INTERVAL '24 hours'; neither is reset by succession.
END;
$history$;
"""

CONFIG_FIELDS = {
    "channel": ("channel_profiles", "id config_version name positioning language default_aspect_ratio risk_policy_json content_mix_policy_json cadence_policy_json alert_policy_json enabled dry_run"),
    "account": ("publishing_accounts", "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json default_privacy external_asset_auto_publish enabled paused_until"),
    "lane": ("topic_lanes", "id channel_profile_id name description weight keywords_json negative_keywords_json min_posts_per_week max_posts_per_day max_consecutive_streak cooldown_after_post_minutes enabled paused_until"),
    "format": ("lane_format_matrix", "id topic_lane_id format_key enabled weight target_duration_sec template_pool_json source_platforms_json default_publish_visibility"),
}


def config_sql():
    fields = []
    for name, (table, names) in CONFIG_FIELDS.items():
        columns = [column if column != "weight" else
                   "(weight::text || CASE WHEN weight::text !~ '[.eE]' THEN '.0' ELSE '' END)::json AS weight"
                   for column in names.split()]
        fields.append(f"'{name}', (SELECT row_to_json(r) FROM (SELECT {','.join(columns)} FROM public.{table} WHERE id = p_{name}) r)")
    return """
CREATE FUNCTION public.vp_owned_producer_config(p_channel uuid, p_account uuid, p_lane uuid, p_format uuid) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog AS $config$
    SELECT public.vp_owned_producer_hash(json_build_object(""" + ",\n".join(fields) + """))
$config$;
"""


GRAPH_SQL = """
CREATE FUNCTION public.vp_owned_producer_graph(p_graph jsonb, p_asset text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $graph$
DECLARE source jsonb; upload jsonb; export jsonb; parent text; n jsonb; v_reachable integer;
BEGIN
    IF jsonb_typeof(p_graph->'nodes') IS DISTINCT FROM 'array' OR jsonb_typeof(p_graph->'edges') IS DISTINCT FROM 'array'
        OR jsonb_array_length(p_graph->'nodes') > 4096 OR jsonb_array_length(p_graph->'edges') > 4096 THEN RETURN FALSE; END IF;
    IF (SELECT count(*) FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'type' = 'source') <> 1
        OR (SELECT count(*) FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'type' = 'youtube_upload') <> 1
        OR (SELECT count(*) FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'type' = 'export') <> 1
        OR (SELECT count(DISTINCT node_row->>'id') FROM jsonb_array_elements(p_graph->'nodes') node_row) <> jsonb_array_length(p_graph->'nodes')
        OR (SELECT count(DISTINCT e->>'id') FROM jsonb_array_elements(p_graph->'edges') e) <> jsonb_array_length(p_graph->'edges')
        OR jsonb_array_length(p_graph->'edges') <> jsonb_array_length(p_graph->'nodes') - 1 THEN RETURN FALSE; END IF;
    SELECT value INTO source FROM jsonb_array_elements(p_graph->'nodes') WHERE value->>'type' = 'source';
    SELECT value INTO upload FROM jsonb_array_elements(p_graph->'nodes') WHERE value->>'type' = 'youtube_upload';
    SELECT value INTO export FROM jsonb_array_elements(p_graph->'nodes') WHERE value->>'type' = 'export';
    IF NOT COALESCE(source->'data'->>'asset_id' = p_asset AND source->'data'->'config'->>'asset_id' = p_asset
        AND source->'data'->'config'->>'media_type' = 'video' AND upload->'data'->'config'->>'privacy' = 'unlisted', FALSE)
    THEN RETURN FALSE; END IF;
    SELECT e->>'source' INTO parent FROM jsonb_array_elements(p_graph->'edges') e WHERE e->>'target' = upload->>'id';
    IF NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'id' = parent AND node_row->>'type' = 'transcode')
        OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_graph->'edges') e WHERE e->>'source' = parent AND e->>'target' = export->>'id')
    THEN RETURN FALSE; END IF;
    FOR n IN SELECT value FROM jsonb_array_elements(p_graph->'nodes') LOOP
        IF NOT COALESCE(n->>'type' IN ('source','trim','vertical_crop','title_overlay','transcode','export','youtube_upload')
            AND (n = source OR n->'data'->>'asset_id' IS NULL AND NOT n->'data'->'config' ? 'asset_id'), FALSE)
            OR (SELECT count(*) FROM jsonb_array_elements(p_graph->'edges') e WHERE e->>'target' = n->>'id') <> (CASE WHEN n = source THEN 0 ELSE 1 END)
            OR (SELECT count(*) FROM jsonb_array_elements(p_graph->'edges') e WHERE e->>'source' = n->>'id') <>
                (CASE WHEN n = upload OR n = export THEN 0 WHEN n->>'id' = parent THEN 2 ELSE 1 END)
        THEN RETURN FALSE; END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_graph->'edges') e WHERE
        e->>'sourceHandle' IS DISTINCT FROM 'output' OR e->>'targetHandle' IS DISTINCT FROM 'input'
        OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'id' = e->>'source')
        OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_graph->'nodes') node_row WHERE node_row->>'id' = e->>'target')) THEN RETURN FALSE; END IF;
    WITH RECURSIVE reachable(id) AS (
        SELECT source->>'id'
        UNION SELECT e->>'target' FROM jsonb_array_elements(p_graph->'edges') e JOIN reachable r ON e->>'source' = r.id
    ) SELECT count(*) INTO v_reachable FROM reachable;
    RETURN v_reachable = jsonb_array_length(p_graph->'nodes');
END;
$graph$;
"""

UPLOAD_SQL = """
CREATE FUNCTION public.vp_owned_producer_upload(p_job_id uuid, p_node_id uuid, p_artifact_id uuid,
    p_sha text, p_title text, p_privacy text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $upload$
DECLARE r json; t json; a json; c json; i json; item json; seed json; asset json; entry json; part json; binding json;
        j json; n json; art json; graph jsonb; evidence jsonb; history jsonb; request jsonb;
        plan public.autoflow_plans%ROWTYPE; run public.autoflow_runs%ROWTYPE; v_now timestamptz;
        v_task uuid; v_uc text; v_count integer; v_fields text[]; v_descriptor jsonb; v_source json;
        v_ancestors text[]; v_predecessor json; v_input json;
BEGIN
    -- The existing RPC entry already holds channel -> schedule -> canonical /
    -- inventory before its task/job/node/registration locks. No new worker grant.
    IF NOT EXISTS (SELECT 1 FROM public.owned_seed_inventories WHERE approved_at IS NOT NULL) THEN RETURN; END IF;
    r := public.vp_owned_producer_rows(); v_now := pg_catalog.clock_timestamp();
    IF (SELECT count(*) FROM json_array_elements(r->'production_tasks') task_row WHERE task_row->>'job_id' = p_job_id::text) <> 1
    THEN RAISE EXCEPTION 'owned_inventory_producer_missing'; END IF;
    SELECT value INTO t FROM json_array_elements(r->'production_tasks') WHERE value->>'job_id' = p_job_id::text;
    v_task := (t->>'id')::uuid;
    a := public.vp_owned_producer_find(r, 'publishing_accounts', t->>'target_account_id');
    c := public.vp_owned_producer_find(r, 'channel_profiles', t->>'channel_profile_id');
    v_uc := a->>'platform_account_id';
    history := public.vp_owned_producer_history(r, v_task, v_uc, v_now);
    IF history->'bindings' ? (a->>'id') OR a->>'id' = history->'retired'->>'legacy_account_id'
        OR c->>'id' = history->'retired'->>'legacy_channel_profile_id'
        OR EXISTS (SELECT 1 FROM jsonb_each(history->'bindings') b WHERE b.value->>'legacy_channel_profile_id' = c->>'id')
    THEN RAISE EXCEPTION 'owned_inventory_historical_producer_pinned'; END IF;
    IF NOT COALESCE(COALESCE(NULLIF(a->>'platform',''), 'youtube') = 'youtube' AND v_uc ~ '^UC[A-Za-z0-9_-]{22}$'
        AND a->>'channel_profile_id' = c->>'id', FALSE) THEN RAISE EXCEPTION 'owned_history_unclassified'; END IF;
    SELECT count(*) INTO v_count FROM json_array_elements(r->'owned_seed_inventories') inventory_row
    WHERE inventory_row->>'approved_at' IS NOT NULL AND inventory_row->>'succession_released_at' IS NULL AND inventory_row->>'platform_channel_id' = v_uc;
    IF EXISTS (SELECT 1 FROM json_array_elements(r->'youtube_upload_operations') o
        JOIN json_array_elements(r->'production_tasks') old_t ON old_t->>'id' = o->>'production_task_id'
        WHERE old_t->>'target_account_id' IN (SELECT jsonb_array_elements_text(history->'members'))
        AND old_t->>'id' <> t->>'id' AND o->>'content_sha256' = p_sha)
        OR history->'retired_hashes' ? p_sha THEN RAISE EXCEPTION 'owned_inventory_render_reuse'; END IF;
    IF v_count = 0 THEN
        IF EXISTS (SELECT 1 FROM json_array_elements(r->'owned_seed_inventory_items') it WHERE it->>'production_task_id' = t->>'id')
            OR t::jsonb->'agent_approval_evidence_json' ? 'owned_inventory'
            OR t::jsonb->'channel_config_snapshot_json' ? 'owned_inventory'
        THEN RAISE EXCEPTION 'owned_inventory_producer_binding'; END IF;
        IF jsonb_array_length(history->'retired_hashes') > 0 THEN RAISE EXCEPTION 'owned_inventory_source_evidence_missing'; END IF;
        RETURN;
    END IF;
    IF v_count <> 1 OR (SELECT count(*) FROM json_array_elements(r->'owned_seed_inventory_items') it
        WHERE it->>'production_task_id' = t->>'id') <> 1 THEN RAISE EXCEPTION 'owned_inventory_producer_binding'; END IF;
    SELECT value INTO i FROM json_array_elements(r->'owned_seed_inventories') WHERE value->>'approved_at' IS NOT NULL
        AND value->>'succession_released_at' IS NULL AND value->>'platform_channel_id' = v_uc;
    SELECT value INTO item FROM json_array_elements(r->'owned_seed_inventory_items') WHERE value->>'production_task_id' = t->>'id';
    IF NOT COALESCE(i->>'state' IN ('approved','exhausted') AND i->>'revoked_at' IS NULL AND i->>'hold_reason' IS NULL
        AND (i->>'starts_at')::timestamptz <= v_now AND v_now < (i->>'expires_at')::timestamptz
        AND (i->>'expires_at')::timestamptz - (i->>'starts_at')::timestamptz = INTERVAL '7 days'
        AND i->>'privacy' = 'unlisted' AND i->>'max_admissions' = '7' AND i->>'minimum_interval_seconds' = '86400', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_producer_inactive'; END IF;
    IF NOT COALESCE(item->>'inventory_id' = i->>'id' AND item->>'state' = 'reserved' AND item->>'consumed_at' IS NOT NULL
        AND item->>'completed_at' IS NULL AND item->>'manual_seed_id' = t->>'manual_seed_id'
        AND item->>'platform_channel_id' = v_uc AND c->>'owned_seed_inventory_id' = i->>'id'
        AND i->>'channel_profile_id' = c->>'id' AND i->>'target_account_id' = a->>'id'
        AND i->>'topic_lane_id' = t->>'topic_lane_id' AND i->>'lane_format_id' = t->>'lane_format_id', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_producer_binding'; END IF;
    IF NOT COALESCE(c->>'enabled' = 'true' AND c->>'dry_run' = 'false' AND c->>'halted_at' IS NULL
        AND (c->>'intake_paused_at' IS NULL OR i->>'state' = 'exhausted' AND c->>'intake_pause_reason' = 'owned_inventory_exhausted')
        AND a->>'enabled' = 'true' AND a->>'paused_until' IS NULL AND a->>'default_privacy' = 'unlisted'
        AND a->>'external_asset_auto_publish' = 'false' AND p_privacy = 'unlisted'
        AND t->>'source' = 'manual_seed' AND t->>'approval_mode' = 'agent' AND t->>'uses_external_assets' = 'false'
        AND t::jsonb->'source_platforms_json' = '[]'::jsonb AND t::jsonb->'material_library_ids_json' = '[]'::jsonb
        AND t->>'retry_count' = '0' AND t->>'failure_reason' IS NULL AND t->>'blocked_by_guard' IS NULL
        AND t->>'state' = 'producing', FALSE) THEN RAISE EXCEPTION 'owned_inventory_producer_controls'; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.topic_lanes l JOIN public.lane_format_matrix f ON f.topic_lane_id = l.id
        WHERE l.id = (i->>'topic_lane_id')::uuid AND f.id = (i->>'lane_format_id')::uuid
        AND l.channel_profile_id = (c->>'id')::uuid AND l.enabled AND l.paused_until IS NULL
        AND f.enabled AND f.source_platforms_json::jsonb = '[]'::jsonb AND f.default_publish_visibility = 'unlisted')
        OR public.vp_owned_producer_config((c->>'id')::uuid, (a->>'id')::uuid, (i->>'topic_lane_id')::uuid, (i->>'lane_format_id')::uuid)
            IS DISTINCT FROM i->'manifest_json'->>'configuration_sha256'
    THEN RAISE EXCEPTION 'owned_inventory_configuration_changed'; END IF;
    seed := public.vp_owned_producer_find(r, 'manual_seeds', item->>'manual_seed_id');
    IF NOT COALESCE(seed->>'status' = 'exhausted' AND seed->>'prompt' = t->>'prompt' AND seed->>'title_seed' = t->>'title_seed'
        AND seed->>'source_policy' = 'owned_only' AND seed->>'channel_profile_id' = c->>'id'
        AND seed->>'target_account_id' = a->>'id' AND seed::jsonb->'source_platforms_json' = '[]'
        AND seed::jsonb->'material_library_ids_json' = '[]'
        AND seed->'constraints_json'->>'input_asset_id' = item->>'asset_id'
        AND seed->'constraints_json'->>'source_strategy' = 'input_video' AND seed->'constraints_json'->>'planning_mode' = 'template'
        AND t::jsonb->'channel_config_snapshot_json'->'manual_seed'->'constraints_json' = seed::jsonb->'constraints_json', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_seed_changed'; END IF;
    IF (SELECT count(*) FROM json_array_elements(r->'owned_seed_inventory_items') it WHERE it->>'inventory_id' = i->>'id') <> 7
        OR json_array_length(i->'manifest_json'->'entries') <> 7 THEN RAISE EXCEPTION 'owned_inventory_cardinality_invalid'; END IF;
    v_count := 0;
    FOR part IN SELECT value FROM json_array_elements(r->'owned_seed_inventory_items') WHERE value->>'inventory_id' = i->>'id'
                ORDER BY (value->>'ordinal')::integer LOOP
        v_count := v_count + 1;
        binding := public.vp_owned_producer_find(r, 'manual_seeds', part->>'manual_seed_id');
        entry := i->'manifest_json'->'entries'->(v_count - 1);
        IF NOT COALESCE(part->>'ordinal' = v_count::text AND entry->>'id' = part->>'id'
            AND entry->>'manual_seed_id' = part->>'manual_seed_id' AND entry->>'asset_id' = part->>'asset_id'
            AND entry->>'content_sha256' = part->>'content_sha256' AND entry->>'byte_size' = part->>'byte_size'
            AND entry::jsonb->'storage_descriptor' = part::jsonb->'storage_descriptor_json'
            AND entry->>'seed_sha256' = part->>'seed_sha256' AND entry->>'provenance_sha256' = part->>'provenance_sha256'
            AND entry->>'prompt' = binding->>'prompt' AND entry->>'title_seed' = binding->>'title_seed'
            AND public.vp_owned_producer_hash(part->'provenance_evidence_json') = part->>'provenance_sha256'
            AND entry::jsonb->'provenance_evidence' = part::jsonb->'provenance_evidence_json'
            AND public.vp_owned_producer_hash(public.vp_owned_producer_fields(binding,
                ARRAY['id','channel_profile_id','topic_lane_id','target_account_id','prompt','title_seed','source_policy',
                      'source_platforms_json','material_library_ids_json','constraints_json'])) = part->>'seed_sha256', FALSE)
        THEN RAISE EXCEPTION 'owned_inventory_manifest_changed'; END IF;
    END LOOP;
    IF history->'retired_hashes' ? (item->>'content_sha256') OR EXISTS (
        SELECT 1 FROM json_array_elements(r->'owned_seed_inventory_items') it WHERE it->>'platform_channel_id' = v_uc
        AND it->>'state' <> 'unused' AND it->>'production_task_id' <> t->>'id' AND it->>'content_sha256' = item->>'content_sha256')
    THEN RAISE EXCEPTION 'owned_inventory_retired_hash_reuse'; END IF;
    asset := public.vp_owned_producer_find(r, 'assets', item->>'asset_id');
    SELECT array_agg(key) INTO v_fields FROM json_each(asset->'media_info') WHERE key NOT IN ('license','provenance');
    v_descriptor := public.vp_owned_producer_fields(asset, ARRAY['id','storage_backend','storage_path','file_size','mime_type'])::jsonb
        || jsonb_build_object('media_info_sha256', public.vp_owned_producer_hash(public.vp_owned_producer_fields(asset->'media_info', COALESCE(v_fields, '{}'))));
    IF v_descriptor IS DISTINCT FROM item::jsonb->'storage_descriptor_json'
        OR asset->'media_info'->>'license' IS DISTINCT FROM 'owned' OR asset->'media_info'->>'provenance' IS DISTINCT FROM 'generated'
    THEN RAISE EXCEPTION 'owned_inventory_asset_changed'; END IF;
    evidence := t::jsonb->'agent_approval_evidence_json';
    IF evidence->'owned_inventory' IS DISTINCT FROM t::jsonb->'channel_config_snapshot_json'->'owned_inventory'
        OR NOT COALESCE(evidence->'owned_inventory'->>'inventory_id' = i->>'id'
            AND evidence->'owned_inventory'->>'item_id' = item->>'id'
            AND evidence->'owned_inventory'->>'input_asset_id' = item->>'asset_id'
            AND evidence->'owned_inventory'->>'source_content_sha256' = item->>'content_sha256'
            AND evidence->'owned_inventory'->>'seed_sha256' = item->>'seed_sha256'
            AND evidence->'owned_inventory'->>'manifest_sha256' = i->>'manifest_sha256'
            AND evidence->'owned_inventory'->>'configuration_sha256' = i->'manifest_json'->>'configuration_sha256', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_evidence_changed'; END IF;
    IF NOT public.vp_owned_producer_pds(evidence->'candidate_pds') OR NOT public.vp_owned_producer_pds(evidence->'plan_pds'->'response')
    THEN RAISE EXCEPTION 'owned_inventory_pds_evidence'; END IF;
    request := evidence->'candidate_pds_request';
    IF NOT COALESCE(request->>'actor_id' = a->>'id' AND request->>'action_type' = 'candidate_accept' AND request->>'platform' = 'youtube'
        AND request->'context'->>'candidate_id' = 'owned_inventory:' || (i->>'id') || ':' || (item->>'id')
        AND request->'context'->'owned_inventory' = evidence->'owned_inventory'
        AND request->'content'->>'title' = t->>'title_seed' AND request->'content'->>'description' = t->>'prompt', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_pds_binding'; END IF;
    request := evidence->'plan_pds'->'request';
    IF NOT COALESCE(request->>'actor_id' = a->>'id' AND request->>'action_type' = 'plan_approval' AND request->>'platform' = 'youtube'
        AND request->'context'->>'production_task_id' = t->>'id' AND request->'context'->>'autoflow_plan_id' = t->>'autoflow_plan_id'
        AND (request->'context'->>'channel_id' IS NULL OR request->'context'->>'channel_id' = c->>'id')
        AND request->'content'->>'title' = t->>'title_seed' AND request->'content'->>'description' = t->>'prompt', FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_pds_binding'; END IF;
    SELECT * INTO plan FROM public.autoflow_plans WHERE id = (t->>'autoflow_plan_id')::uuid;
    IF NOT FOUND THEN RAISE EXCEPTION 'owned_inventory_plan_missing'; END IF;
    SELECT * INTO run FROM public.autoflow_runs WHERE id = (t->>'autoflow_run_id')::uuid;
    IF NOT FOUND OR NOT COALESCE(run.job_id = p_job_id AND run.plan_id = plan.id AND run.pipeline_id = (t->>'pipeline_id')::uuid
        AND plan.approved_revision = plan.execution_revision
        AND plan.approved_revision_hash = t->'rationale_json'->'autoflow_plan_payload'->>'expected_approved_revision_hash'
        AND plan.approved_revision::text = t->'rationale_json'->'autoflow_plan_payload'->>'expected_approved_revision'
        AND (plan.review_approved_at IS NOT NULL OR plan.agent_approved_by IS NOT NULL), FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_plan_changed'; END IF;
    j := public.vp_owned_producer_find(r, 'jobs', p_job_id::text);
    n := public.vp_owned_producer_find(r, 'node_executions', p_node_id::text);
    art := public.vp_owned_producer_find(r, 'artifacts', p_artifact_id::text);
    graph := j::jsonb->'pipeline_snapshot';
    IF NOT COALESCE(j->>'status' = 'RUNNING' AND j->>'error_message' IS NULL
        AND graph = plan.pipeline_definition::jsonb AND public.vp_owned_producer_graph(graph, item->>'asset_id')
        AND n->>'job_id' = p_job_id::text AND n->>'node_type' = 'youtube_upload' AND n->>'status' = 'RUNNING'
        AND n::jsonb->'input_artifact_ids' = jsonb_build_array(p_artifact_id::text)
        AND n->'node_config'->>'title' = p_title AND n->'node_config'->>'privacy' = p_privacy
        AND art->>'job_id' = p_job_id::text
        AND EXISTS (SELECT 1 FROM json_array_elements(r->'node_executions')
            WHERE value->>'id' = art->>'node_execution_id' AND value->>'job_id' = p_job_id::text), FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_pipeline_binding'; END IF;
    WITH RECURSIVE ancestors(node_key) AS (
        SELECT n->>'node_id'
        UNION SELECT edge_row->>'source' FROM jsonb_array_elements(graph->'edges') edge_row
            JOIN ancestors ancestor_row ON edge_row->>'target' = ancestor_row.node_key
    ) SELECT array_agg(node_key) INTO v_ancestors FROM ancestors;
    v_count := 0;
    FOR part IN SELECT value FROM json_array_elements(r->'node_executions') WHERE value->>'job_id' = p_job_id::text LOOP
        v_count := v_count + 1;
        SELECT value::json INTO entry FROM jsonb_array_elements(graph->'nodes') WHERE value->>'id' = part->>'node_id';
        IF NOT COALESCE(entry->>'type' = part->>'node_type' AND part->>'error_message' IS NULL
            AND part->>'retry_count' = '0' AND part->>'status' IN ('PENDING','QUEUED','RUNNING','SUCCEEDED')
            AND part::jsonb->'node_config' = (entry::jsonb->'data'->'config' || CASE WHEN entry->'data'->>'asset_id' IS NULL THEN '{}'::jsonb
                ELSE jsonb_build_object('asset_id', entry->'data'->>'asset_id') END), FALSE)
        THEN RAISE EXCEPTION 'owned_inventory_pipeline_binding'; END IF;
        IF part->>'node_id' = ANY(v_ancestors) THEN
            IF part->>'id' <> p_node_id::text AND part->>'status' IS DISTINCT FROM 'SUCCEEDED'
            THEN RAISE EXCEPTION 'owned_inventory_artifact_lineage'; END IF;
            IF part->>'node_type' = 'source' THEN
                IF part::jsonb->'input_artifact_ids' IS DISTINCT FROM '[]'::jsonb
                THEN RAISE EXCEPTION 'owned_inventory_artifact_lineage'; END IF;
            ELSE
                SELECT value INTO v_predecessor FROM json_array_elements(r->'node_executions')
                WHERE value->>'job_id' = p_job_id::text AND value->>'node_id' = (
                    SELECT edge_row->>'source' FROM jsonb_array_elements(graph->'edges') edge_row
                    WHERE edge_row->>'target' = part->>'node_id');
                SELECT value INTO v_input FROM json_array_elements(r->'artifacts')
                WHERE value->>'id' = v_predecessor->>'output_artifact_id';
                IF NOT COALESCE(v_predecessor->>'status' = 'SUCCEEDED'
                    AND v_input->>'job_id' = p_job_id::text
                    AND v_input->>'node_execution_id' = v_predecessor->>'id'
                    AND part::jsonb->'input_artifact_ids' = jsonb_build_array(v_predecessor->>'output_artifact_id'), FALSE)
                THEN RAISE EXCEPTION 'owned_inventory_artifact_lineage'; END IF;
            END IF;
        END IF;
        IF part->>'node_type' = 'source' THEN
            v_source := public.vp_owned_producer_find(r, 'artifacts', part->>'output_artifact_id');
            IF part->>'status' IS DISTINCT FROM 'SUCCEEDED' OR v_source->>'node_execution_id' IS DISTINCT FROM part->>'id'
                OR v_source->'media_info'->>'asset_id' IS DISTINCT FROM item->>'asset_id'
                OR v_source->'media_info'->>'source_asset_id' IS DISTINCT FROM item->>'asset_id'
                OR public.vp_owned_producer_fields(v_source, ARRAY['filename','mime_type','file_size','storage_backend','storage_path'])::jsonb IS DISTINCT FROM
                   public.vp_owned_producer_fields(asset, ARRAY['filename','mime_type','file_size','storage_backend','storage_path'])::jsonb
            THEN RAISE EXCEPTION 'owned_inventory_source_changed'; END IF;
        END IF;
        IF part->>'id' = art->>'node_execution_id' AND (part->>'status' <> 'SUCCEEDED'
            OR part->>'output_artifact_id' IS DISTINCT FROM p_artifact_id::text
            OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(graph->'edges') e
                WHERE e->>'source' = part->>'node_id' AND e->>'target' = n->>'node_id'))
        THEN RAISE EXCEPTION 'owned_inventory_artifact_changed'; END IF;
    END LOOP;
    IF v_count <> jsonb_array_length(graph->'nodes') THEN RAISE EXCEPTION 'owned_inventory_pipeline_binding'; END IF;
END;
$upload$;
"""

HELPERS = {
    "vp_owned_producer_canonical(json)": CANONICAL_SQL,
    "vp_owned_producer_hash(json)": HASH_SQL,
    "vp_owned_producer_fields(json,text[])": FIELDS_SQL,
    "vp_owned_producer_rows()": rows_sql(),
    "vp_owned_producer_find(json,text,text)": FIND_SQL,
    "vp_owned_producer_pds(jsonb)": PDS_SQL,
    "vp_owned_producer_retired(json,json,timestamptz)": RETIRED_SQL,
    "vp_owned_producer_queue(jsonb,text[])": QUEUE_SQL,
    "vp_owned_producer_normal(json,json,timestamptz)": NORMAL_SQL,
    "vp_owned_producer_history(json,uuid,text,timestamptz)": HISTORY_SQL,
    "vp_owned_producer_config(uuid,uuid,uuid,uuid)": config_sql(),
    "vp_owned_producer_graph(jsonb,text)": GRAPH_SQL,
    "vp_owned_producer_upload(uuid,uuid,uuid,text,text,text)": UPLOAD_SQL,
}


def upgrade():
    for signature, body in HELPERS.items():
        op.execute(body)
        op.execute(f"REVOKE ALL ON FUNCTION public.{signature} FROM PUBLIC")
    op.execute(patch_sql())


def downgrade():
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM public.owned_seed_inventories WHERE approved_at IS NOT NULL)
        THEN RAISE EXCEPTION 'owned_inventory_history_requires_preservation'; END IF;
    END $$;""")
    op.execute(patch_sql(remove=True))
    for signature in reversed(HELPERS):
        op.execute(f"DROP FUNCTION public.{signature}")
