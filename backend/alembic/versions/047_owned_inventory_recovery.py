"""Audited recovery of an unused inventory's diagnosed operational hold.

Revision ID: 047_owned_inventory_recovery
Revises: 046_owned_retained_terminal
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "047_owned_inventory_recovery"
down_revision = "046_owned_retained_terminal"
branch_labels = None
depends_on = None

COMMIT_SIGNATURE = "public.vp_recover_owned_inventory(uuid,uuid,uuid,text,text,text,jsonb,jsonb,jsonb)"
CONSUME_SIGNATURE = "public.vp_owned_inventory_consume_recovery(jsonb,jsonb)"
OLD_PREDICATE = "OR (OLD.state <> 'approved' AND NEW.state = 'approved'))"
NEW_PREDICATE = "OR (OLD.state <> 'approved' AND NEW.state = 'approved' AND NOT public.vp_owned_inventory_consume_recovery(to_jsonb(OLD),to_jsonb(NEW))))"


def upgrade():
    op.create_table("owned_inventory_recoveries",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("inventory_id", UUID(as_uuid=True), sa.ForeignKey("owned_seed_inventories.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.Column("expected_hold_sha256", sa.String(64), nullable=False),
        sa.Column("recovered_by", sa.String(255), nullable=False),
        sa.Column("recovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("transaction_id", sa.BigInteger(), nullable=False),
        sa.Column("before_inventory_json", JSONB(), nullable=False),
        sa.Column("before_channel_json", JSONB(), nullable=False),
        sa.Column("evidence_json", JSONB(), nullable=False),
        sa.UniqueConstraint("inventory_id", "expected_hold_sha256", name="uq_owned_recovery_incident"))
    op.create_table("owned_inventory_recovery_permits",
        sa.Column("inventory_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("recovery_id", UUID(as_uuid=True), sa.ForeignKey("owned_inventory_recoveries.id", ondelete="RESTRICT"), nullable=False, unique=True),
        sa.Column("transaction_id", sa.BigInteger(), nullable=False),
        sa.Column("before_inventory_json", JSONB(), nullable=False),
        sa.Column("after_inventory_json", JSONB(), nullable=False))
    op.execute("REVOKE ALL ON TABLE public.owned_inventory_recoveries,public.owned_inventory_recovery_permits FROM PUBLIC")
    op.execute("""
CREATE FUNCTION public.vp_owned_inventory_recovery_audit_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $audit$
BEGIN RAISE EXCEPTION 'owned_inventory_recovery_audit_immutable'; END;
$audit$;
""")
    op.execute("REVOKE ALL ON FUNCTION public.vp_owned_inventory_recovery_audit_immutable() FROM PUBLIC")
    op.execute("CREATE TRIGGER owned_inventory_recovery_audit_immutable BEFORE UPDATE OR DELETE ON public.owned_inventory_recoveries FOR EACH ROW EXECUTE FUNCTION public.vp_owned_inventory_recovery_audit_immutable()")
    op.execute("CREATE TRIGGER owned_inventory_recovery_audit_no_truncate BEFORE TRUNCATE ON public.owned_inventory_recoveries FOR EACH STATEMENT EXECUTE FUNCTION public.vp_owned_inventory_recovery_audit_immutable()")
    op.execute("""
CREATE FUNCTION public.vp_owned_inventory_consume_recovery(p_before jsonb,p_after jsonb) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $permit$
DECLARE consumed uuid;
BEGIN
    DELETE FROM public.owned_inventory_recovery_permits p
    USING public.owned_inventory_recoveries a
    WHERE p.inventory_id=(p_before->>'id')::uuid AND p.transaction_id=txid_current()
      AND p.before_inventory_json=p_before AND p.after_inventory_json=p_after
      AND a.id=p.recovery_id AND a.transaction_id=p.transaction_id
      AND a.inventory_id=p.inventory_id AND a.before_inventory_json=p_before
    RETURNING p.recovery_id INTO consumed;
    RETURN consumed IS NOT NULL;
END;
$permit$;
""")
    op.execute(f"REVOKE ALL ON FUNCTION {CONSUME_SIGNATURE} FROM PUBLIC")
    # Preserve the installed function's owner, ACL, search path and all other guards.
    op.execute(f"""
DO $replace$ DECLARE definition text;
BEGIN
    SELECT pg_get_functiondef('public.owned_inventory_immutable()'::regprocedure) INTO definition;
    IF length(definition)-length(replace(definition,$old${OLD_PREDICATE}$old$,'')) <> length($old${OLD_PREDICATE}$old$)
    THEN RAISE EXCEPTION 'owned_inventory_recovery_guard_definition_changed'; END IF;
    EXECUTE replace(definition,$old${OLD_PREDICATE}$old$,$new${NEW_PREDICATE}$new$);
END;
$replace$;
""")
    op.execute("""
CREATE FUNCTION public.vp_recover_owned_inventory(
    p_channel uuid,p_inventory uuid,p_request uuid,p_subject text,p_request_sha text,p_hold_sha text,
    p_before_inventory jsonb,p_before_channel jsonb,p_evidence jsonb) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $recover$
DECLARE c public.channel_profiles; i public.owned_seed_inventories; s public.runtime_schedules;
    n timestamptz; next_inventory jsonb; existing public.owned_inventory_recoveries; changed integer;
BEGIN
    IF NOT COALESCE(length(btrim(p_subject)) BETWEEN 1 AND 255 AND p_request_sha ~ '^[0-9a-f]{64}$'
        AND p_hold_sha ~ '^[0-9a-f]{64}$' AND p_request IS NOT NULL,FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_recovery_request'; END IF;
    SELECT * INTO c FROM public.channel_profiles WHERE id=p_channel FOR UPDATE;
    SELECT * INTO s FROM public.runtime_schedules WHERE service_name='videoprocess' FOR UPDATE;
    SELECT * INTO i FROM public.owned_seed_inventories WHERE id=p_inventory;
    IF c.id IS NULL OR i.id IS NULL OR i.channel_profile_id<>c.id THEN RAISE EXCEPTION 'owned_inventory_recovery_scope'; END IF;
    PERFORM pg_advisory_xact_lock(('x'||substr(encode(sha256(convert_to('owned-inventory:'||i.platform_channel_id,'UTF8')),'hex'),1,16))::bit(64)::bigint);
    SELECT * INTO i FROM public.owned_seed_inventories WHERE id=p_inventory FOR UPDATE;
    SELECT * INTO existing FROM public.owned_inventory_recoveries WHERE id=p_request;
    IF existing.id IS NOT NULL THEN
        IF existing.inventory_id<>p_inventory OR existing.request_sha256<>p_request_sha OR existing.recovered_by<>p_subject
        THEN RAISE EXCEPTION 'owned_inventory_recovery_idempotency_conflict'; END IF;
        RETURN existing.id;
    END IF;
    n := clock_timestamp();
    IF NOT COALESCE(to_jsonb(i)=p_before_inventory AND to_jsonb(c)=p_before_channel
        AND i.state='held' AND i.hold_reason='redis_consumer_identity_invalid' AND i.manifest_json::jsonb->>'version'='2'
        AND i.approved_at IS NOT NULL AND i.approved_at<=n AND length(btrim(i.approved_by))>0 AND length(btrim(i.approval_reference))>0
        AND i.revoked_at IS NULL AND i.revoked_by IS NULL AND i.succession_released_at IS NULL
        AND i.starts_at<=n AND n<i.expires_at AND i.expires_at-i.starts_at=interval '168 hours'
        AND i.privacy='unlisted' AND i.max_admissions=7 AND i.minimum_interval_seconds=86400
        AND c.owned_seed_inventory_id=i.id AND c.tick_interval_minutes=1 AND c.enabled AND NOT c.dry_run
        AND c.halted_at IS NULL AND c.halt_reason IS NULL AND c.intake_paused_at IS NOT NULL
        AND c.intake_pause_reason='redis_consumer_identity_invalid' AND s.state='CLOSED' AND s.guarded_job_id IS NULL,FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_recovery_before_state_changed'; END IF;
    PERFORM id FROM public.owned_seed_inventory_items WHERE inventory_id=i.id ORDER BY ordinal FOR UPDATE;
    IF (SELECT count(*) FROM public.owned_seed_inventory_items WHERE inventory_id=i.id)<>7
        OR EXISTS(SELECT 1 FROM public.owned_seed_inventory_items item JOIN public.manual_seeds seed ON seed.id=item.manual_seed_id
            WHERE item.inventory_id=i.id AND (item.state<>'unused' OR item.production_task_id IS NOT NULL
            OR item.consumed_at IS NOT NULL OR item.completed_at IS NOT NULL OR item.hold_reason IS NOT NULL OR seed.status<>'active'))
        OR EXISTS(SELECT 1 FROM public.production_tasks t WHERE t.channel_profile_id=c.id OR t.target_account_id=i.target_account_id
            OR t.manual_seed_id IN(SELECT manual_seed_id FROM public.owned_seed_inventory_items WHERE inventory_id=i.id))
    THEN RAISE EXCEPTION 'owned_inventory_recovery_current_work'; END IF;
    IF NOT COALESCE(jsonb_typeof(p_evidence)='object' AND octet_length(p_evidence::text)<=32768
        AND p_evidence->>'version'='1' AND p_evidence->'operator_runtime_evidence'->>'verification'='authenticated_operator_attestation'
        AND p_evidence->>'history_sha256' ~ '^[0-9a-f]{64}$' AND p_evidence->>'history_authority_sha256' ~ '^[0-9a-f]{64}$'
        AND jsonb_array_length(p_evidence->'runtime_redis')=5 AND jsonb_array_length(p_evidence->'source_sha256')=7
        AND (p_evidence->>'observed_at')::timestamptz<=n AND n-(p_evidence->>'observed_at')::timestamptz<=interval '60 seconds',FALSE)
    THEN RAISE EXCEPTION 'owned_inventory_recovery_evidence_invalid'; END IF;
    INSERT INTO public.owned_inventory_recoveries(id,inventory_id,request_sha256,expected_hold_sha256,recovered_by,recovered_at,
        transaction_id,before_inventory_json,before_channel_json,evidence_json)
    VALUES(p_request,i.id,p_request_sha,p_hold_sha,p_subject,n,txid_current(),to_jsonb(i),to_jsonb(c),p_evidence);
    next_inventory := to_jsonb(i)||jsonb_build_object('state','approved','hold_reason',NULL,'updated_at',n AT TIME ZONE 'UTC');
    INSERT INTO public.owned_inventory_recovery_permits(inventory_id,recovery_id,transaction_id,before_inventory_json,after_inventory_json)
    VALUES(i.id,p_request,txid_current(),to_jsonb(i),next_inventory);
    UPDATE public.owned_seed_inventories SET state='approved',hold_reason=NULL,updated_at=n AT TIME ZONE 'UTC' WHERE id=i.id;
    IF EXISTS(SELECT 1 FROM public.owned_inventory_recovery_permits WHERE recovery_id=p_request)
    THEN RAISE EXCEPTION 'owned_inventory_recovery_permit_unconsumed'; END IF;
    UPDATE public.channel_profiles SET intake_paused_at=NULL,intake_pause_reason=NULL,updated_at=n AT TIME ZONE 'UTC'
    WHERE id=c.id AND to_jsonb(channel_profiles)=p_before_channel;
    GET DIAGNOSTICS changed=ROW_COUNT;
    IF changed<>1 THEN RAISE EXCEPTION 'owned_inventory_recovery_pause_changed'; END IF;
    RETURN p_request;
END;
$recover$;
""")
    # Only the existing administrative API identity can invoke recovery. No worker,
    # watcher, operator-runtime or orchestration-runtime permissions are added.
    op.execute(f"REVOKE ALL ON FUNCTION {COMMIT_SIGNATURE} FROM PUBLIC")


def downgrade():
    op.execute("""DO $$ BEGIN
        IF EXISTS(SELECT 1 FROM public.owned_inventory_recoveries)
        THEN RAISE EXCEPTION 'owned_inventory_recovery_history_requires_preservation'; END IF;
    END $$""")
    op.execute(f"DROP FUNCTION {COMMIT_SIGNATURE}")
    op.execute(f"""DO $$ DECLARE definition text; BEGIN
        SELECT pg_get_functiondef('public.owned_inventory_immutable()'::regprocedure) INTO definition;
        IF position($new${NEW_PREDICATE}$new$ IN definition)=0 THEN RAISE EXCEPTION 'owned_inventory_recovery_guard_definition_changed'; END IF;
        EXECUTE replace(definition,$new${NEW_PREDICATE}$new$,$old${OLD_PREDICATE}$old$);
    END $$""")
    op.execute(f"DROP FUNCTION {CONSUME_SIGNATURE}")
    op.drop_table("owned_inventory_recovery_permits")
    op.drop_table("owned_inventory_recoveries")
    op.execute("DROP FUNCTION public.vp_owned_inventory_recovery_audit_immutable()")
