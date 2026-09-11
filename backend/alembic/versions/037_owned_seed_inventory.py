"""Add the disabled finite owned-seed inventory foundation."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID


revision = "037_owned_seed_inventory"
down_revision = "036_worker_session_signal"
branch_labels = None
depends_on = None


def _id():
    return sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()"))


def _ref(name, target, nullable=False):
    return sa.Column(name, UUID(as_uuid=True), sa.ForeignKey(target, ondelete="RESTRICT"), nullable=nullable)


def upgrade():
    op.create_table(
        "owned_seed_inventories", _id(),
        sa.Column("client_request_id", UUID(as_uuid=True), nullable=False),
        _ref("channel_profile_id", "channel_profiles.id"),
        _ref("topic_lane_id", "topic_lanes.id"),
        _ref("lane_format_id", "lane_format_matrix.id"),
        _ref("target_account_id", "publishing_accounts.id"),
        sa.Column("platform_channel_id", sa.String(24), nullable=False),
        sa.Column("request_sha256", sa.String(64), nullable=False),
        sa.Column("manifest_sha256", sa.String(64), nullable=False),
        sa.Column("manifest_json", sa.JSON(), nullable=False),
        sa.Column("privacy", sa.String(16), nullable=False, server_default="unlisted"),
        sa.Column("max_admissions", sa.Integer(), nullable=False, server_default="7"),
        sa.Column("minimum_interval_seconds", sa.Integer(), nullable=False, server_default="86400"),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="draft"),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True)),
        sa.Column("approved_by", sa.String(255)),
        sa.Column("approval_reference", sa.String(512)),
        _ref("predecessor_inventory_id", "owned_seed_inventories.id", nullable=True),
        sa.Column("predecessor_closeout_sha256", sa.String(64)),
        sa.Column("succession_released_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.Column("revoked_by", sa.String(255)),
        sa.Column("hold_reason", sa.Text()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("client_request_id", name="uq_owned_inventory_request"),
        sa.UniqueConstraint("id", "platform_channel_id", name="uq_owned_inventory_platform_binding"),
        sa.CheckConstraint("state IN ('draft','approved','held','exhausted','expired','revoked')", name="ck_owned_inventory_state"),
        sa.CheckConstraint("privacy = 'unlisted' AND max_admissions = 7 AND minimum_interval_seconds = 86400", name="ck_owned_inventory_limits"),
        sa.CheckConstraint("expires_at > starts_at", name="ck_owned_inventory_window"),
        sa.CheckConstraint("expires_at = starts_at + interval '168 hours'", name="ck_owned_inventory_exact_window"),
        sa.CheckConstraint("approved_at IS NOT NULL OR state IN ('draft','revoked')", name="ck_owned_inventory_approval"),
        sa.CheckConstraint("approved_at IS NULL OR (approved_by IS NOT NULL AND approval_reference IS NOT NULL)", name="ck_owned_inventory_approval_actor"),
        sa.CheckConstraint("succession_released_at IS NULL OR (approved_at IS NOT NULL AND state IN ('held','exhausted','expired','revoked'))", name="ck_owned_inventory_release"),
    )
    for field in ("channel_profile_id", "target_account_id", "platform_channel_id"):
        op.create_index(f"uq_owned_inventory_occupied_{field}", "owned_seed_inventories", [field], unique=True,
                        postgresql_where=sa.text("approved_at IS NOT NULL AND succession_released_at IS NULL"))
    op.create_table(
        "owned_seed_inventory_items", _id(),
        sa.Column("inventory_id", UUID(as_uuid=True), nullable=False),
        sa.Column("platform_channel_id", sa.String(24), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        _ref("manual_seed_id", "manual_seeds.id"), _ref("asset_id", "assets.id"),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("storage_descriptor_json", sa.JSON(), nullable=False),
        sa.Column("provenance_evidence_json", sa.JSON(), nullable=False),
        sa.Column("provenance_sha256", sa.String(64), nullable=False),
        sa.Column("seed_sha256", sa.String(64), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="unused"),
        _ref("production_task_id", "production_tasks.id", nullable=True),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("hold_reason", sa.Text()),
        sa.ForeignKeyConstraint(["inventory_id", "platform_channel_id"],
                                ["owned_seed_inventories.id", "owned_seed_inventories.platform_channel_id"],
                                name="fk_owned_item_scope", ondelete="RESTRICT"),
        sa.UniqueConstraint("inventory_id", "ordinal", name="uq_owned_item_ordinal"),
        sa.UniqueConstraint("inventory_id", "asset_id", name="uq_owned_item_asset"),
        sa.UniqueConstraint("platform_channel_id", "content_sha256", name="uq_owned_item_platform_content"),
        sa.UniqueConstraint("manual_seed_id", name="uq_owned_item_seed"),
        sa.UniqueConstraint("production_task_id", name="uq_owned_item_task"),
        sa.CheckConstraint("ordinal BETWEEN 1 AND 7", name="ck_owned_item_ordinal"),
        sa.CheckConstraint("state IN ('unused','reserved','completed','held')", name="ck_owned_item_state"),
        sa.CheckConstraint("byte_size > 0 AND byte_size <= 67108864", name="ck_owned_item_size"),
        sa.CheckConstraint("(state = 'unused' AND production_task_id IS NULL AND consumed_at IS NULL) OR (state <> 'unused' AND production_task_id IS NOT NULL AND consumed_at IS NOT NULL)", name="ck_owned_item_consumption"),
    )
    op.create_index("uq_owned_item_outstanding", "owned_seed_inventory_items", ["inventory_id"], unique=True,
                    postgresql_where=sa.text("state = 'reserved'"))
    op.add_column("channel_profiles", sa.Column("owned_seed_inventory_id", UUID(as_uuid=True)))
    op.create_foreign_key("fk_channel_owned_inventory", "channel_profiles", "owned_seed_inventories",
                          ["owned_seed_inventory_id"], ["id"], ondelete="RESTRICT")
    op.execute("""
CREATE FUNCTION public.owned_inventory_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_history_immutable', ERRCODE = 'P0001';
    END IF;
    IF OLD.manifest_json::jsonb <> '{}'::jsonb AND
       (to_jsonb(NEW) - ARRAY['state','approved_at','approved_by','approval_reference',
           'predecessor_inventory_id','predecessor_closeout_sha256','succession_released_at',
           'revoked_at','revoked_by','hold_reason','updated_at']) IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['state','approved_at','approved_by','approval_reference',
           'predecessor_inventory_id','predecessor_closeout_sha256','succession_released_at',
           'revoked_at','revoked_by','hold_reason','updated_at']) THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_manifest_immutable', ERRCODE = 'P0001';
    END IF;
    IF OLD.approved_at IS NOT NULL AND
       (ROW(NEW.approved_at, NEW.approved_by, NEW.approval_reference,
            NEW.predecessor_inventory_id, NEW.predecessor_closeout_sha256) IS DISTINCT FROM
        ROW(OLD.approved_at, OLD.approved_by, OLD.approval_reference,
            OLD.predecessor_inventory_id, OLD.predecessor_closeout_sha256)
        OR (OLD.state <> 'approved' AND NEW.state = 'approved')) THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_approval_immutable', ERRCODE = 'P0001';
    END IF;
    IF OLD.succession_released_at IS NOT NULL AND
       NEW.succession_released_at IS DISTINCT FROM OLD.succession_released_at THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_release_immutable', ERRCODE = 'P0001';
    END IF;
    IF OLD.approved_at IS NULL AND NEW.approved_at IS NOT NULL AND
       (SELECT count(*) FROM public.owned_seed_inventory_items WHERE inventory_id = NEW.id) <> 7 THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_cardinality_invalid', ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;
""")
    op.execute("""
CREATE TRIGGER owned_inventory_immutable BEFORE UPDATE OR DELETE ON public.owned_seed_inventories
FOR EACH ROW EXECUTE FUNCTION public.owned_inventory_immutable();
""")
    op.execute("""
CREATE FUNCTION public.owned_item_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_history_immutable', ERRCODE = 'P0001';
    END IF;
    IF (to_jsonb(NEW) - ARRAY['state','production_task_id','consumed_at','completed_at','hold_reason'])
       IS DISTINCT FROM
       (to_jsonb(OLD) - ARRAY['state','production_task_id','consumed_at','completed_at','hold_reason'])
       OR (OLD.state <> 'unused' AND NEW.state = 'unused')
       OR (OLD.state IN ('held','completed') AND NEW.state = 'reserved')
       OR (OLD.state = 'completed' AND NEW.state <> 'completed')
       OR (OLD.production_task_id IS NOT NULL AND NEW.production_task_id IS DISTINCT FROM OLD.production_task_id)
       OR (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at) THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_item_immutable', ERRCODE = 'P0001';
    END IF;
    IF NEW.production_task_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.production_tasks task
        JOIN public.owned_seed_inventories inventory ON inventory.id = NEW.inventory_id
        WHERE task.id = NEW.production_task_id AND task.manual_seed_id = NEW.manual_seed_id
          AND task.channel_profile_id = inventory.channel_profile_id
          AND task.target_account_id = inventory.target_account_id
    ) THEN
        RAISE EXCEPTION USING MESSAGE = 'owned_inventory_task_mismatch', ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$;
""")
    op.execute("""
CREATE TRIGGER owned_item_immutable BEFORE UPDATE OR DELETE ON public.owned_seed_inventory_items
FOR EACH ROW EXECUTE FUNCTION public.owned_item_immutable();
""")


def downgrade():
    op.execute("""
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.owned_seed_inventories) THEN
        RAISE EXCEPTION 'owned_inventory_history_requires_preservation';
    END IF;
END $$;
""")
    op.drop_constraint("fk_channel_owned_inventory", "channel_profiles", type_="foreignkey")
    op.drop_column("channel_profiles", "owned_seed_inventory_id")
    op.drop_table("owned_seed_inventory_items")
    op.drop_table("owned_seed_inventories")
    op.execute("DROP FUNCTION public.owned_item_immutable()")
    op.execute("DROP FUNCTION public.owned_inventory_immutable()")
