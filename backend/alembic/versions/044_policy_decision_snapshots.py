"""Add immutable policy facts and passive candidate snapshot audit links.

Revision ID: 044_policy_decision_snapshots
Revises: 043_owned_history_snapshot_rows
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "044_policy_decision_snapshots"
down_revision = "043_owned_history_snapshot_rows"
branch_labels = None
depends_on = None

IMMUTABLE_TABLES = (
    "decision_policy_versions", "policy_activation_history", "candidate_feature_snapshots",
)


def upgrade() -> None:
    op.create_table(
        "decision_policy_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("policy_key", sa.String(255), nullable=False),
        sa.Column("version", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("feature_schema_version", sa.String(255), nullable=False),
        sa.Column("reward_version", sa.String(255), nullable=False),
        sa.Column("formula_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("hard_guard_config_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("portfolio_config_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("exploration_config_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("code_commit_sha", sa.String(40), nullable=False),
        sa.Column("template_registry_version", sa.String(255), nullable=False),
        sa.Column("prompt_bundle_version", sa.String(255), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("change_reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("policy_key", "version", name="uq_decision_policy_versions_key_version"),
        sa.CheckConstraint("status IN ('draft','validated','retired')", name="ck_decision_policy_versions_status"),
    )
    op.create_table(
        "policy_activation_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("policy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("rollout_percentage", sa.Float(), nullable=False),
        sa.Column("deterministic_salt", sa.Text(), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("previous_activation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("request_id", sa.String(255), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("rollback_reason", sa.Text(), nullable=True),
        sa.Column("feature_flag_snapshot_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["target_account_id"], ["publishing_accounts.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["policy_version_id"], ["decision_policy_versions.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["previous_activation_id"], ["policy_activation_history.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("request_id", name="uq_policy_activation_history_request_id"),
        sa.CheckConstraint("mode IN ('off','shadow','canary','active')", name="ck_policy_activation_history_mode"),
        sa.CheckConstraint(
            "rollout_percentage >= 0 AND rollout_percentage <= 100", name="ck_policy_activation_history_rollout",
        ),
        sa.CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from", name="ck_policy_activation_history_interval",
        ),
    )
    op.create_table(
        "candidate_feature_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tick_audit_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_id", sa.String(255), nullable=False),
        sa.Column("candidate_source", sa.String(64), nullable=False),
        sa.Column("source_kind", sa.String(64), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lane_format_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("policy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("feature_schema_version", sa.String(255), nullable=False),
        sa.Column("feature_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_features_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("normalized_features_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("missing_feature_mask_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("cadence_snapshot_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("content_mix_snapshot_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("material_supply_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("production_reliability_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("learning_references_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("source_record_refs_json", postgresql.JSON(none_as_null=True), nullable=False),
        sa.Column("cost_estimate_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("risk_estimate_json", postgresql.JSON(none_as_null=True), nullable=True),
        sa.Column("candidate_set_hash", sa.String(64), nullable=False),
        sa.Column("feature_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tick_audit_id"], ["agent_tick_audits.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["policy_version_id"], ["decision_policy_versions.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "tick_audit_id", "candidate_id", "feature_schema_version",
            name="uq_candidate_feature_snapshots_tick_candidate_schema",
        ),
    )
    op.add_column("agent_tick_audits", sa.Column("policy_version_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("agent_tick_audits", sa.Column("candidate_set_hash", sa.String(64), nullable=True))
    op.add_column("agent_tick_audits", sa.Column("feature_as_of", sa.DateTime(timezone=True), nullable=True))
    # The constant default backfills existing ticks and keeps old writers explicitly unreplayable.
    op.add_column("agent_tick_audits", sa.Column(
        "replay_status", sa.String(32), nullable=False, server_default="legacy_unreplayable",
    ))
    op.create_foreign_key(
        "fk_agent_tick_audits_policy_version", "agent_tick_audits", "decision_policy_versions",
        ["policy_version_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_agent_tick_audits_replay_status", "agent_tick_audits",
        "replay_status IN ('legacy_unreplayable','snapshot_pending','snapshot_complete')",
    )
    for name, column_type in (
        ("policy_version_id", postgresql.UUID(as_uuid=True)),
        ("feature_snapshot_id", postgresql.UUID(as_uuid=True)),
        ("candidate_set_hash", sa.String(64)), ("decision_hash", sa.String(64)),
        ("decision", sa.String(16)), ("baseline_score", sa.Float()), ("final_score", sa.Float()),
        ("rank", sa.Integer()), ("shadow_score", sa.Float()), ("shadow_rank", sa.Integer()),
        ("shadow_selected", sa.Boolean()), ("experiment_id", postgresql.UUID(as_uuid=True)),
    ):
        op.add_column("decision_audit_entries", sa.Column(name, column_type, nullable=True))
    op.create_foreign_key(
        "fk_decision_audit_entries_policy_version", "decision_audit_entries", "decision_policy_versions",
        ["policy_version_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_decision_audit_entries_feature_snapshot", "decision_audit_entries", "candidate_feature_snapshots",
        ["feature_snapshot_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_decision_audit_entries_decision", "decision_audit_entries", "decision IN ('accepted','rejected')",
    )
    op.execute("""
        CREATE FUNCTION public.vp_immutable_policy_fact() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            RAISE EXCEPTION 'immutable_policy_fact';
        END;
        $$
    """)
    for table in IMMUTABLE_TABLES:
        op.execute(f"""
            CREATE TRIGGER trg_{table}_immutable
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION public.vp_immutable_policy_fact()
        """)
    # Legacy retention remains unchanged, but replayable decision evidence cannot be erased.
    op.execute("""
        CREATE TRIGGER trg_replayable_decision_immutable
        BEFORE UPDATE OR DELETE ON decision_audit_entries
        FOR EACH ROW WHEN (OLD.feature_snapshot_id IS NOT NULL)
        EXECUTE FUNCTION public.vp_immutable_policy_fact()
    """)
    op.execute("""
        CREATE TRIGGER trg_snapshot_complete_tick_retained
        BEFORE UPDATE OR DELETE ON agent_tick_audits
        FOR EACH ROW WHEN (OLD.replay_status = 'snapshot_complete')
        EXECUTE FUNCTION public.vp_immutable_policy_fact()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_snapshot_complete_tick_retained ON agent_tick_audits")
    op.execute("DROP TRIGGER trg_replayable_decision_immutable ON decision_audit_entries")
    for table in reversed(IMMUTABLE_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_immutable ON {table}")
    op.execute("DROP FUNCTION public.vp_immutable_policy_fact()")
    op.drop_constraint("ck_decision_audit_entries_decision", "decision_audit_entries", type_="check")
    op.drop_constraint("fk_decision_audit_entries_feature_snapshot", "decision_audit_entries", type_="foreignkey")
    op.drop_constraint("fk_decision_audit_entries_policy_version", "decision_audit_entries", type_="foreignkey")
    for name in (
        "experiment_id", "shadow_selected", "shadow_rank", "shadow_score", "rank", "final_score",
        "baseline_score", "decision", "decision_hash", "candidate_set_hash", "feature_snapshot_id", "policy_version_id",
    ):
        op.drop_column("decision_audit_entries", name)
    op.drop_constraint("ck_agent_tick_audits_replay_status", "agent_tick_audits", type_="check")
    op.drop_constraint("fk_agent_tick_audits_policy_version", "agent_tick_audits", type_="foreignkey")
    for name in ("replay_status", "feature_as_of", "candidate_set_hash", "policy_version_id"):
        op.drop_column("agent_tick_audits", name)
    for table in reversed(IMMUTABLE_TABLES):
        op.drop_table(table)
