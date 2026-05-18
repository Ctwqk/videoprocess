"""add channel agent production tables

Revision ID: 011
Revises: 010
Create Date: 2026-05-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "manual_seeds",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("title_seed", sa.Text(), nullable=False),
        sa.Column("source_policy", sa.String(length=64), nullable=False),
        sa.Column("source_platforms_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("material_library_ids_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("constraints_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "production_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_group_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lane_format_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_account_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("manual_seed_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("title_seed", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("rationale_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("score_breakdown_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("portfolio_bucket", sa.String(length=32), nullable=False),
        sa.Column("source_platforms_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("material_library_ids_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("uses_external_assets", sa.Boolean(), nullable=False),
        sa.Column("autoflow_plan_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("autoflow_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("pipeline_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("priority", sa.Float(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("state_updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("blocked_by_guard", sa.String(length=255), nullable=True),
        sa.Column("channel_config_version_snapshot", sa.Integer(), nullable=False),
        sa.Column("channel_config_snapshot_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("transition_history_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_production_tasks_channel_state", "production_tasks", ["channel_profile_id", "state"])
    op.create_table(
        "material_usage_ledger",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("material_id", sa.String(length=255), nullable=False),
        sa.Column("asset_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("publishing_account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("used_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("segment_signature", sa.String(length=255), nullable=False),
        sa.Column("metadata_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "publication_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("production_task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform", sa.String(length=64), nullable=False),
        sa.Column("account_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform_content_id", sa.String(length=255), nullable=False),
        sa.Column("permalink", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("tags_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("thumbnail_storage_path", sa.Text(), nullable=True),
        sa.Column("desired_privacy", sa.String(length=32), nullable=False),
        sa.Column("current_privacy", sa.String(length=32), nullable=False),
        sa.Column("publish_status", sa.String(length=32), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scheduled_publish_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("public_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("compliance_disposition", sa.String(length=64), nullable=False),
        sa.Column("quota_units_estimated", sa.Integer(), nullable=False),
        sa.Column("last_metrics_polled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("warnings_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_publication_records_task", "publication_records", ["production_task_id"])
    op.create_table(
        "takedown_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("raw_payload_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("auto_actions_taken_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "feedback_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("views", sa.Integer(), nullable=False),
        sa.Column("likes", sa.Integer(), nullable=False),
        sa.Column("comments", sa.Integer(), nullable=False),
        sa.Column("shares", sa.Integer(), nullable=False),
        sa.Column("avg_view_duration_sec", sa.Float(), nullable=False),
        sa.Column("retention_curve_json", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("ctr", sa.Float(), nullable=True),
        sa.Column("impressions", sa.Integer(), nullable=True),
        sa.Column("virality_score", sa.Float(), nullable=False),
        sa.Column("raw_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("feedback_snapshots")
    op.drop_table("takedown_events")
    op.drop_index("ix_publication_records_task", table_name="publication_records")
    op.drop_table("publication_records")
    op.drop_table("material_usage_ledger")
    op.drop_index("ix_production_tasks_channel_state", table_name="production_tasks")
    op.drop_table("production_tasks")
    op.drop_table("manual_seeds")

