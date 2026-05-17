"""autoflow schema

Revision ID: 004
Revises: 003
Create Date: 2026-05-16
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autoflow_plans",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("request_json", postgresql.JSON(), nullable=False),
        sa.Column("intent_json", postgresql.JSON(), nullable=False),
        sa.Column("template_id", sa.String(length=255), nullable=False),
        sa.Column("pipeline_definition", postgresql.JSON(), nullable=False),
        sa.Column("candidates_json", postgresql.JSON(), server_default="[]", nullable=False),
        sa.Column("metadata_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("rights_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("validation_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("status", sa.String(length=32), server_default="drafted", nullable=False),
        sa.Column("review_approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("public_approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("review_notes", sa.Text(), nullable=True),
        sa.Column("rejected_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_autoflow_plans_status", "autoflow_plans", ["status"])
    op.create_index("idx_autoflow_plans_created_at", "autoflow_plans", [sa.text("created_at DESC")])

    op.create_table(
        "autoflow_runs",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("plan_id", sa.UUID(), sa.ForeignKey("autoflow_plans.id", ondelete="CASCADE"), nullable=False),
        sa.Column("pipeline_id", sa.UUID(), sa.ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True),
        sa.Column("job_id", sa.UUID(), sa.ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("artifacts_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("publish_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_autoflow_runs_plan_id", "autoflow_runs", ["plan_id"])
    op.create_index("idx_autoflow_runs_status", "autoflow_runs", ["status"])

    op.create_table(
        "content_metrics",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("run_id", sa.UUID(), sa.ForeignKey("autoflow_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("platform", sa.String(length=64), nullable=False),
        sa.Column("platform_content_id", sa.String(length=255), server_default="", nullable=False),
        sa.Column("views", sa.Integer(), server_default="0", nullable=False),
        sa.Column("likes", sa.Integer(), server_default="0", nullable=False),
        sa.Column("comments", sa.Integer(), server_default="0", nullable=False),
        sa.Column("shares", sa.Integer(), server_default="0", nullable=False),
        sa.Column("watch_time_sec", sa.Float(), server_default="0", nullable=False),
        sa.Column("avg_view_duration_sec", sa.Float(), server_default="0", nullable=False),
        sa.Column("retention_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_content_metrics_run_id", "content_metrics", ["run_id"])
    op.create_index("idx_content_metrics_platform", "content_metrics", ["platform"])

    op.create_table(
        "trend_signals",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("keyword", sa.String(length=255), nullable=False),
        sa.Column("score", sa.Float(), server_default="0", nullable=False),
        sa.Column("metadata_json", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_trend_signals_keyword", "trend_signals", ["keyword"])
    op.create_index("idx_trend_signals_observed_at", "trend_signals", [sa.text("observed_at DESC")])


def downgrade() -> None:
    op.drop_table("trend_signals")
    op.drop_table("content_metrics")
    op.drop_table("autoflow_runs")
    op.drop_table("autoflow_plans")
