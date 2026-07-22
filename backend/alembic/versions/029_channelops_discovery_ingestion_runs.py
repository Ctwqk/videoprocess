"""add durable discovery ingestion runs

Revision ID: 029_channelops_discovery_ingestion_runs
Revises: 028_channelops_metric_schedules
Create Date: 2026-07-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "029_channelops_discovery_ingestion_runs"
down_revision: Union[str, None] = "028_channelops_metric_schedules"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "discovery_ingestion_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("queue_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "source",
            sa.String(length=64),
            server_default="youtube_search",
            nullable=False,
        ),
        sa.Column("scheduler_bucket", sa.String(length=64), nullable=False),
        sa.Column(
            "query_version",
            sa.String(length=64),
            server_default="youtube-lane-keyword-v1",
            nullable=False,
        ),
        sa.Column("status", sa.String(length=16), server_default="running", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("query_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("created_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("refreshed_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("expired_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "quota_units_estimated",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "policy_snapshot_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.CheckConstraint("source = 'youtube_search'", name="ck_discovery_ingestion_run_source"),
        sa.CheckConstraint(
            "query_version = 'youtube-lane-keyword-v1'",
            name="ck_discovery_ingestion_run_query_version",
        ),
        sa.CheckConstraint(
            "status IN ('running','succeeded','failed')",
            name="ck_discovery_ingestion_run_status",
        ),
        sa.CheckConstraint("attempt_count >= 1", name="ck_discovery_ingestion_run_attempt_count"),
        sa.CheckConstraint("query_count >= 0", name="ck_discovery_ingestion_run_query_count"),
        sa.CheckConstraint("created_count >= 0", name="ck_discovery_ingestion_run_created_count"),
        sa.CheckConstraint("refreshed_count >= 0", name="ck_discovery_ingestion_run_refreshed_count"),
        sa.CheckConstraint("expired_count >= 0", name="ck_discovery_ingestion_run_expired_count"),
        sa.CheckConstraint(
            "quota_units_estimated >= 0",
            name="ck_discovery_ingestion_run_quota_units_estimated",
        ),
        sa.ForeignKeyConstraint(
            ["channel_profile_id"],
            ["channel_profiles.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["queue_item_id"],
            ["channel_ops_queue_items.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "channel_profile_id",
            "source",
            "scheduler_bucket",
            name="uq_discovery_ingestion_run_channel_source_bucket",
        ),
        sa.UniqueConstraint("queue_item_id", name="uq_discovery_ingestion_run_queue_item"),
    )


def downgrade() -> None:
    op.drop_table("discovery_ingestion_runs")
