"""add channel ops self driving scheduler state

Revision ID: 017_channel_ops_self_driving
Revises: 016_channel_ops_material_ledger
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "017_channel_ops_self_driving"
down_revision: Union[str, None] = "016_channel_ops_material_ledger"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "channel_profiles",
        sa.Column("tick_interval_minutes", sa.Integer(), nullable=False, server_default="60"),
    )
    op.alter_column("channel_profiles", "tick_interval_minutes", server_default=None)
    op.create_table(
        "internal_scheduler_runs",
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("bucket", sa.String(length=64), nullable=False),
        sa.Column("enqueued_queue_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("ran_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("metadata_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("channel_profile_id", "bucket", name="uq_internal_scheduler_channel_bucket"),
    )
    op.create_index(
        "ix_internal_scheduler_runs_channel_profile_id",
        "internal_scheduler_runs",
        ["channel_profile_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_internal_scheduler_runs_channel_profile_id", table_name="internal_scheduler_runs")
    op.drop_table("internal_scheduler_runs")
    op.drop_column("channel_profiles", "tick_interval_minutes")
