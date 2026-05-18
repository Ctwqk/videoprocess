"""add channel agent queue tables

Revision ID: 010
Revises: 009
Create Date: 2026-05-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "channel_ops_queue_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=512), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("parent_queue_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("payload_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("run_after", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(length=255), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("dead_letter_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_channel_ops_queue_idempotency_key"),
    )
    op.create_index(
        "ix_channel_ops_queue_ready",
        "channel_ops_queue_items",
        ["status", "run_after", "priority"],
    )
    op.create_table(
        "agent_tick_audits",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("queue_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("tick_id", sa.String(length=255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dry_run", sa.Boolean(), nullable=False),
        sa.Column("ideas_discovered", sa.Integer(), nullable=False),
        sa.Column("candidates_scored", sa.Integer(), nullable=False),
        sa.Column("tasks_selected", sa.Integer(), nullable=False),
        sa.Column("tasks_rejected", sa.Integer(), nullable=False),
        sa.Column("guards_triggered_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("decision_summary_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_agent_tick_audits_channel_profile_id", "agent_tick_audits", ["channel_profile_id"])


def downgrade() -> None:
    op.drop_index("ix_agent_tick_audits_channel_profile_id", table_name="agent_tick_audits")
    op.drop_table("agent_tick_audits")
    op.drop_index("ix_channel_ops_queue_ready", table_name="channel_ops_queue_items")
    op.drop_table("channel_ops_queue_items")

