"""add channelops decision audit and failure category

Revision ID: 020_channelops_decision_audit_failure_category
Revises: 019_channelops_go_live_phase0
Create Date: 2026-05-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "020_channelops_decision_audit_failure_category"
down_revision: Union[str, None] = "019_channelops_go_live_phase0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "production_tasks",
        sa.Column("failure_category", sa.String(length=64), nullable=True),
    )
    op.create_table(
        "decision_audit_entries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tick_audit_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_id", sa.String(length=255), nullable=False),
        sa.Column("candidate_source", sa.String(length=64), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lane_format_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("score_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("guard_results_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("pds_decision_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("learning_context_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("selected", sa.Boolean(), nullable=False),
        sa.Column("rejection_reason", sa.String(length=255), nullable=True),
        sa.Column("created_task_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["tick_audit_id"], ["agent_tick_audits.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_decision_audit_entries_tick", "decision_audit_entries", ["tick_audit_id"])
    op.create_index(
        "ix_decision_audit_entries_channel_created",
        "decision_audit_entries",
        ["channel_profile_id", "created_at"],
    )
    op.create_index("ix_decision_audit_entries_task", "decision_audit_entries", ["created_task_id"])
    op.create_index(
        "ix_decision_audit_entries_source_created",
        "decision_audit_entries",
        ["candidate_source", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_decision_audit_entries_source_created", table_name="decision_audit_entries")
    op.drop_index("ix_decision_audit_entries_task", table_name="decision_audit_entries")
    op.drop_index("ix_decision_audit_entries_channel_created", table_name="decision_audit_entries")
    op.drop_index("ix_decision_audit_entries_tick", table_name="decision_audit_entries")
    op.drop_table("decision_audit_entries")
    op.drop_column("production_tasks", "failure_category")
