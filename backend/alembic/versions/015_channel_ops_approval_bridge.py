"""add channel ops approval bridge

Revision ID: 015_channel_ops_approval_bridge
Revises: 014_channel_ops_live_loop
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "015_channel_ops_approval_bridge"
down_revision: Union[str, None] = "014_channel_ops_live_loop"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "production_tasks",
        sa.Column("approval_mode", sa.String(length=16), nullable=False, server_default="agent"),
    )
    op.add_column(
        "production_tasks",
        sa.Column(
            "agent_approval_evidence_json",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.add_column("autoflow_plans", sa.Column("agent_approved_by", sa.String(length=255), nullable=True))
    op.alter_column("production_tasks", "approval_mode", server_default=None)
    op.alter_column("production_tasks", "agent_approval_evidence_json", server_default=None)


def downgrade() -> None:
    op.drop_column("autoflow_plans", "agent_approved_by")
    op.drop_column("production_tasks", "agent_approval_evidence_json")
    op.drop_column("production_tasks", "approval_mode")
