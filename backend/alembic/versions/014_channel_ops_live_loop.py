"""add channel ops live loop constraints

Revision ID: 014_channel_ops_live_loop
Revises: 013_event_outbox
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "014_channel_ops_live_loop"
down_revision: Union[str, None] = "013_event_outbox"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_agent_tick_audit_channel_tick",
        "agent_tick_audits",
        ["channel_profile_id", "tick_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_agent_tick_audit_channel_tick",
        "agent_tick_audits",
        type_="unique",
    )
