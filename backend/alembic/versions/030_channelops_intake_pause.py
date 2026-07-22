"""add channel intake pause state

Revision ID: 030_channelops_intake_pause
Revises: 029_channelops_discovery_ingestion_runs
Create Date: 2026-07-22 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "030_channelops_intake_pause"
down_revision: Union[str, None] = "029_channelops_discovery_ingestion_runs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "channel_profiles",
        sa.Column("intake_paused_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "channel_profiles",
        sa.Column("intake_pause_reason", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("channel_profiles", "intake_pause_reason")
    op.drop_column("channel_profiles", "intake_paused_at")
