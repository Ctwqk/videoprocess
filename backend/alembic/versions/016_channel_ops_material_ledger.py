"""add channel ops material ledger indexes

Revision ID: 016_channel_ops_material_ledger
Revises: 015_channel_ops_approval_bridge
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "016_channel_ops_material_ledger"
down_revision: Union[str, None] = "015_channel_ops_approval_bridge"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_material_usage_channel_lane_segment_used",
        "material_usage_ledger",
        ["channel_profile_id", "topic_lane_id", "segment_signature", "used_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_material_usage_channel_lane_segment_used", table_name="material_usage_ledger")
