"""persist channelops leader epoch

Revision ID: 032_channelops_leader_epoch
Revises: 031_guarded_schedule_job_authority
Create Date: 2026-07-23 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "032_channelops_leader_epoch"
down_revision: Union[str, None] = "031_guarded_schedule_job_authority"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "channelops_leader_epochs",
        sa.Column("service_name", sa.String(length=64), primary_key=True),
        sa.Column("epoch", sa.BigInteger(), nullable=False),
        sa.Column("holder_id", sa.String(length=255), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("epoch > 0", name="ck_channelops_leader_epoch_positive"),
        sa.CheckConstraint(
            "length(btrim(holder_id)) > 0",
            name="ck_channelops_leader_holder_nonempty",
        ),
        sa.CheckConstraint(
            "heartbeat_at >= acquired_at",
            name="ck_channelops_leader_heartbeat_order",
        ),
        sa.CheckConstraint(
            "released_at IS NULL OR released_at >= acquired_at",
            name="ck_channelops_leader_release_order",
        ),
    )


def downgrade() -> None:
    op.drop_table("channelops_leader_epochs")
