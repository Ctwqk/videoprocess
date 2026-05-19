"""add event outbox

Revision ID: 013_event_outbox
Revises: 012
Create Date: 2026-05-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "013_event_outbox"
# The file is named 012_channel_ops_hardening.py, but its Alembic revision id is "012".
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "event_outbox",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_token", sa.String(length=64), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index("ix_event_outbox_undelivered", "event_outbox", ["delivered_at", "created_at"])
    op.create_index("ix_event_outbox_claim_token", "event_outbox", ["claim_token"])


def downgrade() -> None:
    op.drop_index("ix_event_outbox_claim_token", table_name="event_outbox")
    op.drop_index("ix_event_outbox_undelivered", table_name="event_outbox")
    op.drop_table("event_outbox")
