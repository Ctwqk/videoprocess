"""add AutoFlow exact revision authority and execute idempotency

Revision ID: 025_autoflow_revision_idempotency
Revises: 024_channelops_human_review_authority
Create Date: 2026-07-19 00:00:01.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "025_autoflow_revision_idempotency"
down_revision: Union[str, None] = "024_channelops_human_review_authority"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autoflow_plans",
        sa.Column("approved_revision_hash", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "autoflow_runs",
        sa.Column("execute_idempotency_key", sa.String(length=512), nullable=True),
    )
    op.create_unique_constraint(
        "uq_autoflow_runs_execute_idempotency_key",
        "autoflow_runs",
        ["execute_idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_autoflow_runs_execute_idempotency_key",
        "autoflow_runs",
        type_="unique",
    )
    op.drop_column("autoflow_runs", "execute_idempotency_key")
    op.drop_column("autoflow_plans", "approved_revision_hash")
