"""add autoflow used clips table

Revision ID: 007
Revises: 006
Create Date: 2026-05-17 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autoflow_used_clips",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_id", sa.String(length=255), nullable=False),
        sa.Column("source_platform", sa.String(length=64), nullable=True),
        sa.Column("candidate_title", sa.Text(), nullable=True),
        sa.Column("selected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("metadata_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_autoflow_used_clips_asset_selected_at",
        "autoflow_used_clips",
        ["asset_id", "selected_at"],
    )
    op.create_index("ix_autoflow_used_clips_run_id", "autoflow_used_clips", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_autoflow_used_clips_run_id", table_name="autoflow_used_clips")
    op.drop_index("ix_autoflow_used_clips_asset_selected_at", table_name="autoflow_used_clips")
    op.drop_table("autoflow_used_clips")
