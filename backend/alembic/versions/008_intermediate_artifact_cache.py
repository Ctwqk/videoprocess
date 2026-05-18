"""add intermediate artifact cache

Revision ID: 008
Revises: 007
Create Date: 2026-05-17 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "intermediate_artifact_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("cache_key", sa.String(length=128), nullable=False),
        sa.Column("node_type", sa.String(length=100), nullable=False),
        sa.Column("node_config_hash", sa.String(length=128), nullable=False),
        sa.Column("input_signature_hash", sa.String(length=128), nullable=False),
        sa.Column("output_artifact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("hit_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("metadata_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["output_artifact_id"], ["artifacts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_intermediate_artifact_cache_cache_key",
        "intermediate_artifact_cache",
        ["cache_key"],
        unique=True,
    )
    op.create_index(
        "ix_intermediate_artifact_cache_node_type",
        "intermediate_artifact_cache",
        ["node_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_intermediate_artifact_cache_node_type", table_name="intermediate_artifact_cache")
    op.drop_index("ix_intermediate_artifact_cache_cache_key", table_name="intermediate_artifact_cache")
    op.drop_table("intermediate_artifact_cache")
