"""harden channel ops queue schema

Revision ID: 012
Revises: 011
Create Date: 2026-05-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "lane_format_matrix",
        sa.Column(
            "source_platforms_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
    )
    op.alter_column("lane_format_matrix", "source_platforms_json", server_default=None)

    op.add_column(
        "channel_ops_queue_items",
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_channel_ops_queue_items_channel_profile_id_channel_profiles",
        "channel_ops_queue_items",
        "channel_profiles",
        ["channel_profile_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_channel_ops_queue_channel_profile_id",
        "channel_ops_queue_items",
        ["channel_profile_id"],
    )
    op.create_index(
        "ix_channel_ops_queue_channel_ready",
        "channel_ops_queue_items",
        ["channel_profile_id", "status", "run_after", "priority"],
    )


def downgrade() -> None:
    op.drop_index("ix_channel_ops_queue_channel_ready", table_name="channel_ops_queue_items")
    op.drop_index("ix_channel_ops_queue_channel_profile_id", table_name="channel_ops_queue_items")
    op.drop_constraint(
        "fk_channel_ops_queue_items_channel_profile_id_channel_profiles",
        "channel_ops_queue_items",
        type_="foreignkey",
    )
    op.drop_column("channel_ops_queue_items", "channel_profile_id")
    op.drop_column("lane_format_matrix", "source_platforms_json")
