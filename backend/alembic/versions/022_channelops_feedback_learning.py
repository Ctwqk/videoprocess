"""add channelops feedback learning state

Revision ID: 022_channelops_feedback_learning
Revises: 021_channelops_discovery_signals
Create Date: 2026-05-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "022_channelops_feedback_learning"
down_revision: Union[str, None] = "021_channelops_discovery_signals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "feedback_snapshots",
        sa.Column("snapshot_stage", sa.String(length=16), nullable=False, server_default="24h"),
    )
    op.add_column(
        "feedback_snapshots",
        sa.Column("reward_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "feedback_snapshots",
        sa.Column(
            "reward_components_json",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    row_number() OVER (
                        PARTITION BY publication_id
                        ORDER BY collected_at DESC, id DESC
                    ) AS stage_rank
                FROM feedback_snapshots
            )
            UPDATE feedback_snapshots
            SET snapshot_stage = ('legacy_' || ranked.stage_rank::text)::varchar(16)
            FROM ranked
            WHERE feedback_snapshots.id = ranked.id
              AND ranked.stage_rank > 1
            """
        )
    )
    op.create_index(
        "ux_feedback_snapshots_publication_stage",
        "feedback_snapshots",
        ["publication_id", "snapshot_stage"],
        unique=True,
    )
    op.alter_column("feedback_snapshots", "snapshot_stage", server_default=None)
    op.alter_column("feedback_snapshots", "reward_components_json", server_default=None)

    op.create_table(
        "learning_states",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dimension_type", sa.String(length=64), nullable=False),
        sa.Column("dimension_key", sa.String(length=255), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("avg_reward", sa.Float(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("recommendation_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("last_computed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "channel_profile_id",
            "dimension_type",
            "dimension_key",
            "window_days",
            name="uq_learning_state_channel_dimension_window",
        ),
    )
    op.create_index(
        "ix_learning_states_channel_dimension",
        "learning_states",
        ["channel_profile_id", "dimension_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_learning_states_channel_dimension", table_name="learning_states")
    op.drop_table("learning_states")
    op.drop_index("ux_feedback_snapshots_publication_stage", table_name="feedback_snapshots")
    op.drop_column("feedback_snapshots", "reward_components_json")
    op.drop_column("feedback_snapshots", "reward_score")
    op.drop_column("feedback_snapshots", "snapshot_stage")
