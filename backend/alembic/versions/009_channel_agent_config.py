"""add channel agent config tables

Revision ID: 009
Revises: 008
Create Date: 2026-05-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "channel_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operator_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("positioning", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=32), nullable=False),
        sa.Column("default_aspect_ratio", sa.String(length=16), nullable=False),
        sa.Column("risk_policy_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("content_mix_policy_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("cadence_policy_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("alert_policy_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False),
        sa.Column("halted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("halt_reason", sa.Text(), nullable=True),
        sa.Column("config_version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "topic_lanes",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.Column("learned_weight", sa.Float(), nullable=True),
        sa.Column("keywords_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("negative_keywords_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("min_posts_per_week", sa.Integer(), nullable=False),
        sa.Column("max_posts_per_day", sa.Integer(), nullable=False),
        sa.Column("max_consecutive_streak", sa.Integer(), nullable=False),
        sa.Column("cooldown_after_post_minutes", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("paused_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_topic_lanes_channel_profile_id", "topic_lanes", ["channel_profile_id"])
    op.create_table(
        "publishing_accounts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform", sa.String(length=64), nullable=False),
        sa.Column("account_label", sa.String(length=255), nullable=False),
        sa.Column("platform_account_id", sa.String(length=255), nullable=False),
        sa.Column("credential_ref", sa.String(length=512), nullable=False),
        sa.Column("platform_specific_config_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("default_privacy", sa.String(length=32), nullable=False),
        sa.Column("external_asset_auto_publish", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("paused_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_token_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_token_check_status", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_publishing_accounts_channel_profile_id", "publishing_accounts", ["channel_profile_id"])
    op.create_table(
        "lane_format_matrix",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("format_key", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.Column("target_duration_sec", sa.Integer(), nullable=False),
        sa.Column("template_pool_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("default_publish_visibility", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["topic_lane_id"], ["topic_lanes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_lane_format_matrix_topic_lane_id", "lane_format_matrix", ["topic_lane_id"])


def downgrade() -> None:
    op.drop_index("ix_lane_format_matrix_topic_lane_id", table_name="lane_format_matrix")
    op.drop_table("lane_format_matrix")
    op.drop_index("ix_publishing_accounts_channel_profile_id", table_name="publishing_accounts")
    op.drop_table("publishing_accounts")
    op.drop_index("ix_topic_lanes_channel_profile_id", table_name="topic_lanes")
    op.drop_table("topic_lanes")
    op.drop_table("channel_profiles")

