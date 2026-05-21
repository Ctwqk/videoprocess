"""add channelops discovery signals

Revision ID: 021_channelops_discovery_signals
Revises: 020_channelops_decision_audit_failure_category
Create Date: 2026-05-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "021_channelops_discovery_signals"
down_revision: Union[str, None] = "020_channelops_decision_audit_failure_category"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "discovery_signals",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic_lane_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_external_id", sa.String(length=255), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("keywords_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trend_score", sa.Float(), nullable=False),
        sa.Column("novelty_score", sa.Float(), nullable=False),
        sa.Column("raw_json", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("converted_task_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["channel_profile_id"], ["channel_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "channel_profile_id",
            "source",
            "source_external_id",
            name="uq_discovery_signal_channel_source_external",
        ),
    )
    op.create_index(
        "ix_discovery_signals_channel_lane_observed",
        "discovery_signals",
        ["channel_profile_id", "topic_lane_id", "observed_at"],
    )
    op.create_index(
        "ix_discovery_signals_channel_status_expires",
        "discovery_signals",
        ["channel_profile_id", "status", "expires_at"],
    )
    op.add_column(
        "production_tasks",
        sa.Column("discovery_signal_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(
        sa.text(
            """
            INSERT INTO discovery_signals (
                id,
                channel_profile_id,
                topic_lane_id,
                source,
                source_url,
                source_external_id,
                title,
                summary,
                keywords_json,
                observed_at,
                expires_at,
                trend_score,
                novelty_score,
                raw_json,
                status,
                converted_task_id,
                created_at,
                updated_at
            )
            SELECT
                gen_random_uuid(),
                channel_profile_id,
                topic_lane_id,
                'youtube_search',
                COALESCE(
                    NULLIF(constraints_json->>'source_url', ''),
                    NULLIF(constraints_json->>'url', '')
                ),
                LEFT(
                    COALESCE(
                        NULLIF(constraints_json->>'source_video_id', ''),
                        NULLIF(constraints_json->>'source_external_id', ''),
                        NULLIF(constraints_json->>'external_id', ''),
                        id::text
                    ),
                    255
                ),
                COALESCE(NULLIF(title_seed, ''), prompt, ''),
                prompt,
                CASE
                    WHEN json_typeof(constraints_json->'keywords') = 'array'
                    THEN constraints_json->'keywords'
                    ELSE '[]'::json
                END,
                COALESCE(updated_at, created_at, now()),
                CASE
                    WHEN NULLIF(constraints_json->>'expires_at', '') ~
                        '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}'
                    THEN (constraints_json->>'expires_at')::timestamptz
                    ELSE NULL
                END,
                CASE
                    WHEN NULLIF(constraints_json->>'trend_score', '') ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (constraints_json->>'trend_score')::float
                    ELSE 0.0
                END,
                CASE
                    WHEN NULLIF(constraints_json->>'novelty_score', '') ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN (constraints_json->>'novelty_score')::float
                    ELSE 0.0
                END,
                json_build_object(
                    'legacy_manual_seed_id', id::text,
                    'source_video_id', constraints_json->>'source_video_id',
                    'source_url', COALESCE(
                        NULLIF(constraints_json->>'source_url', ''),
                        NULLIF(constraints_json->>'url', '')
                    ),
                    'view_count', constraints_json->'view_count',
                    'raw_constraints', constraints_json,
                    'constraints', constraints_json,
                    'source_platforms', source_platforms_json,
                    'material_library_ids', material_library_ids_json
                ),
                'active',
                NULL,
                COALESCE(created_at, now()),
                COALESCE(updated_at, created_at, now())
            FROM manual_seeds
            WHERE source_policy = 'trend_youtube'
              AND status = 'active'
            ON CONFLICT ON CONSTRAINT uq_discovery_signal_channel_source_external
            DO UPDATE SET
                source_url = COALESCE(EXCLUDED.source_url, discovery_signals.source_url),
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                keywords_json = EXCLUDED.keywords_json,
                observed_at = EXCLUDED.observed_at,
                trend_score = EXCLUDED.trend_score,
                novelty_score = EXCLUDED.novelty_score,
                raw_json = EXCLUDED.raw_json,
                status = 'active',
                updated_at = now()
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE manual_seeds
            SET status = 'exhausted',
                updated_at = now()
            WHERE source_policy = 'trend_youtube'
              AND status = 'active'
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE manual_seeds
            SET status = 'active',
                updated_at = now()
            FROM discovery_signals
            WHERE manual_seeds.id::text = discovery_signals.raw_json->>'legacy_manual_seed_id'
              AND manual_seeds.source_policy = 'trend_youtube'
              AND manual_seeds.status = 'exhausted'
            """
        )
    )
    op.drop_column("production_tasks", "discovery_signal_id")
    op.drop_index("ix_discovery_signals_channel_status_expires", table_name="discovery_signals")
    op.drop_index("ix_discovery_signals_channel_lane_observed", table_name="discovery_signals")
    op.drop_table("discovery_signals")
