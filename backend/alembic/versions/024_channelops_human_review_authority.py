"""add ChannelOps human review evidence and repair queue authority

Revision ID: 024_channelops_human_review_authority
Revises: 023_youtube_upload_operations
Create Date: 2026-07-19 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "024_channelops_human_review_authority"
down_revision: Union[str, None] = "023_youtube_upload_operations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def _authority_cte() -> str:
    return f"""
        WITH queue_references AS (
            SELECT
                q.id,
                q.kind,
                q.channel_profile_id AS stored_channel_id,
                CASE
                    WHEN q.payload_json ->> 'production_task_id' ~ '{_UUID_PATTERN}'
                    THEN (q.payload_json ->> 'production_task_id')::uuid
                END AS task_id,
                CASE
                    WHEN q.payload_json ->> 'publication_id' ~ '{_UUID_PATTERN}'
                    THEN (q.payload_json ->> 'publication_id')::uuid
                END AS publication_id,
                CASE
                    WHEN q.payload_json ->> 'account_id' ~ '{_UUID_PATTERN}'
                    THEN (q.payload_json ->> 'account_id')::uuid
                END AS account_id,
                CASE
                    WHEN q.payload_json ->> 'channel_id' ~ '{_UUID_PATTERN}'
                    THEN (q.payload_json ->> 'channel_id')::uuid
                END AS payload_channel_id,
                q.payload_json ->> 'channel_id' AS payload_channel_value,
                q.status
            FROM channel_ops_queue_items AS q
        ), authoritative_queue_channels AS (
            SELECT
                refs.id,
                refs.kind,
                refs.stored_channel_id,
                refs.status,
                CASE
                    WHEN refs.kind IN ('plan_task', 'execute_task', 'observe_job', 'publish_task')
                        THEN task.channel_profile_id
                    WHEN refs.kind IN ('promote_publication', 'reconcile_publication', 'collect_metrics')
                        THEN publication_task.channel_profile_id
                    WHEN refs.kind = 'account_health' THEN account.channel_profile_id
                    WHEN refs.kind IN ('agent_tick', 'learning_recompute') THEN payload_channel.id
                    WHEN refs.kind = 'send_alert' THEN
                        CASE
                            WHEN NULLIF(BTRIM(refs.payload_channel_value), '') IS NULL
                                THEN stored_channel.id
                            ELSE payload_channel.id
                        END
                END AS authoritative_channel_id,
                refs.kind = 'cleanup_expired'
                    OR (
                        refs.kind = 'send_alert'
                        AND NULLIF(BTRIM(refs.payload_channel_value), '') IS NULL
                        AND refs.stored_channel_id IS NULL
                    )
                    AS is_global
            FROM queue_references AS refs
            LEFT JOIN production_tasks AS task ON task.id = refs.task_id
            LEFT JOIN publication_records AS publication ON publication.id = refs.publication_id
            LEFT JOIN production_tasks AS publication_task
                ON publication_task.id = publication.production_task_id
            LEFT JOIN publishing_accounts AS account ON account.id = refs.account_id
            LEFT JOIN channel_profiles AS payload_channel ON payload_channel.id = refs.payload_channel_id
            LEFT JOIN channel_profiles AS stored_channel ON stored_channel.id = refs.stored_channel_id
        )
    """


def upgrade() -> None:
    op.add_column(
        "production_tasks",
        sa.Column(
            "human_review_evidence_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
    )

    op.execute(
        sa.text(
            _authority_cte()
            + """
            UPDATE channel_ops_queue_items AS q
            SET channel_profile_id = authority.authoritative_channel_id,
                updated_at = NOW()
            FROM authoritative_queue_channels AS authority
            WHERE q.id = authority.id
              AND q.channel_profile_id IS NULL
              AND authority.authoritative_channel_id IS NOT NULL
              AND NOT authority.is_global
            """
        )
    )
    op.execute(
        sa.text(
            _authority_cte()
            + """
            UPDATE channel_ops_queue_items AS q
            SET status = 'dead_lettered',
                last_error = 'queue_authority_unresolved',
                dead_letter_at = COALESCE(q.dead_letter_at, NOW()),
                locked_by = NULL,
                locked_at = NULL,
                updated_at = NOW()
            FROM authoritative_queue_channels AS authority
            WHERE q.id = authority.id
              AND authority.status IN ('queued', 'running')
              AND NOT authority.is_global
              AND (
                    authority.authoritative_channel_id IS NULL
                    OR q.channel_profile_id IS DISTINCT FROM authority.authoritative_channel_id
              )
            """
        )
    )


def downgrade() -> None:
    op.drop_column("production_tasks", "human_review_evidence_json")
