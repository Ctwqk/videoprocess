"""add durable YouTube upload operations

Revision ID: 023_youtube_upload_operations
Revises: 022_channelops_feedback_learning
Create Date: 2026-07-12 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "023_youtube_upload_operations"
down_revision: Union[str, None] = "022_channelops_feedback_learning"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _manager_task_id_check_sql() -> str:
    non_hex_characters = "replace(manager_task_id, '-', '')"
    for character in "0123456789abcdef":
        non_hex_characters = f"replace({non_hex_characters}, '{character}', '')"
    canonical_shape = (
        "length(manager_task_id) = 36 "
        "AND manager_task_id = lower(manager_task_id) "
        "AND substr(manager_task_id, 9, 1) = '-' "
        "AND substr(manager_task_id, 14, 1) = '-' "
        "AND substr(manager_task_id, 19, 1) = '-' "
        "AND substr(manager_task_id, 24, 1) = '-' "
        "AND length(replace(manager_task_id, '-', '')) = 32 "
        f"AND {non_hex_characters} = ''"
    )
    return (
        f"(manager_task_id IS NULL OR ({canonical_shape})) "
        "AND (status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL)"
    )


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                duplicate_record RECORD;
            BEGIN
                SELECT production_task_id, count(*) AS duplicate_count
                INTO duplicate_record
                FROM publication_records
                GROUP BY production_task_id
                HAVING count(*) > 1
                ORDER BY production_task_id
                LIMIT 1;

                IF FOUND THEN
                    RAISE EXCEPTION 'cannot add ux_publication_records_production_task: '
                        'production_task_id % has % records',
                        duplicate_record.production_task_id,
                        duplicate_record.duplicate_count;
                END IF;
            END
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                duplicate_record RECORD;
            BEGIN
                SELECT platform, platform_content_id, count(*) AS duplicate_count
                INTO duplicate_record
                FROM publication_records
                GROUP BY platform, platform_content_id
                HAVING count(*) > 1
                ORDER BY platform, platform_content_id
                LIMIT 1;

                IF FOUND THEN
                    RAISE EXCEPTION 'cannot add ux_publication_records_platform_content: '
                        'platform % content % has % records',
                        duplicate_record.platform,
                        duplicate_record.platform_content_id,
                        duplicate_record.duplicate_count;
                END IF;
            END
            $$
            """
        )
    )

    op.execute(
        """
        CREATE UNIQUE INDEX ux_publication_records_production_task
        ON publication_records (production_task_id)
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX ux_publication_records_platform_content
        ON publication_records (platform, platform_content_id)
        """
    )

    op.create_table(
        "youtube_upload_operations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("production_task_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("node_execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("input_artifact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("privacy", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("manager_task_id", sa.String(length=255), nullable=True),
        sa.Column("platform_video_id", sa.String(length=255), nullable=True),
        sa.Column(
            "receipt_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("request_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["production_task_id"], ["production_tasks.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["node_execution_id"], ["node_executions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["input_artifact_id"], ["artifacts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            _manager_task_id_check_sql(),
            name="ck_youtube_upload_operations_manager_task",
        ),
        sa.UniqueConstraint("node_execution_id", name="uq_youtube_upload_operations_node_execution"),
    )
    op.execute(
        """
        CREATE UNIQUE INDEX ux_youtube_upload_operations_production_task
        ON youtube_upload_operations (production_task_id)
        WHERE production_task_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX ux_youtube_upload_operations_platform_video
        ON youtube_upload_operations (platform_video_id)
        WHERE platform_video_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ux_youtube_upload_operations_platform_video")
    op.execute("DROP INDEX IF EXISTS ux_youtube_upload_operations_production_task")
    op.drop_table("youtube_upload_operations")
    op.execute("DROP INDEX IF EXISTS ux_publication_records_platform_content")
    op.execute("DROP INDEX IF EXISTS ux_publication_records_production_task")
