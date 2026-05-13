"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enum types
    job_status = postgresql.ENUM(
        "PENDING", "VALIDATING", "PLANNING", "RUNNING",
        "SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED",
        name="job_status",
        create_type=True,
    )
    node_status = postgresql.ENUM(
        "PENDING", "QUEUED", "RUNNING", "SUCCEEDED",
        "FAILED", "SKIPPED", "CANCELLED",
        name="node_status",
        create_type=True,
    )
    artifact_kind = postgresql.ENUM(
        "INTERMEDIATE", "FINAL",
        name="artifact_kind",
        create_type=True,
    )

    # pipelines
    op.create_table(
        "pipelines",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column("definition", postgresql.JSON(), nullable=False),
        sa.Column("is_template", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("template_tags", postgresql.ARRAY(sa.String()), server_default="{}", nullable=False),
        sa.Column("thumbnail_url", sa.String(512), nullable=True),
        sa.Column("created_by", sa.String(255), server_default="system", nullable=False),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_pipelines_is_template", "pipelines", ["is_template"], postgresql_where=sa.text("is_template = true"))
    op.create_index("idx_pipelines_updated_at", "pipelines", [sa.text("updated_at DESC")])

    # jobs
    op.create_table(
        "jobs",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("pipeline_id", sa.UUID(), sa.ForeignKey("pipelines.id"), nullable=False),
        sa.Column("pipeline_snapshot", postgresql.JSON(), nullable=False),
        sa.Column("status", job_status, server_default="PENDING", nullable=False),
        sa.Column("execution_plan", postgresql.JSON(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("submitted_by", sa.String(255), server_default="system", nullable=False),
        sa.Column("parent_job_id", sa.UUID(), sa.ForeignKey("jobs.id"), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_jobs_status", "jobs", ["status"])
    op.create_index("idx_jobs_pipeline_id", "jobs", ["pipeline_id"])
    op.create_index("idx_jobs_submitted_at", "jobs", [sa.text("submitted_at DESC")])

    # node_executions
    op.create_table(
        "node_executions",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", sa.UUID(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("node_id", sa.String(255), nullable=False),
        sa.Column("node_type", sa.String(100), nullable=False),
        sa.Column("node_label", sa.String(255), server_default="", nullable=False),
        sa.Column("node_config", postgresql.JSON(), server_default="{}", nullable=False),
        sa.Column("status", node_status, server_default="PENDING", nullable=False),
        sa.Column("progress", sa.SmallInteger(), server_default="0", nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_trace", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("input_artifact_ids", postgresql.ARRAY(sa.UUID()), server_default="{}", nullable=False),
        sa.Column("output_artifact_id", sa.UUID(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", "node_id", name="uq_job_node"),
        sa.CheckConstraint("progress >= 0 AND progress <= 100", name="ck_progress_range"),
    )
    op.create_index("idx_node_exec_job_id", "node_executions", ["job_id"])
    op.create_index("idx_node_exec_status", "node_executions", ["status"])

    # assets
    op.create_table(
        "assets",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("filename", sa.String(512), nullable=False),
        sa.Column("original_name", sa.String(512), nullable=False),
        sa.Column("mime_type", sa.String(100), nullable=True),
        sa.Column("file_size", sa.BigInteger(), nullable=True),
        sa.Column("storage_backend", sa.String(50), server_default="local", nullable=False),
        sa.Column("storage_path", sa.String(1024), nullable=False),
        sa.Column("media_info", postgresql.JSON(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("uploaded_by", sa.String(255), server_default="system", nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_assets_uploaded_at", "assets", [sa.text("uploaded_at DESC")])

    # artifacts
    op.create_table(
        "artifacts",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", sa.UUID(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("node_execution_id", sa.UUID(), sa.ForeignKey("node_executions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("kind", artifact_kind, server_default="INTERMEDIATE", nullable=False),
        sa.Column("filename", sa.String(512), nullable=False),
        sa.Column("mime_type", sa.String(100), nullable=True),
        sa.Column("file_size", sa.BigInteger(), nullable=True),
        sa.Column("storage_backend", sa.String(50), server_default="local", nullable=False),
        sa.Column("storage_path", sa.String(1024), nullable=False),
        sa.Column("media_info", postgresql.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_artifacts_job_id", "artifacts", ["job_id"])
    op.create_index("idx_artifacts_node_exec", "artifacts", ["node_execution_id"])


def downgrade() -> None:
    op.drop_table("artifacts")
    op.drop_table("node_executions")
    op.drop_table("jobs")
    op.drop_table("assets")
    op.drop_table("pipelines")
    op.execute("DROP TYPE IF EXISTS artifact_kind")
    op.execute("DROP TYPE IF EXISTS node_status")
    op.execute("DROP TYPE IF EXISTS job_status")
