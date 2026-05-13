"""material library schema

Revision ID: 002
Revises: 001
Create Date: 2026-03-27
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "material_libraries",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column("is_disabled", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "material_items",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("library_id", sa.UUID(), sa.ForeignKey("material_libraries.id", ondelete="CASCADE"), nullable=False),
        sa.Column("asset_id", sa.UUID(), sa.ForeignKey("assets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="READY", nullable=False),
        sa.Column("duration", sa.Float(), nullable=True),
        sa.Column("subtitle_source", sa.String(length=64), server_default="asr_if_missing", nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("metadata", postgresql.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("library_id", "asset_id", name="uq_material_item_library_asset"),
    )
    op.create_index("idx_material_items_library_id", "material_items", ["library_id"])
    op.create_index("idx_material_items_asset_id", "material_items", ["asset_id"])

    op.create_table(
        "material_clips",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("library_id", sa.UUID(), sa.ForeignKey("material_libraries.id", ondelete="CASCADE"), nullable=False),
        sa.Column("parent_material_item_id", sa.UUID(), sa.ForeignKey("material_items.id", ondelete="SET NULL"), nullable=True),
        sa.Column("source_asset_id", sa.UUID(), sa.ForeignKey("assets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("clip_id", sa.String(length=255), nullable=False),
        sa.Column("start_sec", sa.Float(), nullable=False),
        sa.Column("end_sec", sa.Float(), nullable=False),
        sa.Column("subtitle_text", sa.Text(), server_default="", nullable=False),
        sa.Column("ocr_text", sa.Text(), nullable=True),
        sa.Column("caption", sa.Text(), nullable=True),
        sa.Column("neighbor_clip_ids", postgresql.JSON(), server_default="[]", nullable=False),
        sa.Column("clip_kind", sa.String(length=32), server_default="coarse_window", nullable=False),
        sa.Column("storage_asset_id", sa.UUID(), sa.ForeignKey("assets.id", ondelete="SET NULL"), nullable=True),
        sa.Column("metadata", postgresql.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_material_clips_library_id", "material_clips", ["library_id"])
    op.create_index("idx_material_clips_source_asset_id", "material_clips", ["source_asset_id"])
    op.create_index("idx_material_clips_clip_kind", "material_clips", ["clip_kind"])

    op.create_table(
        "material_queries",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("query_text", sa.Text(), nullable=False),
        sa.Column("source_library_ids", postgresql.JSON(), server_default="[]", nullable=False),
        sa.Column("result_library_ids", postgresql.JSON(), server_default="[]", nullable=False),
        sa.Column("config", postgresql.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "material_query_results",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("query_id", sa.UUID(), sa.ForeignKey("material_queries.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_asset_id", sa.UUID(), sa.ForeignKey("assets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("material_clip_id", sa.UUID(), sa.ForeignKey("material_clips.id", ondelete="SET NULL"), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("coarse_score", sa.Float(), nullable=True),
        sa.Column("lighthouse_score", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("start_sec", sa.Float(), nullable=False),
        sa.Column("end_sec", sa.Float(), nullable=False),
        sa.Column("metadata", postgresql.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_material_query_results_query_id", "material_query_results", ["query_id"])


def downgrade() -> None:
    op.drop_table("material_query_results")
    op.drop_table("material_queries")
    op.drop_table("material_clips")
    op.drop_table("material_items")
    op.drop_table("material_libraries")
