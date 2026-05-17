from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class MaterialLibrary(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "material_libraries"

    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    is_disabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    items: Mapped[list["MaterialItem"]] = relationship(
        back_populates="library",
        cascade="all, delete-orphan",
    )
    clips: Mapped[list["MaterialClip"]] = relationship(
        back_populates="library",
        cascade="all, delete-orphan",
    )


class MaterialItem(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "material_items"

    library_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("material_libraries.id", ondelete="CASCADE"), nullable=False
    )
    asset_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(32), default="READY", nullable=False)
    duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    subtitle_source: Mapped[str] = mapped_column(String(64), default="asr_if_missing", nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)

    library: Mapped["MaterialLibrary"] = relationship(back_populates="items")
    clips: Mapped[list["MaterialClip"]] = relationship(
        back_populates="material_item",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("library_id", "asset_id", name="uq_material_item_library_asset"),
    )


class MaterialClip(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "material_clips"

    library_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("material_libraries.id", ondelete="CASCADE"), nullable=False
    )
    parent_material_item_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("material_items.id", ondelete="SET NULL"), nullable=True
    )
    source_asset_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    clip_id: Mapped[str] = mapped_column(String(255), nullable=False)
    start_sec: Mapped[float] = mapped_column(Float, nullable=False)
    end_sec: Mapped[float] = mapped_column(Float, nullable=False)
    subtitle_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    ocr_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    caption: Mapped[str | None] = mapped_column(Text, nullable=True)
    neighbor_clip_ids: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    clip_kind: Mapped[str] = mapped_column(String(32), default="coarse_window", nullable=False)
    storage_asset_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="SET NULL"), nullable=True
    )
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    library: Mapped["MaterialLibrary"] = relationship(back_populates="clips")
    material_item: Mapped["MaterialItem | None"] = relationship(back_populates="clips")


class MaterialQuery(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "material_queries"

    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    source_library_ids: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    result_library_ids: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    config_json: Mapped[dict | None] = mapped_column("config", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    results: Mapped[list["MaterialQueryResult"]] = relationship(
        back_populates="query",
        cascade="all, delete-orphan",
    )


class MaterialQueryResult(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "material_query_results"

    query_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("material_queries.id", ondelete="CASCADE"), nullable=False
    )
    source_asset_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False
    )
    material_clip_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("material_clips.id", ondelete="SET NULL"), nullable=True
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    coarse_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    lighthouse_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    start_sec: Mapped[float] = mapped_column(Float, nullable=False)
    end_sec: Mapped[float] = mapped_column(Float, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)

    query: Mapped["MaterialQuery"] = relationship(back_populates="results")
