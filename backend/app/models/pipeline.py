from __future__ import annotations
import uuid as uuid_mod
from sqlalchemy import String, Text, Boolean, Integer, JSON
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Pipeline(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "pipelines"

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(Text, default="")
    definition: Mapped[dict] = mapped_column(JSON, nullable=False)
    is_template: Mapped[bool] = mapped_column(Boolean, default=False)
    template_tags: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    thumbnail_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_by: Mapped[str] = mapped_column(String(255), default="system")
    version: Mapped[int] = mapped_column(Integer, default=1)
