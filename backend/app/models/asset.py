from __future__ import annotations
import uuid as uuid_mod
from datetime import datetime
from sqlalchemy import String, BigInteger, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base, UUIDPrimaryKeyMixin


class Asset(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "assets"

    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    original_name: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    file_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    storage_backend: Mapped[str] = mapped_column(String(50), default="local")
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    media_info: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(server_default=func.now())
    uploaded_by: Mapped[str] = mapped_column(String(255), default="system")
