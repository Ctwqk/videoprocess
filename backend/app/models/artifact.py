from __future__ import annotations
import enum
import uuid
from datetime import datetime
from sqlalchemy import String, BigInteger, Enum, ForeignKey, JSON, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base, UUIDPrimaryKeyMixin


class ArtifactKind(str, enum.Enum):
    INTERMEDIATE = "INTERMEDIATE"
    FINAL = "FINAL"


class Artifact(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "artifacts"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    kind: Mapped[ArtifactKind] = mapped_column(
        Enum(ArtifactKind, name="artifact_kind", create_constraint=True),
        default=ArtifactKind.INTERMEDIATE,
    )
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    file_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    storage_backend: Mapped[str] = mapped_column(String(50), default="local")
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    media_info: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
