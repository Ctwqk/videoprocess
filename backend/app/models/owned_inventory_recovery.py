from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class OwnedInventoryRecoveryRecord(Base):
    """Append-only receipt. PostgreSQL procedure owns insertion and state transition."""

    __tablename__ = "owned_inventory_recoveries"
    __table_args__ = (UniqueConstraint("inventory_id", "expected_hold_sha256", name="uq_owned_recovery_incident"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    inventory_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("owned_seed_inventories.id", ondelete="RESTRICT"))
    request_sha256: Mapped[str] = mapped_column(String(64))
    expected_hold_sha256: Mapped[str] = mapped_column(String(64))
    recovered_by: Mapped[str] = mapped_column(String(255))
    recovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    transaction_id: Mapped[int] = mapped_column(BigInteger)
    before_inventory_json: Mapped[dict] = mapped_column(JSON)
    before_channel_json: Mapped[dict] = mapped_column(JSON)
    evidence_json: Mapped[dict] = mapped_column(JSON)
