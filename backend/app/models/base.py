from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import func, text
from datetime import datetime
import uuid as uuid_mod


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class UUIDPrimaryKeyMixin:
    id: Mapped[uuid_mod.UUID] = mapped_column(
        primary_key=True,
        default=uuid_mod.uuid4,
        server_default=text("gen_random_uuid()"),
    )
