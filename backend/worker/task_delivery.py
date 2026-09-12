"""Canonical delivery identity shared by imported and module-entry workers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass
class WorkerTaskDelivery:
    redis_stream: str
    consumer_group: str
    message_id: str
    payload_sha256: str
    dispatch_key: uuid.UUID | None
    attestation_id: uuid.UUID | None = None
    event_emission_id: uuid.UUID | None = None
