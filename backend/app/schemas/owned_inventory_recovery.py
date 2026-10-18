"""Recovery preserves an existing approval; callers cannot replace its authority."""
from pydantic import BaseModel, ConfigDict

from app.schemas.channel_agent import InventorySHA256, InventoryText, InventoryUUID


class OperatorRuntimeEvidence(BaseModel):
    """Authenticated operator attestation, not server verification of host deployment."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    reference: InventoryText
    sha256: InventorySHA256
    attestation: InventoryText


class OwnedInventoryRecovery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    client_request_id: InventoryUUID
    manifest_sha256: InventorySHA256
    expected_hold_sha256: InventorySHA256
    recovery_reference: InventoryText
    operator_runtime_evidence: OperatorRuntimeEvidence
