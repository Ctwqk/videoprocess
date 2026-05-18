from __future__ import annotations

from pydantic import BaseModel, Field

from app.node_registry.registry import NodeTypeRegistry
from app.schemas.autoflow import AutoFlowClipCandidate
from app.schemas.pipeline import PipelineDefinition, ValidationError


class AutoFlowRepairResult(BaseModel):
    definition: PipelineDefinition
    repaired: bool = False
    applied_repairs: list[str] = Field(default_factory=list)
    unrepairable_errors: list[str] = Field(default_factory=list)


class AutoFlowUnrepairableError(RuntimeError):
    def __init__(self, *, unrepairable_errors: list[str], applied_repairs: list[str]) -> None:
        super().__init__("AutoFlow workflow could not be repaired")
        self.unrepairable_errors = unrepairable_errors
        self.applied_repairs = applied_repairs


class AutoFlowRepairService:
    def repair(
        self,
        definition: PipelineDefinition,
        errors: list[ValidationError],
        candidates: list[AutoFlowClipCandidate],
    ) -> AutoFlowRepairResult:
        data = definition.model_dump()
        applied: list[str] = []
        unrepairable: list[str] = []

        for error in errors:
            if error.type == "cycle_detected":
                unrepairable.append("cycle_detected")
                continue
            if error.type == "port_type_mismatch":
                unrepairable.append(f"port_type_mismatch:{error.edge_id}")
                continue
            if error.type == "invalid_param" and error.node_id and error.param_name:
                if error.param_name == "asset_id" and self._repair_missing_asset(data, error.node_id, candidates):
                    applied.append(f"missing_asset:{error.node_id}")
                    continue
                if self._repair_invalid_param(data, error.node_id, error.param_name):
                    applied.append(f"invalid_param:{error.node_id}.{error.param_name}")
                else:
                    unrepairable.append(f"invalid_param:{error.node_id}.{error.param_name}")
                continue
            if error.type == "missing_asset" and error.node_id:
                if self._repair_missing_asset(data, error.node_id, candidates):
                    applied.append(f"missing_asset:{error.node_id}")
                else:
                    unrepairable.append(f"missing_asset:{error.node_id}")
                continue
            unrepairable.append(error.type)

        if unrepairable:
            raise AutoFlowUnrepairableError(
                unrepairable_errors=unrepairable,
                applied_repairs=applied,
            )

        repaired_definition = PipelineDefinition.model_validate(data)
        return AutoFlowRepairResult(
            definition=repaired_definition,
            repaired=bool(applied),
            applied_repairs=applied,
            unrepairable_errors=[],
        )

    def _repair_invalid_param(self, data: dict, node_id: str, param_name: str) -> bool:
        node = self._node(data, node_id)
        if not node:
            return False
        node_type = node.get("type")
        node_def = NodeTypeRegistry.get().get_type(node_type)
        if not node_def:
            return False
        param_def = next((param for param in node_def.params if param.name == param_name), None)
        if not param_def:
            return False
        config = node.setdefault("data", {}).setdefault("config", {})
        config[param_name] = param_def.default
        return True

    def _repair_missing_asset(
        self,
        data: dict,
        node_id: str,
        candidates: list[AutoFlowClipCandidate],
    ) -> bool:
        node = self._node(data, node_id)
        if not node:
            return False
        candidate = next((item for item in candidates if item.asset_id), None)
        if not candidate or not candidate.asset_id:
            return False
        data_bucket = node.setdefault("data", {})
        config = data_bucket.setdefault("config", {})
        config["asset_id"] = candidate.asset_id
        data_bucket["asset_id"] = candidate.asset_id
        return True

    def _node(self, data: dict, node_id: str) -> dict | None:
        return next((node for node in data.get("nodes", []) if node.get("id") == node_id), None)
