from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.autoflow import AutoFlowRequest
from app.schemas.pipeline import PipelineDefinition


EXTERNAL_SOURCE_NODE_TYPES = {
    "youtube_search",
    "bilibili_search",
    "x_search",
    "xiaohongshu_search",
    "url_download",
}
UPLOAD_NODE_TYPES = {"youtube_upload", "x_upload", "xiaohongshu_upload"}


class PipelinePolicyIssue(BaseModel):
    code: str
    message: str
    node_id: str | None = None
    severity: str = "error"


class PipelinePolicyResult(BaseModel):
    valid: bool
    definition: PipelineDefinition
    errors: list[PipelinePolicyIssue] = Field(default_factory=list)
    warnings: list[PipelinePolicyIssue] = Field(default_factory=list)
    repairs: list[str] = Field(default_factory=list)
    requires_review: bool = False


def validate_pipeline_policy(
    definition: PipelineDefinition,
    request: AutoFlowRequest,
) -> PipelinePolicyResult:
    data = definition.model_dump()
    errors: list[PipelinePolicyIssue] = []
    warnings: list[PipelinePolicyIssue] = []
    repairs: list[str] = []
    requires_review = False

    if request.source_policy == "owned_only":
        for node in data["nodes"]:
            if node.get("type") in EXTERNAL_SOURCE_NODE_TYPES:
                errors.append(
                    PipelinePolicyIssue(
                        code="external_source_blocked",
                        node_id=node.get("id"),
                        message=f"Node '{node.get('id')}' uses external source '{node.get('type')}' under owned_only policy.",
                    )
                )

    if any(node.get("type") in EXTERNAL_SOURCE_NODE_TYPES for node in data["nodes"]):
        requires_review = True

    if request.publish_mode == "preview_only":
        upload_ids = {node["id"] for node in data["nodes"] if node.get("type") in UPLOAD_NODE_TYPES}
        if upload_ids:
            data["nodes"] = [node for node in data["nodes"] if node.get("id") not in upload_ids]
            data["edges"] = [
                edge
                for edge in data["edges"]
                if edge.get("source") not in upload_ids and edge.get("target") not in upload_ids
            ]
            repairs.extend(f"removed_upload:{node_id}" for node_id in sorted(upload_ids))
    else:
        for node in data["nodes"]:
            if node.get("type") not in UPLOAD_NODE_TYPES:
                continue
            requires_review = True
            config = node.setdefault("data", {}).setdefault("config", {})
            desired_privacy = _desired_upload_privacy(request.publish_mode, str(config.get("privacy") or "private"))
            if config.get("privacy") != desired_privacy:
                config["privacy"] = desired_privacy
                repairs.append(f"privacy:{node.get('id')}:{desired_privacy}")
            if request.publish_mode == "public_after_review":
                warnings.append(
                    PipelinePolicyIssue(
                        code="public_requires_approval",
                        node_id=node.get("id"),
                        severity="warning",
                        message="Public publishing requires explicit public approval; upload privacy remains private.",
                    )
                )

    repaired_definition = PipelineDefinition.model_validate(data)
    return PipelinePolicyResult(
        valid=not errors,
        definition=repaired_definition if not errors else definition,
        errors=errors,
        warnings=warnings,
        repairs=repairs if not errors else [],
        requires_review=requires_review or bool(errors),
    )


def _desired_upload_privacy(publish_mode: str, current_privacy: str) -> str:
    if publish_mode == "private_upload":
        return "private"
    if publish_mode == "unlisted_upload":
        return current_privacy if current_privacy in {"private", "unlisted"} else "unlisted"
    if publish_mode == "public_after_review":
        return "private"
    return current_privacy
