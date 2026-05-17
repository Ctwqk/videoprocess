#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description="Create an AutoFlow material-library remix demo plan.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Running API base URL.")
    args = parser.parse_args()

    payload = {
        "prompt": "用素材库里的旅行素材做一个 20 秒海边日落治愈混剪，竖屏，先导出预览。",
        "material_library_ids": ["travel-library"],
    }
    plan = _post_plan(args.base_url, payload)
    _require_plan(plan, expected_intent="material_library_remix", expected_template="material_library_remix")
    _require_rights(plan, expected_status="allowed", expected_review=False)
    print(json.dumps(_summary(plan), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _post_plan(base_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        _endpoint(base_url),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"AutoFlow plan request failed with HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"AutoFlow API is not reachable at {base_url}: {exc.reason}") from exc


def _endpoint(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/v1/autoflow"):
        return f"{base}/plan"
    return f"{base}/api/v1/autoflow/plan"


def _require_plan(plan: dict[str, Any], *, expected_intent: str, expected_template: str) -> None:
    intent_type = plan.get("intent", {}).get("intent_type")
    template_id = plan.get("template_id")
    validation = plan.get("validation") or {}
    if intent_type != expected_intent:
        raise SystemExit(f"Unexpected intent_type: expected {expected_intent}, got {intent_type}")
    if template_id != expected_template:
        raise SystemExit(f"Unexpected template_id: expected {expected_template}, got {template_id}")
    if validation.get("valid") is not True:
        raise SystemExit(
            "AutoFlow returned an invalid plan: "
            + json.dumps({"errors": validation.get("errors", []), "warnings": validation.get("warnings", [])})
        )


def _require_rights(plan: dict[str, Any], *, expected_status: str, expected_review: bool) -> None:
    status = plan.get("rights", {}).get("status")
    needs_review = bool(plan.get("needs_review"))
    if status != expected_status:
        raise SystemExit(f"Unexpected rights status: expected {expected_status}, got {status}")
    if needs_review is not expected_review:
        raise SystemExit(f"Unexpected review gate: expected {expected_review}, got {needs_review}")


def _summary(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_id": plan.get("plan_id"),
        "intent_type": plan.get("intent", {}).get("intent_type"),
        "template_id": plan.get("template_id"),
        "validation_valid": plan.get("validation", {}).get("valid"),
        "rights_status": plan.get("rights", {}).get("status"),
        "needs_review": plan.get("needs_review"),
        "candidate_count": len(plan.get("candidates", [])),
        "keywords": plan.get("intent", {}).get("keywords", []),
        "node_types": [node.get("type") for node in plan.get("pipeline_definition", {}).get("nodes", [])],
        "warnings": plan.get("warnings", []),
    }


if __name__ == "__main__":
    sys.exit(main())
