from copy import deepcopy

import pytest

from app.services import owned_seed_inventory as inv
from tests.services.test_owned_producer_fence import owned_graph


def graph_rows():
    graph = owned_graph("11111111-1111-4111-8111-111111111111")
    nodes, artifacts = [], []
    for spec in graph["nodes"]:
        name = spec["id"]
        node = {"id": "node-" + name, "job_id": "job", "node_id": name, "node_type": spec["type"],
            "node_config": {**spec["data"]["config"], **({"asset_id": spec["data"]["asset_id"]} if spec["data"].get("asset_id") else {})},
            "status": "RUNNING" if spec["type"] == "youtube_upload" else "PENDING" if spec["type"] == "export" else "SUCCEEDED",
            "error_message": None, "retry_count": 0, "output_artifact_id": "artifact-" + name,
            "input_artifact_ids": ["artifact-" + e["source"] for e in graph["edges"] if e["target"] == name]}
        if spec["type"] in {"export", "youtube_upload"}:
            node["output_artifact_id"] = None
        else:
            artifacts.append({"id": node["output_artifact_id"], "job_id": "job", "node_execution_id": node["id"]})
        nodes.append(node)
    upload = next(n for n in nodes if n["node_type"] == "youtube_upload")
    return graph, nodes, artifacts, upload


@pytest.mark.parametrize("fault", [None, "trim_input", "transcode_input", "source_output", "artifact_job", "artifact_node", "ancestor_pending"])
def test_all_upload_ancestor_links_are_exact_but_export_may_be_pending(fault):
    from app.services.owned_producer_lineage import require_upload_lineage
    graph, nodes, artifacts, upload = deepcopy(graph_rows())
    source = next(n for n in nodes if n["node_type"] == "source")
    trim = next(n for n in nodes if n["node_type"] == "trim")
    transcode = next(n for n in nodes if n["node_type"] == "transcode")
    if fault == "trim_input":
        trim["input_artifact_ids"] = upload["input_artifact_ids"]
    elif fault == "transcode_input":
        transcode["input_artifact_ids"] = [source["output_artifact_id"]]
    elif fault == "source_output":
        source["output_artifact_id"] = transcode["output_artifact_id"]
    elif fault == "artifact_job":
        artifacts[0]["job_id"] = "other-job"
    elif fault == "artifact_node":
        artifacts[0]["node_execution_id"] = transcode["id"]
    elif fault == "ancestor_pending":
        trim["status"] = "PENDING"
    if fault is None:
        require_upload_lineage(graph, nodes, artifacts, job_id="job", upload_id=upload["id"], input_id=upload["input_artifact_ids"][0])
    else:
        with pytest.raises(inv.OwnedInventoryError, match="owned_inventory_artifact_lineage"):
            require_upload_lineage(graph, nodes, artifacts, job_id="job", upload_id=upload["id"], input_id=upload["input_artifact_ids"][0])
