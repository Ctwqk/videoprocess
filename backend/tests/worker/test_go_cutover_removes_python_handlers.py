from __future__ import annotations

from pathlib import Path

from worker.handlers import HANDLER_MAP


GO_CUTOVER_NODE_TYPES = {
    "bgm",
    "concat_horizontal",
    "concat_many",
    "concat_timeline",
    "concat_vertical",
    "concat_vertical_timeline",
    "export",
    "montage_assembler",
    "replace_audio",
    "title_overlay",
    "transcode",
    "trim",
    "vertical_crop",
    "watermark",
}


def test_go_cutover_nodes_are_not_registered_in_python_worker() -> None:
    assert GO_CUTOVER_NODE_TYPES.isdisjoint(HANDLER_MAP)


def test_go_cutover_python_handler_files_are_removed() -> None:
    handlers_dir = Path(__file__).parents[2] / "worker" / "handlers"

    remaining = sorted(
        node_type for node_type in GO_CUTOVER_NODE_TYPES if (handlers_dir / f"{node_type}.py").exists()
    )

    assert remaining == []
