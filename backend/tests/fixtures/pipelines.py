from __future__ import annotations

from app.schemas.pipeline import PipelineDefinition


def source_trim_export_pipeline(asset_id: str = "00000000-0000-0000-0000-000000000001") -> PipelineDefinition:
    return PipelineDefinition.model_validate(
        {
            "nodes": [
                {
                    "id": "src",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source",
                        "config": {"asset_id": asset_id, "media_type": "video"},
                        "asset_id": asset_id,
                    },
                },
                {
                    "id": "trim",
                    "type": "trim",
                    "position": {"x": 260, "y": 0},
                    "data": {
                        "label": "Trim",
                        "config": {"start_time": "00:00:00", "duration": "3"},
                    },
                },
                {
                    "id": "export",
                    "type": "export",
                    "position": {"x": 520, "y": 0},
                    "data": {
                        "label": "Export",
                        "config": {"output_dir": "/tmp/vp_test_export", "filename": "clip.mp4"},
                    },
                },
            ],
            "edges": [
                {
                    "id": "e-src-trim",
                    "source": "src",
                    "target": "trim",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
                {
                    "id": "e-trim-export",
                    "source": "trim",
                    "target": "export",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        }
    )


def two_source_concat_export_pipeline(
    first_asset_id: str = "00000000-0000-0000-0000-000000000001",
    second_asset_id: str = "00000000-0000-0000-0000-000000000002",
) -> PipelineDefinition:
    return PipelineDefinition.model_validate(
        {
            "nodes": [
                {
                    "id": "src_1",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source 1",
                        "config": {"asset_id": first_asset_id, "media_type": "video"},
                        "asset_id": first_asset_id,
                    },
                },
                {
                    "id": "src_2",
                    "type": "source",
                    "position": {"x": 0, "y": 140},
                    "data": {
                        "label": "Source 2",
                        "config": {"asset_id": second_asset_id, "media_type": "video"},
                        "asset_id": second_asset_id,
                    },
                },
                {
                    "id": "concat",
                    "type": "concat_timeline",
                    "position": {"x": 260, "y": 70},
                    "data": {"label": "Concat", "config": {"transition": "none"}},
                },
                {
                    "id": "export",
                    "type": "export",
                    "position": {"x": 520, "y": 70},
                    "data": {"label": "Export", "config": {"output_dir": "/tmp/vp_test_export"}},
                },
            ],
            "edges": [
                {
                    "id": "e-src1-concat",
                    "source": "src_1",
                    "target": "concat",
                    "sourceHandle": "output",
                    "targetHandle": "video_first",
                },
                {
                    "id": "e-src2-concat",
                    "source": "src_2",
                    "target": "concat",
                    "sourceHandle": "output",
                    "targetHandle": "video_second",
                },
                {
                    "id": "e-concat-export",
                    "source": "concat",
                    "target": "export",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        }
    )


def planner_search_url_download_pipeline() -> PipelineDefinition:
    return PipelineDefinition.model_validate(
        {
            "nodes": [
                {
                    "id": "search",
                    "type": "youtube_search",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "YouTube Search",
                        "config": {"query": "cat compilation", "max_results": 5},
                    },
                },
                {
                    "id": "zip",
                    "type": "zip_records",
                    "position": {"x": 260, "y": 0},
                    "data": {"label": "Zip", "config": {"channel_count": 1, "record_limit": 2}},
                },
                {
                    "id": "download",
                    "type": "url_download",
                    "position": {"x": 520, "y": 0},
                    "data": {"label": "Download", "config": {"format": "best"}},
                },
                {
                    "id": "export",
                    "type": "export",
                    "position": {"x": 780, "y": 0},
                    "data": {"label": "Export", "config": {"output_dir": "/tmp/vp_test_export"}},
                },
            ],
            "edges": [
                {
                    "id": "e-search-zip",
                    "source": "search",
                    "target": "zip",
                    "sourceHandle": "results",
                    "targetHandle": "input_1",
                },
                {
                    "id": "e-zip-download",
                    "source": "zip",
                    "target": "download",
                    "sourceHandle": "output_1",
                    "targetHandle": "url_input",
                },
                {
                    "id": "e-download-export",
                    "source": "download",
                    "target": "export",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        }
    )
