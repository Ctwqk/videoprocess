from __future__ import annotations

from pathlib import Path

import pytest

from worker.handlers.export import ExportHandler


class FakeQAResult:
    def __init__(self, report, repaired_path=None):
        self.report = report
        self.repaired_path = repaired_path


class FakeQualityService:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def qa_export(self, *, source_path, output_path, node_config):
        self.calls.append((source_path, output_path, node_config))
        return self.result


@pytest.mark.asyncio
async def test_export_returns_quality_report_metadata(tmp_path: Path):
    source = tmp_path / "input.mp4"
    source.write_bytes(b"input")
    output = tmp_path / "artifact.mp4"
    export_dir = tmp_path / "exports"
    service = FakeQualityService(FakeQAResult({"enabled": True, "qa_action": "passed"}))

    result = await ExportHandler(quality_service=service).execute(
        {"output_dir": str(export_dir), "filename": "final.mp4"},
        {"input": str(source)},
        str(output),
    )

    assert output.read_bytes() == b"input"
    assert (export_dir / "final.mp4").read_bytes() == b"input"
    assert result == {"quality_report": {"enabled": True, "qa_action": "passed"}}
    assert service.calls[0][0] == str(source)


@pytest.mark.asyncio
async def test_export_replaces_output_with_repaired_file(tmp_path: Path):
    source = tmp_path / "input.mp4"
    source.write_bytes(b"input")
    repaired = tmp_path / "repaired.mp4"
    repaired.write_bytes(b"repaired")
    output = tmp_path / "artifact.mp4"
    export_dir = tmp_path / "exports"
    service = FakeQualityService(FakeQAResult({"qa_action": "reencoded_once"}, str(repaired)))

    result = await ExportHandler(quality_service=service).execute(
        {"output_dir": str(export_dir), "filename": "final.mp4"},
        {"input": str(source)},
        str(output),
    )

    assert output.read_bytes() == b"repaired"
    assert (export_dir / "final.mp4").read_bytes() == b"repaired"
    assert result["quality_report"]["qa_action"] == "reencoded_once"
