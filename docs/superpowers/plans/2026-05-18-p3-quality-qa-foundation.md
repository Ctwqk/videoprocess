# P3 Quality QA Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add export-time soft quality QA with one automatic repair attempt and reusable loudnorm/VMAF helpers.

**Architecture:** Add a focused worker helper module, `worker.handlers.media_quality`, that owns QA config parsing, VMAF parsing/measurement, loudnorm parsing/filter construction, and one repair encode. Keep `ExportHandler` as the only integration point for P3A so final artifact metadata receives `quality_report` without changing orchestrator or database schema. Expose QA parameters on the `export` node registry while keeping the gate soft: repair failures and unavailable tools produce warnings instead of node failure.

**Tech Stack:** Python worker handlers, ffmpeg/ffprobe command construction, pytest, existing node registry dataclasses.

---

## File Map

- Create `backend/worker/handlers/media_quality.py`: reusable QA config, report helpers, VMAF JSON parser, loudnorm JSON parser, second-pass loudnorm filter builder, and `MediaQualityService`.
- Modify `backend/worker/handlers/export.py`: copy as before, call `MediaQualityService.qa_export()`, replace output/export copies when repaired, return `quality_report`.
- Modify `backend/app/node_registry/builtin/export.py`: add QA config params.
- Add `backend/tests/worker/test_media_quality_service.py`: unit tests for parsers, report behavior, and repair decisions.
- Add `backend/tests/worker/test_export_handler.py`: export integration tests with a fake QA service.
- Modify `backend/tests/autoflow/test_node_registration.py`: assert export exposes QA params.

## Task 1: Media Quality Helper

**Files:**
- Create: `backend/worker/handlers/media_quality.py`
- Test: `backend/tests/worker/test_media_quality_service.py`

- [ ] **Step 1: Write failing parser/config tests**

Create `backend/tests/worker/test_media_quality_service.py` with these tests:

```python
from __future__ import annotations

import json
from pathlib import Path

from worker.handlers.media_quality import (
    QualityQAConfig,
    MediaQualityService,
    build_loudnorm_apply_filter,
    parse_loudnorm_json,
    parse_vmaf_score,
)


def test_quality_config_parses_defaults_and_node_overrides():
    default_config = QualityQAConfig.from_node_config({})
    assert default_config.enabled is True
    assert default_config.gate_mode == "soft_repair_once"
    assert default_config.vmaf_min_score == 80

    overridden = QualityQAConfig.from_node_config(
        {
            "enable_quality_qa": "false",
            "vmaf_min_score": "92",
            "loudnorm_target_i": "-18",
            "loudnorm_target_lra": "9",
            "loudnorm_target_tp": "-2",
        }
    )
    assert overridden.enabled is False
    assert overridden.vmaf_min_score == 92
    assert overridden.loudnorm_target_i == -18
    assert overridden.loudnorm_target_lra == 9
    assert overridden.loudnorm_target_tp == -2


def test_parse_vmaf_score_reads_pooled_metrics(tmp_path: Path):
    log_path = tmp_path / "vmaf.json"
    log_path.write_text(json.dumps({"pooled_metrics": {"vmaf": {"mean": 83.42}}}))
    assert parse_vmaf_score(log_path) == 83.42


def test_parse_loudnorm_json_reads_ffmpeg_stderr_block():
    stderr = '''
    ignored
    {
      "input_i" : "-19.03",
      "input_tp" : "-1.12",
      "input_lra" : "8.20",
      "input_thresh" : "-29.11",
      "target_offset" : "2.10"
    }
    '''
    stats = parse_loudnorm_json(stderr)
    assert stats["input_i"] == "-19.03"
    assert stats["target_offset"] == "2.10"


def test_loudnorm_apply_filter_uses_measured_values():
    stats = {
        "input_i": "-19.03",
        "input_tp": "-1.12",
        "input_lra": "8.20",
        "input_thresh": "-29.11",
        "target_offset": "2.10",
    }
    filter_text = build_loudnorm_apply_filter(stats, target_i=-16, target_lra=11, target_tp=-1.5)
    assert "measured_I=-19.03" in filter_text
    assert "measured_LRA=8.20" in filter_text
    assert "measured_TP=-1.12" in filter_text
    assert "measured_thresh=-29.11" in filter_text
    assert "offset=2.10" in filter_text
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_media_quality_service.py -q
```

Expected: import fails because `worker.handlers.media_quality` does not exist.

- [ ] **Step 3: Implement minimal parser/config helper**

Create `backend/worker/handlers/media_quality.py` with:

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from worker.handlers.base import BaseHandler


@dataclass(frozen=True)
class QualityQAConfig:
    enabled: bool = True
    gate_mode: str = "soft_repair_once"
    vmaf_min_score: float = 80.0
    loudnorm_target_i: float = -16.0
    loudnorm_target_lra: float = 11.0
    loudnorm_target_tp: float = -1.5

    @classmethod
    def from_node_config(cls, node_config: dict[str, Any]) -> "QualityQAConfig":
        return cls(
            enabled=BaseHandler.parse_bool_param(node_config.get("enable_quality_qa"), True),
            gate_mode=str(node_config.get("quality_gate_mode") or "soft_repair_once"),
            vmaf_min_score=_float_param(node_config.get("vmaf_min_score"), 80.0),
            loudnorm_target_i=_float_param(node_config.get("loudnorm_target_i"), -16.0),
            loudnorm_target_lra=_float_param(node_config.get("loudnorm_target_lra"), 11.0),
            loudnorm_target_tp=_float_param(node_config.get("loudnorm_target_tp"), -1.5),
        )
```

Also add `parse_vmaf_score()`, `parse_loudnorm_json()`, `build_loudnorm_apply_filter()`, and `_float_param()`.

- [ ] **Step 4: Verify parser/config tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_media_quality_service.py -q
```

Expected: current tests pass.

## Task 2: Soft Repair Service

**Files:**
- Modify: `backend/worker/handlers/media_quality.py`
- Test: `backend/tests/worker/test_media_quality_service.py`

- [ ] **Step 1: Add failing service behavior tests**

Append tests using a fake service:

```python
import shutil


class FakeQualityService(MediaQualityService):
    def __init__(self, *, vmaf_score=90.0, audio_stats=None, repair_fails=False):
        super().__init__()
        self.vmaf_score = vmaf_score
        self.audio_stats = audio_stats
        self.repair_fails = repair_fails
        self.repair_calls = 0

    async def measure_vmaf(self, reference_path, distorted_path):
        return self.vmaf_score

    async def measure_loudnorm(self, media_path, config):
        return self.audio_stats

    async def repair_export(self, source_path, output_path, config, loudnorm_stats):
        self.repair_calls += 1
        if self.repair_fails:
            raise RuntimeError("repair failed")
        repaired = Path(output_path).with_suffix(".repaired.mp4")
        shutil.copy2(output_path, repaired)
        return str(repaired)


async def test_qa_export_warns_only_when_disabled(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    result = await FakeQualityService(vmaf_score=10).qa_export(
        source_path=str(source),
        output_path=str(output),
        node_config={"enable_quality_qa": False},
    )
    assert result.repaired_path is None
    assert result.report["enabled"] is False


async def test_qa_export_repairs_once_when_vmaf_is_below_threshold(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    service = FakeQualityService(vmaf_score=70)
    result = await service.qa_export(source_path=str(source), output_path=str(output), node_config={})
    assert service.repair_calls == 1
    assert result.repaired_path is not None
    assert result.report["reencode_attempted"] is True
    assert result.report["qa_action"] == "reencoded_once"


async def test_qa_export_keeps_original_when_repair_fails(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    result = await FakeQualityService(vmaf_score=70, repair_fails=True).qa_export(
        source_path=str(source),
        output_path=str(output),
        node_config={},
    )
    assert result.repaired_path is None
    assert "repair_failed" in result.report["warnings"]
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_media_quality_service.py -q
```

Expected: fails because `MediaQualityService.qa_export()` and `QualityQAResult` do not exist.

- [ ] **Step 3: Implement service and result object**

Add:

```python
@dataclass(frozen=True)
class QualityQAResult:
    report: dict[str, Any]
    repaired_path: str | None = None
```

Implement `MediaQualityService`:

- `qa_export(source_path, output_path, node_config)` returns disabled report when config disabled.
- Calls `measure_vmaf()` and records `vmaf_score`; unavailable measurement appends warnings.
- Calls `measure_loudnorm()` and records `audio_lufs`, `audio_true_peak`; unavailable measurement appends warnings.
- If VMAF is below threshold or loudness is out of range and gate mode is `soft_repair_once`, calls `repair_export()` once.
- Catches repair exceptions, records `repair_failed`, and preserves original output.

- [ ] **Step 4: Verify service tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_media_quality_service.py -q
```

Expected: all media quality service tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/worker/handlers/media_quality.py backend/tests/worker/test_media_quality_service.py
git commit -m "feat: add media quality qa service"
```

## Task 3: Export Handler Integration

**Files:**
- Modify: `backend/worker/handlers/export.py`
- Test: `backend/tests/worker/test_export_handler.py`

- [ ] **Step 1: Write failing export integration tests**

Create `backend/tests/worker/test_export_handler.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify red**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_export_handler.py -q
```

Expected: fails because `ExportHandler` does not accept `quality_service` and returns no metadata.

- [ ] **Step 3: Implement export integration**

Modify `ExportHandler`:

- Add `__init__(self, quality_service: MediaQualityService | None = None)`.
- Copy input to export destination and `output_path` as today.
- Call `self.quality_service.qa_export(source_path=input_file, output_path=output_path, node_config=node_config)`.
- If `repaired_path` is returned, copy repaired file to export destination and `output_path`.
- Return `{"quality_report": qa_result.report, "export_path": export_path}`.

- [ ] **Step 4: Verify export tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_export_handler.py tests/worker/test_media_quality_service.py -q
```

Expected: selected worker QA/export tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/worker/handlers/export.py backend/tests/worker/test_export_handler.py
git commit -m "feat: run soft quality qa during export"
```

## Task 4: Export Registry Contract

**Files:**
- Modify: `backend/app/node_registry/builtin/export.py`
- Modify: `backend/tests/autoflow/test_node_registration.py`

- [ ] **Step 1: Add failing registry test**

Add to `backend/tests/autoflow/test_node_registration.py`:

```python
def test_export_node_contract_exposes_quality_qa_params():
    definition = NodeTypeRegistry.get().get_type("export")
    assert definition is not None
    params = {param.name: param for param in definition.params}
    assert params["enable_quality_qa"].default is True
    assert params["quality_gate_mode"].default == "soft_repair_once"
    assert params["quality_gate_mode"].options == ["soft_repair_once"]
    assert params["vmaf_min_score"].default == 80
    assert params["loudnorm_target_i"].default == -16
    assert params["loudnorm_target_lra"].default == 11
    assert params["loudnorm_target_tp"].default == -1.5
```

- [ ] **Step 2: Run test to verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_node_registration.py::test_export_node_contract_exposes_quality_qa_params -q
```

Expected: fails because export node lacks QA params.

- [ ] **Step 3: Add export params**

Add these `ParamDefinition` entries to `backend/app/node_registry/builtin/export.py`:

```python
ParamDefinition(name="enable_quality_qa", param_type="boolean", default=True, description="Run export quality QA"),
ParamDefinition(name="quality_gate_mode", param_type="select", default="soft_repair_once", options=["soft_repair_once"], description="Soft quality repair policy"),
ParamDefinition(name="vmaf_min_score", param_type="number", default=80, min_value=0, max_value=100, description="Minimum VMAF score before soft repair"),
ParamDefinition(name="loudnorm_target_i", param_type="number", default=-16, min_value=-40, max_value=0, description="Integrated loudness target"),
ParamDefinition(name="loudnorm_target_lra", param_type="number", default=11, min_value=1, max_value=30, description="Loudness range target"),
ParamDefinition(name="loudnorm_target_tp", param_type="number", default=-1.5, min_value=-9, max_value=0, description="True peak target"),
```

- [ ] **Step 4: Verify registry tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_node_registration.py -q
```

Expected: node registration tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/app/node_registry/builtin/export.py backend/tests/autoflow/test_node_registration.py
git commit -m "feat: expose export quality qa params"
```

## Task 5: Final Verification

**Files:**
- All files changed in Tasks 1-4.

- [ ] **Step 1: Run targeted tests**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_media_quality_service.py tests/worker/test_export_handler.py tests/worker/test_media_quality_args.py tests/autoflow/test_node_registration.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run full backend suite**

Run:

```bash
cd backend
python3 -m pytest
```

Expected: full backend suite passes.

- [ ] **Step 3: Run required optional checks**

Run:

```bash
cd backend
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: command may report missing modules in this environment; capture exact output.

- [ ] **Step 4: Run git hygiene checks**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors and no unstaged changes after commits.
