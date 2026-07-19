#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUARANTINE="$ROOT_DIR/scripts/quarantine_channelops_backlog.py"
CANARY="$ROOT_DIR/scripts/run_vp_unlisted_canary.py"

for script in "$QUARANTINE" "$CANARY"; do
  [[ -f "$script" ]] || {
    echo "FAIL: missing script: $script" >&2
    exit 1
  }
done

grep -Fq 'action="store_true"' "$QUARANTINE"
grep -Fq 'default=False' "$QUARANTINE"
grep -Fq 'apply=args.apply' "$QUARANTINE"
grep -Fq 'DATABASE_URL' "$QUARANTINE"
grep -Fq -- '--evidence' "$QUARANTINE"

grep -Fq -- '--confirm-live-unlisted' "$CANARY"
grep -Fq 'DATABASE_URL' "$CANARY"
grep -Fq 'pg_try_advisory_lock' "$CANARY"
grep -Fq '1080x1920' "$CANARY"
grep -Fq 'duration_seconds=8' "$CANARY"
grep -Fq '"license": "owned"' "$CANARY"
grep -Fq '"provenance": "generated"' "$CANARY"
grep -Fq 'external_asset_auto_publish=False' "$CANARY"
grep -Fq 'default_privacy="unlisted"' "$CANARY"
grep -Fq 'default_publish_visibility="unlisted"' "$CANARY"
grep -Fq 'source_policy="owned_only"' "$CANARY"
grep -Fq 'source_platforms_json=[]' "$CANARY"
grep -Fq '"source_strategy": "input_video"' "$CANARY"
grep -Fq '"planning_mode": "template"' "$CANARY"
grep -Fq '"max_posts_per_day": 1' "$CANARY"
grep -Fq 'operator_preapproved_live_unlisted_canary' "$CANARY"
grep -Fq 'pre-existing runnable jobs' "$CANARY"
grep -Fq 'unsafe ChannelOps backlog' "$CANARY"
grep -Fq 'exactly one runnable job' "$CANARY"
grep -Fq 'finally:' "$CANARY"
grep -Fq '"/internal/schedule/video/close"' "$CANARY"
grep -Fq '"/internal/schedule/video/drain"' "$CANARY"
grep -Fq '"/internal/schedule/video/open"' "$CANARY"
grep -Fq 'never deletes the YouTube video' "$CANARY"
grep -Fq 'operator_canary_failure' "$CANARY"
grep -Fq 'CANARY_PLAN_DELAY_SECONDS = 300' "$CANARY"
grep -Fq 'mark_schedule_close_failure(evidence, close_error)' "$CANARY"
if grep -Fq '/api/v1/channel-agent' "$CANARY"; then
  echo "FAIL: canary runner must not call the unexposed ChannelAgent HTTP API" >&2
  exit 1
fi

python3 - "$CANARY" <<'PY'
import ast
import pathlib
import sys

tree = ast.parse(pathlib.Path(sys.argv[1]).read_text())
finally_calls = []
for node in ast.walk(tree):
    if isinstance(node, ast.Try) and node.finalbody:
        finally_calls.extend(
            child
            for statement in node.finalbody
            for child in ast.walk(statement)
            if isinstance(child, ast.Call)
        )
if not any("close_schedule" in ast.unparse(call.func) for call in finally_calls):
    raise SystemExit("FAIL: schedule close must be called from a finally block")
PY

"$ROOT_DIR/backend/.venv/bin/python" - "$CANARY" "$QUARANTINE" <<'PY'
import importlib.util
import os
import pathlib
import stat
import sys
import tempfile

path = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("vp_unlisted_canary", path)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

quarantine_path = pathlib.Path(sys.argv[2])
quarantine_spec = importlib.util.spec_from_file_location("vp_quarantine", quarantine_path)
quarantine_module = importlib.util.module_from_spec(quarantine_spec)
assert quarantine_spec.loader is not None
quarantine_spec.loader.exec_module(quarantine_module)

sanitized = module.sanitize(
    {
        "DATABASE_URL": "postgresql://user:secret@example/db",
        "nested": {"access_token": "secret"},
        "message": "driver failed at redis://user:secret@example/0",
        "safe": "retained",
    }
)
assert sanitized == {
    "nested": {},
    "message": "driver failed at [redacted connection URL]",
    "safe": "retained",
}
assert module.safe_failure_message(RuntimeError("postgresql://user:secret@example/db")) == (
    "unexpected failure; inspect sanitized service logs by exception type"
)
assert module.recognized_metrics({"metrics": {"views": 0, "unknown": 5}}) == {"views": 0}
quota = module.quota_evidence(
    {
        "authenticated": True,
        "quota_estimate": {
            "daily_limit": 10_000,
            "estimated_units_used": 100,
            "estimated_units_remaining": 9_900,
            "upload_cost_per_request": 1_600,
        },
    }
)
assert quota["authenticated"] is True
try:
    module.quota_evidence(
        {
            "authenticated": True,
            "quota_estimate": {
                "daily_limit": 10_000,
                "estimated_units_used": 8_300,
                "estimated_units_remaining": 1_700,
                "upload_cost_per_request": 2_000,
            },
        }
    )
except module.CanaryError:
    pass
else:
    raise AssertionError("quota below the manager's upload cost must fail closed")

with tempfile.TemporaryDirectory() as directory:
    parent = pathlib.Path(directory) / "caller-owned"
    parent.mkdir(mode=0o755)
    os.chmod(parent, 0o755)
    module.atomic_write_json(parent / "canary.json", {"ok": True})
    quarantine_module.atomic_write_json(parent / "quarantine.json", {"ok": True})
    assert stat.S_IMODE(parent.stat().st_mode) == 0o755
    assert stat.S_IMODE((parent / "canary.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((parent / "quarantine.json").stat().st_mode) == 0o600
PY
