#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/.runtime/video-smoke"
TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
OUTPUT_PATH="$OUTPUT_DIR/vp-smoke-$TIMESTAMP.mp4"

for command_name in ffmpeg ffprobe git ssh uv; do
  command -v "$command_name" >/dev/null || {
    echo "missing required command: $command_name" >&2
    exit 1
  }
done

mkdir -p "$OUTPUT_DIR"
VP_PYTHON_API="${VP_PYTHON_API:-http://10.0.0.127:18080}"
VP_REDIS_URL="${VP_REDIS_URL:-redis://10.0.0.150:6380/0}"
VP_SMOKE_COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD)"
VP_SMOKE_DEPLOYED_COMMIT="$(ssh 10.0.0.127 \
  'tr -d "\n" < /Users/wenjieliu/VideoProcess-app/.deploy-sync-source-commit')"

if [[ "$VP_SMOKE_COMMIT" != "$VP_SMOKE_DEPLOYED_COMMIT" ]]; then
  echo "source/deployed commit mismatch: $VP_SMOKE_COMMIT != $VP_SMOKE_DEPLOYED_COMMIT" >&2
  exit 1
fi

export VP_PYTHON_API VP_REDIS_URL VP_SMOKE_COMMIT VP_SMOKE_DEPLOYED_COMMIT
export VP_GO_WORKER_SMOKE_STRICT=1
export VP_GO_SMOKE_OUTPUT="$OUTPUT_PATH"

cd "$ROOT_DIR/backend"
uv run python -m pytest \
  ../tests/go_migration/test_go_trim_worker_smoke.py::test_trim_worker_mixed_mode_smoke_requires_real_job_completion \
  -q -s

printf 'video=%s\n' "$OUTPUT_PATH"
printf 'evidence=%s\n' "${OUTPUT_PATH%.mp4}.json"
