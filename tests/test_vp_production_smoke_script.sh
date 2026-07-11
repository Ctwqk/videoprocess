#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/run_vp_production_video_smoke.sh"

if [[ ! -f "$SCRIPT" ]]; then
  echo "FAIL: missing production smoke wrapper: $SCRIPT" >&2
  exit 1
fi

bash -n "$SCRIPT"
grep -Fq 'VP_PYTHON_API="${VP_PYTHON_API:-http://10.0.0.127:18080}"' "$SCRIPT"
grep -Fq 'VP_REDIS_URL="${VP_REDIS_URL:-redis://10.0.0.150:6380/0}"' "$SCRIPT"
grep -Fq 'VP_GO_WORKER_SMOKE_STRICT=1' "$SCRIPT"
grep -Fq '.runtime/video-smoke' "$SCRIPT"
if grep -Eiq 'youtube|bilibili|xiaohongshu|private_upload|public_after_review' "$SCRIPT"; then
  echo 'FAIL: production video smoke must not publish' >&2
  exit 1
fi
