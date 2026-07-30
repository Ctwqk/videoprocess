#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow="$ROOT_DIR/.github/workflows/ci.yml"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "$workflow" ]] || fail "missing VideoProcess CI workflow"

required_lines=(
  "name: VideoProcess CI"
  'python-version: "3.12"'
  "go-version-file: go.mod"
  'node-version: "22"'
  "CHANNEL_OPS_POSTGRES_TEST_URL:"
  "CHANNEL_OPS_GO_POSTGRES_TEST_URL:"
  "CHANNEL_OPS_GO_REDIS_TEST_URL:"
  "uv sync --frozen --extra dev"
  ".venv/bin/alembic upgrade head"
  ".venv/bin/python -m pytest"
  'CHANNELOPS_REQUIRE_DATABASE="1"'
  'go test -count=1 ./internal/channelops ./internal/store'
  "name: Run Go worker registration fence integration tests"
  "go test -count=1 -v ./internal/worker"
  "Go worker registration integration tests skipped"
  "go test ./..."
  "npm run build"
  "bash tests/test_vp_deploy_sync_extension.sh"
  "bash tests/test_vp_deploy_ci_gate.sh"
  "bash tests/test_worker_admission_deploy.sh"
  "bash tests/test_worker_admission_rollback.sh"
  "bash tests/test_staging_object_janitor_install.sh"
  "bash tests/test_staging_object_janitor_run.sh"
  "bash tests/test_worker_redis_marker_control.sh"
  "actions/upload-artifact@v7"
)

for line in "${required_lines[@]}"; do
  grep -Fq -- "$line" "$workflow" || fail "workflow is missing contract: $line"
done

grep -Fq "name: Install deployment contract dependencies" "$workflow" \
  || fail "deployment contracts do not install the backend test environment"

grep -Eq '^  (backend|go|frontend|deploy-contracts):$' "$workflow" \
  || fail "workflow has no blocking jobs"

echo "VideoProcess CI workflow contract passed"
