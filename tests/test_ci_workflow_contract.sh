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
  "tests/migrations/test_worker_operator_creator_edges_postgres.py"
  "tests/migrations/test_worker_session_signal_postgres.py"
  "PG16 worker operator lifecycle tests skipped"
  'CHANNELOPS_REQUIRE_DATABASE="1"'
  'go test -count=1 ./internal/channelops ./internal/store'
  "name: Run Go worker registration fence integration tests"
  "go test -count=1 -v ./internal/worker ./cmd/vp-ffmpeg-worker"
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

deploy_checkout_block="$(
  awk '
    /^  deploy-contracts:$/ { in_deploy_job=1; next }
    in_deploy_job && /^  [A-Za-z0-9_-]+:$/ { exit }
    in_deploy_job && /- uses: actions\/checkout@v6/ { in_checkout=1 }
    in_checkout && /^      - / && ! /actions\/checkout@v6/ { exit }
    in_checkout { print }
  ' "$workflow"
)"
grep -Fq "uses: actions/checkout@v6" <<<"$deploy_checkout_block" \
  || fail "deployment contracts do not check out the repository"
grep -Fq "fetch-depth: 0" <<<"$deploy_checkout_block" \
  || fail "deployment contracts do not fetch legacy journal history"

grep -Eq '^  (backend|go|frontend|deploy-contracts):$' "$workflow" \
  || fail "workflow has no blocking jobs"

fast_contract_line="$(grep -nF 'bash tests/test_worker_admission_deploy.sh' "$workflow" | cut -d: -f1)"
full_contract_line="$(grep -nF 'bash tests/test_vp_deploy_sync_extension.sh' "$workflow" | cut -d: -f1)"
[[ "$fast_contract_line" -lt "$full_contract_line" ]] \
  || fail "short admission contracts must run before the full deployment suite"

signal_test_line="$(grep -nF 'tests/migrations/test_worker_session_signal_postgres.py' "$workflow" | cut -d: -f1)"
go_integration_line="$(grep -nF 'name: Run PostgreSQL ChannelOps integration tests' "$workflow" | cut -d: -f1)"
[[ "$signal_test_line" -lt "$go_integration_line" ]] \
  || fail "session signaling tests must run before shared-role integration suites"

echo "VideoProcess CI workflow contract passed"
