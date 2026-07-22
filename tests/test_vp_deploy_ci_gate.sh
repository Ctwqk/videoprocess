#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/vp-deploy-ci-gate.XXXXXX")"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

TRACE="$TEST_ROOT/trace"
SHA="0123456789abcdef0123456789abcdef01234567"
OTHER_SHA="89abcdef0123456789abcdef0123456789abcdef"
UPPER_SHA="$(printf '%s' "$SHA" | tr '[:lower:]' '[:upper:]')"
export TRACE SHA OTHER_SHA

mkdir -p "$TEST_ROOT/bin" "$TEST_ROOT/repos"
cat >"$TEST_ROOT/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh|%s\n' "$*" >>"$TRACE"
jq_filter=""
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "--jq" ]]; then
    shift
    jq_filter="${1:-}"
  fi
  shift
done
case "${GH_SCENARIO:-success}" in
  success)
    printf 'found\tcompleted\tsuccess\t%s\t101\n' "$SHA"
    ;;
  queued)
    printf 'found\tqueued\t\t%s\t102\n' "$SHA"
    ;;
  failed)
    printf 'found\tcompleted\tfailure\t%s\t103\n' "$SHA"
    ;;
  missing)
    printf 'missing\t\t\t\t\n'
    ;;
  mismatch)
    printf 'found\tcompleted\tsuccess\t%s\t104\n' "$OTHER_SHA"
    ;;
  latest_failed)
    if [[ "$jq_filter" == *'.run_number'* ]]; then
      printf 'found\tcompleted\tfailure\t%s\t106\n' "$SHA"
    else
      printf 'found\tcompleted\tsuccess\t%s\t105\n' "$SHA"
    fi
    ;;
  api_error)
    exit 1
    ;;
  *)
    exit 2
    ;;
esac
EOF
chmod +x "$TEST_ROOT/bin/gh"
PATH="$TEST_ROOT/bin:$PATH"
export PATH

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/controller"
VP_RUNTIME_HOST="10.0.0.127"
export REPO_ROOT ROOT VP_RUNTIME_HOST

log() {
  printf 'log|%s\n' "$*" >>"$TRACE"
}

image_tag() {
  printf '%s:deploy-%s\n' "$1" "${2:0:12}"
}

build_image_on_host() {
  printf 'build|remote|%s\n' "$*" >>"$TRACE"
}

source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

vp_build_manager_image() {
  printf 'build|manager|%s\n' "$*" >>"$TRACE"
}

fail() {
  echo "FAIL: $*" >&2
  if [[ -s "$TRACE" ]]; then
    cat "$TRACE" >&2
  fi
  exit 1
}

assert_gate_before_build() {
  local function_name="$1"
  : >"$TRACE"
  GH_SCENARIO=success "$function_name" "$SHA" >/dev/null \
    || fail "$function_name rejected successful exact-SHA CI"

  local gh_line
  local build_line
  gh_line="$(grep -n '^gh|' "$TRACE" | head -n 1 | cut -d: -f1)"
  build_line="$(grep -n '^build|' "$TRACE" | head -n 1 | cut -d: -f1)"
  [[ -n "$gh_line" && -n "$build_line" ]] \
    || fail "$function_name did not call both CI and build"
  [[ "$gh_line" -lt "$build_line" ]] \
    || fail "$function_name started a build before CI"
}

assert_rejected_without_build() {
  local scenario="$1"
  : >"$TRACE"
  if GH_SCENARIO="$scenario" build_vp_app_images "$SHA" >/dev/null 2>&1; then
    fail "vp-app accepted CI scenario $scenario"
  fi
  grep -q '^gh|' "$TRACE" || fail "scenario $scenario did not query CI"
  if grep -q '^build|' "$TRACE"; then
    fail "scenario $scenario reached a build"
  fi
}

declare -F vp_require_github_actions_success >/dev/null \
  || fail "deploy extension has no exact-SHA CI gate"

assert_gate_before_build build_vp_app_images
assert_gate_before_build build_feature_aggregator_images
assert_gate_before_build build_pds_images

for scenario in queued failed missing mismatch api_error latest_failed; do
  assert_rejected_without_build "$scenario"
done

invalid_inputs=(
  "bad_repository|ci.yml|$SHA"
  "Ctwqk/videoprocess|../ci.yml|$SHA"
  "Ctwqk/videoprocess|ci.yml|not-a-sha"
  "Ctwqk/videoprocess|ci.yml|$UPPER_SHA"
)
for invalid in "${invalid_inputs[@]}"; do
  IFS='|' read -r repository workflow commit <<<"$invalid"
  : >"$TRACE"
  if vp_require_github_actions_success "$repository" "$workflow" "$commit" >/dev/null 2>&1; then
    fail "accepted invalid CI gate input: $invalid"
  fi
  [[ ! -s "$TRACE" ]] || fail "invalid input called gh: $invalid"
done

: >"$TRACE"
BUILD_IMAGES=0 UPDATE_SERVICES=0 \
  vp_require_github_actions_success Ctwqk/videoprocess ci.yml "$SHA" \
  || fail "read-only dry run was CI-gated"
[[ ! -s "$TRACE" ]] || fail "read-only dry run called gh"

runbook="$ROOT_DIR/deploy/four-machine-topology.md"
grep -Fq '*/15 * * * * /home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --apply --project vp-app --project vp-feature-aggregator' "$runbook" \
  || fail "runbook is missing the independent VideoProcess cron"
grep -Fq '7-59/15 * * * * /home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --apply --project vp-pds' "$runbook" \
  || fail "runbook is missing the offset PDS cron"
if grep -Eq '^\*/15 .+--project vp-app --project vp-feature-aggregator --project vp-pds' "$runbook"; then
  fail "runbook still couples VideoProcess and PDS in one cron invocation"
fi

echo "VideoProcess exact-SHA deploy CI gate tests passed"
