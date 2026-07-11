#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMON="$ROOT_DIR/deploy/macos/common.sh"
TOPOLOGY="$ROOT_DIR/deploy/four-machine-topology.md"

bash -n "$COMMON"

assert_contains() {
  local needle="$1"
  if ! grep -Fq "$needle" "$COMMON"; then
    printf 'FAIL: expected macOS common.sh to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_not_contains() {
  local needle="$1"
  if grep -Fq "$needle" "$COMMON"; then
    printf 'FAIL: expected macOS common.sh not to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_file_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq -- "$needle" "$file"; then
    printf 'FAIL: expected %s to contain %s\n' "$file" "$needle" >&2
    exit 1
  fi
}

assert_contains 'CONSTRUCTURE_REPOS_DIR="${CONSTRUCTURE_REPOS_DIR:-$(cd "$VIDEO_PROCESS_ROOT/.." && pwd)}"'
assert_contains 'LEGACY_CONSTRUCTURE_ROOT="${LEGACY_CONSTRUCTURE_ROOT:-$HOME/Constructure}"'
assert_contains 'PLATFORM_UPLOAD_ROOT="${PLATFORM_UPLOAD_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-platform-upload}"'
assert_contains 'INFRA_ROOT="${INFRA_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-runtime/infra}"'
assert_contains 'MAIN_HOST="${MAIN_HOST:-10.0.0.150}"'
assert_contains 'MAC1_TARGET="${MAC1_TARGET:-wenjieliu@10.0.0.127}"'
assert_contains 'MAC3_TARGET="${MAC3_TARGET:-magi1@10.0.0.126}"'
assert_not_contains 'CONSTRUCTURE_ROOT="$(cd "$VIDEO_PROCESS_ROOT/../.." && pwd)"'
assert_not_contains 'PLATFORM_UPLOAD_ROOT="${PLATFORM_UPLOAD_ROOT:-$CONSTRUCTURE_ROOT/platform-upload}"'
assert_file_contains "$TOPOLOGY" 'node.labels.vp.runtime == true'
assert_file_contains "$TOPOLOGY" '126 is not a VideoProcess automatic failover target'
assert_file_contains "$TOPOLOGY" '--project vp-app --project vp-feature-aggregator'
