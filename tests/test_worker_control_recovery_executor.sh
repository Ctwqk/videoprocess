#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$ROOT_DIR"
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

old_image=vp-ffmpeg-worker-python:deploy-8940cac83c0f
new_image=vp-ffmpeg-worker-python:deploy-123456789abc
generation=c-8940cac83c0f9dd374cb
VP_WORKER_ADMISSION_RECOVERY_DCL_IMAGE="$new_image"
VP_PIPELINE_NETWORK_ID=network123456789012345678
vp_require_pipeline_network_identity() { return 0; }
vp_python_worker_prepare_controlled_directory() { printf '%s\n' "$1"; }
vp_worker_database_dcl_file() { printf '/test/deploy-migrator.url\n'; }
vp_worker_admission_image_commit() {
  [[ "$1" == "$new_image" ]] || return 1
  printf '123456789abcdef0123456789abcdef0123456789\n'
}
vp_run_python_worker_container() {
  [[ "$1" == "$new_image" ]] || return 1
  [[ "$2" == /test/deploy-migrator.url ]] || return 1
  [[ "$*" == *"revoke --generation $generation --state-dir /control-state" ]]
}

vp_worker_control_revoke_authority "$old_image" "$generation" /test/state
if vp_worker_control_revoke_authority "$old_image" c-00000000000000000000 /test/state; then
  echo 'FAIL: a new executor bypassed the old generation identity check' >&2
  exit 1
fi
vp_worker_admission_image_commit() { return 1; }
if vp_worker_control_revoke_authority "$old_image" "$generation" /test/state; then
  echo 'FAIL: an unverified recovery executor was used' >&2
  exit 1
fi
printf 'worker control recovery executor tests passed\n'
