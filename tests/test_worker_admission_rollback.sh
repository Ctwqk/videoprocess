#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rc=$?; rm -rf "$TEST_ROOT"; exit "$rc"' EXIT

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
UPDATE_SERVICES=1
mkdir -p "$ROOT"
log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

CALLS="$TEST_ROOT/calls"
GENERATION_SEQUENCE="$TEST_ROOT/generation-sequence"
printf '700\n' >"$GENERATION_SEQUENCE"
: >"$CALLS"

VP_WORKER_ADMISSION_PREPARED=true
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa
VP_WORKER_CONTROL_GENERATION=c-aaaaaaaaaaaaaaaaaaaa

old_commit=1111111111111111111111111111111111111111
old_short="${old_commit:0:12}"
new_commit=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
VP_WORKER_CONTROL_PRIOR_GENERATION=c-${old_commit:0:20}
VP_WORKER_CONTROL_PRIOR_IMAGE=vp-ffmpeg-worker-python:deploy-$old_short
VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=prior-operator-secret
VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=prior-orchestrator-secret
VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=prior-staging-secret
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=prior-staging-minio-access
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=prior-staging-minio-secret
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=prior-worker-minio-access
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=prior-worker-minio-secret
snapshots="$(
  printf '%s|%s\n' \
    vp-ffmpeg-worker-go-swarm \
    "vp-ffmpeg-worker-go:deploy-$old_short" \
    "$VP_PYTHON_WORKER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    "$VP_VISION_WORKER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    "$VP_PUBLISHER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short"
)"
attempted_services="vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE"

for service in $attempted_services; do
  kind="$(vp_worker_admission_kind "$service")"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image="vp-ffmpeg-worker-go:deploy-$old_short"
      old_generation=101
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=102
      ;;
    "$VP_VISION_WORKER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=103
      ;;
    "$VP_PUBLISHER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=104
      ;;
  esac
  vp_worker_admission_write_manifest \
    "$ROOT/state/vp-worker-admission/current/$kind.conf" \
    "$service" "$old_commit" "$image" "$old_generation" \
    "old-$kind-db-$old_generation" \
    "old-$kind-admission-$old_generation"
done

VP_WORKER_ADMISSION_COMMIT="$new_commit"
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$new_commit"
VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$attempted_services"
failed_forward_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
failed_records="$(
  printf '%s|%s|%s|%s\n' \
    vp-ffmpeg-worker-go-swarm 201 failed-go-db-201 failed-go-admission-201 \
    "$VP_PYTHON_WORKER_SERVICE" 202 failed-ffmpeg-db-202 failed-ffmpeg-admission-202 \
    "$VP_VISION_WORKER_SERVICE" 203 failed-vision-db-203 failed-vision-admission-203 \
    "$VP_PUBLISHER_SERVICE" 204 failed-publisher-db-204 failed-publisher-admission-204
)"
while IFS='|' read -r service generation database_secret admission_secret; do
  kind="$(vp_worker_admission_kind "$service")"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image=vp-ffmpeg-worker-go:deploy-${new_commit:0:12}
      ;;
    *)
      image=vp-ffmpeg-worker-python:deploy-${new_commit:0:12}
      ;;
  esac
  vp_worker_admission_write_manifest \
    "$ROOT/state/vp-worker-admission/candidates/$failed_forward_namespace/$kind.conf" \
    "$service" "$new_commit" "$image" "$generation" \
    "$database_secret" "$admission_secret"
done <<<"$failed_records"

vp_worker_admission_new_generation() {
  local value
  value="$(<"$GENERATION_SEQUENCE")"
  printf '%s\n' "$((value + 1))" >"$GENERATION_SEQUENCE"
  printf '%s\n' "$value"
}

vp_worker_admission_image_commit() {
  [[ "$1" == "vp-ffmpeg-worker-python:deploy-$old_short" ]]
  printf '%s\n' "$old_commit"
}

vp_worker_admission_prepare_service() {
  local service="$1"
  local image="$2"
  local _control_image="$3"
  local commit="$4"
  local root="$5"
  local namespace="$6"
  local kind
  kind="$(vp_worker_admission_kind "$service")"
  local generation
  generation="$(vp_worker_admission_new_generation)"
  local database_secret="fresh-$kind-db-$generation"
  local admission_secret="fresh-$kind-admission-$generation"
  vp_worker_admission_write_manifest \
    "$root/candidates/$namespace/$kind.conf" \
    "$service" "$commit" "$image" "$generation" \
    "$database_secret" "$admission_secret"
  vp_worker_admission_set_candidate \
    "$service" "$generation" "$database_secret" "$admission_secret"
  printf 'prepare|%s|%s|%s|%s|%s\n' \
    "$service" "$commit" "$generation" "$namespace" \
    "$_control_image" >>"$CALLS"
}

vp_require_worker_redis_marker_status() {
  printf 'marker|status\n' >>"$CALLS"
}

vp_activate_worker_admission() {
  local service="$1"
  local contract
  contract="$(vp_worker_service_contract "$service")"
  printf 'activate|%s|%s|%s\n' \
    "$service" "$(cut -d'|' -f6 <<<"$contract")" \
    "$VP_WORKER_ADMISSION_COMMIT" >>"$CALLS"
}

vp_update_runtime_service() {
  printf 'restore|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_deploy_python_worker() {
  printf 'restore|%s|%s\n' "$VP_PYTHON_WORKER_SERVICE" "$1" >>"$CALLS"
}

vp_deploy_vision_worker() {
  printf 'restore|%s|%s\n' "$VP_VISION_WORKER_SERVICE" "$1" >>"$CALLS"
}

vp_deploy_publisher() {
  printf 'restore|%s|%s\n' "$VP_PUBLISHER_SERVICE" "$1" >>"$CALLS"
}

vp_install_staging_object_janitor() {
  printf 'janitor|install|%s\n' "$1" >>"$CALLS"
}

vp_run_staging_object_janitor_once() {
  printf 'janitor|ready\n' >>"$CALLS"
}

FAIL_READY_SERVICE="$VP_VISION_WORKER_SERVICE"
vp_require_worker_deployment_ready() {
  local service="$1"
  local contract
  contract="$(vp_worker_service_contract "$service")"
  printf 'ready|%s|%s\n' \
    "$service" "$(cut -d'|' -f6 <<<"$contract")" >>"$CALLS"
  [[ "$service" != "$FAIL_READY_SERVICE" ]]
}

vp_worker_admission_retire_generation() {
  printf 'retire|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"
}

if vp_restore_worker_admission_transaction \
  "$snapshots" "$attempted_services" "$failed_records"; then
  echo 'FAIL: rollback readiness failure unexpectedly committed' >&2
  exit 1
fi
first_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
if grep -Fq 'retire|' "$CALLS"; then
  echo 'FAIL: failed rollback retired a generation before convergence' >&2
  exit 1
fi
if [[ ! -d "$ROOT/state/vp-worker-admission/candidates/$first_namespace" ]]; then
  echo 'FAIL: failed rollback did not preserve its managed candidate state' >&2
  exit 1
fi

: >"$CALLS"
FAIL_READY_SERVICE=
vp_restore_worker_admission_transaction \
  "$snapshots" "$attempted_services" "$failed_records"
second_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
if [[ -z "$first_namespace" || -z "$second_namespace" \
  || "$first_namespace" == "$second_namespace" ]]; then
  echo 'FAIL: rollback attempt reused its prior credential namespace' >&2
  exit 1
fi
if [[ -e "$ROOT/state/vp-worker-admission/candidates/$first_namespace" ]]; then
  echo 'FAIL: converged rollback retained a superseded rollback namespace' >&2
  exit 1
fi
if [[ -e "$ROOT/state/vp-worker-admission/candidates/$failed_forward_namespace" ]]; then
  echo 'FAIL: converged rollback retained the failed forward namespace' >&2
  exit 1
fi

assert_order() {
  local first="$1"
  local second="$2"
  local first_line
  local second_line
  first_line="$(grep -nF "$first" "$CALLS" | head -1 | cut -d: -f1)"
  second_line="$(grep -nF "$second" "$CALLS" | head -1 | cut -d: -f1)"
  if [[ -z "$first_line" || -z "$second_line" \
    || "$first_line" -ge "$second_line" ]]; then
    echo "FAIL: rollback order is invalid: $first -> $second" >&2
    exit 1
  fi
}

for service in $attempted_services; do
  kind="$(vp_worker_admission_kind "$service")"
  current="$ROOT/state/vp-worker-admission/current/$kind.conf"
  vp_worker_admission_read_manifest "$current" "$service"
  if [[ "$VP_WORKER_MANIFEST_COMMIT" != "$old_commit" \
    || "$VP_WORKER_MANIFEST_GENERATION" =~ ^10[1-4]$ \
    || "$VP_WORKER_MANIFEST_GENERATION" =~ ^20[1-4]$ \
    || "$VP_WORKER_MANIFEST_DATABASE_SECRET" != fresh-* \
    || "$VP_WORKER_MANIFEST_ADMISSION_SECRET" != fresh-* ]]; then
    echo "FAIL: rollback did not commit fresh prior-image state: $service" >&2
    exit 1
  fi
  generation="$VP_WORKER_MANIFEST_GENERATION"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image="vp-ffmpeg-worker-go:deploy-$old_short"
      ;;
    *)
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      ;;
  esac
  assert_order "activate|$service|$generation|$old_commit" \
    "restore|$service|$image"
  assert_order "restore|$service|$image" "ready|$service|$generation"
  grep -Eq \
    "^prepare\\|$service\\|$old_commit\\|$generation\\|rollback-[^|]+\\|vp-ffmpeg-worker-python:deploy-$old_short$" \
    "$CALLS"
done

for generation in 201 202 203 204; do
  grep -Eq "^retire\\|[^|]+\\|$generation\\|" "$CALLS"
done
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: rollback referenced host 126' >&2
  exit 1
fi

: >"$CALLS"
VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=c-aaaaaaaaaaaaaaaaaaaa
VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa
VP_WORKER_CONTROL_GENERATION=c-11111111111111111111
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
vp_require_pipeline_network_identity() {
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
}
vp_require_worker_service_descriptor() {
  printf 'descriptor|%s\n' "$1" >>"$CALLS"
}
vp_require_staging_object_janitor_control() {
  printf 'janitor|converged\n' >>"$CALLS"
}
vp_worker_control_schedule_retirement() {
  printf 'control|journal|%s|%s\n' "$2" "$3" >>"$CALLS"
}
vp_worker_control_write_manifest() {
  printf 'control|current|%s|%s\n' "$2" "$3" >>"$CALLS"
}
vp_worker_control_process_retirements() {
  printf 'control|process|%s\n' "$2" >>"$CALLS"
}
vp_finalize_worker_control_rollback
assert_order 'janitor|converged' \
  'control|journal|vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa|c-aaaaaaaaaaaaaaaaaaaa'
assert_order \
  'control|journal|vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa|c-aaaaaaaaaaaaaaaaaaaa' \
  'control|current|c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111'
assert_order \
  'control|current|c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111' \
  'control|process|c-11111111111111111111'
[[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" ]]
[[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" ]]

echo 'worker admission rollback transaction tests passed'
