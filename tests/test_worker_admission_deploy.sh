#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
TEST_ROOT="$(cd "$TEST_ROOT" && pwd -P)"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
mkdir -p "$ROOT"
log() {
  :
}
source "$EXTENSION"
mkdir -p "$(vp_worker_admission_root)"
chmod 0700 "$(vp_worker_admission_root)"

if grep -Eq 'chown[[:space:]]+-R' "$EXTENSION"; then
  echo 'FAIL: Python worker one-shot recursively chowns caller state' >&2
  exit 1
fi

(
  helper_root="$TEST_ROOT/helper-bind-guards"
  ROOT="$helper_root/sync"
  REPO_ROOT="$helper_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  mkdir -p "$admission_root"
  chmod 0700 "$admission_root"
  secret="$helper_root/secret"
  printf '%s\n' 'guard-secret' >"$secret"
  chmod 0400 "$secret"
  unrelated="$helper_root/unrelated"
  mkdir -p "$unrelated"
  chmod 0700 "$unrelated"
  printf '%s\n' 'preserve' >"$unrelated/preserve"
  chmod 0600 "$unrelated/preserve"

  DOCKER_GUARD_CALLS="$helper_root/docker-calls"
  : >"$DOCKER_GUARD_CALLS"
  docker() {
    printf 'docker|%s\n' "$*" >>"$DOCKER_GUARD_CALLS"
  }

  symlink_source="$helper_root/symlink-state"
  ln -s "$unrelated" "$symlink_source"
  set +e
  vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$symlink_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  symlink_status=$?
  set -e
  if [[ "$symlink_status" -eq 0 ]]; then
    echo 'FAIL: symlink bind source was accepted' >&2
    exit 1
  fi
  if [[ "$(<"$unrelated/preserve")" != preserve ]]; then
    echo 'FAIL: symlink rejection changed unrelated content' >&2
    exit 1
  fi
  if [[ -s "$DOCKER_GUARD_CALLS" ]]; then
    echo 'FAIL: symlink bind source reached Docker' >&2
    exit 1
  fi
  if compgen -G "$admission_root/one-shot-runs/run.*" >/dev/null; then
    echo 'FAIL: symlink rejection retained staged one-shot state' >&2
    exit 1
  fi

  first_guard_source="$helper_root/first-guard-state"
  mkdir -p "$first_guard_source"
  chmod 0700 "$first_guard_source"
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state,/requests \
    --mount "type=bind,src=$first_guard_source,dst=/runtime-state" \
    --mount "type=bind,src=$symlink_source,dst=/requests" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: partially guarded bind set was accepted' >&2
    exit 1
  fi
  if compgen -G "$first_guard_source/.vp-python-worker-bind-*" \
    >/dev/null; then
    echo 'FAIL: partial bind validation retained an earlier sentinel' >&2
    exit 1
  fi
  if compgen -G "$admission_root/one-shot-runs/run.*" >/dev/null; then
    echo 'FAIL: partial bind validation retained staged one-shot state' >&2
    exit 1
  fi
  if [[ -s "$DOCKER_GUARD_CALLS" ]]; then
    echo 'FAIL: partially guarded bind set reached Docker' >&2
    exit 1
  fi

  hardlink_source="$helper_root/hardlink-state"
  mkdir -p "$hardlink_source"
  chmod 0700 "$hardlink_source"
  printf '%s\n' 'linked' >"$hardlink_source/state"
  chmod 0600 "$hardlink_source/state"
  ln "$hardlink_source/state" "$helper_root/state-hardlink"
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$hardlink_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: hardlink-bearing bind source was accepted' >&2
    exit 1
  fi
  [[ "$(<"$helper_root/state-hardlink")" == linked ]]
  [[ ! -s "$DOCKER_GUARD_CALLS" ]]

  race_source="$helper_root/race-state"
  displaced_source="$helper_root/race-state-displaced"
  mkdir -p "$race_source"
  chmod 0700 "$race_source"
  docker() {
    printf 'docker|%s\n' "$*" >>"$DOCKER_GUARD_CALLS"
    mv "$race_source" "$displaced_source"
    ln -s "$unrelated" "$race_source"
  }
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$race_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: bind source replacement was accepted' >&2
    exit 1
  fi
  [[ -L "$race_source" ]]
  [[ "$(<"$unrelated/preserve")" == preserve ]]
  if compgen -G "$unrelated/.vp-python-worker-bind-*" >/dev/null; then
    echo 'FAIL: bind replacement cleanup touched unrelated target' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/sentinel-tamper"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  secret="$probe_root/secret"
  printf '%s\n' 'sentinel-tamper-credential' >"$secret"
  chmod 0400 "$secret"

  docker() {
    cat >/dev/null
    local sentinel
    sentinel="$(compgen -G "$bind_source/.vp-python-worker-bind-*" | head -1)"
    [[ -n "$sentinel" ]]
    chmod 0600 "$sentinel"
    : >"$sentinel"
    chmod 0400 "$sentinel"
  }

  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: completed one-shot accepted a truncated bind sentinel' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/prepare-service-symlink"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  mkdir -p "$admission_root"
  chmod 0700 "$admission_root"
  unrelated="$probe_root/unrelated"
  mkdir -p "$unrelated"
  chmod 0755 "$unrelated"
  printf '%s\n' 'prepare-preserve' >"$unrelated/preserve"
  chmod 0600 "$unrelated/preserve"
  ln -s "$unrelated" "$admission_root/runtime"

  runtime_owner="$probe_root/runtime-owner"
  printf '%s\n' 'prepare-owner-credential' >"$runtime_owner"
  chmod 0400 "$runtime_owner"
  VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="$runtime_owner"
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  PREPARE_DOCKER_CALLS="$probe_root/docker-calls"
  : >"$PREPARE_DOCKER_CALLS"

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_read_manifest() {
    VP_WORKER_MANIFEST_COMMIT="$commit"
    VP_WORKER_MANIFEST_IMAGE="$image"
    VP_WORKER_MANIFEST_GENERATION=901
    VP_WORKER_MANIFEST_DATABASE_SECRET=prepare-db-901
    VP_WORKER_MANIFEST_ADMISSION_SECRET=prepare-admission-901
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$PREPARE_DOCKER_CALLS"
  }

  if vp_worker_admission_prepare_service \
    vp-ffmpeg-worker-go-swarm \
    vp-ffmpeg-worker-go:deploy-0123456789ab \
    vp-ffmpeg-worker-python:deploy-0123456789ab \
    0123456789abcdef0123456789abcdef01234567 \
    "$admission_root" \
    0123456789abcdef0123456789abcdef01234567 \
    >/dev/null 2>&1; then
    echo 'FAIL: prepare_service accepted a symlinked runtime source' >&2
    exit 1
  fi
  if [[ "$(vp_worker_redis_marker_file_mode "$unrelated")" != 755 \
    || "$(<"$unrelated/preserve")" != prepare-preserve ]]; then
    echo 'FAIL: prepare_service mutated a symlink target before guard' >&2
    exit 1
  fi
  if [[ -s "$PREPARE_DOCKER_CALLS" ]]; then
    echo 'FAIL: prepare_service symlink source reached Docker' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/marker-revoke-symlink"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  control_root="$ROOT/state/worker-redis-marker-control"
  mkdir -p "$admission_root" "$control_root"
  chmod 0700 "$admission_root" "$control_root"
  unrelated="$probe_root/unrelated"
  mkdir -p "$unrelated"
  chmod 0755 "$unrelated"
  printf '%s\n' 'revoke-preserve' >"$unrelated/preserve"
  chmod 0600 "$unrelated/preserve"
  ln -s "$unrelated" "$control_root/roles"

  marker_owner="$probe_root/marker-owner"
  printf '%s\n' 'marker-owner-credential' >"$marker_owner"
  chmod 0400 "$marker_owner"
  VP_WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE="$marker_owner"
  MARKER_REVOKE_DOCKER_CALLS="$probe_root/docker-calls"
  : >"$MARKER_REVOKE_DOCKER_CALLS"

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$MARKER_REVOKE_DOCKER_CALLS"
  }

  if vp_worker_redis_marker_revoke_roles \
    vp-ffmpeg-worker-python:deploy-0123456789ab \
    m-symlink-revoke-1780000000-0001 \
    "$control_root" >/dev/null 2>&1; then
    echo 'FAIL: marker revoke accepted a symlinked role source' >&2
    exit 1
  fi
  if [[ "$(vp_worker_redis_marker_file_mode "$unrelated")" != 755 \
    || "$(<"$unrelated/preserve")" != revoke-preserve ]]; then
    echo 'FAIL: marker revoke mutated a symlink target before guard' >&2
    exit 1
  fi
  if [[ -s "$MARKER_REVOKE_DOCKER_CALLS" ]]; then
    echo 'FAIL: marker revoke symlink source reached Docker' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-lock-symlink"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  unrelated_lock="$probe_root/unrelated-lock"
  docker_entered="$probe_root/docker-entered"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  printf 'preserve\n' >"$unrelated_lock"
  chmod 0600 "$unrelated_lock"
  ln -s "$unrelated_lock" "$admission_root/transaction.lock"
  docker() {
    : >"$docker_entered"
  }
  if vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: symlink transaction lock was accepted' >&2
    exit 1
  fi
  [[ -L "$admission_root/transaction.lock" ]]
  [[ "$(<"$unrelated_lock")" == preserve ]]
  if [[ -e "$docker_entered" ]] \
    || compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo 'FAIL: symlink transaction lock reached one-shot mutation' >&2
    exit 1
  fi
)

for signal_case in HUP:129 INT:130; do
(
  signal_name="${signal_case%%:*}"
  expected_status="${signal_case##*:}"
  probe_root="$TEST_ROOT/one-shot-caller-$signal_name"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  secret="$probe_root/secret"
  printf '%s\n' 'caller-signal-credential' >"$secret"
  chmod 0400 "$secret"
  caller_trap_ran="$probe_root/caller-trap-ran"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"
  export signal_target_pid

  docker() {
    cat >/dev/null
    kill "-$signal_name" "$signal_target_pid"
    return "$expected_status"
  }
  trap 'printf "caller\n" >"$caller_trap_ran"' HUP
  trap 'printf "caller\n" >"$caller_trap_ran"' INT
  trap 'printf "caller\n" >"$caller_trap_ran"' TERM
  caller_hup_trap="$(trap -p HUP)"
  caller_int_trap="$(trap -p INT)"
  caller_term_trap="$(trap -p TERM)"

  set +e
  vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e
  if [[ "$operation_status" -ne "$expected_status" ]]; then
    echo "FAIL: $signal_name returned $operation_status instead of $expected_status" >&2
    exit 1
  fi
  if [[ -e "$caller_trap_ran" \
    || "$(trap -p HUP)" != "$caller_hup_trap" \
    || "$(trap -p INT)" != "$caller_int_trap" \
    || "$(trap -p TERM)" != "$caller_term_trap" ]]; then
    echo "FAIL: $signal_name did not preserve caller traps" >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo "FAIL: $signal_name retained one-shot state" >&2
    exit 1
  fi
)
done

(
  probe_root="$TEST_ROOT/one-shot-overlap"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  first_entered="$probe_root/first-docker-entered"
  first_side_effect="$probe_root/first-side-effect"
  release_first="$probe_root/release-first"
  second_docker_entered="$probe_root/second-docker-entered"

  docker() {
    if mkdir "$probe_root/docker-owner" 2>/dev/null; then
      : >"$first_entered"
      : >"$first_side_effect"
      while [[ ! -e "$release_first" ]]; do
        sleep 0.05
      done
      return 0
    fi
    : >"$second_docker_entered"
    return 0
  }

  set +e
  vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1 &
  first_pid=$!
  set -e
  for _attempt in $(seq 1 100); do
    [[ -e "$first_entered" ]] && break
    sleep 0.05
  done
  if [[ ! -e "$first_entered" ]]; then
    : >"$release_first"
    wait "$first_pid" >/dev/null 2>&1 || true
    echo 'FAIL: overlap probe did not enter the first Docker operation' >&2
    exit 1
  fi

  first_record="$(
    find "$admission_root/one-shot-operations" -type d -name 'op.*' \
      -print -quit 2>/dev/null || true
  )"
  first_sentinel="$(
    find "$bind_source" -type f -name '.vp-python-worker-bind-*' \
      -print -quit 2>/dev/null || true
  )"
  if [[ -z "$first_record" || -z "$first_sentinel" ]]; then
    : >"$release_first"
    wait "$first_pid" >/dev/null 2>&1 || true
    echo 'FAIL: overlap fixture did not observe the first live operation' >&2
    exit 1
  fi

  set +e
  vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  second_status=$?
  set -e
  first_record_survived=0
  first_sentinel_survived=0
  [[ -f "$first_record/operation.json" ]] && first_record_survived=1
  [[ -f "$first_sentinel" ]] && first_sentinel_survived=1

  : >"$release_first"
  set +e
  wait "$first_pid" >/dev/null 2>&1
  first_status=$?
  set -e

  if [[ "$second_status" -eq 0 ]]; then
    echo 'FAIL: overlapping one-shot invocation did not fail or serialize' >&2
    exit 1
  fi
  if [[ -e "$second_docker_entered" ]]; then
    echo 'FAIL: overlapping one-shot invocation reached Docker' >&2
    exit 1
  fi
  if [[ "$first_record_survived" -ne 1 \
    || "$first_sentinel_survived" -ne 1 ]]; then
    echo 'FAIL: overlapping invocation removed the first live operation' >&2
    exit 1
  fi
  if [[ ! -e "$first_side_effect" || "$first_status" -ne 0 ]]; then
    echo 'FAIL: successful first side effect was reported as failed' >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo 'FAIL: overlap probe retained completed operation state' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/outer-lock-inherited-overlap"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  nested_status_file="$probe_root/nested-status"
  second_docker_entered="$probe_root/second-docker-entered"
  first_identity_survived="$probe_root/first-identity-survived"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"

  vp_validate_deploy_config() {
    vp_worker_admission_lock_assert
  }
  vp_worker_admission_prepare_transaction() {
    vp_worker_admission_lock_assert
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  }
  _vp_deploy_vp_app_services_locked() {
    vp_worker_admission_lock_assert
    vp_run_python_worker_container \
      synthetic-image - - /runtime-state \
      --mount "type=bind,src=$bind_source,dst=/runtime-state" \
      -- /bin/true
    [[ "$VP_WORKER_ADMISSION_LOCK_DEPTH" -eq 1 ]]
  }
  docker() {
    if [[ "${VP_INHERITED_LOCK_PROBE_NESTED:-false}" == true ]]; then
      : >"$second_docker_entered"
      return 0
    fi
    local first_record
    local first_sentinel
    first_record="$(
      find "$admission_root/one-shot-operations" \
        -type d -name 'op.*' -print -quit 2>/dev/null || true
    )"
    first_sentinel="$(
      find "$bind_source" \
        -type f -name '.vp-python-worker-bind-*' \
        -print -quit 2>/dev/null || true
    )"
    [[ -n "$first_record" && -n "$first_sentinel" ]] || return 91
    export VP_INHERITED_LOCK_PROBE_NESTED=true
    set +e
    vp_run_python_worker_container \
      synthetic-image - - /runtime-state \
      --mount "type=bind,src=$bind_source,dst=/runtime-state" \
      -- /bin/true >/dev/null 2>&1
    local nested_status=$?
    set -e
    printf '%s\n' "$nested_status" >"$nested_status_file"
    if [[ -f "$first_record/operation.json" \
      && -f "$first_sentinel" ]]; then
      : >"$first_identity_survived"
    fi
    return 0
  }

  set +e
  deploy_vp_app_services a b synthetic-image d e f \
    >/dev/null 2>&1
  outer_status=$?
  set -e
  if [[ ! -f "$nested_status_file" \
    || "$(<"$nested_status_file")" -eq 0 ]]; then
    echo 'FAIL: inherited child was accepted as the outer lock owner' >&2
    exit 1
  fi
  if [[ -e "$second_docker_entered" ]]; then
    echo 'FAIL: inherited child reached a second Docker mutation' >&2
    exit 1
  fi
  if [[ ! -e "$first_identity_survived" ]]; then
    echo 'FAIL: inherited child reconciled the first live operation' >&2
    exit 1
  fi
  if [[ "$outer_status" -ne 0 ]]; then
    echo 'FAIL: inherited overlap misreported the first operation' >&2
    exit 1
  fi
  if [[ "$VP_WORKER_ADMISSION_LOCK_HELD" != false \
    || "$VP_WORKER_ADMISSION_LOCK_DEPTH" -ne 0 ]]; then
    echo 'FAIL: outer deploy did not release its transaction lock' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-sigterm"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  secret="$probe_root/secret"
  printf '%s\n' 'term-probe-credential' >"$secret"
  chmod 0400 "$secret"
  entered="$probe_root/docker-entered"
  docker_terminated="$probe_root/docker-terminated"

  docker() {
    trap ': >"$docker_terminated"; exit 143' TERM
    : >"$entered"
    while true; do
      sleep 0.05
    done
  }

  set +e
  (
    vp_run_python_worker_container \
      synthetic-image "$secret" synthetic-secret /runtime-state \
      --mount "type=bind,src=$bind_source,dst=/runtime-state" \
      -- /bin/true
  ) >/dev/null 2>&1 &
  operation_pid=$!
  set -e
  for _attempt in $(seq 1 100); do
    [[ -e "$entered" ]] && break
    sleep 0.05
  done
  if [[ ! -e "$entered" ]]; then
    kill -KILL "$operation_pid" >/dev/null 2>&1 || true
    wait "$operation_pid" >/dev/null 2>&1 || true
    echo 'FAIL: SIGTERM probe did not enter Docker' >&2
    exit 1
  fi
  lock_path="$admission_root/transaction.lock"
  if [[ ! -f "$lock_path" || -L "$lock_path" \
    || "$(vp_worker_redis_marker_file_mode "$lock_path")" != 600 ]]; then
    kill -TERM "$operation_pid" >/dev/null 2>&1 || true
    wait "$operation_pid" >/dev/null 2>&1 || true
    echo 'FAIL: one-shot did not hold the exact regular mode-0600 transaction lock' >&2
    exit 1
  fi

  kill -TERM "$operation_pid"
  set +e
  wait "$operation_pid" >/dev/null 2>&1
  operation_status=$?
  set -e
  if [[ "$operation_status" -ne 143 ]]; then
    echo "FAIL: SIGTERM probe exited $operation_status instead of 143" >&2
    exit 1
  fi
  if [[ ! -e "$docker_terminated" ]]; then
    echo 'FAIL: SIGTERM did not reach the exact Docker boundary' >&2
    exit 1
  fi
  if grep -R -Fq 'term-probe-credential' "$ROOT" 2>/dev/null; then
    echo 'FAIL: SIGTERM retained credential material in host state' >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null; then
    echo 'FAIL: SIGTERM retained a bind sentinel after returning' >&2
    exit 1
  fi
  if compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo 'FAIL: SIGTERM retained an operation record after returning' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-caller-trap"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  secret="$probe_root/secret"
  printf '%s\n' 'caller-trap-credential' >"$secret"
  chmod 0400 "$secret"
  caller_trap_ran="$probe_root/caller-trap-ran"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"
  export signal_target_pid

  docker() {
    cat >/dev/null
    kill -TERM "$signal_target_pid"
    return 143
  }

  trap 'printf "caller\n" >"$caller_trap_ran"' HUP
  trap 'printf "caller\n" >"$caller_trap_ran"' INT
  trap 'printf "caller\n" >"$caller_trap_ran"' TERM
  caller_hup_trap="$(trap -p HUP)"
  caller_int_trap="$(trap -p INT)"
  caller_term_trap="$(trap -p TERM)"

  set +e
  vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e
  if [[ "$operation_status" -ne 143 ]]; then
    echo "FAIL: caller-trap TERM returned $operation_status instead of 143" >&2
    exit 1
  fi
  if [[ -e "$caller_trap_ran" ]]; then
    echo 'FAIL: one-shot TERM invoked the caller trap before restoring it' >&2
    exit 1
  fi
  if [[ "$(trap -p HUP)" != "$caller_hup_trap" \
    || "$(trap -p INT)" != "$caller_int_trap" \
    || "$(trap -p TERM)" != "$caller_term_trap" ]]; then
    echo 'FAIL: one-shot signal handling did not restore caller traps' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-post-spawn-term"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  secret="$probe_root/secret"
  signal_forwarded="$probe_root/signal-forwarded"
  signal_timed_out="$probe_root/signal-timed-out"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  printf '%s\n' 'post-spawn-credential' >"$secret"
  chmod 0400 "$secret"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"
  kill() {
    if [[ "${1:-}" == -TERM \
      && "${2:-}" != "$signal_target_pid" ]]; then
      : >"$signal_forwarded"
    fi
    builtin kill "$@"
  }

  docker() {
    trap 'exit 143' TERM
    for _attempt in $(seq 1 40); do
      sleep 0.05
    done
    : >"$signal_timed_out"
    return 97
  }

  set -T
  trap '
    if [[ "$BASH_COMMAND" == "VP_PYTHON_WORKER_ACTIVE_CHILD_PID=\$!" ]]; then
      trap - DEBUG
      builtin kill -TERM "$signal_target_pid"
    fi
  ' DEBUG
  set +e
  vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e
  trap - DEBUG
  set +T
  if [[ "$operation_status" -ne 143 ]]; then
    echo "FAIL: post-spawn TERM returned $operation_status instead of 143" >&2
    exit 1
  fi
  if [[ ! -e "$signal_forwarded" || -e "$signal_timed_out" ]]; then
    echo 'FAIL: post-spawn TERM was not forwarded after PID publication' >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo 'FAIL: post-spawn TERM retained one-shot identity state' >&2
    exit 1
  fi
)

for signal_case in HUP:129 INT:130 TERM:143; do
(
  signal_name="${signal_case%%:*}"
  expected_status="${signal_case##*:}"
  probe_root="$TEST_ROOT/outer-deploy-signal-$signal_name"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/control-state"
  caller_trap_ran="$probe_root/caller-trap-ran"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"
  export signal_target_pid

  docker() {
    kill "-$signal_name" "$signal_target_pid"
    return "$expected_status"
  }
  vp_validate_deploy_config() {
    vp_worker_admission_lock_assert
  }
  vp_worker_admission_prepare_transaction() {
    vp_worker_admission_lock_assert
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  }
  vp_worker_admission_prepare_control_roles() {
    vp_run_python_worker_container \
      synthetic-image - - /control-state \
      --mount "type=bind,src=$bind_source,dst=/control-state" \
      -- /bin/true >/dev/null || return 1
  }
  _vp_deploy_vp_app_services_locked() {
    vp_worker_admission_prepare_control_roles || return 1
  }

  trap 'printf "caller\n" >"$caller_trap_ran"' HUP
  trap 'printf "caller\n" >"$caller_trap_ran"' INT
  trap 'printf "caller\n" >"$caller_trap_ran"' TERM
  caller_hup_trap="$(trap -p HUP)"
  caller_int_trap="$(trap -p INT)"
  caller_term_trap="$(trap -p TERM)"

  set +e
  deploy_vp_app_services a b synthetic-image d e f \
    >/dev/null 2>&1
  deploy_status=$?
  set -e
  if [[ "$deploy_status" -ne "$expected_status" ]]; then
    echo "FAIL: outer $signal_name returned $deploy_status instead of $expected_status" >&2
    exit 1
  fi
  if [[ -e "$caller_trap_ran" \
    || "$(trap -p HUP)" != "$caller_hup_trap" \
    || "$(trap -p INT)" != "$caller_int_trap" \
    || "$(trap -p TERM)" != "$caller_term_trap" ]]; then
    echo "FAIL: outer $signal_name did not preserve caller traps" >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo "FAIL: outer $signal_name retained nested control-role state" >&2
    exit 1
  fi
)
done

VP_WORKER_ADMISSION_COMMIT=0123456789abcdef0123456789abcdef01234567
VP_WORKER_FFMPEG_GO_GENERATION=101
VP_WORKER_FFMPEG_GENERATION=102
VP_WORKER_VISION_GENERATION=103
VP_WORKER_YOUTUBE_PUBLISHER_GENERATION=104
VP_WORKER_FFMPEG_GO_DATABASE_SECRET=go-db-secret
VP_WORKER_FFMPEG_GO_ADMISSION_SECRET=go-admission-secret
VP_WORKER_FFMPEG_DATABASE_SECRET=ffmpeg-db-secret
VP_WORKER_FFMPEG_ADMISSION_SECRET=ffmpeg-admission-secret
VP_WORKER_VISION_DATABASE_SECRET=vision-db-secret
VP_WORKER_VISION_ADMISSION_SECRET=vision-admission-secret
VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET=publisher-db-secret
VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET=publisher-admission-secret
VP_WORKER_REDIS_FFMPEG_GO_SECRET=go-redis-secret
VP_WORKER_REDIS_FFMPEG_SECRET=ffmpeg-redis-secret
VP_WORKER_REDIS_VISION_SECRET=vision-redis-secret
VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=publisher-redis-secret
VP_WORKER_MINIO_ACCESS_SECRET=worker-minio-access-secret
VP_WORKER_MINIO_SECRET_SECRET=worker-minio-secret-secret

assert_worker_contract() {
  local service="$1"
  local image="$2"
  local generation="$3"
  local worker_type="$4"
  local host="$5"
  local capabilities="$6"
  local stream="$7"
  local group="$8"
  local expected_db_secret="$9"
  local expected_admission_secret="${10}"
  local expected_redis_secret="${11}"
  local env_file="$TEST_ROOT/$worker_type.env"
  local secret_file="$TEST_ROOT/$worker_type.secrets"

  vp_worker_service_registration_env "$service" "$image" >"$env_file"
  grep -Fxq "DEPLOY_MODE=production" "$env_file"
  grep -Fxq "WORKER_SERVICE_NAME=$service" "$env_file"
  grep -Fxq "WORKER_ADMISSION_GENERATION=$generation" "$env_file"
  grep -Fxq "WORKER_TYPE=$worker_type" "$env_file"
  grep -Fxq "WORKER_HOST=$host" "$env_file"
  grep -Fxq "WORKER_CAPABILITIES=$capabilities" "$env_file"
  grep -Fxq "WORKER_RELEASE_COMMIT=$VP_WORKER_ADMISSION_COMMIT" "$env_file"
  grep -Fxq "WORKER_IMAGE_IDENTITY=$image" "$env_file"
  grep -Fxq "WORKER_REDIS_STREAM=$stream" "$env_file"
  grep -Fxq "WORKER_REDIS_GROUP=$group" "$env_file"
  grep -Fxq "WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url" "$env_file"
  grep -Fxq "WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token" "$env_file"
  grep -Fxq "WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url" "$env_file"
  grep -Fxq "WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key" "$env_file"
  grep -Fxq "WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key" "$env_file"
  grep -Fxq "VP_REQUIRE_STAGING_JANITOR=true" "$env_file"
  if grep -Eq \
    '^(DATABASE_URL|REDIS_URL|WORKER_ADMISSION_TOKEN|MINIO_ACCESS_KEY|MINIO_SECRET_KEY)=' \
    "$env_file"; then
    echo "FAIL: $service exposes a credential in its environment" >&2
    exit 1
  fi

  vp_worker_service_secret_specs "$service" >"$secret_file"
  grep -Fxq \
    "source=$expected_db_secret,target=vp-worker-database-url,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$expected_admission_secret,target=vp-worker-admission-token,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$expected_redis_secret,target=vp-worker-redis-url,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$VP_WORKER_MINIO_ACCESS_SECRET,target=vp-worker-minio-access-key,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$VP_WORKER_MINIO_SECRET_SECRET,target=vp-worker-minio-secret-key,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  [[ "$(wc -l <"$secret_file" | tr -d ' ')" -eq 5 ]]
}

assert_worker_contract \
  vp-ffmpeg-worker-go-swarm \
  vp-ffmpeg-worker-go:deploy-0123456789ab \
  101 ffmpeg_go colima-127 media_cpu \
  vp:tasks:ffmpeg_go ffmpeg_go-workers \
  go-db-secret go-admission-secret go-redis-secret
assert_worker_contract \
  vp-ffmpeg-worker-gpu-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  102 ffmpeg 150-gpu media_gpu \
  vp:tasks:ffmpeg ffmpeg-workers \
  ffmpeg-db-secret ffmpeg-admission-secret ffmpeg-redis-secret
assert_worker_contract \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  103 vision 150-vision vision_gpu \
  vp:tasks:vision vision-workers \
  vision-db-secret vision-admission-secret vision-redis-secret
assert_worker_contract \
  vp-youtube-publisher-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  104 youtube_publisher 150-publisher youtube_publisher \
  vp:tasks:youtube_publisher youtube_publisher-workers \
  publisher-db-secret publisher-admission-secret publisher-redis-secret

(
  expected_service=vp-ffmpeg-worker-go-swarm
  expected_generation=901
  retirement_uuid=01234567-89ab-4def-8123-456789abcdef
  RETIREMENT_RESPONSE_SERVICE="$expected_service"
  RETIREMENT_RESPONSE_GENERATION="$expected_generation"
  RETIREMENT_OPERATOR_CALLS="$TEST_ROOT/retirement-response-operator-calls"
  : >"$RETIREMENT_OPERATOR_CALLS"
  VP_WORKER_ADMISSION_CONTROL_IMAGE=synthetic-control-image

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "$TEST_ROOT/retirement-response-read"
  }
  vp_run_python_worker_container() {
    printf '{"code":"worker_deployment_retirement_candidates","generation":%s,"registration_ids":["%s"],"service_name":"%s","status":"ok"}\n' \
      "$RETIREMENT_RESPONSE_GENERATION" \
      "$retirement_uuid" \
      "$RETIREMENT_RESPONSE_SERVICE"
  }
  vp_worker_admission_operator() {
    printf 'operator|%s\n' "$*" >>"$RETIREMENT_OPERATOR_CALLS"
  }

  RETIREMENT_RESPONSE_SERVICE=vp-vision-worker-swarm
  set +e
  mismatch_output="$(
    vp_worker_admission_retirement_ids \
      "$expected_service" "$expected_generation"
  )"
  mismatch_status=$?
  set -e
  if [[ "$mismatch_status" -eq 0 || -n "$mismatch_output" ]]; then
    echo 'FAIL: retirement response accepted a mismatched service' >&2
    exit 1
  fi

  RETIREMENT_RESPONSE_SERVICE="$expected_service"
  RETIREMENT_RESPONSE_GENERATION=999
  set +e
  mismatch_output="$(
    vp_worker_admission_retirement_ids \
      "$expected_service" "$expected_generation"
  )"
  mismatch_status=$?
  set -e
  if [[ "$mismatch_status" -eq 0 || -n "$mismatch_output" ]]; then
    echo 'FAIL: retirement response accepted a mismatched generation' >&2
    exit 1
  fi
  [[ ! -s "$RETIREMENT_OPERATOR_CALLS" ]]

  RETIREMENT_RESPONSE_GENERATION="$expected_generation"
  exact_output="$(
    vp_worker_admission_retirement_ids \
      "$expected_service" "$expected_generation"
  )"
  [[ "$exact_output" == "$retirement_uuid" ]]
)

(
  immutable_calls="$TEST_ROOT/immutable-secret-calls"
  : >"$immutable_calls"
  old_id=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  replacement_id=1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  expected_name=vp-worker-secret-901
  expected_service=vp-ffmpeg-worker-go-swarm
  expected_generation=901
  expected_purpose=database
  SECRET_INSPECT_MODE=match

  docker() {
    printf 'docker|%s\n' "$*" >>"$immutable_calls"
    if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
      case "$SECRET_INSPECT_MODE|${3:-}" in
        "match|$old_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$old_id" "$expected_name" "$expected_service" \
            "$expected_generation" "$expected_purpose"
          return 0
          ;;
        "mismatch|$old_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$old_id" "$expected_name" unrelated-service \
            "$expected_generation" "$expected_purpose"
          return 0
          ;;
        "replacement|$old_id")
          return 1
          ;;
        "replacement|$expected_name")
          printf '%s|%s|%s|%s|%s\n' \
            "$replacement_id" "$expected_name" "$expected_service" \
            "$expected_generation" "$expected_purpose"
          return 0
          ;;
      esac
      return 1
    fi
    if [[ "${1:-} ${2:-}" == "secret rm" ]]; then
      return 0
    fi
    return 97
  }

  if ! vp_remove_managed_secret \
    "$old_id" "$expected_name" "$expected_service" \
    "$expected_generation" "$expected_purpose" >/dev/null 2>&1; then
    echo 'FAIL: immutable managed-secret removal helper is unavailable' >&2
    exit 1
  fi
  if [[ "$(<"$immutable_calls")" != \
      $'docker|secret inspect '"$old_id"$' --format {{.ID}}|{{.Spec.Name}}|{{index .Spec.Labels "vp.service"}}|{{index .Spec.Labels "vp.generation"}}|{{index .Spec.Labels "vp.purpose"}}\n''docker|secret rm '"$old_id" ]]; then
    echo 'FAIL: managed-secret removal was not adjacent inspect-ID then rm-ID' >&2
    exit 1
  fi

  : >"$immutable_calls"
  SECRET_INSPECT_MODE=mismatch
  if vp_remove_managed_secret \
    "$old_id" "$expected_name" "$expected_service" \
    "$expected_generation" "$expected_purpose" >/dev/null 2>&1; then
    echo 'FAIL: managed-secret removal accepted wrong labels' >&2
    exit 1
  fi
  if grep -Fq 'docker|secret rm' "$immutable_calls"; then
    echo 'FAIL: wrong-label secret reached destructive removal' >&2
    exit 1
  fi

  : >"$immutable_calls"
  SECRET_INSPECT_MODE=replacement
  if vp_remove_managed_secret \
    "$old_id" "$expected_name" "$expected_service" \
    "$expected_generation" "$expected_purpose" >/dev/null 2>&1; then
    echo 'FAIL: missing immutable secret ID was treated as replacement' >&2
    exit 1
  fi
  if grep -Fq 'docker|secret rm' "$immutable_calls" \
    || grep -Fq "docker|secret inspect $expected_name" "$immutable_calls"; then
    echo 'FAIL: name reuse caused replacement lookup or removal' >&2
    exit 1
  fi
)

(
  hydration_root="$TEST_ROOT/worker-v1-hydration"
  hydration_manifest="$hydration_root/ffmpeg-go.conf"
  hydration_service=vp-ffmpeg-worker-go-swarm
  hydration_generation=901
  hydration_commit=0123456789abcdef0123456789abcdef01234567
  hydration_image=vp-ffmpeg-worker-go:deploy-0123456789ab
  hydration_database_name=vp-worker-db-901
  hydration_admission_name=vp-worker-admission-901
  hydration_database_id=3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  hydration_admission_id=4123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  HYDRATION_LABEL_MODE=match

  vp_worker_admission_write_manifest \
    "$hydration_manifest" \
    "$hydration_service" \
    "$hydration_commit" \
    "$hydration_image" \
    "$hydration_generation" \
    "$hydration_database_name" \
    "$hydration_admission_name"
  docker() {
    [[ "${1:-} ${2:-}" == "secret inspect" ]] || return 97
    case "${3:-}" in
      "$hydration_database_name")
        printf '%s|%s|%s|%s|%s\n' \
          "$hydration_database_id" "$hydration_database_name" \
          "$hydration_service" "$hydration_generation" database
        ;;
      "$hydration_admission_name")
        local purpose=admission
        if [[ "$HYDRATION_LABEL_MODE" == mismatch ]]; then
          purpose=database
        fi
        printf '%s|%s|%s|%s|%s\n' \
          "$hydration_admission_id" "$hydration_admission_name" \
          "$hydration_service" "$hydration_generation" "$purpose"
        ;;
      *)
        return 1
        ;;
    esac
  }
  vp_worker_admission_require_v2_manifest \
    "$hydration_manifest" "$hydration_service"
  vp_worker_admission_read_manifest \
    "$hydration_manifest" "$hydration_service"
  [[ "$VP_WORKER_MANIFEST_VERSION" == 2 \
    && "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
      == "$hydration_database_id" \
    && "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" \
      == "$hydration_admission_id" ]]

  mismatch_manifest="$hydration_root/mismatch.conf"
  vp_worker_admission_write_manifest \
    "$mismatch_manifest" \
    "$hydration_service" \
    "$hydration_commit" \
    "$hydration_image" \
    "$hydration_generation" \
    "$hydration_database_name" \
    "$hydration_admission_name"
  HYDRATION_LABEL_MODE=mismatch
  if vp_worker_admission_require_v2_manifest \
    "$mismatch_manifest" "$hydration_service" >/dev/null 2>&1; then
    echo 'FAIL: v1 manifest hydrated a wrong-purpose secret' >&2
    exit 1
  fi
  grep -Fxq 'VERSION=1' "$mismatch_manifest"
  if grep -Fq '_SECRET_ID=' "$mismatch_manifest"; then
    echo 'FAIL: failed v1 hydration rewrote immutable identity evidence' >&2
    exit 1
  fi
)

(
  retirement_service=vp-ffmpeg-worker-go-swarm
  retirement_generation=811
  retirement_database_name=stale-db-811
  retirement_admission_name=stale-admission-811
  retirement_database_id=5123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  retirement_admission_id=6123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  RETIREMENT_HYDRATION_MODE=match
  RETIREMENT_HYDRATION_CALLS="$TEST_ROOT/v1-retirement-hydration-calls"
  RETIREMENT_HYDRATION_JOURNAL=""
  : >"$RETIREMENT_HYDRATION_CALLS"

  docker() {
    printf '%s|%s|%s\n' \
      "${1:-}" "${2:-}" "${3:-}" >>"$RETIREMENT_HYDRATION_CALLS"
    if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
      case "${3:-}" in
        "$retirement_database_name")
          local service="$retirement_service"
          local generation="$retirement_generation"
          if [[ "$RETIREMENT_HYDRATION_MODE" == wrong-label ]]; then
            service=unrelated-service
          elif [[ "$RETIREMENT_HYDRATION_MODE" == replacement ]]; then
            generation=999
          fi
          printf '%s|%s|%s|%s|%s\n' \
            "$retirement_database_id" "$retirement_database_name" \
            "$service" "$generation" database
          ;;
        "$retirement_admission_name")
          local admission_id="$retirement_admission_id"
          if [[ "$RETIREMENT_HYDRATION_MODE" == duplicate-id ]]; then
            admission_id="$retirement_database_id"
          fi
          printf '%s|%s|%s|%s|%s\n' \
            "$admission_id" "$retirement_admission_name" \
            "$retirement_service" "$retirement_generation" admission
          ;;
        "$retirement_database_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$retirement_database_id" "$retirement_database_name" \
            "$retirement_service" "$retirement_generation" database
          ;;
        "$retirement_admission_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$retirement_admission_id" "$retirement_admission_name" \
            "$retirement_service" "$retirement_generation" admission
          ;;
        *)
          return 1
          ;;
      esac
      return 0
    fi
    [[ "${1:-} ${2:-}" == "secret rm" ]] || return 97
  }
  vp_worker_admission_retire_generation() {
    local expected_records="$retirement_service|$retirement_generation|$retirement_database_name|$retirement_database_id|$retirement_admission_name|$retirement_admission_id"
    [[ -f "$RETIREMENT_HYDRATION_JOURNAL" \
      && "$(<"$RETIREMENT_HYDRATION_JOURNAL")" == "$expected_records" ]] \
      || return 1
    vp_remove_managed_secret \
      "$4" "$3" "$1" "$2" database || return 1
    vp_remove_managed_secret \
      "$6" "$5" "$1" "$2" admission
  }

  for RETIREMENT_HYDRATION_MODE in \
    wrong-label replacement duplicate-id; do
    failure_root="$TEST_ROOT/v1-retirement-$RETIREMENT_HYDRATION_MODE"
    failure_journal="$failure_root/retirements/legacy.records"
    legacy_record="$retirement_service|$retirement_generation|$retirement_database_name|$retirement_admission_name"
    vp_worker_admission_write_retirement_journal \
      "$failure_journal" "$legacy_record"
    before_failure_journal="$(shasum -a 256 "$failure_journal")"
    : >"$RETIREMENT_HYDRATION_CALLS"
    RETIREMENT_HYDRATION_JOURNAL="$failure_journal"
    if vp_worker_admission_process_retirement_journals \
      "$failure_root" >/dev/null 2>&1; then
      echo "FAIL: v1 retirement accepted $RETIREMENT_HYDRATION_MODE identity" >&2
      exit 1
    fi
    [[ "$(shasum -a 256 "$failure_journal")" == \
      "$before_failure_journal" ]]
    if grep -Fq 'secret|rm|' "$RETIREMENT_HYDRATION_CALLS"; then
      echo 'FAIL: invalid v1 retirement reached secret removal' >&2
      exit 1
    fi
    [[ "$(grep -Fc "secret|inspect|$retirement_database_name" \
      "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
    if [[ "$RETIREMENT_HYDRATION_MODE" == duplicate-id ]]; then
      [[ "$(grep -Fc "secret|inspect|$retirement_admission_name" \
        "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
    else
      [[ "$(grep -Fc "secret|inspect|$retirement_admission_name" \
        "$RETIREMENT_HYDRATION_CALLS")" -eq 0 ]]
    fi
  done

  valid_root="$TEST_ROOT/v1-retirement-valid"
  valid_journal="$valid_root/retirements/legacy.records"
  legacy_record="$retirement_service|$retirement_generation|$retirement_database_name|$retirement_admission_name"
  vp_worker_admission_write_retirement_journal \
    "$valid_journal" "$legacy_record"
  RETIREMENT_HYDRATION_MODE=match
  RETIREMENT_HYDRATION_JOURNAL="$valid_journal"
  : >"$RETIREMENT_HYDRATION_CALLS"
  if ! vp_worker_admission_process_retirement_journals "$valid_root"; then
    echo 'FAIL: valid v1 retirement journal did not recover' >&2
    exit 1
  fi
  [[ ! -e "$valid_journal" ]]
  [[ "$(grep -Fc "secret|inspect|$retirement_database_name" \
    "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
  [[ "$(grep -Fc "secret|inspect|$retirement_admission_name" \
    "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
  [[ "$(grep -Fc "secret|rm|$retirement_database_id" \
    "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
  [[ "$(grep -Fc "secret|rm|$retirement_admission_id" \
    "$RETIREMENT_HYDRATION_CALLS")" -eq 1 ]]
)

DOCKER_SECRET_PAYLOAD="$TEST_ROOT/docker-secret-payload"
DOCKER_SECRET_CREATED="$TEST_ROOT/docker-secret-created"
DOCKER_SECRET_ID=2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
docker() {
  if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
    if [[ -e "$DOCKER_SECRET_CREATED" ]]; then
      printf '%s|%s|%s|%s|%s\n' \
        "$DOCKER_SECRET_ID" test-secret test-service 105 database
      return 0
    fi
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "secret create" ]]; then
    cat >"$DOCKER_SECRET_PAYLOAD"
    : >"$DOCKER_SECRET_CREATED"
    printf '%s\n' "$DOCKER_SECRET_ID"
    return 0
  fi
  return 90
}

credential_file="$TEST_ROOT/credential"
printf '%s\n' "credential-material" >"$credential_file"
chmod 0600 "$credential_file"
if vp_worker_admission_create_secret \
  test-secret "$credential_file" test-service 105 database \
  >/dev/null 2>&1; then
  echo 'FAIL: mode-0600 credential input was accepted' >&2
  exit 1
fi
chmod 0400 "$credential_file"
vp_worker_admission_create_secret \
  test-secret "$credential_file" test-service 105 database
grep -Fxq "credential-material" "$DOCKER_SECRET_PAYLOAD"

(
  partial_root="$TEST_ROOT/partial-secret-journal"
  ROOT="$partial_root/sync"
  REPO_ROOT="$partial_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  secret_state="$partial_root/secrets"
  secret_calls="$partial_root/secret-calls"
  fail_second="$partial_root/fail-second"
  mkdir -p "$admission_root" "$secret_state"
  chmod 0700 "$admission_root"
  : >"$secret_calls"
  : >"$fail_second"

  partial_credentials=()
  partial_principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for purpose in \
    deploy-migrator deploy-read control-owner runtime-owner; do
    credential="$partial_root/$purpose"
    printf 'postgresql://identity:credential@database/videoprocess\n' \
      >"$credential"
    chmod 0400 "$credential"
    partial_credentials+=("$credential")
  done
  credential_records="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      validate-credentials \
      "${partial_credentials[0]}" "${partial_principals[0]}" \
      "${partial_credentials[1]}" "${partial_principals[1]}" \
      "${partial_credentials[2]}" "${partial_principals[2]}" \
      "${partial_credentials[3]}" "${partial_principals[3]}"
  )"
  vp_worker_admission_lock_acquire "$admission_root"
  partial_commit=0123456789abcdef0123456789abcdef01234567
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$partial_commit" \
    vp-backend:deploy-0123456789ab \
    vp-ffmpeg-worker-go:deploy-0123456789ab \
    "$partial_commit" legacy_no_control \
    <<<"$credential_records" >/dev/null
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true

  first_secret=vp-wr-ffmpeg-go-db-901
  first_secret_id=1111111111111111111111111111111111111111111111111111111111111111
  second_secret=vp-wr-ffmpeg-go-admission-901
  second_secret_id=2222222222222222222222222222222222222222222222222222222222222222
  first_payload="$partial_root/first-payload"
  second_payload="$partial_root/second-payload"
  printf 'first-secret-material\n' >"$first_payload"
  printf 'second-secret-material\n' >"$second_payload"
  chmod 0400 "$first_payload" "$second_payload"

  docker() {
    if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
      local reference="${3:-}"
      printf 'inspect|%s\n' "$reference" >>"$secret_calls"
      local state
      for state in "$secret_state"/*; do
        [[ -f "$state" ]] || continue
        local saved_id
        local saved_name
        local saved_service
        local saved_generation
        local saved_purpose
        IFS='|' read -r \
          saved_id saved_name saved_service \
          saved_generation saved_purpose <"$state"
        if [[ "$reference" == "$saved_id" \
          || "$reference" == "$saved_name" ]]; then
          printf '%s|%s|%s|%s|%s\n' \
            "$saved_id" "$saved_name" "$saved_service" \
            "$saved_generation" "$saved_purpose"
          return 0
        fi
      done
      return 1
    fi
    if [[ "${1:-} ${2:-}" == "secret create" ]]; then
      shift 2
      local service=""
      local generation=""
      local purpose=""
      while [[ "$#" -gt 0 && "$1" == --label ]]; do
        case "$2" in
          vp.service=*) service="${2#*=}" ;;
          vp.generation=*) generation="${2#*=}" ;;
          vp.purpose=*) purpose="${2#*=}" ;;
          *) return 1 ;;
        esac
        shift 2
      done
      [[ "$#" -eq 2 && "$2" == - \
        && -n "$service" && -n "$generation" && -n "$purpose" ]] \
        || return 1
      local name="$1"
      cat >/dev/null
      printf 'create|%s\n' "$name" >>"$secret_calls"
      if [[ "$name" == "$second_secret" && -e "$fail_second" ]]; then
        return 1
      fi
      local secret_id="$first_secret_id"
      if [[ "$name" == "$second_secret" ]]; then
        secret_id="$second_secret_id"
      fi
      printf '%s|%s|%s|%s|%s\n' \
        "$secret_id" "$name" "$service" "$generation" "$purpose" \
        >"$secret_state/$name"
      printf '%s\n' "$secret_id"
      return 0
    fi
    return 97
  }

  vp_worker_admission_create_secret \
    "$first_secret" "$first_payload" \
    vp-ffmpeg-worker-go-swarm 901 database
  if vp_worker_admission_create_secret \
    "$second_secret" "$second_payload" \
    vp-ffmpeg-worker-go-swarm 901 admission \
    >/dev/null 2>&1; then
    echo 'FAIL: partial secret fixture did not fail its second create' >&2
    exit 1
  fi
  active="$admission_root/transactions/active.json"
  if ! python3 - "$active" "$first_secret" "$first_secret_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    transaction = json.load(handle)
if transaction["prepared_secrets"] != [
    {
        "docker_secret_id": sys.argv[3],
        "generation": "901",
        "name": sys.argv[2],
        "purpose": "database",
        "service": "vp-ffmpeg-worker-go-swarm",
    }
]:
    raise SystemExit(1)
PY
  then
    echo 'FAIL: first immutable secret ID was not durably journaled' >&2
    exit 1
  fi

  : >"$secret_calls"
  vp_worker_admission_create_secret \
    "$first_secret" "$first_payload" \
    vp-ffmpeg-worker-go-swarm 901 database
  grep -Fxq "inspect|$first_secret_id" "$secret_calls"
  if grep -Fxq "inspect|$first_secret" "$secret_calls" \
    || grep -Fxq "create|$first_secret" "$secret_calls"; then
    echo 'FAIL: partial secret resume guessed by name or minted again' >&2
    exit 1
  fi

  rm -f "$fail_second"
  vp_worker_admission_create_secret \
    "$second_secret" "$second_payload" \
    vp-ffmpeg-worker-go-swarm 901 admission
  python3 - "$active" "$first_secret_id" "$second_secret_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    transaction = json.load(handle)
ids = {
    secret["docker_secret_id"]
    for secret in transaction["prepared_secrets"]
}
if ids != {sys.argv[2], sys.argv[3]}:
    raise SystemExit("partial secret prefix did not resume")
PY
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  vp_worker_admission_lock_release
)

(
  rollback_calls="$TEST_ROOT/partial-capture-rollback-calls"
  : >"$rollback_calls"
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=vp-ffmpeg-worker-go-swarm
  VP_APP_ATTEMPTED_SERVICES=vp-ffmpeg-worker-go-swarm
  vp_vision_cutover_required() {
    printf 'false\n'
  }
  vp_capture_app_snapshots() {
    printf 'vp-ffmpeg-worker-go-swarm|baseline-image\n'
  }
  vp_apply_app_services() {
    return 1
  }
  vp_worker_admission_candidate_records() {
    return 1
  }
  vp_restore_worker_admission_transaction() {
    printf 'restore|%s|%s|%s|%s\n' "$1" "$2" "$3" "${4:-}" \
      >>"$rollback_calls"
  }
  vp_restore_worker_redis_marker_controls() {
    printf 'marker-restore\n' >>"$rollback_calls"
  }
  vp_finalize_worker_control_rollback() {
    printf 'control-finalize\n' >>"$rollback_calls"
  }

  if _vp_deploy_vp_app_services_locked a b c d e f \
    >/dev/null 2>&1; then
    echo 'FAIL: partial capture fixture unexpectedly deployed' >&2
    exit 1
  fi
  if ! grep -Fq 'restore|' "$rollback_calls"; then
    echo 'FAIL: candidate capture failure short-circuited rollback' >&2
    exit 1
  fi
  if ! grep -Eq '\|true$' "$rollback_calls"; then
    echo 'FAIL: incomplete candidate rollback was not evidence-preserving' >&2
    exit 1
  fi
  if grep -Eq '^(marker-restore|control-finalize)$' "$rollback_calls"; then
    echo 'FAIL: incomplete candidate evidence reached final retirement' >&2
    exit 1
  fi
)

generated_credential="$TEST_ROOT/generated-credential"
vp_worker_admission_write_secret_file \
  "$generated_credential" "generated-material"
[[ "$(vp_worker_redis_marker_file_mode "$generated_credential")" == 400 ]]

read_url_file="$TEST_ROOT/deploy-read-url"
printf '%s\n' \
  'postgresql://deploy_read:credential@vp-postgres/videoprocess' \
  >"$read_url_file"
chmod 0400 "$read_url_file"
VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$read_url_file"
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
VP_WORKER_ADMISSION_PREPARED=true
SERVICE_SPEC="$TEST_ROOT/service-spec.json"
cat >"$SERVICE_SPEC" <<EOF
{
  "Mode": {"Replicated": {"Replicas": 1}},
  "TaskTemplate": {
    "ContainerSpec": {
      "Image": "vp-ffmpeg-worker-python:deploy-0123456789ab",
      "Env": [
        "DEPLOY_MODE=production",
        "WORKER_SERVICE_NAME=vp-vision-worker-swarm",
        "WORKER_ADMISSION_GENERATION=103",
        "WORKER_SLOT=1",
        "WORKER_TYPE=vision",
        "WORKER_HOST=150-vision",
        "WORKER_CAPABILITIES=vision_gpu",
        "WORKER_RELEASE_COMMIT=$VP_WORKER_ADMISSION_COMMIT",
        "WORKER_IMAGE_IDENTITY=vp-ffmpeg-worker-python:deploy-0123456789ab",
        "WORKER_REDIS_STREAM=vp:tasks:vision",
        "WORKER_REDIS_GROUP=vision-workers",
        "WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url",
        "WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token",
        "WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url",
        "WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key",
        "WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key",
        "VP_REQUIRE_STAGING_JANITOR=true"
      ],
      "Secrets": [
        {"SecretName": "vision-db-secret", "File": {"Name": "vp-worker-database-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "vision-admission-secret", "File": {"Name": "vp-worker-admission-token", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "vision-redis-secret", "File": {"Name": "vp-worker-redis-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "worker-minio-access-secret", "File": {"Name": "vp-worker-minio-access-key", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "worker-minio-secret-secret", "File": {"Name": "vp-worker-minio-secret-key", "UID": "10001", "GID": "10001", "Mode": 256}}
      ]
    },
    "Placement": {
      "Constraints": [
        "node.labels.vp.gpu==true",
        "node.hostname==ccttww-lap"
      ]
    },
    "Networks": [{"Target": "vp-pipeline-network-id"}]
  }
}
EOF
cp "$SERVICE_SPEC" "$TEST_ROOT/valid-worker-service-spec.json"
READINESS_ATTEMPTS="$TEST_ROOT/readiness-attempts"
printf '0\n' >"$READINESS_ATTEMPTS"
docker() {
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    printf '%s\n' \
      "${NETWORK_IDENTITY_OUTPUT:-vp-pipeline-network-id|vp-pipeline-net|overlay|swarm}"
    return
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" \
    && "$*" == *"{{json .Spec}}"* ]]; then
    cat "$SERVICE_SPEC"
    return
  fi
  if [[ "${1:-} ${2:-}" == "service ls" ]]; then
    printf '%s\n' "${CONTROL_SERVICE_NAMES:-}"
    return
  fi
  if [[ "${1:-}" == run \
    && "$*" == *"worker_deployment_cli readiness"* ]]; then
    cat >/dev/null
    local attempts
    attempts="$(<"$READINESS_ATTEMPTS")"
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" >"$READINESS_ATTEMPTS"
    [[ "$attempts" -ge 3 ]]
    return
  fi
  return 90
}
sleep() {
  [[ "$1" == 2 ]]
}
VP_WORKER_DEPLOY_READINESS_ATTEMPTS=3
VP_WORKER_DEPLOY_READINESS_INTERVAL_SECONDS=2
vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab
vp_require_worker_deployment_ready vp-vision-worker-swarm
[[ "$(<"$READINESS_ATTEMPTS")" == 3 ]]

python3 - \
  "$TEST_ROOT/valid-worker-service-spec.json" \
  "$SERVICE_SPEC" <<'PY'
import json
import sys

source, target = sys.argv[1:]
with open(source, encoding="utf-8") as handle:
    spec = json.load(handle)
spec["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["File"]["UID"] = "0"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(spec, handle)
PY
if vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: worker descriptor accepted a root-owned runtime secret' >&2
  exit 1
fi

sed 's/"vision-redis-secret"/"worker-deploy-read-secret"/' \
  "$TEST_ROOT/valid-worker-service-spec.json" \
  >"$TEST_ROOT/bad-service-spec.json"
mv "$TEST_ROOT/bad-service-spec.json" "$SERVICE_SPEC"
if vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: worker descriptor accepted a control secret' >&2
  exit 1
fi

VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
VP_STAGING_JANITOR_DATABASE_SECRET=staging-db-secret
VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=staging-minio-access-secret
VP_STAGING_JANITOR_MINIO_SECRET_SECRET=staging-minio-secret-secret
admission_root="$(vp_worker_admission_root)"
mkdir -p "$admission_root"
cat >"$admission_root/staging-object-janitor.conf" <<EOF
VERSION=2
GENERATION=$VP_WORKER_CONTROL_GENERATION
IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
NETWORK=vp-pipeline-net
NETWORK_ID=vp-pipeline-network-id
DATABASE_SECRET=$VP_STAGING_JANITOR_DATABASE_SECRET
MINIO_ACCESS_SECRET=$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET
MINIO_SECRET_SECRET=$VP_STAGING_JANITOR_MINIO_SECRET_SECRET
EVIDENCE_VOLUME=vp-staging-janitor-evidence
MANAGER_NODE=ccttww-lap
EOF
chmod 0600 "$admission_root/staging-object-janitor.conf"
cat >"$SERVICE_SPEC" <<EOF
{
  "Labels": {
    "vp.videoprocess.job": "staging-object-janitor",
    "vp.videoprocess.generation": "$VP_WORKER_CONTROL_GENERATION"
  },
  "Mode": {
    "ReplicatedJob": {"MaxConcurrent": 1, "TotalCompletions": 1}
  },
  "TaskTemplate": {
    "ContainerSpec": {
      "Image": "vp-ffmpeg-worker-python:deploy-0123456789ab",
      "User": "10001:10001",
      "Args": ["python", "-m", "app.channel_agent.staging_object_janitor_cli"],
      "Env": [
        "DEPLOY_MODE=production",
        "VP_STAGING_JANITOR_RUNNER_ID=ccttww-lap",
        "VP_STAGING_JANITOR_DATABASE_URL_FILE=/run/secrets/vp-staging-janitor-database-url",
        "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-staging-janitor-minio-access-key",
        "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE=/run/secrets/vp-staging-janitor-minio-secret-key",
        "VP_STAGING_JANITOR_STATUS_FILE=/run/videoprocess/staging-janitor/status.json",
        "STORAGE_BACKEND=minio",
        "MINIO_ENDPOINT=10.0.0.150:9000",
        "MINIO_BUCKET=videoprocess"
      ],
      "Secrets": [
        {"SecretName": "$VP_STAGING_JANITOR_DATABASE_SECRET", "File": {"Name": "vp-staging-janitor-database-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET", "File": {"Name": "vp-staging-janitor-minio-access-key", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$VP_STAGING_JANITOR_MINIO_SECRET_SECRET", "File": {"Name": "vp-staging-janitor-minio-secret-key", "UID": "10001", "GID": "10001", "Mode": 256}}
      ],
      "Mounts": [
        {"Type": "volume", "Source": "vp-staging-janitor-evidence", "Target": "/run/videoprocess/staging-janitor"}
      ]
    },
    "RestartPolicy": {"Condition": "none"},
    "Placement": {"Constraints": ["node.hostname==ccttww-lap"]},
    "Networks": [{"Target": "vp-pipeline-network-id"}]
  }
}
EOF
vp_require_staging_object_janitor_control \
  "$admission_root" vp-ffmpeg-worker-python:deploy-0123456789ab
sed 's/"TotalCompletions": 1/"TotalCompletions": 2/' \
  "$SERVICE_SPEC" >"$TEST_ROOT/bad-janitor-spec.json"
mv "$TEST_ROOT/bad-janitor-spec.json" "$SERVICE_SPEC"
if vp_require_staging_object_janitor_control \
  "$admission_root" vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: control retirement accepted a multi-completion janitor job' >&2
  exit 1
fi

if vp_worker_control_generation_unused \
  c-11111111111111111111 >/dev/null 2>&1; then
  echo 'FAIL: control retirement ignored a dependency inspection failure' >&2
  exit 1
fi
vp_worker_control_generation_unused \
  c-11111111111111111111 true
CONTROL_SERVICE_NAMES=vp-ffmpeg-worker-go-swarm
if vp_worker_control_generation_unused \
  c-11111111111111111111 true >/dev/null 2>&1; then
  echo 'FAIL: candidate cleanup treated an existing service as absent' >&2
  exit 1
fi
CONTROL_SERVICE_NAMES=

VP_PIPELINE_NETWORK_ID=""
NETWORK_IDENTITY_OUTPUT='substitute-id|substitute-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: substitute pipeline network name was accepted' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
vp_require_pipeline_network_identity
VP_PIPELINE_NETWORK_ID=""
NETWORK_IDENTITY_OUTPUT=$'vp-pipeline-network-id|vp-pipeline-net|overlay|swarm\nsecond-id|vp-pipeline-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: ambiguous pipeline network inspection was accepted' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
vp_require_pipeline_network_identity
NETWORK_IDENTITY_OUTPUT='recreated-network-id|vp-pipeline-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: pipeline network identity changed inside one transaction' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
if grep -Eq 'docker[[:space:]]+network[[:space:]]+create' "$EXTENSION"; then
  echo 'FAIL: deployment creates or substitutes the pipeline network' >&2
  exit 1
fi

if grep -Eiq 'ACL[[:space:]]+SETUSER|redis-cli[^\n]*ACL' "$EXTENSION"; then
  echo 'FAIL: VideoProcess deployment attempts to own Redis ACL users' >&2
  exit 1
fi
if grep -Eq 'VP_RUNTIME_HOST=.*10\\.0\\.0\\.126|remote_sh[[:space:]]+10\\.0\\.0\\.126' \
  "$EXTENSION"; then
  echo 'FAIL: worker deployment targets host 126' >&2
  exit 1
fi
for worker_cli in \
  app.channel_agent.staging_object_janitor_cli \
  app.services.worker_control_role_cli \
  app.services.worker_deployment_cli \
  app.services.worker_registration_operator_cli \
  app.services.worker_runtime_role_cli; do
  if ! grep -Fq "$worker_cli" "$ROOT_DIR/backend/Dockerfile.worker"; then
    echo "FAIL: Python worker image does not verify CLI: $worker_cli" >&2
    exit 1
  fi
done
python_worker_dockerfile="$ROOT_DIR/backend/Dockerfile.worker"
if ! grep -Fq '/usr/local/share/videoprocess/worker-build-commit' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image does not use a fixed build identity artifact' >&2
  exit 1
fi
if ! grep -Fq 'USER videoprocess-worker' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image does not use its dedicated runtime user' >&2
  exit 1
fi
if ! grep -Fq '["/opt/venv/bin/python", "-I", "-m", "worker.main"]' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker entrypoint does not reject import path injection' >&2
  exit 1
fi
if grep -Fq '/app/worker/_build_identity.py' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker still uses an import-shadowable identity module' >&2
  exit 1
fi
if grep -Eq '^ENV[[:space:]]+VP_BUILD_COMMIT=' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image exposes a runtime-overridable build commit' >&2
  exit 1
fi
python_worker_image_test="$ROOT_DIR/tests/test_python_worker_image_identity.sh"
if [[ ! -x "$python_worker_image_test" ]] \
  || ! bash -n "$python_worker_image_test"; then
  echo 'FAIL: Python worker image behavior probe is missing or invalid' >&2
  exit 1
fi

control_manifest="$TEST_ROOT/control-current.conf"
vp_worker_control_write_manifest \
  "$control_manifest" \
  c-0123456789abcdef0123 \
  vp-ffmpeg-worker-python:deploy-0123456789ab
[[ "$(vp_worker_redis_marker_file_mode "$control_manifest")" == 600 ]]
vp_worker_control_read_manifest "$control_manifest"
[[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == \
  c-0123456789abcdef0123 ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET" == \
  vp-wc-operator-c-0123456789abcdef0123 ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET" == \
  vp-wc-worker-minio-secret-c-0123456789abcdef0123 ]]

stale_control_root="$TEST_ROOT/stale-control"
active_control_generation=c-0123456789abcdef0123
active_control_image=vp-ffmpeg-worker-python:deploy-0123456789ab
stale_control_generation=c-11111111111111111111
stale_control_image=vp-ffmpeg-worker-python:deploy-111111111111
vp_worker_control_write_manifest \
  "$stale_control_root/control-candidates/$active_control_generation.conf" \
  "$active_control_generation" "$active_control_image"
vp_worker_control_write_manifest \
  "$stale_control_root/control-candidates/$stale_control_generation.conf" \
  "$stale_control_generation" "$stale_control_image"
STALE_CONTROL_CALLS="$TEST_ROOT/stale-control-calls"
: >"$STALE_CONTROL_CALLS"
VP_WORKER_CONTROL_PRIOR_GENERATION="$active_control_generation"
VP_WORKER_CONTROL_PRIOR_IMAGE="$active_control_image"
vp_worker_control_retire_generation() {
  printf 'retire|%s|%s\n' "$1" "$2" >>"$STALE_CONTROL_CALLS"
}
vp_worker_control_cleanup_stale_candidates "$stale_control_root"
[[ ! -e "$stale_control_root/control-candidates/$active_control_generation.conf" ]]
[[ ! -e "$stale_control_root/control-candidates/$stale_control_generation.conf" ]]
[[ "$(<"$STALE_CONTROL_CALLS")" == \
  "retire|$stale_control_image|$stale_control_generation" ]]

control_retirement_root="$TEST_ROOT/control-retirement"
control_retirement_generation=c-11111111111111111111
control_retirement_image=vp-ffmpeg-worker-python:deploy-111111111111
control_retirement_ids=()
for identity_index in 1 2 3 4 5 6 7; do
  control_retirement_ids+=("$(printf '%064d' "$identity_index")")
done
vp_worker_control_write_manifest \
  "$control_retirement_root/control-candidates/$control_retirement_generation.conf" \
  "$control_retirement_generation" \
  "$control_retirement_image" \
  "${control_retirement_ids[@]}"
vp_worker_control_schedule_retirement \
  "$control_retirement_root" \
  "$control_retirement_image" \
  "$control_retirement_generation"
control_retirement_journal="$control_retirement_root/control-retirements/$control_retirement_generation.conf"
[[ "$(vp_worker_redis_marker_file_mode "$control_retirement_journal")" == 600 ]]
vp_worker_control_read_manifest "$control_retirement_journal"
[[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == \
  "$control_retirement_generation" ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$control_retirement_image" ]]
vp_worker_control_schedule_retirement \
  "$control_retirement_root" \
  "$control_retirement_image" \
  "$control_retirement_generation"

CONTROL_RETIREMENT_CALLS="$TEST_ROOT/control-retirement-calls"
: >"$CONTROL_RETIREMENT_CALLS"
CONTROL_RETIREMENT_FAIL=true
vp_worker_control_retire_generation() {
  printf 'retire|%s|%s\n' "$1" "$2" >>"$CONTROL_RETIREMENT_CALLS"
  [[ "$CONTROL_RETIREMENT_FAIL" == false ]]
}
if vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123; then
  echo 'FAIL: failed control retirement discarded its journal' >&2
  exit 1
fi
[[ -f "$control_retirement_journal" ]]
CONTROL_RETIREMENT_FAIL=false
vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123
[[ ! -e "$control_retirement_journal" ]]
vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123
[[ "$(grep -c '^retire|' "$CONTROL_RETIREMENT_CALLS")" -eq 2 ]]

worker_retirement_root="$TEST_ROOT/worker-retirement"
worker_retirement_journal="$worker_retirement_root/retirements/stale-commit.records"
worker_retirement_records="vp-ffmpeg-worker-go-swarm|811|stale-db-811|$(printf '%064d' 811)|stale-admission-811|$(printf '%064d' 1811)"
vp_worker_admission_write_retirement_journal \
  "$worker_retirement_journal" "$worker_retirement_records"
[[ "$(vp_worker_redis_marker_file_mode "$worker_retirement_journal")" == 600 ]]
WORKER_RETIREMENT_CALLS="$TEST_ROOT/worker-retirement-calls"
: >"$WORKER_RETIREMENT_CALLS"
WORKER_RETIREMENT_FAIL=true
vp_worker_admission_retire_records() {
  printf 'retire|%s\n' "$1" >>"$WORKER_RETIREMENT_CALLS"
  [[ "$WORKER_RETIREMENT_FAIL" == false ]]
}
if vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"; then
  echo 'FAIL: failed worker retirement discarded its journal' >&2
  exit 1
fi
[[ -f "$worker_retirement_journal" ]]
WORKER_RETIREMENT_FAIL=false
vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"
[[ ! -e "$worker_retirement_journal" ]]
vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"
[[ "$(grep -c '^retire|' "$WORKER_RETIREMENT_CALLS")" -eq 2 ]]

VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=partial-generation
VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
vp_require_pipeline_network_identity() {
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
}
vp_worker_admission_new_generation() {
  printf '901\n'
}
vp_worker_admission_required_file() {
  return 1
}
partial_root="$(vp_worker_admission_root)"
if vp_worker_admission_prepare_service \
  vp-ffmpeg-worker-go-swarm \
  vp-ffmpeg-worker-go:deploy-0123456789ab \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  "$VP_WORKER_ADMISSION_COMMIT" \
  "$partial_root" \
  "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"; then
  echo 'FAIL: partial worker preparation unexpectedly succeeded' >&2
  exit 1
fi
[[ "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" == \
  vp-ffmpeg-worker-go-swarm ]]
if vp_worker_admission_candidate_records >/dev/null 2>&1; then
  echo 'FAIL: name-only partial state was emitted as v2 retirement evidence' >&2
  exit 1
fi
grep -Fxq 'VERSION=1' \
  "$partial_root/candidates/partial-generation/ffmpeg-go.conf"

(
  resume_root="$partial_root"
  resume_namespace=resume-generation
  resume_service=vp-ffmpeg-worker-go-swarm
  resume_image=vp-ffmpeg-worker-go:deploy-0123456789ab
  resume_control_image=vp-ffmpeg-worker-python:deploy-0123456789ab
  resume_generation=902
  resume_candidate="$resume_root/candidates/$resume_namespace/ffmpeg-go.conf"
  resume_database_secret=vp-wr-ffmpeg-go-db-902
  resume_admission_secret=vp-wr-ffmpeg-go-admission-902
  resume_calls="$TEST_ROOT/worker-partial-resume-calls"
  : >"$resume_calls"
  vp_worker_admission_write_manifest \
    "$resume_candidate" "$resume_service" \
    "$VP_WORKER_ADMISSION_COMMIT" "$resume_image" "$resume_generation" \
    "$resume_database_secret" "$resume_admission_secret"

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "$TEST_ROOT/runtime-owner"
  }
  vp_run_python_worker_container() {
    printf 'container|%s\n' "$*" >>"$resume_calls"
  }
  vp_worker_admission_require_v2_manifest() {
    printf 'name-hydration\n' >>"$resume_calls"
    return 1
  }
  vp_worker_admission_create_secret() {
    printf 'secret|%s|%s\n' "$1" "$5" >>"$resume_calls"
    if [[ "$5" == database ]]; then
      VP_WORKER_CREATED_SECRET_ID="$(printf '%064d' 902)"
    else
      VP_WORKER_CREATED_SECRET_ID="$(printf '%064d' 1902)"
    fi
  }
  vp_worker_admission_operator() {
    :
  }

  if ! vp_worker_admission_prepare_service \
    "$resume_service" "$resume_image" "$resume_control_image" \
    "$VP_WORKER_ADMISSION_COMMIT" "$resume_root" "$resume_namespace"; then
    echo 'FAIL: worker partial manifest did not resume through durable IDs' >&2
    exit 1
  fi
  if grep -Fxq name-hydration "$resume_calls"; then
    echo 'FAIL: worker partial resume hydrated its secret by name' >&2
    exit 1
  fi
  vp_worker_admission_read_manifest "$resume_candidate" "$resume_service"
  [[ "$VP_WORKER_MANIFEST_VERSION" == 2 ]]
  [[ "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" == "$(printf '%064d' 902)" ]]
  [[ "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" == "$(printf '%064d' 1902)" ]]
)

(
  control_resume_root="$partial_root"
  control_resume_commit=abcdef0123456789abcdef0123456789abcdef01
  control_resume_generation="c-${control_resume_commit:0:20}"
  control_resume_image=vp-ffmpeg-worker-python:deploy-abcdef012345
  control_resume_candidate="$control_resume_root/control-candidates/$control_resume_generation.conf"
  control_resume_ids=()
  for control_resume_index in 1 2 3 4 5 6 7; do
    control_resume_ids+=("$(printf '%064d' "$control_resume_index")")
  done
  vp_worker_control_write_manifest \
    "$control_resume_candidate" "$control_resume_generation" \
    "$control_resume_image" "${control_resume_ids[@]}"

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "$TEST_ROOT/control-owner"
  }
  vp_run_python_worker_container() {
    :
  }
  vp_worker_admission_create_secret() {
    return 1
  }

  if vp_worker_admission_prepare_control_roles \
    "$control_resume_image" "$control_resume_commit" \
    "$control_resume_root"; then
    echo 'FAIL: control partial resume fixture unexpectedly completed' >&2
    exit 1
  fi
  vp_worker_control_read_manifest "$control_resume_candidate"
  if [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" != 2 \
    || "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID" \
      != "${control_resume_ids[0]}" ]]; then
    echo 'FAIL: control retry downgraded durable candidate identity' >&2
    exit 1
  fi
)

CLEANUP_CALLS="$TEST_ROOT/cleanup-calls"
: >"$CLEANUP_CALLS"
CLEANUP_GRANT_STATE=active
vp_worker_admission_generation_state() {
  printf '%s\n' "$CLEANUP_GRANT_STATE"
}
vp_worker_admission_retirement_ids() {
  :
}
vp_worker_admission_operator() {
  printf 'operator|%s\n' "$*" >>"$CLEANUP_CALLS"
}
vp_worker_admission_required_file() {
  printf '%s\n' "$TEST_ROOT/runtime-owner"
}
printf '%s\n' 'synthetic-runtime-owner' >"$TEST_ROOT/runtime-owner"
chmod 0400 "$TEST_ROOT/runtime-owner"
docker() {
  if [[ "${1:-}" == run ]]; then
    cat >/dev/null
    printf 'runtime-role|%s\n' "$*" >>"$CLEANUP_CALLS"
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
    return 1
  fi
  return 90
}
partial_database_id="$(printf '%064d' 901)"
partial_admission_id="$(printf '%064d' 1901)"
vp_remove_managed_secret() {
  printf 'secret-rm|%s|%s|%s|%s|%s\n' "$@" >>"$CLEANUP_CALLS"
}
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
vp_worker_admission_retire_generation \
  vp-ffmpeg-worker-go-swarm 901 \
  vp-wr-ffmpeg-go-db-901 "$partial_database_id" \
  vp-wr-ffmpeg-go-admission-901 "$partial_admission_id" \
  "$partial_root"
grep -Fq \
  'revoke-grant --service-name vp-ffmpeg-worker-go-swarm --generation 901 --reason replaced' \
  "$CLEANUP_CALLS"
CLEANUP_GRANT_STATE=absent
: >"$CLEANUP_CALLS"
vp_worker_admission_retire_generation \
  vp-ffmpeg-worker-go-swarm 901 \
  vp-wr-ffmpeg-go-db-901 "$partial_database_id" \
  vp-wr-ffmpeg-go-admission-901 "$partial_admission_id" \
  "$partial_root"
if grep -Fq 'operator|' "$CLEANUP_CALLS"; then
  echo 'FAIL: absent partial grant was sent to the operator' >&2
  exit 1
fi
grep -Fq 'worker_runtime_role_cli revoke' "$CLEANUP_CALLS"

CONTROL_COMMIT_CALLS="$TEST_ROOT/control-commit-calls"
: >"$CONTROL_COMMIT_CALLS"
VP_WORKER_CONTROL_PREPARED=true
VP_WORKER_ADMISSION_COMMITTED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
VP_WORKER_CONTROL_PRIOR_GENERATION=c-11111111111111111111
VP_WORKER_CONTROL_PRIOR_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
vp_require_worker_service_descriptor() {
  printf 'descriptor|%s\n' "$1" >>"$CONTROL_COMMIT_CALLS"
}
vp_require_staging_object_janitor_control() {
  printf 'janitor|converged\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_write_manifest() {
  printf 'manifest|current\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_schedule_retirement() {
  printf 'control|journal|%s|%s\n' "$2" "$3" \
    >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_process_retirements() {
  printf 'control|process\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_commit_worker_control_generation
[[ "$(grep -c '^descriptor|' "$CONTROL_COMMIT_CALLS")" -eq 4 ]]
janitor_line="$(grep -n '^janitor|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
journal_line="$(grep -n '^control|journal|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
manifest_line="$(grep -n '^manifest|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
process_line="$(grep -n '^control|process$' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
if [[ -z "$janitor_line" || -z "$journal_line" \
  || -z "$manifest_line" || -z "$process_line" \
  || "$janitor_line" -ge "$journal_line" \
  || "$journal_line" -ge "$manifest_line" \
  || "$manifest_line" -ge "$process_line" ]]; then
  echo 'FAIL: control generation retired before dependencies converged' >&2
  exit 1
fi

echo "worker admission deployment contract tests passed"
