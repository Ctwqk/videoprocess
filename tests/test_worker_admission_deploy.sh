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
  probe_root="$TEST_ROOT/one-shot-pre-handler-term"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  docker_entered="$probe_root/docker-entered"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"

  docker() {
    : >"$docker_entered"
    cat >/dev/null
  }

  set -T
  trap '
    if [[ "$BASH_COMMAND" == "local launched_child_pid=\"\"" \
      && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true ]]; then
      trap - DEBUG
      builtin kill -TERM "$signal_target_pid"
    fi
  ' DEBUG
  set +e
  vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e
  trap - DEBUG
  set +T

  if [[ "$operation_status" -ne 143 ]]; then
    echo "FAIL: pre-handler TERM returned $operation_status instead of 143" >&2
    exit 1
  fi
  if [[ -e "$docker_entered" ]]; then
    echo 'FAIL: pre-handler TERM continued into Docker' >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null; then
    echo 'FAIL: pre-handler TERM retained one-shot identity state' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-final-gate-term"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  docker_entered="$probe_root/docker-entered"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"

  docker() {
    : >"$docker_entered"
    cat >/dev/null
  }
  kill() {
    if [[ "${1:-}" == -TERM \
      && "${2:-}" != "$signal_target_pid" \
      && "${VP_PYTHON_WORKER_LAUNCH_GATE_OPEN:-false}" != true ]]; then
      return 0
    fi
    builtin kill "$@"
  }

  original_raise_definition="$(
    declare -f vp_worker_admission_raise_if_signaled
  )"
  eval "${original_raise_definition/vp_worker_admission_raise_if_signaled/vp_worker_admission_raise_if_signaled_original}"
  release_gate_available=false
  if declare -F vp_python_worker_release_launch_gate >/dev/null 2>&1; then
    release_gate_available=true
    original_release_definition="$(
      declare -f vp_python_worker_release_launch_gate
    )"
    eval "${original_release_definition/vp_python_worker_release_launch_gate/vp_python_worker_release_launch_gate_original}"
    vp_python_worker_release_launch_gate() {
      builtin kill -TERM "$signal_target_pid"
      vp_python_worker_release_launch_gate_original
    }
  fi
  final_gate_raise_count=0
  vp_worker_admission_raise_if_signaled() {
    local status=0
    vp_worker_admission_raise_if_signaled_original || status=$?
    if [[ "$status" -eq 0 \
      && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true \
      && -z "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" ]]; then
      final_gate_raise_count=$((final_gate_raise_count + 1))
    fi
    if [[ "$status" -eq 0 \
      && "$release_gate_available" != true \
      && "$final_gate_raise_count" -eq 2 ]]; then
      builtin kill -TERM "$signal_target_pid"
    fi
    return "$status"
  }
  set +e
  vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e

  if [[ "$operation_status" -ne 143 ]]; then
    echo "FAIL: final-gate TERM returned $operation_status instead of 143" >&2
    exit 1
  fi
  if [[ -e "$docker_entered" ]]; then
    echo 'FAIL: final-gate TERM continued into Docker' >&2
    exit 1
  fi
  if compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || compgen -G "$admission_root/one-shot-operations/op.*" >/dev/null \
    || [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true ]]; then
    echo 'FAIL: final-gate TERM retained one-shot identity or lock state' >&2
    exit 1
  fi
)

(
  probe_root="$TEST_ROOT/one-shot-launch-gate-failure"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  docker_entered="$probe_root/docker-entered"
  supervisor_pid_file="$probe_root/supervisor-pid"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"

  docker() {
    : >"$docker_entered"
  }
  vp_python_worker_release_launch_gate() {
    printf '%s\n' "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" \
      >"$supervisor_pid_file"
    return 74
  }

  set +e
  vp_run_python_worker_container \
    synthetic-image - - /runtime-state \
    --mount "type=bind,src=$bind_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  operation_status=$?
  set -e
  supervisor_pid="$(<"$supervisor_pid_file")"
  supervisor_live=false
  if [[ "$supervisor_pid" =~ ^[1-9][0-9]*$ ]] \
    && builtin kill -0 "$supervisor_pid" 2>/dev/null; then
    supervisor_live=true
    builtin kill -TERM "$supervisor_pid" 2>/dev/null || true
    wait "$supervisor_pid" 2>/dev/null || true
  fi
  if [[ "$operation_status" -eq 0 \
    || "$supervisor_live" == true \
    || -e "$docker_entered" \
    || "$VP_PYTHON_WORKER_LAUNCH_GATE_OPEN" != false \
    || -n "$VP_PYTHON_WORKER_LAUNCH_GATE_PATH" \
    || -n "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY" \
    || -n "$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN" \
    || -e /dev/fd/16 ]]; then
    echo 'FAIL: launch-gate failure leaked its waiting supervisor' >&2
    exit 1
  fi
)

for parent_death_boundary in release-entry verified-before-token; do
(
  probe_root="$TEST_ROOT/one-shot-parent-death-$parent_death_boundary"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  bind_source="$probe_root/runtime-state"
  docker_calls="$probe_root/docker-calls"
  boundary_ready="$probe_root/boundary-ready"
  supervisor_pid_file="$probe_root/supervisor-pid"
  parent_log="$probe_root/parent.log"
  fresh_owner_result="$probe_root/fresh-owner-result"
  mkdir -p "$admission_root" "$bind_source"
  chmod 0700 "$admission_root" "$bind_source"
  secret_source=-
  secret_target=-
  if [[ "$parent_death_boundary" == verified-before-token ]]; then
    secret_source="$probe_root/stream-payload"
    secret_target=stream-payload
    dd if=/dev/zero of="$secret_source" bs=262144 count=1 \
      >/dev/null 2>&1
    chmod 0400 "$secret_source"
  fi
  : >"$docker_calls"
  parent_pid=""
  supervisor_pid=""
  parent_child_pids=""

  cleanup_parent_death_probe() {
    local status=$?
    trap - EXIT
    if [[ "$parent_pid" =~ ^[1-9][0-9]*$ ]] \
      && builtin kill -0 "$parent_pid" 2>/dev/null; then
      builtin kill -KILL "$parent_pid" 2>/dev/null || true
      wait "$parent_pid" 2>/dev/null || true
    fi
    if [[ "$supervisor_pid" =~ ^[1-9][0-9]*$ ]] \
      && builtin kill -0 "$supervisor_pid" 2>/dev/null; then
      builtin kill -KILL "$supervisor_pid" 2>/dev/null || true
    fi
    local child_pid
    for child_pid in $parent_child_pids; do
      if [[ "$child_pid" =~ ^[1-9][0-9]*$ ]] \
        && builtin kill -0 "$child_pid" 2>/dev/null; then
        builtin kill -KILL "$child_pid" 2>/dev/null || true
      fi
    done
    exit "$status"
  }
  trap cleanup_parent_death_probe EXIT

  process_fd_access() {
    local process_id="$1"
    local descriptor="$2"
    if [[ -r "/proc/$process_id/fdinfo/$descriptor" ]]; then
      local raw_flags
      raw_flags="$(
        awk '$1 == "flags:" { print $2 }' \
          "/proc/$process_id/fdinfo/$descriptor"
      )" || return 1
      [[ "$raw_flags" =~ ^[0-7]+$ ]] || return 1
      case "$((8#$raw_flags & 3))" in
        0) printf 'r\n' ;;
        1) printf 'w\n' ;;
        2) printf 'u\n' ;;
        *) return 1 ;;
      esac
      return 0
    fi
    if [[ "$(uname -s)" == Darwin ]] && command -v lsof >/dev/null 2>&1; then
      local lsof_record
      lsof_record="$(
        lsof -a -p "$process_id" -d "$descriptor" -Faf 2>/dev/null
      )" || {
        printf -- '-\n'
        return 0
      }
      if [[ -z "$lsof_record" ]]; then
        printf -- '-\n'
        return 0
      fi
      case "$lsof_record" in
        *$'\nar\n'*|*$'\nar') printf 'r\n' ;;
        *$'\naw\n'*|*$'\naw') printf 'w\n' ;;
        *$'\nau\n'*|*$'\nau') printf 'u\n' ;;
        *) return 1 ;;
      esac
      return 0
    fi
    return 1
  }

  fresh_owner_reconcile() {
    (
      exec 18<>"$admission_root/transaction.lock"
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        lock-acquire "$admission_root" 18 >/dev/null 2>&1 \
        || exit 1
      vp_python_worker_host_guard \
        prepare-operation "$admission_root" "$(id -u)" "$(id -g)" \
        >"$fresh_owner_result"
    )
  }

  (
    docker() {
      printf 'docker\n' >>"$docker_calls"
    }
    vp_python_worker_release_launch_gate() {
      if [[ "$parent_death_boundary" == verified-before-token ]]; then
        vp_worker_admission_raise_if_signaled || return $?
        vp_python_worker_verify_launch_gate || return 1
      fi
      printf '%s\n' "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" \
        >"$supervisor_pid_file"
      : >"$boundary_ready"
      while :; do
        :
      done
    }
    vp_run_python_worker_container \
      synthetic-image "$secret_source" "$secret_target" /runtime-state \
      --mount "type=bind,src=$bind_source,dst=/runtime-state" \
      -- /bin/true
  ) >"$parent_log" 2>&1 &
  parent_pid=$!

  for _attempt in $(seq 1 200); do
    [[ -e "$boundary_ready" ]] && break
    if ! builtin kill -0 "$parent_pid" 2>/dev/null; then
      break
    fi
    sleep 0.01
  done
  if [[ ! -e "$boundary_ready" || ! -s "$supervisor_pid_file" ]]; then
    echo "FAIL: $parent_death_boundary probe did not reach its pre-token boundary" >&2
    exit 1
  fi
  supervisor_pid="$(<"$supervisor_pid_file")"
  if [[ ! "$supervisor_pid" =~ ^[1-9][0-9]*$ ]] \
    || ! builtin kill -0 "$supervisor_pid" 2>/dev/null; then
    echo "FAIL: $parent_death_boundary probe did not publish a live supervisor" >&2
    exit 1
  fi

  child_read_access="$(process_fd_access "$supervisor_pid" 17 || true)"
  child_gate_writer_access="$(process_fd_access "$supervisor_pid" 16 || true)"
  child_lock_access="$(process_fd_access "$supervisor_pid" 19 || true)"
  child_fd_audit=false
  if [[ "$child_read_access" == r \
    && "$child_gate_writer_access" == - \
    && "$child_lock_access" == - ]]; then
    child_fd_audit=true
  fi
  parent_child_pids="$(
    pgrep -P "$parent_pid" 2>/dev/null \
      | tr '\n' ' ' \
      || true
  )"
  producer_fd_audit=true
  child_pid=""
  for child_pid in $parent_child_pids; do
    [[ "$child_pid" == "$supervisor_pid" ]] && continue
    if [[ "$(process_fd_access "$child_pid" 16 || true)" != - \
      || "$(process_fd_access "$child_pid" 17 || true)" != - \
      || "$(process_fd_access "$child_pid" 19 || true)" != - ]]; then
      producer_fd_audit=false
    fi
  done
  if ! compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    || ! compgen -G "$admission_root/one-shot-operations/op.*" \
      >/dev/null; then
    echo "FAIL: $parent_death_boundary probe lacked stale operation evidence" >&2
    exit 1
  fi

  builtin kill -KILL "$parent_pid"
  wait "$parent_pid" 2>/dev/null || true
  parent_pid=""

  supervisor_exited=false
  all_children_exited=false
  fresh_lock_acquired=false
  for _attempt in $(seq 1 200); do
    if ! builtin kill -0 "$supervisor_pid" 2>/dev/null; then
      supervisor_exited=true
    fi
    all_children_exited=true
    for child_pid in $parent_child_pids; do
      if builtin kill -0 "$child_pid" 2>/dev/null; then
        all_children_exited=false
      fi
    done
    if [[ "$fresh_lock_acquired" != true ]] \
      && fresh_owner_reconcile >/dev/null 2>&1; then
      fresh_lock_acquired=true
    fi
    if [[ "$supervisor_exited" == true \
      && "$all_children_exited" == true \
      && "$fresh_lock_acquired" == true ]]; then
      break
    fi
    sleep 0.01
  done

  stale_reconciled=false
  if [[ "$fresh_lock_acquired" == true \
    && -f "$fresh_owner_result" \
    && "$(<"$fresh_owner_result")" == - ]] \
    && ! compgen -G "$bind_source/.vp-python-worker-bind-*" >/dev/null \
    && ! compgen -G "$admission_root/one-shot-operations/op.*" \
      >/dev/null; then
    stale_reconciled=true
  fi
  docker_call_count="$(wc -l <"$docker_calls" | tr -d '[:space:]')"
  if [[ "$child_fd_audit" != true \
    || "$producer_fd_audit" != true \
    || "$supervisor_exited" != true \
    || "$all_children_exited" != true \
    || "$fresh_lock_acquired" != true \
    || "$stale_reconciled" != true \
    || "$docker_call_count" -ne 0 ]]; then
    printf '%s\n' \
      "FAIL: $parent_death_boundary parent death retained supervisor, lock, writer, or stale operation" \
      "fd=$child_read_access/$child_gate_writer_access/$child_lock_access child_audit=$child_fd_audit producer_audit=$producer_fd_audit supervisor_exit=$supervisor_exited children_exit=$all_children_exited fresh_lock=$fresh_lock_acquired reconcile=$stale_reconciled docker=$docker_call_count" \
      >&2
    exit 1
  fi
  supervisor_pid=""
  trap - EXIT
)
done

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

for signal_case in HUP:129 INT:130 TERM:143; do
(
  signal_name="${signal_case%%:*}"
  expected_status="${signal_case##*:}"
  probe_root="$TEST_ROOT/outer-validation-signal-$signal_name"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  prepare_entered="$probe_root/prepare-entered"
  mutation_entered="$probe_root/mutation-entered"
  caller_trap_ran="$probe_root/caller-trap-ran"
  mkdir -p "$admission_root"
  chmod 0700 "$admission_root"
  signal_target_pid="$(
    exec sh -c 'printf "%s\n" "$PPID"'
  )"

  vp_validate_deploy_config() {
    builtin kill "-$signal_name" "$signal_target_pid"
    return 0
  }
  vp_worker_admission_prepare_transaction() {
    : >"$prepare_entered"
    return 0
  }
  _vp_deploy_vp_app_services_locked() {
    : >"$mutation_entered"
    return 0
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
    echo "FAIL: validation $signal_name returned $deploy_status instead of $expected_status" >&2
    exit 1
  fi
  if [[ -e "$prepare_entered" || -e "$mutation_entered" ]]; then
    echo "FAIL: validation $signal_name continued after first signal" >&2
    exit 1
  fi
  if [[ -e "$caller_trap_ran" \
    || "$(trap -p HUP)" != "$caller_hup_trap" \
    || "$(trap -p INT)" != "$caller_int_trap" \
    || "$(trap -p TERM)" != "$caller_term_trap" ]]; then
    echo "FAIL: validation $signal_name did not preserve caller traps" >&2
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
  replacement_uuid=11234567-89ab-4def-8123-456789abcdef
  RETIREMENT_RESPONSE_SERVICE="$expected_service"
  RETIREMENT_RESPONSE_GENERATION="$expected_generation"
  RETIREMENT_RESPONSE_REPLACE_PATH=false
  RETIREMENT_RESPONSE_STATUS=0
  RETIREMENT_OPERATOR_CALLS="$TEST_ROOT/retirement-response-operator-calls"
  : >"$RETIREMENT_OPERATOR_CALLS"
  VP_WORKER_ADMISSION_CONTROL_IMAGE=synthetic-control-image
  retirement_response_root="$(vp_worker_admission_root)"
  vp_worker_admission_lock_acquire "$retirement_response_root"

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
    if [[ "$RETIREMENT_RESPONSE_REPLACE_PATH" == true ]]; then
      local query_path="$VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE"
      if [[ -e "$query_path" ]]; then
        unlink "$query_path"
      fi
      printf '{"code":"worker_deployment_retirement_candidates","generation":%s,"registration_ids":["%s"],"service_name":"%s","status":"ok"}\n' \
        "$RETIREMENT_RESPONSE_GENERATION" \
        "$replacement_uuid" \
        "$RETIREMENT_RESPONSE_SERVICE" >"$query_path"
      chmod 0600 "$query_path"
    fi
    [[ "$RETIREMENT_RESPONSE_STATUS" -eq 0 ]] \
      || return "$RETIREMENT_RESPONSE_STATUS"
  }
  vp_worker_admission_operator() {
    printf 'operator|%s\n' "$*" >>"$RETIREMENT_OPERATOR_CALLS"
  }

  RETIREMENT_RESPONSE_SERVICE=vp-vision-worker-swarm
  set +e
  vp_worker_admission_retirement_ids \
    "$expected_service" "$expected_generation"
  mismatch_status=$?
  mismatch_output="$VP_WORKER_ADMISSION_RETIREMENT_IDS"
  set -e
  if [[ "$mismatch_status" -eq 0 || -n "$mismatch_output" ]]; then
    echo 'FAIL: retirement response accepted a mismatched service' >&2
    exit 1
  fi

  RETIREMENT_RESPONSE_SERVICE="$expected_service"
  RETIREMENT_RESPONSE_GENERATION=999
  set +e
  vp_worker_admission_retirement_ids \
    "$expected_service" "$expected_generation"
  mismatch_status=$?
  mismatch_output="$VP_WORKER_ADMISSION_RETIREMENT_IDS"
  set -e
  if [[ "$mismatch_status" -eq 0 || -n "$mismatch_output" ]]; then
    echo 'FAIL: retirement response accepted a mismatched generation' >&2
    exit 1
  fi
  [[ ! -s "$RETIREMENT_OPERATOR_CALLS" ]]

  RETIREMENT_RESPONSE_GENERATION="$expected_generation"
  RETIREMENT_RESPONSE_STATUS=143
  set +e
  vp_worker_admission_retirement_ids \
    "$expected_service" "$expected_generation"
  signal_status=$?
  set -e
  if [[ "$signal_status" -ne 143 \
    || -n "$VP_WORKER_ADMISSION_RETIREMENT_IDS" \
    || "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" != false \
    || "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" != false \
    || -n "$VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE" \
    || -n "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
    || -e /dev/fd/14 || -e /dev/fd/15 ]]; then
    echo 'FAIL: signaled retirement query leaked its output channel' >&2
    exit 1
  fi

  RETIREMENT_RESPONSE_STATUS=0
  RETIREMENT_RESPONSE_REPLACE_PATH=true
  vp_worker_admission_retirement_ids \
    "$expected_service" "$expected_generation"
  exact_output="$VP_WORKER_ADMISSION_RETIREMENT_IDS"
  if [[ "$exact_output" != "$retirement_uuid" ]]; then
    echo 'FAIL: retirement query accepted replacement pathname JSON' >&2
    exit 1
  fi
  if [[ "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" != false \
    || "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" != false \
    || -n "$VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE" \
    || -n "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
    || -e /dev/fd/14 || -e /dev/fd/15 ]]; then
    echo 'FAIL: successful retirement query leaked its output channel' >&2
    exit 1
  fi
  vp_worker_admission_lock_release
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

(
  probe_root="$TEST_ROOT/outer-lock-real-retirement"
  ROOT="$probe_root/sync"
  REPO_ROOT="$probe_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  retirement_service=vp-ffmpeg-worker-go-swarm
  retirement_generation=821
  retirement_database_name=stale-db-821
  retirement_admission_name=stale-admission-821
  retirement_database_id=7123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  retirement_admission_id=8123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  retirement_uuid=01234567-89ab-4def-8123-456789abcdef
  retirement_journal="$admission_root/retirements/legacy.records"
  retirement_calls="$probe_root/calls"
  read_file="$probe_root/deploy-read"
  owner_file="$probe_root/runtime-owner"
  VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
  operator_file="$admission_root/control/$VP_WORKER_CONTROL_GENERATION/worker-registration-operator-database-url"
  VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$read_file"
  VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="$owner_file"
  VP_WORKER_ADMISSION_CONTROL_IMAGE=synthetic-control-image
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  mkdir -p \
    "$admission_root/control/$VP_WORKER_CONTROL_GENERATION"
  chmod 0700 \
    "$admission_root" \
    "$admission_root/control" \
    "$admission_root/control/$VP_WORKER_CONTROL_GENERATION"
  printf '%s\n' synthetic-read >"$read_file"
  printf '%s\n' synthetic-owner >"$owner_file"
  printf '%s\n' synthetic-operator >"$operator_file"
  chmod 0400 "$read_file" "$owner_file" "$operator_file"
  : >"$retirement_calls"

  docker() {
    printf 'docker|%s\n' "$*" >>"$retirement_calls"
    if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
      printf '%s\n' 'network-id|vp-pipeline-net|overlay|swarm'
      return 0
    fi
    if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
      case "${3:-}" in
        "$retirement_database_name"|"$retirement_database_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$retirement_database_id" "$retirement_database_name" \
            "$retirement_service" "$retirement_generation" database
          return 0
          ;;
        "$retirement_admission_name"|"$retirement_admission_id")
          printf '%s|%s|%s|%s|%s\n' \
            "$retirement_admission_id" "$retirement_admission_name" \
            "$retirement_service" "$retirement_generation" admission
          return 0
          ;;
      esac
      return 1
    fi
    if [[ "${1:-} ${2:-}" == "secret rm" ]]; then
      return 0
    fi
    if [[ "${1:-}" == run ]]; then
      cat >/dev/null
      case "$*" in
        *"worker_deployment_cli generation-state"*)
          printf '{"code":"worker_deployment_generation_state","generation":821,"grant_state":"active","service_name":"vp-ffmpeg-worker-go-swarm","status":"ok"}\n'
          ;;
        *"worker_deployment_cli retirement-candidates"*)
          printf '{"code":"worker_deployment_retirement_candidates","generation":821,"registration_ids":["%s"],"service_name":"vp-ffmpeg-worker-go-swarm","status":"ok"}\n' \
            "$retirement_uuid"
          ;;
        *"worker_registration_operator_cli revoke-registration"*|\
        *"worker_registration_operator_cli revoke-grant"*|\
        *"worker_runtime_role_cli revoke"*)
          :
          ;;
        *)
          return 96
          ;;
      esac
      return 0
    fi
    return 97
  }

  vp_worker_admission_write_retirement_journal \
    "$retirement_journal" \
    "$retirement_service|$retirement_generation|$retirement_database_name|$retirement_admission_name"
  vp_worker_admission_lock_acquire "$admission_root"
  if ! vp_worker_admission_process_retirement_journals "$admission_root"; then
    echo 'FAIL: outer-lock retirement command substitution was rejected' >&2
    exit 1
  fi
  [[ ! -e "$retirement_journal" ]]
  grep -Fq 'worker_deployment_cli generation-state' "$retirement_calls"
  grep -Fq 'worker_deployment_cli retirement-candidates' "$retirement_calls"
  grep -Fq \
    "worker_registration_operator_cli revoke-registration --service-name $retirement_service --registration-id $retirement_uuid --reason replaced" \
    "$retirement_calls"
  grep -Fq "secret rm $retirement_database_id" "$retirement_calls"
  grep -Fq "secret rm $retirement_admission_id" "$retirement_calls"
  [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_LOCK_DEPTH" -eq 1 ]]
  vp_worker_admission_lock_release
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
  partial_control_generation=c-${partial_commit:0:20}
  partial_control_image="vp-ffmpeg-worker-python:deploy-${partial_commit:0:12}"
  partial_operator_reference="control/$partial_control_generation/worker-registration-operator-database-url"
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-authority-intent \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime vp-ffmpeg-worker-go-swarm 901 \
    "$partial_control_image" "$partial_control_generation" \
    "$partial_operator_reference" >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioning \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime vp-ffmpeg-worker-go-swarm 901 >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioned \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime vp-ffmpeg-worker-go-swarm 901 >/dev/null

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
  abort_root="$TEST_ROOT/partial-secret-abort"
  mkdir -p "$abort_root"
  abort_commit=2123456789abcdef0123456789abcdef01234567
  abort_control_generation=c-${abort_commit:0:20}
  abort_worker_generation=901
  abort_control_image="vp-ffmpeg-worker-python:deploy-${abort_commit:0:12}"
  abort_backend_image="vp-backend:deploy-${abort_commit:0:12}"
  abort_go_image="vp-ffmpeg-worker-go:deploy-${abort_commit:0:12}"
  abort_calls=""
  abort_secret_state=""
  abort_active=""
  abort_mismatch_id=""
  abort_mismatch_kind=""
  abort_crash_id=""
  abort_crash_once=""
  abort_mounted_secret_id=""
  abort_authorities=""

  abort_secret_id() {
    printf '%064x\n' "$1"
  }

  begin_abort_fixture() {
    local fixture_name="$1"
    local fixture_root="$abort_root/$fixture_name"
    ROOT="$fixture_root/sync"
    REPO_ROOT="$fixture_root/repos"
    local admission_root="$ROOT/state/vp-worker-admission"
    abort_calls="$fixture_root/calls"
    abort_secret_state="$fixture_root/secrets"
    abort_active="$admission_root/transactions/active.json"
    abort_mismatch_id=""
    abort_mismatch_kind=""
    abort_crash_id=""
    abort_crash_once="$fixture_root/crash-once"
    abort_mounted_secret_id=""
    abort_authorities=""
    mkdir -p "$admission_root" "$abort_secret_state"
    chmod 0700 "$admission_root" "$abort_secret_state"
    : >"$abort_calls"
    rm -f "$abort_crash_once"

    local credentials=()
    local principals=(
      vp_deploy_migrator
      vp_deploy_read
      vp_control_role_owner
      vp_runtime_role_owner
    )
    local index
    for index in 0 1 2 3; do
      local credential="$fixture_root/credential-$index"
      printf 'postgresql://abort-%s:credential@database/videoprocess\n' \
        "$index" >"$credential"
      chmod 0400 "$credential"
      credentials+=("$credential")
    done
    local credential_records
    credential_records="$(
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        validate-credentials \
        "${credentials[0]}" "${principals[0]}" \
        "${credentials[1]}" "${principals[1]}" \
        "${credentials[2]}" "${principals[2]}" \
        "${credentials[3]}" "${principals[3]}"
    )"
    vp_worker_admission_lock_acquire "$admission_root"
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
      "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$abort_commit" "$abort_backend_image" "$abort_go_image" \
      "$abort_commit" legacy_no_control \
      <<<"$credential_records" >/dev/null
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
    VP_WORKER_ADMISSION_CONTROL_IMAGE="$abort_control_image"
    VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$abort_commit"
  }

  record_abort_secret() {
    local name="$1"
    local secret_id="$2"
    local service="$3"
    local generation="$4"
    local purpose="$5"
    local kind=runtime
    [[ "$service" == vp-worker-control ]] && kind=control
    local authority_key="$kind:$service:$generation"
    if [[ "|$abort_authorities|" != *"|$authority_key|"* ]]; then
      local operator_reference="control/$abort_control_generation/worker-registration-operator-database-url"
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        record-authority-intent \
        "$VP_WORKER_ADMISSION_LOCK_ROOT" \
        "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$kind" "$service" "$generation" \
        "$abort_control_image" "$abort_control_generation" \
        "$operator_reference" >/dev/null
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        mark-authority-provisioning \
        "$VP_WORKER_ADMISSION_LOCK_ROOT" \
        "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$kind" "$service" "$generation" >/dev/null
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        mark-authority-provisioned \
        "$VP_WORKER_ADMISSION_LOCK_ROOT" \
        "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$kind" "$service" "$generation" >/dev/null
      abort_authorities="${abort_authorities:+$abort_authorities|}$authority_key"
    fi
    printf '%s|%s|%s|%s|%s\n' \
      "$secret_id" "$name" "$service" "$generation" "$purpose" \
      >"$abort_secret_state/$secret_id"
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      record-prepared-secret \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$name" "$secret_id" "$service" "$generation" "$purpose" \
      >/dev/null
  }

  docker() {
    if [[ "${1:-} ${2:-}" == "service ls" ]]; then
      printf 'service-ls\n' >>"$abort_calls"
      if [[ -n "$abort_mounted_secret_id" ]]; then
        printf 'dddddddddddddddddddddddd\n'
      fi
      return 0
    fi
    if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
      printf 'service-inspect|%s\n' "${3:-}" >>"$abort_calls"
      [[ -n "$abort_mounted_secret_id" ]] \
        && printf '%s\n' "$abort_mounted_secret_id"
      return 0
    fi
    if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
      local reference="${3:-}"
      printf 'inspect|%s\n' "$reference" >>"$abort_calls"
      local state="$abort_secret_state/$reference"
      [[ -f "$state" ]] || return 1
      local secret_id
      local name
      local service
      local generation
      local purpose
      IFS='|' read -r \
        secret_id name service generation purpose <"$state"
      if [[ "$reference" == "$abort_mismatch_id" ]]; then
        case "$abort_mismatch_kind" in
          id) secret_id="$(abort_secret_id 999)" ;;
          label) purpose=replacement ;;
          *) return 1 ;;
        esac
      fi
      printf '%s|%s|%s|%s|%s\n' \
        "$secret_id" "$name" "$service" "$generation" "$purpose"
      return 0
    fi
    if [[ "${1:-} ${2:-}" == "secret ls" ]]; then
      local filter="${4:-}"
      local secret_id="${filter#id=}"
      printf 'secret-ls|%s\n' "$secret_id" >>"$abort_calls"
      [[ -f "$abort_secret_state/$secret_id" ]] \
        && printf '%s\n' "$secret_id"
      return 0
    fi
    if [[ "${1:-} ${2:-}" == "secret rm" ]]; then
      local secret_id="${3:-}"
      printf 'rm|%s\n' "$secret_id" >>"$abort_calls"
      [[ -f "$abort_secret_state/$secret_id" ]] || return 1
      rm -f "$abort_secret_state/$secret_id"
      if [[ "$secret_id" == "$abort_crash_id" \
        && ! -e "$abort_crash_once" ]]; then
        : >"$abort_crash_once"
        return 99
      fi
      return 0
    fi
    return 98
  }

  vp_worker_control_revoke_authority() {
    printf 'authority|control|%s|%s\n' "$2" "$3" >>"$abort_calls"
  }
  vp_worker_admission_revoke_generation_authority() {
    printf 'authority|runtime|%s|%s\n' "$1" "$2" >>"$abort_calls"
  }

  control_purposes=(
    operator
    orchestrator
    staging-janitor
    staging-minio-access
    staging-minio-secret
    worker-minio-access
    worker-minio-secret
  )
  for prefix in 1 2 3 4 5 6 7; do
    begin_abort_fixture "control-prefix-$prefix"
    expected_removals=""
    for index in $(seq 1 "$prefix"); do
      secret_id="$(abort_secret_id "$index")"
      purpose="${control_purposes[$((index - 1))]}"
      name="vp-control-$purpose-$abort_control_generation"
      record_abort_secret \
        "$name" "$secret_id" \
        vp-worker-control "$abort_control_generation" "$purpose"
      expected_removals="rm|$secret_id${expected_removals:+$'\n'$expected_removals}"
    done
    if ! vp_worker_admission_abort_preparing_transaction preparing_failed; then
      echo "FAIL: control partial-secret prefix $prefix did not abort" >&2
      exit 1
    fi
    actual_removals="$(awk -F'|' '$1 == "rm" { print }' "$abort_calls")"
    if [[ "$actual_removals" != "$expected_removals" ]]; then
      echo "FAIL: control partial-secret prefix $prefix cleanup order drifted" >&2
      exit 1
    fi
    if [[ "$(grep -Fc 'service-ls' "$abort_calls")" -ne "$prefix" ]] \
      || ! awk -F'|' '
        $1 == "rm" && (previous != "inspect|" $2) { exit 1 }
        { previous = $0 }
      ' "$abort_calls"; then
      echo "FAIL: control prefix $prefix skipped unused/final-inspect proof" >&2
      exit 1
    fi
    grep -Fxq \
      "authority|control|$abort_control_generation|$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$abort_calls"
    [[ ! -e "$abort_active" ]]
    vp_worker_admission_lock_release
  done

  for prefix in 1 2; do
    begin_abort_fixture "worker-prefix-$prefix"
    database_id="$(abort_secret_id 101)"
    admission_id="$(abort_secret_id 102)"
    record_abort_secret \
      vp-worker-database "$database_id" \
      vp-ffmpeg-worker-go-swarm "$abort_worker_generation" database
    if [[ "$prefix" -eq 2 ]]; then
      record_abort_secret \
        vp-worker-admission "$admission_id" \
        vp-ffmpeg-worker-go-swarm "$abort_worker_generation" admission
    fi
    if ! vp_worker_admission_abort_preparing_transaction preparing_failed; then
      echo "FAIL: worker partial-secret prefix $prefix did not abort" >&2
      exit 1
    fi
    [[ "$(grep -Fc \
      "authority|runtime|vp-ffmpeg-worker-go-swarm|$abort_worker_generation" \
      "$abort_calls")" -eq 1 ]]
    if grep -Eq '^rm\|vp-' "$abort_calls"; then
      echo "FAIL: worker partial-secret abort removed by name" >&2
      exit 1
    fi
    [[ ! -e "$abort_active" ]]
    vp_worker_admission_lock_release
  done

  begin_abort_fixture mid-cleanup-crash
  database_id="$(abort_secret_id 201)"
  admission_id="$(abort_secret_id 202)"
  record_abort_secret \
    vp-worker-database "$database_id" \
    vp-ffmpeg-worker-go-swarm "$abort_worker_generation" database
  record_abort_secret \
    vp-worker-admission "$admission_id" \
    vp-ffmpeg-worker-go-swarm "$abort_worker_generation" admission
  abort_crash_id="$admission_id"
  if vp_worker_admission_abort_preparing_transaction preparing_failed \
    >/dev/null 2>&1; then
    echo 'FAIL: partial-secret crash fixture unexpectedly completed' >&2
    exit 1
  fi
  [[ -e "$abort_active" \
    && "$(grep -Fc "rm|$admission_id" "$abort_calls")" -eq 1 ]]
  abort_crash_id=""
  vp_worker_admission_abort_preparing_transaction preparing_failed
  [[ "$(grep -Fc "rm|$admission_id" "$abort_calls")" -eq 1 \
    && "$(grep -Fc "rm|$database_id" "$abort_calls")" -eq 1 \
    && "$(grep -Fc "secret-ls|$admission_id" "$abort_calls")" -eq 1 \
    && ! -e "$abort_active" ]]
  vp_worker_admission_lock_release

  for mismatch_kind in id label; do
    begin_abort_fixture "replacement-$mismatch_kind-mismatch"
    mismatch_id="$(abort_secret_id 301)"
    record_abort_secret \
      vp-worker-database "$mismatch_id" \
      vp-ffmpeg-worker-go-swarm "$abort_worker_generation" database
    abort_mismatch_id="$mismatch_id"
    abort_mismatch_kind="$mismatch_kind"
    if vp_worker_admission_abort_preparing_transaction preparing_failed \
      >/dev/null 2>&1; then
      echo "FAIL: replacement secret $mismatch_kind mismatch was removed" >&2
      exit 1
    fi
    [[ -e "$abort_active" && "$(grep -c '^rm|' "$abort_calls")" -eq 0 ]]
    vp_worker_admission_lock_release
  done

  begin_abort_fixture mounted-secret
  mounted_id="$(abort_secret_id 401)"
  record_abort_secret \
    vp-worker-database "$mounted_id" \
    vp-ffmpeg-worker-go-swarm "$abort_worker_generation" database
  abort_mounted_secret_id="$mounted_id"
  if vp_worker_admission_abort_preparing_transaction preparing_failed \
    >/dev/null 2>&1; then
    echo 'FAIL: mounted prepared secret was removed' >&2
    exit 1
  fi
  [[ -e "$abort_active" && "$(grep -c '^rm|' "$abort_calls")" -eq 0 ]]
  vp_worker_admission_lock_release
)

(
  wal_root="$TEST_ROOT/authority-wal"
  wal_commit=4123456789abcdef0123456789abcdef01234567
  wal_control_generation="c-${wal_commit:0:20}"
  wal_control_image="vp-ffmpeg-worker-python:deploy-${wal_commit:0:12}"
  wal_owner_file="$wal_root/runtime-owner"
  wal_calls="$wal_root/calls"
  mkdir -p "$wal_root"
  printf 'postgresql://authority-owner:credential@database/videoprocess\n' \
    >"$wal_owner_file"
  chmod 0400 "$wal_owner_file"
  : >"$wal_calls"

  wal_credentials=()
  wal_principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for wal_index in 0 1 2 3; do
    wal_credential="$wal_root/credential-$wal_index"
    printf 'postgresql://wal-%s:credential@database/videoprocess\n' \
      "$wal_index" >"$wal_credential"
    chmod 0400 "$wal_credential"
    wal_credentials+=("$wal_credential")
  done
  wal_credential_records="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      validate-credentials \
      "${wal_credentials[0]}" "${wal_principals[0]}" \
      "${wal_credentials[1]}" "${wal_principals[1]}" \
      "${wal_credentials[2]}" "${wal_principals[2]}" \
      "${wal_credentials[3]}" "${wal_principals[3]}"
  )"

  begin_wal_fixture() {
    local fixture="$1"
    ROOT="$wal_root/$fixture/sync"
    REPO_ROOT="$wal_root/$fixture/repos"
    wal_admission_root="$ROOT/state/vp-worker-admission"
    mkdir -p "$wal_admission_root"
    chmod 0700 "$wal_admission_root"
    vp_worker_admission_lock_acquire "$wal_admission_root"
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
      "$wal_admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$wal_commit" \
      "vp-backend:deploy-${wal_commit:0:12}" \
      "vp-ffmpeg-worker-go:deploy-${wal_commit:0:12}" \
      "$wal_commit" legacy_no_control \
      <<<"$wal_credential_records" >/dev/null
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
    VP_WORKER_ADMISSION_CONTROL_IMAGE="$wal_control_image"
    VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$wal_commit"
    VP_WORKER_CONTROL_GENERATION="$wal_control_generation"
  }

  assert_wal_authority() {
    local state="$1"
    local kind="$2"
    local service="$3"
    local generation="$4"
    local expected_state="${5:-provisioned}"
    python3 - \
      "$state" "$kind" "$service" "$generation" "$expected_state" <<'PY'
import json
import sys

path, kind, service, generation, expected_state = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    document = json.load(handle)
assert document["prepared_secrets"] == []
assert document["authorities"] == [
    {
        "control_generation": "c-4123456789abcdef0123",
        "control_image": "vp-ffmpeg-worker-python:deploy-4123456789ab",
        "generation": generation,
        "kind": kind,
        "operator_reference": (
            "control/c-4123456789abcdef0123/"
            "worker-registration-operator-database-url"
        ),
        "service": service,
        "state": expected_state,
    }
]
PY
  }

  assert_wal_done() {
    local done_root="$1"
    local kind="$2"
    local service="$3"
    local generation="$4"
    python3 - "$done_root" "$kind" "$service" "$generation" <<'PY'
import json
import pathlib
import sys

done_root, kind, service, generation = sys.argv[1:]
paths = list(pathlib.Path(done_root).glob("*/done.json"))
assert len(paths) == 1
with paths[0].open(encoding="utf-8") as handle:
    document = json.load(handle)
assert document["phase"] == "DONE"
assert document["outcome"] == "aborted"
assert document["prepared_secrets"] == []
assert document["abort"]["authorities"] == []
assert document["operation"] is None
assert document["authorities"] == [
    {
        "control_generation": "c-4123456789abcdef0123",
        "control_image": "vp-ffmpeg-worker-python:deploy-4123456789ab",
        "generation": generation,
        "kind": kind,
        "operator_reference": (
            "control/c-4123456789abcdef0123/"
            "worker-registration-operator-database-url"
        ),
        "service": service,
        "state": "revoked",
    }
]
PY
  }

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "$wal_owner_file"
  }
  vp_python_worker_prepare_controlled_directory() {
    mkdir -p "$1"
    chmod 0700 "$1"
    printf '%s\n' "$1"
  }
  vp_worker_control_write_manifest() {
    :
  }
  vp_worker_admission_read_manifest() {
    return 1
  }
  vp_worker_admission_write_manifest() {
    :
  }
  vp_worker_admission_track_candidate() {
    :
  }
  vp_worker_admission_set_candidate() {
    :
  }
  vp_worker_admission_new_generation() {
    printf '%s\n' "$wal_generation"
  }
  vp_run_python_worker_container() {
    printf 'provision|%s\n' "$*" >>"$wal_calls"
  }
  vp_worker_admission_create_secret() {
    printf 'secret|%s|%s|%s\n' "$1" "$3" "$4" >>"$wal_calls"
    return 73
  }
  vp_worker_control_revoke_authority() {
    printf 'revoke|control|%s|%s|%s\n' "$1" "$2" "$3" >>"$wal_calls"
  }
  vp_worker_admission_revoke_generation_authority() {
    printf 'revoke|runtime|%s|%s|%s|%s|%s|%s\n' \
      "$1" "$2" "$3" "$4" "$5" "$6" >>"$wal_calls"
  }

  wal_services=(
    vp-worker-control
    vp-ffmpeg-worker-go-swarm
    vp-ffmpeg-worker-gpu-swarm
    vp-vision-worker-swarm
    vp-youtube-publisher-swarm
  )
  for wal_index in 0 1 2 3 4; do
    (
      : >"$wal_calls"
      wal_service="${wal_services[$wal_index]}"
      wal_kind=runtime
      wal_generation="$((930 + wal_index))"
      wal_image="vp-backend:deploy-${wal_commit:0:12}"
      [[ "$wal_index" -eq 0 ]] && {
        wal_kind=control
        wal_generation="$wal_control_generation"
        wal_image="$wal_control_image"
      }
      begin_wal_fixture "first-secret-$wal_index"
      set +e
      if [[ "$wal_kind" == control ]]; then
        vp_worker_admission_prepare_control_roles \
          "$wal_control_image" "$wal_commit" "$wal_admission_root"
        wal_status=$?
      else
        vp_worker_admission_prepare_service \
          "$wal_service" "$wal_image" "$wal_control_image" \
          "$wal_commit" "$wal_admission_root" "$wal_commit"
        wal_status=$?
      fi
      set -e
      if [[ "$wal_status" -eq 0 ]]; then
        echo "FAIL: $wal_service first-secret failure was ignored" >&2
        exit 1
      fi
      assert_wal_authority \
        "$wal_admission_root/transactions/active.json" \
        "$wal_kind" "$wal_service" "$wal_generation"
      vp_worker_admission_abort_preparing_transaction preparing_failed
      grep -Fq "revoke|$wal_kind|" "$wal_calls"
      [[ ! -e "$wal_admission_root/transactions/active.json" ]]
      assert_wal_done \
        "$wal_admission_root/transactions" \
        "$wal_kind" "$wal_service" "$wal_generation"
      vp_worker_admission_lock_release
    )
  done

  (
    : >"$wal_calls"
    wal_generation=940
    begin_wal_fixture mark-provisioned-crash
    vp_worker_admission_mark_authority_provisioned() {
      printf 'mark-crash\n' >>"$wal_calls"
      return 91
    }
    set +e
    vp_worker_admission_prepare_service \
      vp-ffmpeg-worker-go-swarm \
      "vp-ffmpeg-worker-go:deploy-${wal_commit:0:12}" \
      "$wal_control_image" "$wal_commit" \
      "$wal_admission_root" "$wal_commit"
    wal_status=$?
    set -e
    if [[ "$wal_status" -eq 0 ]] \
      || grep -Fq 'secret|' "$wal_calls"; then
      echo 'FAIL: post-provision WAL mark crash reached secret creation' >&2
      exit 1
    fi
    assert_wal_authority \
      "$wal_admission_root/transactions/active.json" \
      runtime vp-ffmpeg-worker-go-swarm "$wal_generation" provisioning
    vp_worker_admission_lock_release

    source "$EXTENSION"
    [[ -z "$VP_WORKER_CONTROL_GENERATION" \
      && -z "$VP_WORKER_ADMISSION_CONTROL_IMAGE" ]]
    vp_worker_admission_revoke_generation_authority() {
      printf 'revoke|fresh-runtime|%s|%s|%s|%s|%s|%s\n' \
        "$1" "$2" "$3" "$4" "$5" "$6" >>"$wal_calls"
    }
    vp_worker_admission_lock_acquire "$wal_admission_root"
    vp_worker_admission_abort_preparing_transaction preparing_failed
    grep -Fq \
      "revoke|fresh-runtime|vp-ffmpeg-worker-go-swarm|$wal_generation|$wal_admission_root|$wal_control_image|$wal_control_generation|control/$wal_control_generation/worker-registration-operator-database-url" \
      "$wal_calls"
    [[ ! -e "$wal_admission_root/transactions/active.json" ]]
    assert_wal_done \
      "$wal_admission_root/transactions" \
      runtime vp-ffmpeg-worker-go-swarm "$wal_generation"
    vp_worker_admission_lock_release
  )
)

(
  replay_root="$TEST_ROOT/fresh-aborting-replay"
  ROOT="$replay_root/sync"
  REPO_ROOT="$replay_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  mkdir -p "$admission_root"
  chmod 0700 "$admission_root"
  replay_commit=3123456789abcdef0123456789abcdef01234567
  replay_control_generation=c-${replay_commit:0:20}
  replay_control_image="vp-ffmpeg-worker-python:deploy-${replay_commit:0:12}"
  replay_runtime_service=vp-ffmpeg-worker-go-swarm
  replay_runtime_generation=902
  replay_control_id=9111111111111111111111111111111111111111111111111111111111111111
  replay_runtime_id=9222222222222222222222222222222222222222222222222222222222222222
  replay_calls="$replay_root/replay-calls"
  : >"$replay_calls"

  replay_credentials=()
  replay_principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$replay_root/credential-$index"
    printf 'postgresql://fresh-%s:credential@database/videoprocess\n' \
      "$index" >"$credential"
    chmod 0400 "$credential"
    replay_credentials+=("$credential")
  done
  replay_credential_records="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      validate-credentials \
      "${replay_credentials[0]}" "${replay_principals[0]}" \
      "${replay_credentials[1]}" "${replay_principals[1]}" \
      "${replay_credentials[2]}" "${replay_principals[2]}" \
      "${replay_credentials[3]}" "${replay_principals[3]}"
  )"
  vp_worker_admission_lock_acquire "$admission_root"
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$replay_commit" \
    "vp-backend:deploy-${replay_commit:0:12}" \
    "vp-ffmpeg-worker-go:deploy-${replay_commit:0:12}" \
    "$replay_commit" legacy_no_control \
    <<<"$replay_credential_records" >/dev/null
  replay_operator_reference="control/$replay_control_generation/worker-registration-operator-database-url"
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-authority-intent \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    control vp-worker-control "$replay_control_generation" \
    "$replay_control_image" "$replay_control_generation" \
    "$replay_operator_reference" >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioning \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    control vp-worker-control "$replay_control_generation" >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioned \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    control vp-worker-control "$replay_control_generation" >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-authority-intent \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime "$replay_runtime_service" "$replay_runtime_generation" \
    "$replay_control_image" "$replay_control_generation" \
    "$replay_operator_reference" >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioning \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime "$replay_runtime_service" "$replay_runtime_generation" \
    >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioned \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    runtime "$replay_runtime_service" "$replay_runtime_generation" \
    >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-prepared-secret \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    "vp-wc-operator-$replay_control_generation" "$replay_control_id" \
    vp-worker-control "$replay_control_generation" operator >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-prepared-secret \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    "vp-wr-ffmpeg-go-db-$replay_runtime_generation" "$replay_runtime_id" \
    "$replay_runtime_service" "$replay_runtime_generation" database \
    >/dev/null
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin-abort \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    8 preparing_failed >/dev/null

  revision=9
  for secret_id in "$replay_runtime_id" "$replay_control_id"; do
    intent="$(
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        intent-prepared-secret-removal \
        "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$revision" "$secret_id"
    )"
    operation_id="$(
      python3 -c \
        'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
        <<<"$intent"
    )"
    revision=$((revision + 1))
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      complete-prepared-secret-removal \
      "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$revision" "$operation_id" >/dev/null
    revision=$((revision + 1))
  done
  vp_worker_admission_lock_release

  source "$EXTENSION"
  [[ -z "$VP_WORKER_CONTROL_GENERATION" \
    && -z "$VP_WORKER_ADMISSION_CONTROL_IMAGE" ]]
  expected_operator="$admission_root/control/$replay_control_generation/worker-registration-operator-database-url"
  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_generation_state() {
    VP_WORKER_ADMISSION_GENERATION_STATE=active
  }
  vp_worker_admission_retirement_ids() {
    VP_WORKER_ADMISSION_RETIREMENT_IDS=""
  }
  vp_worker_admission_operator() {
    printf 'operator|%s|%s\n' "$1" "$*" >>"$replay_calls"
    [[ "$1" == "$expected_operator" ]]
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "${replay_credentials[3]}"
  }
  vp_run_python_worker_container() {
    printf 'one-shot|%s\n' "$*" >>"$replay_calls"
  }
  docker() {
    if [[ "${1:-} ${2:-}" == "service ls" ]]; then
      return 0
    fi
    return 98
  }

  vp_worker_admission_lock_acquire "$admission_root"
  set +e
  vp_worker_admission_abort_preparing_transaction preparing_failed \
    >/dev/null 2>&1
  replay_status=$?
  set -e
  if [[ "$replay_status" -ne 0 ]]; then
    vp_worker_admission_lock_release
    echo 'FAIL: fresh ABORTING replay did not reconstruct authority context' >&2
    exit 1
  fi
  if grep -Fq '/control//' "$replay_calls" \
    || ! grep -Fq "operator|$expected_operator|" "$replay_calls" \
    || ! grep -Fq \
      "worker_runtime_role_cli revoke --service-name $replay_runtime_service --generation $replay_runtime_generation" \
      "$replay_calls" \
    || ! grep -Fq \
      "worker_control_role_cli revoke --generation $replay_control_generation" \
      "$replay_calls" \
    || [[ -e "$admission_root/transactions/active.json" ]]; then
    echo 'FAIL: fresh ABORTING replay did not converge exact authorities' >&2
    exit 1
  fi
  vp_worker_admission_lock_release
)

(
  restore_root="$TEST_ROOT/preparing-restore-abort"
  ROOT="$restore_root/sync"
  REPO_ROOT="$restore_root/repos"
  mkdir -p "$ROOT/state/vp-worker-admission"
  restore_calls="$restore_root/calls"
  : >"$restore_calls"
  VP_WORKER_ADMISSION_PREPARED=false
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=partial-candidate
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
  vp_restore_app_snapshots() {
    printf 'snapshots|%s|%s\n' "$2" "$3" >>"$restore_calls"
  }
  vp_worker_admission_abort_preparing_transaction() {
    printf 'abort|%s\n' "$1" >>"$restore_calls"
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  }
  vp_worker_admission_retire_records() {
    printf 'legacy-retire\n' >>"$restore_calls"
  }
  vp_worker_admission_discard_namespace() {
    printf 'legacy-discard\n' >>"$restore_calls"
  }
  vp_worker_control_cleanup_candidate() {
    printf 'legacy-control-cleanup\n' >>"$restore_calls"
  }

  vp_restore_worker_admission_transaction \
    synthetic-snapshots synthetic-service "" true
  if [[ "$(sed -n '1p' "$restore_calls")" \
      != 'snapshots|synthetic-service|false' \
    || "$(sed -n '2p' "$restore_calls")" \
      != 'abort|preparing_failed' \
    || "$(wc -l <"$restore_calls" | tr -d ' ')" -ne 2 ]]; then
    echo 'FAIL: permanent PREPARING restore did not select durable abort' >&2
    exit 1
  fi
  [[ "$VP_WORKER_ADMISSION_ROLLBACK_CONVERGED" == true ]]
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
  vp_worker_admission_record_authority_intent() {
    :
  }
  vp_worker_admission_mark_authority_provisioning() {
    :
  }
  vp_worker_admission_mark_authority_provisioned() {
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
  vp_worker_admission_record_authority_intent() {
    :
  }
  vp_worker_admission_mark_authority_provisioning() {
    :
  }
  vp_worker_admission_mark_authority_provisioned() {
    :
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
  VP_WORKER_ADMISSION_GENERATION_STATE="$CLEANUP_GRANT_STATE"
}
vp_worker_admission_retirement_ids() {
  VP_WORKER_ADMISSION_RETIREMENT_IDS=""
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
