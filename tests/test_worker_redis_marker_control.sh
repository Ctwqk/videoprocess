#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$ROOT_DIR/deploy/swarm/worker-redis-marker-control.sh"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
FAKE_BIN="$TEST_ROOT/bin"
DOCKER_CALLS="$TEST_ROOT/docker-calls"
SERVICE_DIR="$TEST_ROOT/services"
SECRET_DIR="$TEST_ROOT/secrets"
CONTROL_EVENTS="$TEST_ROOT/control-events"
FAKE_CRONTAB="$TEST_ROOT/crontab"
FAKE_CRONTAB_WRITE_COUNT="$TEST_ROOT/crontab-write-count"
CONFIG_FILE="$TEST_ROOT/control.conf"
STATE_DIR="$TEST_ROOT/state"
LOCK_DIR="$TEST_ROOT/locks"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -f "$LAUNCHER" ]] || fail "missing worker Redis marker launcher"
[[ -x "$LAUNCHER" ]] || fail "worker Redis marker launcher is not executable"
bash -n "$LAUNCHER"
bash -n "$EXTENSION"

mkdir -p "$FAKE_BIN" "$SERVICE_DIR" "$SECRET_DIR" "$STATE_DIR" "$LOCK_DIR"
: >"$DOCKER_CALLS"
: >"$CONTROL_EVENTS"
printf '0\n' >"$FAKE_CRONTAB_WRITE_COUNT"

cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

service_path() {
  printf '%s/%s.%s\n' "$SERVICE_DIR" "$1" "$2"
}

printf 'docker' >>"$DOCKER_CALLS"
printf '|%s' "$@" >>"$DOCKER_CALLS"
printf '\n' >>"$DOCKER_CALLS"

if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
  printf 'vp-pipeline-network-id\n'
  exit
fi
if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
  name="${3:-}"
  state_path="$(service_path "$name" state)"
  identity_path="$(service_path "$name" identity)"
  [[ -f "$state_path" ]] || exit 1
  if [[ "$*" == *"--format"* ]]; then
    if [[ "$*" == *"vp.worker-redis-marker.mode"* ]]; then
      cat "$identity_path"
    elif [[ -n "${FAKE_SERVICE_GENERATION:-}" ]]; then
      printf '%s\n' "$FAKE_SERVICE_GENERATION"
    else
      cut -d'|' -f3 "$identity_path"
    fi
  fi
  exit
fi
if [[ "${1:-} ${2:-}" == "service ps" ]]; then
  name="${3:-}"
  if [[ "${FAKE_EMPTY_SERVICE_PS:-false}" != true ]]; then
    cat "$(service_path "$name" state)"
  fi
  exit
fi
if [[ "${1:-} ${2:-}" == "service rm" ]]; then
  name="${3:-}"
  generation="$(
    cut -d'|' -f3 "$(service_path "$name" identity)" 2>/dev/null || true
  )"
  printf 'job|remove|%s|%s\n' "$name" "$generation" >>"$CONTROL_EVENTS"
  rm -f "$(service_path "$name" state)" "$(service_path "$name" identity)"
  exit
fi
if [[ "${1:-} ${2:-}" == "service create" ]]; then
  shift 2
  name=""
  mode=""
  generation=""
  image=""
  network=""
  placement=""
  restart=""
  replicas=""
  database_secret=""
  redis_secret=""
  envs=""
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --detach=true|--no-resolve-image)
        shift
        ;;
      --name)
        name="$2"
        shift 2
        ;;
      --mode)
        job_mode="$2"
        shift 2
        ;;
      --replicas)
        replicas="$2"
        shift 2
        ;;
      --restart-condition)
        restart="$2"
        shift 2
        ;;
      --constraint)
        placement="$2"
        shift 2
        ;;
      --network)
        network="$2"
        shift 2
        ;;
      --label)
        case "$2" in
          vp.worker-redis-marker.mode=*)
            mode="${2#*=}"
            ;;
          vp.worker-redis-marker.generation=*)
            generation="${2#*=}"
            ;;
        esac
        shift 2
        ;;
      --secret)
        case "$2" in
          *,target=worker-marker-database-url,*)
            database_secret="${2#source=}"
            database_secret="${database_secret%%,*}"
            ;;
          *,target=worker-marker-redis-url,*)
            redis_secret="${2#source=}"
            redis_secret="${redis_secret%%,*}"
            ;;
        esac
        shift 2
        ;;
      --env)
        envs="${envs:+$envs,}$2"
        shift 2
        ;;
      *)
        image="$1"
        shift
        command_args="$(printf '%s,' "$@")"
        break
        ;;
    esac
  done
  [[ "$job_mode" == replicated-job && "$network" == vp-pipeline-net ]]
  printf '%s\n' \
    "2|$mode|$generation|$image|replicated-job|$replicas|$replicas|$restart|$placement|vp-pipeline-network-id|$database_secret:worker-marker-database-url:256,$redis_secret:worker-marker-redis-url:256|$envs|${command_args%,}" \
    >"$(service_path "$name" identity)"
  created_state=Complete
  if [[ "$mode" == readiness && -n "${FAKE_READINESS_TASK_STATE:-}" ]]; then
    created_state="$FAKE_READINESS_TASK_STATE"
  elif [[ "$mode" == janitor && -n "${FAKE_JANITOR_TASK_STATE:-}" ]]; then
    created_state="$FAKE_JANITOR_TASK_STATE"
  fi
  printf '%s\n' "$created_state" >"$(service_path "$name" state)"
  exit
fi
if [[ "${1:-} ${2:-}" == "service logs" ]]; then
  case "${4:-}" in
    vp-worker-redis-marker-readiness-job)
      case "${FAKE_READINESS_RESULT:-ready}" in
        ready)
          printf '%s\n' \
            '{"checked_count":7,"code":"ready","expected_count":7,"status":"ok"}'
          ;;
        failed)
          printf '%s\n' \
            '{"checked_count":0,"code":"database_unready","expected_count":7,"status":"failed"}'
          ;;
        malformed)
          printf '%s\n' 'not-json'
          ;;
        unavailable)
          exit 3
          ;;
      esac
      ;;
    vp-worker-redis-marker-janitor-job)
      printf '%s\n' \
        '{"absent":1,"claimed":3,"code":"ready","conflict":0,"deleted":2,"status":"ok"}'
      ;;
    *)
      exit 91
      ;;
  esac
  exit
fi
if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
  if [[ "${3:-}" == "${FAKE_MISSING_SECRET:-__none__}" ]]; then
    exit 1
  fi
  if [[ "${3:-}" == vp-wrm-* ]]; then
    [[ -f "$SECRET_DIR/${3:-}" ]]
    exit
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "secret create" ]]; then
  payload="$(cat)"
  [[ -n "$payload" ]]
  printf 'stdin-bytes|%s|%s\n' "${3:-}" "${#payload}" >>"$DOCKER_CALLS"
  if [[ "${3:-}" == "${FAKE_FAIL_SECRET_CREATE:-__none__}" ]]; then
    exit 1
  fi
  : >"$SECRET_DIR/${3:-}"
  printf 'secret|create|%s\n' "${3:-}" >>"$CONTROL_EVENTS"
  exit
fi
if [[ "${1:-} ${2:-}" == "secret rm" ]]; then
  if [[ "${3:-}" == "${FAKE_FAIL_SECRET_REMOVE:-__none__}" ]]; then
    exit 1
  fi
  rm -f "$SECRET_DIR/${3:-}"
  printf 'secret|remove|%s\n' "${3:-}" >>"$CONTROL_EVENTS"
  exit
fi
if [[ "${1:-}" == run ]]; then
  control_state=""
  operation=""
  generation=""
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --mount)
        if [[ "$2" == *",dst=/control-state"* ]]; then
          control_state="${2#*src=}"
          control_state="${control_state%%,*}"
        fi
        shift 2
        ;;
      provision|revoke)
        operation="$1"
        shift
        ;;
      --generation)
        generation="$2"
        shift 2
        ;;
      *)
        shift
        ;;
    esac
  done
  [[ -n "$control_state" && -n "$operation" && -n "$generation" ]]
  printf 'role|%s|%s\n' "$operation" "$generation" >>"$CONTROL_EVENTS"
  if [[ "$operation" == provision ]]; then
    mkdir -p "$control_state/$generation"
    for purpose in readiness janitor repair; do
      printf 'credential-%s-%s\n' "$purpose" "$generation" \
        >"$control_state/$generation/worker-marker-$purpose-database-url"
      chmod 0400 \
        "$control_state/$generation/worker-marker-$purpose-database-url"
    done
  else
    if [[ "$generation" == "${FAKE_FAIL_ROLE_REVOKE:-__none__}" ]]; then
      exit 1
    fi
    rm -rf "$control_state/$generation"
  fi
  exit
fi
exit 92
EOF
chmod +x "$FAKE_BIN/docker"

cat >"$FAKE_BIN/crontab" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == -l ]]; then
  if [[ -f "$FAKE_CRONTAB" ]]; then
    cat "$FAKE_CRONTAB"
    exit
  fi
  echo 'no crontab for video-test' >&2
  exit 1
fi
if [[ "${1:-}" == -r ]]; then
  rm -f "$FAKE_CRONTAB"
  printf 'cron|remove\n' >>"$CONTROL_EVENTS"
  exit
fi
[[ "$#" -eq 1 ]]
if [[ "${FAKE_FAIL_CRONTAB_INSTALL:-false}" == true ]]; then
  exit 1
fi
write_count="$(cat "$FAKE_CRONTAB_WRITE_COUNT")"
write_count="$((write_count + 1))"
printf '%s\n' "$write_count" >"$FAKE_CRONTAB_WRITE_COUNT"
cp "$1" "$FAKE_CRONTAB"
if [[ "$write_count" == "${FAKE_CORRUPT_CRONTAB_ON_WRITE:-__none__}" ]]; then
  printf '# injected verification mismatch\n' >>"$FAKE_CRONTAB"
fi
printf 'cron|install\n' >>"$CONTROL_EVENTS"
EOF
chmod +x "$FAKE_BIN/crontab"

cat >"$CONFIG_FILE" <<'EOF'
GENERATION=release-0123456789ab
IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
NETWORK=vp-pipeline-net
READINESS_DATABASE_SECRET=vp-worker-marker-readiness-db-release-0123456789ab
READINESS_REDIS_SECRET=vp-marker-readiness-redis-runtime-abcdef012345
JANITOR_DATABASE_SECRET=vp-worker-marker-janitor-db-release-0123456789ab
JANITOR_REDIS_SECRET=vp-marker-janitor-redis-runtime-abcdef012345
EOF
chmod 0600 "$CONFIG_FILE"

export DOCKER_CALLS SERVICE_DIR SECRET_DIR CONTROL_EVENTS FAKE_CRONTAB
export FAKE_CRONTAB_WRITE_COUNT
export PATH="$FAKE_BIN:$PATH"
export VP_WORKER_REDIS_MARKER_CONFIG_FILE="$CONFIG_FILE"
export VP_WORKER_REDIS_MARKER_STATE_DIR="$STATE_DIR"
export VP_WORKER_REDIS_MARKER_LOCK_DIR="$LOCK_DIR"
export VP_WORKER_REDIS_MARKER_MAX_WAIT_SECONDS=2

assert_control_job() {
  local mode="$1"
  local name="$2"
  local database_secret="$3"
  local redis_secret="$4"
  local module="$5"
  local command="$6"
  local create

  create="$(grep -F "docker|service|create" "$DOCKER_CALLS" | tail -1)"
  [[ "$create" == *"|--name|$name|"* ]] \
    || fail "$mode did not use its fixed job name"
  [[ "$create" == *"|--replicas|1|"* ]] \
    || fail "$mode did not use one replica"
  [[ "$create" == *"|--mode|replicated-job|"* ]] \
    || fail "$mode did not use Swarm replicated-job mode"
  [[ "$create" == *"|--restart-condition|none|"* ]] \
    || fail "$mode did not disable restarts"
  [[ "$create" == *"|--no-resolve-image|"* ]] \
    || fail "$mode did not preserve the exact reviewed image identity"
  [[ "$create" == *"|--constraint|node.hostname==ccttww-lap|"* ]] \
    || fail "$mode did not use exact host-150 placement"
  [[ "$create" == *"|--network|vp-pipeline-net|"* ]] \
    || fail "$mode did not use the reviewed network"
  [[ "$create" == *"|--secret|source=$database_secret,target=worker-marker-database-url,mode=0400|"* ]] \
    || fail "$mode did not mount its own database secret at mode 0400"
  [[ "$create" == *"|--secret|source=$redis_secret,target=worker-marker-redis-url,mode=0400|"* ]] \
    || fail "$mode did not mount its own Redis secret at mode 0400"
  [[ "$create" == *"|--env|WORKER_REDIS_MARKER_DATABASE_URL_FILE=/run/secrets/worker-marker-database-url|"* ]] \
    || fail "$mode database file environment is missing"
  [[ "$create" == *"|--env|WORKER_REDIS_MARKER_REDIS_URL_FILE=/run/secrets/worker-marker-redis-url|"* ]] \
    || fail "$mode Redis file environment is missing"
  [[ "$create" == *"|vp-ffmpeg-worker-python:deploy-0123456789ab|python|-m|$module|$command"* ]] \
    || fail "$mode did not run the reviewed Python worker/control image"
  if [[ "$create" == *"repair"* || "$create" == *"DATABASE_URL="* \
    || "$create" == *"REDIS_URL=redis"* || "$create" == *"postgresql://"* ]]; then
    fail "$mode mounted repair authority or exposed a credential"
  fi
}

set_service_state() {
  local name="$1"
  local state="$2"
  printf '%s\n' "$state" >"$SERVICE_DIR/$name.state"
}

readiness_output="$("$LAUNCHER" readiness)"
assert_control_job \
  readiness \
  vp-worker-redis-marker-readiness-job \
  vp-worker-marker-readiness-db-release-0123456789ab \
  vp-marker-readiness-redis-runtime-abcdef012345 \
  app.channel_agent.worker_redis_marker_readiness_cli \
  check
[[ "$readiness_output" == *"code=ready"* \
  && "$readiness_output" == *"checked_count=7"* \
  && "$readiness_output" == *"expected_count=7"* ]] \
  || fail "readiness output did not contain only stable status and counts"
[[ -f "$STATE_DIR/readiness.status" ]] \
  || fail "readiness did not persist sanitized status"

set_service_state vp-worker-redis-marker-readiness-job Complete
FAKE_READINESS_TASK_STATE=Failed
export FAKE_READINESS_TASK_STATE
if failed_readiness_output="$("$LAUNCHER" readiness)"; then
  fail "Swarm Failed readiness task unexpectedly succeeded"
else
  failed_readiness_exit=$?
fi
[[ "$failed_readiness_exit" -eq 3 \
  && "$failed_readiness_output" == "mode=readiness code=job_failed" ]] \
  || fail "Swarm Failed readiness task did not use the stable failure result"
[[ ! -e "$STATE_DIR/readiness.status" ]] \
  || fail "failed readiness attempt retained a reusable prior ready status"
if "$LAUNCHER" status >"$TEST_ROOT/failed-readiness-status.out" 2>&1; then
  fail "status reused readiness from before a newer failed task"
fi
grep -Fq 'code=readiness_status_missing' \
  "$TEST_ROOT/failed-readiness-status.out" \
  || fail "failed readiness attempt did not invalidate prior ready status"
unset FAKE_READINESS_TASK_STATE
set_service_state vp-worker-redis-marker-readiness-job Complete
"$LAUNCHER" readiness >/dev/null \
  || fail "readiness did not recover after a failed Swarm task"

for failure_case in rejected timeout malformed-log unavailable-log failed-result; do
  unset FAKE_READINESS_TASK_STATE
  FAKE_READINESS_RESULT=ready
  export FAKE_READINESS_RESULT
  set_service_state vp-worker-redis-marker-readiness-job Complete
  "$LAUNCHER" readiness >/dev/null \
    || fail "$failure_case fixture did not establish prior readiness"
  set_service_state vp-worker-redis-marker-readiness-job Complete
  case "$failure_case" in
    rejected)
      FAKE_READINESS_TASK_STATE=Rejected
      ;;
    timeout)
      FAKE_READINESS_TASK_STATE=Running
      ;;
    malformed-log)
      FAKE_READINESS_RESULT=malformed
      ;;
    unavailable-log)
      FAKE_READINESS_RESULT=unavailable
      ;;
    failed-result)
      FAKE_READINESS_RESULT=failed
      ;;
  esac
  export FAKE_READINESS_RESULT
  if [[ -n "${FAKE_READINESS_TASK_STATE:-}" ]]; then
    export FAKE_READINESS_TASK_STATE
  fi
  if "$LAUNCHER" readiness \
    >"$TEST_ROOT/$failure_case-readiness.out" 2>&1; then
    fail "$failure_case readiness attempt unexpectedly succeeded"
  fi
  if "$LAUNCHER" status \
    >"$TEST_ROOT/$failure_case-status.out" 2>&1; then
    fail "$failure_case readiness attempt reused a prior ready status"
  fi
done
unset FAKE_READINESS_TASK_STATE
FAKE_READINESS_RESULT=ready
export FAKE_READINESS_RESULT
set_service_state vp-worker-redis-marker-readiness-job Complete
"$LAUNCHER" readiness >/dev/null \
  || fail "readiness did not recover after the failure matrix"

: >"$DOCKER_CALLS"
janitor_output="$("$LAUNCHER" janitor)"
assert_control_job \
  janitor \
  vp-worker-redis-marker-janitor-job \
  vp-worker-marker-janitor-db-release-0123456789ab \
  vp-marker-janitor-redis-runtime-abcdef012345 \
  app.channel_agent.worker_redis_marker_janitor_cli \
  run
[[ "$janitor_output" == *"code=ready"* \
  && "$janitor_output" == *"claimed=3"* \
  && "$janitor_output" == *"conflict=0"* ]] \
  || fail "janitor output did not contain only stable status and counts"

: >"$DOCKER_CALLS"
set_service_state vp-worker-redis-marker-readiness-job Running
running_output="$("$LAUNCHER" readiness)"
[[ "$running_output" == "mode=readiness code=job_running" ]] \
  || fail "running readiness job was not a clean skip"
if grep -Eq 'service\|(create|rm)' "$DOCKER_CALLS"; then
  fail "running readiness job was created or removed"
fi
if "$LAUNCHER" status >"$TEST_ROOT/running-readiness-status.out" 2>&1; then
  fail "overlapping nonterminal readiness job reused prior ready status"
fi

: >"$DOCKER_CALLS"
set_service_state vp-worker-redis-marker-readiness-job Complete
"$LAUNCHER" readiness >/dev/null
grep -Fxq \
  'docker|service|rm|vp-worker-redis-marker-readiness-job' \
  "$DOCKER_CALLS" \
  || fail "completed readiness job was not removed"
if grep -F 'docker|service|rm|' "$DOCKER_CALLS" \
  | grep -Fvq 'vp-worker-redis-marker-readiness-job'; then
  fail "launcher removed a job outside its mode"
fi

: >"$DOCKER_CALLS"
"$LAUNCHER" readiness >/dev/null
set_service_state vp-worker-redis-marker-readiness-job Complete
sed 's/^2|readiness|/2|janitor|/' \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity" \
  >"$TEST_ROOT/wrong-identity"
mv "$TEST_ROOT/wrong-identity" \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity"
: >"$DOCKER_CALLS"
if "$LAUNCHER" readiness >"$TEST_ROOT/wrong-identity.out" 2>&1; then
  fail "wrong-label fixed-name service was accepted"
fi
if grep -Fq \
  'docker|service|rm|vp-worker-redis-marker-readiness-job' \
  "$DOCKER_CALLS"; then
  fail "wrong-label fixed-name service was removed"
fi
grep -Fq 'code=job_identity_invalid' "$TEST_ROOT/wrong-identity.out" \
  || fail "wrong-label fixed-name service did not fail closed"
rm -f \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.state" \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity"

LOCK_READY="$TEST_ROOT/kernel-lock-ready"
python3 - "$LOCK_DIR/readiness.lock" "$LOCK_READY" <<'PY' &
import fcntl
import os
import signal
import sys
import time

lock_path, ready_path = sys.argv[1:]
with open(lock_path, "a", encoding="utf-8") as lock_file:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    with open(ready_path, "w", encoding="utf-8"):
        pass
    while True:
        time.sleep(1)
PY
LOCK_HOLDER_PID=$!
for _ in 1 2 3 4 5; do
  [[ -f "$LOCK_READY" ]] && break
  sleep 1
done
[[ -f "$LOCK_READY" ]] || fail "kernel lock holder did not start"
: >"$DOCKER_CALLS"
lock_output="$("$LAUNCHER" readiness)"
[[ "$lock_output" == "mode=readiness code=lock_busy" ]] \
  || fail "real kernel lock contention was not rejected"
[[ ! -s "$DOCKER_CALLS" ]] \
  || fail "contended launcher reached Docker"
kill -9 "$LOCK_HOLDER_PID"
wait "$LOCK_HOLDER_PID" 2>/dev/null || true
: >"$DOCKER_CALLS"
"$LAUNCHER" readiness >/dev/null \
  || fail "SIGKILL-released kernel lock stayed permanently busy"
grep -Fq 'docker|service|create' "$DOCKER_CALLS" \
  || fail "launcher did not proceed after kernel lock holder crash"

cat >"$FAKE_BIN/flock" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_FLOCK_CALLS"
exit "${FAKE_FLOCK_STATUS:?}"
EOF
chmod +x "$FAKE_BIN/flock"
FAKE_FLOCK_CALLS="$TEST_ROOT/flock-calls"
export FAKE_FLOCK_CALLS
: >"$FAKE_FLOCK_CALLS"
: >"$DOCKER_CALLS"
fake_contention_output="$(
  FAKE_FLOCK_STATUS=75 "$LAUNCHER" readiness
)"
[[ "$fake_contention_output" == "mode=readiness code=lock_busy" ]] \
  || fail "native flock contention was not a clean skip"
[[ ! -s "$DOCKER_CALLS" ]] \
  || fail "native flock contention reached Docker"

: >"$DOCKER_CALLS"
if FAKE_FLOCK_STATUS=69 "$LAUNCHER" readiness \
  >"$TEST_ROOT/flock-operational-error.out" 2>&1; then
  fail "native flock operational failure was accepted as contention"
fi
grep -Fxq \
  'mode=readiness code=lock_unavailable' \
  "$TEST_ROOT/flock-operational-error.out" \
  || fail "native flock operational failure lacked a stable reason code"
[[ ! -s "$DOCKER_CALLS" ]] \
  || fail "native flock operational failure reached Docker"
rm -f "$FAKE_BIN/flock"

: >"$DOCKER_CALLS"
rm -f "$STATE_DIR/readiness.status" "$STATE_DIR/janitor.status"
VP_WORKER_REDIS_MARKER_DRY_RUN=1 "$LAUNCHER" readiness \
  >"$TEST_ROOT/dry-run.out"
[[ ! -s "$DOCKER_CALLS" ]] \
  || fail "dry-run called Docker"
[[ ! -e "$STATE_DIR/readiness.status" && ! -e "$STATE_DIR/janitor.status" ]] \
  || fail "dry-run changed marker control status"
grep -Fxq 'mode=readiness code=dry_run' "$TEST_ROOT/dry-run.out" \
  || fail "dry-run status was not sanitized"

if status_output="$("$LAUNCHER" status)"; then
  fail "status succeeded without fresh readiness"
else
  status_exit=$?
fi
[[ "$status_exit" -eq 3 ]] \
  || fail "unready status did not use the stable failure exit"
[[ "$status_output" == *"code=readiness_status_missing"* ]] \
  || fail "status did not fail closed without fresh readiness"

write_readiness_status() {
  local extra="${1:-}"
  {
    printf 'GENERATION=release-0123456789ab\n'
    printf 'RECORDED_AT=%s\n' "$(date +%s)"
    printf 'CODE=ready\n'
    printf 'CHECKED_COUNT=7\n'
    printf 'EXPECTED_COUNT=7\n'
    [[ -z "$extra" ]] || printf '%s\n' "$extra"
  } >"$STATE_DIR/readiness.status"
  chmod 0600 "$STATE_DIR/readiness.status"
}

for malformed_status in \
  'UNKNOWN=value' \
  'CODE=ready' \
  'MALFORMED' \
  'CHECKED_COUNT=7=extra'; do
  write_readiness_status "$malformed_status"
  if "$LAUNCHER" status >"$TEST_ROOT/status-invalid.out" 2>&1; then
    fail "status parser accepted malformed record: $malformed_status"
  fi
  grep -Fq 'code=readiness_status_invalid' \
    "$TEST_ROOT/status-invalid.out" \
    || fail "malformed status did not use stable rejection"
done
write_readiness_status
sed '/^EXPECTED_COUNT=/d' "$STATE_DIR/readiness.status" \
  >"$TEST_ROOT/readiness-status-missing"
mv "$TEST_ROOT/readiness-status-missing" "$STATE_DIR/readiness.status"
chmod 0600 "$STATE_DIR/readiness.status"
if "$LAUNCHER" status >"$TEST_ROOT/status-missing-field.out" 2>&1; then
  fail "status parser accepted a missing allowlisted field"
fi
grep -Fq 'code=readiness_status_invalid' \
  "$TEST_ROOT/status-missing-field.out" \
  || fail "missing status field did not use stable rejection"

for forbidden in \
  10.0.0.126 \
  CASPERs-Mac-mini \
  colima-swarmbridged \
  node.hostname==colima-126; do
  sed "s#NETWORK=vp-pipeline-net#NETWORK=$forbidden#" \
    "$CONFIG_FILE" >"$TEST_ROOT/forbidden.conf"
  chmod 0600 "$TEST_ROOT/forbidden.conf"
  if VP_WORKER_REDIS_MARKER_CONFIG_FILE="$TEST_ROOT/forbidden.conf" \
    "$LAUNCHER" readiness >/dev/null 2>&1; then
    fail "launcher accepted forbidden 126 target: $forbidden"
  fi
done

if "$LAUNCHER" repair >/dev/null 2>&1; then
  fail "repair was exposed as a launcher mode"
fi
if "$LAUNCHER" unknown >/dev/null 2>&1; then
  fail "unknown launcher mode was accepted"
fi

grep -Fq \
  'app.channel_agent.worker_redis_marker_readiness_cli' \
  "$ROOT_DIR/backend/Dockerfile.worker" \
  || fail "Python worker/control image does not verify readiness CLI"
grep -Fq \
  'app.channel_agent.worker_redis_marker_janitor_cli' \
  "$ROOT_DIR/backend/Dockerfile.worker" \
  || fail "Python worker/control image does not verify janitor CLI"
grep -Fq \
  'app.services.worker_redis_marker_repair_cli' \
  "$ROOT_DIR/backend/Dockerfile.worker" \
  || fail "Python worker/control image does not verify repair CLI"
if grep -Eq 'python|worker_redis_marker|app/' \
  "$ROOT_DIR/backend/Dockerfile.ffmpeg-worker-go"; then
  fail "Go worker image acquired an ad hoc Python marker-control runtime"
fi

REPO_ROOT="$ROOT_DIR"
ROOT="$TEST_ROOT/deploy-root"
VP_WORKER_REDIS_MARKER_CONTROL_SOURCE="$LAUNCHER"
export REPO_ROOT ROOT VP_WORKER_REDIS_MARKER_CONTROL_SOURCE
log() {
  :
}
source "$EXTENSION"

: >"$DOCKER_CALLS"
rm -rf "$ROOT"
UPDATE_SERVICES=0
vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  || fail "disabled service updates rejected marker-control dry-run"
vp_require_worker_redis_marker_status \
  || fail "disabled service updates rejected marker-control status dry-run"
[[ ! -s "$DOCKER_CALLS" && ! -e "$ROOT" ]] \
  || fail "deployment dry-run mutated Docker or marker-control state"
UPDATE_SERVICES=1

BAD_ACTIVE_CONFIG="$TEST_ROOT/bad-active-control.conf"
cat >"$BAD_ACTIVE_CONFIG" <<'EOF'
GENERATION=m-0123456789ab-1780000000-9999
IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
EOF
chmod 0600 "$BAD_ACTIVE_CONFIG"
if vp_worker_redis_marker_read_prior_config "$BAD_ACTIVE_CONFIG"; then
  fail "active control config accepted missing network and secret bindings"
fi

RETIREMENT_IMAGE=vp-ffmpeg-worker-python:retirement-identity
RETIREMENT_GENERATION=m-retirement-1780000000-9999
VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=vp-marker-readiness-redis-retirement
VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=vp-marker-janitor-redis-retirement
RETIREMENT_JOB=vp-worker-redis-marker-readiness-job
printf '%s\n' Complete >"$SERVICE_DIR/$RETIREMENT_JOB.state"
vp_worker_redis_marker_expected_job_identity \
  "$RETIREMENT_IMAGE" "$RETIREMENT_GENERATION" readiness \
  >"$SERVICE_DIR/$RETIREMENT_JOB.identity"
FAKE_EMPTY_SERVICE_PS=true
export FAKE_EMPTY_SERVICE_PS
: >"$DOCKER_CALLS"
if vp_worker_redis_marker_remove_generation_jobs \
  "$RETIREMENT_IMAGE" "$RETIREMENT_GENERATION" 2>/dev/null; then
  fail "generation retirement accepted an empty Swarm task set"
fi
unset FAKE_EMPTY_SERVICE_PS
if grep -Fq "docker|service|rm|$RETIREMENT_JOB" "$DOCKER_CALLS"; then
  fail "generation retirement removed a job with an empty Swarm task set"
fi

sed 's/^2|readiness|/0||/' \
  "$SERVICE_DIR/$RETIREMENT_JOB.identity" \
  >"$TEST_ROOT/unlabeled-retirement-identity"
mv "$TEST_ROOT/unlabeled-retirement-identity" \
  "$SERVICE_DIR/$RETIREMENT_JOB.identity"
: >"$DOCKER_CALLS"
if vp_worker_redis_marker_remove_generation_jobs \
  "$RETIREMENT_IMAGE" "$RETIREMENT_GENERATION" 2>/dev/null; then
  fail "generation retirement accepted an unlabeled fixed-name job"
fi
if grep -Fq "docker|service|rm|$RETIREMENT_JOB" "$DOCKER_CALLS"; then
  fail "generation retirement removed an unlabeled fixed-name job"
fi

vp_worker_redis_marker_expected_job_identity \
  vp-ffmpeg-worker-python:wrong-image \
  "$RETIREMENT_GENERATION" \
  readiness \
  >"$SERVICE_DIR/$RETIREMENT_JOB.identity"
: >"$DOCKER_CALLS"
if vp_worker_redis_marker_remove_generation_jobs \
  "$RETIREMENT_IMAGE" "$RETIREMENT_GENERATION" 2>/dev/null; then
  fail "generation retirement accepted a wrong fixed-name job identity"
fi
if grep -Fq "docker|service|rm|$RETIREMENT_JOB" "$DOCKER_CALLS"; then
  fail "generation retirement removed a wrong fixed-name job identity"
fi
rm -f \
  "$SERVICE_DIR/$RETIREMENT_JOB.state" \
  "$SERVICE_DIR/$RETIREMENT_JOB.identity"

RUNTIME_GENERATION="abcdef0123456789abcdef0123456789abcdef01"
RUNTIME_STATE="$TEST_ROOT/runtime-state"
write_runtime_state() {
  local generation="$1"
  local identity="$2"
  local aof_enabled="$3"
  local aof_status="$4"
  local policy="$5"
  local readiness_secret="${6:-vp-marker-readiness-redis-runtime-abcdef012345}"
  local janitor_secret="${7:-vp-marker-janitor-redis-runtime-abcdef012345}"
  local repair_secret="${8:-vp-marker-repair-redis-runtime-abcdef012345}"
  if [[ -e "$RUNTIME_STATE" ]]; then
    chmod 0600 "$RUNTIME_STATE"
  fi
  cat >"$RUNTIME_STATE" <<EOF
GENERATION=$generation
ACL_IDENTITY=$identity
AOF_ENABLED=$aof_enabled
AOF_STATUS=$aof_status
MAXMEMORY_POLICY=$policy
NETWORK=vp-pipeline-net
READINESS_REDIS_SECRET=$readiness_secret
JANITOR_REDIS_SECRET=$janitor_secret
REPAIR_REDIS_SECRET=$repair_secret
EOF
  chmod 0400 "$RUNTIME_STATE"
}

VP_WORKER_REDIS_RUNTIME_STATE_FILE="$RUNTIME_STATE"
VP_WORKER_REDIS_RUNTIME_GENERATION="$RUNTIME_GENERATION"
export VP_WORKER_REDIS_RUNTIME_STATE_FILE VP_WORKER_REDIS_RUNTIME_GENERATION
write_runtime_state \
  "$RUNTIME_GENERATION" vp-marker-acl-v1 yes ok noeviction
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "exact runtime marker state was rejected"

for invalid_case in identity aof-enabled aof-status policy generation; do
  case "$invalid_case" in
    identity)
      write_runtime_state \
        "$RUNTIME_GENERATION" default yes ok noeviction
      ;;
    aof-enabled)
      write_runtime_state \
        "$RUNTIME_GENERATION" vp-marker-acl-v1 no ok noeviction
      ;;
    aof-status)
      write_runtime_state \
        "$RUNTIME_GENERATION" vp-marker-acl-v1 yes error noeviction
      ;;
    policy)
      write_runtime_state \
        "$RUNTIME_GENERATION" vp-marker-acl-v1 yes ok allkeys-lru
      ;;
    generation)
      write_runtime_state \
        1111111111111111111111111111111111111111 \
        vp-marker-acl-v1 yes ok noeviction
      ;;
  esac
  if vp_require_worker_redis_runtime_state >/dev/null 2>&1; then
    fail "runtime marker state accepted invalid $invalid_case"
  fi
done

write_runtime_state \
  "$RUNTIME_GENERATION" vp-marker-acl-v1 yes ok noeviction
FAKE_MISSING_SECRET=vp-marker-janitor-redis-runtime-abcdef012345
export FAKE_MISSING_SECRET
if vp_require_worker_redis_runtime_state >/dev/null 2>&1; then
  fail "runtime marker state accepted a missing ACL secret"
fi
unset FAKE_MISSING_SECRET
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "exact runtime state did not recover after a missing secret"

cat >"$FAKE_CRONTAB" <<'EOF'
*/2 * * * * /srv/deploy/bin/deploy-github-sync.sh videoprocess
*/3 * * * * /srv/deploy/bin/deploy-github-sync.sh policy-decision-service
*/7 * * * * /srv/deploy/bin/deploy-github-sync.sh feature-aggregator
*/11 * * * * /srv/deploy/bin/channel-schedule-watch.sh
EOF
CRON_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
CRON_GENERATION=m-0123456789ab-1780000000-4321
vp_install_worker_redis_marker_control \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  "$CRON_GENERATION" \
  "$CRON_CONTROL_ROOT" \
  || fail "worker marker launcher and cron were not installed"
rm -f \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.state" \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity"
vp_run_worker_redis_marker_readiness "$CRON_CONTROL_ROOT" \
  || fail "worker gate fixture did not establish readiness"
FAKE_READINESS_TASK_STATE=Failed
export FAKE_READINESS_TASK_STATE
if vp_run_worker_redis_marker_readiness "$CRON_CONTROL_ROOT" \
  >"$TEST_ROOT/worker-gate-failed-readiness.out" 2>&1; then
  fail "worker gate fixture accepted a newer failed readiness task"
fi
unset FAKE_READINESS_TASK_STATE
if vp_require_worker_redis_marker_status; then
  fail "registered-worker gate reused readiness from before a newer failed task"
fi
for independent_cron in \
  '*/2 * * * * /srv/deploy/bin/deploy-github-sync.sh videoprocess' \
  '*/3 * * * * /srv/deploy/bin/deploy-github-sync.sh policy-decision-service' \
  '*/7 * * * * /srv/deploy/bin/deploy-github-sync.sh feature-aggregator' \
  '*/11 * * * * /srv/deploy/bin/channel-schedule-watch.sh'; do
  grep -Fxq "$independent_cron" "$FAKE_CRONTAB" \
    || fail "marker cron changed independent schedule: $independent_cron"
done
[[ "$(grep -Fc '# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL' "$FAKE_CRONTAB")" -eq 1 \
  && "$(grep -Fc '# END VIDEOPROCESS WORKER REDIS MARKER CONTROL' "$FAKE_CRONTAB")" -eq 1 ]] \
  || fail "marker cron block was not unique"
[[ "$(grep -Ec '^\* \* \* \* \* .*worker-redis-marker-control.sh readiness ' "$FAKE_CRONTAB")" -eq 1 ]] \
  || fail "readiness cron was not exactly every minute"
[[ "$(grep -Ec '^\*/5 \* \* \* \* .*worker-redis-marker-control.sh janitor ' "$FAKE_CRONTAB")" -eq 1 ]] \
  || fail "janitor cron was not exactly every five minutes"
if grep -F 'worker-redis-marker-control.sh repair' "$FAKE_CRONTAB"; then
  fail "repair was installed in cron"
fi
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-once"
vp_install_worker_redis_marker_control \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  "$CRON_GENERATION" \
  "$CRON_CONTROL_ROOT" \
  || fail "repeated marker cron install failed"
cmp -s "$TEST_ROOT/cron-once" "$FAKE_CRONTAB" \
  || fail "marker cron install was not idempotent"

cp "$ROOT/bin/worker-redis-marker-control.sh" \
  "$TEST_ROOT/launcher-before-malformed"
cp "$CRON_CONTROL_ROOT/control.conf" \
  "$TEST_ROOT/config-before-malformed"
printf '# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL\n' \
  >>"$FAKE_CRONTAB"
cp "$FAKE_CRONTAB" "$TEST_ROOT/malformed-cron-before"
if vp_install_worker_redis_marker_control \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  m-0123456789ab-1780000000-5555 \
  "$CRON_CONTROL_ROOT" >/dev/null 2>&1; then
  fail "malformed marker cron unexpectedly converged"
fi
cmp -s "$TEST_ROOT/malformed-cron-before" "$FAKE_CRONTAB" \
  || fail "malformed marker cron changed the prior crontab"
cmp -s "$TEST_ROOT/launcher-before-malformed" \
  "$ROOT/bin/worker-redis-marker-control.sh" \
  || fail "malformed marker cron changed the installed launcher"
cmp -s "$TEST_ROOT/config-before-malformed" \
  "$CRON_CONTROL_ROOT/control.conf" \
  || fail "malformed marker cron changed the active config"

ROLE_STATE="$TEST_ROOT/role-state"
CONTROL_GENERATION=m-0123456789ab-1780000000-1234
mkdir -p "$ROLE_STATE/roles/$CONTROL_GENERATION"
for purpose in readiness janitor repair; do
  printf 'credential-%s\n' "$purpose" \
    >"$ROLE_STATE/roles/$CONTROL_GENERATION/worker-marker-$purpose-database-url"
  chmod 0400 \
    "$ROLE_STATE/roles/$CONTROL_GENERATION/worker-marker-$purpose-database-url"
done
: >"$DOCKER_CALLS"
vp_worker_redis_marker_create_database_secrets \
  "$CONTROL_GENERATION" "$ROLE_STATE" \
  || fail "generation-scoped database secrets were not created"
[[ "$(grep -c '^docker|secret|create|' "$DOCKER_CALLS")" -eq 3 ]] \
  || fail "database secret provisioning did not create exactly three secrets"
[[ "$(grep -c '^stdin-bytes|' "$DOCKER_CALLS")" -eq 3 ]] \
  || fail "database secrets were not supplied through stdin"
for purpose in readiness janitor repair; do
  grep -Fq \
    "docker|secret|create|vp-wrm-$purpose-db-$CONTROL_GENERATION|-" \
    "$DOCKER_CALLS" \
    || fail "$purpose database secret was not generation-scoped"
  if grep -Fq "credential-$purpose" "$DOCKER_CALLS"; then
    fail "$purpose database credential was printed"
  fi
done

OWNER_DATABASE_FILE="$TEST_ROOT/owner-database-url"
printf 'owner-database-credential\n' >"$OWNER_DATABASE_FILE"
chmod 0400 "$OWNER_DATABASE_FILE"
VP_WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE="$OWNER_DATABASE_FILE"
export VP_WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE

reset_marker_transaction_fixture() {
  local name="$1"
  ROOT="$TEST_ROOT/transactions/$name"
  export ROOT
  rm -rf "$ROOT" "$SERVICE_DIR" "$SECRET_DIR"
  mkdir -p "$SERVICE_DIR" "$SECRET_DIR"
  cat >"$FAKE_CRONTAB" <<'EOF'
*/2 * * * * /srv/deploy/bin/deploy-github-sync.sh videoprocess
*/3 * * * * /srv/deploy/bin/deploy-github-sync.sh policy-decision-service
*/7 * * * * /srv/deploy/bin/deploy-github-sync.sh feature-aggregator
*/11 * * * * /srv/deploy/bin/channel-schedule-watch.sh
EOF
  : >"$DOCKER_CALLS"
  : >"$CONTROL_EVENTS"
  printf '0\n' >"$FAKE_CRONTAB_WRITE_COUNT"
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
  unset FAKE_FAIL_ROLE_REVOKE FAKE_FAIL_SECRET_CREATE
  unset FAKE_FAIL_SECRET_REMOVE FAKE_READINESS_TASK_STATE
  unset FAKE_FAIL_CRONTAB_INSTALL FAKE_CORRUPT_CRONTAB_ON_WRITE
  FAKE_READINESS_RESULT=ready
  export FAKE_READINESS_RESULT
}

managed_backup_matches() {
  local control_root="$1"
  local expected_launcher="$2"
  local expected_config="$3"
  local expected_crontab="$4"
  local state
  for state in "$control_root"/.managed-state.*; do
    [[ -d "$state" ]] || continue
    if cmp -s "$expected_launcher" "$state/launcher" \
      && cmp -s "$expected_config" "$state/control.conf" \
      && cmp -s "$expected_crontab" "$state/crontab"; then
      return 0
    fi
  done
  return 1
}

event_line() {
  local event="$1"
  grep -nF "$event" "$CONTROL_EVENTS" | tail -1 | cut -d: -f1
}

reset_marker_transaction_fixture first-ever-unready
FAKE_READINESS_RESULT=failed
export FAKE_READINESS_RESULT
if vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:first-ever-unready >/dev/null 2>&1; then
  fail "first-ever unready marker candidate unexpectedly prepared"
fi
FIRST_GENERATION="$(
  awk -F'|' '$1 == "role" && $2 == "provision" { print $3; exit }' \
    "$CONTROL_EVENTS"
)"
[[ -n "$FIRST_GENERATION" ]] \
  || fail "first-ever failure did not provision a candidate generation"
FIRST_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
[[ ! -e "$FIRST_CONTROL_ROOT/control.conf" \
  && ! -e "$ROOT/bin/worker-redis-marker-control.sh" ]] \
  || fail "first-ever failure did not restore prior launcher/config absence"
if grep -Fq '# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL' \
  "$FAKE_CRONTAB"; then
  fail "first-ever failure left managed marker cron active"
fi
for independent_cron in \
  '*/2 * * * * /srv/deploy/bin/deploy-github-sync.sh videoprocess' \
  '*/3 * * * * /srv/deploy/bin/deploy-github-sync.sh policy-decision-service' \
  '*/7 * * * * /srv/deploy/bin/deploy-github-sync.sh feature-aggregator' \
  '*/11 * * * * /srv/deploy/bin/channel-schedule-watch.sh'; do
  grep -Fxq "$independent_cron" "$FAKE_CRONTAB" \
    || fail "first-ever cleanup changed independent cron: $independent_cron"
done
[[ ! -e "$FIRST_CONTROL_ROOT/roles/$FIRST_GENERATION" ]] \
  || fail "first-ever failure retained database credential files"
for purpose in readiness janitor repair; do
  [[ ! -e "$SECRET_DIR/vp-wrm-$purpose-db-$FIRST_GENERATION" ]] \
    || fail "first-ever failure retained $purpose database secret"
done
FIRST_CRON_RESTORE_LINE="$(event_line 'cron|install')"
FIRST_JOB_REMOVE_LINE="$(
  event_line "job|remove|vp-worker-redis-marker-readiness-job|$FIRST_GENERATION"
)"
FIRST_ROLE_REVOKE_LINE="$(event_line "role|revoke|$FIRST_GENERATION")"
FIRST_SECRET_REMOVE_LINE="$(
  event_line "secret|remove|vp-wrm-readiness-db-$FIRST_GENERATION"
)"
if [[ -z "$FIRST_CRON_RESTORE_LINE" || -z "$FIRST_JOB_REMOVE_LINE" \
  || -z "$FIRST_ROLE_REVOKE_LINE" || -z "$FIRST_SECRET_REMOVE_LINE" \
  || "$FIRST_CRON_RESTORE_LINE" -ge "$FIRST_JOB_REMOVE_LINE" \
  || "$FIRST_JOB_REMOVE_LINE" -ge "$FIRST_ROLE_REVOKE_LINE" \
  || "$FIRST_ROLE_REVOKE_LINE" -ge "$FIRST_SECRET_REMOVE_LINE" ]]; then
  fail "first-ever cleanup order was not cron/config -> job -> role -> secret"
fi

reset_marker_transaction_fixture running-candidate
FAKE_READINESS_TASK_STATE=Running
export FAKE_READINESS_TASK_STATE
if vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:running-candidate >/dev/null 2>&1; then
  fail "running unready marker candidate unexpectedly prepared"
fi
RUNNING_GENERATION="$(
  awk -F'|' '$1 == "role" && $2 == "provision" { print $3; exit }' \
    "$CONTROL_EVENTS"
)"
[[ -n "$RUNNING_GENERATION" ]] \
  || fail "running candidate generation was not provisioned"
if grep -Fq "role|revoke|$RUNNING_GENERATION" "$CONTROL_EVENTS"; then
  fail "possibly running candidate had its database roles revoked"
fi
[[ -d "$ROOT/state/worker-redis-marker-control/roles/$RUNNING_GENERATION" ]] \
  || fail "possibly running candidate lost credential files"
for purpose in readiness janitor repair; do
  [[ -e "$SECRET_DIR/vp-wrm-$purpose-db-$RUNNING_GENERATION" ]] \
    || fail "possibly running candidate lost $purpose database secret"
done
[[ ! -e "$ROOT/state/worker-redis-marker-control/control.conf" \
  && ! -e "$ROOT/bin/worker-redis-marker-control.sh" ]] \
  || fail "possibly running candidate controls were not deactivated"

reset_marker_transaction_fixture partial-secret
PARTIAL_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$PARTIAL_CONTROL_ROOT"
PARTIAL_GENERATION=m-partial-secret-1780000000-0001
FAKE_FAIL_SECRET_CREATE="vp-wrm-janitor-db-$PARTIAL_GENERATION"
export FAKE_FAIL_SECRET_CREATE
if vp_worker_redis_marker_provision_generation \
  vp-ffmpeg-worker-python:partial-secret \
  "$PARTIAL_GENERATION" \
  "$PARTIAL_CONTROL_ROOT" >/dev/null 2>&1; then
  fail "partial database secret failure unexpectedly provisioned"
fi
[[ ! -e "$PARTIAL_CONTROL_ROOT/roles/$PARTIAL_GENERATION" ]] \
  || fail "partial secret failure retained credential files"
[[ ! -e "$SECRET_DIR/vp-wrm-readiness-db-$PARTIAL_GENERATION" ]] \
  || fail "partial secret failure retained the created readiness secret"
PARTIAL_ROLE_REVOKE_LINE="$(event_line "role|revoke|$PARTIAL_GENERATION")"
PARTIAL_SECRET_REMOVE_LINE="$(
  event_line "secret|remove|vp-wrm-readiness-db-$PARTIAL_GENERATION"
)"
if [[ -z "$PARTIAL_ROLE_REVOKE_LINE" || -z "$PARTIAL_SECRET_REMOVE_LINE" \
  || "$PARTIAL_ROLE_REVOKE_LINE" -ge "$PARTIAL_SECRET_REMOVE_LINE" ]]; then
  fail "partial secret cleanup did not revoke roles before deleting secrets"
fi

reset_marker_transaction_fixture generation-failure
GENERATION_FAILURE_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$GENERATION_FAILURE_CONTROL_ROOT"
GENERATION_FAILURE_PRIOR=m-generation-prior-1780000000-0001
GENERATION_FAILURE_IMAGE=vp-ffmpeg-worker-python:generation-prior
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "generation failure fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$GENERATION_FAILURE_IMAGE" \
  "$GENERATION_FAILURE_PRIOR" \
  "$GENERATION_FAILURE_CONTROL_ROOT" \
  || fail "generation failure prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$GENERATION_FAILURE_IMAGE" \
  "$GENERATION_FAILURE_PRIOR" \
  "$GENERATION_FAILURE_CONTROL_ROOT" \
  || fail "generation failure prior controls were not installed"
cp "$FAKE_CRONTAB" "$TEST_ROOT/generation-failure-crontab"
cp "$GENERATION_FAILURE_CONTROL_ROOT/control.conf" \
  "$TEST_ROOT/generation-failure-control.conf"
eval "$(
  declare -f vp_worker_redis_marker_new_generation \
    | sed '1s/vp_worker_redis_marker_new_generation/vp_worker_redis_marker_new_generation_real/'
)"
vp_worker_redis_marker_new_generation() {
  return 1
}
if vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:generation-failure >/dev/null 2>&1; then
  fail "generation-name failure unexpectedly prepared controls"
fi
cmp -s "$TEST_ROOT/generation-failure-crontab" "$FAKE_CRONTAB" \
  || fail "generation-name failure did not restore managed cron"
cmp -s "$TEST_ROOT/generation-failure-control.conf" \
  "$GENERATION_FAILURE_CONTROL_ROOT/control.conf" \
  || fail "generation-name failure did not preserve active config"
managed_backup_matches \
  "$GENERATION_FAILURE_CONTROL_ROOT" \
  "$ROOT/bin/worker-redis-marker-control.sh" \
  "$TEST_ROOT/generation-failure-control.conf" \
  "$TEST_ROOT/generation-failure-crontab" \
  || fail "generation-name failure did not retain its exact managed-state backup"
eval "$(
  declare -f vp_worker_redis_marker_new_generation_real \
    | sed '1s/vp_worker_redis_marker_new_generation_real/vp_worker_redis_marker_new_generation/'
)"

reset_marker_transaction_fixture deactivation-failure
DEACTIVATION_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$DEACTIVATION_CONTROL_ROOT"
DEACTIVATION_PRIOR=m-deactivation-prior-1780000000-0001
DEACTIVATION_IMAGE=vp-ffmpeg-worker-python:deactivation-prior
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "deactivation failure fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$DEACTIVATION_IMAGE" \
  "$DEACTIVATION_PRIOR" \
  "$DEACTIVATION_CONTROL_ROOT" \
  || fail "deactivation failure prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$DEACTIVATION_IMAGE" \
  "$DEACTIVATION_PRIOR" \
  "$DEACTIVATION_CONTROL_ROOT" \
  || fail "deactivation failure prior controls were not installed"
cp "$ROOT/bin/worker-redis-marker-control.sh" \
  "$TEST_ROOT/deactivation-failure-launcher"
cp "$DEACTIVATION_CONTROL_ROOT/control.conf" \
  "$TEST_ROOT/deactivation-failure-control.conf"
cp "$FAKE_CRONTAB" "$TEST_ROOT/deactivation-failure-crontab"
FAKE_FAIL_CRONTAB_INSTALL=true
export FAKE_FAIL_CRONTAB_INSTALL
if vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:deactivation-failure >/dev/null 2>&1; then
  fail "managed-cron deactivation failure unexpectedly prepared controls"
fi
unset FAKE_FAIL_CRONTAB_INSTALL
managed_backup_matches \
  "$DEACTIVATION_CONTROL_ROOT" \
  "$TEST_ROOT/deactivation-failure-launcher" \
  "$TEST_ROOT/deactivation-failure-control.conf" \
  "$TEST_ROOT/deactivation-failure-crontab" \
  || fail "deactivation failure discarded its exact managed-state backup"

reset_marker_transaction_fixture failed-rollback
ROLLBACK_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$ROLLBACK_CONTROL_ROOT"
PRIOR_GENERATION=m-prior-ready-1780000000-0001
PRIOR_IMAGE=vp-ffmpeg-worker-python:prior-ready
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "rollback fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$PRIOR_IMAGE" "$PRIOR_GENERATION" "$ROLLBACK_CONTROL_ROOT" \
  || fail "rollback fixture prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$PRIOR_IMAGE" "$PRIOR_GENERATION" "$ROLLBACK_CONTROL_ROOT" \
  || fail "rollback fixture prior controls were not installed"
vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:candidate-ready >/dev/null \
  || fail "rollback fixture candidate did not prepare"
ROLLBACK_CANDIDATE_GENERATION="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
cp "$ROLLBACK_CONTROL_ROOT/control.conf" "$TEST_ROOT/candidate-control.conf"
cp "$FAKE_CRONTAB" "$TEST_ROOT/candidate-crontab"
: >"$CONTROL_EVENTS"
FAKE_READINESS_RESULT=failed
export FAKE_READINESS_RESULT
if vp_restore_worker_redis_marker_controls >/dev/null 2>&1; then
  fail "unready rollback generation unexpectedly restored"
fi
FAILED_ROLLBACK_GENERATION="$(
  awk -F'|' '$1 == "role" && $2 == "provision" { print $3; exit }' \
    "$CONTROL_EVENTS"
)"
[[ -n "$FAILED_ROLLBACK_GENERATION" \
  && "$FAILED_ROLLBACK_GENERATION" != "$ROLLBACK_CANDIDATE_GENERATION" ]] \
  || fail "failed rollback generation was not independently provisioned"
cmp -s "$TEST_ROOT/candidate-control.conf" \
  "$ROLLBACK_CONTROL_ROOT/control.conf" \
  || fail "failed rollback did not restore candidate control config"
cmp -s "$TEST_ROOT/candidate-crontab" "$FAKE_CRONTAB" \
  || fail "failed rollback did not restore candidate managed cron"
[[ -d "$ROLLBACK_CONTROL_ROOT/roles/$ROLLBACK_CANDIDATE_GENERATION" ]] \
  || fail "failed rollback revoked the still-active candidate roles"
[[ ! -e "$ROLLBACK_CONTROL_ROOT/roles/$FAILED_ROLLBACK_GENERATION" ]] \
  || fail "failed rollback retained rollback credential files"
for purpose in readiness janitor repair; do
  [[ -e "$SECRET_DIR/vp-wrm-$purpose-db-$ROLLBACK_CANDIDATE_GENERATION" ]] \
    || fail "failed rollback deleted candidate $purpose secret"
  [[ ! -e "$SECRET_DIR/vp-wrm-$purpose-db-$FAILED_ROLLBACK_GENERATION" ]] \
    || fail "failed rollback retained rollback $purpose secret"
done
ROLLBACK_CRON_RESTORE_LINE="$(event_line 'cron|install')"
ROLLBACK_JOB_REMOVE_LINE="$(
  event_line \
    "job|remove|vp-worker-redis-marker-readiness-job|$FAILED_ROLLBACK_GENERATION"
)"
ROLLBACK_ROLE_REVOKE_LINE="$(
  event_line "role|revoke|$FAILED_ROLLBACK_GENERATION"
)"
ROLLBACK_SECRET_REMOVE_LINE="$(
  event_line "secret|remove|vp-wrm-readiness-db-$FAILED_ROLLBACK_GENERATION"
)"
if [[ -z "$ROLLBACK_CRON_RESTORE_LINE" || -z "$ROLLBACK_JOB_REMOVE_LINE" \
  || -z "$ROLLBACK_ROLE_REVOKE_LINE" || -z "$ROLLBACK_SECRET_REMOVE_LINE" \
  || "$ROLLBACK_CRON_RESTORE_LINE" -ge "$ROLLBACK_JOB_REMOVE_LINE" \
  || "$ROLLBACK_JOB_REMOVE_LINE" -ge "$ROLLBACK_ROLE_REVOKE_LINE" \
  || "$ROLLBACK_ROLE_REVOKE_LINE" -ge "$ROLLBACK_SECRET_REMOVE_LINE" ]]; then
  fail "failed rollback cleanup order was not cron/config -> job -> role -> secret"
fi
managed_backup_matches \
  "$ROLLBACK_CONTROL_ROOT" \
  "$ROOT/bin/worker-redis-marker-control.sh" \
  "$TEST_ROOT/candidate-control.conf" \
  "$TEST_ROOT/candidate-crontab" \
  || fail "failed rollback readiness discarded the exact candidate backup"

prepare_rollback_backup_fixture() {
  local name="$1"
  reset_marker_transaction_fixture "$name"
  ROLLBACK_FAILURE_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
  mkdir -p "$ROLLBACK_FAILURE_CONTROL_ROOT"
  local prior_generation="m-$name-prior-1780000000-0001"
  local prior_image="vp-ffmpeg-worker-python:$name-prior"
  vp_require_worker_redis_runtime_state >/dev/null \
    || fail "$name fixture runtime state was not ready"
  vp_worker_redis_marker_provision_generation \
    "$prior_image" "$prior_generation" "$ROLLBACK_FAILURE_CONTROL_ROOT" \
    || fail "$name prior generation was not provisioned"
  vp_install_worker_redis_marker_control \
    "$prior_image" "$prior_generation" "$ROLLBACK_FAILURE_CONTROL_ROOT" \
    || fail "$name prior controls were not installed"
  vp_prepare_worker_redis_marker_controls \
    "vp-ffmpeg-worker-python:$name-candidate" >/dev/null \
    || fail "$name candidate did not prepare"
  cp "$ROOT/bin/worker-redis-marker-control.sh" \
    "$TEST_ROOT/$name-candidate-launcher"
  cp "$ROLLBACK_FAILURE_CONTROL_ROOT/control.conf" \
    "$TEST_ROOT/$name-candidate-control.conf"
  cp "$FAKE_CRONTAB" "$TEST_ROOT/$name-candidate-crontab"
  printf '0\n' >"$FAKE_CRONTAB_WRITE_COUNT"
}

prepare_rollback_backup_fixture rollback-install-verify
FAKE_CORRUPT_CRONTAB_ON_WRITE=2
export FAKE_CORRUPT_CRONTAB_ON_WRITE
if vp_restore_worker_redis_marker_controls >/dev/null 2>&1; then
  fail "rollback install verification mismatch unexpectedly restored"
fi
unset FAKE_CORRUPT_CRONTAB_ON_WRITE
managed_backup_matches \
  "$ROLLBACK_FAILURE_CONTROL_ROOT" \
  "$TEST_ROOT/rollback-install-verify-candidate-launcher" \
  "$TEST_ROOT/rollback-install-verify-candidate-control.conf" \
  "$TEST_ROOT/rollback-install-verify-candidate-crontab" \
  || fail "rollback install verification failure discarded candidate backup"

prepare_rollback_backup_fixture rollback-restore-verify
FAKE_READINESS_TASK_STATE=Failed
FAKE_CORRUPT_CRONTAB_ON_WRITE=4
export FAKE_READINESS_TASK_STATE FAKE_CORRUPT_CRONTAB_ON_WRITE
if vp_restore_worker_redis_marker_controls >/dev/null 2>&1; then
  fail "rollback restore verification mismatch unexpectedly restored"
fi
unset FAKE_READINESS_TASK_STATE FAKE_CORRUPT_CRONTAB_ON_WRITE
managed_backup_matches \
  "$ROLLBACK_FAILURE_CONTROL_ROOT" \
  "$TEST_ROOT/rollback-restore-verify-candidate-launcher" \
  "$TEST_ROOT/rollback-restore-verify-candidate-control.conf" \
  "$TEST_ROOT/rollback-restore-verify-candidate-crontab" \
  || fail "rollback restore verification failure discarded candidate backup"

reset_marker_transaction_fixture commit
COMMIT_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$COMMIT_CONTROL_ROOT"
COMMIT_PRIOR_GENERATION=m-commit-prior-1780000000-0001
COMMIT_PRIOR_IMAGE=vp-ffmpeg-worker-python:commit-prior
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "commit fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$COMMIT_PRIOR_IMAGE" "$COMMIT_PRIOR_GENERATION" "$COMMIT_CONTROL_ROOT" \
  || fail "commit fixture prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$COMMIT_PRIOR_IMAGE" "$COMMIT_PRIOR_GENERATION" "$COMMIT_CONTROL_ROOT" \
  || fail "commit fixture prior controls were not installed"
vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:commit-candidate >/dev/null \
  || fail "commit fixture candidate did not prepare"
COMMIT_CANDIDATE_GENERATION="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
vp_commit_worker_redis_marker_controls \
  || fail "prepared marker candidate did not commit"
[[ "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" == false ]] \
  || fail "commit retained prepared transaction state"
[[ ! -e "$COMMIT_CONTROL_ROOT/roles/$COMMIT_PRIOR_GENERATION" ]] \
  || fail "commit retained prior database credential files"
[[ -d "$COMMIT_CONTROL_ROOT/roles/$COMMIT_CANDIDATE_GENERATION" ]] \
  || fail "commit removed candidate database credential files"
if compgen -G "$COMMIT_CONTROL_ROOT/.managed-state.*" >/dev/null; then
  fail "commit retained a stale managed-state backup"
fi

reset_marker_transaction_fixture rotated-runtime-secrets
ROTATION_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$ROTATION_CONTROL_ROOT"
ROTATION_PRIOR_GENERATION=m-rotation-prior-1780000000-0001
ROTATION_PRIOR_IMAGE=vp-ffmpeg-worker-python:rotation-prior
ROTATION_OLD_READINESS_SECRET=vp-marker-readiness-redis-runtime-old123456789
ROTATION_OLD_JANITOR_SECRET=vp-marker-janitor-redis-runtime-old123456789
ROTATION_NEW_READINESS_SECRET=vp-marker-readiness-redis-runtime-new123456789
ROTATION_NEW_JANITOR_SECRET=vp-marker-janitor-redis-runtime-new123456789
ROTATION_NEW_REPAIR_SECRET=vp-marker-repair-redis-runtime-new123456789
write_runtime_state \
  "$RUNTIME_GENERATION" \
  vp-marker-acl-v1 \
  yes \
  ok \
  noeviction \
  "$ROTATION_NEW_READINESS_SECRET" \
  "$ROTATION_NEW_JANITOR_SECRET" \
  "$ROTATION_NEW_REPAIR_SECRET"
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "rotation fixture current runtime state was not ready"
VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET="$ROTATION_OLD_READINESS_SECRET"
VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET="$ROTATION_OLD_JANITOR_SECRET"
vp_worker_redis_marker_provision_generation \
  "$ROTATION_PRIOR_IMAGE" \
  "$ROTATION_PRIOR_GENERATION" \
  "$ROTATION_CONTROL_ROOT" \
  || fail "rotation fixture prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$ROTATION_PRIOR_IMAGE" \
  "$ROTATION_PRIOR_GENERATION" \
  "$ROTATION_CONTROL_ROOT" \
  || fail "rotation fixture prior controls were not installed"
rm -f \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.state" \
  "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity" \
  "$SERVICE_DIR/vp-worker-redis-marker-janitor-job.state" \
  "$SERVICE_DIR/vp-worker-redis-marker-janitor-job.identity"
vp_run_worker_redis_marker_readiness "$ROTATION_CONTROL_ROOT" \
  || fail "rotation fixture prior readiness job did not complete"
VP_WORKER_REDIS_MARKER_CONFIG_FILE="$ROTATION_CONTROL_ROOT/control.conf" \
VP_WORKER_REDIS_MARKER_STATE_DIR="$ROTATION_CONTROL_ROOT/status" \
VP_WORKER_REDIS_MARKER_LOCK_DIR="$ROTATION_CONTROL_ROOT/locks" \
  "$ROOT/bin/worker-redis-marker-control.sh" janitor >/dev/null \
  || fail "rotation fixture prior janitor job did not complete"
: >"$CONTROL_EVENTS"
if ! vp_prepare_worker_redis_marker_controls \
  vp-ffmpeg-worker-python:rotation-candidate >/dev/null 2>&1; then
  fail "staged runtime secret rotation could not safely retire prior jobs"
fi
[[ "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    == "$ROTATION_OLD_READINESS_SECRET" \
  && "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" \
    == "$ROTATION_OLD_JANITOR_SECRET" ]] \
  || fail "prior generation did not retain its exact Redis secret identities"
ROTATION_CANDIDATE_IDENTITY="$(
  cat "$SERVICE_DIR/vp-worker-redis-marker-readiness-job.identity"
)"
[[ "$ROTATION_CANDIDATE_IDENTITY" \
    == *"$ROTATION_NEW_READINESS_SECRET:worker-marker-redis-url:256"* \
  && "$ROTATION_CANDIDATE_IDENTITY" != *"$ROTATION_OLD_READINESS_SECRET"* ]] \
  || fail "rotation candidate did not use the current readiness Redis secret"
vp_commit_worker_redis_marker_controls \
  || fail "staged runtime secret rotation did not commit"
ROTATION_PRIOR_JOB_REMOVE_LINE="$(
  event_line \
    "job|remove|vp-worker-redis-marker-janitor-job|$ROTATION_PRIOR_GENERATION"
)"
ROTATION_PRIOR_ROLE_REVOKE_LINE="$(
  event_line "role|revoke|$ROTATION_PRIOR_GENERATION"
)"
ROTATION_PRIOR_SECRET_REMOVE_LINE="$(
  event_line \
    "secret|remove|vp-wrm-readiness-db-$ROTATION_PRIOR_GENERATION"
)"
if [[ -z "$ROTATION_PRIOR_JOB_REMOVE_LINE" \
  || -z "$ROTATION_PRIOR_ROLE_REVOKE_LINE" \
  || -z "$ROTATION_PRIOR_SECRET_REMOVE_LINE" \
  || "$ROTATION_PRIOR_JOB_REMOVE_LINE" -ge "$ROTATION_PRIOR_ROLE_REVOKE_LINE" \
  || "$ROTATION_PRIOR_ROLE_REVOKE_LINE" \
    -ge "$ROTATION_PRIOR_SECRET_REMOVE_LINE" ]]; then
  fail "rotated prior generation did not retire job -> role -> secret"
fi

reset_marker_transaction_fixture stale-after-ffmpeg
FRESHNESS_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$FRESHNESS_CONTROL_ROOT"
FRESHNESS_PRIOR_GENERATION=m-freshness-prior-1780000000-0001
FRESHNESS_PRIOR_IMAGE=vp-ffmpeg-worker-python:freshness-prior
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "freshness fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "freshness fixture prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "freshness fixture prior controls were not installed"

WORKER_MUTATIONS="$TEST_ROOT/worker-mutations"
: >"$WORKER_MUTATIONS"
stale_active_readiness_status() {
  local status_file="$FRESHNESS_CONTROL_ROOT/status/readiness.status"
  local stale_at="$(( $(date +%s) - 91 ))"
  awk -F= -v stale_at="$stale_at" '
    $1 == "RECORDED_AT" { print "RECORDED_AT=" stale_at; next }
    { print }
  ' "$status_file" >"$status_file.next"
  chmod 0600 "$status_file.next"
  mv -f "$status_file.next" "$status_file"
}
vp_update_app_runtime_service() {
  :
}
http_health() {
  :
}
vp_deploy_python_worker() {
  printf 'ffmpeg\n' >>"$WORKER_MUTATIONS"
  stale_active_readiness_status
}
vp_deploy_vision_worker() {
  printf 'vision\n' >>"$WORKER_MUTATIONS"
  return 1
}
vp_deploy_publisher() {
  printf 'publisher\n' >>"$WORKER_MUTATIONS"
  return 1
}
vp_retire_legacy_vision_worker() {
  :
}
vp_reconcile_vision_consumers() {
  :
}
vp_require_channelops_migration_head() {
  :
}
vp_install_soak_watch() {
  :
}
swarm_service_running() {
  :
}
VP_VISION_CUTOVER_REQUIRED=false
FAKE_READINESS_RESULT=ready
export FAKE_READINESS_RESULT
if vp_apply_app_services \
  vp-api:freshness \
  vp-frontend:freshness \
  vp-backend:freshness \
  vp-channelops:freshness \
  vp-ffmpeg-go:freshness \
  vp-ffmpeg-worker-python:freshness >/dev/null 2>&1; then
  fail "stale status after ffmpeg unexpectedly completed app apply"
fi
grep -Fxq ffmpeg "$WORKER_MUTATIONS" \
  || fail "freshness fixture did not attempt ffmpeg mutation"
if grep -Eq '^(vision|publisher)$' "$WORKER_MUTATIONS"; then
  fail "stale status after ffmpeg allowed a later worker mutation"
fi

reset_marker_transaction_fixture stale-after-vision
FRESHNESS_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$FRESHNESS_CONTROL_ROOT"
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "publisher freshness fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "publisher freshness prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "publisher freshness prior controls were not installed"
: >"$WORKER_MUTATIONS"
vp_deploy_python_worker() {
  printf 'ffmpeg\n' >>"$WORKER_MUTATIONS"
}
vp_deploy_vision_worker() {
  printf 'vision\n' >>"$WORKER_MUTATIONS"
  stale_active_readiness_status
}
if vp_apply_app_services \
  vp-api:publisher-freshness \
  vp-frontend:publisher-freshness \
  vp-backend:publisher-freshness \
  vp-channelops:publisher-freshness \
  vp-ffmpeg-go:publisher-freshness \
  vp-ffmpeg-worker-python:publisher-freshness >/dev/null 2>&1; then
  fail "stale status after vision unexpectedly completed app apply"
fi
for expected_mutation in ffmpeg vision; do
  grep -Fxq "$expected_mutation" "$WORKER_MUTATIONS" \
    || fail "publisher freshness fixture missed $expected_mutation"
done
if grep -Fxq publisher "$WORKER_MUTATIONS"; then
  fail "stale status after vision allowed publisher mutation"
fi

eval "$(
  declare -f vp_prepare_worker_redis_marker_controls \
    | sed '1s/vp_prepare_worker_redis_marker_controls/vp_prepare_worker_redis_marker_controls_real/'
)"
vp_prepare_worker_redis_marker_controls() {
  vp_prepare_worker_redis_marker_controls_real "$@" || return 1
  stale_active_readiness_status
}
reset_marker_transaction_fixture stale-before-ffmpeg
FRESHNESS_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$FRESHNESS_CONTROL_ROOT"
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "ffmpeg freshness fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "ffmpeg freshness prior generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$FRESHNESS_PRIOR_IMAGE" \
  "$FRESHNESS_PRIOR_GENERATION" \
  "$FRESHNESS_CONTROL_ROOT" \
  || fail "ffmpeg freshness prior controls were not installed"
: >"$WORKER_MUTATIONS"
vp_deploy_python_worker() {
  printf 'ffmpeg\n' >>"$WORKER_MUTATIONS"
  return 1
}
if vp_apply_app_services \
  vp-api:ffmpeg-freshness \
  vp-frontend:ffmpeg-freshness \
  vp-backend:ffmpeg-freshness \
  vp-channelops:ffmpeg-freshness \
  vp-ffmpeg-go:ffmpeg-freshness \
  vp-ffmpeg-worker-python:ffmpeg-freshness >/dev/null 2>&1; then
  fail "stale status before ffmpeg unexpectedly completed app apply"
fi
if [[ -s "$WORKER_MUTATIONS" ]]; then
  fail "stale status before ffmpeg allowed the first worker mutation"
fi

reset_marker_transaction_fixture stale-during-snapshot-restore
FRESHNESS_CONTROL_ROOT="$ROOT/state/worker-redis-marker-control"
mkdir -p "$FRESHNESS_CONTROL_ROOT"
SNAPSHOT_GENERATION=m-snapshot-ready-1780000000-0001
SNAPSHOT_IMAGE=vp-ffmpeg-worker-python:snapshot-ready
vp_require_worker_redis_runtime_state >/dev/null \
  || fail "snapshot freshness fixture runtime state was not ready"
vp_worker_redis_marker_provision_generation \
  "$SNAPSHOT_IMAGE" "$SNAPSHOT_GENERATION" "$FRESHNESS_CONTROL_ROOT" \
  || fail "snapshot freshness generation was not provisioned"
vp_install_worker_redis_marker_control \
  "$SNAPSHOT_IMAGE" "$SNAPSHOT_GENERATION" "$FRESHNESS_CONTROL_ROOT" \
  || fail "snapshot freshness controls were not installed"
vp_run_worker_redis_marker_readiness "$FRESHNESS_CONTROL_ROOT" \
  || fail "snapshot freshness readiness was not established"
: >"$WORKER_MUTATIONS"
vp_restore_gpu_service() {
  printf 'ffmpeg\n' >>"$WORKER_MUTATIONS"
  stale_active_readiness_status
}
vp_deploy_vision_worker() {
  printf 'vision\n' >>"$WORKER_MUTATIONS"
}
vp_deploy_publisher() {
  printf 'publisher\n' >>"$WORKER_MUTATIONS"
}
SNAPSHOTS="$VP_PYTHON_WORKER_SERVICE|vp-python:prior
$VP_VISION_WORKER_SERVICE|vp-vision:prior
$VP_PUBLISHER_SERVICE|vp-publisher:prior"
if vp_restore_app_snapshots "$SNAPSHOTS" \
  "$VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE" \
  >/dev/null 2>&1; then
  fail "stale snapshot status unexpectedly completed all worker restores"
fi
grep -Fxq ffmpeg "$WORKER_MUTATIONS" \
  || fail "snapshot freshness fixture did not restore ffmpeg"
if grep -Eq '^(vision|publisher)$' "$WORKER_MUTATIONS"; then
  fail "stale snapshot status allowed a later worker restore mutation"
fi

printf 'Running\n' >"$SERVICE_DIR/$VP_PYTHON_WORKER_SERVICE.state"
printf 'unrelated\n' >"$SERVICE_DIR/$VP_PYTHON_WORKER_SERVICE.identity"
: >"$DOCKER_CALLS"
if vp_restore_app_snapshots "" "$VP_PYTHON_WORKER_SERVICE" \
  >/dev/null 2>&1; then
  fail "stale status unexpectedly allowed new worker rollback removal"
fi
if grep -Fq "docker|service|rm|$VP_PYTHON_WORKER_SERVICE" "$DOCKER_CALLS"; then
  fail "stale status reached new worker rollback removal"
fi

if grep -RE \
  'postgres(ql)?://|redis://[^[:space:]]*:[^[:space:]@]+@|marker_key|payload_sha256' \
  "$STATE_DIR" "$TEST_ROOT"/*.out 2>/dev/null; then
  fail "launcher persisted or logged a credential, marker value, or payload"
fi

echo "worker Redis marker control tests passed"
