#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$ROOT_DIR/deploy/swarm/worker-redis-marker-control.sh"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
FAKE_BIN="$TEST_ROOT/bin"
DOCKER_CALLS="$TEST_ROOT/docker-calls"
SERVICE_STATE="$TEST_ROOT/service-state"
FAKE_CRONTAB="$TEST_ROOT/crontab"
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

mkdir -p "$FAKE_BIN" "$STATE_DIR" "$LOCK_DIR"
: >"$DOCKER_CALLS"
printf 'absent\n' >"$SERVICE_STATE"

cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'docker' >>"$DOCKER_CALLS"
printf '|%s' "$@" >>"$DOCKER_CALLS"
printf '\n' >>"$DOCKER_CALLS"

if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
  [[ "$(<"$SERVICE_STATE")" != absent ]] || exit 1
  if [[ "$*" == *"--format"* ]]; then
    printf '%s\n' "${FAKE_SERVICE_GENERATION:-}"
  fi
  exit
fi
if [[ "${1:-} ${2:-}" == "service ps" ]]; then
  if [[ "${FAKE_EMPTY_SERVICE_PS:-false}" != true ]]; then
    printf '%s\n' "$(<"$SERVICE_STATE")"
  fi
  exit
fi
if [[ "${1:-} ${2:-}" == "service rm" ]]; then
  printf 'absent\n' >"$SERVICE_STATE"
  exit
fi
if [[ "${1:-} ${2:-}" == "service create" ]]; then
  printf 'Complete\n' >"$SERVICE_STATE"
  exit
fi
if [[ "${1:-} ${2:-}" == "service logs" ]]; then
  case "$*" in
    *vp-worker-redis-marker-readiness-job*)
      printf '%s\n' \
        '{"checked_count":7,"code":"ready","expected_count":7,"status":"ok"}'
      ;;
    *vp-worker-redis-marker-janitor-job*)
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
  if [[ "${3:-}" == vp-wrm-* \
    || "${3:-}" == "${FAKE_MISSING_SECRET:-}" ]]; then
    exit 1
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "secret create" ]]; then
  payload="$(cat)"
  [[ -n "$payload" ]]
  printf 'stdin-bytes|%s|%s\n' "${3:-}" "${#payload}" >>"$DOCKER_CALLS"
  exit
fi
if [[ "${1:-} ${2:-}" == "secret rm" ]]; then
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
[[ "$#" -eq 1 ]]
cp "$1" "$FAKE_CRONTAB"
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

export DOCKER_CALLS SERVICE_STATE FAKE_CRONTAB
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

: >"$DOCKER_CALLS"
printf 'absent\n' >"$SERVICE_STATE"
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
printf 'Running\n' >"$SERVICE_STATE"
running_output="$("$LAUNCHER" readiness)"
[[ "$running_output" == "mode=readiness code=job_running" ]] \
  || fail "running readiness job was not a clean skip"
if grep -Eq 'service\|(create|rm)' "$DOCKER_CALLS"; then
  fail "running readiness job was created or removed"
fi

: >"$DOCKER_CALLS"
printf 'Complete\n' >"$SERVICE_STATE"
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
  'vp_prepare_worker_redis_marker_controls "$python_worker"' \
  "$EXTENSION" \
  || fail "registered Python workers are not gated by marker readiness"
prepare_line="$(grep -nF \
  'vp_prepare_worker_redis_marker_controls "$python_worker"' \
  "$EXTENSION" | head -1 | cut -d: -f1)"
worker_line="$(grep -nF \
  'vp_deploy_python_worker "$python_worker"' \
  "$EXTENSION" | head -1 | cut -d: -f1)"
[[ "$prepare_line" -lt "$worker_line" ]] \
  || fail "marker readiness does not precede registered worker updates"

for contract in \
  'python -m app.services.worker_marker_control_role_cli' \
  'WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url' \
  'docker secret create "$secret_name" -' \
  'VP_WORKER_REDIS_RUNTIME_GENERATION' \
  'VP_WORKER_REDIS_RUNTIME_ACL_IDENTITY="vp-marker-acl-v1"' \
  '"$aof_enabled" != yes' \
  '"$aof_status" != ok' \
  '"$maxmemory_policy" != noeviction' \
  'vp_restore_worker_redis_marker_controls' \
  'vp_commit_worker_redis_marker_controls'; do
  grep -Fq "$contract" "$EXTENSION" \
    || fail "deploy integration is missing contract: $contract"
done

grep -Fq '* * * * * ' "$EXTENSION" \
  || fail "readiness is not scheduled every minute"
grep -Fq '*/5 * * * * ' "$EXTENSION" \
  || fail "janitor is not scheduled every five minutes"
if grep -E '^[^#]*(cron|CRON).*repair|^[^#]*\* \* \* \* \*.*repair' \
  "$EXTENSION"; then
  fail "repair was scheduled"
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

printf 'Complete\n' >"$SERVICE_STATE"
FAKE_SERVICE_GENERATION=m-0123456789ab-1780000000-9999
FAKE_EMPTY_SERVICE_PS=true
export FAKE_SERVICE_GENERATION FAKE_EMPTY_SERVICE_PS
if vp_worker_redis_marker_generation_jobs_stopped \
  "$FAKE_SERVICE_GENERATION" 2>/dev/null; then
  fail "generation retirement accepted an empty Swarm task set"
fi
unset FAKE_SERVICE_GENERATION FAKE_EMPTY_SERVICE_PS
printf 'Complete\n' >"$SERVICE_STATE"
if vp_worker_redis_marker_generation_jobs_stopped \
  m-0123456789ab-1780000000-9999 2>/dev/null; then
  fail "generation retirement accepted an unlabeled fixed-name job"
fi

RUNTIME_GENERATION="abcdef0123456789abcdef0123456789abcdef01"
RUNTIME_STATE="$TEST_ROOT/runtime-state"
write_runtime_state() {
  local generation="$1"
  local identity="$2"
  local aof_enabled="$3"
  local aof_status="$4"
  local policy="$5"
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
READINESS_REDIS_SECRET=vp-marker-readiness-redis-runtime-abcdef012345
JANITOR_REDIS_SECRET=vp-marker-janitor-redis-runtime-abcdef012345
REPAIR_REDIS_SECRET=vp-marker-repair-redis-runtime-abcdef012345
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

if grep -RE \
  'postgres(ql)?://|redis://[^[:space:]]*:[^[:space:]@]+@|marker_key|payload_sha256' \
  "$STATE_DIR" "$TEST_ROOT"/*.out 2>/dev/null; then
  fail "launcher persisted or logged a credential, marker value, or payload"
fi

echo "worker Redis marker control tests passed"
