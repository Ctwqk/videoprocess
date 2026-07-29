#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
CALLS="$TEST_ROOT/calls"
ROOT="$TEST_ROOT/deploy-github-sync"
FAKE_BIN="$TEST_ROOT/bin"
FAKE_CRONTAB="$TEST_ROOT/crontab"
FAKE_CRONTAB_CALLS="$TEST_ROOT/crontab-calls"
FAKE_CRONTAB_FAILURE_USED="$TEST_ROOT/crontab-failure-used"
FAKE_WATCH_TARGET="$ROOT/bin/channelops-soak-watch.sh"
CHANNEL_RUNNER_ENV_STATE_FILE="$TEST_ROOT/channelops-runner-env-state"
VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
TEST_COMMIT="0123456789abcdef0123456789abcdef01234567"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

mkdir -p "$FAKE_BIN"
printf 'legacy\n' >"$CHANNEL_RUNNER_ENV_STATE_FILE"
cat >"$FAKE_BIN/crontab" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'crontab|%s|lc_all=%s\n' "$*" "${LC_ALL:-}" >>"$CALLS"

if [[ "${1:-}" == "-l" ]]; then
  if [[ "${FAKE_CRONTAB_READ_MODE:-normal}" == "error" ]]; then
    echo 'crontab: permission denied' >&2
    exit 1
  fi
  if [[ -f "$FAKE_CRONTAB" ]]; then
    cat "$FAKE_CRONTAB"
    exit 0
  fi
  echo 'no crontab for video-test' >&2
  exit 1
fi

if [[ "${1:-}" == "-r" ]]; then
  if [[ "${FAKE_CRONTAB_ROLLBACK_FAIL:-false}" == "true" ]]; then
    echo 'crontab: injected rollback removal failure' >&2
    exit 1
  fi
  if [[ ! -f "$FAKE_CRONTAB" ]]; then
    echo 'no crontab for video-test' >&2
    exit 1
  fi
  rm -f "$FAKE_CRONTAB"
  printf 'remove\n' >>"$FAKE_CRONTAB_CALLS"
  exit 0
fi

[[ "$#" -eq 1 ]]
if [[ "${FAKE_CRONTAB_ROLLBACK_FAIL:-false}" == "true" \
  && -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
  echo 'crontab: injected rollback install failure' >&2
  exit 1
fi
case "${FAKE_CRONTAB_INSTALL_MODE:-normal}" in
  fail-before)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      : >"$FAKE_CRONTAB_FAILURE_USED"
      echo 'crontab: injected install failure' >&2
      exit 1
    fi
    ;;
  mutate-then-fail)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      echo 'crontab: injected post-mutation failure' >&2
      exit 1
    fi
    ;;
  verify-mismatch)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      printf '# injected verification mismatch\n' >>"$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      printf 'write-mismatch\n' >>"$FAKE_CRONTAB_CALLS"
      exit 0
    fi
    ;;
  target-verify-mismatch)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      printf '#!/usr/bin/env bash\nprintf "injected target mismatch\\n"\n' >"$FAKE_WATCH_TARGET"
      chmod 0644 "$FAKE_WATCH_TARGET"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      printf 'write-target-mismatch\n' >>"$FAKE_CRONTAB_CALLS"
      exit 0
    fi
    ;;
  signal-term)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      kill -TERM "$PPID"
      exit 0
    fi
    ;;
esac
cp "$1" "$FAKE_CRONTAB"
printf 'write\n' >>"$FAKE_CRONTAB_CALLS"
EOF
chmod +x "$FAKE_BIN/crontab"
export CALLS FAKE_CRONTAB FAKE_CRONTAB_CALLS FAKE_CRONTAB_FAILURE_USED FAKE_WATCH_TARGET
export CHANNEL_RUNNER_ENV_STATE_FILE
FAKE_CRONTAB_READ_MODE=normal
FAKE_CRONTAB_INSTALL_MODE=normal
FAKE_CRONTAB_ROLLBACK_FAIL=false
export FAKE_CRONTAB_READ_MODE FAKE_CRONTAB_INSTALL_MODE FAKE_CRONTAB_ROLLBACK_FAIL
PATH="$FAKE_BIN:$PATH"
export PATH

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=video-ops@example.com
0 2 * * * /usr/local/bin/backup-video-state
*/10 * * * * $VP_SOAK_WATCH_SOURCE >> $ROOT/logs/channelops-soak-watch.log 2>&1
*/15 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/legacy-soak-watch.log 2>&1
# BEGIN VIDEOPROCESS SOAK WATCH
0 * * * * $ROOT/bin/channelops-soak-watch.sh --legacy
# END VIDEOPROCESS SOAK WATCH
@reboot /usr/local/bin/restore-video-network
5 * * * * /usr/bin/sha256sum $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/watcher-audit.log 2>&1
10 * * * * /usr/local/bin/notify-ops channelops-soak-watch.sh
# audit checksum notification for $ROOT/bin/channelops-soak-watch.sh
EOF

REPO_ROOT=/home/taiwei/deploy-github-sync/repos
BUILD_IMAGES=1
UPDATE_SERVICES=1
HEALTH_CHECKS=1
VP_API_DATABASE_URL_GO=postgres://test:test@10.0.0.150:5435/videoprocess
VP_PYTHON_WORKER_DATABASE_URL=postgresql+asyncpg://test:test@10.0.0.150:5435/videoprocess
VP_MINIO_ACCESS_KEY=test-access
VP_MINIO_SECRET_KEY=test-secret
GPU_SERVICE_EXISTS=true
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
LEGACY_VISION_CONTAINER_ID=374cabc27904a788beb221571438ed75ba6c6bc716b7c94849e0b7ca055d762e
LEGACY_VISION_CONTAINER_NAME=/vp_vision_worker_1
LEGACY_VISION_CONTAINER_RUNNING=true
LEGACY_VISION_PROJECT=videoprocess
LEGACY_VISION_SERVICE=vision-worker
CONSTRAINT_MODE=legacy
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_NETWORK_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials
PUBLISHER_REPLICAS=3
PUBLISHER_LIST_FAILURE=false
PUBLISHER_LIST_NAME=
FAIL_PUBLISHER_INSPECT_FORMAT=
FAIL_GPU_CONSTRAINT_INSPECT=false
GPU_PREFLIGHT_SUCCEEDS=true
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
FAIL_UPDATE_EXIT=1
FAIL_GPU_CREATE=false
FAIL_RUNNING_SERVICE=
FAIL_HEALTH_CHECK=
FAIL_NODE_UPDATE=false
FAIL_NETWORK_INSPECT=false
FAIL_PUBLISHER_CREATE=false
FAIL_MANAGED_CRON_PRINTF=false
FAIL_SOAK_CLEANUP=false
MIGRATION_GATE_MODE=success
VISION_CUTOVER_GATE_MODE=success
VISION_CONSUMER_CUTOVER_MODE=success
VISION_CONSUMER_AUDIT_MODE=converged
GPU_TASK_NODE=ccttww-lap
VISION_TASK_NODE=ccttww-lap
PUBLISHER_TASK_NODE=ccttww-lap
PDS_TASK_NODE=colima-127
GPU_TASK_STATE='Running 2 seconds ago'
VISION_TASK_STATE='Running 2 seconds ago'
PUBLISHER_TASK_STATE='Running 2 seconds ago'
PDS_TASK_STATE='Running 2 seconds ago'
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=healthy
PDS_READINESS_CALLS_FILE="$TEST_ROOT/pds-readiness-calls"
PDS_CONSTRAINT_INSPECT_MODE=normal
PDS_CONSTRAINT_INSPECT_CALLS_FILE="$TEST_ROOT/pds-constraint-inspect-calls"
RUNTIME_CONSTRAINT_INSPECT_SERVICE=
RUNTIME_CONSTRAINT_INSPECT_MODE=normal
RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE="$TEST_ROOT/runtime-constraint-inspect-calls"
PDS_EXPECTED_TEST='["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]'
PDS_REMOTE_SCRIPT="$TEST_ROOT/pds-container-snapshot.sh"
PDS_REMOTE_BIN="$TEST_ROOT/pds-remote-bin"
FAIL_PDS_READINESS_SLEEP=false
WORKER_READINESS_CONTAINER_MODE=normal
FAIL_WORKER_READINESS_SERVICE=
WORKER_READINESS_EXEC_MODE=normal
WORKER_READINESS_FAIL_SERVICE=
FAIL_WORKER_READINESS_COUNT=false
FAIL_WORKER_READINESS_SLEEP=false
WORKER_READINESS_FAILURE_USED="$TEST_ROOT/worker-readiness-failure-used"
WORKER_READINESS_CONTAINER_CALLS="$TEST_ROOT/worker-readiness-container-calls"
GPU_READINESS_CONTAINER_ID=1111111111111111111111111111111111111111111111111111111111111111
VISION_READINESS_CONTAINER_ID=2222222222222222222222222222222222222222222222222222222222222222
PUBLISHER_READINESS_CONTAINER_ID=3333333333333333333333333333333333333333333333333333333333333333

mkdir -p "$PDS_REMOTE_BIN"
cat >"$PDS_REMOTE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

mode="${PDS_REMOTE_DOCKER_MODE:-healthy}"
if [[ "${1:-} ${2:-}" == "container ls" ]]; then
  [[ "$*" == *'label=com.docker.swarm.service.name=vp-pds-swarm'* \
    && "$*" == *'--filter status=running'* \
    && "$*" == *'--format {{.ID}}'* ]] || exit 80
  case "$mode" in
    list-error)
      exit 81
      ;;
    missing)
      ;;
    duplicate)
      printf '%064d\n%064d\n' 1 2
      ;;
    bad-id)
      printf 'not-a-container-id\n'
      ;;
    *)
      printf '%064d\n' 1
      ;;
  esac
  exit 0
fi

if [[ "${1:-} ${2:-}" == "container inspect" ]]; then
  [[ "${3:-}" == "--format" \
    && "${4:-}" == *'.Config.Image'* \
    && "${4:-}" == *'.Config.Env'* \
    && "${4:-}" == *'.Config.Healthcheck'* ]] || exit 82
  if [[ "$mode" == "inspect-error" ]]; then
    exit 83
  fi
  if [[ "$mode" == "no-health" ]]; then
    printf 'vp-pds:test|none|none|0s|0s|0s|0\n'
  else
    printf '%s\n' \
      'vp-pds:test|healthy|["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]|10s|3s|10s|6'
  fi
  case "$mode" in
    env-missing)
      ;;
    env-duplicate)
      printf 'PDS_HTTP_ADDR=:8080\nPDS_HTTP_ADDR=:8080\n'
      ;;
    wrong-http)
      printf 'PDS_HTTP_ADDR=:9099\n'
      ;;
    *)
      printf 'PDS_HTTP_ADDR=:8080\n'
      ;;
  esac
  exit 0
fi

exit 84
EOF
chmod +x "$PDS_REMOTE_BIN/docker"
awk -v fake_path="$PDS_REMOTE_BIN:/usr/bin:/bin" '
  /^vp_pds_container_snapshot\(\) \{/ { in_function=1; next }
  in_function && /<<'\''REMOTE'\''/ { capture=1; next }
  capture && /^REMOTE$/ { exit }
  capture {
    if ($0 ~ /^PATH=/) {
      print "PATH=" fake_path
    } else {
      print
    }
  }
' "$EXTENSION" >"$PDS_REMOTE_SCRIPT"
if ! grep -Fq 'container_data=' "$PDS_REMOTE_SCRIPT"; then
  echo 'FAIL: PDS remote snapshot script extraction failed' >&2
  exit 1
fi

printf() {
  if [[ "$FAIL_MANAGED_CRON_PRINTF" == "true" \
    && "${1:-}" == '%s\n%s\n%s\n' ]]; then
    return 1
  fi
  builtin printf "$@"
}

log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}

gh() {
  printf 'gh|%s\n' "$*" >>"$CALLS"
  printf 'found\tcompleted\tsuccess\t%s\t101\n' "$TEST_COMMIT"
}

mv() {
  printf 'mv|%s\n' "$*" >>"$CALLS"
  command mv "$@"
}

rm() {
  if [[ "$FAIL_SOAK_CLEANUP" == "true" \
    && "$*" == *"vp-soak-watch-cron."* || "$FAIL_SOAK_CLEANUP" == "true" \
    && "$*" == *".channelops-soak-watch.txn."* ]]; then
    printf 'rm-failed|%s\n' "$*" >>"$CALLS"
    return 1
  fi
  command rm "$@"
}

sleep() {
  printf 'sleep|%s\n' "$*" >>"$CALLS"
  [[ "$FAIL_WORKER_READINESS_SLEEP" != "true" \
    && "$FAIL_PDS_READINESS_SLEEP" != "true" ]]
}

awk() {
  if [[ "$FAIL_WORKER_READINESS_COUNT" == "true" \
    && "${1:-}" == 'NF { count++ } END { print count+0 }' ]]; then
    return 1
  fi
  command awk "$@"
}

build_image_on_host() {
  printf 'build|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"
}

http_health() {
  printf 'health|%s|%s\n' "$1" "$2" >>"$CALLS"
  [[ "$1" != "$FAIL_HEALTH_CHECK" ]]
}

swarm_service_running() {
  printf 'running|%s\n' "$1" >>"$CALLS"
  [[ "$1" != "$FAIL_RUNNING_SERVICE" ]]
}

remote_sh() {
  printf 'remote|%s' "$1" >>"$CALLS"
  shift
  printf '|%s' "$@" >>"$CALLS"
  printf '\n' >>"$CALLS"
  command cat >/dev/null

  [[ "${1:-}" == "/bin/sh" \
    && "${2:-}" == "-s" \
    && "${3:-}" == "--" \
    && "${4:-}" == "vp-pds-swarm" ]] || return 1

  local readiness_call=0
  if [[ -f "$PDS_READINESS_CALLS_FILE" ]]; then
    readiness_call="$(<"$PDS_READINESS_CALLS_FILE")"
  fi
  readiness_call=$((readiness_call + 1))
  printf '%s\n' "$readiness_call" >"$PDS_READINESS_CALLS_FILE"

  local image="$PDS_CURRENT_IMAGE"
  local http_addr="$PDS_CURRENT_HTTP_ADDR"
  local health=healthy
  local test="$PDS_EXPECTED_TEST"
  local interval=10s
  local timeout=3s
  local start_period=10s
  local retries=6
  case "$PDS_READINESS_MODE" in
    healthy)
      ;;
    starting-then-healthy)
      if [[ "$readiness_call" -lt 3 ]]; then
        health=starting
      fi
      ;;
    always-starting)
      health=starting
      ;;
    missing|duplicate)
      printf 'pending|container_set\n'
      return 0
      ;;
    container-set-then-healthy)
      if [[ "$readiness_call" -lt 3 ]]; then
        printf 'pending|container_set\n'
        return 0
      fi
      ;;
    wrong-image)
      image=vp-pds:unexpected
      ;;
    wrong-http-addr)
      http_addr=:9099
      ;;
    wrong-command)
      test='["CMD-SHELL","curl http://sentinel-secret"]'
      ;;
    wrong-timing)
      interval=30s
      ;;
    unhealthy)
      health=unhealthy
      ;;
    new-unhealthy-rollback-healthy)
      if [[ "$image" != "baseline-vp-pds-swarm:stable" ]]; then
        health=unhealthy
      fi
      ;;
    new-unhealthy-rollback-no-health)
      if [[ "$image" == "baseline-vp-pds-swarm:stable" ]]; then
        health=none
        test=none
        interval=0s
        timeout=0s
        start_period=0s
        retries=0
      else
        health=unhealthy
      fi
      ;;
    remote-error)
      printf 'daemon=tcp://sentinel-secret container=aaaaaaaaaaaaaaaa\n' >&2
      return 11
      ;;
    malformed)
      printf 'not-a-valid-snapshot\n'
      return 0
      ;;
    *)
      return 1
      ;;
  esac
  printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
    "$image" "$http_addr" "$health" "$test" \
    "$interval" "$timeout" "$start_period" "$retries"
}

docker() {
  if [[ "${1:-}" == "exec" ]]; then
    printf 'docker'
    printf '|%s' "$@"
    printf '\n'
  else
    printf 'docker|%s\n' "$*"
  fi >>"$CALLS"
  if [[ "${1:-}" == "run" \
    && "$*" == *"--env DATABASE_URL"* \
    && "$*" == *"SELECT version_num FROM alembic_version"* ]]; then
    builtin printf 'CUDA migration gate banner\n'
    [[ "$MIGRATION_GATE_MODE" == "success" ]]
    return
  fi
  if [[ "${1:-}" == "run" \
    && "$*" == *"runtime_schedules"* \
    && "$*" == *"vp:tasks:vision"* ]]; then
    [[ "$VISION_CUTOVER_GATE_MODE" == "success" ]]
    return
  fi
  if [[ "${1:-}" == "run" \
    && "$*" == *"python -m app.services.vision_consumer_cutover --check-only"* ]]; then
    [[ "$VISION_CONSUMER_AUDIT_MODE" == "converged" ]]
    return
  fi
  if [[ "${1:-}" == "run" \
    && "$*" == *"python -m app.services.vision_consumer_cutover"* ]]; then
    [[ "$VISION_CONSUMER_CUTOVER_MODE" == "success" ]]
    return
  fi
  if [[ "${1:-}" == "run" && "$GPU_PREFLIGHT_SUCCEEDS" != "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "node update" && "$FAIL_NODE_UPDATE" == "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-ffmpeg-worker-gpu-swarm"* ]]; then
    GPU_SERVICE_EXISTS=true
    if [[ "$FAIL_GPU_CREATE" == "true" ]]; then
      return 1
    fi
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-vision-worker-swarm"* ]]; then
    VISION_SERVICE_EXISTS=true
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-youtube-publisher-swarm"* ]]; then
    PUBLISHER_SERVICE_EXISTS=true
    if [[ "$FAIL_PUBLISHER_CREATE" == "true" ]]; then
      return 1
    fi
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-ffmpeg-worker-gpu-swarm" ]]; then
    GPU_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-vision-worker-swarm" ]]; then
    VISION_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-youtube-publisher-swarm" ]]; then
    PUBLISHER_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && -n "$FAIL_UPDATE_SERVICE" \
    && "$*" == *"--image $FAIL_UPDATE_IMAGE"* \
    && "$*" == *"$FAIL_UPDATE_SERVICE"* ]]; then
    return "$FAIL_UPDATE_EXIT"
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && "$*" == *"vp-pds-swarm"* ]]; then
    local previous=""
    local argument
    for argument in "$@"; do
      if [[ "$previous" == "--image" ]]; then
        PDS_CURRENT_IMAGE="$argument"
        break
      fi
      previous="$argument"
    done
    if [[ "$*" == *"--env-add PDS_HTTP_ADDR=:8080"* ]]; then
      PDS_CURRENT_HTTP_ADDR=:8080
    fi
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && "$*" == *"vp-channel-agent-runner-swarm"* \
    && "$*" == *"CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1"* ]]; then
    printf 'converged\n' >"$CHANNEL_RUNNER_ENV_STATE_FILE"
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && "$*" == *"vp-youtube-publisher-swarm"* ]]; then
    if [[ "$*" == *"--replicas 0"* ]]; then
      PUBLISHER_REPLICAS=0
    fi
    if [[ "$*" == *"--mount-rm /data/storage"* ]]; then
      PUBLISHER_MOUNT_MODE=scratch_removed
    fi
    if [[ "$*" == *"--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage"* ]]; then
      PUBLISHER_MOUNT_MODE=desired
    fi
    if [[ "$*" == *"--replicas 1"* ]]; then
      PUBLISHER_REPLICAS=1
    fi
  fi
  if [[ "${1:-} ${2:-}" == "service ps" ]]; then
    if [[ "$#" -ne 7 \
      || "${4:-}" != "--filter" \
      || "${5:-}" != "desired-state=running" \
      || "${6:-}" != "--format" \
      || "${7:-}" != '{{.Node}}|{{.CurrentState}}' ]]; then
      return 1
    fi
    case "${3:-}" in
      vp-ffmpeg-worker-gpu-swarm)
        printf '%s|%s\n' "$GPU_TASK_NODE" "$GPU_TASK_STATE"
        ;;
      vp-vision-worker-swarm)
        printf '%s|%s\n' "$VISION_TASK_NODE" "$VISION_TASK_STATE"
        ;;
      vp-youtube-publisher-swarm)
        printf '%s|%s\n' "$PUBLISHER_TASK_NODE" "$PUBLISHER_TASK_STATE"
        ;;
      vp-pds-swarm)
        printf '%s|%s\n' "$PDS_TASK_NODE" "$PDS_TASK_STATE"
        ;;
      *)
        return 1
        ;;
    esac
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "container ls" \
    && "$*" == *'--filter label=com.docker.swarm.service.name='* ]]; then
    if [[ "$*" != *'--filter status=running'* \
      || "$*" != *'--format {{.ID}}'* ]]; then
      return 1
    fi
    local readiness_service
    local readiness_container_id
    case "$*" in
      *"label=com.docker.swarm.service.name=$VP_PYTHON_WORKER_SERVICE"*)
        readiness_service="$VP_PYTHON_WORKER_SERVICE"
        readiness_container_id="$GPU_READINESS_CONTAINER_ID"
        ;;
      *"label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE"*)
        readiness_service="$VP_VISION_WORKER_SERVICE"
        readiness_container_id="$VISION_READINESS_CONTAINER_ID"
        ;;
      *"label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE"*)
        readiness_service="$VP_PUBLISHER_SERVICE"
        readiness_container_id="$PUBLISHER_READINESS_CONTAINER_ID"
        ;;
      *)
        return 1
        ;;
    esac
    case "$WORKER_READINESS_CONTAINER_MODE" in
      normal)
        printf '%s\n' "$readiness_container_id"
        ;;
      missing)
        ;;
      duplicate)
        printf '%s\n%s\n' "$readiness_container_id" "${readiness_container_id}duplicate"
        ;;
      transition)
        local readiness_container_call=0
        if [[ -f "$WORKER_READINESS_CONTAINER_CALLS" ]]; then
          readiness_container_call="$(<"$WORKER_READINESS_CONTAINER_CALLS")"
        fi
        readiness_container_call=$((readiness_container_call + 1))
        printf '%s\n' "$readiness_container_call" >"$WORKER_READINESS_CONTAINER_CALLS"
        if [[ "$readiness_container_call" -lt 3 ]]; then
          printf '%s\n%s\n' "$readiness_container_id" "${readiness_container_id}duplicate"
        else
          printf '%s\n' "$readiness_container_id"
        fi
        ;;
      error)
        printf 'daemon=tcp://test-secret container=%s\n' "$readiness_container_id" >&2
        return 1
        ;;
      *)
        return 1
        ;;
    esac
    return 0
  fi
  if [[ "${1:-}" == "exec" \
    && "${3:-} ${4:-} ${5:-}" == "python -m app.channel_agent.worker_storage_readiness_cli" ]]; then
    case "${2:-}" in
      "$GPU_READINESS_CONTAINER_ID") readiness_service="$VP_PYTHON_WORKER_SERVICE" ;;
      "$VISION_READINESS_CONTAINER_ID") readiness_service="$VP_VISION_WORKER_SERVICE" ;;
      "$PUBLISHER_READINESS_CONTAINER_ID") readiness_service="$VP_PUBLISHER_SERVICE" ;;
      *) return 1 ;;
    esac
    if [[ "$WORKER_READINESS_EXEC_MODE" == "fail-first" \
      && "$readiness_service" == "$WORKER_READINESS_FAIL_SERVICE" \
      && ! -e "$WORKER_READINESS_FAILURE_USED" ]]; then
      : >"$WORKER_READINESS_FAILURE_USED"
      return 1
    fi
    if [[ "$readiness_service" == "$FAIL_WORKER_READINESS_SERVICE" ]]; then
      printf 'No such container: %s test-secret\n' "${2:-}" >&2
      return 1
    fi
    return
  fi
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    if [[ "$FAIL_NETWORK_INSPECT" == "true" ]]; then
      return 1
    fi
    echo vp-pipeline-network-id
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "container inspect" && "$*" == *"vp_vision_worker_1"* ]]; then
    if [[ "$LEGACY_VISION_CONTAINER_EXISTS" != "true" ]]; then
      return 1
    fi
    printf '%s|%s|%s|%s|%s\n' \
      "$LEGACY_VISION_CONTAINER_ID" \
      "$LEGACY_VISION_CONTAINER_NAME" \
      "$LEGACY_VISION_CONTAINER_RUNNING" \
      "$LEGACY_VISION_PROJECT" \
      "$LEGACY_VISION_SERVICE"
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "container ls" && "$*" == *"vp_vision_worker_1"* ]]; then
    if [[ "$LEGACY_VISION_CONTAINER_EXISTS" == "true" ]]; then
      printf 'vp_vision_worker_1\n'
    fi
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "rm -f" \
    && "${3:-}" == "$LEGACY_VISION_CONTAINER_ID" ]]; then
    LEGACY_VISION_CONTAINER_EXISTS=false
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service ls" && "$*" == *"--filter name=vp-youtube-publisher-swarm"* ]]; then
    if [[ "$PUBLISHER_LIST_FAILURE" == "true" ]]; then
      return 1
    fi
    if [[ "$PUBLISHER_SERVICE_EXISTS" == "true" ]]; then
      printf '%s\n' "${PUBLISHER_LIST_NAME:-vp-youtube-publisher-swarm}"
    fi
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
    local service="${3:-}"
    if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" && "$GPU_SERVICE_EXISTS" != "true" ]]; then
      return 1
    fi
    if [[ "$service" == "vp-vision-worker-swarm" && "$VISION_SERVICE_EXISTS" != "true" ]]; then
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" && "$PUBLISHER_SERVICE_EXISTS" != "true" ]]; then
      echo "no such service: $service" >&2
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" \
      && -n "$FAIL_PUBLISHER_INSPECT_FORMAT" \
      && "$*" == *"$FAIL_PUBLISHER_INSPECT_FORMAT"* ]]; then
      return 1
    fi
    case "$*" in
      *ContainerSpec.Image*)
        if [[ "$service" == "vp-pds-swarm" ]]; then
          echo "$PDS_CURRENT_IMAGE"
        else
          echo "baseline-$service:stable"
        fi
        ;;
      *Spec.Mode.Replicated.Replicas*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo "$PUBLISHER_REPLICAS"
        fi
        ;;
      *Placement.Constraints*)
        if [[ -n "$RUNTIME_CONSTRAINT_INSPECT_SERVICE" \
          && "$service" == "$RUNTIME_CONSTRAINT_INSPECT_SERVICE" ]]; then
          local runtime_constraint_inspect_call=0
          if [[ -f "$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE" ]]; then
            runtime_constraint_inspect_call="$(<"$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE")"
          fi
          runtime_constraint_inspect_call=$((runtime_constraint_inspect_call + 1))
          printf '%s\n' "$runtime_constraint_inspect_call" \
            >"$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE"
          case "$RUNTIME_CONSTRAINT_INSPECT_MODE" in
            fail-first)
              if [[ "$runtime_constraint_inspect_call" -eq 1 ]]; then
                return 1
              fi
              ;;
            *)
              return 1
              ;;
          esac
          echo 'node.labels.role==app'
        elif [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" \
          && "$FAIL_GPU_CONSTRAINT_INSPECT" == "true" ]]; then
          return 1
        elif [[ "$service" == "vp-pds-swarm" ]]; then
          local pds_constraint_inspect_call=0
          if [[ -f "$PDS_CONSTRAINT_INSPECT_CALLS_FILE" ]]; then
            pds_constraint_inspect_call="$(<"$PDS_CONSTRAINT_INSPECT_CALLS_FILE")"
          fi
          pds_constraint_inspect_call=$((pds_constraint_inspect_call + 1))
          printf '%s\n' "$pds_constraint_inspect_call" >"$PDS_CONSTRAINT_INSPECT_CALLS_FILE"
          case "$PDS_CONSTRAINT_INSPECT_MODE" in
            normal)
              ;;
            fail-always)
              return 1
              ;;
            fail-first)
              if [[ "$pds_constraint_inspect_call" -eq 1 ]]; then
                return 1
              fi
              ;;
            fail-second)
              if [[ "$pds_constraint_inspect_call" -eq 2 ]]; then
                return 1
              fi
              ;;
            *)
              return 1
              ;;
          esac
          if [[ "$CONSTRAINT_MODE" == "runtime" ]]; then
            echo 'node.labels.vp.runtime==true'
            echo 'node.hostname==colima-127'
          elif [[ "$CONSTRAINT_MODE" == "pds-stale" ]]; then
            echo 'node.hostname==CASPERs-Mac-mini'
            echo 'node.labels.vp.legacy==true'
          else
            echo 'node.labels.role==app'
          fi
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          case "$PUBLISHER_CONSTRAINT_MODE" in
            publisher)
              echo 'node.labels.vp.publisher==true'
              echo 'node.hostname==ccttww-lap'
              ;;
            stale)
              echo 'node.labels.vp.publisher==true'
              echo 'node.labels.vp.runtime==true'
              echo 'node.labels.vp.gpu==true'
              echo 'node.hostname==colima-swarmbridged'
              echo 'node.labels.vp.legacy==true'
              ;;
            *)
              echo 'node.labels.role==app'
              ;;
          esac
        elif [[ "$CONSTRAINT_MODE" == "runtime" ]]; then
          echo 'node.labels.vp.runtime==true'
          echo 'node.hostname==colima-127'
        elif [[ "$CONSTRAINT_MODE" == "stale-runtime" ]]; then
          echo 'node.labels.vp.runtime==true'
          echo 'node.hostname==CASPERs-Mac-mini'
          echo 'node.labels.vp.legacy==true'
        elif [[ "$CONSTRAINT_MODE" == "gpu-stale" ]]; then
          echo 'node.labels.vp.gpu==true'
          echo 'node.hostname==ccttww-lap'
          echo 'node.labels.vp.runtime==true'
          echo 'node.labels.vp.legacy==true'
          echo 'node.labels.role==app'
          echo 'node.hostname==CASPERs-Mac-mini'
          echo 'node.hostname==colima-swarmbridged'
        elif [[ "$CONSTRAINT_MODE" == "gpu-duplicate" ]]; then
          echo 'node.labels.vp.gpu==true'
          echo 'node.labels.vp.gpu==true'
          echo 'node.hostname==ccttww-lap'
        else
          echo 'node.labels.role==app'
        fi
        ;;
      *ContainerSpec.Env*)
        if [[ "$service" == "vp-api-swarm" ]]; then
          echo 'DATABASE_URL=legacy'
        elif [[ "$service" == "vp-channel-agent-runner-swarm" ]]; then
          echo 'CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=30'
          case "$(cat "$CHANNEL_RUNNER_ENV_STATE_FILE")" in
            absent)
              ;;
            legacy)
              echo 'CHANNELOPS_RUNNER_ID=legacy-channelops-runner'
              ;;
            duplicate)
              echo 'CHANNELOPS_RUNNER_ID=legacy-channelops-runner-a'
              echo 'CHANNELOPS_RUNNER_ID=legacy-channelops-runner-b'
              ;;
            converged)
              echo 'CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1'
              ;;
            *)
              echo 'invalid ChannelOps runner environment state' >&2
              return 1
              ;;
          esac
        elif [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
          echo 'YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials'
        elif [[ "$service" == "vp-vision-worker-swarm" ]]; then
          echo 'WORKER_TYPE=legacy'
          echo 'WORKER_HOST=legacy'
          echo 'HF_TOKEN=legacy-token'
          echo 'PUBLIC_PUBLISH_ENABLED=true'
          echo 'YOUTUBE_MANAGER_URL=http://legacy-youtube-manager'
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
          if [[ "$PUBLISHER_ENV_MODE" == "credentials" ]]; then
            echo 'YOUTUBE_MANAGER_URL=http://10.0.0.150:18999'
            echo 'YOUTUBE_PUBLISH_ENABLED=false'
            echo 'YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials'
            echo 'YOUTUBE_CREDENTIALS_JSON=fixture-json'
            echo 'YOUTUBE_LEGACY_MODE=fixture'
            echo 'GOOGLE_CLIENT_SECRETS_FILE=fixture-file'
            echo 'YOUTUBE_REFRESH_TOKEN=fixture-token'
          fi
        elif [[ "$service" == "vp-pds-swarm" ]]; then
          echo 'PDS_HTTP_ADDR=:9099'
        fi
        ;;
      *ContainerSpec.Secrets*)
        if [[ "$service" == "vp-vision-worker-swarm" ]]; then
          echo vision-legacy-secret
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo publisher-credential-reference
        fi
        ;;
      *ContainerSpec.Configs*)
        if [[ "$service" == "vp-vision-worker-swarm" ]]; then
          echo vision-legacy-config
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo publisher-config-reference
        fi
        ;;
      *TaskTemplate.Networks*)
        if [[ "$service" == "vp-youtube-publisher-swarm" && "$PUBLISHER_NETWORK_MODE" == "pipeline" ]]; then
          echo vp-pipeline-network-id
        else
          echo legacy-network-id
        fi
        ;;
      *ContainerSpec.Mounts*)
        if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo /app/youtube_credentials
        elif [[ "$service" == "vp-vision-worker-swarm" ]]; then
          echo ''
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          case "$PUBLISHER_MOUNT_MODE" in
            desired)
              echo 'volume|vp-youtube-publisher-scratch|/data/storage|false'
              ;;
            wrong)
              echo 'volume|vp-youtube-publisher-scratch|/data/storage|true'
              echo 'bind|credential-source|/app/cache|false'
              echo 'bind|/tmp/publisher|/APP/OAUTH|false'
              ;;
            scratch_removed)
              echo 'bind|credential-source|/app/cache|false'
              echo 'bind|/tmp/publisher|/APP/OAUTH|false'
              ;;
            missing)
              echo 'bind|credential-source|/app/cache|false'
              ;;
          esac
        fi
        ;;
    esac
  fi
}

if [[ ! -f "$EXTENSION" ]]; then
  echo "FAIL: missing deploy extension: $EXTENSION" >&2
  exit 1
fi
if ! grep -Fq 'CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1' "$EXTENSION"; then
  echo 'FAIL: managed ChannelOps runner must use the exact 127 identity' >&2
  exit 1
fi
if ! grep -Fq 'vp_update_app_runtime_service \' "$EXTENSION" \
  || ! grep -Fq 'vp-channel-agent-runner-swarm "$channelops_runner" stop-first' "$EXTENSION"; then
  echo 'FAIL: managed ChannelOps runner must replace stop-first' >&2
  exit 1
fi
if ! grep -Fq 'VP_VISION_WORKER_SERVICE="vp-vision-worker-swarm"' "$EXTENSION"; then
  echo 'FAIL: deployment must define the managed vision worker service' >&2
  exit 1
fi
if ! grep -Fq 'vp_require_channelops_migration_head "$python_worker"' "$EXTENSION"; then
  echo 'FAIL: managed ChannelOps runner must be gated on the exact migration head' >&2
  exit 1
fi
if ! grep -Fq 'rows != [\"033_legacy_worker_event_resolutions\"]' "$EXTENSION" \
  || grep -Fq '032_channelops_leader_epoch' "$EXTENSION"; then
  echo 'FAIL: migration head gate must require exactly revision 033' >&2
  exit 1
fi
for expected_order in \
  'vp_update_app_runtime_service vp-api-swarm "$api" stop-first' \
  'vp_update_app_runtime_service vp-frontend-swarm "$frontend" stop-first' \
  'vp_update_app_runtime_service vp-autoflow-api-swarm "$backend" start-first' \
  'vp_update_app_runtime_service vp-event-outbox-relay-swarm "$backend" start-first' \
  'vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first'; do
  if ! grep -Fq "$expected_order" "$EXTENSION"; then
    echo "FAIL: neighboring runtime rollout order changed: $expected_order" >&2
    exit 1
  fi
done
if ! grep -Fq 'HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \' \
  "$ROOT_DIR/backend/Dockerfile.channelops-runner-go" \
  || ! grep -Fq 'CMD wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1' \
  "$ROOT_DIR/backend/Dockerfile.channelops-runner-go"; then
  echo 'FAIL: ChannelOps runner image must actively healthcheck readyz' >&2
  exit 1
fi
if ! grep -Fq 'test: ["CMD", "wget", "-qO-", "http://127.0.0.1:8080/readyz"]' \
  "$ROOT_DIR/docker-compose.yml" \
  || ! grep -Fq 'interval: 10s' "$ROOT_DIR/docker-compose.yml" \
  || ! grep -Fq 'timeout: 3s' "$ROOT_DIR/docker-compose.yml" \
  || ! grep -Fq 'retries: 6' "$ROOT_DIR/docker-compose.yml" \
  || ! grep -Fq 'start_period: 10s' "$ROOT_DIR/docker-compose.yml"; then
  echo 'FAIL: Compose ChannelOps runner must healthcheck active readiness' >&2
  exit 1
fi
if grep -Eq 'YOUTUBE_CREDENTIALS_DIR=|VP_YOUTUBE|--mount-add.*youtube_credentials|--mount .*youtube_credentials' "$EXTENSION"; then
  echo 'FAIL: general production worker must not receive publication credentials' >&2
  exit 1
fi
source "$EXTENSION"

vp_require_worker_redis_runtime_state() {
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=marker-readiness-runtime
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=marker-janitor-runtime
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET=marker-repair-runtime
}

vp_worker_redis_marker_owner_file() {
  printf '/dev/null\n'
}

vp_worker_redis_marker_read_prior_config() {
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=marker-test-prior
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=vp-ffmpeg-worker-python:marker-test-prior
}

vp_worker_redis_marker_capture_managed_state() {
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$1/.test-managed-$RANDOM"
  mkdir -p "$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
}

vp_worker_redis_marker_deactivate_managed_cron() {
  :
}

vp_worker_redis_marker_restore_managed_state() {
  :
}

vp_worker_redis_marker_remove_generation_jobs() {
  printf 'marker-control|jobs-absent|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_worker_redis_marker_provision_generation() {
  printf 'marker-control|provision|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_install_worker_redis_marker_control() {
  printf 'marker-control|install|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_run_worker_redis_marker_readiness() {
  printf 'marker-control|readiness\n' >>"$CALLS"
}

vp_require_worker_redis_marker_status() {
  printf 'marker-control|status\n' >>"$CALLS"
}

vp_worker_redis_marker_retire_generation() {
  printf 'marker-control|retire|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_worker_redis_marker_discard_managed_state() {
  rm -rf "$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
}

VP_RUNTIME_HOST=10.0.0.126
if vp_validate_deploy_config >/dev/null 2>&1; then
  echo 'FAIL: host 126 was accepted as the VP runtime host' >&2
  exit 1
fi
VP_RUNTIME_HOST=10.0.0.127
VP_RUNTIME_NODE=colima-swarmbridged
if vp_validate_deploy_config >/dev/null 2>&1; then
  echo 'FAIL: the 126 Swarm node was accepted as the VP runtime node' >&2
  exit 1
fi
VP_RUNTIME_NODE=colima-127
VP_MANAGER_NODE=CASPERs-Mac-mini
if vp_validate_deploy_config >/dev/null 2>&1; then
  echo 'FAIL: host 126 was accepted as the VP manager node' >&2
  exit 1
fi
VP_MANAGER_NODE=ccttww-lap

: >"$CALLS"
VP_RUNTIME_HOST=10.0.0.126
if build_pds_images "$TEST_COMMIT" >/dev/null 2>&1; then
  echo 'FAIL: independent PDS build accepted host 126 topology' >&2
  exit 1
fi
if grep -Fq 'build|10.0.0.126|' "$CALLS"; then
  echo 'FAIL: independent PDS build used host 126' >&2
  exit 1
fi

: >"$CALLS"
if deploy_pds_services vp-pds:forbidden-topology-test >/dev/null 2>&1; then
  echo 'FAIL: independent PDS deployment accepted host 126 topology' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: independent PDS deployment mutated services with host 126 configured' >&2
  exit 1
fi
VP_RUNTIME_HOST=10.0.0.127

images="$(build_vp_app_images "$TEST_COMMIT")"
if ! deploy_output="$(deploy_vp_app_services $images)"; then
  echo 'FAIL: deploy_vp_app_services returned non-zero' >&2
  exit 1
fi
if [[ "$deploy_output" != "$VP_APP_SERVICES" ]]; then
  echo 'FAIL: deploy_vp_app_services stdout must contain only the service inventory' >&2
  exit 1
fi

gpu_readiness_probe="docker|exec|$GPU_READINESS_CONTAINER_ID|python|-m|app.channel_agent.worker_storage_readiness_cli"
vision_readiness_probe="docker|exec|$VISION_READINESS_CONTAINER_ID|python|-m|app.channel_agent.worker_storage_readiness_cli|--require-artifact-api"
publisher_readiness_probe="docker|exec|$PUBLISHER_READINESS_CONTAINER_ID|python|-m|app.channel_agent.worker_storage_readiness_cli"
for expected_readiness_call in \
  "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PYTHON_WORKER_SERVICE --filter status=running --format {{.ID}}" \
  "docker|container ls --filter label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE --filter status=running --format {{.ID}}" \
  "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE --filter status=running --format {{.ID}}" \
  "$gpu_readiness_probe" \
  "$vision_readiness_probe" \
  "$publisher_readiness_probe"; do
  if ! grep -Fqx "$expected_readiness_call" "$CALLS"; then
    echo "FAIL: missing managed worker storage readiness call: $expected_readiness_call" >&2
    exit 1
  fi
done
readiness_exec_calls="$(grep -F 'docker|exec|' "$CALLS" || true)"
if [[ "$(printf '%s\n' "$readiness_exec_calls" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 3 ]]; then
  echo 'FAIL: managed worker storage readiness must execute exactly once per worker' >&2
  exit 1
fi
if grep -Eq '10\.0\.0\.126|CASPERs-Mac-mini|colima-swarmbridged|test-access|test-secret|postgres(ql)?://' \
  <<<"$readiness_exec_calls"; then
  echo 'FAIL: managed worker storage readiness calls exposed a forbidden target or secret' >&2
  exit 1
fi

vision_cutover_gate_line="$(
  grep -nF 'docker|run --rm' "$CALLS" \
    | grep -F 'runtime_schedules' \
    | grep -F 'vp:tasks:vision' \
    | head -1 \
    | cut -d: -f1 \
    || true
)"
first_service_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
if [[ -z "$vision_cutover_gate_line" \
  || -z "$first_service_update_line" \
  || "$vision_cutover_gate_line" -ge "$first_service_update_line" ]]; then
  echo 'FAIL: CLOSED vision cutover gate must precede every service update' >&2
  exit 1
fi

runner_identity_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-channelops-runner-go:deploy-0123456789ab' \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | head -n 1
)"
if [[ "$runner_identity_update" != *'--env-rm CHANNELOPS_RUNNER_ID'* \
  || "$runner_identity_update" != *'--env-add CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1'* ]]; then
  echo 'FAIL: managed ChannelOps identity must replace a prior service identity' >&2
  exit 1
fi
if [[ "$runner_identity_update" != *'--health-cmd wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1'* \
  || "$runner_identity_update" != *'--health-interval 10s'* \
  || "$runner_identity_update" != *'--health-timeout 3s'* \
  || "$runner_identity_update" != *'--health-retries 6'* \
  || "$runner_identity_update" != *'--health-start-period 10s'* ]]; then
  echo 'FAIL: managed ChannelOps update must replace service-level health with active readyz' >&2
  exit 1
fi

python_worker_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:deploy-0123456789ab' \
    | grep -F 'vp-ffmpeg-worker-gpu-swarm' \
    | head -1 \
    | cut -d: -f1
)"
vision_worker_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:deploy-0123456789ab' \
    | grep -F 'vp-vision-worker-swarm' \
    | head -1 \
    | cut -d: -f1
)"
publisher_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:deploy-0123456789ab' \
    | grep -F 'vp-youtube-publisher-swarm' \
    | head -1 \
    | cut -d: -f1
)"
python_listener_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-backend-api:deploy-0123456789ab' \
    | grep -F 'vp-autoflow-api-swarm' \
    | head -1 \
    | cut -d: -f1
)"
if [[ -z "$python_worker_update_line" \
  || -z "$vision_worker_update_line" \
  || -z "$publisher_update_line" \
  || -z "$python_listener_update_line" \
  || "$python_worker_update_line" -ge "$python_listener_update_line" \
  || "$vision_worker_update_line" -ge "$python_listener_update_line" \
  || "$publisher_update_line" -ge "$python_listener_update_line" ]]; then
  echo 'FAIL: claim-aware Python event producers must deploy before their listener' >&2
  exit 1
fi

legacy_vision_remove_line="$(
  grep -nF "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
vision_running_line="$(
  grep -nF 'running|vp-vision-worker-swarm' "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
vision_readiness_probe_line="$(
  grep -nF "$vision_readiness_probe" "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
vision_consumer_cutover_line="$(
  grep -nF 'python -m app.services.vision_consumer_cutover' "$CALLS" \
    | head -1 \
    | cut -d: -f1 \
    || true
)"
if [[ -z "$legacy_vision_remove_line" \
  || -z "$vision_running_line" \
  || -z "$vision_readiness_probe_line" \
  || -z "$vision_consumer_cutover_line" \
  || "$vision_running_line" -ge "$legacy_vision_remove_line" \
  || "$vision_readiness_probe_line" -ge "$legacy_vision_remove_line" \
  || "$legacy_vision_remove_line" -ge "$vision_consumer_cutover_line" ]]; then
  echo 'FAIL: managed health/readiness, legacy retirement, and consumer reconciliation order is unsafe' >&2
  exit 1
fi

cp "$CALLS" "$TEST_ROOT/successful-vision-deploy-calls"

worker_service_ps_call() {
  local service="$1"
  printf 'docker|service ps %s --filter desired-state=running --format {{.Node}}|{{.CurrentState}}\n' \
    "$service"
}

assert_managed_worker_gate_sequence() {
  local service="$1"
  local readiness_probe="$2"
  local running_line
  local placement_line
  local container_line
  local exec_line
  running_line="$(grep -nF "running|$service" "$CALLS" | head -1 | cut -d: -f1)"
  placement_line="$(grep -nF "$(worker_service_ps_call "$service")" "$CALLS" | head -1 | cut -d: -f1)"
  container_line="$(grep -nF "docker|container ls --filter label=com.docker.swarm.service.name=$service" "$CALLS" | head -1 | cut -d: -f1)"
  exec_line="$(grep -nF "$readiness_probe" "$CALLS" | head -1 | cut -d: -f1)"
  if [[ -z "$running_line" || -z "$placement_line" || -z "$container_line" || -z "$exec_line" \
    || "$running_line" -ge "$placement_line" \
    || "$placement_line" -ge "$container_line" \
    || "$container_line" -ge "$exec_line" ]]; then
    echo "FAIL: $service readiness gates are not running -> exact placement -> local task -> exec" >&2
    exit 1
  fi
}

assert_managed_worker_gate_sequence "$VP_PYTHON_WORKER_SERVICE" "$gpu_readiness_probe"
assert_managed_worker_gate_sequence "$VP_VISION_WORKER_SERVICE" "$vision_readiness_probe"
assert_managed_worker_gate_sequence "$VP_PUBLISHER_SERVICE" "$publisher_readiness_probe"

if docker service ps "$VP_PYTHON_WORKER_SERVICE" \
  --filter desired-state=running \
  --format '{{.Node}}' >/dev/null 2>&1; then
  echo 'FAIL: service ps fake accepted an incomplete placement format' >&2
  exit 1
fi

deploy_managed_worker_for_gate_test() {
  local service="$1"
  case "$service" in
    "$VP_PYTHON_WORKER_SERVICE")
      vp_deploy_python_worker vp-ffmpeg-worker-python:placement-gate-test
      ;;
    "$VP_VISION_WORKER_SERVICE")
      vp_deploy_vision_worker vp-ffmpeg-worker-python:placement-gate-test
      ;;
    "$VP_PUBLISHER_SERVICE")
      vp_deploy_publisher vp-ffmpeg-worker-python:placement-gate-test
      ;;
    *)
      return 1
      ;;
  esac
}

set_managed_worker_task() {
  local service="$1"
  local node="$2"
  local state="$3"
  case "$service" in
    "$VP_PYTHON_WORKER_SERVICE")
      GPU_TASK_NODE="$node"
      GPU_TASK_STATE="$state"
      ;;
    "$VP_VISION_WORKER_SERVICE")
      VISION_TASK_NODE="$node"
      VISION_TASK_STATE="$state"
      ;;
    "$VP_PUBLISHER_SERVICE")
      PUBLISHER_TASK_NODE="$node"
      PUBLISHER_TASK_STATE="$state"
      ;;
    *)
      return 1
      ;;
  esac
}

assert_bad_worker_task_rejected_before_readiness() {
  local service="$1"
  local node="$2"
  local state="$3"
  : >"$CALLS"
  set_managed_worker_task "$service" "$node" "$state"
  if deploy_managed_worker_for_gate_test "$service" >/dev/null 2>&1; then
    echo "FAIL: $service accepted task $node|$state" >&2
    exit 1
  fi
  if ! grep -Fqx "$(worker_service_ps_call "$service")" "$CALLS" \
    || grep -Fq "docker|container ls --filter label=com.docker.swarm.service.name=$service" "$CALLS" \
    || grep -Fq 'docker|exec|' "$CALLS"; then
    echo "FAIL: $service task $node|$state reached readiness after placement failure" >&2
    exit 1
  fi
  set_managed_worker_task "$service" ccttww-lap 'Running 2 seconds ago'
}

for worker_service in \
  "$VP_PYTHON_WORKER_SERVICE" \
  "$VP_VISION_WORKER_SERVICE" \
  "$VP_PUBLISHER_SERVICE"; do
  for forbidden_node in 10.0.0.126 CASPERs-Mac-mini colima-swarmbridged; do
    assert_bad_worker_task_rejected_before_readiness \
      "$worker_service" "$forbidden_node" 'Running 2 seconds ago'
  done
  assert_bad_worker_task_rejected_before_readiness \
    "$worker_service" ccttww-lap 'Shutdown 2 seconds ago'
done

: >"$CALLS"
rm -f "$WORKER_READINESS_CONTAINER_CALLS"
WORKER_READINESS_CONTAINER_MODE=transition
if ! vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false; then
  echo 'FAIL: publisher readiness did not wait for the local task set to converge' >&2
  exit 1
fi
if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 3 \
  || "$(grep -Fc 'sleep|1' "$CALLS")" -ne 2 \
  || "$(grep -Fc "$publisher_readiness_probe" "$CALLS")" -ne 1 ]]; then
  echo 'FAIL: publisher readiness convergence did not poll twice before one probe execution' >&2
  exit 1
fi
third_publisher_container_line="$(
  grep -nF "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS" \
    | sed -n '3p' \
    | cut -d: -f1
)"
publisher_exec_line="$(grep -nF "$publisher_readiness_probe" "$CALLS" | cut -d: -f1)"
if [[ -z "$third_publisher_container_line" \
  || -z "$publisher_exec_line" \
  || "$third_publisher_container_line" -ge "$publisher_exec_line" ]]; then
  echo 'FAIL: publisher readiness probe ran before the local task set converged' >&2
  exit 1
fi
WORKER_READINESS_CONTAINER_MODE=normal

assert_readiness_failure_calls_are_safe() {
  local readiness_calls
  readiness_calls="$(grep -E '^(docker\|(container ls|exec)|log\|managed worker storage readiness)' "$CALLS" || true)"
  if grep -Eq '10\.0\.0\.126|CASPERs-Mac-mini|colima-swarmbridged|test-access|test-secret|postgres(ql)?://' \
    <<<"$readiness_calls"; then
    echo 'FAIL: readiness failure calls exposed a forbidden target or secret' >&2
    exit 1
  fi
}

assert_persistent_readiness_container_set_rejected() {
  local mode="$1"
  : >"$CALLS"
  WORKER_READINESS_CONTAINER_MODE="$mode"
  if vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false \
    2>"$TEST_ROOT/readiness-${mode}.err"; then
    echo "FAIL: persistent $mode readiness container set unexpectedly succeeded" >&2
    exit 1
  fi
  if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 10 \
    || "$(grep -Fc 'sleep|1' "$CALLS")" -ne 9 \
    || "$(grep -Fc 'docker|exec|' "$CALLS")" -ne 0 ]]; then
    echo "FAIL: persistent $mode readiness container set did not stop at the bounded limit" >&2
    exit 1
  fi
}

assert_persistent_readiness_container_set_rejected missing
assert_persistent_readiness_container_set_rejected duplicate

: >"$CALLS"
WORKER_READINESS_CONTAINER_MODE=error
if vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false \
  2>"$TEST_ROOT/readiness-list-error.err"; then
  echo 'FAIL: readiness container discovery error unexpectedly succeeded' >&2
  exit 1
fi
if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 1 \
  || "$(grep -Fc 'sleep|' "$CALLS")" -ne 0 \
  || "$(grep -Fc 'docker|exec|' "$CALLS")" -ne 0 \
  || ! "$(cat "$TEST_ROOT/readiness-list-error.err")" =~ ^managed\ worker\ container\ discovery\ failed:\ $VP_PUBLISHER_SERVICE$ ]]; then
  echo 'FAIL: readiness container discovery error did not fail immediately with a service-only message' >&2
  exit 1
fi
if grep -Eq 'test-secret|111111|222222|333333|tcp://' "$TEST_ROOT/readiness-list-error.err"; then
  echo 'FAIL: readiness container discovery error exposed sensitive Docker output' >&2
  exit 1
fi

: >"$CALLS"
WORKER_READINESS_CONTAINER_MODE=normal
FAIL_WORKER_READINESS_COUNT=true
if vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false \
  2>"$TEST_ROOT/readiness-count-error.err"; then
  echo 'FAIL: readiness container count error unexpectedly succeeded' >&2
  exit 1
fi
FAIL_WORKER_READINESS_COUNT=false
if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 1 \
  || "$(grep -Fc 'sleep|' "$CALLS")" -ne 0 \
  || "$(grep -Fc 'docker|exec|' "$CALLS")" -ne 0 ]]; then
  echo 'FAIL: readiness container count error did not fail immediately' >&2
  exit 1
fi

: >"$CALLS"
WORKER_READINESS_CONTAINER_MODE=missing
FAIL_WORKER_READINESS_SLEEP=true
if vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false \
  2>"$TEST_ROOT/readiness-sleep-error.err"; then
  echo 'FAIL: readiness convergence sleep error unexpectedly succeeded' >&2
  exit 1
fi
FAIL_WORKER_READINESS_SLEEP=false
if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 1 \
  || "$(grep -Fc 'sleep|1' "$CALLS")" -ne 1 \
  || "$(grep -Fc 'docker|exec|' "$CALLS")" -ne 0 ]]; then
  echo 'FAIL: readiness convergence sleep error did not fail immediately' >&2
  exit 1
fi

: >"$CALLS"
WORKER_READINESS_CONTAINER_MODE=normal
FAIL_WORKER_READINESS_SERVICE="$VP_PUBLISHER_SERVICE"
if vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false \
  2>"$TEST_ROOT/readiness-exec-error.err"; then
  echo 'FAIL: readiness execution error unexpectedly succeeded' >&2
  exit 1
fi
FAIL_WORKER_READINESS_SERVICE=
if [[ "$(grep -Fc "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PUBLISHER_SERVICE" "$CALLS")" -ne 1 \
  || "$(grep -Fc 'sleep|' "$CALLS")" -ne 0 \
  || "$(grep -Fc "$publisher_readiness_probe" "$CALLS")" -ne 1 \
  || ! "$(cat "$TEST_ROOT/readiness-exec-error.err")" =~ ^managed\ worker\ storage\ readiness\ failed:\ $VP_PUBLISHER_SERVICE$ ]]; then
  echo 'FAIL: readiness execution error did not fail once with a service-only message' >&2
  exit 1
fi
if grep -Eq 'test-secret|111111|222222|333333|No such container' "$TEST_ROOT/readiness-exec-error.err"; then
  echo 'FAIL: readiness execution error exposed sensitive Docker output' >&2
  exit 1
fi

assert_deploy_rejected_by_readiness() {
  local name="$1"
  : >"$CALLS"
  GPU_SERVICE_EXISTS=true
  VISION_SERVICE_EXISTS=true
  PUBLISHER_SERVICE_EXISTS=true
  LEGACY_VISION_CONTAINER_EXISTS=true
  if deploy_vp_app_services \
    "vp-api:worker-readiness-$name" \
    "vp-frontend:worker-readiness-$name" \
    "vp-backend-api:worker-readiness-$name" \
    "vp-channelops-runner-go:worker-readiness-$name" \
    "vp-ffmpeg-worker-go:worker-readiness-$name" \
    "vp-ffmpeg-worker-python:worker-readiness-$name" >/dev/null 2>&1; then
    echo "FAIL: $name worker storage readiness failure unexpectedly allowed deployment" >&2
    exit 1
  fi
  if ! grep -Fq 'log|VideoProcess service apply failed; restoring prior images without legacy placement' "$CALLS"; then
    echo "FAIL: $name readiness failure did not trigger the existing deployment rollback" >&2
    exit 1
  fi
  assert_readiness_failure_calls_are_safe
}

WORKER_READINESS_CONTAINER_MODE=missing
FAIL_WORKER_READINESS_SERVICE=
assert_deploy_rejected_by_readiness missing-container
if grep -Fq 'docker|exec|' "$CALLS" \
  || grep -Fq "docker|container ls --filter label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS"; then
  echo 'FAIL: missing readiness container advanced past the GPU deployment gate' >&2
  exit 1
fi

WORKER_READINESS_CONTAINER_MODE=duplicate
assert_deploy_rejected_by_readiness duplicate-container
if grep -Fq 'docker|exec|' "$CALLS" \
  || grep -Fq "docker|container ls --filter label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS"; then
  echo 'FAIL: duplicate readiness containers advanced past the GPU deployment gate' >&2
  exit 1
fi

WORKER_READINESS_CONTAINER_MODE=normal
for failed_readiness_service in \
  "$VP_PYTHON_WORKER_SERVICE" \
  "$VP_VISION_WORKER_SERVICE" \
  "$VP_PUBLISHER_SERVICE"; do
  FAIL_WORKER_READINESS_SERVICE="$failed_readiness_service"
  assert_deploy_rejected_by_readiness "exec-${failed_readiness_service}"
  case "$failed_readiness_service" in
    "$VP_PYTHON_WORKER_SERVICE")
      if grep -Fq "$vision_readiness_probe" "$CALLS" \
        || grep -Fq "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS"; then
        echo 'FAIL: GPU readiness execution failure advanced to vision retirement' >&2
        exit 1
      fi
      ;;
    "$VP_VISION_WORKER_SERVICE")
      if grep -Fq "$publisher_readiness_probe" "$CALLS" \
        || grep -Fq "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS"; then
        echo 'FAIL: vision readiness execution failure advanced to retirement or publisher' >&2
        exit 1
      fi
      ;;
    "$VP_PUBLISHER_SERVICE")
      if grep -Fq -- '--image vp-backend-api:worker-readiness-' "$CALLS"; then
        echo 'FAIL: publisher readiness execution failure advanced to later managed services' >&2
        exit 1
      fi
      ;;
  esac
done
FAIL_WORKER_READINESS_SERVICE=

: >"$CALLS"
if vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" invalid >/dev/null 2>&1; then
  echo 'FAIL: invalid artifact API readiness mode unexpectedly succeeded' >&2
  exit 1
fi
if ! grep -Fq "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PYTHON_WORKER_SERVICE" "$CALLS" \
  || grep -Fq 'docker|exec|' "$CALLS"; then
  echo 'FAIL: invalid artifact API readiness mode did not fail before probe execution' >&2
  exit 1
fi

deploy_worker_review_fixture() {
  local name="$1"
  deploy_vp_app_services \
    "vp-api:worker-review-$name" \
    "vp-frontend:worker-review-$name" \
    "vp-backend-api:worker-review-$name" \
    "vp-channelops-runner-go:worker-review-$name" \
    "vp-ffmpeg-worker-go:worker-review-$name" \
    "vp-ffmpeg-worker-python:worker-review-$name"
}

assert_rollback_targets_are_safe() {
  local placement_calls
  placement_calls="$(grep -E '^docker\|(node update|service (create|update))' "$CALLS" || true)"
  if grep -Eq -- '--constraint(-add)? node\.hostname==(10\.0\.0\.126|CASPERs-Mac-mini|colima-swarmbridged)( |$)' \
    <<<"$placement_calls" \
    || grep -Eq 'docker\|node update.*(10\.0\.0\.126|CASPERs-Mac-mini|colima-swarmbridged)( |$)' \
      <<<"$placement_calls"; then
    echo 'FAIL: rollback used a forbidden execution or placement target' >&2
    exit 1
  fi
}

first_call_line_after() {
  local after_line="$1"
  local needle="$2"
  grep -nF "$needle" "$CALLS" \
    | awk -F: -v after_line="$after_line" '$1 > after_line { print $1; exit }'
}

assert_worker_gate_sequence_after() {
  local service="$1"
  local readiness_probe="$2"
  local after_line="$3"
  local running_line
  local placement_line
  local container_line
  local exec_line
  running_line="$(first_call_line_after "$after_line" "running|$service")"
  placement_line="$(first_call_line_after "$after_line" "$(worker_service_ps_call "$service")")"
  container_line="$(first_call_line_after "$after_line" "docker|container ls --filter label=com.docker.swarm.service.name=$service")"
  exec_line="$(first_call_line_after "$after_line" "$readiness_probe")"
  if [[ -z "$running_line" || -z "$placement_line" || -z "$container_line" || -z "$exec_line" \
    || "$running_line" -ge "$placement_line" \
    || "$placement_line" -ge "$container_line" \
    || "$container_line" -ge "$exec_line" ]]; then
    echo "FAIL: $service baseline restore did not run the complete readiness gate chain" >&2
    exit 1
  fi
}

: >"$CALLS"
GPU_SERVICE_EXISTS=true
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
FAIL_NODE_UPDATE=true
gpu_node_failure_contract_failed=false
if deploy_worker_review_fixture gpu-node-label-write-failure \
  >"$TEST_ROOT/gpu-node-label-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU node label update unexpectedly allowed deployment' >&2
  exit 1
fi
gpu_node_update_line="$(
  grep -nF 'docker|node update --label-add vp.gpu=true ccttww-lap' "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
gpu_node_rollback_line="$(
  grep -nF 'log|VideoProcess service apply failed; restoring prior images without legacy placement' "$CALLS" \
    | head -1 \
    | cut -d: -f1
)"
if [[ -z "$gpu_node_update_line" || -z "$gpu_node_rollback_line" \
  || "$gpu_node_update_line" -ge "$gpu_node_rollback_line" ]]; then
  echo 'FAIL: GPU node label failure did not enter the outer rollback transaction' >&2
  exit 1
fi
if sed -n "$((gpu_node_update_line + 1)),$((gpu_node_rollback_line - 1))p" "$CALLS" \
  | grep -Eq "docker\\|service (create|update).*${VP_PYTHON_WORKER_SERVICE}|docker\\|service ps ${VP_PYTHON_WORKER_SERVICE}|docker\\|container ls --filter label=com.docker.swarm.service.name=${VP_PYTHON_WORKER_SERVICE}|docker\\|exec\\|${GPU_READINESS_CONTAINER_ID}"; then
  gpu_node_failure_contract_failed=true
fi
gpu_node_baseline_line="$(
  grep -nF -- '--image baseline-vp-ffmpeg-worker-gpu-swarm:stable vp-ffmpeg-worker-gpu-swarm' "$CALLS" \
    | tail -1 \
    | cut -d: -f1
)"
if [[ -z "$gpu_node_baseline_line" || "$gpu_node_rollback_line" -ge "$gpu_node_baseline_line" ]]; then
  echo 'FAIL: GPU node label failure did not explicitly restore the baseline' >&2
  exit 1
fi
assert_worker_gate_sequence_after \
  "$VP_PYTHON_WORKER_SERVICE" "$gpu_readiness_probe" "$gpu_node_baseline_line"
FAIL_NODE_UPDATE=false

assert_gpu_constraints_normalized() {
  local update_call="$1"
  local constraint
  for constraint in \
    'node.labels.vp.runtime==true' \
    'node.labels.vp.legacy==true' \
    'node.labels.role==app' \
    'node.hostname==CASPERs-Mac-mini' \
    'node.hostname==colima-swarmbridged'; do
    if [[ "$update_call" != *"--constraint-rm $constraint"* ]]; then
      echo "FAIL: GPU placement did not remove existing constraint: $constraint" >&2
      exit 1
    fi
  done
  for constraint in \
    'node.labels.vp.gpu==true' \
    'node.hostname==ccttww-lap'; do
    if [[ "$update_call" == *"--constraint-rm $constraint"* \
      || "$update_call" == *"--constraint-add $constraint"* ]]; then
      echo "FAIL: GPU placement rewrote an already exact approved constraint: $constraint" >&2
      exit 1
    fi
  done
  if [[ "$update_call" == *'--constraint-add node.labels.vp.runtime==true'* \
    || "$update_call" == *'--constraint-add node.labels.vp.legacy==true'* \
    || "$update_call" == *'--constraint-add node.labels.role==app'* \
    || "$update_call" == *'--constraint-add node.hostname==CASPERs-Mac-mini'* \
    || "$update_call" == *'--constraint-add node.hostname==colima-swarmbridged'* ]]; then
    echo 'FAIL: GPU placement added a forbidden or stale constraint' >&2
    exit 1
  fi
}

: >"$CALLS"
CONSTRAINT_MODE=gpu-stale
vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-placement-normalization \
  >/dev/null
gpu_normalized_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:gpu-placement-normalization' \
    | grep -F "$VP_PYTHON_WORKER_SERVICE" \
    | head -1
)"
if [[ -z "$gpu_normalized_update" ]]; then
  echo 'FAIL: normal GPU placement fixture did not issue a service update' >&2
  exit 1
fi
assert_gpu_constraints_normalized "$gpu_normalized_update"
assert_rollback_targets_are_safe

: >"$CALLS"
CONSTRAINT_MODE=gpu-duplicate
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-duplicate-constraint \
  >/dev/null 2>&1; then
  echo 'FAIL: duplicate approved GPU constraints unexpectedly passed' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq "running|$VP_PYTHON_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "$gpu_readiness_probe" "$CALLS"; then
  echo 'FAIL: duplicate approved GPU constraints reached a service write or readiness' >&2
  exit 1
fi

: >"$CALLS"
CONSTRAINT_MODE=gpu-stale
FAIL_GPU_CONSTRAINT_INSPECT=true
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-constraint-inspect-failure \
  >/dev/null 2>&1; then
  echo 'FAIL: GPU constraint inspect failure unexpectedly passed deployment' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq "running|$VP_PYTHON_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "$gpu_readiness_probe" "$CALLS"; then
  echo 'FAIL: GPU constraint inspect failure reached a service write or readiness' >&2
  exit 1
fi

: >"$CALLS"
if vp_restore_gpu_service baseline-vp-ffmpeg-worker-gpu-swarm:inspect-failure \
  >/dev/null 2>&1; then
  echo 'FAIL: GPU rollback constraint inspect failure unexpectedly passed' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq "running|$VP_PYTHON_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "$gpu_readiness_probe" "$CALLS"; then
  echo 'FAIL: GPU rollback inspect failure reached a service write or readiness' >&2
  exit 1
fi
FAIL_GPU_CONSTRAINT_INSPECT=false
CONSTRAINT_MODE=gpu-stale

: >"$CALLS"
rm -f "$WORKER_READINESS_FAILURE_USED"
WORKER_READINESS_EXEC_MODE=fail-first
WORKER_READINESS_FAIL_SERVICE="$VP_PYTHON_WORKER_SERVICE"
if deploy_worker_review_fixture gpu-placement-rollback \
  >"$TEST_ROOT/gpu-placement-rollback.out" 2>&1; then
  echo 'FAIL: GPU rollback fixture unexpectedly succeeded after first readiness failure' >&2
  exit 1
fi
gpu_constraint_baseline_line="$(
  grep -nF -- '--image baseline-vp-ffmpeg-worker-gpu-swarm:stable vp-ffmpeg-worker-gpu-swarm' "$CALLS" \
    | tail -1 \
    | cut -d: -f1
)"
gpu_constraint_baseline_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image baseline-vp-ffmpeg-worker-gpu-swarm:stable' \
    | grep -F "$VP_PYTHON_WORKER_SERVICE" \
    | tail -1
)"
if [[ -z "$gpu_constraint_baseline_line" || -z "$gpu_constraint_baseline_update" ]]; then
  echo 'FAIL: GPU placement rollback did not issue the baseline update' >&2
  exit 1
fi
assert_gpu_constraints_normalized "$gpu_constraint_baseline_update"
assert_worker_gate_sequence_after \
  "$VP_PYTHON_WORKER_SERVICE" "$gpu_readiness_probe" "$gpu_constraint_baseline_line"
assert_rollback_targets_are_safe
WORKER_READINESS_EXEC_MODE=normal
WORKER_READINESS_FAIL_SERVICE=
rm -f "$WORKER_READINESS_FAILURE_USED"
CONSTRAINT_MODE=legacy

if [[ "$gpu_node_failure_contract_failed" == true ]]; then
  echo 'FAIL: GPU node label failure reached a GPU write, placement, or readiness gate' >&2
  exit 1
fi

: >"$CALLS"
GPU_SERVICE_EXISTS=true
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
FAIL_UPDATE_SERVICE="$VP_PYTHON_WORKER_SERVICE"
FAIL_UPDATE_IMAGE=vp-ffmpeg-worker-python:worker-review-gpu-update-write-failure
if deploy_worker_review_fixture gpu-update-write-failure >"$TEST_ROOT/gpu-update-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU update was masked by an old healthy task' >&2
  exit 1
fi
gpu_failed_update_call="$(grep -F 'docker|service update' "$CALLS" \
  | grep -F -- '--image vp-ffmpeg-worker-python:worker-review-gpu-update-write-failure' \
  | grep -F "$VP_PYTHON_WORKER_SERVICE" \
  | head -1)"
if [[ -z "$gpu_failed_update_call" ]]; then
  echo 'FAIL: GPU update failure fixture did not issue the attempted image update' >&2
  exit 1
fi
gpu_attempt_line="$(grep -nF "$gpu_failed_update_call" "$CALLS" | head -1 | cut -d: -f1)"
grep -Fq 'log|VideoProcess service apply failed; restoring prior images without legacy placement' "$CALLS"
gpu_baseline_line="$(grep -nF -- '--image baseline-vp-ffmpeg-worker-gpu-swarm:stable vp-ffmpeg-worker-gpu-swarm' "$CALLS" | tail -1 | cut -d: -f1)"
if [[ -z "$gpu_attempt_line" || -z "$gpu_baseline_line" || "$gpu_attempt_line" -ge "$gpu_baseline_line" ]]; then
  echo 'FAIL: GPU update rollback did not attempt the exact baseline image after the failed write' >&2
  exit 1
fi
if grep -nF "$gpu_readiness_probe" "$CALLS" \
  | awk -F: -v attempt_line="$gpu_attempt_line" -v baseline_line="$gpu_baseline_line" \
    '$1 > attempt_line && $1 < baseline_line { found=1 } END { exit found ? 0 : 1 }'; then
  echo 'FAIL: failed GPU update reached readiness before baseline rollback' >&2
  exit 1
fi
assert_worker_gate_sequence_after "$VP_PYTHON_WORKER_SERVICE" "$gpu_readiness_probe" "$gpu_baseline_line"
if grep -Fq 'VideoProcess image restore did not fully converge' "$TEST_ROOT/gpu-update-write-failure.out"; then
  echo 'FAIL: successful GPU baseline readiness was reported as non-convergent' >&2
  exit 1
fi
assert_rollback_targets_are_safe
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

: >"$CALLS"
GPU_SERVICE_EXISTS=false
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
FAIL_GPU_CREATE=true
if deploy_worker_review_fixture gpu-create-write-failure >"$TEST_ROOT/gpu-create-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU create was masked by a healthy task' >&2
  exit 1
fi
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq 'log|VideoProcess service apply failed; restoring prior images without legacy placement' "$CALLS"
grep -Fq 'docker|service rm vp-ffmpeg-worker-gpu-swarm' "$CALLS"
if grep -Fq "docker|container ls --filter label=com.docker.swarm.service.name=$VP_PYTHON_WORKER_SERVICE" "$CALLS" \
  || grep -Fq "$gpu_readiness_probe" "$CALLS"; then
  echo 'FAIL: failed GPU create reached readiness before rollback' >&2
  exit 1
fi
assert_rollback_targets_are_safe
FAIL_GPU_CREATE=false
GPU_SERVICE_EXISTS=true

assert_rollback_readiness_recovered() {
  local service="$1"
  local readiness_probe="$2"
  local output="$3"
  local baseline_line
  baseline_line="$(grep -nF -- "--image baseline-$service:stable $service" "$CALLS" | tail -1 | cut -d: -f1)"
  if [[ "$(grep -F "$readiness_probe" "$CALLS" | wc -l | tr -d ' ')" -lt 2 \
    || -z "$baseline_line" ]]; then
    echo "FAIL: $service rollback did not restore baseline readiness" >&2
    exit 1
  fi
  if grep -Fq 'VideoProcess image restore did not fully converge' "$output"; then
    echo "FAIL: $service rollback reported non-convergence after baseline readiness passed" >&2
    exit 1
  fi
  assert_worker_gate_sequence_after "$service" "$readiness_probe" "$baseline_line"
}

for rollback_service in \
  "$VP_PYTHON_WORKER_SERVICE" \
  "$VP_VISION_WORKER_SERVICE" \
  "$VP_PUBLISHER_SERVICE"; do
  : >"$CALLS"
  rm -f "$WORKER_READINESS_FAILURE_USED"
  GPU_SERVICE_EXISTS=true
  VISION_SERVICE_EXISTS=true
  PUBLISHER_SERVICE_EXISTS=true
  LEGACY_VISION_CONTAINER_EXISTS=true
  WORKER_READINESS_EXEC_MODE=fail-first
  WORKER_READINESS_FAIL_SERVICE="$rollback_service"
  rollback_output="$TEST_ROOT/${rollback_service}-first-readiness-failure.out"
  if deploy_worker_review_fixture "${rollback_service}-first-readiness-failure" >"$rollback_output" 2>&1; then
    echo "FAIL: $rollback_service first readiness failure unexpectedly succeeded" >&2
    exit 1
  fi
  case "$rollback_service" in
    "$VP_PYTHON_WORKER_SERVICE")
      assert_rollback_readiness_recovered "$rollback_service" "$gpu_readiness_probe" "$rollback_output"
      ;;
    "$VP_VISION_WORKER_SERVICE")
      assert_rollback_readiness_recovered "$rollback_service" "$vision_readiness_probe" "$rollback_output"
      ;;
    "$VP_PUBLISHER_SERVICE")
      assert_rollback_readiness_recovered "$rollback_service" "$publisher_readiness_probe" "$rollback_output"
      ;;
  esac
  assert_rollback_targets_are_safe
done
WORKER_READINESS_EXEC_MODE=normal
WORKER_READINESS_FAIL_SERVICE=
rm -f "$WORKER_READINESS_FAILURE_USED"

for rollback_service in \
  "$VP_PYTHON_WORKER_SERVICE" \
  "$VP_VISION_WORKER_SERVICE" \
  "$VP_PUBLISHER_SERVICE"; do
  : >"$CALLS"
  GPU_SERVICE_EXISTS=true
  VISION_SERVICE_EXISTS=true
  PUBLISHER_SERVICE_EXISTS=true
  LEGACY_VISION_CONTAINER_EXISTS=true
  FAIL_WORKER_READINESS_SERVICE="$rollback_service"
  rollback_output="$TEST_ROOT/${rollback_service}-persistent-readiness-failure.out"
  if deploy_worker_review_fixture "${rollback_service}-persistent-readiness-failure" >"$rollback_output" 2>&1; then
    echo "FAIL: persistent $rollback_service readiness failure unexpectedly succeeded" >&2
    exit 1
  fi
  case "$rollback_service" in
    "$VP_PYTHON_WORKER_SERVICE") readiness_probe="$gpu_readiness_probe" ;;
    "$VP_VISION_WORKER_SERVICE") readiness_probe="$vision_readiness_probe" ;;
    "$VP_PUBLISHER_SERVICE") readiness_probe="$publisher_readiness_probe" ;;
  esac
  baseline_line="$(grep -nF -- "--image baseline-$rollback_service:stable $rollback_service" "$CALLS" | tail -1 | cut -d: -f1)"
  if [[ -z "$baseline_line" \
    || "$(grep -F "$readiness_probe" "$CALLS" | wc -l | tr -d ' ')" -lt 2 ]]; then
    echo "FAIL: persistent $rollback_service failure did not attempt and verify baseline restore" >&2
    exit 1
  fi
  assert_worker_gate_sequence_after "$rollback_service" "$readiness_probe" "$baseline_line"
  grep -Fq 'VideoProcess image restore did not fully converge' "$rollback_output"
  assert_rollback_targets_are_safe
done
FAIL_WORKER_READINESS_SERVICE=
: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=true
LEGACY_VISION_PROJECT=unexpected-project
LEGACY_VISION_SERVICE=vision-worker
if vp_retire_legacy_vision_worker >/dev/null 2>&1; then
  echo 'FAIL: mismatched legacy vision project was removed' >&2
  exit 1
fi
if grep -Fq 'docker|rm -f ' "$CALLS"; then
  echo 'FAIL: mismatched legacy vision identity reached docker rm' >&2
  exit 1
fi

: >"$CALLS"
LEGACY_VISION_PROJECT=videoprocess
LEGACY_VISION_SERVICE=unexpected-service
if vp_retire_legacy_vision_worker >/dev/null 2>&1; then
  echo 'FAIL: mismatched legacy vision service was removed' >&2
  exit 1
fi
if grep -Fq 'docker|rm -f ' "$CALLS"; then
  echo 'FAIL: mismatched legacy vision service reached docker rm' >&2
  exit 1
fi

: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=false
LEGACY_VISION_PROJECT=videoprocess
LEGACY_VISION_SERVICE=vision-worker
vp_retire_legacy_vision_worker >/dev/null
if grep -Fq 'docker|rm -f ' "$CALLS"; then
  echo 'FAIL: absent legacy vision container reached docker rm' >&2
  exit 1
fi

: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=true
LEGACY_VISION_CONTAINER_RUNNING=false
if vp_retire_legacy_vision_worker >/dev/null 2>&1; then
  echo 'FAIL: stopped legacy vision container was removed' >&2
  exit 1
fi
if grep -Fq 'docker|rm -f ' "$CALLS"; then
  echo 'FAIL: stopped legacy vision container reached docker rm' >&2
  exit 1
fi
LEGACY_VISION_CONTAINER_RUNNING=true

: >"$CALLS"
LEGACY_VISION_CONTAINER_NAME=/replacement_container
if vp_retire_legacy_vision_worker >/dev/null 2>&1; then
  echo 'FAIL: renamed legacy vision container was removed' >&2
  exit 1
fi
if grep -Fq 'docker|rm -f ' "$CALLS"; then
  echo 'FAIL: renamed legacy vision container reached docker rm' >&2
  exit 1
fi
LEGACY_VISION_CONTAINER_NAME=/vp_vision_worker_1
LEGACY_VISION_CONTAINER_EXISTS=true
cp "$TEST_ROOT/successful-vision-deploy-calls" "$CALLS"

backend_migration_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-backend-api:deploy-0123456789ab' \
    | grep -F 'vp-event-outbox-relay-swarm' \
    | head -1 \
    | cut -d: -f1
)"
migration_gate_line="$(
  grep -nF 'docker|run --rm' "$CALLS" \
    | grep -F -- '--env DATABASE_URL' \
    | grep -F 'SELECT version_num FROM alembic_version' \
    | head -1 \
    | cut -d: -f1
)"
runner_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | grep -F -- '--image vp-channelops-runner-go:deploy-0123456789ab' \
    | head -1 \
    | cut -d: -f1
)"
if [[ -z "$backend_migration_update_line" \
  || -z "$migration_gate_line" \
  || -z "$runner_update_line" \
  || "$backend_migration_update_line" -ge "$migration_gate_line" \
  || "$migration_gate_line" -ge "$runner_update_line" ]]; then
  echo 'FAIL: exact migration head gate must run after backend and before runner update' >&2
  exit 1
fi
migration_gate_call="$(
  grep -F 'docker|run --rm' "$CALLS" \
    | grep -F -- '--env DATABASE_URL' \
    | grep -F 'SELECT version_num FROM alembic_version' \
    | head -1
)"
if [[ "$migration_gate_call" == *"$VP_PYTHON_WORKER_DATABASE_URL"* ]]; then
  echo 'FAIL: migration gate printed the deploy database URL' >&2
  exit 1
fi

cp "$CALLS" "$TEST_ROOT/successful-deploy-calls"
for migration_gate_mode in wrong missing error; do
  : >"$CALLS"
  MIGRATION_GATE_MODE="$migration_gate_mode"
  if deploy_vp_app_services \
    "vp-api:gate-$migration_gate_mode" \
    "vp-frontend:gate-$migration_gate_mode" \
    "vp-backend-api:gate-$migration_gate_mode" \
    "vp-channelops-runner-go:gate-$migration_gate_mode" \
    "vp-ffmpeg-worker-go:gate-$migration_gate_mode" \
    "vp-ffmpeg-worker-python:gate-$migration_gate_mode" >/dev/null 2>&1; then
    echo "FAIL: $migration_gate_mode migration gate unexpectedly succeeded" >&2
    exit 1
  fi
  if grep -F 'docker|service update' "$CALLS" \
    | grep -Fq 'vp-channel-agent-runner-swarm'; then
    echo "FAIL: $migration_gate_mode migration gate mutated the runner" >&2
    exit 1
  fi
  if grep -F 'docker|service update' "$CALLS" \
    | grep -Eq -- '--image baseline-vp-(autoflow-api|event-outbox-relay)-swarm:stable'; then
    echo "FAIL: $migration_gate_mode rollback restored a pre-migration backend image" >&2
    exit 1
  fi
  if ! grep -Fq 'SELECT version_num FROM alembic_version' "$CALLS"; then
    echo "FAIL: $migration_gate_mode migration gate did not query alembic_version" >&2
    exit 1
  fi
done
MIGRATION_GATE_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=true
VISION_CUTOVER_GATE_MODE=unsafe
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: unsafe vision cutover gate unexpectedly allowed deployment' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq 'docker|rm -f ' "$CALLS" \
  || grep -Fq 'python -m app.services.vision_consumer_cutover' "$CALLS"; then
  echo 'FAIL: unsafe vision cutover gate mutated services or the legacy worker' >&2
  exit 1
fi
VISION_CUTOVER_GATE_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

: >"$CALLS"
VISION_CONSUMER_CUTOVER_MODE=failure
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: failed vision consumer reconciliation unexpectedly allowed deployment' >&2
  exit 1
fi
grep -Fq 'python -m app.services.vision_consumer_cutover' "$CALLS"
VISION_CONSUMER_CUTOVER_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=false
VISION_SERVICE_EXISTS=true
VISION_CUTOVER_GATE_MODE=unsafe
VISION_CONSUMER_CUTOVER_MODE=failure
VISION_CONSUMER_AUDIT_MODE=needs-cutover
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: incomplete vision consumer cutover was misclassified as converged' >&2
  exit 1
fi
grep -Fq 'python -m app.services.vision_consumer_cutover --check-only' "$CALLS"
grep -Fq 'runtime_schedules' "$CALLS"

: >"$CALLS"
VISION_CONSUMER_AUDIT_MODE=converged
if ! deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: converged vision deployment was blocked by the migration-only gate' >&2
  exit 1
fi
if grep -Fq 'runtime_schedules' "$CALLS" \
  || grep -F 'python -m app.services.vision_consumer_cutover' "$CALLS" \
    | grep -Fvq -- '--check-only'; then
  echo 'FAIL: converged vision deployment reran migration-only checks' >&2
  exit 1
fi

: >"$CALLS"
VISION_SERVICE_EXISTS=false
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: missing managed vision service bypassed the cutover gate' >&2
  exit 1
fi
grep -Fq 'runtime_schedules' "$CALLS"
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: missing managed vision service mutated services before the cutover gate' >&2
  exit 1
fi
VISION_SERVICE_EXISTS=true
VISION_CUTOVER_GATE_MODE=success
VISION_CONSUMER_CUTOVER_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

if [[ ! -x "$ROOT/bin/channelops-soak-watch.sh" ]]; then
  echo 'FAIL: successful deployment did not install an executable soak watcher' >&2
  exit 1
fi
if ! cmp -s "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"; then
  echo 'FAIL: installed soak watcher differs from the repository source' >&2
  exit 1
fi
cat >"$TEST_ROOT/expected-crontab" <<EOF
MAILTO=video-ops@example.com
0 2 * * * /usr/local/bin/backup-video-state
@reboot /usr/local/bin/restore-video-network
5 * * * * /usr/bin/sha256sum $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/watcher-audit.log 2>&1
10 * * * * /usr/local/bin/notify-ops channelops-soak-watch.sh
# audit checksum notification for $ROOT/bin/channelops-soak-watch.sh
# BEGIN VIDEOPROCESS SOAK WATCH
*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/channelops-soak-watch.log 2>&1
# END VIDEOPROCESS SOAK WATCH
EOF
if ! cmp -s "$TEST_ROOT/expected-crontab" "$FAKE_CRONTAB"; then
  echo 'FAIL: managed soak watcher cron did not preserve unrelated entries exactly' >&2
  diff -u "$TEST_ROOT/expected-crontab" "$FAKE_CRONTAB" >&2 || true
  exit 1
fi
if [[ "$(grep -Fc '# BEGIN VIDEOPROCESS SOAK WATCH' "$FAKE_CRONTAB" || true)" -ne 1 \
  || "$(grep -Fc '# END VIDEOPROCESS SOAK WATCH' "$FAKE_CRONTAB" || true)" -ne 1 ]]; then
  echo 'FAIL: successful deployment must leave exactly one managed cron block' >&2
  exit 1
fi
if [[ "$(grep -Fc "*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh" "$FAKE_CRONTAB" || true)" -ne 1 ]]; then
  echo 'FAIL: successful deployment must leave exactly one managed soak watcher command' >&2
  exit 1
fi
if grep -Fq '*/10 * * * *' "$FAKE_CRONTAB"; then
  echo 'FAIL: successful deployment retained the historical unmarked watcher line' >&2
  exit 1
fi
if grep -E '^crontab\|-l\|lc_all=' "$CALLS" | grep -Fvq 'lc_all=C'; then
  echo 'FAIL: a crontab read did not force the C locale' >&2
  exit 1
fi
final_health_line="$(grep -nF 'running|vp-youtube-publisher-swarm' "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
watcher_rename_line="$(grep -nE "mv\|-f .*/\.channelops-soak-watch\.txn\.[^/]+/staged-watcher $ROOT/bin/channelops-soak-watch.sh$" "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
cron_install_line="$(grep -nE 'crontab\|/.+\|lc_all=C$' "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
if [[ -z "$final_health_line" || -z "$watcher_rename_line" || -z "$cron_install_line" \
  || "$final_health_line" -ge "$watcher_rename_line" \
  || "$watcher_rename_line" -ge "$cron_install_line" ]]; then
  echo 'FAIL: atomic watcher rename and cron install must follow every VP service health check' >&2
  exit 1
fi

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-after-first-install"
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: repeated deploy_vp_app_services returned non-zero' >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/cron-after-first-install" "$FAKE_CRONTAB"; then
  echo 'FAIL: repeated watcher installation is not idempotent' >&2
  exit 1
fi

TMPDIR="$TEST_ROOT/tmp"
mkdir -p "$TMPDIR"
FAIL_SOAK_CLEANUP=true
if ! vp_install_soak_watch >"$TEST_ROOT/post-commit-cleanup.out" 2>&1; then
  echo 'FAIL: post-commit cleanup failure turned verified watcher install into failure' >&2
  exit 1
fi
FAIL_SOAK_CLEANUP=false
if ! grep -Fq 'cleanup failed' "$TEST_ROOT/post-commit-cleanup.out"; then
  echo 'FAIL: post-commit cleanup warning was not reported' >&2
  exit 1
fi
command rm -rf "$TMPDIR"/vp-soak-watch-cron.* "$ROOT/bin"/.channelops-soak-watch.txn.*

cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-precommit-cleanup"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-precommit-cleanup"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=fail-before
FAIL_SOAK_CLEANUP=true
if vp_install_soak_watch >"$TEST_ROOT/precommit-cleanup.out" 2>&1; then
  echo 'FAIL: pre-commit install failure with cleanup failure unexpectedly succeeded' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
FAIL_SOAK_CLEANUP=false
if ! cmp -s "$TEST_ROOT/target-before-precommit-cleanup" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-precommit-cleanup" "$FAKE_CRONTAB"; then
  echo 'FAIL: pre-commit cleanup failure prevented rollback' >&2
  exit 1
fi
if ! grep -Fq 'cleanup failed' "$TEST_ROOT/precommit-cleanup.out"; then
  echo 'FAIL: pre-commit cleanup failure was not reported' >&2
  exit 1
fi
command rm -rf "$TMPDIR"/vp-soak-watch-cron.* "$ROOT/bin"/.channelops-soak-watch.txn.*

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-no-crontab-test"
rm -f "$FAKE_CRONTAB"
if ! vp_install_soak_watch >/dev/null 2>&1; then
  echo 'FAIL: recognized no-crontab response did not allow first cron install' >&2
  exit 1
fi
cat >"$TEST_ROOT/expected-empty-crontab-install" <<EOF
# BEGIN VIDEOPROCESS SOAK WATCH
*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/channelops-soak-watch.log 2>&1
# END VIDEOPROCESS SOAK WATCH
EOF
if ! cmp -s "$TEST_ROOT/expected-empty-crontab-install" "$FAKE_CRONTAB"; then
  echo 'FAIL: recognized no-crontab response produced unexpected managed cron' >&2
  exit 1
fi
cp "$TEST_ROOT/cron-before-no-crontab-test" "$FAKE_CRONTAB"

printf '#!/usr/bin/env bash\nprintf "prior watcher\\n"\n' >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-read-error"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-read-error"
FAKE_CRONTAB_READ_MODE=error
if vp_install_soak_watch >"$TEST_ROOT/read-error.out" 2>&1; then
  echo 'FAIL: unrecognized crontab read error unexpectedly allowed installation' >&2
  exit 1
fi
FAKE_CRONTAB_READ_MODE=normal
if ! cmp -s "$TEST_ROOT/target-before-read-error" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-read-error" "$FAKE_CRONTAB"; then
  echo 'FAIL: crontab read error changed the prior watcher or crontab' >&2
  exit 1
fi
if ! grep -Fq 'permission denied' "$TEST_ROOT/read-error.out"; then
  echo 'FAIL: crontab read error was not reported' >&2
  exit 1
fi
install -m 0755 "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=transaction-test@example.com
7 1 * * * /usr/local/bin/prior-job
EOF
for install_mode in fail-before mutate-then-fail verify-mismatch target-verify-mismatch; do
  printf '#!/usr/bin/env bash\nprintf "prior-%s\\n"\n' "$install_mode" \
    >"$ROOT/bin/channelops-soak-watch.sh"
  chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
  cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-$install_mode"
  cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-$install_mode"
  rm -f "$FAKE_CRONTAB_FAILURE_USED"
  FAKE_CRONTAB_INSTALL_MODE="$install_mode"
  if vp_install_soak_watch >"$TEST_ROOT/$install_mode.out" 2>&1; then
    echo "FAIL: $install_mode unexpectedly allowed watcher and cron convergence" >&2
    exit 1
  fi
  FAKE_CRONTAB_INSTALL_MODE=normal
  if ! cmp -s "$TEST_ROOT/target-before-$install_mode" "$ROOT/bin/channelops-soak-watch.sh" \
    || [[ ! -x "$ROOT/bin/channelops-soak-watch.sh" ]] \
    || ! cmp -s "$TEST_ROOT/cron-before-$install_mode" "$FAKE_CRONTAB"; then
    echo "FAIL: $install_mode did not restore the prior watcher and crontab" >&2
    exit 1
  fi
done

rm -f "$FAKE_CRONTAB" "$ROOT/bin/channelops-soak-watch.sh" "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=mutate-then-fail
if vp_install_soak_watch >"$TEST_ROOT/absent-rollback.out" 2>&1; then
  echo 'FAIL: mutate-then-fail with absent prior artifacts unexpectedly succeeded' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
if [[ -e "$FAKE_CRONTAB" || -e "$ROOT/bin/channelops-soak-watch.sh" ]]; then
  echo 'FAIL: rollback did not restore absent watcher and no-crontab state' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=rollback-failure@example.com
EOF
printf '#!/usr/bin/env bash\nprintf "rollback-failure-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-rollback-failure"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=mutate-then-fail
FAKE_CRONTAB_ROLLBACK_FAIL=true
if vp_install_soak_watch >"$TEST_ROOT/rollback-failure.out" 2>&1; then
  echo 'FAIL: rollback failure unexpectedly claimed convergence' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
FAKE_CRONTAB_ROLLBACK_FAIL=false
if ! cmp -s "$TEST_ROOT/target-before-rollback-failure" "$ROOT/bin/channelops-soak-watch.sh"; then
  echo 'FAIL: cron rollback failure prevented watcher rollback' >&2
  exit 1
fi
if ! grep -Fq 'rollback failed' "$TEST_ROOT/rollback-failure.out"; then
  echo 'FAIL: rollback failure was not reported' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=signal-test@example.com
11 4 * * * /usr/local/bin/prior-signal-job
EOF
printf '#!/usr/bin/env bash\nprintf "signal-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-signal"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-signal"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
trap ':' HUP
trap ':' INT
trap ':' TERM
parent_hup_trap="$(trap -p HUP)"
parent_int_trap="$(trap -p INT)"
parent_term_trap="$(trap -p TERM)"
FAKE_CRONTAB_INSTALL_MODE=signal-term
set +e
vp_install_soak_watch >"$TEST_ROOT/signal-term.out" 2>&1
signal_status=$?
set -e
FAKE_CRONTAB_INSTALL_MODE=normal
if [[ "$signal_status" -ne 143 ]]; then
  echo "FAIL: TERM-interrupted installer returned $signal_status instead of 143" >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/target-before-signal" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-signal" "$FAKE_CRONTAB"; then
  echo 'FAIL: TERM-interrupted installer did not restore prior artifacts' >&2
  exit 1
fi
if [[ "$(trap -p HUP)" != "$parent_hup_trap" \
  || "$(trap -p INT)" != "$parent_int_trap" \
  || "$(trap -p TERM)" != "$parent_term_trap" ]]; then
  echo 'FAIL: installer signal handling clobbered a parent trap' >&2
  exit 1
fi
trap - HUP INT TERM
if ! grep -Fq 'interrupted by TERM' "$TEST_ROOT/signal-term.out"; then
  echo 'FAIL: TERM interruption was not reported' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=malformed-marker@example.com
# BEGIN VIDEOPROCESS SOAK WATCH
3 * * * * /usr/local/bin/unrelated-inside-malformed-block
EOF
printf '#!/usr/bin/env bash\nprintf "malformed-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-malformed-marker"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-malformed-marker"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
if vp_install_soak_watch >"$TEST_ROOT/malformed-marker.out" 2>&1; then
  echo 'FAIL: malformed managed markers unexpectedly allowed convergence' >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/target-before-malformed-marker" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-malformed-marker" "$FAKE_CRONTAB"; then
  echo 'FAIL: malformed managed markers changed the watcher or crontab' >&2
  exit 1
fi

cp "$TEST_ROOT/cron-before-no-crontab-test" "$FAKE_CRONTAB"
install -m 0755 "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"
if [[ -n "$(find "$ROOT/bin" -maxdepth 1 -name '.channelops-soak-watch.txn.*' -print -quit)" ]]; then
  echo 'FAIL: transaction failure leaked a watcher staging directory' >&2
  exit 1
fi

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-skips"
cron_writes_before="$(wc -l <"$FAKE_CRONTAB_CALLS" | tr -d ' ')"
UPDATE_SERVICES=0
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: UPDATE_SERVICES=0 deployment returned non-zero' >&2
  exit 1
fi
UPDATE_SERVICES=1
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: UPDATE_SERVICES=0 rewrote the crontab' >&2
  exit 1
fi

FAIL_RUNNING_SERVICE=vp-api-swarm
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: failed service convergence unexpectedly succeeded' >&2
  exit 1
fi
FAIL_RUNNING_SERVICE=
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: failed service convergence rewrote the crontab' >&2
  exit 1
fi

printf 'if then\n' >"$TEST_ROOT/invalid-channelops-soak-watch.sh"
VP_SOAK_WATCH_SOURCE="$TEST_ROOT/invalid-channelops-soak-watch.sh"
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: invalid watcher syntax unexpectedly allowed deployment' >&2
  exit 1
fi
VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: invalid watcher syntax rewrote the crontab' >&2
  exit 1
fi

TMPDIR="$TEST_ROOT/tmp"
mkdir -p "$TMPDIR"
FAIL_MANAGED_CRON_PRINTF=true
if vp_install_soak_watch >/dev/null 2>&1; then
  echo 'FAIL: managed cron rendering failure unexpectedly succeeded' >&2
  exit 1
fi
FAIL_MANAGED_CRON_PRINTF=false
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: managed cron rendering failure rewrote the crontab' >&2
  exit 1
fi
if [[ -n "$(find "$TMPDIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo 'FAIL: managed cron rendering failure leaked a temporary path' >&2
  exit 1
fi
cron_writes_after="$(wc -l <"$FAKE_CRONTAB_CALLS" | tr -d ' ')"
if [[ "$cron_writes_after" -ne "$cron_writes_before" ]]; then
  echo 'FAIL: skipped or failed deployment called crontab with an install file' >&2
  exit 1
fi

grep -Fq 'build|10.0.0.127|/Users/wenjieliu/VideoProcess-app|backend/Dockerfile.ffmpeg-worker-go|vp-ffmpeg-worker-go:deploy-0123456789ab' "$CALLS"
grep -Fq 'docker|build -f /home/taiwei/deploy-github-sync/repos/videoprocess/backend/Dockerfile.worker -t vp-ffmpeg-worker-python:deploy-0123456789ab /home/taiwei/deploy-github-sync/repos/videoprocess/backend' "$CALLS"
if grep -Fq 'build|10.0.0.150|' "$CALLS"; then
  echo 'FAIL: manager-local images must use the manager Docker CLI directly' >&2
  exit 1
fi
grep -Fq 'node.labels.vp.runtime==true' "$CALLS"
grep -Fq 'node.labels.vp.gpu==true' "$CALLS"
grep -Fq 'health|vp-api|http://10.0.0.127:18080/health' "$CALLS"
grep -Fq 'health|vp-frontend|http://10.0.0.127:3001/' "$CALLS"
grep -Fq 'vp-autoflow-api-swarm' "$CALLS"
grep -Fq 'vp-ffmpeg-worker-go-swarm' "$CALLS"
grep -Fq 'vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq 'vp-vision-worker-swarm' "$CALLS"
if ! grep -Fq 'vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: deployment must include the dedicated YouTube publisher' >&2
  exit 1
fi
grep -Fq -- '--constraint-rm node.labels.role==app' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==colima-127' "$CALLS"
grep -Fq -- '--env-rm DATABASE_URL' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_ENABLED=true' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_JOB_WRITES=true' "$CALLS"
grep -Fq -- '--env-add VP_PYTHON_SCHEDULE_URL=http://vp-autoflow-api-swarm:8080' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=colima-127' "$CALLS"
if [[ "$VP_APP_SERVICES" != 'vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm vp-ffmpeg-worker-gpu-swarm vp-vision-worker-swarm vp-youtube-publisher-swarm' ]]; then
  echo 'FAIL: discovery deployment must not add a VP service' >&2
  exit 1
fi
grep -Fq -- '--env-add WORKER_TYPE=vision' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=150-vision' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--env-rm HF_TOKEN' "$CALLS"
grep -Fq -- '--env-rm PUBLIC_PUBLISH_ENABLED' "$CALLS"
grep -Fq -- '--env-rm YOUTUBE_MANAGER_URL' "$CALLS"
grep -Fq -- '--secret-rm vision-legacy-secret' "$CALLS"
grep -Fq -- '--config-rm vision-legacy-config' "$CALLS"
grep -Fq -- '--network-rm legacy-network-id' "$CALLS"
grep -Fq -- '--mount-add type=volume,src=vp-vision-worker-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--env-rm YOUTUBE_CREDENTIALS_DIR' "$CALLS"
grep -Fq 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS"
grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--env-add WORKER_TYPE=youtube_publisher' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=150-publisher' "$CALLS"
grep -Fq -- '--env-add YOUTUBE_MANAGER_URL=http://10.0.0.150:18999' "$CALLS"
grep -Fq -- '--env-add YOUTUBE_PUBLISH_ENABLED=true' "$CALLS"
grep -Fq -- '--env-add PUBLIC_PUBLISH_ENABLED=false' "$CALLS"
grep -Fq -- '--env-add WORKER_CONCURRENCY=1' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
grep -Fq -- '--mount-rm /data/storage' "$CALLS"
grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
publisher_mount_remove_line="$(grep -nF 'docker|service update' "$CALLS" \
  | grep -F 'vp-youtube-publisher-swarm' \
  | grep -F -- '--mount-rm /data/storage' \
  | head -n 1 \
  | cut -d: -f1)"
publisher_mount_add_line="$(grep -nF 'docker|service update' "$CALLS" \
  | grep -F 'vp-youtube-publisher-swarm' \
  | grep -F -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' \
  | head -n 1 \
  | cut -d: -f1)"
if [[ -z "$publisher_mount_remove_line" || -z "$publisher_mount_add_line" \
  || "$publisher_mount_remove_line" -ge "$publisher_mount_add_line" ]]; then
  echo 'FAIL: publisher scratch replacement must remove and add the target in separate ordered updates' >&2
  exit 1
fi
publisher_mount_remove_call="$(sed -n "${publisher_mount_remove_line}p" "$CALLS")"
if ! grep -Fq -- '--replicas 0' <<<"$publisher_mount_remove_call" \
  || grep -Fq -- '--mount-add ' <<<"$publisher_mount_remove_call" \
  || grep -Fq -- '--image ' <<<"$publisher_mount_remove_call"; then
  echo 'FAIL: publisher scratch removal must first stop the publisher without applying the release' >&2
  exit 1
fi
publisher_mount_add_call="$(sed -n "${publisher_mount_add_line}p" "$CALLS")"
if grep -Fq -- '--mount-rm /data/storage' <<<"$publisher_mount_add_call" \
  || ! grep -Fq -- '--replicas 1' <<<"$publisher_mount_add_call" \
  || ! grep -Fq -- '--image ' <<<"$publisher_mount_add_call"; then
  echo 'FAIL: publisher release update must add scratch without removing the same target again' >&2
  exit 1
fi
grep -Fq -- '--mount-rm /app/cache' "$CALLS"
grep -Fq -- '--mount-rm /APP/OAUTH' "$CALLS"
grep -Fq -- '--secret-rm publisher-credential-reference' "$CALLS"
grep -Fq -- '--config-rm publisher-config-reference' "$CALLS"
for publisher_env in \
  YOUTUBE_CREDENTIALS_DIR \
  YOUTUBE_CREDENTIALS_JSON \
  YOUTUBE_LEGACY_MODE \
  GOOGLE_CLIENT_SECRETS_FILE \
  YOUTUBE_REFRESH_TOKEN; do
  grep -Fq -- "--env-rm $publisher_env" "$CALLS"
  if grep -Fq -- "--env-add $publisher_env=" "$CALLS"; then
    echo "FAIL: publisher deploy re-added removed credential environment $publisher_env" >&2
    exit 1
  fi
done
grep -Fq -- '--env-add MINIO_SECRET_KEY=' "$CALLS"

publisher_calls="$(grep -F 'vp-youtube-publisher-swarm' "$CALLS" || true)"
if printf '%s\n' "$publisher_calls" | grep -Eq -- 'YOUTUBE_(OAUTH|CLIENT|CREDENTIALS|TOKEN|REFRESH)_[A-Z_]*='; then
  echo 'FAIL: publisher deploy must not add OAuth credential environments' >&2
  exit 1
fi
if printf '%s\n' "$publisher_calls" | grep -Eq -- '--mount(-add)? .*youtube_credentials'; then
  echo 'FAIL: publisher deploy must not add a credentials mount' >&2
  exit 1
fi

publisher_health_line="$(grep -nF 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS" | head -n 1 | cut -d: -f1)"
publisher_update_line="$(grep -nF 'vp-youtube-publisher-swarm' "$CALLS" | grep -F 'docker|service update' | head -n 1 | cut -d: -f1)"
if [[ -z "$publisher_health_line" || -z "$publisher_update_line" || "$publisher_health_line" -ge "$publisher_update_line" ]]; then
  echo 'FAIL: publisher manager auth health must precede publisher updates' >&2
  exit 1
fi

if (
  unset VP_API_DATABASE_URL_GO VP_PYTHON_WORKER_DATABASE_URL \
    VP_MINIO_ACCESS_KEY VP_MINIO_SECRET_KEY
  deploy_vp_app_services $images >/dev/null 2>&1
); then
  echo 'FAIL: production deployment must fail closed when secrets are missing' >&2
  exit 1
fi

CONSTRAINT_MODE=runtime
if ! vp_update_runtime_service vp-frontend-swarm vp-frontend:repeat-test start-first \
  >/dev/null 2>>"$CALLS"; then
  echo 'FAIL: repeat runtime update returned non-zero' >&2
  exit 1
fi
if grep -Fq 'unbound variable' "$CALLS"; then
  echo 'FAIL: repeat runtime update is not compatible with Bash 3.2 set -u' >&2
  exit 1
fi
grep -Fq 'docker|service update --detach=false --no-resolve-image --update-order start-first --image vp-frontend:repeat-test vp-frontend-swarm' "$CALLS"
CONSTRAINT_MODE=legacy

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
if ! vp_deploy_publisher vp-ffmpeg-worker-python:publisher-repeat-test >/dev/null 2>>"$CALLS"; then
  echo 'FAIL: repeat publisher update returned non-zero' >&2
  exit 1
fi
if grep -Fq 'unbound variable' "$CALLS"; then
  echo 'FAIL: repeat publisher update is not compatible with Bash 3.2 set -u' >&2
  exit 1
fi
grep -Fq 'docker|service update --detach=false --no-resolve-image --update-order stop-first' "$CALLS"
grep -Fq -- '--image vp-ffmpeg-worker-python:publisher-repeat-test vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
if grep -Fq -- '--constraint-add node.labels.vp.publisher==true' "$CALLS" \
  || grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS" \
  || grep -Fq -- '--network-add vp-pipeline-net' "$CALLS" \
  || grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS" \
  || grep -Fq -- '--mount-rm ' "$CALLS"; then
  echo 'FAIL: repeat publisher update must not change the exact desired mount set' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_NETWORK_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials

: >"$CALLS"
FAIL_HEALTH_CHECK=vp-youtube-manager
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-health-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must stop when manager auth health fails' >&2
  exit 1
fi
grep -Fq 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS"
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: publisher deploy mutated Swarm after manager auth health failed' >&2
  exit 1
fi
FAIL_HEALTH_CHECK=

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_LIST_FAILURE=true
if deploy_vp_app_services \
  vp-api:publisher-list-daemon-test \
  vp-frontend:publisher-list-daemon-test \
  vp-backend-api:publisher-list-daemon-test \
  vp-channelops-runner-go:publisher-list-daemon-test \
  vp-ffmpeg-worker-go:publisher-list-daemon-test \
  vp-ffmpeg-worker-python:publisher-list-daemon-test >/dev/null 2>&1; then
  echo 'FAIL: publisher list daemon failure unexpectedly allowed deployment' >&2
  exit 1
fi
if ! grep -Fq 'docker|service ls --filter name=vp-youtube-publisher-swarm --format {{.Name}}' "$CALLS"; then
  echo 'FAIL: optional publisher snapshot must use an exact service list probe' >&2
  exit 1
fi
if grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS" \
  || grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: publisher list daemon failure must not omit, create, or delete the existing publisher' >&2
  exit 1
fi
PUBLISHER_LIST_FAILURE=false

: >"$CALLS"
PUBLISHER_LIST_NAME=vp-youtube-publisher-swarm-stale
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-list-name-test >/dev/null 2>&1; then
  echo 'FAIL: non-exact publisher list result unexpectedly selected a service state' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: non-exact publisher list result must not update or create a publisher' >&2
  exit 1
fi
PUBLISHER_LIST_NAME=

: >"$CALLS"
FAIL_PUBLISHER_INSPECT_FORMAT=ContainerSpec.Configs
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-config-inspect-test >/dev/null 2>&1; then
  echo 'FAIL: publisher config inspection failure unexpectedly allowed deployment' >&2
  exit 1
fi
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after config inspection failure' >&2
  exit 1
fi
FAIL_PUBLISHER_INSPECT_FORMAT=

: >"$CALLS"
GPU_SERVICE_EXISTS=false
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_LIST_FAILURE=true
if vp_restore_app_snapshots "" >/dev/null 2>&1; then
  echo 'FAIL: publisher rollback removal accepted a list daemon failure' >&2
  exit 1
fi
if grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: publisher rollback removal deleted a publisher after list daemon failure' >&2
  exit 1
fi
PUBLISHER_LIST_FAILURE=false
GPU_SERVICE_EXISTS=true

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
FAIL_NODE_UPDATE=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-node-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when manager label update fails' >&2
  exit 1
fi
grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS"
if grep -Fq 'docker|service update' "$CALLS" || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after manager label update failure' >&2
  exit 1
fi
FAIL_NODE_UPDATE=false

: >"$CALLS"
FAIL_NETWORK_INSPECT=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-network-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when network inspection fails' >&2
  exit 1
fi
grep -Fq 'docker|network inspect vp-pipeline-net --format {{.ID}}' "$CALLS"
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after pipeline network inspection failure' >&2
  exit 1
fi
FAIL_NETWORK_INSPECT=false

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
FAIL_PUBLISHER_CREATE=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-create-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when service creation fails' >&2
  exit 1
fi
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
FAIL_PUBLISHER_CREATE=false

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_REPLICAS=3
FAIL_UPDATE_SERVICE=vp-youtube-publisher-swarm
FAIL_UPDATE_IMAGE=vp-ffmpeg-worker-python:publisher-update-failure-test
if deploy_vp_app_services \
  vp-api:publisher-update-failure-test \
  vp-frontend:publisher-update-failure-test \
  vp-backend-api:publisher-update-failure-test \
  vp-channelops-runner-go:publisher-update-failure-test \
  vp-ffmpeg-worker-go:publisher-update-failure-test \
  vp-ffmpeg-worker-python:publisher-update-failure-test >/dev/null 2>&1; then
  echo 'FAIL: failed publisher update with an old running service unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-youtube-publisher-swarm:stable vp-youtube-publisher-swarm' "$CALLS"
if [[ "$PUBLISHER_MOUNT_MODE" != desired || "$PUBLISHER_REPLICAS" -ne 1 ]]; then
  echo 'FAIL: publisher rollback did not recover one replica with the desired scratch mount' >&2
  exit 1
fi
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

VP_GPU_RUNTIME_READY=true
GPU_PREFLIGHT_SUCCEEDS=false
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-preflight-test >/dev/null 2>&1; then
  echo 'FAIL: requested GPU mode must fail when the runtime preflight fails' >&2
  exit 1
fi
grep -Fq 'docker|run --rm --gpus all vp-ffmpeg-worker-python:gpu-preflight-test nvidia-smi' "$CALLS"
GPU_PREFLIGHT_SUCCEEDS=true
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-swarm-allocation-test \
  >/dev/null 2>&1; then
  echo 'FAIL: GPU mode must remain disabled until Swarm task allocation is configured' >&2
  exit 1
fi
VP_GPU_RUNTIME_READY=false

: >"$CALLS"
GPU_SERVICE_EXISTS=true
FAIL_UPDATE_SERVICE=vp-channel-agent-runner-swarm
FAIL_UPDATE_IMAGE=vp-channelops-runner-go:rollback-test
if deploy_vp_app_services \
  vp-api:rollback-test \
  vp-frontend:rollback-test \
  vp-backend-api:rollback-test \
  vp-channelops-runner-go:rollback-test \
  vp-ffmpeg-worker-go:rollback-test \
  vp-ffmpeg-worker-python:rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected service update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-api-swarm:stable vp-api-swarm' "$CALLS"
grep -Fq -- '--image baseline-vp-channel-agent-runner-swarm:stable vp-channel-agent-runner-swarm' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
runner_rollback_call="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image baseline-vp-channel-agent-runner-swarm:stable' \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | head -1
)"
if [[ "$runner_rollback_call" != *'--update-order stop-first'* \
  || "$runner_rollback_call" != *'--env-add CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1'* \
  || "$runner_rollback_call" != *'--health-cmd wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1'* \
  || "$runner_rollback_call" != *'--health-interval 10s'* \
  || "$runner_rollback_call" != *'--health-timeout 3s'* \
  || "$runner_rollback_call" != *'--health-retries 6'* \
  || "$runner_rollback_call" != *'--health-start-period 10s'* ]]; then
  echo 'FAIL: runner rollback must retain stop-first, exact identity, and exact readyz health' >&2
  exit 1
fi
if grep -Fq 'docker|service rollback' "$CALLS"; then
  echo 'FAIL: VP rollback must not restore the legacy service specification' >&2
  exit 1
fi
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: rollback must not target host 126' >&2
  exit 1
fi

: >"$CALLS"
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
GPU_SERVICE_EXISTS=false
FAIL_RUNNING_SERVICE=vp-ffmpeg-worker-gpu-swarm
if deploy_vp_app_services \
  vp-api:create-rollback-test \
  vp-frontend:create-rollback-test \
  vp-backend-api:create-rollback-test \
  vp-channelops-runner-go:create-rollback-test \
  vp-ffmpeg-worker-go:create-rollback-test \
  vp-ffmpeg-worker-python:create-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected new-worker health failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'docker|service rm vp-ffmpeg-worker-gpu-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
FAIL_RUNNING_SERVICE=vp-youtube-publisher-swarm
if deploy_vp_app_services \
  vp-api:publisher-rollback-test \
  vp-frontend:publisher-rollback-test \
  vp-backend-api:publisher-rollback-test \
  vp-channelops-runner-go:publisher-rollback-test \
  vp-ffmpeg-worker-go:publisher-rollback-test \
  vp-ffmpeg-worker-python:publisher-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: existing publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-youtube-publisher-swarm:stable vp-youtube-publisher-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=false
FAIL_RUNNING_SERVICE=vp-youtube-publisher-swarm
if deploy_vp_app_services \
  vp-api:publisher-create-rollback-test \
  vp-frontend:publisher-create-rollback-test \
  vp-backend-api:publisher-create-rollback-test \
  vp-channelops-runner-go:publisher-create-rollback-test \
  vp-ffmpeg-worker-go:publisher-create-rollback-test \
  vp-ffmpeg-worker-python:publisher-create-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: new publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=stale
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-placement-test >/dev/null
grep -Fq -- '--constraint-rm node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--constraint-rm node.labels.vp.gpu==true' "$CALLS"
grep -Fq -- '--constraint-rm node.hostname==colima-swarmbridged' "$CALLS"
grep -Fq -- '--constraint-rm node.labels.vp.legacy==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: publisher deployment must never target 126' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy

: >"$CALLS"
CONSTRAINT_MODE=stale-runtime
vp_update_runtime_service vp-api-swarm vp-api:runtime-placement-test stop-first \
  >/dev/null
grep -Fq -- '--constraint-rm node.hostname==CASPERs-Mac-mini' "$CALLS"
grep -Fq -- '--constraint-rm node.labels.vp.legacy==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==colima-127' "$CALLS"
CONSTRAINT_MODE=legacy

: >"$CALLS"
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_MOUNT_MODE=missing
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-missing-scratch-test >/dev/null
grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
if grep -Fq -- '--mount-rm /data/storage' "$CALLS"; then
  echo 'FAIL: publisher deploy removed an absent scratch target' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials

remote_script_output="$(
  PDS_REMOTE_DOCKER_MODE=healthy \
    /bin/sh "$PDS_REMOTE_SCRIPT" vp-pds-swarm
)"
if [[ "$remote_script_output" \
  != "vp-pds:test|:8080|healthy|$PDS_EXPECTED_TEST|10s|3s|10s|6" ]]; then
  echo 'FAIL: real PDS remote script did not produce the healthy snapshot contract' >&2
  exit 1
fi
for pds_remote_pending_mode in missing duplicate inspect-error; do
  remote_script_output="$(
    PDS_REMOTE_DOCKER_MODE="$pds_remote_pending_mode" \
      /bin/sh "$PDS_REMOTE_SCRIPT" vp-pds-swarm
  )"
  if [[ "$remote_script_output" != "pending|container_set" ]]; then
    echo "FAIL: real PDS remote script did not sanitize $pds_remote_pending_mode as pending" >&2
    exit 1
  fi
done
for pds_remote_failure_mode in list-error bad-id env-missing env-duplicate wrong-http; do
  if remote_script_output="$(
    PDS_REMOTE_DOCKER_MODE="$pds_remote_failure_mode" \
      /bin/sh "$PDS_REMOTE_SCRIPT" vp-pds-swarm 2>/dev/null
  )"; then
    echo "FAIL: real PDS remote script accepted $pds_remote_failure_mode" >&2
    exit 1
  fi
  if [[ -n "$remote_script_output" ]]; then
    echo "FAIL: real PDS remote script exposed output for $pds_remote_failure_mode" >&2
    exit 1
  fi
done
remote_script_output="$(
  PDS_REMOTE_DOCKER_MODE=no-health \
    /bin/sh "$PDS_REMOTE_SCRIPT" vp-pds-swarm
)"
if [[ "$remote_script_output" != "vp-pds:test|:8080|none|none|0s|0s|0s|0" ]]; then
  echo 'FAIL: real PDS remote script did not sanitize an absent health config' >&2
  exit 1
fi

: >"$CALLS"
CONSTRAINT_MODE=pds-stale
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:9099
PDS_READINESS_MODE=starting-then-healthy
rm -f "$PDS_READINESS_CALLS_FILE"
if ! pds_output="$(deploy_pds_services vp-pds:health-gated-test)"; then
  echo 'FAIL: healthy PDS deployment did not converge' >&2
  exit 1
fi
if [[ "$pds_output" != "vp-pds-swarm" ]]; then
  echo 'FAIL: PDS deployment stdout must contain only the service name' >&2
  exit 1
fi
pds_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-pds:health-gated-test vp-pds-swarm' \
    | head -n 1
)"
for expected_pds_update_arg in \
  '--constraint-rm node.hostname==CASPERs-Mac-mini' \
  '--constraint-rm node.labels.vp.legacy==true' \
  '--constraint-add node.labels.vp.runtime==true' \
  '--constraint-add node.hostname==colima-127' \
  '--env-rm PDS_HTTP_ADDR' \
  '--env-add PDS_HTTP_ADDR=:8080' \
  '--health-cmd  --health-interval 10s' \
  '--health-timeout 3s' \
  '--health-retries 6' \
  '--health-start-period 10s'; do
  if [[ "$pds_update" != *"$expected_pds_update_arg"* ]]; then
    echo "FAIL: PDS update is missing $expected_pds_update_arg" >&2
    exit 1
  fi
done
grep -Fq 'docker|service ps vp-pds-swarm --filter desired-state=running --format {{.Node}}|{{.CurrentState}}' "$CALLS"
if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 3 \
  || "$(grep -Fc 'sleep|5' "$CALLS")" -ne 2 ]]; then
  echo 'FAIL: PDS starting state did not use bounded readiness retries' >&2
  exit 1
fi
pds_node_line="$(grep -nF 'docker|service ps vp-pds-swarm' "$CALLS" | head -n 1 | cut -d: -f1)"
pds_remote_line="$(grep -nF 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS" | head -n 1 | cut -d: -f1)"
if [[ -z "$pds_node_line" || -z "$pds_remote_line" || "$pds_node_line" -ge "$pds_remote_line" ]]; then
  echo 'FAIL: PDS node verification must precede remote container health' >&2
  exit 1
fi
if grep -Eq '10\.0\.0\.126|CASPERs-Mac-mini|colima-swarmbridged|test-access|test-secret|sentinel-secret|aaaaaaaaaaaaaaaa' \
  <<<"$(grep -F 'remote|' "$CALLS")"; then
  echo 'FAIL: PDS remote readiness exposed a forbidden host, container ID, or secret' >&2
  exit 1
fi
CONSTRAINT_MODE=legacy

for pds_failure_mode in \
  wrong-image wrong-http-addr wrong-command wrong-timing unhealthy remote-error malformed; do
  : >"$CALLS"
  PDS_CURRENT_IMAGE=vp-pds:readiness-failure-test
  PDS_CURRENT_HTTP_ADDR=:8080
  PDS_READINESS_MODE="$pds_failure_mode"
  rm -f "$PDS_READINESS_CALLS_FILE"
  if vp_require_pds_ready "$PDS_CURRENT_IMAGE" >/dev/null 2>&1; then
    echo "FAIL: PDS readiness accepted mode $pds_failure_mode" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 1 ]]; then
    echo "FAIL: terminal PDS readiness mode $pds_failure_mode was retried" >&2
    exit 1
  fi
done

: >"$CALLS"
PDS_CURRENT_IMAGE=vp-pds:remote-error-test
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=remote-error
rm -f "$PDS_READINESS_CALLS_FILE"
if vp_require_pds_ready "$PDS_CURRENT_IMAGE" \
  >/dev/null 2>"$TEST_ROOT/pds-remote-error.err"; then
  echo 'FAIL: PDS remote inspection error unexpectedly passed' >&2
  exit 1
fi
if [[ "$(<"$TEST_ROOT/pds-remote-error.err")" \
  != "PDS container inspection failed: vp-pds-swarm" ]]; then
  echo 'FAIL: PDS remote inspection error was not service-only' >&2
  exit 1
fi
if grep -Eq 'sentinel-secret|aaaaaaaaaaaaaaaa|tcp://' "$TEST_ROOT/pds-remote-error.err"; then
  echo 'FAIL: PDS remote inspection error exposed remote details' >&2
  exit 1
fi

: >"$CALLS"
PDS_CURRENT_IMAGE=vp-pds:container-transition-test
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=container-set-then-healthy
rm -f "$PDS_READINESS_CALLS_FILE"
if ! vp_require_pds_ready "$PDS_CURRENT_IMAGE" >/dev/null; then
  echo 'FAIL: transient PDS container-set transition did not converge' >&2
  exit 1
fi
if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 3 \
  || "$(grep -Fc 'sleep|5' "$CALLS")" -ne 2 ]]; then
  echo 'FAIL: transient PDS container-set transition was not retried safely' >&2
  exit 1
fi

for pds_container_set_mode in missing duplicate; do
  : >"$CALLS"
  PDS_CURRENT_IMAGE=vp-pds:container-set-timeout-test
  PDS_CURRENT_HTTP_ADDR=:8080
  PDS_READINESS_MODE="$pds_container_set_mode"
  rm -f "$PDS_READINESS_CALLS_FILE"
  if vp_require_pds_ready "$PDS_CURRENT_IMAGE" >/dev/null 2>&1; then
    echo "FAIL: persistent PDS $pds_container_set_mode container set passed readiness" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 18 \
    || "$(grep -Fc 'sleep|5' "$CALLS")" -ne 17 ]]; then
    echo "FAIL: persistent PDS $pds_container_set_mode container set was not bounded" >&2
    exit 1
  fi
done

: >"$CALLS"
PDS_CURRENT_IMAGE=vp-pds:starting-timeout-test
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=always-starting
rm -f "$PDS_READINESS_CALLS_FILE"
if vp_require_pds_ready "$PDS_CURRENT_IMAGE" >/dev/null 2>&1; then
  echo 'FAIL: indefinitely starting PDS container passed readiness' >&2
  exit 1
fi
if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 18 \
  || "$(grep -Fc 'sleep|5' "$CALLS")" -ne 17 ]]; then
  echo 'FAIL: PDS readiness retry deadline is not exactly bounded' >&2
  exit 1
fi

: >"$CALLS"
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=new-unhealthy-rollback-healthy
rm -f "$PDS_READINESS_CALLS_FILE"
if deploy_pds_services vp-pds:rollback-test >/dev/null 2>&1; then
  echo 'FAIL: unhealthy PDS update unexpectedly succeeded after rollback' >&2
  exit 1
fi
candidate_pds_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--update-order start-first' \
    | grep -F -- '--image vp-pds:rollback-test vp-pds-swarm' \
    | head -n 1
)"
rollback_pds_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--update-order stop-first' \
    | grep -F -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' \
    | head -n 1
)"
if [[ "$candidate_pds_update" != *'--health-cmd  --health-interval 10s'* \
  || "$rollback_pds_update" != *'--health-cmd  --health-interval 10s'* ]]; then
  echo 'FAIL: PDS candidate and rollback must retain the health contract' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' "$CALLS"
if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 2 ]]; then
  echo 'FAIL: PDS rollback did not verify both candidate and baseline readiness' >&2
  exit 1
fi

: >"$CALLS"
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=new-unhealthy-rollback-no-health
rm -f "$PDS_READINESS_CALLS_FILE"
if deploy_pds_services vp-pds:rollback-readiness-failure-test >/dev/null 2>&1; then
  echo 'FAIL: PDS rollback readiness failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' "$CALLS"

: >"$CALLS"
FAIL_UPDATE_SERVICE=vp-pds-swarm
FAIL_UPDATE_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=unhealthy
rm -f "$PDS_READINESS_CALLS_FILE"
if deploy_pds_services vp-pds:rollback-update-failure-test >/dev/null 2>&1; then
  echo 'FAIL: PDS rollback update failure unexpectedly succeeded' >&2
  exit 1
fi
if [[ "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 1 ]]; then
  echo 'FAIL: PDS readiness ran after rollback update failed' >&2
  exit 1
fi
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

: >"$CALLS"
PDS_TASK_NODE=CASPERs-Mac-mini
PDS_CURRENT_IMAGE=vp-pds:forbidden-node-test
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=healthy
rm -f "$PDS_READINESS_CALLS_FILE"
if vp_require_pds_ready "$PDS_CURRENT_IMAGE" >/dev/null 2>&1; then
  echo 'FAIL: PDS task on host 126 passed readiness' >&2
  exit 1
fi
if grep -Fq 'remote|' "$CALLS"; then
  echo 'FAIL: PDS readiness contacted 127 after node verification failed' >&2
  exit 1
fi
PDS_TASK_NODE=colima-127

: >"$CALLS"
FAIL_UPDATE_SERVICE=vp-pds-swarm
FAIL_UPDATE_IMAGE=vp-pds:update-failure-test
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=healthy
if deploy_pds_services vp-pds:update-failure-test >/dev/null 2>&1; then
  echo 'FAIL: injected PDS update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

: >"$CALLS"
PDS_CONSTRAINT_INSPECT_MODE=fail-first
rm -f "$PDS_CONSTRAINT_INSPECT_CALLS_FILE"
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=healthy
if deploy_pds_services vp-pds:constraint-inspect-failure-test >/dev/null 2>&1; then
  echo 'FAIL: PDS candidate constraint inspect failure unexpectedly succeeded' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: PDS candidate constraint inspect failure mutated Swarm' >&2
  exit 1
fi
PDS_CONSTRAINT_INSPECT_MODE=normal

: >"$CALLS"
PDS_CONSTRAINT_INSPECT_MODE=fail-second
rm -f "$PDS_CONSTRAINT_INSPECT_CALLS_FILE"
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_CURRENT_HTTP_ADDR=:8080
PDS_READINESS_MODE=unhealthy
if deploy_pds_services vp-pds:rollback-constraint-inspect-failure-test \
  >/dev/null 2>&1; then
  echo 'FAIL: PDS rollback constraint inspect failure unexpectedly succeeded' >&2
  exit 1
fi
if [[ "$(grep -Fc 'docker|service update' "$CALLS")" -ne 1 \
  || "$(grep -Fc 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS")" -ne 1 ]]; then
  echo 'FAIL: PDS rollback constraint inspect failure wrote after the failed inspect' >&2
  exit 1
fi
PDS_CONSTRAINT_INSPECT_MODE=normal
rm -f "$PDS_CONSTRAINT_INSPECT_CALLS_FILE"

: >"$CALLS"
RUNTIME_CONSTRAINT_INSPECT_SERVICE=vp-feature-aggregator-swarm
RUNTIME_CONSTRAINT_INSPECT_MODE=fail-first
rm -f "$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE"
if deploy_feature_aggregator_services vp-feature-aggregator:preflight-failure-test \
  >/dev/null 2>&1; then
  echo 'FAIL: feature aggregator preflight failure unexpectedly succeeded' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: feature aggregator preflight failure mutated Swarm' >&2
  exit 1
fi
RUNTIME_CONSTRAINT_INSPECT_SERVICE=
RUNTIME_CONSTRAINT_INSPECT_MODE=normal

: >"$CALLS"
FAIL_UPDATE_SERVICE=vp-feature-aggregator-swarm
FAIL_UPDATE_IMAGE=vp-feature-aggregator:docker-exit-two-test
FAIL_UPDATE_EXIT=2
if deploy_feature_aggregator_services "$FAIL_UPDATE_IMAGE" >/dev/null 2>&1; then
  echo 'FAIL: feature aggregator Docker exit 2 unexpectedly succeeded' >&2
  exit 1
fi
if [[ "$(grep -Fc 'docker|service update' "$CALLS")" -ne 2 ]]; then
  echo 'FAIL: feature aggregator Docker exit 2 did not trigger one rollback' >&2
  exit 1
fi
candidate_feature_update_line="$(
  grep -nF -- "--image $FAIL_UPDATE_IMAGE vp-feature-aggregator-swarm" "$CALLS" \
    | cut -d: -f1
)"
rollback_feature_update_line="$(
  grep -nF -- \
    '--image baseline-vp-feature-aggregator-swarm:stable vp-feature-aggregator-swarm' \
    "$CALLS" \
    | cut -d: -f1
)"
if [[ -z "$candidate_feature_update_line" \
  || -z "$rollback_feature_update_line" \
  || "$candidate_feature_update_line" -ge "$rollback_feature_update_line" ]]; then
  echo 'FAIL: feature aggregator rollback did not follow the failed candidate' >&2
  exit 1
fi
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
FAIL_UPDATE_EXIT=1

: >"$CALLS"
RUNTIME_CONSTRAINT_INSPECT_SERVICE=vp-api-swarm
RUNTIME_CONSTRAINT_INSPECT_MODE=fail-first
rm -f "$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE"
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: app preflight failure unexpectedly succeeded' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: app preflight failure mutated Swarm' >&2
  exit 1
fi
RUNTIME_CONSTRAINT_INSPECT_SERVICE=
RUNTIME_CONSTRAINT_INSPECT_MODE=normal
rm -f "$RUNTIME_CONSTRAINT_INSPECT_CALLS_FILE"

GPU_SERVICE_EXISTS=false
vp_deploy_python_worker vp-ffmpeg-worker-python:deploy-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi

: >"$CALLS"
VISION_SERVICE_EXISTS=false
vp_deploy_vision_worker vp-ffmpeg-worker-python:vision-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-vision-worker-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-net' "$CALLS"
grep -Fq -- '--mount type=volume,src=vp-vision-worker-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--env WORKER_TYPE=vision' "$CALLS"
grep -Fq -- '--env WORKER_HOST=150-vision' "$CALLS"

: >"$CALLS"
VISION_SERVICE_EXISTS=true
VISION_TASK_NODE=CASPERs-Mac-mini
if vp_deploy_vision_worker vp-ffmpeg-worker-python:vision-forbidden-node-test \
  >/dev/null 2>&1; then
  echo 'FAIL: vision worker placement on 126 unexpectedly passed verification' >&2
  exit 1
fi
grep -Fq 'docker|service ps vp-vision-worker-swarm' "$CALLS"
VISION_TASK_NODE=ccttww-lap

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-net' "$CALLS"
grep -Fq -- '--mount type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"

: >"$CALLS"
vp_update_runtime_service vp-channel-agent-runner-swarm vp-channelops-runner-go:discovery-timeout-test start-first >/dev/null
timeout_calls="$(grep -F -- 'CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120' "$CALLS" || true)"
if [[ "$(printf '%s\n' "$timeout_calls" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1 \
  || "$timeout_calls" != *"vp-channel-agent-runner-swarm"* ]]; then
  echo 'FAIL: discovery timeout must be added exactly once to the Go runner' >&2
  exit 1
fi
if printf '%s\n' "$timeout_calls" | grep -Eq 'vp-(youtube-publisher|ffmpeg-worker|api|frontend|autoflow-api|event-outbox-relay)-swarm'; then
  echo 'FAIL: discovery timeout must not be added to another VP service' >&2
  exit 1
fi
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: discovery timeout deployment must never target 126' >&2
  exit 1
fi

channelops_runner_argument_pair_count() {
  local update_call="$1"
  local option="$2"
  local value="$3"
  printf '%s\n' "$update_call" | awk -v option="$option" -v value="$value" '
    {
      for (field = 1; field < NF; field++) {
        if ($field == option && $(field + 1) == value) count++
      }
    }
    END { print count + 0 }
  '
}

assert_channelops_runner_identity_reconciliation() {
  local state="$1"
  local expected_removals="$2"
  local update_call
  local update_count
  local managed_additions
  local removals

  printf '%s\n' "$state" >"$CHANNEL_RUNNER_ENV_STATE_FILE"
  : >"$CALLS"
  vp_update_runtime_service \
    vp-channel-agent-runner-swarm "vp-channelops-runner-go:${state}-test" stop-first >/dev/null

  update_count="$(grep -F 'docker|service update' "$CALLS" \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | wc -l | tr -d ' ')"
  if [[ "$update_count" -ne 1 ]]; then
    echo "FAIL: $state ChannelOps identity update must issue exactly one service update" >&2
    exit 1
  fi
  update_call="$(grep -F 'docker|service update' "$CALLS" \
    | grep -F 'vp-channel-agent-runner-swarm')"
  managed_additions="$(channelops_runner_argument_pair_count "$update_call" \
    '--env-add' 'CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1')"
  removals="$(channelops_runner_argument_pair_count "$update_call" \
    '--env-rm' 'CHANNELOPS_RUNNER_ID')"
  if [[ "$managed_additions" -ne 1 || "$removals" -ne "$expected_removals" ]]; then
    echo "FAIL: $state ChannelOps identity reconciliation added=$managed_additions removed=$removals" >&2
    exit 1
  fi
  if [[ "$(cat "$CHANNEL_RUNNER_ENV_STATE_FILE")" != converged ]]; then
    echo "FAIL: $state ChannelOps identity update did not converge the fake service" >&2
    exit 1
  fi
}

assert_channelops_runner_identity_reconciliation absent 0
assert_channelops_runner_identity_reconciliation legacy 1
assert_channelops_runner_identity_reconciliation duplicate 1
assert_channelops_runner_identity_reconciliation converged 1
assert_channelops_runner_identity_reconciliation converged 1
