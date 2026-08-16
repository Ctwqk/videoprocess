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
FAKE_NODE_UPDATE_FAILURE_USED="$TEST_ROOT/node-update-failure-used"
FAKE_RUNNING_FAILURE_USED="$TEST_ROOT/running-failure-used"
FAKE_WATCH_TARGET="$ROOT/bin/channelops-soak-watch.sh"
CHANNEL_RUNNER_ENV_STATE_FILE="$TEST_ROOT/channelops-runner-env-state"
VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
TEST_COMMIT="0123456789abcdef0123456789abcdef01234567"
cleanup_test_root() {
  local exit_status=$?
  if [[ "${KEEP_TEST_ROOT:-false}" == true ]]; then
    printf 'preserved test root: %s\n' "$TEST_ROOT" >&2
  else
    rm -rf "$TEST_ROOT"
  fi
  exit "$exit_status"
}
trap cleanup_test_root EXIT

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
VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE="$TEST_ROOT/deploy-migrator-url"
VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$TEST_ROOT/deploy-read-url"
VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE="$TEST_ROOT/control-role-owner-url"
VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="$TEST_ROOT/runtime-role-owner-url"
VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL=vp_deploy_migrator
VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL=vp_deploy_read
VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL=vp_control_role_owner
VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL=vp_runtime_role_owner
for credential_file in \
  "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
  "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
  "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE" \
  "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE"; do
  printf '%s\n' \
    'postgresql+asyncpg://test:test@10.0.0.150:5435/videoprocess' \
    >"$credential_file"
  chmod 0400 "$credential_file"
done
VP_MINIO_ACCESS_KEY=test-access
VP_MINIO_SECRET_KEY=test-secret
GPU_SERVICE_EXISTS=true
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
GPU_SERVICE_STATE_FILE="$TEST_ROOT/gpu-service-created"
GPU_SERVICE_GENERATION_FILE="$TEST_ROOT/gpu-service-generation"
VISION_SERVICE_STATE_FILE="$TEST_ROOT/vision-service-created"
PUBLISHER_SERVICE_STATE_FILE="$TEST_ROOT/publisher-service-created"
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
FAIL_RUNNING_SERVICE_ONCE=
FAIL_HEALTH_CHECK=
FAIL_NODE_UPDATE=false
FAIL_NODE_UPDATE_ONCE=false
FAIL_NETWORK_INSPECT=false
FAIL_PUBLISHER_CREATE=false
FAIL_MANAGED_CRON_PRINTF=false
FAIL_SOAK_CLEANUP=false
MIGRATION_GATE_MODE=success
MIGRATION_RUN_MODE=success
VISION_CUTOVER_GATE_MODE=success
VISION_FINAL_CUTOVER_GATE_MODE=success
VISION_CONSUMER_CUTOVER_MODE=success
VISION_CONSUMER_AUDIT_MODE=converged
VISION_JOB_EXISTS=false
VISION_JOB_MODE=
VISION_JOB_NAME=
VISION_JOB_IMAGE=
VISION_SAFETY_JOB_ID=dddddddddddddddddddddddddddddddd
VISION_FINAL_SAFETY_JOB_ID=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
VISION_CHECK_JOB_ID=ffffffffffffffffffffffffffffffff
VISION_RECONCILE_JOB_ID=gggggggggggggggggggggggggggggggg
VISION_SAFETY_JOB_TASK_ID=hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
VISION_FINAL_SAFETY_JOB_TASK_ID=kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk
VISION_CHECK_JOB_TASK_ID=iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii
VISION_RECONCILE_JOB_TASK_ID=jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj
VISION_JOB_ID=
VISION_JOB_TASK_ID=
VISION_JOB_STATE_FILE="$TEST_ROOT/vision-job-state"
printf '|||false\n' >"$VISION_JOB_STATE_FILE"
VISION_SAFETY_DATABASE_SECRET_ID=cccccccccccccccccccccccccccccccc
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
  if [[ "$1" == "$FAIL_RUNNING_SERVICE" ]]; then
    return 1
  fi
  if [[ "$1" == "$FAIL_RUNNING_SERVICE_ONCE" \
    && ! -f "$FAKE_RUNNING_FAILURE_USED" ]]; then
    : >"$FAKE_RUNNING_FAILURE_USED"
    return 1
  fi
  return 0
}

remote_sh() {
  local host="$1"
  printf 'remote|%s' "$1" >>"$CALLS"
  shift
  printf '|%s' "$@" >>"$CALLS"
  printf '\n' >>"$CALLS"
  local remote_script
  remote_script="$(command cat)"

  if [[ "$host" == "$VP_RUNTIME_HOST" \
    && "${1:-}" == "/bin/sh" \
    && "${2:-}" == "-s" \
    && "${3:-}" == "--" \
    && "${4:-}" == "/Users/wenjieliu/VideoProcess-app" \
    && "${5:-}" == "backend/Dockerfile.ffmpeg-worker-go" \
    && "${6:-}" == "vp-ffmpeg-worker-go:deploy-0123456789ab" \
    && "${7:-}" == "$TEST_COMMIT" ]]; then
    grep -Fq -- '--build-arg "VP_BUILD_COMMIT=$build_commit"' \
      <<<"$remote_script"
    return
  fi

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

test_service_id() {
  builtin printf '%s' "$1" | shasum -a 256 \
    | command awk '{print substr($1, 1, 24)}'
}

test_worker_generation() {
  case "$1" in
    vp-ffmpeg-worker-go-swarm)
      builtin printf '%s\n' "$VP_WORKER_FFMPEG_GO_GENERATION"
      ;;
    vp-ffmpeg-worker-gpu-swarm)
      if [[ -f "$GPU_SERVICE_GENERATION_FILE" ]]; then
        command cat "$GPU_SERVICE_GENERATION_FILE"
      else
        builtin printf '%s\n' "$VP_WORKER_FFMPEG_GENERATION"
      fi
      ;;
    vp-vision-worker-swarm)
      builtin printf '%s\n' "$VP_WORKER_VISION_GENERATION"
      ;;
    vp-youtube-publisher-swarm)
      builtin printf '%s\n' "$VP_WORKER_YOUTUBE_PUBLISHER_GENERATION"
      ;;
    *) return 1 ;;
  esac
}

test_worker_service_exists() {
  case "$1" in
    vp-ffmpeg-worker-gpu-swarm)
      [[ "$GPU_SERVICE_EXISTS" == true || -f "$GPU_SERVICE_STATE_FILE" ]]
      ;;
    vp-vision-worker-swarm)
      [[ "$VISION_SERVICE_EXISTS" == true || -f "$VISION_SERVICE_STATE_FILE" ]]
      ;;
    vp-youtube-publisher-swarm)
      [[ "$PUBLISHER_SERVICE_EXISTS" == true \
        || -f "$PUBLISHER_SERVICE_STATE_FILE" ]]
      ;;
    vp-ffmpeg-worker-go-swarm)
      return 0
      ;;
    *) return 1 ;;
  esac
}

test_mark_worker_service_created() {
  case "$1" in
    vp-ffmpeg-worker-gpu-swarm) : >"$GPU_SERVICE_STATE_FILE" ;;
    vp-vision-worker-swarm) : >"$VISION_SERVICE_STATE_FILE" ;;
    vp-youtube-publisher-swarm) : >"$PUBLISHER_SERVICE_STATE_FILE" ;;
    *) return 1 ;;
  esac
}

test_mark_worker_service_absent() {
  case "$1" in
    vp-ffmpeg-worker-gpu-swarm)
      command rm -f \
        "$GPU_SERVICE_STATE_FILE" "$GPU_SERVICE_GENERATION_FILE"
      ;;
    vp-vision-worker-swarm) command rm -f "$VISION_SERVICE_STATE_FILE" ;;
    vp-youtube-publisher-swarm) command rm -f "$PUBLISHER_SERVICE_STATE_FILE" ;;
    *) return 1 ;;
  esac
}

test_service_name_for_reference() {
  local reference="$1"
  local service
  for service in \
    $VP_APP_SERVICES "$VP_PDS_SERVICE" vp-feature-aggregator-swarm; do
    if [[ "$reference" == "$service" \
      || "$reference" == "$(test_service_id "$service")" ]]; then
      builtin printf '%s\n' "$service"
      return 0
    fi
  done
  return 1
}

docker() {
  local update_service=""
  if [[ "${1:-} ${2:-}" == "service update" ]]; then
    update_service="$(test_service_name_for_reference "${!#}")" || return 1
  fi
  if [[ "${1:-}" == "exec" ]]; then
    printf 'docker'
    printf '|%s' "$@"
    printf '\n'
  else
    printf 'docker|%s' "$*"
    if [[ -n "$update_service" ]]; then
      printf ' logical-service=%s' "$update_service"
    fi
    printf '\n'
  fi >>"$CALLS"
  IFS='|' read -r \
    VISION_JOB_NAME VISION_JOB_MODE VISION_JOB_IMAGE VISION_JOB_EXISTS \
    <"$VISION_JOB_STATE_FILE"
  case "$VISION_JOB_MODE" in
    safety)
      VISION_JOB_ID="$VISION_SAFETY_JOB_ID"
      VISION_JOB_TASK_ID="$VISION_SAFETY_JOB_TASK_ID"
      ;;
    final-safety)
      VISION_JOB_ID="$VISION_FINAL_SAFETY_JOB_ID"
      VISION_JOB_TASK_ID="$VISION_FINAL_SAFETY_JOB_TASK_ID"
      ;;
    check)
      VISION_JOB_ID="$VISION_CHECK_JOB_ID"
      VISION_JOB_TASK_ID="$VISION_CHECK_JOB_TASK_ID"
      ;;
    reconcile)
      VISION_JOB_ID="$VISION_RECONCILE_JOB_ID"
      VISION_JOB_TASK_ID="$VISION_RECONCILE_JOB_TASK_ID"
      ;;
    *)
      VISION_JOB_ID=
      VISION_JOB_TASK_ID=
      ;;
  esac
  if [[ "${1:-} ${2:-}" == "image inspect" \
    && "${3:-}" == *":deploy-${TEST_COMMIT:0:12}" ]]; then
    builtin printf '%s\n' "$TEST_COMMIT"
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "secret inspect" \
    && "$*" == *'{{.ID}}|{{.Spec.Name}}'* ]]; then
    local runtime_secret_reference="${3:-}"
    if [[ -n "$VP_WORKER_REDIS_WATCHER_SECRET_ID" \
      && "$runtime_secret_reference" == "$VP_WORKER_REDIS_WATCHER_SECRET_ID" ]]; then
      builtin printf '%s|%s\n' \
        "$VP_WORKER_REDIS_WATCHER_SECRET_ID" \
        "$VP_WORKER_REDIS_WATCHER_SECRET"
      return 0
    fi
    if [[ -n "$VP_WORKER_REDIS_CONTROL_SECRET_ID" \
      && "$runtime_secret_reference" == "$VP_WORKER_REDIS_CONTROL_SECRET_ID" ]]; then
      builtin printf '%s|%s\n' \
        "$VP_WORKER_REDIS_CONTROL_SECRET_ID" \
        "$VP_WORKER_REDIS_CONTROL_SECRET"
      return 0
    fi
    local runtime_secret_name="$runtime_secret_reference"
    local runtime_secret_id
    runtime_secret_id="$(
      builtin printf '%s' "$runtime_secret_name" | shasum -a 256 \
        | command awk '{print $1}'
    )"
    builtin printf '%s|%s\n' "$runtime_secret_id" "$runtime_secret_name"
    return 0
  fi
  if [[ "${1:-}" == "run" \
    && "$*" == *"python -m app.services.worker_deployment_cli migrate"* ]]; then
    [[ "$MIGRATION_RUN_MODE" == "success" ]]
    return
  fi
  if [[ "${1:-}" == "run" \
    && "$*" == *"python -m app.services.worker_deployment_cli verify-head"* ]]; then
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
  if [[ "${1:-} ${2:-}" == "node update" ]]; then
    if [[ "$FAIL_NODE_UPDATE" == "true" ]]; then
      return 1
    fi
    if [[ "$FAIL_NODE_UPDATE_ONCE" == "true" \
      && ! -f "$FAKE_NODE_UPDATE_FAILURE_USED" ]]; then
      : >"$FAKE_NODE_UPDATE_FAILURE_USED"
      return 1
    fi
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-ffmpeg-worker-gpu-swarm"* ]]; then
    GPU_SERVICE_EXISTS=true
    test_mark_worker_service_created vp-ffmpeg-worker-gpu-swarm
    if [[ "$FAIL_GPU_CREATE" == "true" ]]; then
      printf '%s\n' "$VP_WORKER_FFMPEG_GENERATION" \
        >"$GPU_SERVICE_GENERATION_FILE"
      return 1
    fi
    test_service_id vp-ffmpeg-worker-gpu-swarm
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-vision-worker-swarm"* ]]; then
    VISION_SERVICE_EXISTS=true
    test_mark_worker_service_created vp-vision-worker-swarm
    test_service_id vp-vision-worker-swarm
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-youtube-publisher-swarm"* ]]; then
    PUBLISHER_SERVICE_EXISTS=true
    test_mark_worker_service_created vp-youtube-publisher-swarm
    if [[ "$FAIL_PUBLISHER_CREATE" == "true" ]]; then
      return 1
    fi
    test_service_id vp-youtube-publisher-swarm
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service create" \
    && "$*" == *"--label vp.service=vision-cutover"* ]]; then
    local previous=""
    local argument
    for argument in "$@"; do
      if [[ "$previous" == --name ]]; then
        VISION_JOB_NAME="$argument"
      elif [[ "$argument" == vp.purpose=* ]]; then
        VISION_JOB_MODE="${argument#vp.purpose=}"
      elif [[ "$argument" == vp-ffmpeg-worker-python:* ]]; then
        VISION_JOB_IMAGE="$argument"
      fi
      previous="$argument"
    done
    case "$VISION_JOB_MODE" in
      safety)
        VISION_JOB_ID="$VISION_SAFETY_JOB_ID"
        VISION_JOB_TASK_ID="$VISION_SAFETY_JOB_TASK_ID"
        ;;
      final-safety)
        VISION_JOB_ID="$VISION_FINAL_SAFETY_JOB_ID"
        VISION_JOB_TASK_ID="$VISION_FINAL_SAFETY_JOB_TASK_ID"
        ;;
      check)
        VISION_JOB_ID="$VISION_CHECK_JOB_ID"
        VISION_JOB_TASK_ID="$VISION_CHECK_JOB_TASK_ID"
        ;;
      reconcile)
        VISION_JOB_ID="$VISION_RECONCILE_JOB_ID"
        VISION_JOB_TASK_ID="$VISION_RECONCILE_JOB_TASK_ID"
        ;;
      *) return 1 ;;
    esac
    VISION_JOB_EXISTS=true
    builtin printf '%s|%s|%s|true\n' \
      "$VISION_JOB_NAME" "$VISION_JOB_MODE" "$VISION_JOB_IMAGE" \
      >"$VISION_JOB_STATE_FILE"
    builtin printf '%s\n' "$VISION_JOB_ID"
    return 0
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-ffmpeg-worker-gpu-swarm" ]]; then
    GPU_SERVICE_EXISTS=false
    test_mark_worker_service_absent vp-ffmpeg-worker-gpu-swarm
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-vision-worker-swarm" ]]; then
    VISION_SERVICE_EXISTS=false
    test_mark_worker_service_absent vp-vision-worker-swarm
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-youtube-publisher-swarm" ]]; then
    PUBLISHER_SERVICE_EXISTS=false
    test_mark_worker_service_absent vp-youtube-publisher-swarm
  fi
  if [[ "${1:-} ${2:-}" == "service rm" ]]; then
    case "${3:-}" in
      "$(test_service_id vp-ffmpeg-worker-gpu-swarm)")
        GPU_SERVICE_EXISTS=false
        test_mark_worker_service_absent vp-ffmpeg-worker-gpu-swarm
        return 0
        ;;
      "$(test_service_id vp-vision-worker-swarm)")
        VISION_SERVICE_EXISTS=false
        test_mark_worker_service_absent vp-vision-worker-swarm
        return 0
        ;;
      "$(test_service_id vp-youtube-publisher-swarm)")
        PUBLISHER_SERVICE_EXISTS=false
        test_mark_worker_service_absent vp-youtube-publisher-swarm
        return 0
        ;;
    esac
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && -n "$FAIL_UPDATE_SERVICE" \
    && "$*" == *"--image $FAIL_UPDATE_IMAGE"* \
    && "$update_service" == "$FAIL_UPDATE_SERVICE" ]]; then
    return "$FAIL_UPDATE_EXIT"
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && "$update_service" == "vp-pds-swarm" ]]; then
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
    && "$update_service" == "vp-channel-agent-runner-swarm" \
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
    if [[ "${3:-}" == "$VISION_JOB_ID" ]]; then
      builtin printf '%s|Shutdown|Complete 1 second ago\n' \
        "$VISION_JOB_TASK_ID"
      return 0
    fi
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
  if [[ "${1:-}" == inspect && "${2:-}" == "$VISION_JOB_TASK_ID" ]]; then
    case "$VISION_JOB_MODE" in
      safety)
        [[ "$VISION_CUTOVER_GATE_MODE" == success ]] && builtin printf '0\n' \
          || builtin printf '1\n'
        ;;
      final-safety)
        [[ "$VISION_FINAL_CUTOVER_GATE_MODE" == success ]] \
          && builtin printf '0\n' || builtin printf '1\n'
        ;;
      check)
        [[ "$VISION_CONSUMER_AUDIT_MODE" == needs-cutover ]] \
          && builtin printf '10\n' || builtin printf '0\n'
        ;;
      reconcile)
        [[ "$VISION_CONSUMER_CUTOVER_MODE" == success ]] \
          && builtin printf '0\n' || builtin printf '1\n'
        ;;
      *) return 1 ;;
    esac
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service logs" \
    && "${3:-}" == "$VISION_JOB_ID" ]]; then
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service rm" \
    && "${3:-}" == "$VISION_JOB_ID" ]]; then
    VISION_JOB_EXISTS=false
    builtin printf '%s|%s|%s|false\n' \
      "$VISION_JOB_NAME" "$VISION_JOB_MODE" "$VISION_JOB_IMAGE" \
      >"$VISION_JOB_STATE_FILE"
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
      missing-then-normal|duplicate-then-normal)
        local readiness_container_call=0
        if [[ -f "$WORKER_READINESS_CONTAINER_CALLS" ]]; then
          readiness_container_call="$(<"$WORKER_READINESS_CONTAINER_CALLS")"
        fi
        readiness_container_call=$((readiness_container_call + 1))
        printf '%s\n' "$readiness_container_call" >"$WORKER_READINESS_CONTAINER_CALLS"
        if [[ "$readiness_container_call" -gt 10 ]]; then
          printf '%s\n' "$readiness_container_id"
        elif [[ "$WORKER_READINESS_CONTAINER_MODE" == duplicate-then-normal ]]; then
          printf '%s\n%s\n' \
            "$readiness_container_id" "${readiness_container_id}duplicate"
        fi
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
    echo 'vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
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
    if test_worker_service_exists vp-youtube-publisher-swarm; then
      printf '%s\n' "${PUBLISHER_LIST_NAME:-vp-youtube-publisher-swarm}"
    fi
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
    local service="${3:-}"
    local registered_worker_service
    for registered_worker_service in \
      $VP_APP_SERVICES "$VP_PDS_SERVICE" vp-feature-aggregator-swarm; do
      if [[ "$service" == "$(test_service_id "$registered_worker_service")" ]]; then
        service="$registered_worker_service"
        break
      fi
    done
    if [[ "$service" == "$VISION_JOB_ID" || "$service" == "$VISION_JOB_NAME" ]]; then
      [[ "$VISION_JOB_EXISTS" == true ]] || return 1
      if [[ "$*" == *'{{.ID}}|{{.Spec.Name}}'* ]]; then
        builtin printf '%s|%s|vision-cutover|%s|%s\n' \
          "$VISION_JOB_ID" "$VISION_JOB_NAME" \
          "$VP_WORKER_ADMISSION_TRANSACTION_ID" "$VISION_JOB_MODE"
      elif [[ "$*" == *'{{json .Spec}}'* ]]; then
        local redis_id="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
        local database_secret_json=""
        local database_env_json=""
        local cli_json=''
        if [[ "$VISION_JOB_MODE" == reconcile ]]; then
          redis_id="$VP_WORKER_REDIS_CONTROL_SECRET_ID"
        elif [[ "$VISION_JOB_MODE" == safety \
          || "$VISION_JOB_MODE" == final-safety ]]; then
          cli_json=',"--safety"'
          database_secret_json=',{"SecretID":"cccccccccccccccccccccccccccccccc","File":{"Name":"vision-cutover-database-url","UID":"10001","GID":"10001","Mode":256}}'
          database_env_json=',"VISION_CUTOVER_DATABASE_URL_FILE=/run/secrets/vision-cutover-database-url"'
        else
          cli_json=',"--check-only"'
        fi
        builtin printf '{"Name":"%s","Labels":{"vp.service":"vision-cutover","vp.generation":"%s","vp.purpose":"%s"},"Mode":{"ReplicatedJob":{"TotalCompletions":1,"MaxConcurrent":1}},"TaskTemplate":{"ContainerSpec":{"Image":"%s","Args":["python","-m","app.services.vision_consumer_cutover"%s],"Env":["VISION_CUTOVER_REDIS_URL_FILE=/run/secrets/vision-cutover-redis-url"%s],"Secrets":[{"SecretID":"%s","File":{"Name":"vision-cutover-redis-url","UID":"10001","GID":"10001","Mode":256}}%s]},"RestartPolicy":{"Condition":"none"},"Placement":{"Constraints":["node.hostname==ccttww-lap"]},"Networks":[{"Target":"vp-pipeline-network-id"}]}}\n' \
          "$VISION_JOB_NAME" "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
          "$VISION_JOB_MODE" "$VISION_JOB_IMAGE" "$cli_json" \
          "$database_env_json" "$redis_id" "$database_secret_json"
      fi
      return 0
    fi
    if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]] \
      && ! test_worker_service_exists "$service"; then
      return 1
    fi
    if [[ "$service" == "vp-vision-worker-swarm" ]] \
      && ! test_worker_service_exists "$service"; then
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" ]] \
      && ! test_worker_service_exists "$service"; then
      echo "no such service: $service" >&2
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" \
      && -n "$FAIL_PUBLISHER_INSPECT_FORMAT" \
      && "$*" == *"$FAIL_PUBLISHER_INSPECT_FORMAT"* ]]; then
      return 1
    fi
    if [[ "$*" == *'vp.managed-by'* ]]; then
      local service_id
      local worker_generation
      service_id="$(test_service_id "$service")"
      worker_generation="$(test_worker_generation "$service")" || return 1
      builtin printf '%s|%s|%s|%s|videoprocess-deploy\n' \
        "$service_id" "$service" "$service" "$worker_generation"
      return 0
    fi
    if [[ "$*" == *'{{.ID}}|{{.Spec.Name}}' ]]; then
      local service_id
      service_id="$(test_service_id "$service")"
      builtin printf '%s|%s\n' "$service_id" "$service"
      return 0
    fi
    if [[ "$*" == *'{{json .Spec}}'* ]]; then
      builtin printf \
        '{"Name":"%s","TaskTemplate":{"ContainerSpec":{"Image":"baseline-%s:stable"}}}\n' \
        "$service" "$service"
      return 0
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
for worker_dockerfile in \
  "$ROOT_DIR/backend/Dockerfile.worker" \
  "$ROOT_DIR/backend/Dockerfile.ffmpeg-worker-go"; do
  if ! grep -Fq 'ARG VP_BUILD_COMMIT' "$worker_dockerfile" \
    || ! grep -Fq 'org.opencontainers.image.revision=$VP_BUILD_COMMIT' \
      "$worker_dockerfile"; then
    echo "FAIL: worker image does not embed the reviewed build commit: $worker_dockerfile" >&2
    exit 1
  fi
done
if ! grep -Fq 'VP_WORKER_REDIS_FFMPEG_GO_SECRET' "$EXTENSION" \
  || ! grep -Fq 'VP_WORKER_REDIS_FFMPEG_SECRET' "$EXTENSION" \
  || ! grep -Fq 'VP_WORKER_REDIS_VISION_SECRET' "$EXTENSION" \
  || ! grep -Fq 'VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET' "$EXTENSION"; then
  echo 'FAIL: deployment does not consume all runtime-published worker Redis secrets' >&2
  exit 1
fi
if grep -Eq '"(DATABASE_URL|REDIS_URL)=\\$|REDIS_URL=redis://10\\.0\\.0\\.150:6380' \
  "$EXTENSION"; then
  echo 'FAIL: registered worker URLs remain exposed in service environment' >&2
  exit 1
fi
if ! grep -Fq 'vp_run_worker_registration_migration "$backend"' "$EXTENSION" \
  || ! grep -Fq 'vp_require_channelops_migration_head "$backend"' "$EXTENSION"; then
  echo 'FAIL: managed ChannelOps runner must be gated on the exact migration head' >&2
  exit 1
fi
if ! grep -Fq 'worker_deployment_cli migrate' "$EXTENSION" \
  || ! grep -Fq 'worker_deployment_cli verify-head' "$EXTENSION"; then
  echo 'FAIL: migration must use the protected file-only deployment CLIs' >&2
  exit 1
fi
migration_contract="$(
  sed -n \
    -e '/^vp_run_worker_registration_migration()/,/^}/p' \
    -e '/^vp_require_channelops_migration_head()/,/^}/p' \
    "$EXTENSION"
)"
if grep -Eq -- '--env DATABASE_URL|VP_PYTHON_WORKER_DATABASE_URL' \
  <<<"$migration_contract"; then
  echo 'FAIL: migration contract retains a raw database credential path' >&2
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

vp_worker_admission_create_secret() {
  builtin printf 'vision-secret-create|%s|%s|%s|%s|%s\n' "$@" >>"$CALLS"
  VP_WORKER_CREATED_SECRET_ID="$VISION_SAFETY_DATABASE_SECRET_ID"
  vp_worker_admission_record_prepared_secret \
    "$1" "$VP_WORKER_CREATED_SECRET_ID" "$3" "$4" "$5"
}

vp_remove_managed_secret() {
  builtin printf 'vision-secret-rm|%s|%s|%s|%s|%s\n' "$@" >>"$CALLS"
}

runtime_state="$TEST_ROOT/worker-redis-runtime.state"
builtin printf '%s\n' \
  "GENERATION=$TEST_COMMIT" \
  "ACL_IDENTITY=vp-marker-acl-v1" \
  "AOF_ENABLED=yes" \
  "AOF_STATUS=ok" \
  "MAXMEMORY_POLICY=noeviction" \
  "NETWORK=vp-pipeline-net" \
  "CONTROL_REDIS_SECRET=vp-control-redis-$TEST_COMMIT" \
  "FFMPEG_GO_REDIS_SECRET=vp-ffmpeg-go-redis-$TEST_COMMIT" \
  "FFMPEG_REDIS_SECRET=vp-ffmpeg-redis-$TEST_COMMIT" \
  "VISION_REDIS_SECRET=vp-vision-redis-$TEST_COMMIT" \
  "YOUTUBE_PUBLISHER_REDIS_SECRET=vp-youtube-redis-$TEST_COMMIT" \
  "WATCHER_REDIS_SECRET=vp-watcher-redis-$TEST_COMMIT" \
  "READINESS_REDIS_SECRET=vp-marker-readiness-$TEST_COMMIT" \
  "JANITOR_REDIS_SECRET=vp-marker-janitor-$TEST_COMMIT" \
  "REPAIR_REDIS_SECRET=vp-marker-repair-$TEST_COMMIT" \
  >"$runtime_state"
chmod 0400 "$runtime_state"
VP_WORKER_REDIS_RUNTIME_STATE_FILE="$runtime_state"
VP_WORKER_REDIS_RUNTIME_GENERATION="$TEST_COMMIT"
vp_require_worker_redis_runtime_state
for loaded_secret in \
  "$VP_WORKER_REDIS_CONTROL_SECRET" \
  "$VP_WORKER_REDIS_FFMPEG_GO_SECRET" \
  "$VP_WORKER_REDIS_FFMPEG_SECRET" \
  "$VP_WORKER_REDIS_VISION_SECRET" \
  "$VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET" \
  "$VP_WORKER_REDIS_WATCHER_SECRET"; do
  if [[ -z "$loaded_secret" ]]; then
    echo 'FAIL: worker Redis runtime state did not publish every client secret' >&2
    exit 1
  fi
done

VP_WORKER_ADMISSION_PREPARED=true
VP_WORKER_ADMISSION_COMMIT="$TEST_COMMIT"
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab

vp_worker_service_registration_env() {
  local service="$1"
  local image="$2"
  local worker_type=""
  local worker_host=""
  local capabilities=""
  local stream=""
  local group=""
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      worker_type=ffmpeg_go
      worker_host=colima-127
      capabilities=media_cpu
      stream=vp:tasks:ffmpeg_go
      group=ffmpeg_go-workers
      ;;
    vp-ffmpeg-worker-gpu-swarm)
      worker_type=ffmpeg
      worker_host=150-gpu
      capabilities=media_gpu
      stream=vp:tasks:ffmpeg
      group=ffmpeg-workers
      ;;
    vp-vision-worker-swarm)
      worker_type=vision
      worker_host=150-vision
      capabilities=vision_gpu
      stream=vp:tasks:vision
      group=vision-workers
      ;;
    vp-youtube-publisher-swarm)
      worker_type=youtube_publisher
      worker_host=150-publisher
      capabilities=youtube_publisher
      stream=vp:tasks:youtube_publisher
      group=youtube_publisher-workers
      ;;
    *)
      return 1
      ;;
  esac
  builtin printf '%s\n' \
    DEPLOY_MODE=production \
    "WORKER_SERVICE_NAME=$service" \
    WORKER_ADMISSION_GENERATION=101 \
    WORKER_SLOT=1 \
    "WORKER_TYPE=$worker_type" \
    "WORKER_HOST=$worker_host" \
    "WORKER_CAPABILITIES=$capabilities" \
    "WORKER_RELEASE_COMMIT=$TEST_COMMIT" \
    "WORKER_IMAGE_IDENTITY=$image" \
    "WORKER_REDIS_STREAM=$stream" \
    "WORKER_REDIS_GROUP=$group" \
    WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url \
    WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token \
    WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url \
    WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key \
    WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key \
    VP_REQUIRE_STAGING_JANITOR=true
}

vp_worker_service_secret_specs() {
  builtin printf '%s\n' \
    source=test-worker-db,target=vp-worker-database-url,mode=0400 \
    source=test-worker-admission,target=vp-worker-admission-token,mode=0400 \
    source=test-worker-redis,target=vp-worker-redis-url,mode=0400 \
    source=test-worker-minio-access,target=vp-worker-minio-access-key,mode=0400 \
    source=test-worker-minio-secret,target=vp-worker-minio-secret-key,mode=0400
}

vp_prepare_worker_admission() {
  local control_image="$1"
  local go_image="$2"
  VP_WORKER_ADMISSION_PREPARED=true
  VP_WORKER_CONTROL_PREPARED=false
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  VP_WORKER_CONTROL_GENERATION="c-${TEST_COMMIT:0:20}"
  VP_WORKER_OPERATOR_DATABASE_SECRET="test-control-operator"
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET="test-control-orchestrator"
  VP_STAGING_JANITOR_DATABASE_SECRET="test-control-staging"
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET="test-control-staging-minio-access"
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET="test-control-staging-minio-secret"
  VP_WORKER_MINIO_ACCESS_SECRET="test-control-worker-minio-access"
  VP_WORKER_MINIO_SECRET_SECRET="test-control-worker-minio-secret"
  local control_candidate="$root/control-candidates/$VP_WORKER_CONTROL_GENERATION.conf"
  vp_worker_control_write_manifest \
    "$control_candidate" "$VP_WORKER_CONTROL_GENERATION" "$control_image" \
    301111111111111111111111 \
    302222222222222222222222 \
    303333333333333333333333 \
    304444444444444444444444 \
    305555555555555555555555 \
    306666666666666666666666 \
    307777777777777777777777
  local operator_reference="control/$VP_WORKER_CONTROL_GENERATION/worker-registration-operator-database-url"
  vp_worker_admission_record_authority_intent \
    control vp-worker-control "$VP_WORKER_CONTROL_GENERATION" \
    "$control_image" "$VP_WORKER_CONTROL_GENERATION" \
    "$operator_reference"
  vp_worker_admission_mark_authority_provisioning \
    control vp-worker-control "$VP_WORKER_CONTROL_GENERATION"
  vp_worker_admission_mark_authority_provisioned \
    control vp-worker-control "$VP_WORKER_CONTROL_GENERATION"
  vp_worker_admission_record_control_selection forward "$control_candidate"
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    local image="$control_image"
    local generation
    local database_id
    local admission_id
    case "$service" in
      vp-ffmpeg-worker-go-swarm)
        image="$go_image"
        generation=101
        database_id=aaaaaaaaaaaaaaaaaaaaaaaa
        admission_id=bbbbbbbbbbbbbbbbbbbbbbbb
        ;;
      "$VP_PYTHON_WORKER_SERVICE")
        generation=102
        database_id=cccccccccccccccccccccccc
        admission_id=dddddddddddddddddddddddd
        ;;
      "$VP_VISION_WORKER_SERVICE")
        generation=103
        database_id=eeeeeeeeeeeeeeeeeeeeeeee
        admission_id=ffffffffffffffffffffffff
        ;;
      "$VP_PUBLISHER_SERVICE")
        generation=104
        database_id=111111111111111111111111
        admission_id=222222222222222222222222
        ;;
    esac
    vp_worker_admission_record_authority_intent \
      runtime "$service" "$generation" \
      "$control_image" "$VP_WORKER_CONTROL_GENERATION" \
      "$operator_reference"
    vp_worker_admission_mark_authority_provisioning \
      runtime "$service" "$generation"
    vp_worker_admission_mark_authority_provisioned \
      runtime "$service" "$generation"
    local kind
    kind="$(vp_worker_admission_kind "$service")"
    vp_worker_admission_write_manifest \
      "$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf" \
      "$service" "$VP_WORKER_ADMISSION_COMMIT" "$image" "$generation" \
      "test-$kind-database-$generation" \
      "test-$kind-admission-$generation" \
      "$database_id" "$admission_id"
    vp_worker_admission_set_candidate \
      "$service" "$generation" \
      "test-$kind-database-$generation" \
      "test-$kind-admission-$generation"
    vp_worker_admission_track_candidate "$service"
    vp_worker_admission_record_prepared_worker_plan "$service" "$image"
  done
  printf 'worker-admission|prepare|%s|%s\n' \
    "$control_image" "$go_image" >>"$CALLS"
}

vp_activate_worker_admission() {
  printf 'worker-admission|activate|%s\n' "$1" >>"$CALLS"
}

vp_require_worker_deployment_ready() {
  printf 'worker-admission|ready|%s\n' "$1" >>"$CALLS"
}

vp_worker_admission_live_worker_identity() {
  case "$1" in
    vp-ffmpeg-worker-go-swarm)
      printf '%s|%s\n' \
        333333333333333333333333 \
        1111111111111111111111111111111111111111111111111111111111111111
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      printf '%s|%s\n' \
        444444444444444444444444 \
        2222222222222222222222222222222222222222222222222222222222222222
      ;;
    "$VP_VISION_WORKER_SERVICE")
      printf '%s|%s\n' \
        555555555555555555555555 \
        3333333333333333333333333333333333333333333333333333333333333333
      ;;
    "$VP_PUBLISHER_SERVICE")
      printf '%s|%s\n' \
        666666666666666666666666 \
        4444444444444444444444444444444444444444444444444444444444444444
      ;;
    *) return 1 ;;
  esac
}

vp_install_staging_object_janitor() {
  printf 'staging-janitor|install|%s\n' "$1" >>"$CALLS"
}

vp_run_staging_object_janitor_once() {
  printf 'staging-janitor|run\n' >>"$CALLS"
}

vp_worker_admission_janitor_service_json() {
  local attempt="${VP_WORKER_ADMISSION_ROLLBACK_ATTEMPT:-0}"
  local generation="test-janitor-$attempt"
  local service_id
  service_id="$(
    printf '%s' \
      "vp-staging-object-janitor|$VP_WORKER_ADMISSION_TRANSACTION_ID|$generation" \
      | shasum -a 256 | cut -c1-24
  )" || return 1
  local spec_digest
  spec_digest="$(
    printf '%s' "vp-staging-object-janitor|$generation|spec" \
      | shasum -a 256 | cut -c1-64
  )" || return 1
  python3 -I -c '
import json
import sys

service_id, generation, spec_digest = sys.argv[1:]
print(json.dumps({
    "name": "vp-staging-object-janitor",
    "docker_service_id": service_id,
    "generation": generation,
    "spec_digest": spec_digest,
}, sort_keys=True, separators=(",", ":")))
' "$service_id" "$generation" "$spec_digest"
}

vp_worker_admission_promotion_identity() {
  local kind="$1"
  local name=""
  local service=""
  local generation=""
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      name=worker-manifests
      service=worker-admission
      generation="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
      ;;
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      name=control.conf
      service=worker-redis-marker-control
      generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
      ;;
    PROMOTE_CONTROL|PROMOTE_ROLLBACK_CONTROL)
      name=control-current.conf
      service=worker-admission-control
      generation="$VP_WORKER_CONTROL_GENERATION"
      ;;
    *) return 1 ;;
  esac
  local identity="$TEST_ROOT/promotion-$kind.json"
  printf '%s\n' \
    "{\"docker_id\":null,\"generation\":\"$generation\",\"kind\":\"manifest\",\"name\":\"$name\",\"purpose\":\"promotion\",\"service\":\"$service\",\"spec_digest\":\"0000000000000000000000000000000000000000000000000000000000000000\"}" \
    >"$identity"
  chmod 0600 "$identity"
  printf '%s\n' "$identity"
}

vp_worker_admission_current_promotion_matches() {
  return 0
}

vp_worker_admission_retire_transaction() {
  printf 'worker-admission|retire\n' >>"$CALLS"
}

vp_commit_worker_admission() {
  printf 'worker-admission|commit\n' >>"$CALLS"
}

vp_commit_worker_redis_marker_controls() {
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  printf 'worker-marker|commit\n' >>"$CALLS"
}

vp_commit_worker_control_generation() {
  printf 'worker-control|commit\n' >>"$CALLS"
}

vp_finalize_worker_control_rollback() {
  VP_WORKER_CONTROL_PREPARED=false
  printf 'worker-control|rollback-finalize\n' >>"$CALLS"
}

vp_restore_worker_redis_marker_controls() {
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  printf 'worker-marker|rollback\n' >>"$CALLS"
}

test_record_rollback_worker_plan() {
  local service="$1"
  local image="$2"
  local generation=""
  local database_id=""
  local admission_id=""
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      generation=201
      database_id=333333333333333333333333
      admission_id=444444444444444444444444
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      generation=202
      database_id=555555555555555555555555
      admission_id=666666666666666666666666
      ;;
    "$VP_VISION_WORKER_SERVICE")
      generation=203
      database_id=777777777777777777777777
      admission_id=888888888888888888888888
      ;;
    "$VP_PUBLISHER_SERVICE")
      generation=204
      database_id=999999999999999999999999
      admission_id=aaaaaaaaaaaaaaaaaaaaaaab
      ;;
    *) return 1 ;;
  esac
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  if [[ -n "${VP_WORKER_ADMISSION_LOCK_ROOT:-}" \
    && -n "${VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE:-}" ]]; then
    vp_worker_admission_write_manifest \
      "$VP_WORKER_ADMISSION_LOCK_ROOT/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf" \
      "$service" "$TEST_COMMIT" "$image" "$generation" \
      "test-rollback-$kind-database-$generation" \
      "test-rollback-$kind-admission-$generation" \
      "$database_id" "$admission_id" || return 1
  fi
  vp_worker_admission_set_candidate \
    "$service" "$generation" \
    "test-rollback-$kind-database-$generation" \
    "test-rollback-$kind-admission-$generation" || return 1
  vp_worker_admission_track_candidate "$service" || return 1
  vp_worker_admission_load_replay_plan || return 1
  printf '%s\n' \
    "{\"admission_secret\":{\"docker_secret_id\":\"$admission_id\",\"generation\":\"$generation\",\"name\":\"test-rollback-$kind-admission-$generation\",\"purpose\":\"admission\",\"service\":\"$service\"},\"commit\":\"$TEST_COMMIT\",\"database_secret\":{\"docker_secret_id\":\"$database_id\",\"generation\":\"$generation\",\"name\":\"test-rollback-$kind-database-$generation\",\"purpose\":\"database\",\"service\":\"$service\"},\"generation\":$generation,\"image\":\"$image\",\"service\":\"$service\",\"target_spec_digest\":null}" \
    | python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      record-worker-plan \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$VP_WORKER_ADMISSION_REPLAY_REVISION" rollback \
      >/dev/null || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    advance-worker-stage \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" rollback \
    "$service" "$generation" pending prepared - - \
    >/dev/null
}

vp_worker_admission_select_candidate() {
  local service="$1"
  local image=""
  image="$(
    vp_worker_admission_snapshot_image \
      "$TEST_ROLLBACK_SNAPSHOTS" "$service"
  )" || return 1
  test_record_rollback_worker_plan "$service" "$image"
}

vp_restore_worker_admission_transaction() {
  if [[ "$VP_WORKER_ADMISSION_PREPARED" != true ]]; then
    vp_restore_app_snapshots "$1" "$2" false "${5:-}" || return 1
    if [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
      vp_worker_redis_marker_discard_managed_state || return 1
      vp_worker_admission_abort_preparing_transaction \
        preparing_failed || return 1
    fi
    VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
    return 0
  fi
  vp_worker_admission_transition_to ROLLBACK_PREPARING || return 1
  vp_worker_admission_allocate_rollback_attempt || return 1
  TEST_ROLLBACK_SNAPSHOTS="$1"
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    vp_app_service_was_attempted "$service" "$2" || continue
    local image
    image="$(
      vp_worker_admission_snapshot_image "$1" "$service"
    )" || continue
    test_record_rollback_worker_plan "$service" "$image" || return 1
  done
  vp_worker_admission_transition_to ROLLBACK_APPLYING || return 1
  vp_restore_app_snapshots "$1" "$2" true "${5:-}" || return 1
  vp_worker_admission_transition_to ROLLBACK_VERIFIED || return 1
  VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_WORKERS || return 1
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_MARKER || return 1
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_CONTROL || return 1
  vp_worker_admission_finish_transaction rolled_back
}

vp_require_worker_redis_runtime_state() {
  VP_WORKER_REDIS_CONTROL_SECRET=control-runtime
  VP_WORKER_REDIS_CONTROL_SECRET_ID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  VP_WORKER_REDIS_FFMPEG_GO_SECRET=ffmpeg-go-runtime
  VP_WORKER_REDIS_FFMPEG_SECRET=ffmpeg-runtime
  VP_WORKER_REDIS_VISION_SECRET=vision-runtime
  VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=youtube-runtime
  VP_WORKER_REDIS_WATCHER_SECRET=watcher-runtime
  VP_WORKER_REDIS_WATCHER_SECRET_ID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$TEST_COMMIT"
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=marker-readiness-runtime
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID=cccccccccccccccccccccccccccccccc
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=marker-janitor-runtime
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID=dddddddddddddddddddddddddddddddd
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET=marker-repair-runtime
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
}

vp_worker_redis_marker_owner_file() {
  printf '/dev/null\n'
}

vp_worker_redis_marker_read_prior_config() {
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=marker-test-prior
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=vp-ffmpeg-worker-python:marker-test-prior
}

vp_worker_redis_marker_capture_managed_state() {
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="${2:-$1/.test-managed-$RANDOM}"
  mkdir -p "$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  chmod 0700 "$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  printf 'VERSION=1\n' \
    >"$VP_WORKER_REDIS_MARKER_MANAGED_STATE/captured"
  chmod 0600 "$VP_WORKER_REDIS_MARKER_MANAGED_STATE/captured"
  : >"$VP_WORKER_REDIS_MARKER_MANAGED_STATE/crontab"
  : >"$VP_WORKER_REDIS_MARKER_MANAGED_STATE/control.conf"
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
  vp_worker_admission_mark_authority_provisioning \
    marker worker-redis-marker-control "$2"
  printf 'marker-control|provision|%s|%s\n' "$1" "$2" >>"$CALLS"
  vp_worker_admission_mark_authority_provisioned \
    marker worker-redis-marker-control "$2"
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

vp_worker_admission_marker_selection_json() {
  local selection_mode="${1:-active}"
  [[ "$selection_mode" == active || "$selection_mode" == expected ]] \
    || return 1
  local generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
  local image="$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE"
  local readiness_database_id
  local janitor_database_id
  local repair_database_id
  local config_sha256
  local cron_sha256
  readiness_database_id="$(
    printf '%s' "$generation|readiness-database" \
      | shasum -a 256 | cut -c1-32
  )" || return 1
  janitor_database_id="$(
    printf '%s' "$generation|janitor-database" \
      | shasum -a 256 | cut -c1-32
  )" || return 1
  repair_database_id="$(
    printf '%s' "$generation|repair-database" \
      | shasum -a 256 | cut -c1-32
  )" || return 1
  config_sha256="$(
    printf '%s' "$generation|$image|config" \
      | shasum -a 256 | cut -c1-64
  )" || return 1
  cron_sha256="$(
    printf '%s' "$generation|$image|cron" \
      | shasum -a 256 | cut -c1-64
  )" || return 1
  python3 -I -c '
import json
import sys

(
    generation,
    image,
    config_sha256,
    cron_sha256,
    readiness_database_id,
    janitor_database_id,
    repair_database_id,
    runtime_generation,
    readiness_redis_name,
    readiness_redis_id,
    janitor_redis_name,
    janitor_redis_id,
) = sys.argv[1:]
references = [
    {
        "name": f"vp-wrm-readiness-db-{generation}",
        "docker_secret_id": readiness_database_id,
        "service": "worker-redis-marker-control",
        "generation": generation,
        "purpose": "readiness-database",
    },
    {
        "name": f"vp-wrm-janitor-db-{generation}",
        "docker_secret_id": janitor_database_id,
        "service": "worker-redis-marker-control",
        "generation": generation,
        "purpose": "janitor-database",
    },
    {
        "name": f"vp-wrm-repair-db-{generation}",
        "docker_secret_id": repair_database_id,
        "service": "worker-redis-marker-control",
        "generation": generation,
        "purpose": "repair-database",
    },
    {
        "name": readiness_redis_name,
        "docker_secret_id": readiness_redis_id,
        "service": "vp-worker-redis-runtime",
        "generation": runtime_generation,
        "purpose": "readiness-redis",
    },
    {
        "name": janitor_redis_name,
        "docker_secret_id": janitor_redis_id,
        "service": "vp-worker-redis-runtime",
        "generation": runtime_generation,
        "purpose": "janitor-redis",
    },
]
print(json.dumps({
    "generation": generation,
    "image": image,
    "config_sha256": config_sha256,
    "cron_sha256": cron_sha256,
    "secrets": references,
}, sort_keys=True, separators=(",", ":")))
' \
    "$generation" "$image" "$config_sha256" "$cron_sha256" \
    "$readiness_database_id" "$janitor_database_id" \
    "$repair_database_id" "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
    "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID"
}

vp_worker_redis_marker_discard_managed_state() {
  rm -rf "$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
}

(
  identity_root="$TEST_ROOT/database-identity-contract"
  mkdir -p "$identity_root"
  principal_image=vp-backend-api:deploy-0123456789ab
  identity_calls="$identity_root/calls"
  : >"$identity_calls"
  PROBE_DUPLICATE_PURPOSE=""
  PROBE_FAILURE_PURPOSE=""
  PROBE_REPLACE_DURING_PURPOSE=""
  TRANSACTION_IMAGE_COMMIT=""
  PROBE_FAILURE_SENTINEL='postgresql://probe-user:probe-password@database/videoprocess'

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  database_identity_canonical_path() {
    local path="$1"
    printf '%s/%s\n' \
      "$(cd "$(dirname "$path")" && pwd -P)" \
      "$(basename "$path")"
  }
  database_identity_purpose() {
    case "$1" in
      "$(database_identity_canonical_path \
          "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE")")
        printf 'deploy_migrator\n'
        ;;
      "$(database_identity_canonical_path \
          "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE")")
        printf 'deploy_read\n'
        ;;
      "$(database_identity_canonical_path \
          "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE")")
        printf 'control_role_owner\n'
        ;;
      "$(database_identity_canonical_path \
          "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE")")
        printf 'runtime_role_owner\n'
        ;;
      *)
        return 1
        ;;
    esac
  }
  database_identity_expected_principal() {
    case "$1" in
      deploy_migrator)
        printf '%s\n' "$VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL"
        ;;
      deploy_read)
        printf '%s\n' "$VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL"
        ;;
      control_role_owner)
        printf '%s\n' "$VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL"
        ;;
      runtime_role_owner)
        printf '%s\n' "$VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL"
        ;;
      *)
        return 1
        ;;
    esac
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$identity_calls"
    if [[ "${1:-} ${2:-}" == "image inspect" \
      && -n "$TRANSACTION_IMAGE_COMMIT" ]]; then
      printf '%s\n' "$TRANSACTION_IMAGE_COMMIT"
      return 0
    fi
    if [[ "${1:-}" != run \
      || "$*" != *"VP_DATABASE_IDENTITY_URL_FILE=/run/secrets/vp-database-identity-url"* ]]; then
      return 97
    fi
    local source=""
    local argument
    for argument in "$@"; do
      case "$argument" in
        type=bind,src=*,dst=/run/secrets/vp-database-identity-url,readonly)
          source="${argument#type=bind,src=}"
          source="${source%,dst=/run/secrets/vp-database-identity-url,readonly}"
          ;;
      esac
    done
    local purpose
    purpose="$(database_identity_purpose "$source")" || return 96
    if [[ "$purpose" == "$PROBE_FAILURE_PURPOSE" ]]; then
      printf '%s\n' "$PROBE_FAILURE_SENTINEL" >&2
      return 1
    fi
    if [[ "$purpose" == "$PROBE_REPLACE_DURING_PURPOSE" ]]; then
      mv "$source" "$source.probed"
      printf 'postgresql://replacement:credential@database/videoprocess\n' \
        >"$source"
      chmod 0400 "$source"
    fi
    local principal
    principal="$(database_identity_expected_principal "$purpose")" || return 95
    if [[ "$purpose" == "$PROBE_DUPLICATE_PURPOSE" ]]; then
      principal="$VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL"
    fi
    printf '{"current_user":"%s","session_user":"%s"}\n' \
      "$principal" "$principal"
  }
  assert_no_identity_mutation() {
    if grep -Eq \
      'docker\\|(secret (create|rm)|service (create|update|rm))|worker_deployment_cli migrate|worker_runtime_role_cli|worker_control_role_cli' \
      "$identity_calls"; then
      echo 'FAIL: database identity rejection reached a worker mutation' >&2
      exit 1
    fi
  }
  prepare_distinct_identity_files() {
    local fixture="$identity_root/$1"
    mkdir -p "$fixture"
    VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE="$fixture/deploy-migrator"
    VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$fixture/deploy-read"
    VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE="$fixture/control-role-owner"
    VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="$fixture/runtime-role-owner"
    local path
    for path in \
      "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
      "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
      "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE" \
      "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE"; do
      printf 'postgresql://fixture:credential@database/videoprocess\n' >"$path"
      chmod 0400 "$path"
    done
  }

  prepare_distinct_identity_files same-path
  VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE=\
"$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE"
  VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE=\
"$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE"
  VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE=\
"$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE"
  if vp_validate_deploy_config "$principal_image" >/dev/null 2>&1; then
    echo 'FAIL: four database purposes accepted one pathname' >&2
    exit 1
  fi
  assert_no_identity_mutation

  : >"$identity_calls"
  prepare_distinct_identity_files hardlink
  rm "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  ln \
    "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  if vp_validate_deploy_config "$principal_image" >/dev/null 2>&1; then
    echo 'FAIL: database purposes accepted a shared inode' >&2
    exit 1
  fi
  assert_no_identity_mutation

  : >"$identity_calls"
  alias_fixture="$identity_root/parent-alias"
  mkdir -p "$alias_fixture/real"
  ln -s "$alias_fixture/real" "$alias_fixture/alias"
  VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE="$alias_fixture/real/shared"
  VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$alias_fixture/alias/shared"
  VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE="$alias_fixture/control"
  VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="$alias_fixture/runtime"
  for path in \
    "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
    "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE" \
    "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE"; do
    printf 'postgresql://fixture:credential@database/videoprocess\n' >"$path"
    chmod 0400 "$path"
  done
  if vp_validate_deploy_config "$principal_image" >/dev/null 2>&1; then
    echo 'FAIL: database purposes accepted one canonical parent-alias path' >&2
    exit 1
  fi
  assert_no_identity_mutation

  : >"$identity_calls"
  prepare_distinct_identity_files probe-inode-replacement
  PROBE_REPLACE_DURING_PURPOSE=deploy_read
  if vp_validate_deploy_config "$principal_image" >/dev/null 2>&1; then
    echo 'FAIL: principal probe accepted an in-flight inode replacement' >&2
    exit 1
  fi
  [[ -f "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE.probed" ]]
  assert_no_identity_mutation

  : >"$identity_calls"
  PROBE_REPLACE_DURING_PURPOSE=""
  prepare_distinct_identity_files principal-mismatch
  PROBE_DUPLICATE_PURPOSE=runtime_role_owner
  if vp_validate_deploy_config "$principal_image" >/dev/null 2>&1; then
    echo 'FAIL: database purpose accepted the wrong session principal' >&2
    exit 1
  fi
  assert_no_identity_mutation

  : >"$identity_calls"
  PROBE_DUPLICATE_PURPOSE=""
  PROBE_FAILURE_PURPOSE=deploy_read
  set +e
  principal_error="$(
    vp_validate_deploy_config "$principal_image" 2>&1
  )"
  principal_status=$?
  set -e
  if [[ "$principal_status" -eq 0 \
    || "$principal_error" != *database_principal_probe_failed* \
    || "$principal_error" == *"$PROBE_FAILURE_SENTINEL"* ]]; then
    echo 'FAIL: principal probe failure was not sanitized' >&2
    exit 1
  fi
  assert_no_identity_mutation

  : >"$identity_calls"
  PROBE_FAILURE_PURPOSE=""
  vp_validate_deploy_config "$principal_image"
  if [[ "$(
      grep -Fc \
        'VP_DATABASE_IDENTITY_URL_FILE=/run/secrets/vp-database-identity-url' \
        "$identity_calls"
    )" -ne 4 ]]; then
    echo 'FAIL: database identity validation did not probe four purposes' >&2
    exit 1
  fi
  if grep -Fq 'postgresql://' "$identity_calls"; then
    echo 'FAIL: database principal probe exposed a credential in argv or env' >&2
    exit 1
  fi

  (
    prepare_distinct_identity_files post-probe-replacement
    TRANSACTION_IMAGE_COMMIT=0123456789abcdef0123456789abcdef01234567
    vp_validate_deploy_config "$principal_image"
    mv \
      "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
      "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE.probed"
    printf 'postgresql://replacement:credential@database/videoprocess\n' \
      >"$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
    chmod 0400 "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
    ROOT="$identity_root/post-probe-transaction/sync"
    transaction_root="$ROOT/state/vp-worker-admission"
    mkdir -p "$transaction_root"
    chmod 0700 "$transaction_root"
    vp_worker_admission_lock_acquire "$transaction_root"
    if vp_worker_admission_prepare_transaction \
      "$principal_image" \
      vp-ffmpeg-worker-go:deploy-0123456789ab \
      vp-ffmpeg-worker-python:deploy-0123456789ab \
      >/dev/null 2>&1; then
      echo 'FAIL: transaction begin accepted a post-probe replacement' >&2
      exit 1
    fi
    if [[ -e "$transaction_root/transactions/active.json" ]]; then
      echo 'FAIL: post-probe replacement created an active transaction' >&2
      exit 1
    fi
    vp_worker_admission_lock_release
  )

  transaction_root="$identity_root/drift/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  vp_worker_admission_lock_acquire "$transaction_root"
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
    "$transaction_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    0123456789abcdef0123456789abcdef01234567 \
    "$principal_image" \
    vp-ffmpeg-worker-go:deploy-0123456789ab \
    0123456789abcdef0123456789abcdef01234567 \
    legacy_no_control \
    <<<"$VP_WORKER_DATABASE_CREDENTIAL_RECORDS" \
    >/dev/null
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  mv \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE.displaced"
  printf 'postgresql://replacement:credential@database/videoprocess\n' \
    >"$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  chmod 0400 "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  : >"$identity_calls"
  if ! declare -F vp_worker_admission_database_credential_file \
      >/dev/null; then
    echo 'FAIL: bind-time database identity verifier is unavailable' >&2
    exit 1
  fi
  if vp_worker_admission_database_credential_file \
    deploy_read \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
    'worker deploy-read database URL file' >/dev/null 2>&1; then
    echo 'FAIL: bind-time database identity drift was accepted' >&2
    exit 1
  fi
  [[ ! -s "$identity_calls" ]]
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  vp_worker_admission_lock_release
)

(
  ROOT="$TEST_ROOT/transaction-preparation/sync"
  REPO_ROOT="$TEST_ROOT/transaction-preparation/repos"
  UPDATE_SERVICES=1
  transaction_root="$ROOT/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  transaction_calls="$TEST_ROOT/transaction-preparation/calls"
  : >"$transaction_calls"
  transaction_commit=0123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend-api:deploy-${transaction_commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${transaction_commit:0:12}"
  control_image="vp-ffmpeg-worker-python:deploy-${transaction_commit:0:12}"
  docker() {
    printf 'docker|%s\n' "$*" >>"$transaction_calls"
    if [[ "${1:-} ${2:-}" == "image inspect" ]]; then
      printf '%s\n' "$transaction_commit"
      return 0
    fi
    return 97
  }
  VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL=vp_deploy_migrator
  VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL=vp_deploy_read
  VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL=vp_control_role_owner
  VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL=vp_runtime_role_owner
  VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE=\
"$TEST_ROOT/transaction-preparation/deploy-migrator"
  VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE=\
"$TEST_ROOT/transaction-preparation/deploy-read"
  VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE=\
"$TEST_ROOT/transaction-preparation/control-owner"
  VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE=\
"$TEST_ROOT/transaction-preparation/runtime-owner"
  for credential in \
    "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
    "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE" \
    "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE"; do
    printf 'postgresql://identity:credential@database/videoprocess\n' \
      >"$credential"
    chmod 0400 "$credential"
  done

  vp_validate_worker_database_identities
  vp_worker_admission_lock_acquire "$transaction_root"
  VP_WORKER_ADMISSION_PREPARED=true
  VP_WORKER_ADMISSION_COMMITTED=true
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=vp-stale-worker
  VP_WORKER_CONTROL_PREPARED=true
  VP_WORKER_CONTROL_GENERATION=c-stale
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=m-stale
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=/stale/managed-state
  VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=stalejanitorservice
  VP_VISION_CUTOVER_JOB_SERVICE_ID=stalevisionservice
  vp_worker_admission_prepare_transaction \
    "$backend_image" "$go_image" "$control_image"
  if [[ "$VP_WORKER_ADMISSION_PREPARED" != false \
    || "$VP_WORKER_ADMISSION_COMMITTED" != false \
    || -n "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" \
    || "$VP_WORKER_CONTROL_PREPARED" != false \
    || -n "$VP_WORKER_CONTROL_GENERATION" \
    || "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != false \
    || "$VP_WORKER_REDIS_MARKER_CANDIDATE_READY" != false \
    || -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
    || -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" \
    || -n "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" \
    || -n "$VP_VISION_CUTOVER_JOB_SERVICE_ID" ]]; then
    echo 'FAIL: transaction preparation retained stale forward context' >&2
    exit 1
  fi
  active="$transaction_root/transactions/active.json"
  [[ -f "$active" && ! -L "$active" ]]
  python3 - "$active" "$transaction_commit" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    transaction = json.load(handle)
if (
    transaction["phase"] != "PREPARING"
    or transaction["forward"]["namespace"] != sys.argv[2]
):
    raise SystemExit("PREPARING transaction was not connected")
PY
  baseline_payload="$TEST_ROOT/transaction-preparation/baseline.json"
  python3 - "$baseline_payload" <<'PY'
import json
import pathlib
import sys

services = [
    "vp-api-swarm",
    "vp-frontend-swarm",
    "vp-autoflow-api-swarm",
    "vp-event-outbox-relay-swarm",
    "vp-channel-agent-runner-swarm",
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
]
payload = {
    "control": None,
    "kind": "legacy_no_control",
    "services": [
        {
            "docker_service_id": None,
            "existed": False,
            "image": None,
            "name": service,
            "spec_digest": None,
        }
        for service in services
    ],
}
pathlib.Path(sys.argv[1]).write_text(
    json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  vp_worker_admission_load_replay_plan
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" capture-baseline \
    "$transaction_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    <"$baseline_payload" >/dev/null
  vp_worker_admission_transition_to FORWARD_APPLYING
  vp_worker_admission_transition_to FORWARD_VERIFIED
  vp_worker_admission_transition_to WORKERS_PROMOTED
  : >"$transaction_calls"
  if vp_worker_admission_prepare_transaction \
    "$backend_image" "$go_image" "$control_image" >/dev/null 2>&1; then
    echo 'FAIL: stage-1 replay minted through WORKERS_PROMOTED' >&2
    exit 1
  fi
  if [[ -s "$transaction_calls" ]]; then
    echo 'FAIL: WORKERS_PROMOTED replay touched Docker before stage 2' >&2
    exit 1
  fi
  vp_worker_admission_lock_release
  : >"$transaction_calls"
  vp_commit_worker_redis_marker_controls() {
    vp_worker_admission_lock_assert
    printf 'replay|marker\n' >>"$transaction_calls"
  }
  vp_commit_worker_control_generation() {
    vp_worker_admission_lock_assert
    printf 'replay|control\n' >>"$transaction_calls"
  }
  vp_worker_admission_retire_transaction() {
    vp_worker_admission_lock_assert
    printf 'replay|retire\n' >>"$transaction_calls"
  }
  vp_worker_admission_hydrate_recovery_context() {
    vp_worker_admission_lock_assert
  }
  vp_worker_admission_promote_phase() {
    vp_worker_admission_lock_assert
    case "$1" in
      PROMOTE_MARKER)
        vp_commit_worker_redis_marker_controls
        vp_worker_admission_transition_to MARKER_PROMOTED
        ;;
      PROMOTE_CONTROL)
        vp_commit_worker_control_generation
        vp_worker_admission_transition_to CONTROL_PROMOTED
        ;;
      *) return 1 ;;
    esac
  }
  vp_validate_deploy_config() {
    vp_worker_admission_lock_assert
    printf 'deploy|validate\n' >>"$transaction_calls"
  }
  vp_worker_admission_prepare_transaction() {
    vp_worker_admission_lock_assert
    printf 'deploy|prepare\n' >>"$transaction_calls"
  }
  _vp_deploy_vp_app_services_locked() {
    vp_worker_admission_lock_assert
    printf 'deploy|apply\n' >>"$transaction_calls"
  }
  deploy_vp_app_services \
    synthetic-pds synthetic-frontend "$backend_image" \
    synthetic-channelops "$go_image" "$control_image" \
    >/dev/null
  expected_stage2_calls=$'replay|marker\nreplay|control\nreplay|retire\ndeploy|validate\ndeploy|prepare\ndeploy|apply'
  if [[ "$(<"$transaction_calls")" != "$expected_stage2_calls" \
    || -e "$active" \
    || "$(find "$transaction_root/transactions" -name done.json -type f | wc -l | tr -d ' ')" -ne 1 ]]; then
    echo 'FAIL: full deploy entry did not reconcile the promoted transaction before a new deployment' >&2
    exit 1
  fi
)

(
  ROOT="$TEST_ROOT/deploy-lock-lifetime/sync"
  REPO_ROOT="$TEST_ROOT/deploy-lock-lifetime/repos"
  mkdir -p "$ROOT"
  lock_observations="$TEST_ROOT/deploy-lock-lifetime/observations"
  : >"$lock_observations"
  vp_validate_deploy_config() {
    vp_worker_admission_lock_assert
    printf 'validate|locked\n' >>"$lock_observations"
  }
  vp_worker_admission_prepare_transaction() {
    vp_worker_admission_lock_assert
    printf 'reconcile|locked\n' >>"$lock_observations"
  }
  _vp_deploy_vp_app_services_locked() {
    vp_worker_admission_lock_assert
    printf 'return|locked\n' >>"$lock_observations"
  }
  deploy_vp_app_services a b c d e f >/dev/null
  [[ "$(<"$lock_observations")" == $'validate|locked\nreconcile|locked\nreturn|locked' ]]
  [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == false ]]
)

vp_probe_worker_database_principal() {
  printf 'database-principal|%s|%s\n' "$1" "$3" >>"$CALLS"
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
if [[ -e "$ROOT/state/vp-worker-admission/transactions/active.json" ]]; then
  echo 'FAIL: successful deployment left an active worker admission transaction' >&2
  exit 1
fi
forward_done="$(
  find "$ROOT/state/vp-worker-admission/transactions" \
    -name done.json -type f -print
)"
python3 - "$forward_done" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
workers = document["forward"]["workers"]
marker = document["forward"]["marker"]
janitor = document["janitor"]["service"]
if (
    document["phase"] != "DONE"
    or document["outcome"] != "succeeded"
    or len(workers) != 4
    or any(worker["applied_stage"] != "verified" for worker in workers)
    or any(
        worker["docker_service_id"] is None
        or worker["target_spec_digest"] is None
        for worker in workers
    )
    or marker is None
    or {reference["purpose"] for reference in marker["secrets"]} != {
        "readiness-database",
        "janitor-database",
        "repair-database",
        "readiness-redis",
        "janitor-redis",
    }
    or janitor is None
    or janitor["name"] != "vp-staging-object-janitor"
    or janitor["docker_service_id"] is None
    or janitor["spec_digest"] is None
):
    raise SystemExit("successful forward identities were not durable")
PY
for worker_contract in \
  vp-ffmpeg-worker-go-swarm:101 \
  "$VP_PYTHON_WORKER_SERVICE:102" \
  "$VP_VISION_WORKER_SERVICE:103" \
  "$VP_PUBLISHER_SERVICE:104"; do
  worker_service="${worker_contract%%:*}"
  worker_generation="${worker_contract##*:}"
  worker_mutation="$(
    grep -E '^docker\|service (create|update) ' "$CALLS" \
      | grep -E "( |logical-service=)$worker_service( |$)" \
      | tail -1
  )"
  if [[ "$worker_mutation" != *"--label-add vp.service=$worker_service"* \
    || "$worker_mutation" \
      != *"--label-add vp.generation=$worker_generation"* \
    || "$worker_mutation" \
      != *'--label-add vp.managed-by=videoprocess-deploy'* ]]; then
    echo "FAIL: $worker_service bypassed the immutable worker mutation contract" >&2
    exit 1
  fi
done
grep -Fq "docker|service ps $VISION_SAFETY_JOB_ID " "$CALLS"
grep -Fq "docker|service ps $VISION_RECONCILE_JOB_ID " "$CALLS"
worker_admission_commit_line="$(
  grep -nF 'worker-admission|commit' "$CALLS" | sed -n '1p' | cut -d: -f1
)"
worker_marker_commit_line="$(
  grep -nF 'worker-marker|commit' "$CALLS" | sed -n '1p' | cut -d: -f1
)"
worker_control_commit_line="$(
  grep -nF 'worker-control|commit' "$CALLS" | sed -n '1p' | cut -d: -f1
)"
if [[ -z "$worker_admission_commit_line" \
  || -z "$worker_marker_commit_line" \
  || -z "$worker_control_commit_line" \
  || "$worker_admission_commit_line" -ge "$worker_marker_commit_line" \
  || "$worker_marker_commit_line" -ge "$worker_control_commit_line" ]]; then
  echo 'FAIL: control generation retired before worker/marker commit' >&2
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
  grep -nF 'log|vision cutover gate verified: CLOSED and idle' "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1 \
    || true
)"
first_service_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | sed -n '1p' \
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
    | sed -n '1p'
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
    | sed -n '1p' \
    | cut -d: -f1
)"
vision_worker_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:deploy-0123456789ab' \
    | grep -F 'vp-vision-worker-swarm' \
    | sed -n '1p' \
    | cut -d: -f1
)"
publisher_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:deploy-0123456789ab' \
    | grep -F 'vp-youtube-publisher-swarm' \
    | sed -n '1p' \
    | cut -d: -f1
)"
python_listener_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-backend-api:deploy-0123456789ab' \
    | grep -F 'vp-autoflow-api-swarm' \
    | sed -n '1p' \
    | cut -d: -f1
)"
if [[ -z "$python_worker_update_line" \
  || -z "$vision_worker_update_line" \
  || -z "$publisher_update_line" \
  || -z "$python_listener_update_line" \
  || "$python_listener_update_line" -ge "$python_worker_update_line" \
  || "$python_listener_update_line" -ge "$vision_worker_update_line" \
  || "$python_listener_update_line" -ge "$publisher_update_line" ]]; then
  echo 'FAIL: migration-capable backend must deploy before registered workers' >&2
  exit 1
fi

legacy_vision_remove_line="$(
  grep -nF "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1
)"
vision_running_line="$(
  grep -nF 'running|vp-vision-worker-swarm' "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1
)"
vision_readiness_probe_line="$(
  grep -nF "$vision_readiness_probe" "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1
)"
vision_consumer_cutover_line="$(
  grep -nF 'log|vision consumer reconciliation verified' "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1 \
    || true
)"
final_vision_safety_job_line="$(
  grep -nF 'docker|service create' "$CALLS" \
    | grep -F -- '--label vp.purpose=final-safety' \
    | sed -n '1p' \
    | cut -d: -f1 \
    || true
)"
final_vision_safety_gate_line="$(
  grep -nF \
    'log|final vision cutover gate verified immediately before retirement' \
    "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1 \
    || true
)"
if [[ -z "$legacy_vision_remove_line" \
  || -z "$vision_running_line" \
  || -z "$vision_readiness_probe_line" \
  || -z "$final_vision_safety_job_line" \
  || -z "$final_vision_safety_gate_line" \
  || -z "$vision_consumer_cutover_line" \
  || "$vision_running_line" -ge "$legacy_vision_remove_line" \
  || "$vision_readiness_probe_line" -ge "$final_vision_safety_job_line" \
  || "$final_vision_safety_job_line" -ge "$final_vision_safety_gate_line" \
  || "$final_vision_safety_gate_line" -ge "$legacy_vision_remove_line" \
  || "$legacy_vision_remove_line" -ge "$vision_consumer_cutover_line" ]]; then
  echo 'FAIL: final vision safety, retirement, and reconciliation order is unsafe' >&2
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
  running_line="$(grep -nF "running|$service" "$CALLS" | sed -n '1p' | cut -d: -f1)"
  placement_line="$(grep -nF "$(worker_service_ps_call "$service")" "$CALLS" | sed -n '1p' | cut -d: -f1)"
  container_line="$(grep -nF "docker|container ls --filter label=com.docker.swarm.service.name=$service" "$CALLS" | sed -n '1p' | cut -d: -f1)"
  exec_line="$(grep -nF "$readiness_probe" "$CALLS" | sed -n '1p' | cut -d: -f1)"
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

deploy_prepared_python_worker_fixture() {
  VP_WORKER_ADMISSION_PREPARED=true \
    VP_WORKER_FFMPEG_GENERATION=102 \
    vp_deploy_python_worker "$@"
}

deploy_prepared_vision_worker_fixture() {
  VP_WORKER_ADMISSION_PREPARED=true \
    VP_WORKER_VISION_GENERATION=103 \
    vp_deploy_vision_worker "$@"
}

deploy_prepared_publisher_fixture() {
  VP_WORKER_ADMISSION_PREPARED=true \
    VP_WORKER_YOUTUBE_PUBLISHER_GENERATION=104 \
    vp_deploy_publisher "$@"
}

deploy_managed_worker_for_gate_test() {
  local service="$1"
  case "$service" in
    "$VP_PYTHON_WORKER_SERVICE")
      deploy_prepared_python_worker_fixture \
        vp-ffmpeg-worker-python:placement-gate-test
      ;;
    "$VP_VISION_WORKER_SERVICE")
      deploy_prepared_vision_worker_fixture \
        vp-ffmpeg-worker-python:placement-gate-test
      ;;
    "$VP_PUBLISHER_SERVICE")
      deploy_prepared_publisher_fixture \
        vp-ffmpeg-worker-python:placement-gate-test
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
  local output="$TEST_ROOT/readiness-$name.out"
  : >"$CALLS"
  GPU_SERVICE_EXISTS=true
  VISION_SERVICE_EXISTS=true
  PUBLISHER_SERVICE_EXISTS=true
  LEGACY_VISION_CONTAINER_EXISTS=true
  if deploy_vp_app_services $images >"$output" 2>&1; then
    echo "FAIL: $name worker storage readiness failure unexpectedly allowed deployment" >&2
    exit 1
  fi
  if ! grep -Fq 'log|VideoProcess service apply failed; restoring prior images with fresh admission' "$CALLS"; then
    echo "FAIL: $name readiness failure did not trigger the existing deployment rollback" >&2
    exit 1
  fi
  assert_readiness_failure_calls_are_safe
  cp "$CALLS" "$TEST_ROOT/readiness-$name.calls"
}

rm -f "$WORKER_READINESS_CONTAINER_CALLS"
WORKER_READINESS_CONTAINER_MODE=missing-then-normal
FAIL_WORKER_READINESS_SERVICE=
assert_deploy_rejected_by_readiness missing-container
readiness_rollback_line="$(
  grep -nF 'log|VideoProcess service apply failed; restoring prior images with fresh admission' \
    "$CALLS" | sed -n '1p' | cut -d: -f1
)"
if sed -n "1,$((readiness_rollback_line - 1))p" "$CALLS" \
  | grep -Eq "docker\|exec\||docker\|container ls --filter label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE|docker\|rm -f $LEGACY_VISION_CONTAINER_ID"; then
  echo 'FAIL: missing readiness container advanced past the GPU deployment gate' >&2
  exit 1
fi

rm -f "$WORKER_READINESS_CONTAINER_CALLS"
WORKER_READINESS_CONTAINER_MODE=duplicate-then-normal
assert_deploy_rejected_by_readiness duplicate-container
readiness_rollback_line="$(
  grep -nF 'log|VideoProcess service apply failed; restoring prior images with fresh admission' \
    "$CALLS" | sed -n '1p' | cut -d: -f1
)"
if sed -n "1,$((readiness_rollback_line - 1))p" "$CALLS" \
  | grep -Eq "docker\|exec\||docker\|container ls --filter label=com.docker.swarm.service.name=$VP_VISION_WORKER_SERVICE|docker\|rm -f $LEGACY_VISION_CONTAINER_ID"; then
  echo 'FAIL: duplicate readiness containers advanced past the GPU deployment gate' >&2
  exit 1
fi

WORKER_READINESS_CONTAINER_MODE=normal
for failed_readiness_service in \
  "$VP_PYTHON_WORKER_SERVICE" \
  "$VP_VISION_WORKER_SERVICE" \
  "$VP_PUBLISHER_SERVICE"; do
  rm -f "$WORKER_READINESS_FAILURE_USED"
  WORKER_READINESS_EXEC_MODE=fail-first
  WORKER_READINESS_FAIL_SERVICE="$failed_readiness_service"
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
      if grep -Eq \
        'docker\\|service update.*vp-(event-outbox-relay|channel-agent-runner)-swarm' \
        "$CALLS"; then
        echo 'FAIL: publisher readiness execution failure advanced to later managed services' >&2
        exit 1
      fi
      ;;
  esac
done
WORKER_READINESS_EXEC_MODE=normal
WORKER_READINESS_FAIL_SERVICE=
rm -f "$WORKER_READINESS_FAILURE_USED"

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
  vp_require_worker_redis_runtime_state
  deploy_vp_app_services $images
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

test_service_update_line_for_image() {
  local service="$1"
  local image="$2"
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- "--image $image" \
    | grep -F " $(test_service_id "$service")" \
    | tail -1 \
    | cut -d: -f1 \
    || true
}

: >"$CALLS"
GPU_SERVICE_EXISTS=true
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
rm -f "$FAKE_NODE_UPDATE_FAILURE_USED"
FAIL_NODE_UPDATE_ONCE=true
gpu_node_failure_contract_failed=false
if deploy_worker_review_fixture gpu-node-label-write-failure \
  >"$TEST_ROOT/gpu-node-label-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU node label update unexpectedly allowed deployment' >&2
  exit 1
fi
gpu_node_update_line="$(
  grep -nF 'docker|node update --label-add vp.gpu=true ccttww-lap' "$CALLS" \
    | sed -n '1p' \
    | cut -d: -f1
)"
gpu_node_rollback_line="$(
  grep -nF 'log|VideoProcess service apply failed; restoring prior images with fresh admission' "$CALLS" \
    | sed -n '1p' \
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
  test_service_update_line_for_image \
    "$VP_PYTHON_WORKER_SERVICE" 'baseline-vp-ffmpeg-worker-gpu-swarm:stable'
)"
if [[ -z "$gpu_node_baseline_line" || "$gpu_node_rollback_line" -ge "$gpu_node_baseline_line" ]]; then
  echo 'FAIL: GPU node label failure did not explicitly restore the baseline' >&2
  exit 1
fi
assert_worker_gate_sequence_after \
  "$VP_PYTHON_WORKER_SERVICE" "$gpu_readiness_probe" "$gpu_node_baseline_line"
FAIL_NODE_UPDATE_ONCE=false
rm -f "$FAKE_NODE_UPDATE_FAILURE_USED"

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
deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:gpu-placement-normalization \
  >/dev/null
gpu_normalized_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-ffmpeg-worker-python:gpu-placement-normalization' \
    | grep -F "$VP_PYTHON_WORKER_SERVICE" \
    | sed -n '1p'
)"
if [[ -z "$gpu_normalized_update" ]]; then
  echo 'FAIL: normal GPU placement fixture did not issue a service update' >&2
  exit 1
fi
assert_gpu_constraints_normalized "$gpu_normalized_update"
assert_rollback_targets_are_safe

: >"$CALLS"
CONSTRAINT_MODE=gpu-duplicate
if deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:gpu-duplicate-constraint \
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
if deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:gpu-constraint-inspect-failure \
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
  test_service_update_line_for_image \
    "$VP_PYTHON_WORKER_SERVICE" 'baseline-vp-ffmpeg-worker-gpu-swarm:stable'
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
FAIL_UPDATE_IMAGE="vp-ffmpeg-worker-python:deploy-${TEST_COMMIT:0:12}"
if deploy_worker_review_fixture gpu-update-write-failure >"$TEST_ROOT/gpu-update-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU update was masked by an old healthy task' >&2
  exit 1
fi
gpu_failed_update_call="$(grep -F 'docker|service update' "$CALLS" \
  | grep -F -- "--image vp-ffmpeg-worker-python:deploy-${TEST_COMMIT:0:12}" \
  | grep -F "$VP_PYTHON_WORKER_SERVICE" \
  | sed -n '1p')"
if [[ -z "$gpu_failed_update_call" ]]; then
  echo 'FAIL: GPU update failure fixture did not issue the attempted image update' >&2
  exit 1
fi
gpu_attempt_line="$(grep -nF "$gpu_failed_update_call" "$CALLS" | sed -n '1p' | cut -d: -f1)"
grep -Fq 'log|VideoProcess service apply failed; restoring prior images with fresh admission' "$CALLS"
gpu_baseline_line="$(
  test_service_update_line_for_image \
    "$VP_PYTHON_WORKER_SERVICE" 'baseline-vp-ffmpeg-worker-gpu-swarm:stable'
)"
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
test_mark_worker_service_absent "$VP_PYTHON_WORKER_SERVICE"
VISION_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
LEGACY_VISION_CONTAINER_EXISTS=true
FAIL_GPU_CREATE=true
if deploy_worker_review_fixture gpu-create-write-failure >"$TEST_ROOT/gpu-create-write-failure.out" 2>&1; then
  echo 'FAIL: failed GPU create was masked by a healthy task' >&2
  exit 1
fi
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq 'log|VideoProcess service apply failed; restoring prior images with fresh admission' "$CALLS"
if ! grep -Fq \
    "docker|service rm $(test_service_id "$VP_PYTHON_WORKER_SERVICE")" \
    "$CALLS"; then
  echo 'FAIL: failed GPU create rollback did not remove the immutable service ID' >&2
  command cat "$TEST_ROOT/gpu-create-write-failure.out" >&2
  exit 1
fi
if grep -Fq "docker|service rm $VP_PYTHON_WORKER_SERVICE" "$CALLS"; then
  echo 'FAIL: failed GPU create rollback deleted by mutable service name' >&2
  exit 1
fi
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
  baseline_line="$(
    test_service_update_line_for_image "$service" "baseline-$service:stable"
  )"
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

preserve_persistent_rollback_transaction() {
  local service="$1"
  local transactions="$ROOT/state/vp-worker-admission/transactions"
  local active="$transactions/active.json"
  local evidence_root="$TEST_ROOT/persistent-rollback-transactions/$service"
  local transaction_id
  transaction_id="$(
    python3 -I - "$active" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
if (
    document.get("schema") != 3
    or document.get("phase") != "ROLLBACK_APPLYING"
    or document.get("outcome") is not None
    or document.get("baseline", {}).get("captured") is not True
    or document.get("failed_forward", {}).get("captured") is not True
    or re.fullmatch(r"tx-[0-9a-f]{32}", document.get("transaction_id", ""))
    is None
):
    raise SystemExit(1)
print(document["transaction_id"])
PY
  )" || return 1
  [[ -d "$transactions/$transaction_id" \
    && ! -L "$transactions/$transaction_id" \
    && ! -e "$evidence_root" ]] || return 1
  mkdir -p "$evidence_root" || return 1
  mv "$active" "$evidence_root/active.json" || return 1
  mv "$transactions/$transaction_id" "$evidence_root/$transaction_id"
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
  baseline_line="$(
    test_service_update_line_for_image \
      "$rollback_service" "baseline-$rollback_service:stable"
  )"
  if [[ -z "$baseline_line" \
    || "$(grep -F "$readiness_probe" "$CALLS" | wc -l | tr -d ' ')" -lt 2 ]]; then
    echo "FAIL: persistent $rollback_service failure did not attempt and verify baseline restore" >&2
    exit 1
  fi
  assert_worker_gate_sequence_after "$rollback_service" "$readiness_probe" "$baseline_line"
  grep -Fq 'VideoProcess image restore did not fully converge' "$rollback_output"
  assert_rollback_targets_are_safe
  preserve_persistent_rollback_transaction "$rollback_service"
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

migration_run_line="$(
  grep -nF 'docker|run --rm' "$CALLS" \
    | grep -F 'python -m app.services.worker_deployment_cli migrate' \
    | sed -n '1p' \
    | cut -d: -f1
)"
backend_migration_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F -- '--image vp-backend-api:deploy-0123456789ab' \
    | grep -F 'vp-autoflow-api-swarm' \
    | sed -n '1p' \
    | cut -d: -f1
)"
migration_gate_line="$(
  grep -nF 'docker|run --rm' "$CALLS" \
    | grep -F 'python -m app.services.worker_deployment_cli verify-head' \
    | sed -n '1p' \
    | cut -d: -f1
)"
runner_update_line="$(
  grep -nF 'docker|service update' "$CALLS" \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | grep -F -- '--image vp-channelops-runner-go:deploy-0123456789ab' \
    | sed -n '1p' \
    | cut -d: -f1
)"
if [[ -z "$migration_run_line" \
  || -z "$backend_migration_update_line" \
  || -z "$migration_gate_line" \
  || -z "$runner_update_line" \
  || "$migration_run_line" -ge "$migration_gate_line" \
  || "$migration_gate_line" -ge "$backend_migration_update_line" \
  || "$migration_gate_line" -ge "$runner_update_line" ]]; then
  echo 'FAIL: migration and exact head gate must precede backend/runner updates' >&2
  exit 1
fi
migration_gate_call="$(
  grep -F 'docker|run --rm' "$CALLS" \
    | grep -F 'python -m app.services.worker_deployment_cli verify-head' \
    | sed -n '1p'
)"
migration_run_call="$(
  grep -F 'docker|run --rm' "$CALLS" \
    | grep -F 'python -m app.services.worker_deployment_cli migrate' \
    | sed -n '1p'
)"
migration_mount_source="$(
  python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' \
    "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE"
)"
read_mount_source="$(
  python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
)"
if [[ "$migration_run_call" != \
    *"--mount type=bind,src=$migration_mount_source,dst=/run/secrets/worker-deploy-migrator-database-url,readonly"* \
  || "$migration_run_call" != \
    *"--env WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE=/run/secrets/worker-deploy-migrator-database-url"* \
  || "$migration_gate_call" != \
    *"--mount type=bind,src=$read_mount_source,dst=/run/secrets/worker-deploy-read-database-url,readonly"* \
  || "$migration_gate_call" != \
    *"--env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url"* \
  || "$migration_run_call$migration_gate_call" == *'postgresql://'* \
  || "$migration_run_call$migration_gate_call" == *'redis://'* \
  || "$migration_run_call$migration_gate_call" == \
    *'WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE'* \
  || "$migration_run_call$migration_gate_call" == \
    *'WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE'* ]]; then
  echo 'FAIL: migration/read gate did not use isolated secret-file mounts' >&2
  exit 1
fi

cp "$CALLS" "$TEST_ROOT/successful-deploy-calls"
for migration_gate_mode in wrong missing error; do
  : >"$CALLS"
  MIGRATION_GATE_MODE="$migration_gate_mode"
  if deploy_vp_app_services $images >/dev/null 2>&1; then
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
  if ! grep -Fq \
    'python -m app.services.worker_deployment_cli verify-head' "$CALLS"; then
    echo "FAIL: $migration_gate_mode migration gate did not run the verifier" >&2
    exit 1
  fi
done
MIGRATION_GATE_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

: >"$CALLS"
MIGRATION_RUN_MODE=failure
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: failed protected migration unexpectedly succeeded' >&2
  exit 1
fi
if grep -Fq \
  'python -m app.services.worker_deployment_cli verify-head' "$CALLS" \
  || grep -F 'docker|service update' "$CALLS" \
    | grep -Eq ' vp-autoflow-api-swarm$'; then
  echo 'FAIL: failed migration reached head verification or backend update' >&2
  exit 1
fi
MIGRATION_RUN_MODE=success
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
  || grep -F 'python -m app.services.vision_consumer_cutover' "$CALLS" \
    | grep -Fvq -- '--safety'; then
  echo 'FAIL: unsafe vision cutover gate mutated services or the legacy worker' >&2
  exit 1
fi
grep -Fq \
  'python -m app.services.vision_consumer_cutover --safety' "$CALLS"
grep -Fq "docker|service rm $VISION_SAFETY_JOB_ID" "$CALLS"
VISION_CUTOVER_GATE_MODE=success
cp "$TEST_ROOT/successful-deploy-calls" "$CALLS"

: >"$CALLS"
LEGACY_VISION_CONTAINER_EXISTS=true
VISION_FINAL_CUTOVER_GATE_MODE=unsafe
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: stale final vision safety gate unexpectedly allowed retirement' >&2
  exit 1
fi
grep -Fq -- '--label vp.purpose=final-safety' "$CALLS"
if grep -Fq "docker|rm -f $LEGACY_VISION_CONTAINER_ID" "$CALLS" \
  || grep -Fq 'log|vision consumer reconciliation verified' "$CALLS"; then
  echo 'FAIL: failed final vision safety gate retired or reconciled consumers' >&2
  exit 1
fi
VISION_FINAL_CUTOVER_GATE_MODE=success
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
grep -Fq 'python -m app.services.vision_consumer_cutover --safety' "$CALLS"

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
test_mark_worker_service_absent "$VP_VISION_WORKER_SERVICE"
vp_require_worker_redis_runtime_state
if deploy_vp_app_services $images \
  >"$TEST_ROOT/missing-vision-service.out" 2>&1; then
  echo 'FAIL: missing managed vision service bypassed the cutover gate' >&2
  exit 1
fi
grep -Fq \
  'python -m app.services.vision_consumer_cutover --safety' "$CALLS"
if grep -Fq 'docker|service update' "$CALLS" \
  || grep -F 'docker|service create' "$CALLS" \
    | grep -Fvq -- '--label vp.service=vision-cutover'; then
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

grep -Fq \
  "remote|10.0.0.127|/bin/sh|-s|--|/Users/wenjieliu/VideoProcess-app|backend/Dockerfile.ffmpeg-worker-go|vp-ffmpeg-worker-go:deploy-0123456789ab|$TEST_COMMIT" \
  "$CALLS"
if grep -Fq \
  'build|10.0.0.127|/Users/wenjieliu/VideoProcess-app|backend/Dockerfile.ffmpeg-worker-go' \
  "$CALLS"; then
  echo 'FAIL: versioned Go worker used the four-argument build helper' >&2
  exit 1
fi
grep -Fq 'docker|build --build-arg VP_BUILD_COMMIT=0123456789abcdef0123456789abcdef01234567 -f /home/taiwei/deploy-github-sync/repos/videoprocess/backend/Dockerfile.worker -t vp-ffmpeg-worker-python:deploy-0123456789ab /home/taiwei/deploy-github-sync/repos/videoprocess/backend' "$CALLS"
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
  | sed -n '1p' \
  | cut -d: -f1)"
publisher_mount_add_line="$(grep -nF 'docker|service update' "$CALLS" \
  | grep -F 'vp-youtube-publisher-swarm' \
  | grep -F -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' \
  | sed -n '1p' \
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
if grep -Eq -- '--env-add MINIO_(ACCESS|SECRET)_KEY=' "$CALLS"; then
  echo 'FAIL: worker deployment retained MinIO credentials in service env' >&2
  exit 1
fi
grep -Fq -- \
  '--secret-add source=test-worker-minio-access,target=vp-worker-minio-access-key,mode=0400' \
  "$CALLS"
grep -Fq -- \
  '--secret-add source=test-worker-minio-secret,target=vp-worker-minio-secret-key,mode=0400' \
  "$CALLS"

publisher_calls="$(grep -F 'vp-youtube-publisher-swarm' "$CALLS" || true)"
if printf '%s\n' "$publisher_calls" | grep -Eq -- 'YOUTUBE_(OAUTH|CLIENT|CREDENTIALS|TOKEN|REFRESH)_[A-Z_]*='; then
  echo 'FAIL: publisher deploy must not add OAuth credential environments' >&2
  exit 1
fi
if printf '%s\n' "$publisher_calls" | grep -Eq -- '--mount(-add)? .*youtube_credentials'; then
  echo 'FAIL: publisher deploy must not add a credentials mount' >&2
  exit 1
fi

publisher_health_line="$(grep -nF 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS" | sed -n '1p' | cut -d: -f1)"
publisher_update_line="$(grep -nF 'vp-youtube-publisher-swarm' "$CALLS" | grep -F 'docker|service update' | sed -n '1p' | cut -d: -f1)"
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
grep -Fq \
  "docker|service update --detach=false --no-resolve-image --update-order start-first --image vp-frontend:repeat-test $(test_service_id vp-frontend-swarm)" \
  "$CALLS"
CONSTRAINT_MODE=legacy

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
if ! deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-repeat-test >/dev/null 2>>"$CALLS"; then
  echo 'FAIL: repeat publisher update returned non-zero' >&2
  exit 1
fi
if grep -Fq 'unbound variable' "$CALLS"; then
  echo 'FAIL: repeat publisher update is not compatible with Bash 3.2 set -u' >&2
  exit 1
fi
grep -Fq 'docker|service update --detach=false --no-resolve-image --update-order stop-first' "$CALLS"
grep -Fq -- \
  "--image vp-ffmpeg-worker-python:publisher-repeat-test $(test_service_id vp-youtube-publisher-swarm)" \
  "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
if grep -Fq -- '--constraint-add node.labels.vp.publisher==true' "$CALLS" \
  || grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS" \
  || grep -Fq -- '--network-add vp-pipeline-network-id' "$CALLS" \
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
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-health-test >/dev/null 2>&1; then
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
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: publisher list daemon failure unexpectedly allowed deployment' >&2
  exit 1
fi
if ! grep -Fq 'docker|service ls --filter name=vp-youtube-publisher-swarm --format {{.Name}}' "$CALLS"; then
  echo 'FAIL: optional publisher snapshot must use an exact service list probe' >&2
  exit 1
fi
if grep -Fq \
  "docker|service rm $(test_service_id "$VP_PUBLISHER_SERVICE")" "$CALLS" \
  || grep -Fq "docker|service rm $VP_PUBLISHER_SERVICE" "$CALLS" \
  || grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: publisher list daemon failure must not omit, create, or delete the existing publisher' >&2
  exit 1
fi
PUBLISHER_LIST_FAILURE=false

: >"$CALLS"
PUBLISHER_LIST_NAME=vp-youtube-publisher-swarm-stale
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-list-name-test >/dev/null 2>&1; then
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
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-config-inspect-test >/dev/null 2>&1; then
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
test_mark_worker_service_absent "$VP_PYTHON_WORKER_SERVICE"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_LIST_FAILURE=true
if vp_restore_app_snapshots "" >/dev/null 2>&1; then
  echo 'FAIL: publisher rollback removal accepted a list daemon failure' >&2
  exit 1
fi
if grep -Fq \
  "docker|service rm $(test_service_id "$VP_PUBLISHER_SERVICE")" "$CALLS" \
  || grep -Fq "docker|service rm $VP_PUBLISHER_SERVICE" "$CALLS"; then
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
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-node-failure-test >/dev/null 2>&1; then
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
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-network-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when network inspection fails' >&2
  exit 1
fi
grep -Fq \
  'docker|network inspect vp-pipeline-net --format {{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}' \
  "$CALLS"
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after pipeline network inspection failure' >&2
  exit 1
fi
FAIL_NETWORK_INSPECT=false

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
test_mark_worker_service_absent "$VP_PUBLISHER_SERVICE"
FAIL_PUBLISHER_CREATE=true
if deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-create-failure-test >/dev/null 2>&1; then
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
FAIL_UPDATE_IMAGE="vp-ffmpeg-worker-python:deploy-${TEST_COMMIT:0:12}"
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: failed publisher update with an old running service unexpectedly succeeded' >&2
  exit 1
fi
if [[ -z "$(
  test_service_update_line_for_image \
    "$VP_PUBLISHER_SERVICE" 'baseline-vp-youtube-publisher-swarm:stable'
)" ]]; then
  echo 'FAIL: publisher update failure did not restore the baseline image' >&2
  exit 1
fi
if [[ "$PUBLISHER_MOUNT_MODE" != desired || "$PUBLISHER_REPLICAS" -ne 1 ]]; then
  echo 'FAIL: publisher rollback did not recover one replica with the desired scratch mount' >&2
  exit 1
fi
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

VP_GPU_RUNTIME_READY=true
GPU_PREFLIGHT_SUCCEEDS=false
if deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:gpu-preflight-test >/dev/null 2>&1; then
  echo 'FAIL: requested GPU mode must fail when the runtime preflight fails' >&2
  exit 1
fi
grep -Fq 'docker|run --rm --gpus all vp-ffmpeg-worker-python:gpu-preflight-test nvidia-smi' "$CALLS"
GPU_PREFLIGHT_SUCCEEDS=true
if deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:gpu-swarm-allocation-test \
  >/dev/null 2>&1; then
  echo 'FAIL: GPU mode must remain disabled until Swarm task allocation is configured' >&2
  exit 1
fi
VP_GPU_RUNTIME_READY=false

: >"$CALLS"
GPU_SERVICE_EXISTS=true
FAIL_UPDATE_SERVICE=vp-channel-agent-runner-swarm
FAIL_UPDATE_IMAGE="vp-channelops-runner-go:deploy-${TEST_COMMIT:0:12}"
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: injected service update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- \
  "--image baseline-vp-api-swarm:stable $(test_service_id vp-api-swarm)" \
  "$CALLS"
grep -Fq -- \
  "--image baseline-vp-channel-agent-runner-swarm:stable $(test_service_id vp-channel-agent-runner-swarm)" \
  "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
runner_rollback_call="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--image baseline-vp-channel-agent-runner-swarm:stable' \
    | grep -F 'vp-channel-agent-runner-swarm' \
    | sed -n '1p'
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
test_mark_worker_service_absent "$VP_PYTHON_WORKER_SERVICE"
FAIL_RUNNING_SERVICE=vp-ffmpeg-worker-gpu-swarm
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: injected new-worker health failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq \
  "docker|service rm $(test_service_id "$VP_PYTHON_WORKER_SERVICE")" \
  "$CALLS"
if grep -Fq "docker|service rm $VP_PYTHON_WORKER_SERVICE" "$CALLS"; then
  echo 'FAIL: new GPU rollback deleted by mutable service name' >&2
  exit 1
fi
FAIL_RUNNING_SERVICE=

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
rm -f "$FAKE_RUNNING_FAILURE_USED"
FAIL_RUNNING_SERVICE_ONCE=vp-youtube-publisher-swarm
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: existing publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
if [[ -z "$(
  test_service_update_line_for_image \
    "$VP_PUBLISHER_SERVICE" 'baseline-vp-youtube-publisher-swarm:stable'
)" ]]; then
  echo 'FAIL: existing publisher convergence failure did not restore the baseline image' >&2
  exit 1
fi
FAIL_RUNNING_SERVICE_ONCE=
rm -f "$FAKE_RUNNING_FAILURE_USED"

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=false
test_mark_worker_service_absent "$VP_PUBLISHER_SERVICE"
rm -f "$FAKE_RUNNING_FAILURE_USED"
FAIL_RUNNING_SERVICE_ONCE=vp-youtube-publisher-swarm
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: new publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq \
  "docker|service rm $(test_service_id "$VP_PUBLISHER_SERVICE")" \
  "$CALLS"
if grep -Fq "docker|service rm $VP_PUBLISHER_SERVICE" "$CALLS"; then
  echo 'FAIL: new publisher rollback deleted by mutable service name' >&2
  exit 1
fi
FAIL_RUNNING_SERVICE_ONCE=
rm -f "$FAKE_RUNNING_FAILURE_USED"

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=stale
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-placement-test >/dev/null
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
deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-missing-scratch-test >/dev/null
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
    | grep -F -- "--image vp-pds:health-gated-test $(test_service_id vp-pds-swarm)" \
    | sed -n '1p'
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
pds_node_line="$(grep -nF 'docker|service ps vp-pds-swarm' "$CALLS" | sed -n '1p' | cut -d: -f1)"
pds_remote_line="$(grep -nF 'remote|10.0.0.127|/bin/sh|-s|--|vp-pds-swarm' "$CALLS" | sed -n '1p' | cut -d: -f1)"
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
    | grep -F -- "--image vp-pds:rollback-test $(test_service_id vp-pds-swarm)" \
    | sed -n '1p'
)"
rollback_pds_update="$(
  grep -F 'docker|service update' "$CALLS" \
    | grep -F -- '--update-order stop-first' \
    | grep -F -- "--image baseline-vp-pds-swarm:stable $(test_service_id vp-pds-swarm)" \
    | sed -n '1p'
)"
if [[ "$candidate_pds_update" != *'--health-cmd  --health-interval 10s'* \
  || "$rollback_pds_update" != *'--health-cmd  --health-interval 10s'* ]]; then
  echo 'FAIL: PDS candidate and rollback must retain the health contract' >&2
  exit 1
fi
grep -Fq -- \
  "--image baseline-vp-pds-swarm:stable $(test_service_id vp-pds-swarm)" \
  "$CALLS"
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
grep -Fq -- \
  "--image baseline-vp-pds-swarm:stable $(test_service_id vp-pds-swarm)" \
  "$CALLS"

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
grep -Fq -- \
  "--image baseline-vp-pds-swarm:stable $(test_service_id vp-pds-swarm)" \
  "$CALLS"
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
  grep -nF -- \
    "--image $FAIL_UPDATE_IMAGE $(test_service_id vp-feature-aggregator-swarm)" \
    "$CALLS" \
    | cut -d: -f1
)"
rollback_feature_update_line="$(
  grep -nF -- \
    "--image baseline-vp-feature-aggregator-swarm:stable $(test_service_id vp-feature-aggregator-swarm)" \
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
test_mark_worker_service_absent "$VP_PYTHON_WORKER_SERVICE"
deploy_prepared_python_worker_fixture vp-ffmpeg-worker-python:deploy-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi

: >"$CALLS"
VISION_SERVICE_EXISTS=false
test_mark_worker_service_absent "$VP_VISION_WORKER_SERVICE"
deploy_prepared_vision_worker_fixture vp-ffmpeg-worker-python:vision-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-vision-worker-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-network-id' "$CALLS"
grep -Fq -- '--mount type=volume,src=vp-vision-worker-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--env WORKER_TYPE=vision' "$CALLS"
grep -Fq -- '--env WORKER_HOST=150-vision' "$CALLS"

: >"$CALLS"
VISION_SERVICE_EXISTS=true
VISION_TASK_NODE=CASPERs-Mac-mini
if deploy_prepared_vision_worker_fixture vp-ffmpeg-worker-python:vision-forbidden-node-test \
  >/dev/null 2>&1; then
  echo 'FAIL: vision worker placement on 126 unexpectedly passed verification' >&2
  exit 1
fi
grep -Fq 'docker|service ps vp-vision-worker-swarm' "$CALLS"
VISION_TASK_NODE=ccttww-lap

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
test_mark_worker_service_absent "$VP_PUBLISHER_SERVICE"
deploy_prepared_publisher_fixture vp-ffmpeg-worker-python:publisher-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-network-id' "$CALLS"
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

(
  ROOT="$TEST_ROOT/stage2-vision-jobs/sync"
  REPO_ROOT="$TEST_ROOT/stage2-vision-jobs/repos"
  mkdir -p "$ROOT"
  source "$EXTENSION"
  vision_calls="$TEST_ROOT/stage2-vision-jobs/calls"
  vision_state="$TEST_ROOT/stage2-vision-jobs/service-state"
  : >"$vision_calls"
  rm -f "$vision_state"
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  VP_WORKER_ADMISSION_TRANSACTION_ID=tx-0123456789abcdef0123456789abcdef
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$TEST_COMMIT"
  VP_WORKER_REDIS_WATCHER_SECRET=vp-watcher-runtime
  VP_WORKER_REDIS_WATCHER_SECRET_ID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  VP_WORKER_REDIS_CONTROL_SECRET=vp-control-runtime
  VP_WORKER_REDIS_CONTROL_SECRET_ID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$TEST_ROOT/stage2-vision-jobs/deploy-read"
  printf '%s\n' 'postgresql://vision-read:sentinel@database/videoprocess' \
    >"$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  chmod 0400 "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  VISION_JOB_EXIT=0
  VISION_JOURNAL_MODE=
  VISION_JOURNAL_STATE=
  VISION_JOURNAL_SERVICE_ID=-
  VISION_JOURNAL_EXIT=-
  WATCHER_LIVE_ID="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
  CONTROL_LIVE_ID="$VP_WORKER_REDIS_CONTROL_SECRET_ID"

  vp_require_pipeline_network_identity() {
    VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  }
  vp_worker_admission_database_credential_file() {
    printf '%s\n' "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE"
  }
  vp_worker_admission_create_secret() {
    printf 'secret-create|%s|%s|%s|%s|%s\n' "$@" >>"$vision_calls"
    VP_WORKER_CREATED_SECRET_ID=cccccccccccccccccccccccccccccccc
  }
  vp_remove_managed_secret_if_absent_exact() {
    printf 'secret-rm|%s|%s|%s|%s|%s\n' "$@" >>"$vision_calls"
  }
  vp_worker_admission_prepare_vision_job() {
    printf 'journal-plan|%s|%s|%s|%s|%s|%s\n' "$@" >>"$vision_calls"
    if [[ "$VISION_JOURNAL_MODE" != "$1" ]]; then
      VISION_JOURNAL_MODE="$1"
      VISION_JOURNAL_STATE=planned
      VISION_JOURNAL_SERVICE_ID=-
      VISION_JOURNAL_EXIT=-
    fi
  }
  vp_worker_admission_load_vision_job() {
    VP_VISION_JOB_NAME="$VISION_EXPECTED_JOB_NAME"
    VP_VISION_JOB_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
    VP_VISION_JOB_SERVICE_ID="$VISION_JOURNAL_SERVICE_ID"
    VP_VISION_JOB_STATE="$VISION_JOURNAL_STATE"
    VP_VISION_JOB_EXIT_CODE="$VISION_JOURNAL_EXIT"
    if [[ "$1" == reconcile ]]; then
      VP_VISION_JOB_REDIS_SECRET_NAME="$VP_WORKER_REDIS_CONTROL_SECRET"
      VP_VISION_JOB_REDIS_SECRET_ID="$VP_WORKER_REDIS_CONTROL_SECRET_ID"
    else
      VP_VISION_JOB_REDIS_SECRET_NAME="$VP_WORKER_REDIS_WATCHER_SECRET"
      VP_VISION_JOB_REDIS_SECRET_ID="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
    fi
    if [[ "$1" == safety || "$1" == final-safety ]]; then
      if [[ "$1" == final-safety ]]; then
        VP_VISION_JOB_DATABASE_SECRET_NAME=vp-vision-cutover-final-read-db-0123456789ab
      else
        VP_VISION_JOB_DATABASE_SECRET_NAME=vp-vision-cutover-read-db-0123456789ab
      fi
      VP_VISION_JOB_DATABASE_SECRET_ID=cccccccccccccccccccccccccccccccc
    else
      VP_VISION_JOB_DATABASE_SECRET_NAME=-
      VP_VISION_JOB_DATABASE_SECRET_ID=-
    fi
  }
  vp_worker_admission_record_vision_job_service() {
    printf 'journal-created|%s|%s\n' "$@" >>"$vision_calls"
    VISION_JOURNAL_SERVICE_ID="$2"
    VISION_JOURNAL_STATE=created
  }
  vp_worker_admission_record_vision_job_terminal() {
    printf 'journal-terminal|%s|%s\n' "$@" >>"$vision_calls"
    VISION_JOURNAL_EXIT="$2"
    VISION_JOURNAL_STATE=terminal
  }
  vp_worker_admission_complete_vision_job_removal() {
    printf 'journal-removed|%s\n' "$1" >>"$vision_calls"
    VISION_JOURNAL_STATE=removed
  }
  vp_worker_admission_abort_vision_job_removal() {
    printf 'journal-abort-removed|%s\n' "$1" >>"$vision_calls"
    VISION_JOURNAL_EXIT=255
    VISION_JOURNAL_STATE=removed
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$vision_calls"
    if [[ "${1:-} ${2:-}" == 'secret inspect' ]]; then
      case "${3:-}" in
        "$VP_WORKER_REDIS_WATCHER_SECRET_ID"|"$VP_WORKER_REDIS_WATCHER_SECRET")
          printf '%s|%s\n' "$WATCHER_LIVE_ID" "$VP_WORKER_REDIS_WATCHER_SECRET"
          ;;
        "$VP_WORKER_REDIS_CONTROL_SECRET_ID"|"$VP_WORKER_REDIS_CONTROL_SECRET")
          printf '%s|%s\n' "$CONTROL_LIVE_ID" "$VP_WORKER_REDIS_CONTROL_SECRET"
          ;;
        *) return 1 ;;
      esac
      return
    fi
    if [[ "${1:-} ${2:-}" == 'container ls' ]]; then
      return 0
    fi
    if [[ "${1:-} ${2:-}" == 'service create' ]]; then
      printf '%s\n' dddddddddddddddddddddddddddddddd >"$vision_state"
      printf '%s\n' dddddddddddddddddddddddddddddddd
      return
    fi
    if [[ "${1:-} ${2:-}" == 'service ls' ]]; then
      if [[ -f "$vision_state" ]]; then
        printf '%s|%s\n' \
          dddddddddddddddddddddddddddddddd \
          "$VISION_EXPECTED_JOB_NAME"
      fi
      return
    fi
    if [[ "${1:-} ${2:-}" == 'service inspect' ]]; then
      if [[ "${3:-}" == "$VP_VISION_WORKER_SERVICE" ]]; then
        return 0
      fi
      [[ -f "$vision_state" ]] || return 1
      if [[ "$*" == *'{{json .Spec}}'* ]]; then
        local database_secret_json=""
        local database_env_json=""
        if [[ "$VISION_EXPECTED_MODE" == safety \
          || "$VISION_EXPECTED_MODE" == final-safety ]]; then
          database_secret_json=',{"SecretID":"cccccccccccccccccccccccccccccccc","File":{"Name":"vision-cutover-database-url","UID":"10001","GID":"10001","Mode":256}}'
          database_env_json=',"VISION_CUTOVER_DATABASE_URL_FILE=/run/secrets/vision-cutover-database-url"'
        fi
        local redis_id="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
        local cli_json=''
        if [[ "$VISION_EXPECTED_MODE" == reconcile ]]; then
          redis_id="$VP_WORKER_REDIS_CONTROL_SECRET_ID"
        elif [[ "$VISION_EXPECTED_MODE" == safety \
          || "$VISION_EXPECTED_MODE" == final-safety ]]; then
          cli_json=',"--safety"'
        else
          cli_json=',"--check-only"'
        fi
        printf '{"Name":"%s","Labels":{"vp.service":"vision-cutover","vp.generation":"%s","vp.purpose":"%s"},"Mode":{"ReplicatedJob":{"TotalCompletions":1,"MaxConcurrent":1}},"TaskTemplate":{"ContainerSpec":{"Image":"vp-ffmpeg-worker-python:deploy-0123456789ab","Args":["python","-m","app.services.vision_consumer_cutover"%s],"Env":["VISION_CUTOVER_REDIS_URL_FILE=/run/secrets/vision-cutover-redis-url"%s],"Secrets":[{"SecretID":"%s","File":{"Name":"vision-cutover-redis-url","UID":"10001","GID":"10001","Mode":256}}%s]},"RestartPolicy":{"Condition":"none"},"Placement":{"Constraints":["node.hostname==ccttww-lap"]},"Networks":[{"Target":"vp-pipeline-network-id"}]}}\n' \
          "$VISION_EXPECTED_JOB_NAME" \
          "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
          "$VISION_EXPECTED_MODE" "$cli_json" "$database_env_json" \
          "$redis_id" "$database_secret_json"
      elif [[ "$*" == *'{{.ID}}|{{.Spec.Name}}'* ]]; then
        printf '%s|%s|vision-cutover|%s|%s\n' \
          dddddddddddddddddddddddddddddddd \
          "$VISION_EXPECTED_JOB_NAME" \
          "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
          "$VISION_EXPECTED_MODE"
      fi
      return
    fi
    if [[ "${1:-} ${2:-}" == 'service ps' ]]; then
      printf '%s|Shutdown|Complete 1 second ago\n' \
        eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
      return
    fi
    if [[ "${1:-}" == inspect \
      && "${2:-}" == eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ]]; then
      printf '%s\n' "$VISION_JOB_EXIT"
      return
    fi
    if [[ "${1:-} ${2:-}" == 'service logs' ]]; then
      return 0
    fi
    if [[ "${1:-} ${2:-}" == 'service rm' ]]; then
      [[ "${3:-}" == dddddddddddddddddddddddddddddddd ]] || return 1
      rm -f "$vision_state"
      return
    fi
    return 1
  }

  VISION_EXPECTED_MODE=safety
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-safety-0123456789ab
  vp_run_vision_cutover_job \
    safety vp-ffmpeg-worker-python:deploy-0123456789ab
  plan_line="$(grep -n '^journal-plan|safety|' "$vision_calls" | cut -d: -f1)"
  create_line="$(grep -n '^docker|service create' "$vision_calls" | cut -d: -f1)"
  terminal_line="$(grep -n '^journal-terminal|safety|0$' "$vision_calls" | cut -d: -f1)"
  remove_line="$(grep -n '^docker|service rm ddddd' "$vision_calls" | cut -d: -f1)"
  if [[ -z "$plan_line" || -z "$create_line" || -z "$terminal_line" \
    || -z "$remove_line" || "$plan_line" -ge "$create_line" \
    || "$terminal_line" -ge "$remove_line" ]]; then
    echo 'FAIL: vision job Docker mutation is not write-ahead journaled' >&2
    exit 1
  fi
  safety_create="$(grep -F 'docker|service create' "$vision_calls")"
  if [[ "$safety_create" != *'--mode replicated-job'* \
    || "$safety_create" != *'--constraint node.hostname==ccttww-lap'* \
    || "$safety_create" != *'--network vp-pipeline-network-id'* \
    || "$safety_create" != *'source=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,target=vision-cutover-redis-url'* \
    || "$safety_create" != *'source=cccccccccccccccccccccccccccccccc,target=vision-cutover-database-url'* \
    || "$safety_create" != *'VISION_CUTOVER_REDIS_URL_FILE=/run/secrets/vision-cutover-redis-url'* \
    || "$safety_create" != *'VISION_CUTOVER_DATABASE_URL_FILE=/run/secrets/vision-cutover-database-url'* \
    || "$safety_create" != *'app.services.vision_consumer_cutover --safety'* ]]; then
    echo 'FAIL: vision safety job transport is not exact-ID/file-only' >&2
    exit 1
  fi
  if grep -Eq '(^|[ |])(DATABASE_URL|REDIS_URL)=|redis://|postgres(ql)?://' \
    <<<"$safety_create"; then
    echo 'FAIL: vision safety job exposed credential material' >&2
    exit 1
  fi
  for immutable_call in \
    'docker|service ps dddddddddddddddddddddddddddddddd' \
    'docker|inspect eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' \
    'docker|service logs dddddddddddddddddddddddddddddddd' \
    'docker|service rm dddddddddddddddddddddddddddddddd'; do
    grep -Fq "$immutable_call" "$vision_calls"
  done

  : >"$vision_calls"
  rm -f "$vision_state"
  VISION_JOURNAL_MODE=
  VISION_JOURNAL_STATE=
  VISION_JOURNAL_SERVICE_ID=-
  VISION_JOURNAL_EXIT=-
  VISION_EXPECTED_MODE=final-safety
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-final-safety-0123456789ab
  vp_run_vision_cutover_job \
    final-safety vp-ffmpeg-worker-python:deploy-0123456789ab
  final_safety_create="$(grep -F 'docker|service create' "$vision_calls")"
  if [[ "$final_safety_create" != *'--label vp.purpose=final-safety'* \
    || "$final_safety_create" != *'source=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,target=vision-cutover-redis-url'* \
    || "$final_safety_create" != *'source=cccccccccccccccccccccccccccccccc,target=vision-cutover-database-url'* \
    || "$final_safety_create" != *'app.services.vision_consumer_cutover --safety'* ]]; then
    echo 'FAIL: final vision safety job transport is not isolated and exact' >&2
    exit 1
  fi
  grep -Fq \
    'secret-create|vp-vision-cutover-final-read-db-0123456789ab|' \
    "$vision_calls"
  grep -Fq \
    '|vision-cutover|tx-0123456789abcdef0123456789abcdef|final-safety-database' \
    "$vision_calls"
  grep -Fq \
    'secret-rm|cccccccccccccccccccccccccccccccc|vp-vision-cutover-final-read-db-0123456789ab|vision-cutover|tx-0123456789abcdef0123456789abcdef|final-safety-database' \
    "$vision_calls"

  : >"$vision_calls"
  rm -f "$vision_state"
  VISION_JOURNAL_MODE=
  VISION_JOB_EXIT=10
  VISION_EXPECTED_MODE=check
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-check-0123456789ab
  vp_vision_cutover_required \
    vp-ffmpeg-worker-python:deploy-0123456789ab
  if [[ "$VP_VISION_CUTOVER_REQUIRED" != true ]]; then
    echo 'FAIL: vision check-only exit 10 did not require cutover' >&2
    exit 1
  fi
  check_create="$(grep -F 'docker|service create' "$vision_calls")"
  [[ "$check_create" == *'source=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,target=vision-cutover-redis-url'* ]]
  [[ "$check_create" == *'app.services.vision_consumer_cutover --check-only'* ]]
  [[ "$check_create" != *'vision-cutover-database-url'* ]]

  : >"$vision_calls"
  rm -f "$vision_state"
  VISION_JOURNAL_MODE=
  VISION_JOB_EXIT=1
  if vp_vision_cutover_required \
    vp-ffmpeg-worker-python:deploy-0123456789ab >/dev/null 2>&1; then
    echo 'FAIL: vision check-only operational exit was treated as required' >&2
    exit 1
  fi

  : >"$vision_calls"
  rm -f "$vision_state"
  VISION_JOURNAL_MODE=
  VISION_JOB_EXIT=0
  VISION_EXPECTED_MODE=reconcile
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-reconcile-0123456789ab
  vp_reconcile_vision_consumers \
    vp-ffmpeg-worker-python:deploy-0123456789ab
  reconcile_create="$(grep -F 'docker|service create' "$vision_calls")"
  [[ "$reconcile_create" == *'source=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,target=vision-cutover-redis-url'* ]]
  [[ "$reconcile_create" == *'app.services.vision_consumer_cutover'* ]]
  [[ "$reconcile_create" != *'vision-cutover-database-url'* ]]

  : >"$vision_calls"
  printf '%s\n' dddddddddddddddddddddddddddddddd >"$vision_state"
  VISION_JOURNAL_MODE=safety
  VISION_JOURNAL_STATE=planned
  VISION_JOURNAL_SERVICE_ID=-
  VISION_JOURNAL_EXIT=-
  VISION_EXPECTED_MODE=safety
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-safety-0123456789ab
  vp_worker_admission_abort_vision_job safety
  grep -Fq \
    'docker|service rm dddddddddddddddddddddddddddddddd' \
    "$vision_calls"
  grep -Fq \
    'secret-rm|cccccccccccccccccccccccccccccccc|vp-vision-cutover-read-db-0123456789ab|vision-cutover|tx-0123456789abcdef0123456789abcdef|safety-database' \
    "$vision_calls"
  grep -Fxq 'journal-abort-removed|safety' "$vision_calls"

  : >"$vision_calls"
  rm -f "$vision_state"
  VISION_JOURNAL_MODE=
  WATCHER_LIVE_ID=ffffffffffffffffffffffffffffffff
  VISION_EXPECTED_MODE=check
  VISION_EXPECTED_JOB_NAME=vp-vision-cutover-check-0123456789ab
  if vp_run_vision_cutover_job \
    check vp-ffmpeg-worker-python:deploy-0123456789ab >/dev/null 2>&1; then
    echo 'FAIL: replaced watcher secret ID was accepted for a vision job' >&2
    exit 1
  fi
  if grep -Fq 'docker|service create' "$vision_calls"; then
    echo 'FAIL: replaced watcher secret reached vision job creation' >&2
    exit 1
  fi
)

(
  ROOT="$TEST_ROOT/immutable-service-mutations/sync"
  REPO_ROOT="$TEST_ROOT/immutable-service-mutations/repos"
  mkdir -p "$ROOT"
  source "$EXTENSION"
  immutable_calls="$TEST_ROOT/immutable-service-mutations/calls"
  : >"$immutable_calls"
  immutable_service=vp-ffmpeg-worker-go-swarm
  immutable_id=aaaaaaaaaaaaaaaaaaaaaaaa
  vp_worker_admission_require_worker_mutation() {
    :
  }
  vp_worker_admission_complete_worker_mutation() {
    :
  }
  vp_require_worker_redis_marker_status() {
    :
  }
  vp_registered_worker_service_current_id() {
    printf '%s\n' "$immutable_id"
  }
  vp_registered_worker_service_identity() {
    printf '%s\n' "$immutable_id"
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$immutable_calls"
  }
  vp_mutate_registered_worker_service \
    update "$immutable_service" "$immutable_id" 901 \
    service update --detach=false --image vp-go:new "$immutable_service"
  if ! grep -Fxq \
    "docker|service update --detach=false --image vp-go:new $immutable_id" \
    "$immutable_calls"; then
    echo 'FAIL: registered worker update did not target its verified service ID' >&2
    exit 1
  fi

  : >"$immutable_calls"
  docker() {
    printf 'docker|%s\n' "$*" >>"$immutable_calls"
    if [[ "$*" == "service inspect vp-api-swarm --format {{.ID}}|{{.Spec.Name}}" ]]; then
      printf '%s|vp-api-swarm\n' "$immutable_id"
    elif [[ "$*" == "service inspect $immutable_id --format {{json .Spec}}" ]]; then
      printf '%s\n' \
        '{"Name":"vp-api-swarm","TaskTemplate":{"ContainerSpec":{"Image":"vp-api:new"}}}'
    else
      return 1
    fi
  }
  vp_app_service_durable_identity vp-api-swarm vp-api:new >/dev/null
  if grep -Fxq \
    'docker|service inspect vp-api-swarm --format {{json .Spec}}' \
    "$immutable_calls"; then
    echo 'FAIL: baseline capture inspected a mutable service name after ID resolution' >&2
    exit 1
  fi

  : >"$immutable_calls"
  VP_APP_SERVICES=vp-api-swarm
  immutable_spec='{"Name":"vp-api-swarm","TaskTemplate":{"ContainerSpec":{"Image":"vp-api:prior"}}}'
  immutable_digest="$(
    printf '%s\n' "$immutable_spec" | python3 -I -c '
import hashlib
import json
import sys

value = json.load(sys.stdin)
canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
print(hashlib.sha256(canonical).hexdigest())
'
  )"
  docker() {
    printf 'docker|%s\n' "$*" >>"$immutable_calls"
    if [[ "$*" == "service inspect $immutable_id --format {{.Spec.TaskTemplate.ContainerSpec.Image}}" ]]; then
      printf '%s\n' vp-api:prior
    elif [[ "$*" == "service inspect $immutable_id --format {{json .Spec}}" ]]; then
      printf '%s\n' "$immutable_spec"
    else
      return 1
    fi
  }
  immutable_snapshot="vp-api-swarm|$immutable_id|vp-api:prior|$immutable_digest"
  if [[ "$(vp_capture_app_snapshots)" != "$immutable_snapshot" ]]; then
    echo 'FAIL: app snapshot capture did not retain its immutable identity' >&2
    exit 1
  fi
  if grep -Fq 'service inspect vp-api-swarm' "$immutable_calls"; then
    echo 'FAIL: app snapshot capture inspected a mutable name after ID resolution' >&2
    exit 1
  fi

  : >"$immutable_calls"
  vp_worker_admission_root() {
    printf '%s\n' "$TEST_ROOT/immutable-baseline"
  }
  vp_worker_admission_baseline_control_json() {
    printf 'null\n'
  }
  vp_app_service_durable_identity() {
    printf 'rebound-by-name|%s\n' "$*" >>"$immutable_calls"
    return 1
  }
  immutable_baseline="$(
    vp_worker_admission_baseline_payload "$immutable_snapshot"
  )"
  python3 -I - "$immutable_id" "$immutable_digest" \
    "$immutable_baseline" <<'PY'
import json
import sys

service_id, digest, raw = sys.argv[1:]
value = json.loads(raw)
expected = {
    "control": None,
    "kind": "legacy_no_control",
    "services": [{
        "docker_service_id": service_id,
        "existed": True,
        "image": "vp-api:prior",
        "name": "vp-api-swarm",
        "spec_digest": digest,
    }],
}
if value != expected:
    raise SystemExit("baseline did not preserve the captured service identity")
PY
  if grep -Fq 'rebound-by-name|' "$immutable_calls"; then
    echo 'FAIL: baseline rebound an immutable snapshot by service name' >&2
    exit 1
  fi

  : >"$immutable_calls"
  rebound_id=bbbbbbbbbbbbbbbbbbbbbbbb
  vp_registered_worker_service_current_id() {
    printf '%s\n' "$rebound_id"
  }
  vp_update_runtime_service() {
    printf 'mutation|%s\n' "$*" >>"$immutable_calls"
  }
  VP_BACKEND_MIGRATION_APPLIED=false
  if vp_restore_app_snapshots \
    "$immutable_snapshot" vp-api-swarm false >/dev/null 2>&1; then
    echo 'FAIL: rollback accepted a service name rebound to a new ID' >&2
    exit 1
  fi
  if grep -Fq 'mutation|' "$immutable_calls"; then
    echo 'FAIL: rebound rollback reached a service mutation' >&2
    exit 1
  fi
)

(
  ROOT="$TEST_ROOT/stage2-marker-mutations/sync"
  REPO_ROOT="$TEST_ROOT/stage2-marker-mutations/repos"
  mkdir -p "$ROOT"
  source "$EXTENSION"
  mutation_calls="$TEST_ROOT/stage2-marker-mutations/calls"
  : >"$mutation_calls"
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  vp_worker_admission_require_worker_mutation() {
    printf 'journal|%s|%s|%s|%s\n' "$@" >>"$mutation_calls"
  }
  vp_worker_admission_complete_worker_mutation() {
    printf 'applied|%s|%s|%s|%s\n' "$@" >>"$mutation_calls"
  }
  vp_require_worker_redis_marker_status() {
    printf 'marker|status\n' >>"$mutation_calls"
    return 1
  }
  docker() {
    printf 'docker|%s\n' "$*" >>"$mutation_calls"
  }

  mutation_cases="$({
    printf '%s|update|%s|%s|901\n' go-final vp-ffmpeg-worker-go-swarm aaaaaaaaaaaaaaaaaaaaaaaa
    printf '%s|update|%s|%s|902\n' python-update "$VP_PYTHON_WORKER_SERVICE" bbbbbbbbbbbbbbbbbbbbbbbb
    printf '%s|create|%s|absent|902\n' python-create "$VP_PYTHON_WORKER_SERVICE"
    printf '%s|update|%s|%s|903\n' vision-normalize "$VP_VISION_WORKER_SERVICE" cccccccccccccccccccccccc
    printf '%s|update|%s|%s|903\n' vision-final "$VP_VISION_WORKER_SERVICE" cccccccccccccccccccccccc
    printf '%s|create|%s|absent|903\n' vision-create "$VP_VISION_WORKER_SERVICE"
    printf '%s|update|%s|%s|904\n' publisher-normalize "$VP_PUBLISHER_SERVICE" dddddddddddddddddddddddd
    printf '%s|update|%s|%s|904\n' publisher-final "$VP_PUBLISHER_SERVICE" dddddddddddddddddddddddd
    printf '%s|create|%s|absent|904\n' publisher-create "$VP_PUBLISHER_SERVICE"
    printf '%s|update|%s|%s|902\n' rollback-gpu "$VP_PYTHON_WORKER_SERVICE" bbbbbbbbbbbbbbbbbbbbbbbb
    printf '%s|rm|%s|%s|902\n' rollback-python-rm "$VP_PYTHON_WORKER_SERVICE" bbbbbbbbbbbbbbbbbbbbbbbb
    printf '%s|rm|%s|%s|903\n' rollback-vision-rm "$VP_VISION_WORKER_SERVICE" cccccccccccccccccccccccc
    printf '%s|rm|%s|%s|904\n' rollback-publisher-rm "$VP_PUBLISHER_SERVICE" dddddddddddddddddddddddd
    printf '%s|update|%s|%s|903\n' candidate-restore-update "$VP_VISION_WORKER_SERVICE" cccccccccccccccccccccccc
    printf '%s|rm|%s|%s|904\n' candidate-restore-rm "$VP_PUBLISHER_SERVICE" dddddddddddddddddddddddd
  })"
  while IFS='|' read -r mutation_name mutation_action mutation_service \
    mutation_expected_id mutation_generation; do
    : >"$mutation_calls"
    if vp_mutate_registered_worker_service \
      "$mutation_action" "$mutation_service" \
      "$mutation_expected_id" "$mutation_generation" \
      service "$mutation_action" "$mutation_service"; then
      echo "FAIL: stale marker allowed $mutation_name mutation" >&2
      exit 1
    fi
    if [[ "$(grep -c '^marker|status$' "$mutation_calls")" -ne 1 \
      || -n "$(grep '^docker|' "$mutation_calls" || true)" \
      || -n "$(grep '^applied|' "$mutation_calls" || true)" ]]; then
      echo "FAIL: $mutation_name did not stop at its marker boundary" >&2
      exit 1
    fi
  done <<<"$mutation_cases"
)
