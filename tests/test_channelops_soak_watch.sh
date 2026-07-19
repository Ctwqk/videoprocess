#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WATCHER="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
TEST_ROOT="$(mktemp -d)"
FAKE_BIN="$TEST_ROOT/bin"
CALLS="$TEST_ROOT/docker.calls"
OUTPUT="$TEST_ROOT/watcher.out"
STATE_DIR="$TEST_ROOT/state"
DEPLOY_ENV="$TEST_ROOT/deploy.env"
SECRET_URL='postgresql+asyncpg://guard:do-not-log@database.example/videoprocess'

cleanup() {
  local status=$?
  rm -rf "$TEST_ROOT"
  exit "$status"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local needle="$1"
  local file="$2"
  grep -Fq -- "$needle" "$file" || fail "expected '$needle' in $file"
}

assert_not_contains() {
  local needle="$1"
  local file="$2"
  if grep -Fiq -- "$needle" "$file"; then
    fail "did not expect '$needle' in $file"
  fi
}

run_watcher() {
  : >"$OUTPUT"
  set +e
  DEPLOY_GITHUB_SYNC_ROOT="$TEST_ROOT" \
    DEPLOY_GITHUB_SYNC_ENV="$DEPLOY_ENV" \
    PATH="$FAKE_BIN:$PATH" \
    FAKE_DOCKER_CALLS="$CALLS" \
    FAKE_DOCKER_MODE="${FAKE_DOCKER_MODE:-healthy}" \
    FAKE_CLI_EXIT="${FAKE_CLI_EXIT:-0}" \
    bash "$WATCHER" >"$OUTPUT" 2>&1
  WATCHER_EXIT=$?
  set -e
}

write_state() {
  local enabled="${1:-true}"
  local channel_id="${2:-123e4567-e89b-12d3-a456-426614174000}"
  local started_at="${3:-2026-07-19T18:30:00Z}"
  local max_publications="${4:-1}"
  local stale_minutes="${5:-45}"
  local grace_hours="${6:-30}"
  local auto_hold="${7:-false}"

  mkdir -p "$STATE_DIR"
  {
    printf 'VP_SOAK_WATCH_ENABLED=%s\n' "$enabled"
    printf 'VP_SOAK_CHANNEL_ID=%s\n' "$channel_id"
    printf 'VP_SOAK_STARTED_AT=%s\n' "$started_at"
    printf 'VP_SOAK_MAX_PUBLICATIONS_PER_24H=%s\n' "$max_publications"
    printf 'VP_SOAK_UPLOAD_STALE_MINUTES=%s\n' "$stale_minutes"
    printf 'VP_SOAK_FEEDBACK_GRACE_HOURS=%s\n' "$grace_hours"
    printf 'VP_SOAK_AUTO_HOLD=%s\n' "$auto_hold"
  } >"$STATE_DIR/vp-soak-watch.env"
}

mkdir -p "$FAKE_BIN"
: >"$CALLS"
printf 'VP_PYTHON_WORKER_DATABASE_URL=%s\n' "$SECRET_URL" >"$DEPLOY_ENV"

cat >"$FAKE_BIN/docker" <<'FAKE_DOCKER'
#!/usr/bin/env bash
set -euo pipefail

{
  printf 'docker'
  for argument in "$@"; do
    printf '|%s' "$argument"
  done
  printf '\n'
} >>"$FAKE_DOCKER_CALLS"

if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
  service="${3:-}"
  if [[ "$*" == *ContainerSpec.Image* ]]; then
    printf 'vp-ffmpeg-worker-python:deployed\n'
  elif [[ "${FAKE_DOCKER_MODE:-healthy}" == "zero_desired" \
    && "$service" == "vp-api-swarm" ]]; then
    printf '0\n'
  else
    printf '1\n'
  fi
  exit 0
fi

if [[ "${1:-} ${2:-}" == "service ps" ]]; then
  service="${3:-}"
  if [[ "${FAKE_DOCKER_MODE:-healthy}" == "unhealthy" \
    && "$service" == "vp-api-swarm" ]]; then
    exit 0
  fi
  if [[ "${FAKE_DOCKER_MODE:-healthy}" == "zero_desired" \
    && "$service" == "vp-api-swarm" ]]; then
    exit 0
  fi
  if [[ "${FAKE_DOCKER_MODE:-healthy}" == "forbidden" \
    && "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
    printf '10.0.0.126\n'
  else
    printf 'colima-127\n'
  fi
  exit 0
fi

if [[ "${1:-} ${2:-}" == "exec vp-redis" ]]; then
  stream="${@: -1}"
  case "$stream" in
    vp:tasks:ffmpeg_go) group=ffmpeg_go-workers ;;
    vp:tasks:ffmpeg) group=ffmpeg-workers ;;
    vp:tasks:youtube_publisher) group=youtube_publisher-workers ;;
    vp:events) group=orchestrator ;;
    *) exit 1 ;;
  esac
  if [[ "${FAKE_DOCKER_MODE:-healthy}" == "unknown_group" \
    && "$group" == "youtube_publisher-workers" ]]; then
    group=unexpected-workers
  fi
  pending=0
  if [[ "${FAKE_DOCKER_MODE:-healthy}" == "redis_pending" \
    && "$group" == "ffmpeg_go-workers" ]]; then
    pending=2
  fi
  printf 'name\n%s\nconsumers\n1\npending\n%s\nlast-delivered-id\n0-0\nentries-read\n0\nlag\n0\n' "$group" "$pending"
  exit 0
fi

if [[ "${1:-}" == "run" ]]; then
  exit "${FAKE_CLI_EXIT:-0}"
fi

exit 2
FAKE_DOCKER
chmod +x "$FAKE_BIN/docker"

if [[ ! -f "$WATCHER" ]]; then
  fail "missing watcher: $WATCHER"
fi

# Missing and explicitly disabled state are successful no-ops before Docker.
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "missing state must exit zero"
assert_contains 'status=disabled reason=state_missing' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "missing state contacted Docker"
[[ ! -e "$STATE_DIR/vp-soak-watch.env" ]] || fail "watcher created activation state"

write_state false not-a-uuid not-a-timestamp 0 0 0 maybe
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "disabled state must exit zero"
assert_contains 'status=disabled reason=not_enabled' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "disabled state contacted Docker"
assert_contains 'VP_SOAK_WATCH_ENABLED=false' "$STATE_DIR/vp-soak-watch.env"

# Enabled invalid state fails closed before credentials or Docker are touched.
write_state true not-a-uuid
run_watcher
[[ "$WATCHER_EXIT" -ne 0 ]] || fail "invalid UUID must fail"
assert_contains 'status=configuration_error reason=invalid_channel_id' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "invalid UUID contacted Docker"

write_state true 123e4567-e89b-12d3-a456-426614174000 2026-07-19T18:30:00-07:00
run_watcher
[[ "$WATCHER_EXIT" -ne 0 ]] || fail "non-UTC timestamp must fail"
assert_contains 'status=configuration_error reason=invalid_started_at' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "invalid timestamp contacted Docker"

write_state true 123e4567-e89b-12d3-a456-426614174000 2026-02-30T18:30:00Z
run_watcher
[[ "$WATCHER_EXIT" -ne 0 ]] || fail "impossible UTC timestamp must fail"
assert_contains 'status=configuration_error reason=invalid_started_at' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "impossible timestamp contacted Docker"

write_state true 123e4567-e89b-12d3-a456-426614174000 2026-07-19T18:30:00Z 0
run_watcher
[[ "$WATCHER_EXIT" -ne 0 ]] || fail "non-positive threshold must fail"
assert_contains 'status=configuration_error reason=invalid_max_publications_per_24h' "$OUTPUT"
[[ ! -s "$CALLS" ]] || fail "invalid threshold contacted Docker"

# A healthy run checks every service and group, then invokes the guard safely.
write_state
: >"$CALLS"
FAKE_DOCKER_MODE=healthy
FAKE_CLI_EXIT=0
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "healthy watcher run failed"
for service in \
  vp-api-swarm \
  vp-frontend-swarm \
  vp-autoflow-api-swarm \
  vp-event-outbox-relay-swarm \
  vp-channel-agent-runner-swarm \
  vp-ffmpeg-worker-go-swarm \
  vp-ffmpeg-worker-gpu-swarm \
  vp-youtube-publisher-swarm \
  vp-feature-aggregator-swarm \
  vp-pds-swarm; do
  assert_contains "docker|service|inspect|$service" "$CALLS"
  assert_contains "docker|service|ps|$service" "$CALLS"
done
for pair in \
  'vp:tasks:ffmpeg_go|ffmpeg_go-workers' \
  'vp:tasks:ffmpeg|ffmpeg-workers' \
  'vp:tasks:youtube_publisher|youtube_publisher-workers' \
  'vp:events|orchestrator'; do
  stream="${pair%%|*}"
  group="${pair#*|}"
  assert_contains "docker|exec|vp-redis|redis-cli|--raw|XINFO|GROUPS|$stream" "$CALLS"
  assert_contains "stream=$stream group=$group status=healthy" "$OUTPUT"
done
assert_contains 'docker|run|--rm|--env|DATABASE_URL|vp-ffmpeg-worker-python:deployed|python|-m|app.channel_agent.soak_guard_cli' "$CALLS"
assert_contains '|--channel-id|123e4567-e89b-12d3-a456-426614174000' "$CALLS"
assert_contains '|--started-at|2026-07-19T18:30:00Z' "$CALLS"
assert_not_contains "$SECRET_URL" "$CALLS"
assert_not_contains "$SECRET_URL" "$OUTPUT"
assert_not_contains '|--apply' "$CALLS"

# Auto-hold is the sole switch that adds the mutating guard flag.
write_state true 123e4567-e89b-12d3-a456-426614174000 2026-07-19T18:30:00Z 1 45 30 true
: >"$CALLS"
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "auto-hold healthy run failed"
assert_contains '|--apply' "$CALLS"

# Host findings become only the fixed external condition codes.
write_state
: >"$CALLS"
FAKE_DOCKER_MODE=unhealthy
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "unhealthy service assessment did not run"
assert_contains '|--external-condition|service_unhealthy' "$CALLS"

: >"$CALLS"
FAKE_DOCKER_MODE=zero_desired
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "zero-replica service assessment did not run"
assert_contains '|--external-condition|service_unhealthy' "$CALLS"

: >"$CALLS"
FAKE_DOCKER_MODE=forbidden
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "forbidden placement assessment did not run"
assert_contains '|--external-condition|forbidden_node_placement' "$CALLS"

: >"$CALLS"
FAKE_DOCKER_MODE=unknown_group
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "missing Redis group assessment did not run"
assert_contains 'youtube_publisher-workers' "$OUTPUT"
assert_contains '|--external-condition|redis_group_missing' "$CALLS"

: >"$CALLS"
FAKE_DOCKER_MODE=redis_pending
run_watcher
[[ "$WATCHER_EXIT" -eq 0 ]] || fail "pending Redis assessment did not run"
assert_contains '|--external-condition|redis_pending_exceeded' "$CALLS"

# A guard trip remains nonzero for cron, including when quarantine was requested.
write_state true 123e4567-e89b-12d3-a456-426614174000 2026-07-19T18:30:00Z 1 45 30 true
: >"$CALLS"
FAKE_DOCKER_MODE=healthy
FAKE_CLI_EXIT=20
run_watcher
[[ "$WATCHER_EXIT" -eq 20 ]] || fail "guard exit 20 must remain nonzero"
assert_contains 'status=guard_tripped guard_exit=20' "$OUTPUT"

for forbidden_command in upload resume enqueue schedule-open schedule_open; do
  assert_not_contains "|$forbidden_command|" "$CALLS"
done

echo 'PASS: channelops soak watcher contract'
