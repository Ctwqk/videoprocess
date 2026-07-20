#!/usr/bin/env bash
set -euo pipefail

log_status() {
  printf '%s\n' "$*"
}

configuration_error() {
  log_status "status=configuration_error reason=$1"
  exit 2
}

add_external_condition() {
  local condition="$1"
  case " $external_conditions " in
    *" $condition "*) ;;
    *) external_conditions="$external_conditions $condition" ;;
  esac
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_rfc3339_utc() {
  local value="$1"
  local date_part year month day max_day
  if [[ ! "$value" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?Z$ ]]; then
    return 1
  fi

  date_part="${value%%T*}"
  IFS='-' read -r year month day <<<"$date_part"
  year=$((10#$year))
  month=$((10#$month))
  day=$((10#$day))
  [[ "$year" -gt 0 ]] || return 1

  case "$month" in
    1|3|5|7|8|10|12) max_day=31 ;;
    4|6|9|11) max_day=30 ;;
    2)
      max_day=28
      if (( year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) )); then
        max_day=29
      fi
      ;;
    *) return 1 ;;
  esac
  [[ "$day" -le "$max_day" ]]
}

rfc3339_utc_epoch() {
  local value="$1"
  local normalized="$value"
  if [[ "$normalized" == *.*Z ]]; then
    normalized="${normalized%%.*}Z"
  fi
  date -u -d "$normalized" +%s 2>/dev/null \
    || date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$normalized" +%s 2>/dev/null
}

rfc3339_has_positive_fraction() {
  local value="$1"
  local fraction
  [[ "$value" == *.*Z ]] || return 1
  fraction="${value#*.}"
  fraction="${fraction%Z}"
  [[ "$fraction" =~ [1-9] ]]
}

sync_root="${DEPLOY_GITHUB_SYNC_ROOT:-/home/taiwei/deploy-github-sync}"
state_file="$sync_root/state/vp-soak-watch.env"

if [[ ! -f "$state_file" ]]; then
  log_status 'status=disabled reason=state_missing'
  exit 0
fi

VP_SOAK_WATCH_ENABLED=''
VP_SOAK_CHANNEL_ID=''
VP_SOAK_STARTED_AT=''
VP_SOAK_MAX_PUBLICATIONS_PER_24H=''
VP_SOAK_UPLOAD_STALE_MINUTES=''
VP_SOAK_FEEDBACK_GRACE_HOURS=''
VP_SOAK_AUTO_HOLD=''
VP_SOAK_FORBIDDEN_NODE_PATTERN=''
VP_SOAK_REDIS_CONTAINER=''
seen_state_keys=' '
while IFS= read -r state_record || [[ -n "$state_record" ]]; do
  case "$state_record" in
    ''|'#'*) continue ;;
  esac
  if [[ "$state_record" != *=* ]]; then
    configuration_error invalid_state_file
  fi
  state_key="${state_record%%=*}"
  state_value="${state_record#*=}"
  case "$state_key" in
    VP_SOAK_WATCH_ENABLED|VP_SOAK_CHANNEL_ID|VP_SOAK_STARTED_AT|\
    VP_SOAK_MAX_PUBLICATIONS_PER_24H|VP_SOAK_UPLOAD_STALE_MINUTES|\
    VP_SOAK_FEEDBACK_GRACE_HOURS|VP_SOAK_AUTO_HOLD|\
    VP_SOAK_FORBIDDEN_NODE_PATTERN|VP_SOAK_REDIS_CONTAINER) ;;
    *) configuration_error unsupported_state_key ;;
  esac
  case "$seen_state_keys" in
    *" $state_key "*) configuration_error duplicate_state_key ;;
  esac
  seen_state_keys="${seen_state_keys}${state_key} "
  printf -v "$state_key" '%s' "$state_value"
done <"$state_file"

if [[ "${VP_SOAK_WATCH_ENABLED:-}" != "true" ]]; then
  log_status 'status=disabled reason=not_enabled'
  exit 0
fi

channel_id="${VP_SOAK_CHANNEL_ID:-}"
started_at="${VP_SOAK_STARTED_AT:-}"
max_publications="${VP_SOAK_MAX_PUBLICATIONS_PER_24H:-1}"
upload_stale_minutes="${VP_SOAK_UPLOAD_STALE_MINUTES:-45}"
feedback_grace_hours="${VP_SOAK_FEEDBACK_GRACE_HOURS:-30}"
auto_hold="${VP_SOAK_AUTO_HOLD:-false}"
extra_forbidden_pattern="${VP_SOAK_FORBIDDEN_NODE_PATTERN:-}"
forbidden_baseline='CASPERs-Mac-mini|colima-swarmbridged|10\.0\.0\.126'
forbidden_pattern="$forbidden_baseline"
redis_container="${VP_SOAK_REDIS_CONTAINER:-constructure_vp_redis}"

if [[ ! "$channel_id" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]; then
  configuration_error invalid_channel_id
fi
if ! is_rfc3339_utc "$started_at"; then
  configuration_error invalid_started_at
fi
if ! started_at_epoch="$(rfc3339_utc_epoch "$started_at")"; then
  configuration_error invalid_started_at
fi
now_epoch="$(date -u +%s)"
if (( started_at_epoch > now_epoch + 300 )) \
  || { (( started_at_epoch == now_epoch + 300 )) && rfc3339_has_positive_fraction "$started_at"; }; then
  configuration_error future_started_at
fi
if ! is_positive_integer "$max_publications"; then
  configuration_error invalid_max_publications_per_24h
fi
if ! is_positive_integer "$upload_stale_minutes"; then
  configuration_error invalid_upload_stale_minutes
fi
if ! is_positive_integer "$feedback_grace_hours"; then
  configuration_error invalid_feedback_grace_hours
fi
case "$auto_hold" in
  true|false) ;;
  *) configuration_error invalid_auto_hold ;;
esac
if [[ -n "$extra_forbidden_pattern" ]]; then
  set +e
  printf '' | grep -Eq -- "$extra_forbidden_pattern"
  pattern_status=$?
  set -e
  if [[ "$pattern_status" -gt 1 ]]; then
    configuration_error invalid_forbidden_node_pattern
  fi
  forbidden_pattern="($forbidden_baseline)|($extra_forbidden_pattern)"
fi

deploy_env="${DEPLOY_GITHUB_SYNC_ENV_FILE:-$sync_root/env/deploy.env}"
if [[ ! -r "$deploy_env" ]]; then
  configuration_error deploy_environment_missing
fi
if ! source "$deploy_env" >/dev/null 2>&1; then
  configuration_error deploy_environment_invalid
fi
if [[ -z "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]]; then
  configuration_error database_credential_missing
fi

services='vp-api-swarm
vp-frontend-swarm
vp-autoflow-api-swarm
vp-event-outbox-relay-swarm
vp-channel-agent-runner-swarm
vp-ffmpeg-worker-go-swarm
vp-ffmpeg-worker-gpu-swarm
vp-youtube-publisher-swarm
vp-feature-aggregator-swarm
vp-pds-swarm'
external_conditions=''
trusted_python_image=''

while IFS= read -r service; do
  [[ -n "$service" ]] || continue
  if ! service_details="$(docker service inspect "$service" \
    --format '{{if .Spec.Mode.Replicated}}{{.Spec.Mode.Replicated.Replicas}}{{else}}1{{end}}|{{.Spec.TaskTemplate.ContainerSpec.Image}}' \
    2>/dev/null)"; then
    log_status "service=$service status=missing"
    add_external_condition service_missing
    continue
  fi
  desired_replicas="${service_details%%|*}"
  service_image="${service_details#*|}"
  case "$service" in
    vp-ffmpeg-worker-gpu-swarm)
      if [[ -n "$service_image" ]]; then
        trusted_python_image="$service_image"
      fi
      ;;
    vp-youtube-publisher-swarm)
      if [[ -n "$service_image" ]]; then
        trusted_python_image="$service_image"
      fi
      ;;
  esac

  if ! running_nodes="$(docker service ps "$service" \
    --filter desired-state=running --format '{{.Node}}|{{.CurrentState}}' 2>/dev/null)"; then
    log_status "service=$service status=unhealthy"
    add_external_condition service_unhealthy
    continue
  fi

  running_replicas="$(printf '%s\n' "$running_nodes" \
    | awk -F'|' '$2 ~ /^Running([[:space:]]|$)/ { count++ } END { print count + 0 }')"
  if [[ ! "$desired_replicas" =~ ^[1-9][0-9]*$ ]] \
    || [[ "$running_replicas" -ne "$desired_replicas" ]]; then
    log_status "service=$service status=unhealthy desired=${desired_replicas:-unknown} running=$running_replicas"
    add_external_condition service_unhealthy
  else
    log_status "service=$service status=healthy replicas=$running_replicas"
  fi

  if [[ -n "$running_nodes" ]] \
    && printf '%s\n' "$running_nodes" | awk -F'|' 'NF { print $1 }' \
      | grep -Eq -- "$forbidden_pattern"; then
    log_status "service=$service placement=forbidden"
    add_external_condition forbidden_node_placement
  fi
done <<<"$services"

stream_groups='vp:tasks:ffmpeg_go|ffmpeg_go-workers
vp:tasks:ffmpeg|ffmpeg-workers
vp:tasks:youtube_publisher|youtube_publisher-workers
vp:events|orchestrator'

while IFS='|' read -r stream group; do
  [[ -n "$stream" && -n "$group" ]] || continue
  if ! group_output="$(docker exec "$redis_container" \
    redis-cli --raw XINFO GROUPS "$stream" 2>/dev/null)"; then
    log_status "stream=$stream group=$group status=missing"
    add_external_condition redis_group_missing
    continue
  fi

  if ! group_stats="$(printf '%s\n' "$group_output" | awk -v target="$group" '
    $0 == "name" {
      if (getline name <= 0) next
      selected = (name == target)
      next
    }
    selected && $0 == "pending" {
      if (getline pending <= 0) pending = ""
      next
    }
    selected && $0 == "lag" {
      if (getline lag <= 0) lag = ""
      found = 1
      next
    }
    END {
      if (found && pending ~ /^[0-9]+$/ && lag ~ /^[0-9]+$/) {
        print pending "|" lag
      } else {
        exit 1
      }
    }
  ')"; then
    log_status "stream=$stream group=$group status=missing"
    add_external_condition redis_group_missing
    continue
  fi

  pending="${group_stats%%|*}"
  lag="${group_stats#*|}"
  if [[ "$pending" -gt 0 || "$lag" -gt 0 ]]; then
    log_status "stream=$stream group=$group status=critical pending=$pending lag=$lag"
    add_external_condition redis_pending_exceeded
  else
    log_status "stream=$stream group=$group status=healthy pending=0 lag=0"
  fi
done <<<"$stream_groups"

if [[ -z "$trusted_python_image" ]]; then
  configuration_error trusted_python_image_missing
fi

guard_args=(
  run --rm
  --env DATABASE_URL
  "$trusted_python_image"
  python -m app.channel_agent.soak_guard_cli
  --channel-id "$channel_id"
  --started-at "$started_at"
  --max-publications-per-24h "$max_publications"
  --upload-stale-minutes "$upload_stale_minutes"
  --feedback-grace-hours "$feedback_grace_hours"
)
for condition in $external_conditions; do
  guard_args+=(--external-condition "$condition")
done
if [[ "$auto_hold" == "true" ]]; then
  guard_args+=(--apply)
fi

export DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL"
unset VP_PYTHON_WORKER_DATABASE_URL

guard_exit=0
docker "${guard_args[@]}" || guard_exit=$?

case "$guard_exit" in
  0)
    log_status 'status=healthy guard_exit=0'
    ;;
  20)
    log_status 'status=guard_tripped guard_exit=20'
    ;;
  *)
    log_status "status=guard_error guard_exit=$guard_exit"
    ;;
esac
exit "$guard_exit"
