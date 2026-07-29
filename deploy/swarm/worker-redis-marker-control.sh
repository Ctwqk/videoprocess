#!/usr/bin/env bash
set -euo pipefail
umask 077

READINESS_JOB="vp-worker-redis-marker-readiness-job"
JANITOR_JOB="vp-worker-redis-marker-janitor-job"
CONTROL_NODE="ccttww-lap"
READINESS_MAX_AGE_SECONDS=90

emit() {
  local output=""
  local field
  for field in "$@"; do
    if [[ ! "$field" =~ ^[a-z_]+=[A-Za-z0-9._:@/+,-]+$ ]]; then
      printf 'mode=control code=internal_output_invalid\n'
      return 70
    fi
    output="${output:+$output }$field"
  done
  printf '%s\n' "$output"
}

file_mode() {
  local path="$1"
  local mode
  if mode="$(stat -f '%Lp' "$path" 2>/dev/null)"; then
    printf '%s\n' "$mode"
    return 0
  fi
  stat -c '%a' "$path" 2>/dev/null
}

reject_forbidden_topology() {
  local value
  value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    *10.0.0.126*|*caspers-mac-mini*|*colima-swarmbridged*|*colima-126*|*hostname==126*)
      return 1
      ;;
  esac
}

load_config() {
  local path="$1"
  if [[ ! "$path" = /* || ! -f "$path" || -L "$path" ]]; then
    return 1
  fi
  if [[ "$(file_mode "$path")" != 600 ]]; then
    return 1
  fi

  GENERATION=""
  IMAGE=""
  NETWORK=""
  READINESS_DATABASE_SECRET=""
  READINESS_REDIS_SECRET=""
  JANITOR_DATABASE_SECRET=""
  JANITOR_REDIS_SECRET=""

  local key
  local value
  while IFS='=' read -r key value; do
    if [[ -z "$key" || -z "$value" || "$value" == *$'\r'* ]]; then
      return 1
    fi
    case "$key" in
      GENERATION)
        [[ -z "$GENERATION" ]] || return 1
        GENERATION="$value"
        ;;
      IMAGE)
        [[ -z "$IMAGE" ]] || return 1
        IMAGE="$value"
        ;;
      NETWORK)
        [[ -z "$NETWORK" ]] || return 1
        NETWORK="$value"
        ;;
      READINESS_DATABASE_SECRET)
        [[ -z "$READINESS_DATABASE_SECRET" ]] || return 1
        READINESS_DATABASE_SECRET="$value"
        ;;
      READINESS_REDIS_SECRET)
        [[ -z "$READINESS_REDIS_SECRET" ]] || return 1
        READINESS_REDIS_SECRET="$value"
        ;;
      JANITOR_DATABASE_SECRET)
        [[ -z "$JANITOR_DATABASE_SECRET" ]] || return 1
        JANITOR_DATABASE_SECRET="$value"
        ;;
      JANITOR_REDIS_SECRET)
        [[ -z "$JANITOR_REDIS_SECRET" ]] || return 1
        JANITOR_REDIS_SECRET="$value"
        ;;
      *)
        return 1
        ;;
    esac
  done <"$path"

  [[ "$GENERATION" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]] || return 1
  [[ "$IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  [[ "$NETWORK" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$ ]] || return 1
  local secret
  for secret in \
    "$READINESS_DATABASE_SECRET" \
    "$READINESS_REDIS_SECRET" \
    "$JANITOR_DATABASE_SECRET" \
    "$JANITOR_REDIS_SECRET"; do
    [[ "$secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || return 1
  done
  reject_forbidden_topology \
    "$GENERATION $IMAGE $NETWORK $READINESS_DATABASE_SECRET $READINESS_REDIS_SECRET $JANITOR_DATABASE_SECRET $JANITOR_REDIS_SECRET"
}

acquire_mode_lock() {
  local mode="$1"
  mkdir -p "$LOCK_DIR"
  MODE_LOCK="$LOCK_DIR/$mode.lock"
  if ! mkdir "$MODE_LOCK" 2>/dev/null; then
    emit "mode=$mode" "code=lock_busy"
    return 1
  fi
  trap 'rmdir "$MODE_LOCK" 2>/dev/null || true' EXIT HUP INT TERM
}

service_state() {
  local name="$1"
  local states
  if ! states="$(
    docker service ps "$name" --no-trunc \
      --format '{{.CurrentState}}' 2>/dev/null
  )"; then
    return 1
  fi
  local count
  count="$(printf '%s\n' "$states" | awk 'NF { count++ } END { print count+0 }')"
  if [[ "$count" -ne 1 ]]; then
    printf 'pending\n'
    return 0
  fi
  printf '%s\n' "${states%% *}"
}

remove_completed_job() {
  local mode="$1"
  local name="$2"
  if ! docker service inspect "$name" >/dev/null 2>&1; then
    return 0
  fi
  local state
  state="$(service_state "$name")" || {
    emit "mode=$mode" "code=job_inspection_failed"
    return 3
  }
  case "$state" in
    Running|Pending|Preparing|Starting|Ready|New|Assigned|Accepted|pending)
      emit "mode=$mode" "code=job_running"
      return 10
      ;;
    Complete|Failed|Rejected|Shutdown|Orphaned|Remove)
      if ! docker service rm "$name" >/dev/null; then
        emit "mode=$mode" "code=job_remove_failed"
        return 3
      fi
      ;;
    *)
      emit "mode=$mode" "code=job_state_invalid"
      return 3
      ;;
  esac
}

write_status() {
  local mode="$1"
  shift
  local target="$STATE_DIR/$mode.status"
  local temporary="$STATE_DIR/.$mode.status.$$"
  {
    printf 'GENERATION=%s\n' "$GENERATION"
    printf 'RECORDED_AT=%s\n' "$(date +%s)"
    printf '%s\n' "$@"
  } >"$temporary"
  chmod 0600 "$temporary"
  mv -f "$temporary" "$target"
}

finish_readiness() {
  local line="$1"
  if [[ ! "$line" =~ ^\{\"checked_count\":([0-9]+),\"code\":\"([a-z0-9_]+)\",\"expected_count\":([0-9]+),\"status\":\"(ok|failed)\"\}$ ]]; then
    emit "mode=readiness" "code=job_output_invalid"
    return 3
  fi
  local checked_count="${BASH_REMATCH[1]}"
  local code="${BASH_REMATCH[2]}"
  local expected_count="${BASH_REMATCH[3]}"
  local status="${BASH_REMATCH[4]}"
  write_status readiness \
    "CODE=$code" \
    "CHECKED_COUNT=$checked_count" \
    "EXPECTED_COUNT=$expected_count"
  emit \
    "mode=readiness" \
    "code=$code" \
    "checked_count=$checked_count" \
    "expected_count=$expected_count"
  [[ "$status" == ok && "$code" == ready ]]
}

finish_janitor() {
  local line="$1"
  if [[ ! "$line" =~ ^\{\"absent\":([0-9]+),\"claimed\":([0-9]+),\"code\":\"([a-z0-9_]+)\",\"conflict\":([0-9]+),\"deleted\":([0-9]+),\"status\":\"(ok|failed)\"\}$ ]]; then
    emit "mode=janitor" "code=job_output_invalid"
    return 3
  fi
  local absent="${BASH_REMATCH[1]}"
  local claimed="${BASH_REMATCH[2]}"
  local code="${BASH_REMATCH[3]}"
  local conflict="${BASH_REMATCH[4]}"
  local deleted="${BASH_REMATCH[5]}"
  local status="${BASH_REMATCH[6]}"
  write_status janitor \
    "CODE=$code" \
    "ABSENT=$absent" \
    "CLAIMED=$claimed" \
    "CONFLICT=$conflict" \
    "DELETED=$deleted"
  emit \
    "mode=janitor" \
    "code=$code" \
    "claimed=$claimed" \
    "deleted=$deleted" \
    "absent=$absent" \
    "conflict=$conflict"
  [[ "$status" == ok && "$code" == ready && "$conflict" -eq 0 ]]
}

launch_job() {
  local mode="$1"
  local name="$2"
  local database_secret="$3"
  local redis_secret="$4"
  local module="$5"
  local command="$6"

  local remove_status=0
  if remove_completed_job "$mode" "$name"; then
    :
  else
    remove_status=$?
    if [[ "$remove_status" -eq 10 ]]; then
      return 0
    fi
    return "$remove_status"
  fi

  if ! docker service create \
    --detach=true \
    --name "$name" \
    --mode replicated-job \
    --replicas 1 \
    --restart-condition none \
    --constraint "node.hostname==$CONTROL_NODE" \
    --network "$NETWORK" \
    --label "vp.worker-redis-marker.mode=$mode" \
    --label "vp.worker-redis-marker.generation=$GENERATION" \
    --secret "source=$database_secret,target=worker-marker-database-url,mode=0400" \
    --secret "source=$redis_secret,target=worker-marker-redis-url,mode=0400" \
    --env "WORKER_REDIS_MARKER_DATABASE_URL_FILE=/run/secrets/worker-marker-database-url" \
    --env "WORKER_REDIS_MARKER_REDIS_URL_FILE=/run/secrets/worker-marker-redis-url" \
    "$IMAGE" \
    python -m "$module" "$command" >/dev/null; then
    emit "mode=$mode" "code=job_create_failed"
    return 3
  fi

  local attempt
  local state=""
  for ((attempt = 0; attempt < MAX_WAIT_SECONDS; attempt++)); do
    state="$(service_state "$name")" || {
      emit "mode=$mode" "code=job_inspection_failed"
      return 3
    }
    case "$state" in
      Complete)
        break
        ;;
      Failed|Rejected|Shutdown|Orphaned|Remove)
        emit "mode=$mode" "code=job_failed"
        return 3
        ;;
      Running|Pending|Preparing|Starting|Ready|New|Assigned|Accepted|pending)
        ;;
      *)
        emit "mode=$mode" "code=job_state_invalid"
        return 3
        ;;
    esac
    sleep 1
  done
  if [[ "$state" != Complete ]]; then
    emit "mode=$mode" "code=job_timeout"
    return 3
  fi

  local output
  if ! output="$(docker service logs --raw "$name" 2>/dev/null)"; then
    emit "mode=$mode" "code=job_output_unavailable"
    return 3
  fi
  if [[ -z "$output" || "$output" == *$'\n'* ]]; then
    emit "mode=$mode" "code=job_output_invalid"
    return 3
  fi
  case "$mode" in
    readiness)
      finish_readiness "$output"
      ;;
    janitor)
      finish_janitor "$output"
      ;;
  esac
}

read_status_value() {
  local path="$1"
  local key="$2"
  awk -F= -v expected="$key" '
    $1 == expected {
      if (found || NF != 2) {
        exit 2
      }
      value=$2
      found=1
    }
    END {
      if (!found) {
        exit 1
      }
      print value
    }
  ' "$path"
}

print_sanitized_status() {
  local readiness_path="$STATE_DIR/readiness.status"
  if [[ ! -f "$readiness_path" || -L "$readiness_path" \
    || "$(file_mode "$readiness_path")" != 600 ]]; then
    emit "mode=status" "code=readiness_status_missing"
    return 3
  fi
  local generation
  local recorded_at
  local code
  local checked_count
  local expected_count
  generation="$(read_status_value "$readiness_path" GENERATION)" || {
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  }
  recorded_at="$(read_status_value "$readiness_path" RECORDED_AT)" || {
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  }
  code="$(read_status_value "$readiness_path" CODE)" || {
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  }
  checked_count="$(read_status_value "$readiness_path" CHECKED_COUNT)" || {
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  }
  expected_count="$(read_status_value "$readiness_path" EXPECTED_COUNT)" || {
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  }
  if [[ "$generation" != "$GENERATION" \
    || ! "$recorded_at" =~ ^[0-9]+$ \
    || ! "$checked_count" =~ ^[0-9]+$ \
    || ! "$expected_count" =~ ^[0-9]+$ ]]; then
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  fi
  local now
  now="$(date +%s)"
  if [[ "$recorded_at" -gt "$now" \
    || $((now - recorded_at)) -gt "$READINESS_MAX_AGE_SECONDS" ]]; then
    emit "mode=status" "code=readiness_status_stale"
    return 3
  fi
  if [[ "$code" != ready || "$checked_count" -ne "$expected_count" ]]; then
    emit "mode=status" "code=readiness_status_unready"
    return 3
  fi
  emit \
    "mode=status" \
    "code=ready" \
    "generation=$generation" \
    "checked_count=$checked_count" \
    "expected_count=$expected_count"
}

if [[ "$#" -ne 1 ]]; then
  exit 64
fi
MODE="$1"
case "$MODE" in
  readiness|janitor|status)
    ;;
  *)
    exit 64
    ;;
esac

CONFIG_FILE="${VP_WORKER_REDIS_MARKER_CONFIG_FILE:-/etc/videoprocess/worker-redis-marker-control.conf}"
STATE_DIR="${VP_WORKER_REDIS_MARKER_STATE_DIR:-/var/lib/videoprocess/worker-redis-marker-control}"
LOCK_DIR="${VP_WORKER_REDIS_MARKER_LOCK_DIR:-/var/lock/videoprocess-worker-redis-marker-control}"
MAX_WAIT_SECONDS="${VP_WORKER_REDIS_MARKER_MAX_WAIT_SECONDS:-60}"
if [[ ! "$STATE_DIR" = /* || ! "$LOCK_DIR" = /* \
  || ! "$MAX_WAIT_SECONDS" =~ ^[1-9][0-9]?$ ]]; then
  emit "mode=$MODE" "code=control_config_invalid"
  exit 3
fi
if ! load_config "$CONFIG_FILE"; then
  emit "mode=$MODE" "code=control_config_invalid"
  exit 3
fi
if [[ "${VP_WORKER_REDIS_MARKER_DRY_RUN:-0}" == 1 ]]; then
  emit "mode=$MODE" "code=dry_run"
  exit 0
fi

mkdir -p "$STATE_DIR"
chmod 0700 "$STATE_DIR"

case "$MODE" in
  readiness)
    acquire_mode_lock readiness || exit 0
    launch_job \
      readiness \
      "$READINESS_JOB" \
      "$READINESS_DATABASE_SECRET" \
      "$READINESS_REDIS_SECRET" \
      app.channel_agent.worker_redis_marker_readiness_cli \
      check
    ;;
  janitor)
    acquire_mode_lock janitor || exit 0
    launch_job \
      janitor \
      "$JANITOR_JOB" \
      "$JANITOR_DATABASE_SECRET" \
      "$JANITOR_REDIS_SECRET" \
      app.channel_agent.worker_redis_marker_janitor_cli \
      run
    ;;
  status)
    print_sanitized_status
    ;;
esac
