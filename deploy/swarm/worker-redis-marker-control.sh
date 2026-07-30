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
  NETWORK_ID=""
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
      NETWORK_ID)
        [[ -z "$NETWORK_ID" ]] || return 1
        NETWORK_ID="$value"
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
  [[ "$NETWORK" == vp-pipeline-net \
    && "$NETWORK_ID" =~ ^[A-Za-z0-9._:-]+$ ]] || return 1
  local secret
  for secret in \
    "$READINESS_DATABASE_SECRET" \
    "$READINESS_REDIS_SECRET" \
    "$JANITOR_DATABASE_SECRET" \
    "$JANITOR_REDIS_SECRET"; do
    [[ "$secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || return 1
  done
  reject_forbidden_topology \
    "$GENERATION $IMAGE $NETWORK $NETWORK_ID $READINESS_DATABASE_SECRET $READINESS_REDIS_SECRET $JANITOR_DATABASE_SECRET $JANITOR_REDIS_SECRET"
}

acquire_mode_lock() {
  local mode="$1"
  if ! mkdir -p "$LOCK_DIR"; then
    emit "mode=$mode" "code=lock_unavailable"
    return 3
  fi
  MODE_LOCK="$LOCK_DIR/$mode.lock"
  local held="${VP_WORKER_REDIS_MARKER_LOCK_HELD:-}"
  if [[ "$held" =~ ^(readiness|janitor):([0-9]+)$ \
    && "${BASH_REMATCH[1]}" == "$mode" \
    && -e "/dev/fd/${BASH_REMATCH[2]}" ]]; then
    return 0
  fi
  if command -v flock >/dev/null 2>&1; then
    if ! exec 9>>"$MODE_LOCK"; then
      emit "mode=$mode" "code=lock_unavailable"
      return 3
    fi
    local flock_status
    if flock -n -E 75 9; then
      return 0
    else
      flock_status="$?"
    fi
    if [[ "$flock_status" -eq 75 ]]; then
      emit "mode=$mode" "code=lock_busy"
      return 75
    fi
    emit "mode=$mode" "code=lock_unavailable"
    return 3
  fi
  if command -v python3 >/dev/null 2>&1; then
    exec python3 - "$MODE_LOCK" "$0" "$mode" <<'PY'
import fcntl
import os
import sys

lock_path, launcher, mode = sys.argv[1:]
try:
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_WRONLY, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print(f"mode={mode} code=lock_busy")
    raise SystemExit(0)
except OSError:
    print(f"mode={mode} code=lock_unavailable")
    raise SystemExit(3)

os.set_inheritable(lock_fd, True)
environment = os.environ.copy()
environment["VP_WORKER_REDIS_MARKER_LOCK_HELD"] = f"{mode}:{lock_fd}"
os.execve(launcher, [launcher, mode], environment)
PY
  fi
  emit "mode=$mode" "code=lock_unavailable"
  return 3
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

expected_service_identity() {
  local mode="$1"
  local database_secret
  local redis_secret
  local module
  local command
  case "$mode" in
    readiness)
      database_secret="$READINESS_DATABASE_SECRET"
      redis_secret="$READINESS_REDIS_SECRET"
      module="app.channel_agent.worker_redis_marker_readiness_cli"
      command="check"
      ;;
    janitor)
      database_secret="$JANITOR_DATABASE_SECRET"
      redis_secret="$JANITOR_REDIS_SECRET"
      module="app.channel_agent.worker_redis_marker_janitor_cli"
      command="run"
      ;;
    *)
      return 1
      ;;
  esac
  local network_identity
  network_identity="$(
    docker network inspect "$NETWORK" \
      --format '{{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}' 2>/dev/null
  )" || return 1
  [[ "$network_identity" == "$NETWORK_ID|$NETWORK|overlay|swarm" ]] \
    || return 1
  printf '%s\n' \
    "2|$mode|$GENERATION|$IMAGE|replicated-job|1|1|none|node.hostname==$CONTROL_NODE|$NETWORK_ID|$database_secret:worker-marker-database-url:256,$redis_secret:worker-marker-redis-url:256|WORKER_REDIS_MARKER_DATABASE_URL_FILE=/run/secrets/worker-marker-database-url,WORKER_REDIS_MARKER_REDIS_URL_FILE=/run/secrets/worker-marker-redis-url|python,-m,$module,$command"
}

service_identity() {
  local name="$1"
  local identity
  identity="$(
    docker service inspect "$name" --format \
      '{{len .Spec.Labels}}|{{index .Spec.Labels "vp.worker-redis-marker.mode"}}|{{index .Spec.Labels "vp.worker-redis-marker.generation"}}|{{.Spec.TaskTemplate.ContainerSpec.Image}}|{{if .Spec.Mode.ReplicatedJob}}replicated-job{{else}}other{{end}}|{{.Spec.Mode.ReplicatedJob.TotalCompletions}}|{{.Spec.Mode.ReplicatedJob.MaxConcurrent}}|{{.Spec.TaskTemplate.RestartPolicy.Condition}}|{{range .Spec.TaskTemplate.Placement.Constraints}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.Networks}}{{printf "%s," .Target}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{printf "%s:%s:%d," .SecretName .File.Name .File.Mode}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Args}}{{printf "%s," .}}{{end}}' \
      2>/dev/null
  )" || return 1
  identity="${identity//,|/|}"
  printf '%s\n' "${identity%,}"
}

remove_completed_job() {
  local mode="$1"
  local name="$2"
  if ! docker service inspect "$name" >/dev/null 2>&1; then
    return 0
  fi
  local expected_identity
  local actual_identity
  expected_identity="$(expected_service_identity "$mode")" || {
    emit "mode=$mode" "code=job_inspection_failed"
    return 3
  }
  actual_identity="$(service_identity "$name")" || {
    emit "mode=$mode" "code=job_inspection_failed"
    return 3
  }
  if [[ "$actual_identity" != "$expected_identity" ]]; then
    emit "mode=$mode" "code=job_identity_invalid"
    return 3
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

invalidate_readiness_status() {
  if ! rm -f "$STATE_DIR/readiness.status"; then
    emit "mode=readiness" "code=status_invalidation_failed"
    return 3
  fi
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
    --no-resolve-image \
    --name "$name" \
    --mode replicated-job \
    --replicas 1 \
    --restart-condition none \
    --constraint "node.hostname==$CONTROL_NODE" \
    --network "$NETWORK_ID" \
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

load_readiness_status() {
  local path="$1"
  local parsed
  parsed="$(
    awk -F= '
      BEGIN {
        allowed["GENERATION"]=1
        allowed["RECORDED_AT"]=1
        allowed["CODE"]=1
        allowed["CHECKED_COUNT"]=1
        allowed["EXPECTED_COUNT"]=1
      }
      {
        if (NF != 2 || $1 == "" || $2 == "" || $0 ~ /\r/ ||
          !($1 in allowed) || seen[$1]++) {
          invalid=1
          exit
        }
        value[$1]=$2
      }
      END {
        if (invalid) {
          exit 1
        }
        for (key in allowed) {
          if (!(key in value)) {
            exit 1
          }
        }
        printf "%s|%s|%s|%s|%s\n",
          value["GENERATION"],
          value["RECORDED_AT"],
          value["CODE"],
          value["CHECKED_COUNT"],
          value["EXPECTED_COUNT"]
      }
    ' "$path"
  )" || return 1
  IFS='|' read -r \
    STATUS_GENERATION \
    STATUS_RECORDED_AT \
    STATUS_CODE \
    STATUS_CHECKED_COUNT \
    STATUS_EXPECTED_COUNT <<<"$parsed"
  [[ "$STATUS_GENERATION" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    && "$STATUS_RECORDED_AT" =~ ^[0-9]+$ \
    && "$STATUS_CODE" =~ ^[a-z0-9_]+$ \
    && "$STATUS_CHECKED_COUNT" =~ ^[0-9]+$ \
    && "$STATUS_EXPECTED_COUNT" =~ ^[0-9]+$ ]]
}

print_sanitized_status() {
  local readiness_path="$STATE_DIR/readiness.status"
  if [[ ! -f "$readiness_path" || -L "$readiness_path" \
    || "$(file_mode "$readiness_path")" != 600 ]]; then
    emit "mode=status" "code=readiness_status_missing"
    return 3
  fi
  if ! load_readiness_status "$readiness_path"; then
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  fi
  if [[ "$STATUS_GENERATION" != "$GENERATION" ]]; then
    emit "mode=status" "code=readiness_status_invalid"
    return 3
  fi
  local now
  now="$(date +%s)"
  if [[ "$STATUS_RECORDED_AT" -gt "$now" \
    || $((now - STATUS_RECORDED_AT)) -gt "$READINESS_MAX_AGE_SECONDS" ]]; then
    emit "mode=status" "code=readiness_status_stale"
    return 3
  fi
  if [[ "$STATUS_CODE" != ready \
    || "$STATUS_CHECKED_COUNT" -ne "$STATUS_EXPECTED_COUNT" ]]; then
    emit "mode=status" "code=readiness_status_unready"
    return 3
  fi
  emit \
    "mode=status" \
    "code=ready" \
    "generation=$STATUS_GENERATION" \
    "checked_count=$STATUS_CHECKED_COUNT" \
    "expected_count=$STATUS_EXPECTED_COUNT"
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
    if acquire_mode_lock readiness; then
      :
    else
      lock_status="$?"
      [[ "$lock_status" -eq 75 ]] && exit 0
      invalidate_readiness_status || exit 3
      exit "$lock_status"
    fi
    invalidate_readiness_status || exit 3
    launch_job \
      readiness \
      "$READINESS_JOB" \
      "$READINESS_DATABASE_SECRET" \
      "$READINESS_REDIS_SECRET" \
      app.channel_agent.worker_redis_marker_readiness_cli \
      check
    ;;
  janitor)
    if acquire_mode_lock janitor; then
      :
    else
      lock_status="$?"
      [[ "$lock_status" -eq 75 ]] && exit 0
      exit "$lock_status"
    fi
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
