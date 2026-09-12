#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
eval "$(sed -n '/^read_job_output() {/,/^}/p' "$ROOT_DIR/deploy/swarm/worker-redis-marker-control.sh")"
IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
service=aaaaaaaaaaaaaaaaaaaaaaaa
task=eeeeeeeeeeeeeeeeeeeeeeee
node=nnnnnnnnnnnnnnnnnnnnnnnn
container=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
expected_service_identity() { printf 'expected\n'; }
service_identity() {
  [[ "$1" == "$service" ]] || return 1
  printf '%s\n' "${IDENTITY:-expected}"
}
docker() {
  case "$1 ${2:-}" in
    'service logs')
      [[ "$4" == "$service" ]] || return 99
      if [[ "${LOG_TRANSPORT:-failed}" == ok ]]; then
        printf 'swarm-output\n'
        return 0
      fi
      printf 'partial-output-must-be-discarded\n'
      return 1
      ;;
    'service ps')
      [[ "$3" == "$service" ]] || return 99
      printf '%s\n' "${TASKS-$task}"
      ;;
    "inspect $task")
      printf '%s|%s|%s|%s|%s|%s|%s\n' \
        "$task" "${TASK_SERVICE:-$service}" "$node" \
        "${TASK_STATE:-complete}" "${TASK_EXIT:-0}" \
        "${TASK_CONTAINER:-$container}" "${TASK_IMAGE:-$IMAGE}"
      ;;
    'info --format') printf '%s\n' "${LOCAL_NODE:-$node}" ;;
    'container inspect')
      [[ "$3" == "$container" ]] || return 99
      printf '%s|%s|%s|%s|%s|%s|%s\n' \
        "$container" "${CONTAINER_TASK:-$task}" \
        "${CONTAINER_SERVICE:-$service}" "${CONTAINER_NODE:-$node}" \
        "${CONTAINER_STATE:-exited}" "${CONTAINER_EXIT:-0}" \
        "${CONTAINER_IMAGE:-$IMAGE}"
      ;;
    "logs $container")
      [[ "${LOCAL_LOGS_FAIL:-false}" == false ]] || return 1
      printf 'local-output\n'
      ;;
    *) return 99 ;;
  esac
}
output="$(read_job_output readiness "$service")" || {
  echo 'FAIL: completed local task output was unavailable' >&2
  exit 1
}
[[ "$output" == local-output ]] || exit 1
output="$(LOG_TRANSPORT=ok read_job_output readiness "$service")" || exit 1
[[ "$output" == swarm-output ]] || exit 1
for variable in IDENTITY TASK_SERVICE TASK_STATE TASK_EXIT TASK_CONTAINER \
  TASK_IMAGE LOCAL_NODE CONTAINER_TASK CONTAINER_SERVICE CONTAINER_NODE \
  CONTAINER_STATE CONTAINER_EXIT CONTAINER_IMAGE; do
  (
    printf -v "$variable" '%s' invalid
    if read_job_output readiness "$service" >/dev/null; then
      echo "FAIL: accepted mismatched $variable" >&2
      exit 1
    fi
  )
done
for tasks in '' "$task"$'\n'"$task"; do
  if TASKS="$tasks" read_job_output readiness "$service" >/dev/null; then
    echo 'FAIL: accepted missing or ambiguous task identity' >&2
    exit 1
  fi
done
if LOCAL_LOGS_FAIL=true read_job_output readiness "$service" >/dev/null; then
  echo 'FAIL: accepted missing local output' >&2
  exit 1
fi
printf 'worker marker job output identity checks passed\n'
