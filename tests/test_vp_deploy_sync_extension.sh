#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
CALLS="$(mktemp)"
trap 'status=$?; rm -f "$CALLS"; exit "$status"' EXIT

REPO_ROOT=/home/taiwei/deploy-github-sync/repos
BUILD_IMAGES=1
UPDATE_SERVICES=1
HEALTH_CHECKS=1
VP_YOUTUBE_CREDENTIALS_HOST_DIR=/tmp
VP_API_DATABASE_URL_GO=postgres://test:test@10.0.0.150:5435/videoprocess
VP_PYTHON_WORKER_DATABASE_URL=postgresql+asyncpg://test:test@10.0.0.150:5435/videoprocess
VP_MINIO_ACCESS_KEY=test-access
VP_MINIO_SECRET_KEY=test-secret
GPU_SERVICE_EXISTS=true
CONSTRAINT_MODE=legacy
GPU_PREFLIGHT_SUCCEEDS=true
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
FAIL_RUNNING_SERVICE=

log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}

build_image_on_host() {
  printf 'build|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"
}

http_health() {
  printf 'health|%s|%s\n' "$1" "$2" >>"$CALLS"
}

swarm_service_running() {
  printf 'running|%s\n' "$1" >>"$CALLS"
  [[ "$1" != "$FAIL_RUNNING_SERVICE" ]]
}

docker() {
  printf 'docker|%s\n' "$*" >>"$CALLS"
  if [[ "${1:-}" == "run" && "$GPU_PREFLIGHT_SUCCEEDS" != "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-ffmpeg-worker-gpu-swarm"* ]]; then
    GPU_SERVICE_EXISTS=true
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-ffmpeg-worker-gpu-swarm" ]]; then
    GPU_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && -n "$FAIL_UPDATE_SERVICE" \
    && "$*" == *"--image $FAIL_UPDATE_IMAGE $FAIL_UPDATE_SERVICE"* ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    echo vp-pipeline-network-id
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
    local service="${3:-}"
    if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" && "$GPU_SERVICE_EXISTS" != "true" ]]; then
      return 1
    fi
    case "$*" in
      *ContainerSpec.Image*)
        echo "baseline-$service:stable"
        ;;
      *Placement.Constraints*)
        if [[ "$CONSTRAINT_MODE" == "runtime" ]]; then
          echo 'node.labels.vp.runtime==true'
        else
          echo 'node.labels.role==app'
        fi
        ;;
      *ContainerSpec.Env*)
        if [[ "$service" == "vp-api-swarm" ]]; then
          echo 'DATABASE_URL=legacy'
        elif [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
        fi
        ;;
      *TaskTemplate.Networks*)
        echo legacy-network-id
        ;;
    esac
  fi
}

if [[ ! -f "$EXTENSION" ]]; then
  echo "FAIL: missing deploy extension: $EXTENSION" >&2
  exit 1
fi
source "$EXTENSION"
images="$(build_vp_app_images 0123456789abcdef)"
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: deploy_vp_app_services returned non-zero' >&2
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
grep -Fq -- '--constraint-rm node.labels.role==app' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--env-rm DATABASE_URL' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=colima-127' "$CALLS"

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

VP_GPU_RUNTIME_READY=true
GPU_PREFLIGHT_SUCCEEDS=false
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-preflight-test >/dev/null 2>&1; then
  echo 'FAIL: requested GPU mode must fail when the runtime preflight fails' >&2
  exit 1
fi
grep -Fq 'docker|run --rm --gpus all vp-ffmpeg-worker-python:gpu-preflight-test nvidia-smi' "$CALLS"
VP_GPU_RUNTIME_READY=false
GPU_PREFLIGHT_SUCCEEDS=true

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
FAIL_UPDATE_SERVICE=vp-pds-swarm
FAIL_UPDATE_IMAGE=vp-pds:rollback-test
if deploy_pds_services vp-pds:rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected PDS update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

GPU_SERVICE_EXISTS=false
vp_deploy_python_worker vp-ffmpeg-worker-python:deploy-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi
