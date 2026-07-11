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
GPU_SERVICE_EXISTS=true

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
}

docker() {
  printf 'docker|%s\n' "$*" >>"$CALLS"
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
      *Placement.Constraints*)
        echo 'node.labels.role==app'
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
grep -Fq 'build|10.0.0.150|/home/taiwei/deploy-github-sync/repos/videoprocess/backend|Dockerfile.worker|vp-ffmpeg-worker-python:deploy-0123456789ab' "$CALLS"
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

GPU_SERVICE_EXISTS=false
vp_deploy_python_worker vp-ffmpeg-worker-python:deploy-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi
