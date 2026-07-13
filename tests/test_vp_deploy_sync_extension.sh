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
VP_API_DATABASE_URL_GO=postgres://test:test@10.0.0.150:5435/videoprocess
VP_PYTHON_WORKER_DATABASE_URL=postgresql+asyncpg://test:test@10.0.0.150:5435/videoprocess
VP_MINIO_ACCESS_KEY=test-access
VP_MINIO_SECRET_KEY=test-secret
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
CONSTRAINT_MODE=legacy
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_NETWORK_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials
PUBLISHER_REPLICAS=3
GPU_PREFLIGHT_SUCCEEDS=true
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
FAIL_RUNNING_SERVICE=
FAIL_HEALTH_CHECK=
FAIL_NODE_UPDATE=false
FAIL_NETWORK_INSPECT=false
FAIL_PUBLISHER_CREATE=false

log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
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

docker() {
  printf 'docker|%s\n' "$*" >>"$CALLS"
  if [[ "${1:-}" == "run" && "$GPU_PREFLIGHT_SUCCEEDS" != "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "node update" && "$FAIL_NODE_UPDATE" == "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-ffmpeg-worker-gpu-swarm"* ]]; then
    GPU_SERVICE_EXISTS=true
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
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-youtube-publisher-swarm" ]]; then
    PUBLISHER_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && -n "$FAIL_UPDATE_SERVICE" \
    && "$*" == *"--image $FAIL_UPDATE_IMAGE $FAIL_UPDATE_SERVICE"* ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    if [[ "$FAIL_NETWORK_INSPECT" == "true" ]]; then
      return 1
    fi
    echo vp-pipeline-network-id
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
    local service="${3:-}"
    if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" && "$GPU_SERVICE_EXISTS" != "true" ]]; then
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" && "$PUBLISHER_SERVICE_EXISTS" != "true" ]]; then
      echo "no such service: $service" >&2
      return 1
    fi
    case "$*" in
      *ContainerSpec.Image*)
        echo "baseline-$service:stable"
        ;;
      *Spec.Mode.Replicated.Replicas*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo "$PUBLISHER_REPLICAS"
        fi
        ;;
      *Placement.Constraints*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
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
        else
          echo 'node.labels.role==app'
        fi
        ;;
      *ContainerSpec.Env*)
        if [[ "$service" == "vp-api-swarm" ]]; then
          echo 'DATABASE_URL=legacy'
        elif [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
          echo 'YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials'
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
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          case "$PUBLISHER_MOUNT_MODE" in
            desired)
              echo 'vp-youtube-publisher-scratch|/data/storage'
              ;;
            wrong)
              echo 'legacy-publisher-scratch|/data/storage'
              echo 'legacy-auth|/app/youtube_credentials'
              echo 'legacy-auth|/app/credentials'
              echo 'legacy-auth|~/.youtube_credentials'
              echo 'legacy-auth|/var/run/oauth-token'
              ;;
            missing)
              echo 'legacy-auth|/app/credentials'
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
if grep -Eq 'YOUTUBE_CREDENTIALS_DIR=|VP_YOUTUBE|--mount-add.*youtube_credentials|--mount .*youtube_credentials' "$EXTENSION"; then
  echo 'FAIL: general production worker must not receive publication credentials' >&2
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
if ! grep -Fq 'vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: deployment must include the dedicated YouTube publisher' >&2
  exit 1
fi
grep -Fq -- '--constraint-rm node.labels.role==app' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--env-rm DATABASE_URL' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_ENABLED=true' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_JOB_WRITES=true' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=colima-127' "$CALLS"
grep -Fq -- '--mount-rm /app/youtube_credentials' "$CALLS"
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
grep -Fq -- '--mount-rm /app/credentials' "$CALLS"
grep -Fq -- '--mount-rm ~/.youtube_credentials' "$CALLS"
grep -Fq -- '--mount-rm /var/run/oauth-token' "$CALLS"
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
  || grep -Fq -- '--mount-rm /data/storage' "$CALLS"; then
  echo 'FAIL: repeat publisher update must not duplicate desired placement, network, or scratch mount' >&2
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
if grep -Fq 'docker|service update' "$CALLS"; then
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

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-net' "$CALLS"
grep -Fq -- '--mount type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
