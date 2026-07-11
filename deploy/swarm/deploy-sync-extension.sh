#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "deploy-sync-extension.sh must be sourced by deploy-github-sync.sh" >&2
  exit 2
fi

: "${REPO_ROOT:?REPO_ROOT must be set by deploy-github-sync.sh}"

VP_RUNTIME_HOST="${VP_RUNTIME_HOST:-10.0.0.127}"
VP_RUNTIME_CONSTRAINT="node.labels.vp.runtime==true"
VP_GPU_CONSTRAINT="node.labels.vp.gpu==true"
VP_MANAGER_NODE="${VP_MANAGER_NODE:-ccttww-lap}"
VP_PIPELINE_NETWORK="${VP_PIPELINE_NETWORK:-vp-pipeline-net}"
VP_PYTHON_WORKER_SERVICE="vp-ffmpeg-worker-gpu-swarm"

vp_service_values() {
  local service="$1"
  local template="$2"
  docker service inspect "$service" --format "$template"
}

vp_update_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $service $image"
    return 0
  fi

  local constraint
  local has_runtime=false
  local constraint_args=()
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      node.labels.role==app)
        constraint_args+=(--constraint-rm "$constraint")
        ;;
      "$VP_RUNTIME_CONSTRAINT")
        has_runtime=true
        ;;
    esac
  done < <(
    vp_service_values "$service" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )
  if [[ "$has_runtime" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_CONSTRAINT")
  fi

  local api_args=()
  if [[ "$service" == "vp-api-swarm" ]]; then
    api_args+=(--no-healthcheck)
    if vp_service_values "$service" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "DATABASE_URL" { found=1 } END { exit found ? 0 : 1 }'; then
      api_args+=(--env-rm DATABASE_URL)
    fi
    api_args+=(
      --env-add
      "DATABASE_URL=${VP_API_DATABASE_URL_GO:-postgres://vp:vp_secret@10.0.0.150:5435/videoprocess}"
    )
  fi

  local update_args=(
    service update --detach=false --no-resolve-image --update-order "$order"
  )
  update_args+=("${constraint_args[@]}")
  if [[ "${#api_args[@]}" -gt 0 ]]; then
    update_args+=("${api_args[@]}")
  fi
  update_args+=(--image "$image" "$service")
  docker "${update_args[@]}" >&2
}

build_vp_app_images() {
  local commit="$1"
  local short
  short="$(printf '%s' "$commit" | cut -c1-12)"
  local api="vp-api:deploy-$short"
  local frontend="vp-frontend:deploy-$short"
  local backend="vp-backend-api:deploy-$short"
  local channelops_runner="vp-channelops-runner-go:deploy-$short"
  local ffmpeg_go="vp-ffmpeg-worker-go:deploy-$short"
  local python_worker="vp-ffmpeg-worker-python:deploy-$short"

  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.api-go "$api" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app/frontend \
    Dockerfile "$frontend" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app/backend \
    Dockerfile.api "$backend" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.channelops-runner-go "$channelops_runner" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.ffmpeg-worker-go "$ffmpeg_go" || return 1
  build_image_on_host 10.0.0.150 "$REPO_ROOT/videoprocess/backend" \
    Dockerfile.worker "$python_worker" || return 1

  printf '%s %s %s %s %s %s\n' \
    "$api" "$frontend" "$backend" "$channelops_runner" "$ffmpeg_go" "$python_worker"
}

vp_python_worker_env() {
  local db_url="${VP_PYTHON_WORKER_DATABASE_URL:-postgresql+asyncpg://vp:vp_secret@10.0.0.150:5435/videoprocess}"
  local minio_access="${VP_MINIO_ACCESS_KEY:-minioadmin}"
  local minio_secret="${VP_MINIO_SECRET_KEY:-minioadmin}"
  local use_gpu="${VP_GPU_RUNTIME_READY:-false}"
  printf '%s\n' \
    "DEPLOY_MODE=shared" \
    "DATABASE_URL=$db_url" \
    "REDIS_URL=redis://10.0.0.150:6380/0" \
    "STORAGE_BACKEND=minio" \
    "STORAGE_LOCAL_ROOT=/data/storage" \
    "MINIO_ENDPOINT=10.0.0.150:9000" \
    "MINIO_ACCESS_KEY=$minio_access" \
    "MINIO_SECRET_KEY=$minio_secret" \
    "MINIO_BUCKET=videoprocess" \
    "WORKER_TYPE=ffmpeg" \
    "WORKER_HOST=150-gpu" \
    "WORKER_CONCURRENCY=${VP_PYTHON_WORKER_CONCURRENCY:-1}" \
    "VIDEO_USE_GPU=$use_gpu" \
    "VIDEO_GPU_FALLBACK_TO_CPU=true" \
    "NVIDIA_VISIBLE_DEVICES=all" \
    "NVIDIA_DRIVER_CAPABILITIES=compute,video,utility" \
    "YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials"
}

vp_deploy_python_worker() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PYTHON_WORKER_SERVICE $image"
    return 0
  fi

  local credentials_dir="${VP_YOUTUBE_CREDENTIALS_HOST_DIR:-/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager/credentials}"
  if [[ ! -d "$credentials_dir" ]]; then
    echo "missing YouTube credentials directory: $credentials_dir" >&2
    return 1
  fi
  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null

  local env_key
  local env_value
  local env_args=()
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    if docker service inspect "$VP_PYTHON_WORKER_SERVICE" \
      --format '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      2>/dev/null \
      | awk -F= -v key="$env_key" '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
      env_args+=(--env-rm "$env_key")
    fi
    env_args+=(--env-add "$env_value")
  done < <(vp_python_worker_env)

  if docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first
      --image "$image"
    )
    local constraint
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      if [[ "$constraint" == "node.labels.role==app" ]]; then
        update_args+=(--constraint-rm "$constraint")
      fi
    done < <(
      vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
    )
    if ! vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}' \
      | grep -Fxq "$VP_GPU_CONSTRAINT"; then
      update_args+=(--constraint-add "$VP_GPU_CONSTRAINT")
    fi

    local network_id
    network_id="$(docker network inspect "$VP_PIPELINE_NETWORK" --format '{{.ID}}')"
    if ! vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}' \
      | grep -Fxq "$network_id"; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK")
    fi
    if ! vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{println .Target}}{{end}}' \
      | grep -Fxq /app/youtube_credentials; then
      update_args+=(
        --mount-add
        "type=bind,src=$credentials_dir,dst=/app/youtube_credentials,readonly"
      )
    fi
    docker "${update_args[@]}" "${env_args[@]}" \
      "$VP_PYTHON_WORKER_SERVICE" >&2
  else
    local create_args=(
      service create --detach=false --name "$VP_PYTHON_WORKER_SERVICE"
      --constraint "$VP_GPU_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK"
      --restart-condition any --restart-delay 5s
      --mount "type=bind,src=$credentials_dir,dst=/app/youtube_credentials,readonly"
      --mount type=volume,src=vp-gpu-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_python_worker_env)
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2
  fi
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE"
}

deploy_vp_app_services() {
  local api="$1"
  local frontend="$2"
  local backend="$3"
  local channelops_runner="$4"
  local ffmpeg_go="$5"
  local python_worker="$6"
  local services="vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE"

  vp_update_runtime_service vp-api-swarm "$api" stop-first || return 1
  http_health vp-api "http://$VP_RUNTIME_HOST:18080/health" || return 1
  vp_update_runtime_service vp-frontend-swarm "$frontend" stop-first || return 1
  http_health vp-frontend "http://$VP_RUNTIME_HOST:3001/" || return 1
  vp_update_runtime_service vp-autoflow-api-swarm "$backend" start-first || return 1
  vp_update_runtime_service vp-event-outbox-relay-swarm "$backend" start-first || return 1
  vp_update_runtime_service vp-channel-agent-runner-swarm "$channelops_runner" start-first || return 1
  vp_update_runtime_service vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first || return 1
  vp_deploy_python_worker "$python_worker" || return 1

  local service
  for service in $services; do
    swarm_service_running "$service" || return 1
  done
  printf '%s\n' "$services"
}

deploy_feature_aggregator_services() {
  local image="$1"
  vp_update_runtime_service vp-feature-aggregator-swarm "$image" start-first || return 1
  swarm_service_running vp-feature-aggregator-swarm || return 1
  printf '%s\n' vp-feature-aggregator-swarm
}

deploy_pds_services() {
  local image="$1"
  vp_update_runtime_service vp-pds-swarm "$image" start-first || return 1
  swarm_service_running vp-pds-swarm || return 1
  printf '%s\n' vp-pds-swarm
}
