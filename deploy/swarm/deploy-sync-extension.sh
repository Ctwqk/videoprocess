#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "deploy-sync-extension.sh must be sourced by deploy-github-sync.sh" >&2
  exit 2
fi

: "${REPO_ROOT:?REPO_ROOT must be set by deploy-github-sync.sh}"

VP_RUNTIME_HOST="${VP_RUNTIME_HOST:-10.0.0.127}"
VP_RUNTIME_NODE="${VP_RUNTIME_NODE:-colima-127}"
VP_RUNTIME_CONSTRAINT="node.labels.vp.runtime==true"
VP_GPU_CONSTRAINT="node.labels.vp.gpu==true"
VP_MANAGER_NODE="${VP_MANAGER_NODE:-ccttww-lap}"
VP_PUBLISHER_CONSTRAINT="node.labels.vp.publisher==true"
VP_PUBLISHER_MANAGER_CONSTRAINT="node.hostname==$VP_MANAGER_NODE"
VP_PIPELINE_NETWORK="${VP_PIPELINE_NETWORK:-vp-pipeline-net}"
VP_PYTHON_WORKER_SERVICE="vp-ffmpeg-worker-gpu-swarm"
VP_PUBLISHER_SERVICE="vp-youtube-publisher-swarm"
VP_APP_SERVICES="vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE $VP_PUBLISHER_SERVICE"

vp_validate_deploy_config() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi

  local missing=""
  [[ -n "${VP_API_DATABASE_URL_GO:-}" ]] || missing="$missing VP_API_DATABASE_URL_GO"
  [[ -n "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]] || missing="$missing VP_PYTHON_WORKER_DATABASE_URL"
  [[ -n "${VP_MINIO_ACCESS_KEY:-}" ]] || missing="$missing VP_MINIO_ACCESS_KEY"
  [[ -n "${VP_MINIO_SECRET_KEY:-}" ]] || missing="$missing VP_MINIO_SECRET_KEY"
  if [[ -n "$missing" ]]; then
    echo "missing required VideoProcess deploy settings:$missing" >&2
    return 1
  fi
}

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

  local service_args=()
  if [[ "$service" == "vp-api-swarm" ]]; then
    service_args+=(--no-healthcheck)
    local api_env_key
    for api_env_key in \
      DATABASE_URL \
      VP_GO_ORCHESTRATOR_ENABLED \
      VP_GO_ORCHESTRATOR_JOB_WRITES; do
      if vp_service_values "$service" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
        | awk -F= -v key="$api_env_key" \
          '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
        service_args+=(--env-rm "$api_env_key")
      fi
    done
    service_args+=(
      --env-add
      "DATABASE_URL=$VP_API_DATABASE_URL_GO"
      --env-add
      "VP_GO_ORCHESTRATOR_ENABLED=true"
      --env-add
      "VP_GO_ORCHESTRATOR_JOB_WRITES=true"
    )
  fi
  if [[ "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
    if vp_service_values "$service" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "WORKER_HOST" { found=1 } END { exit found ? 0 : 1 }'; then
      service_args+=(--env-rm WORKER_HOST)
    fi
    service_args+=(--env-add "WORKER_HOST=$VP_RUNTIME_NODE")
  fi

  local update_args=(
    service update --detach=false --no-resolve-image --update-order "$order"
  )
  if [[ "${#constraint_args[@]}" -gt 0 ]]; then
    update_args+=("${constraint_args[@]}")
  fi
  if [[ "${#service_args[@]}" -gt 0 ]]; then
    update_args+=("${service_args[@]}")
  fi
  update_args+=(--image "$image" "$service")
  docker "${update_args[@]}" >&2
}

vp_build_manager_image() {
  local context_dir="$1"
  local dockerfile="$2"
  local image="$3"
  if [[ "${BUILD_IMAGES:-1}" -eq 0 ]]; then
    log "build skipped 10.0.0.150:$context_dir $image"
    return 0
  fi
  log "build 10.0.0.150:$context_dir $image"
  docker build -f "$context_dir/$dockerfile" -t "$image" "$context_dir" >&2
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
  vp_build_manager_image "$REPO_ROOT/videoprocess/backend" \
    Dockerfile.worker "$python_worker" || return 1

  printf '%s %s %s %s %s %s\n' \
    "$api" "$frontend" "$backend" "$channelops_runner" "$ffmpeg_go" "$python_worker"
}

vp_resolve_gpu_mode() {
  local image="$1"
  case "${VP_GPU_RUNTIME_READY:-false}" in
    true|TRUE|1|yes|YES|on|ON)
      log "preflight NVIDIA runtime with $image"
      if ! docker run --rm --gpus all "$image" nvidia-smi >/dev/null 2>&1; then
        echo "GPU mode requested but the NVIDIA container runtime preflight failed" >&2
        return 1
      fi
      echo "GPU host preflight passed, but Swarm task GPU allocation is not configured" >&2
      return 1
      ;;
    false|FALSE|0|no|NO|off|OFF|'')
      printf 'false\n'
      ;;
    *)
      echo "invalid VP_GPU_RUNTIME_READY value" >&2
      return 1
      ;;
  esac
}

vp_python_worker_env() {
  local use_gpu="$1"
  local db_url="$VP_PYTHON_WORKER_DATABASE_URL"
  local minio_access="$VP_MINIO_ACCESS_KEY"
  local minio_secret="$VP_MINIO_SECRET_KEY"
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
    "NVIDIA_DRIVER_CAPABILITIES=compute,video,utility"
}

vp_publisher_env() {
  local db_url="$VP_PYTHON_WORKER_DATABASE_URL"
  local minio_access="$VP_MINIO_ACCESS_KEY"
  local minio_secret="$VP_MINIO_SECRET_KEY"
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
    "WORKER_TYPE=youtube_publisher" \
    "WORKER_HOST=150-publisher" \
    "WORKER_CONCURRENCY=1" \
    "YOUTUBE_MANAGER_URL=http://10.0.0.150:18999" \
    "YOUTUBE_PUBLISH_ENABLED=true" \
    "PUBLIC_PUBLISH_ENABLED=false"
}

vp_publisher_mount_is_sensitive() {
  local target="$1"
  case "$target" in
    *credential*|*credentials*|*oauth*|*token*)
      return 0
      ;;
  esac
  return 1
}

vp_publisher_env_is_sensitive() {
  local key="$1"
  case "$key" in
    YOUTUBE_MANAGER_URL|YOUTUBE_PUBLISH_ENABLED)
      return 1
      ;;
    YOUTUBE_*|GOOGLE_*|*OAUTH*|*oauth*|*CLIENT_SECRET*|*client_secret*|*ACCESS_TOKEN*|*access_token*|*REFRESH_TOKEN*|*refresh_token*|*CREDENTIALS_JSON|*credentials_json*|*CREDENTIALS_FILE|*credentials_file*|*CREDENTIAL_FILE|*credential_file*)
      return 0
      ;;
  esac
  return 1
}

vp_deploy_python_worker() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PYTHON_WORKER_SERVICE $image"
    return 0
  fi

  local gpu_mode
  gpu_mode="$(vp_resolve_gpu_mode "$image")" || return 1
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
  done < <(vp_python_worker_env "$gpu_mode")

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
    if vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{println .Target}}{{end}}' \
      | grep -Fxq /app/youtube_credentials; then
      update_args+=(--mount-rm /app/youtube_credentials)
    fi
    if vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "YOUTUBE_CREDENTIALS_DIR" { found=1 } END { exit found ? 0 : 1 }'; then
      update_args+=(--env-rm YOUTUBE_CREDENTIALS_DIR)
    fi
    docker "${update_args[@]}" "${env_args[@]}" \
      "$VP_PYTHON_WORKER_SERVICE" >&2
  else
    local create_args=(
      service create --detach=false --name "$VP_PYTHON_WORKER_SERVICE"
      --constraint "$VP_GPU_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-gpu-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_python_worker_env "$gpu_mode")
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2
  fi
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE"
}

vp_deploy_publisher() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PUBLISHER_SERVICE $image"
    return 0
  fi

  http_health vp-youtube-manager "http://10.0.0.150:18999/api/auth/status" || return 1
  docker node update --label-add vp.publisher=true "$VP_MANAGER_NODE" >/dev/null || return 1

  local env_key
  local env_value
  local env_args=()
  local publisher_exists=false
  local inspect_error
  if inspect_error="$(docker service inspect "$VP_PUBLISHER_SERVICE" --format '{{.ID}}' 2>&1)"; then
    publisher_exists=true
  elif [[ "$inspect_error" != *"no such service"* && "$inspect_error" != *"not found"* ]]; then
    printf '%s\n' "$inspect_error" >&2
    return 1
  fi

  local existing_env=""
  if [[ "$publisher_exists" == true ]]; then
    existing_env="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}')" || return 1
  fi
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    if [[ "$publisher_exists" == true ]] \
      && awk -F= -v key="$env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' <<<"$existing_env"; then
      env_args+=(--env-rm "$env_key")
    fi
    env_args+=(--env-add "$env_value")
  done < <(vp_publisher_env)

  if [[ "$publisher_exists" == true ]]; then
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
    )
    local constraint
    local has_publisher=false
    local has_manager=false
    local existing_constraints
    existing_constraints="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}')" || return 1
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      case "$constraint" in
        "$VP_PUBLISHER_CONSTRAINT")
          has_publisher=true
          ;;
        "$VP_PUBLISHER_MANAGER_CONSTRAINT")
          has_manager=true
          ;;
        *)
          update_args+=(--constraint-rm "$constraint")
          ;;
      esac
    done <<<"$existing_constraints"
    if [[ "$has_publisher" != true ]]; then
      update_args+=(--constraint-add "$VP_PUBLISHER_CONSTRAINT")
    fi
    if [[ "$has_manager" != true ]]; then
      update_args+=(--constraint-add "$VP_PUBLISHER_MANAGER_CONSTRAINT")
    fi

    local network_id
    network_id="$(docker network inspect "$VP_PIPELINE_NETWORK" --format '{{.ID}}')" || return 1
    local existing_networks
    existing_networks="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}')" || return 1
    if ! grep -Fxq "$network_id" <<<"$existing_networks"; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK")
    fi

    local existing_mounts
    existing_mounts="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{printf "%s|%s\\n" .Source .Target}}{{end}}')" || return 1
    local mount_source
    local mount_target
    local scratch_needs_add=true
    while IFS='|' read -r mount_source mount_target; do
      [[ -n "$mount_target" ]] || continue
      if [[ "$mount_target" == /data/storage ]]; then
        if [[ "$mount_source" == "vp-youtube-publisher-scratch" ]]; then
          scratch_needs_add=false
        else
          update_args+=(--mount-rm "$mount_target")
          scratch_needs_add=true
        fi
      elif vp_publisher_mount_is_sensitive "$mount_target"; then
        update_args+=(--mount-rm "$mount_target")
      fi
    done <<<"$existing_mounts"
    if [[ "$scratch_needs_add" == true ]]; then
      update_args+=(--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage)
    fi

    while IFS= read -r env_value; do
      env_key="${env_value%%=*}"
      if vp_publisher_env_is_sensitive "$env_key"; then
        update_args+=(--env-rm "$env_key")
      fi
    done <<<"$existing_env"
    docker "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_PUBLISHER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_PUBLISHER_SERVICE"
      --replicas 1
      --constraint "$VP_PUBLISHER_CONSTRAINT"
      --constraint "$VP_PUBLISHER_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_publisher_env)
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_PUBLISHER_SERVICE" || return 1
}

vp_capture_app_snapshots() {
  local service
  local image
  for service in $VP_APP_SERVICES; do
    if ! docker service inspect "$service" >/dev/null 2>&1; then
      if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" \
        || "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
        continue
      fi
      echo "missing required VideoProcess service: $service" >&2
      return 1
    fi
    image="$(vp_service_values "$service" '{{.Spec.TaskTemplate.ContainerSpec.Image}}')" || return 1
    if [[ -z "$image" ]]; then
      echo "missing current image for VideoProcess service: $service" >&2
      return 1
    fi
    printf '%s|%s\n' "$service" "$image"
  done
}

vp_restore_gpu_service() {
  local image="$1"
  local constraint
  local has_gpu=false
  local constraint_args=()
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      node.labels.role==app)
        constraint_args+=(--constraint-rm "$constraint")
        ;;
      "$VP_GPU_CONSTRAINT")
        has_gpu=true
        ;;
    esac
  done < <(
    vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )
  if [[ "$has_gpu" != true ]]; then
    constraint_args+=(--constraint-add "$VP_GPU_CONSTRAINT")
  fi

  local update_args=(
    service update --detach=false --no-resolve-image --update-order stop-first
  )
  if [[ "${#constraint_args[@]}" -gt 0 ]]; then
    update_args+=("${constraint_args[@]}")
  fi
  update_args+=(--image "$image" "$VP_PYTHON_WORKER_SERVICE")
  docker "${update_args[@]}" >&2
}

vp_restore_app_snapshots() {
  local snapshots="$1"
  local service
  local image
  local gpu_was_present=false
  local publisher_was_present=false
  local status=0

  while IFS='|' read -r service image; do
    [[ -n "$service" ]] || continue
    log "restore $service -> $image with dedicated VP placement"
    if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" ]]; then
      gpu_was_present=true
      if ! vp_restore_gpu_service "$image"; then
        status=1
      fi
    elif [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_was_present=true
      if ! vp_deploy_publisher "$image"; then
        status=1
      fi
    elif ! vp_update_runtime_service "$service" "$image" stop-first; then
      status=1
    fi
  done < <(printf '%s\n' "$snapshots")

  if [[ "$gpu_was_present" != true ]] \
    && docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_PYTHON_WORKER_SERVICE"
    if ! docker service rm "$VP_PYTHON_WORKER_SERVICE" >&2; then
      status=1
    fi
  fi
  if [[ "$publisher_was_present" != true ]] \
    && docker service inspect "$VP_PUBLISHER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_PUBLISHER_SERVICE"
    if ! docker service rm "$VP_PUBLISHER_SERVICE" >&2; then
      status=1
    fi
  fi
  return "$status"
}

vp_apply_app_services() {
  local api="$1"
  local frontend="$2"
  local backend="$3"
  local channelops_runner="$4"
  local ffmpeg_go="$5"
  local python_worker="$6"

  vp_update_runtime_service vp-api-swarm "$api" stop-first || return 1
  http_health vp-api "http://$VP_RUNTIME_HOST:18080/health" || return 1
  vp_update_runtime_service vp-frontend-swarm "$frontend" stop-first || return 1
  http_health vp-frontend "http://$VP_RUNTIME_HOST:3001/" || return 1
  vp_update_runtime_service vp-autoflow-api-swarm "$backend" start-first || return 1
  vp_update_runtime_service vp-event-outbox-relay-swarm "$backend" start-first || return 1
  vp_update_runtime_service vp-channel-agent-runner-swarm "$channelops_runner" start-first || return 1
  vp_update_runtime_service vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first || return 1
  vp_deploy_python_worker "$python_worker" || return 1
  vp_deploy_publisher "$python_worker" || return 1

  local service
  for service in $VP_APP_SERVICES; do
    swarm_service_running "$service" || return 1
  done
}

deploy_vp_app_services() {
  vp_validate_deploy_config || return 1

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_apply_app_services "$@" || return 1
    printf '%s\n' "$VP_APP_SERVICES"
    return 0
  fi

  local snapshots
  snapshots="$(vp_capture_app_snapshots)" || return 1
  if ! vp_apply_app_services "$@"; then
    log "VideoProcess service apply failed; restoring prior images without legacy placement"
    if ! vp_restore_app_snapshots "$snapshots"; then
      echo "VideoProcess image restore did not fully converge" >&2
    fi
    return 1
  fi
  printf '%s\n' "$VP_APP_SERVICES"
}

vp_deploy_single_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_update_runtime_service "$service" "$image" "$order" || return 1
    swarm_service_running "$service" || return 1
    printf '%s\n' "$service"
    return 0
  fi

  local baseline_image
  baseline_image="$(vp_service_values "$service" '{{.Spec.TaskTemplate.ContainerSpec.Image}}')" \
    || return 1
  if [[ -z "$baseline_image" ]]; then
    echo "missing current image for VideoProcess service: $service" >&2
    return 1
  fi

  if vp_update_runtime_service "$service" "$image" "$order" \
    && swarm_service_running "$service"; then
    printf '%s\n' "$service"
    return 0
  fi

  log "restore $service -> $baseline_image with dedicated VP placement"
  if ! vp_update_runtime_service "$service" "$baseline_image" stop-first; then
    echo "VideoProcess image restore did not converge for $service" >&2
  fi
  return 1
}

deploy_feature_aggregator_services() {
  vp_deploy_single_runtime_service \
    vp-feature-aggregator-swarm "$1" start-first
}

deploy_pds_services() {
  vp_deploy_single_runtime_service vp-pds-swarm "$1" start-first
}
