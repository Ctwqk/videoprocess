#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "deploy-sync-extension.sh must be sourced by deploy-github-sync.sh" >&2
  exit 2
fi

: "${REPO_ROOT:?REPO_ROOT must be set by deploy-github-sync.sh}"

VP_RUNTIME_HOST="${VP_RUNTIME_HOST:-10.0.0.127}"
VP_RUNTIME_NODE="${VP_RUNTIME_NODE:-colima-127}"
VP_RUNTIME_CONSTRAINT="node.labels.vp.runtime==true"
VP_RUNTIME_NODE_CONSTRAINT="node.hostname==$VP_RUNTIME_NODE"
VP_GPU_CONSTRAINT="node.labels.vp.gpu==true"
VP_MANAGER_NODE="${VP_MANAGER_NODE:-ccttww-lap}"
VP_GPU_MANAGER_CONSTRAINT="node.hostname==$VP_MANAGER_NODE"
VP_PUBLISHER_CONSTRAINT="node.labels.vp.publisher==true"
VP_PUBLISHER_MANAGER_CONSTRAINT="node.hostname==$VP_MANAGER_NODE"
VP_PIPELINE_NETWORK="${VP_PIPELINE_NETWORK:-vp-pipeline-net}"
VP_APP_CI_REPOSITORY="Ctwqk/videoprocess"
VP_APP_CI_WORKFLOW="ci.yml"
VP_PDS_CI_REPOSITORY="Ctwqk/policy-decision-service"
VP_PDS_CI_WORKFLOW="ci.yml"
VP_PDS_SERVICE="vp-pds-swarm"
VP_PDS_HTTP_ADDR=":8080"
VP_PDS_HEALTH_TEST='["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]'
VP_SERVICE_UPDATE_NOT_ATTEMPTED=2
VP_PYTHON_WORKER_SERVICE="vp-ffmpeg-worker-gpu-swarm"
VP_VISION_WORKER_SERVICE="vp-vision-worker-swarm"
VP_PUBLISHER_SERVICE="vp-youtube-publisher-swarm"
VP_APP_SERVICES="vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE"
VP_WORKER_REDIS_RUNTIME_ACL_IDENTITY="vp-marker-acl-v1"
VP_WORKER_REDIS_MARKER_CONTROL_SOURCE="${VP_WORKER_REDIS_MARKER_CONTROL_SOURCE:-$REPO_ROOT/videoprocess/deploy/swarm/worker-redis-marker-control.sh}"
VP_APP_ATTEMPTED_SERVICES=""
VP_BACKEND_MIGRATION_APPLIED=false
VP_VISION_CUTOVER_REQUIRED=false
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false

vp_validate_topology() {
  if [[ "${BUILD_IMAGES:-1}" -eq 0 && "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi

  if [[ "$VP_RUNTIME_HOST" != "10.0.0.127" ]] \
    || [[ "$VP_RUNTIME_NODE" != "colima-127" ]] \
    || [[ "$VP_MANAGER_NODE" != "ccttww-lap" ]]; then
    echo "VideoProcess deployment topology must remain fixed to 127 and 150" >&2
    return 1
  fi
}

vp_validate_deploy_config() {
  vp_validate_topology || return 1
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

vp_require_channelops_migration_head() {
  local python_worker="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "ChannelOps migration head gate skipped because service updates are disabled"
    return 0
  fi
  if [[ -z "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]]; then
    echo "ChannelOps migration head gate requires VP_PYTHON_WORKER_DATABASE_URL" >&2
    return 1
  fi

  local check
  check='import asyncio, os; from sqlalchemy import text; from sqlalchemy.ext.asyncio import create_async_engine; exec("async def check():\n    engine = create_async_engine(os.environ[\"DATABASE_URL\"])\n    try:\n        async with engine.connect() as connection:\n            rows = list((await connection.execute(text(\"SELECT version_num FROM alembic_version\"))).scalars())\n    except Exception:\n        raise SystemExit(1)\n    finally:\n        await engine.dispose()\n    if rows != [\"033_legacy_worker_event_resolutions\"]:\n        raise SystemExit(1)"); asyncio.run(check())'
  if ! DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" docker run --rm \
    --network "$VP_PIPELINE_NETWORK" \
    --env DATABASE_URL \
    "$python_worker" \
    python -c "$check" >/dev/null; then
    echo "ChannelOps migration head gate failed; expected exactly 033_legacy_worker_event_resolutions" >&2
    return 1
  fi
  log "ChannelOps migration head verified: 033_legacy_worker_event_resolutions"
}

vp_require_vision_cutover_safe() {
  local python_worker="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "vision cutover gate skipped because service updates are disabled"
    return 0
  fi
  if [[ -z "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]]; then
    echo "vision cutover gate requires VP_PYTHON_WORKER_DATABASE_URL" >&2
    return 1
  fi

  local check
  check='import asyncio, os; from sqlalchemy import text; from sqlalchemy.ext.asyncio import create_async_engine; import redis.asyncio as redis; exec("async def check():\n    engine = create_async_engine(os.environ[\"DATABASE_URL\"])\n    client = redis.from_url(os.environ[\"REDIS_URL\"], decode_responses=True)\n    try:\n        async with engine.connect() as connection:\n            schedule = (await connection.execute(text(\"SELECT state, guarded_job_id FROM runtime_schedules WHERE service_name = '\''videoprocess'\''\"))).one_or_none()\n            active_nodes = int((await connection.execute(text(\"SELECT count(*) FROM node_executions WHERE status::text IN ('\''QUEUED'\'', '\''RUNNING'\'')\"))).scalar_one())\n        if schedule is None or schedule.state != \"CLOSED\" or schedule.guarded_job_id is not None or active_nodes != 0:\n            raise SystemExit(1)\n        pending = await client.xpending(\"vp:tasks:vision\", \"vision-workers\")\n        groups = await client.xinfo_groups(\"vp:tasks:vision\")\n        group = next((row for row in groups if row.get(\"name\") == \"vision-workers\"), None)\n        if not isinstance(pending, dict) or pending.get(\"pending\") != 0 or group is None or group.get(\"lag\") != 0:\n            raise SystemExit(1)\n    except Exception:\n        raise SystemExit(1)\n    finally:\n        await client.aclose()\n        await engine.dispose()"); asyncio.run(check())'
  if ! DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" \
    REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK" \
      --env DATABASE_URL \
      --env REDIS_URL \
      "$python_worker" \
      python -c "$check" >/dev/null; then
    echo "vision cutover gate failed; require CLOSED schedule and idle vision work" >&2
    return 1
  fi
  log "vision cutover gate verified: CLOSED and idle"
}

vp_vision_cutover_required() {
  local python_worker="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    printf 'false\n'
    return 0
  fi

  local legacy_names
  legacy_names="$(docker container ls -a \
    --filter 'name=^/vp_vision_worker_1$' \
    --format '{{.Names}}')" || return 1
  case "$legacy_names" in
    vp_vision_worker_1)
      printf 'true\n'
      return 0
      ;;
    '')
      ;;
    *)
      echo "unexpected legacy vision container list result" >&2
      return 1
      ;;
  esac

  if ! docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    printf 'true\n'
    return 0
  fi

  if REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK" \
      --env REDIS_URL \
      "$python_worker" \
      python -m app.services.vision_consumer_cutover --check-only >/dev/null; then
    printf 'false\n'
  else
    printf 'true\n'
  fi
}

vp_service_values() {
  local service="$1"
  local template="$2"
  docker service inspect "$service" --format "$template"
}

vp_require_service_node() {
  local service="$1"
  local expected_node="$2"
  local running_tasks
  running_tasks="$(docker service ps "$service" \
    --filter desired-state=running \
    --format '{{.Node}}|{{.CurrentState}}')" || return 1
  if ! awk -F'|' -v expected="$expected_node" '
    NF {
      total++
      if ($1 == expected && $2 ~ /^Running([[:space:]]|$)/) {
        matched++
      }
    }
    END {
      exit total == 1 && matched == 1 ? 0 : 1
    }
  ' <<<"$running_tasks"; then
    echo "service $service is not running exactly once on $expected_node" >&2
    return 1
  fi
}

vp_gpu_constraint_update_args() {
  local existing_constraints="$1"
  local gpu_count=0
  local manager_count=0
  local constraint
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      "$VP_GPU_CONSTRAINT")
        gpu_count=$((gpu_count + 1))
        ;;
      "$VP_GPU_MANAGER_CONSTRAINT")
        manager_count=$((manager_count + 1))
        ;;
      *)
        printf '%s\n%s\n' --constraint-rm "$constraint"
        ;;
    esac
  done <<<"$existing_constraints"

  if [[ "$gpu_count" -gt 1 || "$manager_count" -gt 1 ]]; then
    echo "GPU worker has duplicate approved placement constraints" >&2
    return 1
  fi
  if [[ "$gpu_count" -eq 0 ]]; then
    printf '%s\n%s\n' --constraint-add "$VP_GPU_CONSTRAINT"
  fi
  if [[ "$manager_count" -eq 0 ]]; then
    printf '%s\n%s\n' --constraint-add "$VP_GPU_MANAGER_CONSTRAINT"
  fi
}

vp_require_managed_worker_storage_ready() {
  local service="$1"
  local require_artifact_api="${2:-false}"
  local containers
  local container_count=0
  local attempt
  for ((attempt = 1; attempt <= 10; attempt++)); do
    if ! containers="$(
      docker container ls \
        --filter "label=com.docker.swarm.service.name=$service" \
        --filter status=running \
        --format '{{.ID}}' \
        2>/dev/null
    )"; then
      echo "managed worker container discovery failed: $service" >&2
      return 1
    fi
    if ! container_count="$(
      printf '%s\n' "$containers" \
        | awk 'NF { count++ } END { print count+0 }' 2>/dev/null
    )"; then
      echo "managed worker container count failed: $service" >&2
      return 1
    fi
    if [[ "$container_count" -eq 1 ]]; then
      break
    fi
    if [[ "$attempt" -lt 10 ]]; then
      if ! sleep 1; then
        echo "managed worker readiness wait failed: $service" >&2
        return 1
      fi
    fi
  done
  if [[ "$container_count" -ne 1 ]]; then
    echo "managed worker storage readiness requires exactly one local running task: $service" >&2
    return 1
  fi
  local args=(python -m app.channel_agent.worker_storage_readiness_cli)
  if [[ "$require_artifact_api" == true ]]; then
    args+=(--require-artifact-api)
  elif [[ "$require_artifact_api" != false ]]; then
    echo "invalid managed worker artifact API readiness mode" >&2
    return 1
  fi
  if ! docker exec "$containers" "${args[@]}" >/dev/null 2>&1; then
    echo "managed worker storage readiness failed: $service" >&2
    return 1
  fi
  log "managed worker storage readiness passed: $service"
}

vp_require_github_actions_success() {
  local repository="$1"
  local workflow="$2"
  local commit="$3"

  if [[ "${BUILD_IMAGES:-1}" -eq 0 && "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  if [[ ! "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "invalid GitHub Actions repository" >&2
    return 1
  fi
  if [[ ! "$workflow" =~ ^[A-Za-z0-9_.-]+\.ya?ml$ ]]; then
    echo "invalid GitHub Actions workflow file" >&2
    return 1
  fi
  if [[ ! "$commit" =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid Git commit for GitHub Actions gate" >&2
    return 1
  fi
  if ! command -v gh >/dev/null 2>&1; then
    echo "GitHub CLI is required for applying VideoProcess deployments" >&2
    return 1
  fi

  local filter
  filter='if (.workflow_runs | length) == 0 then
    ["missing", "", "", "", ""] | @tsv
  else
    (.workflow_runs | max_by([.run_number // 0, .run_attempt // 0])) as $run |
    ["found", $run.status, ($run.conclusion // ""), $run.head_sha, ($run.id | tostring)] | @tsv
  end'
  local record
  if ! record="$(
    gh api --method GET \
      "repos/$repository/actions/workflows/$workflow/runs" \
      -f "head_sha=$commit" \
      -f event=push \
      -f per_page=20 \
      --jq "$filter"
  )"; then
    echo "GitHub Actions lookup failed for $repository@$commit" >&2
    return 1
  fi
  if [[ -z "$record" || "$record" == *$'\n'* ]]; then
    echo "GitHub Actions lookup returned an invalid result for $repository@$commit" >&2
    return 1
  fi

  local marker
  local run_status
  local conclusion
  local head_sha
  local run_id
  marker="$(awk -F '\t' '{print $1}' <<<"$record")"
  run_status="$(awk -F '\t' '{print $2}' <<<"$record")"
  conclusion="$(awk -F '\t' '{print $3}' <<<"$record")"
  head_sha="$(awk -F '\t' '{print $4}' <<<"$record")"
  run_id="$(awk -F '\t' '{print $5}' <<<"$record")"

  if [[ "$marker" != found ]]; then
    echo "no push CI run exists for $repository@$commit" >&2
    return 1
  fi
  if [[ "$head_sha" != "$commit" ]]; then
    echo "GitHub Actions head SHA mismatch for $repository@$commit" >&2
    return 1
  fi
  if [[ "$run_status" != completed || "$conclusion" != success ]]; then
    echo "GitHub Actions run is not successful for $repository@$commit: status=$run_status conclusion=${conclusion:-none}" >&2
    return 1
  fi
  if [[ ! "$run_id" =~ ^[0-9]+$ ]]; then
    echo "GitHub Actions run ID is invalid for $repository@$commit" >&2
    return 1
  fi

  log "GitHub Actions gate passed $repository@$commit workflow=$workflow run=$run_id"
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
  local has_runtime_node=false
  local constraint_args=()
  local existing_constraints
  if ! existing_constraints="$(
    vp_service_values "$service" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )"; then
    echo "service constraint inspection failed: $service" >&2
    return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
  fi
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      "$VP_RUNTIME_CONSTRAINT")
        has_runtime=true
        ;;
      "$VP_RUNTIME_NODE_CONSTRAINT")
        has_runtime_node=true
        ;;
      *)
        constraint_args+=(--constraint-rm "$constraint")
        ;;
    esac
  done <<<"$existing_constraints"
  if [[ "$has_runtime" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_CONSTRAINT")
  fi
  if [[ "$has_runtime_node" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_NODE_CONSTRAINT")
  fi

  local service_args=()
  if [[ "$service" == "vp-api-swarm" ]]; then
    service_args+=(--no-healthcheck)
    local api_env_key
    for api_env_key in \
      DATABASE_URL \
      VP_GO_ORCHESTRATOR_ENABLED \
      VP_GO_ORCHESTRATOR_JOB_WRITES \
      VP_PYTHON_SCHEDULE_URL; do
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
      --env-add
      "VP_PYTHON_SCHEDULE_URL=http://vp-autoflow-api-swarm:8080"
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
  if [[ "$service" == "vp-channel-agent-runner-swarm" ]]; then
    local channelops_env_key
    for channelops_env_key in \
      CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS \
      CHANNELOPS_RUNNER_ID; do
      if vp_service_values "$service" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
        | awk -F= -v key="$channelops_env_key" \
          '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
        service_args+=(--env-rm "$channelops_env_key")
      fi
    done
    service_args+=(
      --env-add
      "CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120"
      --env-add
      "CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1"
      --health-cmd
      "wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1"
      --health-interval
      "10s"
      --health-timeout
      "3s"
      --health-retries
      "6"
      --health-start-period
      "10s"
    )
  fi
  if [[ "$service" == "$VP_PDS_SERVICE" ]]; then
    if vp_service_values "$service" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "PDS_HTTP_ADDR" { found=1 } END { exit found ? 0 : 1 }'; then
      service_args+=(--env-rm PDS_HTTP_ADDR)
    fi
    service_args+=(
      --env-add
      "PDS_HTTP_ADDR=$VP_PDS_HTTP_ADDR"
      --health-cmd
      ""
      --health-interval
      "10s"
      --health-timeout
      "3s"
      --health-retries
      "6"
      --health-start-period
      "10s"
    )
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
  docker "${update_args[@]}" >&2 || return 1
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
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_APP_CI_REPOSITORY" "$VP_APP_CI_WORKFLOW" "$commit" || return 1
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

build_feature_aggregator_images() {
  local commit="$1"
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_APP_CI_REPOSITORY" "$VP_APP_CI_WORKFLOW" "$commit" || return 1
  local tag
  tag="$(image_tag vp-feature-aggregator "$commit")"
  build_image_on_host "$VP_RUNTIME_HOST" \
    /Users/wenjieliu/.deploy-build/vp-feature-aggregator \
    deploy/Dockerfile "$tag" || return 1
  printf '%s\n' "$tag"
}

build_pds_images() {
  local commit="$1"
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_PDS_CI_REPOSITORY" "$VP_PDS_CI_WORKFLOW" "$commit" || return 1
  local tag
  tag="$(image_tag vp-pds "$commit")"
  build_image_on_host "$VP_RUNTIME_HOST" \
    /Users/wenjieliu/.deploy-build/policy-decision-service \
    deploy/Dockerfile "$tag" || return 1
  printf '%s\n' "$tag"
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

vp_vision_worker_env() {
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
    "WORKER_TYPE=vision" \
    "WORKER_HOST=150-vision" \
    "WORKER_CONCURRENCY=${VP_VISION_WORKER_CONCURRENCY:-1}" \
    "VP_ARTIFACT_DOWNLOAD_BASE_URL=http://vp-api-swarm:8080/api/v1" \
    "VISION_EMBEDDING_URL=${VP_VISION_EMBEDDING_URL:-}" \
    "VIDEO_USE_GPU=false" \
    "VIDEO_GPU_FALLBACK_TO_CPU=true" \
    "VIDEO_WHISPER_DEVICE=cpu"
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

vp_publisher_service_state() {
  local service_names
  service_names="$(docker service ls \
    --filter "name=$VP_PUBLISHER_SERVICE" \
    --format '{{.Name}}')" || return 1
  case "$service_names" in
    "$VP_PUBLISHER_SERVICE")
      printf 'exists\n'
      ;;
    '')
      printf 'absent\n'
      ;;
    *)
      echo "unexpected publisher service list result" >&2
      return 1
      ;;
  esac
}

vp_deploy_python_worker() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PYTHON_WORKER_SERVICE $image"
    return 0
  fi

  local gpu_mode
  gpu_mode="$(vp_resolve_gpu_mode "$image")" || return 1
  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null || return 1

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
    local existing_constraints
    existing_constraints="$(
      vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
    )" || return 1
    local constraint_args
    constraint_args="$(vp_gpu_constraint_update_args "$existing_constraints")" || return 1
    local constraint
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      update_args+=("$constraint")
    done <<<"$constraint_args"

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
      "$VP_PYTHON_WORKER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_PYTHON_WORKER_SERVICE"
      --constraint "$VP_GPU_CONSTRAINT"
      --constraint "$VP_GPU_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-gpu-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_python_worker_env "$gpu_mode")
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_deploy_vision_worker() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_VISION_WORKER_SERVICE $image"
    return 0
  fi

  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null || return 1

  local vision_exists=false
  local existing_env=""
  if docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    vision_exists=true
    existing_env="$(vp_service_values "$VP_VISION_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}')" || return 1
  fi

  local env_key
  local env_value
  local env_args=()
  local desired_env_keys=""
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    desired_env_keys="${desired_env_keys}${desired_env_keys:+$'\n'}$env_key"
    if [[ "$vision_exists" == true ]] \
      && awk -F= -v key="$env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' <<<"$existing_env"; then
      env_args+=(--env-rm "$env_key")
    fi
    if [[ "$vision_exists" == true ]]; then
      env_args+=(--env-add "$env_value")
    fi
  done < <(vp_vision_worker_env)

  if [[ "$vision_exists" == true ]]; then
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
    )
    local constraint
    local has_gpu=false
    local has_manager=false
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      if [[ "$constraint" == "$VP_GPU_CONSTRAINT" ]]; then
        has_gpu=true
      elif [[ "$constraint" == "$VP_GPU_MANAGER_CONSTRAINT" ]]; then
        has_manager=true
      else
        update_args+=(--constraint-rm "$constraint")
      fi
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
    )
    if [[ "$has_gpu" != true ]]; then
      update_args+=(--constraint-add "$VP_GPU_CONSTRAINT")
    fi
    if [[ "$has_manager" != true ]]; then
      update_args+=(--constraint-add "$VP_GPU_MANAGER_CONSTRAINT")
    fi

    local network_id
    network_id="$(docker network inspect "$VP_PIPELINE_NETWORK" --format '{{.ID}}')" || return 1
    local network_target
    local has_pipeline_network=false
    while IFS= read -r network_target; do
      [[ -n "$network_target" ]] || continue
      if [[ "$network_target" == "$network_id" ]]; then
        has_pipeline_network=true
      else
        update_args+=(--network-rm "$network_target")
      fi
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}'
    )
    if [[ "$has_pipeline_network" != true ]]; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK")
    fi

    local existing_mounts
    existing_mounts="$(vp_service_values "$VP_VISION_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{printf "%s|%s|%s|%t\n" .Type .Source .Target .ReadOnly}}{{end}}')" || return 1
    local mount_type
    local mount_source
    local mount_target
    local mount_readonly
    local desired_scratch_count=0
    local remove_scratch_target=false
    while IFS='|' read -r mount_type mount_source mount_target mount_readonly; do
      [[ -n "$mount_type$mount_source$mount_target$mount_readonly" ]] || continue
      if [[ -z "$mount_target" ]]; then
        echo "vision worker mount has no target" >&2
        return 1
      fi
      if [[ "$mount_type" == volume \
        && "$mount_source" == "vp-vision-worker-scratch" \
        && "$mount_target" == /data/storage \
        && "$mount_readonly" == false ]]; then
        desired_scratch_count=$((desired_scratch_count + 1))
        if [[ "$desired_scratch_count" -gt 1 ]]; then
          remove_scratch_target=true
        fi
      elif [[ "$mount_target" == /data/storage ]]; then
        remove_scratch_target=true
      else
        update_args+=(--mount-rm "$mount_target")
      fi
    done <<<"$existing_mounts"
    if [[ "$remove_scratch_target" == true ]]; then
      docker service update --detach=false --no-resolve-image --update-order stop-first \
        --replicas 0 --mount-rm /data/storage "$VP_VISION_WORKER_SERVICE" >&2 || return 1
      desired_scratch_count=0
    fi
    if [[ "$desired_scratch_count" -ne 1 ]]; then
      update_args+=(--mount-add type=volume,src=vp-vision-worker-scratch,dst=/data/storage)
    fi

    local existing_secret
    while IFS= read -r existing_secret; do
      [[ -n "$existing_secret" ]] || continue
      update_args+=(--secret-rm "$existing_secret")
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}'
    )
    local existing_config
    while IFS= read -r existing_config; do
      [[ -n "$existing_config" ]] || continue
      update_args+=(--config-rm "$existing_config")
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Configs}}{{println .ConfigName}}{{end}}'
    )
    while IFS= read -r env_value; do
      env_key="${env_value%%=*}"
      if ! grep -Fxq "$env_key" <<<"$desired_env_keys"; then
        update_args+=(--env-rm "$env_key")
      fi
    done <<<"$existing_env"

    docker "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_VISION_WORKER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_VISION_WORKER_SERVICE"
      --replicas 1
      --constraint "$VP_GPU_CONSTRAINT"
      --constraint "$VP_GPU_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-vision-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_vision_worker_env)
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_VISION_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_VISION_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_VISION_WORKER_SERVICE" true
}

vp_retire_legacy_vision_worker() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "legacy vision worker retirement skipped"
    return 0
  fi

  local container="vp_vision_worker_1"
  local inspected
  if ! inspected="$(
    docker container inspect \
      --format '{{.Id}}|{{.Name}}|{{.State.Running}}|{{index .Config.Labels "com.docker.compose.project"}}|{{index .Config.Labels "com.docker.compose.service"}}' \
      "$container" 2>/dev/null
  )"; then
    local matching_names
    matching_names="$(docker container ls -a \
      --filter "name=^/$container$" \
      --format '{{.Names}}')" || return 1
    if [[ -z "$matching_names" ]]; then
      return 0
    fi
    echo "legacy vision worker identity could not be inspected" >&2
    return 1
  fi
  local container_id
  local container_name
  local container_running
  local compose_project
  local compose_service
  local extra
  IFS='|' read -r \
    container_id container_name container_running compose_project compose_service extra \
    <<<"$inspected"
  if [[ ! "$container_id" =~ ^[0-9a-f]{64}$ ]] \
    || [[ "$container_name" != "/$container" ]] \
    || [[ "$container_running" != true ]] \
    || [[ "$compose_project" != videoprocess ]] \
    || [[ "$compose_service" != vision-worker ]] \
    || [[ -n "$extra" ]]; then
    echo "refusing to remove unexpected legacy vision container identity" >&2
    return 1
  fi
  docker rm -f "$container_id" >&2
}

vp_reconcile_vision_consumers() {
  local python_worker="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "vision consumer reconciliation skipped"
    return 0
  fi

  if ! REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK" \
      --env REDIS_URL \
      "$python_worker" \
      python -m app.services.vision_consumer_cutover >/dev/null; then
    echo "vision consumer reconciliation failed" >&2
    return 1
  fi
  log "vision consumer reconciliation verified"
}

vp_deploy_publisher() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PUBLISHER_SERVICE $image"
    return 0
  fi

  http_health vp-youtube-manager "http://10.0.0.150:18999/api/auth/status" || return 1

  local env_key
  local env_value
  local env_args=()
  local publisher_exists=false
  local publisher_state
  publisher_state="$(vp_publisher_service_state)" || return 1
  case "$publisher_state" in
    exists)
      publisher_exists=true
      ;;
    absent)
      ;;
    *)
      return 1
      ;;
  esac

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
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{printf "%s|%s|%s|%t\\n" .Type .Source .Target .ReadOnly}}{{end}}')" || return 1
    local mount_type
    local mount_source
    local mount_target
    local mount_readonly
    local desired_scratch_count=0
    local rebuild_scratch=false
    local remove_scratch_target=false
    while IFS='|' read -r mount_type mount_source mount_target mount_readonly; do
      [[ -n "$mount_type$mount_source$mount_target$mount_readonly" ]] || continue
      if [[ -z "$mount_target" ]]; then
        echo "publisher mount has no target" >&2
        return 1
      fi
      if [[ "$mount_type" == volume \
        && "$mount_source" == "vp-youtube-publisher-scratch" \
        && "$mount_target" == /data/storage \
        && "$mount_readonly" == false ]]; then
        desired_scratch_count=$((desired_scratch_count + 1))
        if [[ "$desired_scratch_count" -gt 1 ]]; then
          remove_scratch_target=true
          rebuild_scratch=true
        fi
      else
        if [[ "$mount_target" == /data/storage ]]; then
          remove_scratch_target=true
          rebuild_scratch=true
        else
          update_args+=(--mount-rm "$mount_target")
        fi
      fi
    done <<<"$existing_mounts"
    if [[ "$desired_scratch_count" -ne 1 || "$rebuild_scratch" == true ]]; then
      update_args+=(--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage)
    fi

    local existing_secrets
    existing_secrets="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}')" || return 1
    local secret_name
    while IFS= read -r secret_name; do
      [[ -n "$secret_name" ]] || continue
      update_args+=(--secret-rm "$secret_name")
    done <<<"$existing_secrets"

    local existing_configs
    existing_configs="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Configs}}{{println .ConfigName}}{{end}}')" || return 1
    local config_name
    while IFS= read -r config_name; do
      [[ -n "$config_name" ]] || continue
      update_args+=(--config-rm "$config_name")
    done <<<"$existing_configs"

    while IFS= read -r env_value; do
      env_key="${env_value%%=*}"
      if vp_publisher_env_is_sensitive "$env_key"; then
        update_args+=(--env-rm "$env_key")
      fi
    done <<<"$existing_env"
    docker node update --label-add vp.publisher=true "$VP_MANAGER_NODE" >/dev/null || return 1
    if [[ "$remove_scratch_target" == true ]]; then
      docker service update --detach=false --no-resolve-image --update-order stop-first \
        --replicas 0 --mount-rm /data/storage "$VP_PUBLISHER_SERVICE" >&2 || return 1
    fi
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
    docker node update --label-add vp.publisher=true "$VP_MANAGER_NODE" >/dev/null || return 1
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_PUBLISHER_SERVICE" || return 1
  vp_require_service_node "$VP_PUBLISHER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false
}

vp_capture_app_snapshots() {
  local service
  local image
  local publisher_state
  for service in $VP_APP_SERVICES; do
    if [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_state="$(vp_publisher_service_state)" || return 1
      if [[ "$publisher_state" == absent ]]; then
        continue
      fi
    elif ! docker service inspect "$service" >/dev/null 2>&1; then
      if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" \
        || "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
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

vp_record_app_service_attempt() {
  local service="$1"
  case " $VP_APP_ATTEMPTED_SERVICES " in
    *" $service "*)
      return 0
      ;;
  esac
  VP_APP_ATTEMPTED_SERVICES="${VP_APP_ATTEMPTED_SERVICES:+$VP_APP_ATTEMPTED_SERVICES }$service"
}

vp_remove_app_service_attempt() {
  local service="$1"
  local attempted_service
  local remaining_services=""
  for attempted_service in $VP_APP_ATTEMPTED_SERVICES; do
    [[ "$attempted_service" == "$service" ]] && continue
    remaining_services="${remaining_services:+$remaining_services }$attempted_service"
  done
  VP_APP_ATTEMPTED_SERVICES="$remaining_services"
}

vp_update_app_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"
  local update_status=0

  vp_record_app_service_attempt "$service"
  if vp_update_runtime_service "$service" "$image" "$order"; then
    return 0
  else
    update_status=$?
  fi
  if [[ "$update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
    vp_remove_app_service_attempt "$service"
  fi
  return 1
}

vp_app_service_was_attempted() {
  local service="$1"
  local attempted_services="$2"
  case " $attempted_services " in
    *" $service "*)
      return 0
      ;;
  esac
  return 1
}

vp_restore_gpu_service() {
  local image="$1"
  local existing_constraints
  existing_constraints="$(
    vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )" || return 1
  local constraint_output
  constraint_output="$(vp_gpu_constraint_update_args "$existing_constraints")" || return 1
  local constraint
  local constraint_args=()
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    constraint_args+=("$constraint")
  done <<<"$constraint_output"

  local update_args=(
    service update --detach=false --no-resolve-image --update-order stop-first
  )
  if [[ "${#constraint_args[@]}" -gt 0 ]]; then
    update_args+=("${constraint_args[@]}")
  fi
  update_args+=(--image "$image" "$VP_PYTHON_WORKER_SERVICE")
  docker "${update_args[@]}" >&2 || return 1
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_restore_app_snapshots() {
  local snapshots="$1"
  local attempted_services="${2-$VP_APP_SERVICES}"
  local service
  local image
  local gpu_was_present=false
  local vision_was_present=false
  local publisher_was_present=false
  local status=0

  while IFS='|' read -r service image; do
    [[ -n "$service" ]] || continue
    vp_app_service_was_attempted "$service" "$attempted_services" || continue
    log "restore $service -> $image with dedicated VP placement"
    if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" ]]; then
      gpu_was_present=true
      vp_require_worker_redis_marker_status || return 1
      if ! vp_restore_gpu_service "$image"; then
        status=1
      fi
    elif [[ "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
      vision_was_present=true
      vp_require_worker_redis_marker_status || return 1
      if ! vp_deploy_vision_worker "$image"; then
        status=1
      fi
    elif [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_was_present=true
      vp_require_worker_redis_marker_status || return 1
      if ! vp_deploy_publisher "$image"; then
        status=1
      fi
    elif [[ "$VP_BACKEND_MIGRATION_APPLIED" == true \
      && ( "$service" == "vp-autoflow-api-swarm" \
        || "$service" == "vp-event-outbox-relay-swarm" ) ]]; then
      log "preserve $service at the migration-compatible attempted image"
    elif ! vp_update_runtime_service "$service" "$image" stop-first; then
      status=1
    fi
  done < <(printf '%s\n' "$snapshots")

  if vp_app_service_was_attempted "$VP_PYTHON_WORKER_SERVICE" "$attempted_services" \
    && [[ "$gpu_was_present" != true ]] \
    && docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_PYTHON_WORKER_SERVICE"
    vp_require_worker_redis_marker_status || return 1
    if ! docker service rm "$VP_PYTHON_WORKER_SERVICE" >&2; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_VISION_WORKER_SERVICE" "$attempted_services" \
    && [[ "$vision_was_present" != true ]] \
    && docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_VISION_WORKER_SERVICE"
    vp_require_worker_redis_marker_status || return 1
    if ! docker service rm "$VP_VISION_WORKER_SERVICE" >&2; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_PUBLISHER_SERVICE" "$attempted_services" \
    && [[ "$publisher_was_present" != true ]]; then
    local publisher_state
    publisher_state="$(vp_publisher_service_state)" || return 1
    if [[ "$publisher_state" == exists ]]; then
      log "remove newly created $VP_PUBLISHER_SERVICE"
      vp_require_worker_redis_marker_status || return 1
      if ! docker service rm "$VP_PUBLISHER_SERVICE" >&2; then
        status=1
      fi
    fi
  fi
  return "$status"
}

vp_install_soak_watch() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "ChannelOps soak watcher install skipped"
    return 0
  fi

  local sync_root="${ROOT:-}"
  if [[ -z "$sync_root" ]]; then
    echo "ROOT must be set by deploy-github-sync.sh" >&2
    return 1
  fi

  local source="${VP_SOAK_WATCH_SOURCE:-$REPO_ROOT/videoprocess/deploy/swarm/channelops-soak-watch.sh}"
  if [[ ! -r "$source" ]]; then
    echo "ChannelOps soak watcher source is not readable: $source" >&2
    return 1
  fi
  if ! bash -n "$source"; then
    echo "ChannelOps soak watcher source has invalid syntax: $source" >&2
    return 1
  fi

  (
    local target="$sync_root/bin/channelops-soak-watch.sh"
    local log_file="$sync_root/logs/channelops-soak-watch.log"
    local cron_begin="# BEGIN VIDEOPROCESS SOAK WATCH"
    local cron_end="# END VIDEOPROCESS SOAK WATCH"
    local cron_command="*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$sync_root $target >> $log_file 2>&1"
    local temp_dir=""
    local watch_txn_dir=""
    local current_cron=""
    local next_cron=""
    local verify_cron=""
    local prior_cron_absent=false
    local watcher_had_prior=false
    local watcher_replaced=false
    local cron_may_have_changed=false
    local transaction_status=1
    local failure_reason=""
    local vp_soak_read_absent=false

    vp_soak_watch_is_no_crontab_error() {
      awk 'NR == 1 && /^no crontab for .+$/ { matched=1; next }
        { matched=0; exit }
        END { exit matched ? 0 : 1 }' "$1"
    }

    vp_soak_watch_read_cron() {
      local output="$1"
      local error_output="$2"
      vp_soak_read_absent=false
      if LC_ALL=C crontab -l >"$output" 2>"$error_output"; then
        return 0
      fi
      if vp_soak_watch_is_no_crontab_error "$error_output"; then
        : >"$output"
        vp_soak_read_absent=true
        return 0
      fi
      cat "$error_output" >&2
      return 1
    }

    vp_soak_watch_cleanup() {
      local cleanup_status=0
      if [[ -n "$watch_txn_dir" ]]; then
        if rm -rf "$watch_txn_dir"; then
          watch_txn_dir=""
        else
          cleanup_status=1
        fi
      fi
      if [[ -n "$temp_dir" ]]; then
        if rm -rf "$temp_dir"; then
          temp_dir=""
        else
          cleanup_status=1
        fi
      fi
      return "$cleanup_status"
    }

    vp_soak_watch_restore() {
      local restore_status=0
      local rollback_read="$temp_dir/rollback-read"
      local rollback_error="$temp_dir/rollback-error"

      if [[ "$cron_may_have_changed" == true ]]; then
        if [[ "$prior_cron_absent" == true ]]; then
          if ! LC_ALL=C crontab -r 2>"$rollback_error" \
            && ! vp_soak_watch_is_no_crontab_error "$rollback_error"; then
            cat "$rollback_error" >&2
            restore_status=1
          elif ! vp_soak_watch_read_cron "$rollback_read" "$rollback_error" \
            || [[ "$vp_soak_read_absent" != true ]]; then
            echo "ChannelOps soak watcher no-crontab rollback verification failed" >&2
            restore_status=1
          fi
        else
          if ! LC_ALL=C crontab "$current_cron"; then
            echo "ChannelOps soak watcher crontab rollback install failed" >&2
            restore_status=1
          elif ! vp_soak_watch_read_cron "$rollback_read" "$rollback_error" \
            || [[ "$vp_soak_read_absent" == true ]] \
            || ! cmp -s "$current_cron" "$rollback_read"; then
            echo "ChannelOps soak watcher crontab rollback verification failed" >&2
            restore_status=1
          fi
        fi
      fi

      if [[ "$watcher_replaced" == true ]]; then
        if [[ "$watcher_had_prior" == true ]]; then
          if ! cp -p "$watch_txn_dir/prior-watcher" "$watch_txn_dir/restore-watcher" \
            || ! mv -f "$watch_txn_dir/restore-watcher" "$target" \
            || ! cmp -s "$watch_txn_dir/prior-watcher" "$target"; then
            echo "ChannelOps soak watcher target rollback failed" >&2
            restore_status=1
          fi
        elif ! rm -f "$target" || [[ -e "$target" ]]; then
          echo "ChannelOps soak watcher target absence rollback failed" >&2
          restore_status=1
        fi
      fi
      return "$restore_status"
    }

    vp_soak_watch_interrupted() {
      local signal_name="$1"
      local signal_status="$2"
      trap - HUP INT TERM
      echo "ChannelOps soak watcher install interrupted by $signal_name" >&2
      if ! vp_soak_watch_restore; then
        echo "ChannelOps soak watcher rollback failed after $signal_name" >&2
      fi
      if ! vp_soak_watch_cleanup; then
        echo "ChannelOps soak watcher cleanup failed after $signal_name" >&2
      fi
      exit "$signal_status"
    }

    trap 'vp_soak_watch_interrupted HUP 129' HUP
    trap 'vp_soak_watch_interrupted INT 130' INT
    trap 'vp_soak_watch_interrupted TERM 143' TERM

    while :; do
      temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/vp-soak-watch-cron.XXXXXX")" || {
        failure_reason="could not create cron transaction directory"
        break
      }
      current_cron="$temp_dir/current"
      next_cron="$temp_dir/next"
      verify_cron="$temp_dir/verify"

      if ! vp_soak_watch_read_cron "$current_cron" "$temp_dir/read-error"; then
        failure_reason="crontab read failed"
        break
      fi
      prior_cron_absent="$vp_soak_read_absent"

      if ! awk -v begin="$cron_begin" -v end="$cron_end" \
        -v target="$target" -v source="$source" \
        -v root_assignment="DEPLOY_GITHUB_SYNC_ROOT=$sync_root" '
        BEGIN { in_managed = 0; expected_end = ""; invalid = 0 }
        $0 == begin {
          if (in_managed) {
            invalid = 1
            exit
          }
          in_managed = 1
          expected_end = end
          next
        }
        $0 == end {
          if (!in_managed || $0 != expected_end) {
            invalid = 1
            exit
          }
          in_managed = 0
          expected_end = ""
          next
        }
        in_managed { next }
        $1 !~ /^#/ && NF >= 6 && ($6 == target || $6 == source) { next }
        $1 !~ /^#/ && NF >= 7 && $6 == root_assignment && ($7 == target || $7 == source) { next }
        { print }
        END {
          if (in_managed) {
            invalid = 1
          }
          if (invalid) {
            exit 1
          }
        }
      ' "$current_cron" >"$next_cron"; then
        failure_reason="managed cron block is malformed"
        break
      fi
      if ! printf '%s\n%s\n%s\n' \
        "$cron_begin" "$cron_command" "$cron_end" >>"$next_cron"; then
        failure_reason="managed cron render failed"
        break
      fi

      if ! mkdir -p "$sync_root/bin" "$sync_root/logs" "$sync_root/state"; then
        failure_reason="watcher directories could not be created"
        break
      fi
      watch_txn_dir="$(mktemp -d "$sync_root/bin/.channelops-soak-watch.txn.XXXXXX")" || {
        failure_reason="watcher transaction directory could not be created"
        break
      }
      if ! install -m 0755 "$source" "$watch_txn_dir/staged-watcher" \
        || [[ ! -x "$watch_txn_dir/staged-watcher" ]] \
        || ! cmp -s "$source" "$watch_txn_dir/staged-watcher"; then
        failure_reason="staged watcher verification failed"
        break
      fi
      if [[ -e "$target" ]]; then
        watcher_had_prior=true
        if ! cp -p "$target" "$watch_txn_dir/prior-watcher"; then
          failure_reason="prior watcher backup failed"
          break
        fi
      fi

      watcher_replaced=true
      if ! mv -f "$watch_txn_dir/staged-watcher" "$target"; then
        failure_reason="atomic watcher install failed"
        break
      fi
      cron_may_have_changed=true
      if ! LC_ALL=C crontab "$next_cron"; then
        failure_reason="crontab install failed"
        break
      fi
      if ! vp_soak_watch_read_cron "$verify_cron" "$temp_dir/verify-error" \
        || [[ "$vp_soak_read_absent" == true ]] \
        || ! cmp -s "$next_cron" "$verify_cron"; then
        failure_reason="crontab verification failed"
        break
      fi
      if [[ ! -x "$target" ]] || ! cmp -s "$source" "$target"; then
        failure_reason="installed watcher verification failed"
        break
      fi

      transaction_status=0
      break
    done

    if [[ "$transaction_status" -ne 0 ]]; then
      if [[ -n "$failure_reason" ]]; then
        echo "ChannelOps soak watcher $failure_reason" >&2
      fi
      if ! vp_soak_watch_restore; then
        echo "ChannelOps soak watcher rollback failed" >&2
      fi
    fi
    trap - HUP INT TERM
    if ! vp_soak_watch_cleanup; then
      if [[ "$transaction_status" -eq 0 ]]; then
        echo "ChannelOps soak watcher cleanup failed after verified install; continuing" >&2
      else
        echo "ChannelOps soak watcher cleanup failed" >&2
      fi
    fi
    exit "$transaction_status"
  )
}

vp_worker_redis_marker_file_mode() {
  local path="$1"
  local mode
  if mode="$(stat -f '%Lp' "$path" 2>/dev/null)"; then
    printf '%s\n' "$mode"
    return 0
  fi
  stat -c '%a' "$path" 2>/dev/null
}

vp_worker_redis_marker_reject_126() {
  local value
  value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    *10.0.0.126*|*caspers-mac-mini*|*colima-swarmbridged*|*colima-126*|*hostname==126*)
      return 1
      ;;
  esac
}

vp_require_worker_redis_runtime_state() {
  local state_file="${VP_WORKER_REDIS_RUNTIME_STATE_FILE:-}"
  local expected_generation="${VP_WORKER_REDIS_RUNTIME_GENERATION:-}"
  if [[ ! "$state_file" = /* || ! -f "$state_file" || -L "$state_file" \
    || "$(vp_worker_redis_marker_file_mode "$state_file")" != 400 \
    || ! "$expected_generation" =~ ^[0-9a-f]{40}$ ]]; then
    echo "worker Redis runtime state is absent or invalid" >&2
    return 1
  fi

  local generation=""
  local acl_identity=""
  local aof_enabled=""
  local aof_status=""
  local maxmemory_policy=""
  local network=""
  local readiness_secret=""
  local janitor_secret=""
  local repair_secret=""
  local key
  local value
  while IFS='=' read -r key value; do
    if [[ -z "$key" || -z "$value" || "$value" == *$'\r'* ]]; then
      echo "worker Redis runtime state is invalid" >&2
      return 1
    fi
    case "$key" in
      GENERATION)
        [[ -z "$generation" ]] || return 1
        generation="$value"
        ;;
      ACL_IDENTITY)
        [[ -z "$acl_identity" ]] || return 1
        acl_identity="$value"
        ;;
      AOF_ENABLED)
        [[ -z "$aof_enabled" ]] || return 1
        aof_enabled="$value"
        ;;
      AOF_STATUS)
        [[ -z "$aof_status" ]] || return 1
        aof_status="$value"
        ;;
      MAXMEMORY_POLICY)
        [[ -z "$maxmemory_policy" ]] || return 1
        maxmemory_policy="$value"
        ;;
      NETWORK)
        [[ -z "$network" ]] || return 1
        network="$value"
        ;;
      READINESS_REDIS_SECRET)
        [[ -z "$readiness_secret" ]] || return 1
        readiness_secret="$value"
        ;;
      JANITOR_REDIS_SECRET)
        [[ -z "$janitor_secret" ]] || return 1
        janitor_secret="$value"
        ;;
      REPAIR_REDIS_SECRET)
        [[ -z "$repair_secret" ]] || return 1
        repair_secret="$value"
        ;;
      *)
        echo "worker Redis runtime state contains an unknown field" >&2
        return 1
        ;;
    esac
  done <"$state_file"

  if [[ "$generation" != "$expected_generation" \
    || "$acl_identity" != "$VP_WORKER_REDIS_RUNTIME_ACL_IDENTITY" \
    || "$aof_enabled" != yes \
    || "$aof_status" != ok \
    || "$maxmemory_policy" != noeviction \
    || "$network" != "$VP_PIPELINE_NETWORK" ]]; then
    echo "worker Redis runtime identity or persistence state is unready" >&2
    return 1
  fi
  if ! vp_worker_redis_marker_reject_126 \
    "$generation $network $readiness_secret $janitor_secret $repair_secret"; then
    echo "worker Redis runtime state contains forbidden topology" >&2
    return 1
  fi

  local secret
  for secret in "$readiness_secret" "$janitor_secret" "$repair_secret"; do
    if [[ ! "$secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] \
      || ! docker secret inspect "$secret" >/dev/null 2>&1; then
      echo "worker Redis runtime secret is absent" >&2
      return 1
    fi
  done
  if [[ "$readiness_secret" == "$janitor_secret" \
    || "$readiness_secret" == "$repair_secret" \
    || "$janitor_secret" == "$repair_secret" ]]; then
    echo "worker Redis runtime secrets are not independent" >&2
    return 1
  fi

  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET="$readiness_secret"
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET="$janitor_secret"
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET="$repair_secret"
}

vp_worker_redis_marker_control_root() {
  local sync_root="${ROOT:-}"
  if [[ -z "$sync_root" || ! "$sync_root" = /* ]]; then
    return 1
  fi
  printf '%s\n' "$sync_root/state/worker-redis-marker-control"
}

vp_worker_redis_marker_new_generation() {
  local image="$1"
  local digest
  digest="$(printf '%s' "$image" | shasum -a 256 | cut -c1-12)" || return 1
  local epoch
  epoch="$(date +%s)" || return 1
  printf 'm-%s-%s-%04d\n' "$digest" "$epoch" "$((RANDOM % 10000))"
}

vp_worker_redis_marker_owner_file() {
  local path="${VP_WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE:-}"
  if [[ ! "$path" = /* || ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 400 ]]; then
    echo "worker marker owner database URL file is absent or invalid" >&2
    return 1
  fi
  printf '%s\n' "$path"
}

vp_worker_redis_marker_database_secret_name() {
  local purpose="$1"
  local generation="$2"
  printf 'vp-wrm-%s-db-%s\n' "$purpose" "$generation"
}

vp_worker_redis_marker_provision_roles() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local owner_file
  owner_file="$(vp_worker_redis_marker_owner_file)" || return 1
  local role_state="$control_root/roles"
  mkdir -p "$role_state" || return 1
  chmod 0700 "$role_state" || return 1

  docker run --rm \
    --network "$VP_PIPELINE_NETWORK" \
    --mount "type=bind,src=$owner_file,dst=/run/secrets/worker-marker-owner-database-url,readonly" \
    --mount "type=bind,src=$role_state,dst=/control-state" \
    --env WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url \
    "$image" \
    python -m app.services.worker_marker_control_role_cli \
      provision \
      --generation "$generation" \
      --state-dir /control-state >/dev/null
}

vp_worker_redis_marker_revoke_roles() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local owner_file
  owner_file="$(vp_worker_redis_marker_owner_file)" || return 1

  docker run --rm \
    --network "$VP_PIPELINE_NETWORK" \
    --mount "type=bind,src=$owner_file,dst=/run/secrets/worker-marker-owner-database-url,readonly" \
    --mount "type=bind,src=$control_root/roles,dst=/control-state" \
    --env WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url \
    "$image" \
    python -m app.services.worker_marker_control_role_cli \
      revoke \
      --generation "$generation" \
      --state-dir /control-state >/dev/null
}

vp_worker_redis_marker_create_database_secrets() {
  local generation="$1"
  local control_root="$2"
  local purpose
  local secret_name
  local credential_file
  VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS=""
  for purpose in readiness janitor repair; do
    secret_name="$(
      vp_worker_redis_marker_database_secret_name "$purpose" "$generation"
    )" || return 1
    credential_file="$control_root/roles/$generation/worker-marker-$purpose-database-url"
    if [[ ! -f "$credential_file" || -L "$credential_file" \
      || "$(vp_worker_redis_marker_file_mode "$credential_file")" != 400 ]] \
      || docker secret inspect "$secret_name" >/dev/null 2>&1; then
      echo "worker marker database credential is absent, invalid, or reused" >&2
      return 1
    fi
    if ! docker secret create "$secret_name" - \
      <"$credential_file" >/dev/null; then
      echo "worker marker database secret creation failed" >&2
      return 1
    fi
    VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS="${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:+$VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS }$secret_name"
  done
}

vp_worker_redis_marker_expected_job_identity() {
  local image="$1"
  local generation="$2"
  local mode="$3"
  local database_secret
  local redis_secret
  local module
  local command
  case "$mode" in
    readiness)
      database_secret="$(
        vp_worker_redis_marker_database_secret_name readiness "$generation"
      )" || return 1
      redis_secret="$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET"
      module="app.channel_agent.worker_redis_marker_readiness_cli"
      command="check"
      ;;
    janitor)
      database_secret="$(
        vp_worker_redis_marker_database_secret_name janitor "$generation"
      )" || return 1
      redis_secret="$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET"
      module="app.channel_agent.worker_redis_marker_janitor_cli"
      command="run"
      ;;
    *)
      return 1
      ;;
  esac
  local network_id
  network_id="$(
    docker network inspect "$VP_PIPELINE_NETWORK" --format '{{.ID}}'
  )" || return 1
  [[ "$network_id" =~ ^[A-Za-z0-9._:-]+$ ]] || return 1
  printf '%s\n' \
    "2|$mode|$generation|$image|replicated-job|1|1|none|node.hostname==$VP_MANAGER_NODE|$network_id|$database_secret:worker-marker-database-url:256,$redis_secret:worker-marker-redis-url:256|WORKER_REDIS_MARKER_DATABASE_URL_FILE=/run/secrets/worker-marker-database-url,WORKER_REDIS_MARKER_REDIS_URL_FILE=/run/secrets/worker-marker-redis-url|python,-m,$module,$command"
}

vp_worker_redis_marker_job_identity() {
  local name="$1"
  local identity
  identity="$(
    docker service inspect "$name" --format \
      '{{len .Spec.Labels}}|{{index .Spec.Labels "vp.worker-redis-marker.mode"}}|{{index .Spec.Labels "vp.worker-redis-marker.generation"}}|{{.Spec.TaskTemplate.ContainerSpec.Image}}|{{if .Spec.Mode.ReplicatedJob}}replicated-job{{else}}other{{end}}|{{.Spec.Mode.ReplicatedJob.TotalCompletions}}|{{.Spec.Mode.ReplicatedJob.MaxConcurrent}}|{{.Spec.TaskTemplate.RestartPolicy.Condition}}|{{range .Spec.TaskTemplate.Placement.Constraints}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.Networks}}{{printf "%s," .Target}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{printf "%s:%s:%d," .SecretName .File.Name .File.Mode}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Args}}{{printf "%s," .}}{{end}}'
  )" || return 1
  identity="${identity//,|/|}"
  printf '%s\n' "${identity%,}"
}

vp_worker_redis_marker_remove_generation_jobs() {
  local image="$1"
  local generation="$2"
  [[ -n "$image" && -n "$generation" ]] || return 0
  local name
  local mode
  local expected_identity
  local actual_identity
  local states
  for mode in readiness janitor; do
    case "$mode" in
      readiness)
        name=vp-worker-redis-marker-readiness-job
        ;;
      janitor)
        name=vp-worker-redis-marker-janitor-job
        ;;
    esac
    if ! docker service inspect "$name" >/dev/null 2>&1; then
      continue
    fi
    expected_identity="$(
      vp_worker_redis_marker_expected_job_identity \
        "$image" "$generation" "$mode"
    )" || return 1
    actual_identity="$(vp_worker_redis_marker_job_identity "$name")" || return 1
    if [[ "$actual_identity" != "$expected_identity" ]]; then
      local actual_label_count
      local actual_mode
      local actual_generation
      local actual_image
      IFS='|' read -r \
        actual_label_count actual_mode actual_generation actual_image \
        _ <<<"$actual_identity"
      if [[ "$actual_label_count" != 2 \
        || "$actual_mode" != "$mode" \
        || ! "$actual_generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
        || "$actual_generation" == "$generation" \
        || ! "$actual_image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
        || "$actual_identity" \
          != "$(vp_worker_redis_marker_expected_job_identity \
            "$actual_image" "$actual_generation" "$mode")" ]]; then
        echo "worker marker fixed-name job identity does not match generation" >&2
        return 1
      fi
      continue
    fi
    states="$(
      docker service ps "$name" --no-trunc \
        --format '{{.CurrentState}}'
    )" || return 1
    if ! printf '%s\n' "$states" | awk '
      NF {
        count++
        if ($1 !~ /^(Complete|Failed|Rejected|Shutdown|Orphaned|Remove)$/) {
          invalid=1
        }
      }
      END { exit count == 1 && !invalid ? 0 : 1 }
    '; then
      echo "worker marker generation still has a running job" >&2
      return 1
    fi
    docker service rm "$name" >/dev/null || return 1
    local attempt
    for ((attempt = 0; attempt < 30; attempt++)); do
      if ! docker service inspect "$name" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if docker service inspect "$name" >/dev/null 2>&1; then
      echo "worker marker fixed-name job removal did not converge" >&2
      return 1
    fi
  done
}

vp_worker_redis_marker_retire_generation() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  [[ -n "$image" && -n "$generation" ]] || return 0
  vp_worker_redis_marker_remove_generation_jobs \
    "$image" "$generation" || return 1
  vp_worker_redis_marker_revoke_roles \
    "$image" "$generation" "$control_root" || return 1
  local purpose
  local secret_name
  for purpose in readiness janitor repair; do
    secret_name="$(
      vp_worker_redis_marker_database_secret_name "$purpose" "$generation"
    )" || return 1
    if docker secret inspect "$secret_name" >/dev/null 2>&1; then
      docker secret rm "$secret_name" >/dev/null || return 1
    fi
  done
}

vp_worker_redis_marker_read_prior_config() {
  local path="$1"
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
  [[ -e "$path" ]] || return 0
  if [[ ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    echo "worker marker active configuration is invalid" >&2
    return 1
  fi
  local key
  local value
  local generation=""
  local image=""
  local network=""
  local readiness_database_secret=""
  local readiness_redis_secret=""
  local janitor_database_secret=""
  local janitor_redis_secret=""
  while IFS='=' read -r key value; do
    [[ -n "$key" && -n "$value" && "$value" != *$'\r'* ]] || return 1
    case "$key" in
      GENERATION)
        [[ -z "$generation" ]] || return 1
        generation="$value"
        ;;
      IMAGE)
        [[ -z "$image" ]] || return 1
        image="$value"
        ;;
      NETWORK)
        [[ -z "$network" ]] || return 1
        network="$value"
        ;;
      READINESS_DATABASE_SECRET)
        [[ -z "$readiness_database_secret" ]] || return 1
        readiness_database_secret="$value"
        ;;
      READINESS_REDIS_SECRET)
        [[ -z "$readiness_redis_secret" ]] || return 1
        readiness_redis_secret="$value"
        ;;
      JANITOR_DATABASE_SECRET)
        [[ -z "$janitor_database_secret" ]] || return 1
        janitor_database_secret="$value"
        ;;
      JANITOR_REDIS_SECRET)
        [[ -z "$janitor_redis_secret" ]] || return 1
        janitor_redis_secret="$value"
        ;;
      *)
        return 1
        ;;
    esac
  done <"$path"
  if [[ ! "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    || ! "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    || "$network" != "$VP_PIPELINE_NETWORK" \
    || "$readiness_database_secret" \
      != "$(vp_worker_redis_marker_database_secret_name readiness "$generation")" \
    || "$janitor_database_secret" \
      != "$(vp_worker_redis_marker_database_secret_name janitor "$generation")" \
    || ! "$readiness_redis_secret" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    || ! "$janitor_redis_secret" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    || "$readiness_redis_secret" == "$janitor_redis_secret" ]]; then
    return 1
  fi
  if ! vp_worker_redis_marker_reject_126 \
    "$generation $image $network $readiness_database_secret $readiness_redis_secret $janitor_database_secret $janitor_redis_secret"; then
    return 1
  fi
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE="$image"
}

vp_worker_redis_marker_render_config() {
  local path="$1"
  local image="$2"
  local generation="$3"
  local readiness_database_secret
  local janitor_database_secret
  readiness_database_secret="$(
    vp_worker_redis_marker_database_secret_name readiness "$generation"
  )" || return 1
  janitor_database_secret="$(
    vp_worker_redis_marker_database_secret_name janitor "$generation"
  )" || return 1

  {
    printf 'GENERATION=%s\n' "$generation"
    printf 'IMAGE=%s\n' "$image"
    printf 'NETWORK=%s\n' "$VP_PIPELINE_NETWORK"
    printf 'READINESS_DATABASE_SECRET=%s\n' "$readiness_database_secret"
    printf 'READINESS_REDIS_SECRET=%s\n' \
      "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET"
    printf 'JANITOR_DATABASE_SECRET=%s\n' "$janitor_database_secret"
    printf 'JANITOR_REDIS_SECRET=%s\n' \
      "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET"
  } >"$path"
  chmod 0600 "$path"
}

vp_worker_redis_marker_is_no_crontab_error() {
  awk 'NR == 1 && /^no crontab for .+$/ { matched=1; next }
    { matched=0; exit }
    END { exit matched ? 0 : 1 }' "$1"
}

vp_worker_redis_marker_read_cron() {
  local output="$1"
  local error_output="$2"
  VP_WORKER_REDIS_MARKER_CRON_ABSENT=false
  if LC_ALL=C crontab -l >"$output" 2>"$error_output"; then
    return 0
  fi
  if vp_worker_redis_marker_is_no_crontab_error "$error_output"; then
    : >"$output"
    VP_WORKER_REDIS_MARKER_CRON_ABSENT=true
    return 0
  fi
  cat "$error_output" >&2
  return 1
}

vp_worker_redis_marker_unmanaged_cron() {
  local prior_cron="$1"
  local output="$2"
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local source="$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  awk -v begin="$cron_begin" -v end="$cron_end" \
    -v target="$target" -v source="$source" '
    BEGIN { managed=0; invalid=0 }
    $0 == begin {
      if (managed) {
        invalid=1
        exit
      }
      managed=1
      next
    }
    $0 == end {
      if (!managed) {
        invalid=1
        exit
      }
      managed=0
      next
    }
    managed { next }
    $1 !~ /^#/ && ($6 == target || $6 == source) { next }
    $1 !~ /^#/ && NF >= 9 && ($9 == target || $9 == source) { next }
    { print }
    END { if (managed || invalid) exit 1 }
  ' "$prior_cron" >"$output"
}

vp_worker_redis_marker_capture_managed_state() {
  local control_root="$1"
  local state
  state="$(mktemp -d "$control_root/.managed-state.XXXXXX")" || return 1
  chmod 0700 "$state" || {
    rm -rf "$state"
    return 1
  }
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  if [[ -e "$target" ]]; then
    cp -p "$target" "$state/launcher" || {
      rm -rf "$state"
      return 1
    }
  fi
  if [[ -e "$config" ]]; then
    cp -p "$config" "$state/control.conf" || {
      rm -rf "$state"
      return 1
    }
  fi
  if ! vp_worker_redis_marker_read_cron \
    "$state/crontab" "$state/crontab-error"; then
    rm -rf "$state"
    return 1
  fi
  if [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]]; then
    : >"$state/crontab.absent"
  fi
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$state"
}

vp_worker_redis_marker_deactivate_managed_cron() {
  local state="${1:-$VP_WORKER_REDIS_MARKER_MANAGED_STATE}"
  local current="$state/current-crontab"
  local unmanaged="$state/unmanaged-crontab"
  if ! vp_worker_redis_marker_read_cron \
    "$current" "$state/current-crontab-error"; then
    return 1
  fi
  if [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]]; then
    return 0
  fi
  vp_worker_redis_marker_unmanaged_cron "$current" "$unmanaged" || return 1
  LC_ALL=C crontab "$unmanaged"
}

vp_worker_redis_marker_restore_managed_state() {
  local state="${1:-$VP_WORKER_REDIS_MARKER_MANAGED_STATE}"
  [[ -n "$state" && -d "$state" ]] || return 1
  vp_worker_redis_marker_deactivate_managed_cron "$state" || return 1
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local config="$control_root/control.conf"
  mkdir -p "$(dirname "$target")" "$control_root" || return 1
  if [[ -f "$state/launcher" ]]; then
    cp -p "$state/launcher" "$target" || return 1
  else
    rm -f "$target" || return 1
  fi
  if [[ -f "$state/control.conf" ]]; then
    cp -p "$state/control.conf" "$config" || return 1
  else
    rm -f "$config" || return 1
  fi
  if [[ -f "$state/crontab.absent" ]]; then
    LC_ALL=C crontab -r >/dev/null 2>&1 || true
  else
    LC_ALL=C crontab "$state/crontab" || return 1
  fi

  if [[ -f "$state/launcher" ]]; then
    cmp -s "$state/launcher" "$target" || return 1
  else
    [[ ! -e "$target" ]] || return 1
  fi
  if [[ -f "$state/control.conf" ]]; then
    cmp -s "$state/control.conf" "$config" || return 1
  else
    [[ ! -e "$config" ]] || return 1
  fi
  local verify="$state/verify-crontab"
  if ! vp_worker_redis_marker_read_cron \
    "$verify" "$state/verify-crontab-error"; then
    return 1
  fi
  if [[ -f "$state/crontab.absent" ]]; then
    [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]] || return 1
  else
    [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == false ]] \
      && cmp -s "$state/crontab" "$verify"
  fi
}

vp_worker_redis_marker_discard_managed_state() {
  if [[ -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" ]]; then
    rm -rf "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" || return 1
  fi
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
}

vp_install_worker_redis_marker_control() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local sync_root="${ROOT:-}"
  local source="$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE"
  if [[ -z "$sync_root" || ! "$sync_root" = /* \
    || ! -r "$source" || ! -x "$source" ]] \
    || ! bash -n "$source"; then
    echo "worker Redis marker launcher source is invalid" >&2
    return 1
  fi

  local bin_dir="$sync_root/bin"
  local log_dir="$sync_root/logs"
  local target="$bin_dir/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local readiness_cron="* * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target readiness >> $log_dir/worker-redis-marker-readiness.log 2>&1"
  local janitor_cron="*/5 * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target janitor >> $log_dir/worker-redis-marker-janitor.log 2>&1"
  local transaction
  transaction="$(mktemp -d "${TMPDIR:-/tmp}/vp-worker-marker-control.XXXXXX")" \
    || return 1
  local status=1
  local prior_cron="$transaction/prior-cron"
  local next_cron="$transaction/next-cron"
  local verify_cron="$transaction/verify-cron"
  local read_error="$transaction/read-error"
  local prior_target=false
  local prior_config=false
  local prior_cron_absent=false
  local cron_read=false
  local cron_may_have_changed=false

  if [[ -e "$target" ]]; then
    prior_target=true
    if ! cp -p "$target" "$transaction/prior-launcher"; then
      rm -rf "$transaction"
      echo "worker Redis marker launcher backup failed" >&2
      return 1
    fi
  fi
  if [[ -e "$config" ]]; then
    prior_config=true
    if ! cp -p "$config" "$transaction/prior-config"; then
      rm -rf "$transaction"
      echo "worker Redis marker config backup failed" >&2
      return 1
    fi
  fi

  if vp_worker_redis_marker_read_cron "$prior_cron" "$read_error"; then
    cron_read=true
    prior_cron_absent="$VP_WORKER_REDIS_MARKER_CRON_ABSENT"
  fi
  if [[ "$cron_read" == true ]] \
    && awk -v begin="$cron_begin" -v end="$cron_end" \
      -v target="$target" -v source="$source" '
      BEGIN { managed=0; invalid=0 }
      $0 == begin {
        if (managed) {
          invalid=1
          exit
        }
        managed=1
        next
      }
      $0 == end {
        if (!managed) {
          invalid=1
          exit
        }
        managed=0
        next
      }
      managed { next }
      $1 !~ /^#/ && ($6 == target || $6 == source) { next }
      $1 !~ /^#/ && NF >= 9 && ($9 == target || $9 == source) { next }
      { print }
      END { if (managed || invalid) exit 1 }
    ' "$prior_cron" >"$next_cron" \
    && printf '%s\n%s\n%s\n%s\n' \
      "$cron_begin" "$readiness_cron" "$janitor_cron" "$cron_end" \
      >>"$next_cron" \
    && mkdir -p "$bin_dir" "$log_dir" "$control_root" "$state_dir" "$lock_dir" \
    && chmod 0700 "$control_root" "$state_dir" "$lock_dir" \
    && install -m 0755 "$source" "$transaction/launcher" \
    && vp_worker_redis_marker_render_config \
      "$transaction/control.conf" "$image" "$generation"; then
    if mv -f "$transaction/launcher" "$target" \
      && mv -f "$transaction/control.conf" "$config"; then
      cron_may_have_changed=true
    fi
    if [[ "$cron_may_have_changed" == true ]] \
      && LC_ALL=C crontab "$next_cron" \
      && vp_worker_redis_marker_read_cron \
        "$verify_cron" "$transaction/verify-error" \
      && cmp -s "$next_cron" "$verify_cron" \
      && cmp -s "$source" "$target" \
      && [[ "$(vp_worker_redis_marker_file_mode "$target")" == 755 ]] \
      && [[ "$(vp_worker_redis_marker_file_mode "$config")" == 600 ]]; then
      status=0
    fi
  fi

  if [[ "$status" -ne 0 ]]; then
    if [[ "$prior_target" == true ]]; then
      cp -p "$transaction/prior-launcher" "$target" || true
    else
      rm -f "$target" || true
    fi
    if [[ "$prior_config" == true ]]; then
      cp -p "$transaction/prior-config" "$config" || true
    else
      rm -f "$config" || true
    fi
    if [[ "$cron_may_have_changed" == true ]]; then
      if [[ "$prior_cron_absent" == true ]]; then
        LC_ALL=C crontab -r >/dev/null 2>&1 || true
      else
        LC_ALL=C crontab "$prior_cron" >/dev/null 2>&1 || true
      fi
    fi
    echo "worker Redis marker launcher install failed" >&2
  fi
  rm -rf "$transaction"
  return "$status"
}

vp_run_worker_redis_marker_readiness() {
  local control_root="$1"
  local launcher="${ROOT}/bin/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$config" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$state_dir" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$lock_dir" \
    "$launcher" readiness >/dev/null \
    || return 1
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$config" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$state_dir" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$lock_dir" \
    "$launcher" status >/dev/null
}

vp_require_worker_redis_marker_status() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker Redis marker status gate skipped"
    return 0
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local launcher="${ROOT}/bin/worker-redis-marker-control.sh"
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$control_root/control.conf" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$control_root/status" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$control_root/locks" \
    "$launcher" status >/dev/null
}

vp_worker_redis_marker_provision_generation() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  vp_worker_redis_marker_provision_roles \
    "$image" "$generation" "$control_root" || return 1
  if ! vp_worker_redis_marker_create_database_secrets \
    "$generation" "$control_root"; then
    vp_worker_redis_marker_revoke_roles \
      "$image" "$generation" "$control_root" || return 1
    local secret
    for secret in ${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:-}; do
      docker secret rm "$secret" >/dev/null || return 1
    done
    return 1
  fi
}

vp_prepare_worker_redis_marker_controls() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker Redis marker control preparation skipped"
    return 0
  fi
  vp_validate_topology || return 1
  vp_require_worker_redis_runtime_state || return 1
  vp_worker_redis_marker_owner_file >/dev/null || return 1

  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  mkdir -p "$control_root" || return 1
  chmod 0700 "$control_root" || return 1
  vp_worker_redis_marker_read_prior_config \
    "$control_root/control.conf" || return 1
  vp_worker_redis_marker_capture_managed_state "$control_root" || return 1
  if ! vp_worker_redis_marker_deactivate_managed_cron; then
    vp_worker_redis_marker_discard_managed_state || true
    return 1
  fi
  if ! vp_worker_redis_marker_remove_generation_jobs \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION"; then
    vp_worker_redis_marker_restore_managed_state || true
    vp_worker_redis_marker_discard_managed_state || true
    return 1
  fi

  local generation
  if ! generation="$(vp_worker_redis_marker_new_generation "$image")"; then
    vp_worker_redis_marker_restore_managed_state || return 1
    vp_worker_redis_marker_discard_managed_state || return 1
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$image"
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false

  if ! vp_worker_redis_marker_provision_generation \
    "$image" "$generation" "$control_root" \
    || ! vp_install_worker_redis_marker_control \
      "$image" "$generation" "$control_root" \
    || ! vp_run_worker_redis_marker_readiness "$control_root"; then
    echo "worker Redis marker candidate readiness failed" >&2
    if vp_worker_redis_marker_restore_managed_state \
      && vp_worker_redis_marker_retire_generation \
        "$image" "$generation" "$control_root"; then
      VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
      VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
      VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
      vp_worker_redis_marker_discard_managed_state || return 1
    else
      echo "worker Redis marker candidate cleanup did not converge" >&2
    fi
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=true
}

vp_restore_worker_redis_marker_controls() {
  if [[ "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != true ]]; then
    return 0
  fi
  if [[ -z "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    || -z "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" ]]; then
    echo "worker Redis marker rollback has no prior generation" >&2
    return 1
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local original_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  vp_worker_redis_marker_capture_managed_state "$control_root" || return 1
  local candidate_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$original_state"
  if ! vp_worker_redis_marker_deactivate_managed_cron "$candidate_state" \
    || ! vp_worker_redis_marker_remove_generation_jobs \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"; then
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    rm -rf "$candidate_state"
    return 1
  fi
  local rollback_generation
  rollback_generation="$(
    vp_worker_redis_marker_new_generation \
      "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE"
  )" || {
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    rm -rf "$candidate_state"
    return 1
  }
  if ! vp_worker_redis_marker_provision_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$rollback_generation" \
    "$control_root" \
    || ! vp_install_worker_redis_marker_control \
      "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
      "$rollback_generation" \
      "$control_root" \
    || ! vp_run_worker_redis_marker_readiness "$control_root"; then
    echo "worker Redis marker rollback readiness failed" >&2
    if ! vp_worker_redis_marker_restore_managed_state "$candidate_state" \
      || ! vp_worker_redis_marker_retire_generation \
        "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
        "$rollback_generation" \
        "$control_root"; then
      echo "worker Redis marker failed rollback cleanup did not converge" >&2
    fi
    rm -rf "$candidate_state"
    return 1
  fi
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
    "$control_root" || return 1
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" || return 1
  rm -rf "$candidate_state" || return 1
  vp_worker_redis_marker_discard_managed_state || return 1
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$rollback_generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE"
}

vp_commit_worker_redis_marker_controls() {
  if [[ "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != true ]]; then
    return 0
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" || return 1
  vp_worker_redis_marker_discard_managed_state || return 1
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
}

vp_apply_app_services() {
  local api="$1"
  local frontend="$2"
  local backend="$3"
  local channelops_runner="$4"
  local ffmpeg_go="$5"
  local python_worker="$6"

  VP_APP_ATTEMPTED_SERVICES=""
  VP_BACKEND_MIGRATION_APPLIED=false
  vp_update_app_runtime_service vp-api-swarm "$api" stop-first || return 1
  http_health vp-api "http://$VP_RUNTIME_HOST:18080/health" || return 1
  vp_update_app_runtime_service vp-frontend-swarm "$frontend" stop-first || return 1
  http_health vp-frontend "http://$VP_RUNTIME_HOST:3001/" || return 1
  vp_prepare_worker_redis_marker_controls "$python_worker" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PYTHON_WORKER_SERVICE"
  vp_deploy_python_worker "$python_worker" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_VISION_WORKER_SERVICE"
  vp_deploy_vision_worker "$python_worker" || return 1
  if [[ "$VP_VISION_CUTOVER_REQUIRED" == true ]]; then
    vp_retire_legacy_vision_worker || return 1
    vp_reconcile_vision_consumers "$python_worker" || return 1
  fi
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PUBLISHER_SERVICE"
  vp_deploy_publisher "$python_worker" || return 1
  vp_update_app_runtime_service vp-autoflow-api-swarm "$backend" start-first || return 1
  VP_BACKEND_MIGRATION_APPLIED=true
  vp_update_app_runtime_service vp-event-outbox-relay-swarm "$backend" start-first || return 1
  vp_require_channelops_migration_head "$python_worker" || return 1
  vp_update_app_runtime_service \
    vp-channel-agent-runner-swarm "$channelops_runner" stop-first || return 1
  vp_update_app_runtime_service \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first || return 1

  local service
  for service in $VP_APP_SERVICES; do
    swarm_service_running "$service" || return 1
  done
  vp_install_soak_watch || return 1
  vp_commit_worker_redis_marker_controls || return 1
}

deploy_vp_app_services() {
  vp_validate_deploy_config || return 1
  VP_VISION_CUTOVER_REQUIRED="$(vp_vision_cutover_required "${6:-}")" || return 1
  case "$VP_VISION_CUTOVER_REQUIRED" in
    true)
      vp_require_vision_cutover_safe "${6:-}" || return 1
      ;;
    false)
      ;;
    *)
      echo "invalid vision cutover state" >&2
      return 1
      ;;
  esac

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_apply_app_services "$@" || return 1
    printf '%s\n' "$VP_APP_SERVICES"
    return 0
  fi

  local snapshots
  snapshots="$(vp_capture_app_snapshots)" || return 1
  if ! vp_apply_app_services "$@"; then
    if ! vp_restore_worker_redis_marker_controls; then
      echo "worker Redis marker control restore did not converge" >&2
    fi
    log "VideoProcess service apply failed; restoring prior images without legacy placement"
    if ! vp_restore_app_snapshots "$snapshots" "$VP_APP_ATTEMPTED_SERVICES"; then
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

  vp_validate_topology || return 1
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

  local candidate_update_status=0
  if vp_update_runtime_service "$service" "$image" "$order"; then
    if swarm_service_running "$service"; then
      printf '%s\n' "$service"
      return 0
    fi
  else
    candidate_update_status=$?
    if [[ "$candidate_update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
      return 1
    fi
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

vp_pds_container_snapshot() {
  remote_sh "$VP_RUNTIME_HOST" /bin/sh -s -- "$VP_PDS_SERVICE" <<'REMOTE'
set -eu
PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
export PATH

service="${1:-}"
if [ "$service" != "vp-pds-swarm" ]; then
  exit 10
fi

container_ids="$(
  docker container ls \
    --filter "label=com.docker.swarm.service.name=$service" \
    --filter status=running \
    --format '{{.ID}}' \
    2>/dev/null
)" || exit 11
container_count="$(
  printf '%s\n' "$container_ids" | awk 'NF { count++ } END { print count+0 }'
)" || exit 11
if [ "$container_count" -ne 1 ]; then
  printf 'pending|container_set\n'
  exit 0
fi
case "$container_ids" in
  ''|*[!0-9a-f]*)
    exit 12
    ;;
esac
container_id_length="${#container_ids}"
if [ "$container_id_length" -lt 12 ] || [ "$container_id_length" -gt 64 ]; then
  exit 12
fi

container_data="$(
  docker container inspect \
    --format '{{.Config.Image}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{if .Config.Healthcheck}}{{json .Config.Healthcheck.Test}}|{{.Config.Healthcheck.Interval}}|{{.Config.Healthcheck.Timeout}}|{{.Config.Healthcheck.StartPeriod}}|{{.Config.Healthcheck.Retries}}{{else}}none|0s|0s|0s|0{{end}}{{println}}{{range .Config.Env}}{{println .}}{{end}}' \
    "$container_ids" \
    2>/dev/null
)" || {
  printf 'pending|container_set\n'
  exit 0
}
container_snapshot="$(printf '%s\n' "$container_data" | sed -n '1p')"
container_env="$(printf '%s\n' "$container_data" | sed -n '2,$p')"
http_addr="$(
  printf '%s\n' "$container_env" \
    | awk -F= '$1 == "PDS_HTTP_ADDR" { count++; value=substr($0, index($0, "=") + 1) } END { if (count == 1) print value; else exit 1 }'
)" || exit 14
if [ "$http_addr" != ":8080" ]; then
  exit 15
fi
case "$container_snapshot" in
  *'|'*)
    image="${container_snapshot%%|*}"
    health_fields="${container_snapshot#*|}"
    ;;
  *)
    exit 16
    ;;
esac
printf '%s|:8080|%s\n' "$image" "$health_fields"
REMOTE
}

vp_require_pds_ready() {
  local expected_image="$1"

  vp_validate_topology || return 1
  swarm_service_running "$VP_PDS_SERVICE" || return 1
  vp_require_service_node "$VP_PDS_SERVICE" "$VP_RUNTIME_NODE" || return 1

  local attempt
  local snapshot
  local actual_image
  local actual_http_addr
  local health
  local health_test
  local health_interval
  local health_timeout
  local health_start_period
  local health_retries
  local extra
  for ((attempt = 1; attempt <= 18; attempt++)); do
    if ! snapshot="$(vp_pds_container_snapshot 2>/dev/null)"; then
      echo "PDS container inspection failed: $VP_PDS_SERVICE" >&2
      return 1
    fi
    if [[ "$snapshot" == "pending|container_set" ]]; then
      if [[ "$attempt" -lt 18 ]]; then
        sleep 5 || {
          echo "PDS readiness wait failed: $VP_PDS_SERVICE" >&2
          return 1
        }
        continue
      fi
      break
    fi
    if [[ -z "$snapshot" || "$snapshot" == *$'\n'* ]]; then
      echo "PDS container inspection returned invalid data: $VP_PDS_SERVICE" >&2
      return 1
    fi

    IFS='|' read -r \
      actual_image \
      actual_http_addr \
      health \
      health_test \
      health_interval \
      health_timeout \
      health_start_period \
      health_retries \
      extra <<<"$snapshot"
    if [[ -n "$extra" \
      || -z "$actual_image" \
      || -z "$actual_http_addr" \
      || -z "$health" \
      || -z "$health_test" \
      || -z "$health_interval" \
      || -z "$health_timeout" \
      || -z "$health_start_period" \
      || -z "$health_retries" ]]; then
      echo "PDS container inspection returned invalid data: $VP_PDS_SERVICE" >&2
      return 1
    fi
    if [[ "$actual_image" != "$expected_image" \
      || "$actual_http_addr" != "$VP_PDS_HTTP_ADDR" \
      || "$health_test" != "$VP_PDS_HEALTH_TEST" \
      || "$health_interval" != "10s" \
      || "$health_timeout" != "3s" \
      || "$health_start_period" != "10s" \
      || "$health_retries" != "6" ]]; then
      echo "PDS container contract mismatch: $VP_PDS_SERVICE" >&2
      return 1
    fi
    case "$health" in
      healthy)
        log "PDS readiness passed: $VP_PDS_SERVICE"
        return 0
        ;;
      starting)
        if [[ "$attempt" -lt 18 ]]; then
          sleep 5 || {
            echo "PDS readiness wait failed: $VP_PDS_SERVICE" >&2
            return 1
          }
          continue
        fi
        ;;
      *)
        echo "PDS container is not healthy: $VP_PDS_SERVICE" >&2
        return 1
        ;;
    esac
  done

  echo "PDS readiness deadline exceeded: $VP_PDS_SERVICE" >&2
  return 1
}

deploy_pds_services() {
  local image="$1"

  vp_validate_topology || return 1
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_update_runtime_service "$VP_PDS_SERVICE" "$image" start-first || return 1
    swarm_service_running "$VP_PDS_SERVICE" || return 1
    printf '%s\n' "$VP_PDS_SERVICE"
    return 0
  fi

  local baseline_image
  baseline_image="$(
    vp_service_values "$VP_PDS_SERVICE" \
      '{{.Spec.TaskTemplate.ContainerSpec.Image}}'
  )" || return 1
  if [[ -z "$baseline_image" ]]; then
    echo "missing current image for VideoProcess service: $VP_PDS_SERVICE" >&2
    return 1
  fi

  local candidate_update_status=0
  if vp_update_runtime_service "$VP_PDS_SERVICE" "$image" start-first; then
    if vp_require_pds_ready "$image"; then
      printf '%s\n' "$VP_PDS_SERVICE"
      return 0
    fi
  else
    candidate_update_status=$?
    if [[ "$candidate_update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
      return 1
    fi
  fi

  log "restore $VP_PDS_SERVICE -> $baseline_image with dedicated VP placement"
  if ! vp_update_runtime_service "$VP_PDS_SERVICE" "$baseline_image" stop-first; then
    echo "PDS image restore did not converge" >&2
    return 1
  fi
  if ! vp_require_pds_ready "$baseline_image"; then
    echo "PDS image restore did not become ready" >&2
  fi
  return 1
}
