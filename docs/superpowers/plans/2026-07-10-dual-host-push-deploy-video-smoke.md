# Dual-Host Push Deploy And Video Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the intended 150 support/control plus 127 VideoProcess runtime, make `main` pushes deploy through a VideoProcess-only 150 sync loop, and prove the deployment by retaining a real generated MP4.

**Architecture:** Recreate the 127 Colima Swarm worker as `colima-127`, label it `vp.runtime=true`, and source-control the VideoProcess-specific build/update behavior in a deploy-sync extension. The existing 150 controller keeps repository polling, staging, state, and rollback ownership, while the extension builds node-local images on 127, builds the managed Python worker on 150, and combines image plus placement changes in one Swarm update. Host 126 is excluded from VideoProcess builds, placement, health checks, and automatic failover.

**Tech Stack:** Bash, launchd, Colima, Docker Swarm, Git, Python 3.12, pytest, Go 1.25, Redis Streams, Postgres, MinIO, FFmpeg/ffprobe.

## Global Constraints

- Host `10.0.0.150` remains the Swarm manager, shared-state host, deploy controller, and managed Python-worker host.
- Host `10.0.0.127` is the only normal VideoProcess application runtime and build target.
- Host `10.0.0.126` must not participate in normal VideoProcess builds, deployment, health gates, placement, or automatic failover.
- No public platform upload may be invoked by this plan.
- Shared Postgres, Redis, MinIO, Qdrant, and Redpanda data must not be deleted or replaced.
- The all-project deploy cron remains disabled; only a VideoProcess-scoped schedule may be enabled after a successful manual apply.
- Runtime source markers advance only after build, update, and health gates pass.
- Existing APIs remain compatible and the smoke pipeline must pass normal pipeline validation.

---

### Task 1: Make The 127 Colima Node Reproducible

**Files:**
- Create: `deploy/macos/install_videoprocess_colima_node.sh`
- Create: `deploy/macos/com.constructure.vp-colima.plist`
- Create: `tests/test_vp_colima_node.sh`
- Modify: `tests/test_macos_deploy_paths.sh`
- Modify: `deploy/macos/common.sh`

**Interfaces:**
- Consumes: `colima`, `docker`, `ssh`, manager host `10.0.0.150`, physical host address `10.0.0.127`.
- Produces: commands `doctor`, `install`, and `status`; Colima profile `swarmbridged`; VM hostname `colima-127`; Swarm label `vp.runtime=true`; launchd label `com.constructure.vp-colima`.

- [ ] **Step 1: Write the failing shell contract test**

Create `tests/test_vp_colima_node.sh` with assertions that the installer:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALLER="$ROOT_DIR/deploy/macos/install_videoprocess_colima_node.sh"
PLIST="$ROOT_DIR/deploy/macos/com.constructure.vp-colima.plist"

bash -n "$INSTALLER"
plutil -lint "$PLIST" >/dev/null

plan="$(VP_DRY_RUN=true "$INSTALLER" install)"
grep -Fq 'colima start --profile swarmbridged' <<<"$plan"
grep -Fq -- '--hostname colima-127' <<<"$plan"
grep -Fq -- '--network-mode bridged' <<<"$plan"
grep -Fq -- '--network-interface en1' <<<"$plan"
grep -Fq '10.0.0.150:2377' <<<"$plan"
grep -Fq 'vp.runtime=true' <<<"$plan"
if grep -Fq '10.0.0.126' <<<"$plan"; then
  echo 'FAIL: 126 must not appear in the VP node installer' >&2
  exit 1
fi
```

Extend `tests/test_macos_deploy_paths.sh` with:

```bash
assert_contains 'MAIN_HOST="${MAIN_HOST:-10.0.0.150}"'
assert_contains 'MAC1_TARGET="${MAC1_TARGET:-wenjieliu@10.0.0.127}"'
assert_contains 'MAC3_TARGET="${MAC3_TARGET:-magi1@10.0.0.126}"'
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
bash tests/test_vp_colima_node.sh
bash tests/test_macos_deploy_paths.sh
```

Expected: the first command fails because the installer and plist do not exist; the second fails because `MAIN_HOST` still uses the obsolete address.

- [ ] **Step 3: Implement the node installer and LaunchAgent**

The installer must:

```bash
#!/usr/bin/env bash
set -euo pipefail

PROFILE="${VP_COLIMA_PROFILE:-swarmbridged}"
VM_HOSTNAME="${VP_COLIMA_HOSTNAME:-colima-127}"
MANAGER_HOST="${VP_MANAGER_HOST:-10.0.0.150}"
EXPECTED_HOST_IP="${VP_RUNTIME_HOST_IP:-10.0.0.127}"
NETWORK_INTERFACE="${VP_NETWORK_INTERFACE:-en1}"
PLIST_LABEL="com.constructure.vp-colima"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="$SCRIPT_DIR/$PLIST_LABEL.plist"
PLIST_TARGET="$HOME/Library/LaunchAgents/$PLIST_LABEL.plist"

run() {
  if [[ "${VP_DRY_RUN:-false}" == "true" ]]; then
    printf '%q ' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

host_ip() {
  ipconfig getifaddr "$NETWORK_INTERFACE"
}

doctor() {
  command -v colima >/dev/null
  command -v docker >/dev/null
  command -v ssh >/dev/null
  local current
  current="$(host_ip)"
  [[ "$current" == "$EXPECTED_HOST_IP" ]] || {
    echo "expected $EXPECTED_HOST_IP on $NETWORK_INTERFACE, got $current" >&2
    return 1
  }
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$MANAGER_HOST" true
}

install_node() {
  if [[ "${VP_DRY_RUN:-false}" != "true" ]]; then
    doctor
  fi
  run colima start --profile "$PROFILE" --cpus 4 --memory 8 --disk 60 \
    --runtime docker --hostname "$VM_HOSTNAME" --vm-type vz \
    --network-address --network-mode bridged \
    --network-interface "$NETWORK_INTERFACE" --port-forwarder ssh

  if [[ "${VP_DRY_RUN:-false}" == "true" ]]; then
    echo "docker --context colima-$PROFILE swarm join 10.0.0.150:2377"
    echo "ssh $MANAGER_HOST docker node update --label-add role=app --label-add vp.runtime=true $VM_HOSTNAME"
    echo "install $PLIST_SOURCE $PLIST_TARGET"
    return 0
  fi

  local swarm_state
  swarm_state="$(docker --context "colima-$PROFILE" info --format '{{.Swarm.LocalNodeState}}')"
  if [[ "$swarm_state" == "inactive" ]]; then
    local token
    token="$(ssh "$MANAGER_HOST" docker swarm join-token -q worker)"
    docker --context "colima-$PROFILE" swarm join --token "$token" "$MANAGER_HOST:2377"
  elif [[ "$swarm_state" != "active" ]]; then
    echo "unexpected Swarm state: $swarm_state" >&2
    return 1
  fi

  ssh "$MANAGER_HOST" docker node update \
    --label-add role=app --label-add vp.runtime=true "$VM_HOSTNAME"
  mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs/constructure"
  install -m 0644 "$PLIST_SOURCE" "$PLIST_TARGET"
  if ! launchctl print "gui/$(id -u)/$PLIST_LABEL" >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST_TARGET"
  fi
}

status_node() {
  colima list
  ssh "$MANAGER_HOST" docker node inspect "$VM_HOSTNAME" \
    --format 'state={{.Status.State}} labels={{json .Spec.Labels}}'
}

case "${1:-doctor}" in
  doctor) doctor ;;
  install) install_node ;;
  status) status_node ;;
  *) echo "usage: $0 {doctor|install|status}" >&2; exit 2 ;;
esac
```

Create the plist with this complete content:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.constructure.vp-colima</string>
  <key>ProgramArguments</key>
  <array>
    <string>/opt/homebrew/bin/colima</string>
    <string>start</string>
    <string>--profile</string>
    <string>swarmbridged</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <false/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>StandardOutPath</key>
  <string>/Users/wenjieliu/Library/Logs/constructure/vp-colima.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/wenjieliu/Library/Logs/constructure/vp-colima.err.log</string>
</dict>
</plist>
```

Change `deploy/macos/common.sh` to:

```bash
MAIN_HOST="${MAIN_HOST:-10.0.0.150}"
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
bash tests/test_vp_colima_node.sh
bash tests/test_macos_deploy_paths.sh
```

Expected: both pass; no command mutates Colima because the contract test uses `VP_DRY_RUN=true`.

- [ ] **Step 5: Commit the reproducible node bootstrap**

```bash
git add deploy/macos/install_videoprocess_colima_node.sh \
  deploy/macos/com.constructure.vp-colima.plist \
  deploy/macos/common.sh tests/test_vp_colima_node.sh \
  tests/test_macos_deploy_paths.sh
git commit -m "ops: define the 127 videoprocess swarm node"
```

---

### Task 2: Source-Control The VideoProcess Deploy-Sync Extension

**Files:**
- Create: `deploy/swarm/deploy-sync-extension.sh`
- Create: `tests/test_vp_deploy_sync_extension.sh`

**Interfaces:**
- Consumes from the installed 150 controller: `REPO_ROOT`, `build_image_on_host`, `http_health`, `swarm_service_running`, `record_state`, and `rollback_services`.
- Produces overrides: `build_vp_app_images(commit)`, `deploy_vp_app_services(api, frontend, backend, channelops_runner, ffmpeg_go, python_worker)`, `deploy_feature_aggregator_services(image)`, and `deploy_pds_services(image)`.
- Produces helper: `vp_update_runtime_service(service, image, order)` that applies image and placement atomically.

- [ ] **Step 1: Write the failing extension contract test**

Create a shell test that stubs manager and build commands, sources the extension, and verifies exact targets:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
CALLS="$(mktemp)"
trap 'rm -f "$CALLS"' EXIT

REPO_ROOT=/home/taiwei/deploy-github-sync/repos
BUILD_IMAGES=1
UPDATE_SERVICES=1
HEALTH_CHECKS=1
VP_YOUTUBE_CREDENTIALS_HOST_DIR=/tmp

build_image_on_host() { printf 'build|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"; }
http_health() { printf 'health|%s|%s\n' "$1" "$2" >>"$CALLS"; }
swarm_service_running() { printf 'running|%s\n' "$1" >>"$CALLS"; }
docker() { printf 'docker|%s\n' "$*" >>"$CALLS"; }

source "$EXTENSION"
images="$(build_vp_app_images 0123456789abcdef)"
deploy_vp_app_services $images >/dev/null

grep -Fq 'build|10.0.0.127|/Users/wenjieliu/VideoProcess-app|backend/Dockerfile.ffmpeg-worker-go|vp-ffmpeg-worker-go:deploy-0123456789ab' "$CALLS"
grep -Fq 'build|10.0.0.150|/home/taiwei/deploy-github-sync/repos/videoprocess/backend|Dockerfile.worker|vp-ffmpeg-worker-python:deploy-0123456789ab' "$CALLS"
grep -Fq 'node.labels.vp.runtime==true' "$CALLS"
grep -Fq 'health|vp-api|http://10.0.0.127:18080/health' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: fail because the extension does not exist.

- [ ] **Step 3: Implement image build coverage**

`build_vp_app_images()` returns six space-separated tags and calls:

```bash
build_image_on_host 10.0.0.127 /Users/wenjieliu/VideoProcess-app \
  backend/Dockerfile.api-go "vp-api:deploy-$short"
build_image_on_host 10.0.0.127 /Users/wenjieliu/VideoProcess-app/frontend \
  Dockerfile "vp-frontend:deploy-$short"
build_image_on_host 10.0.0.127 /Users/wenjieliu/VideoProcess-app/backend \
  Dockerfile.api "vp-backend-api:deploy-$short"
build_image_on_host 10.0.0.127 /Users/wenjieliu/VideoProcess-app \
  backend/Dockerfile.channelops-runner-go "vp-channelops-runner-go:deploy-$short"
build_image_on_host 10.0.0.127 /Users/wenjieliu/VideoProcess-app \
  backend/Dockerfile.ffmpeg-worker-go "vp-ffmpeg-worker-go:deploy-$short"
build_image_on_host 10.0.0.150 "$REPO_ROOT/videoprocess/backend" \
  Dockerfile.worker "vp-ffmpeg-worker-python:deploy-$short"
```

- [ ] **Step 4: Implement atomic image plus placement updates**

The extension inspects existing constraints, removes `node.labels.role==app` when present, preserves unrelated constraints, adds `node.labels.vp.runtime==true` once, and applies the new image in the same `docker service update` invocation.

For `vp-api-swarm`, retain the existing Go database URL replacement and `--no-healthcheck`. For other services, do not mutate unrelated environment or secrets.

The service mapping is:

```bash
vp_update_runtime_service vp-api-swarm "$api" stop-first
vp_update_runtime_service vp-frontend-swarm "$frontend" stop-first
vp_update_runtime_service vp-autoflow-api-swarm "$backend" start-first
vp_update_runtime_service vp-event-outbox-relay-swarm "$backend" start-first
vp_update_runtime_service vp-channel-agent-runner-swarm "$channelops_runner" start-first
vp_update_runtime_service vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first
```

Health gates use `10.0.0.127`, and the returned service list includes all six services plus `vp-ffmpeg-worker-gpu-swarm`.

Override feature aggregator and PDS deployment to use `vp_update_runtime_service` so their next scoped deploy also pins them to 127.

- [ ] **Step 5: Implement the managed 150 Python worker**

Add `vp_deploy_python_worker()` with these invariants:

```bash
docker node update --label-add vp.gpu=true ccttww-lap
```

The service uses:

```text
name=vp-ffmpeg-worker-gpu-swarm
constraint=node.labels.vp.gpu==true
network=vp-pipeline-net
DEPLOY_MODE=shared
DATABASE_URL=${VP_PYTHON_WORKER_DATABASE_URL:-postgresql+asyncpg://vp:vp_secret@10.0.0.150:5435/videoprocess}
REDIS_URL=redis://10.0.0.150:6380/0
STORAGE_BACKEND=minio
MINIO_ENDPOINT=10.0.0.150:9000
MINIO_BUCKET=videoprocess
WORKER_TYPE=ffmpeg
WORKER_HOST=150-gpu
VIDEO_GPU_FALLBACK_TO_CPU=true
YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials
```

Read credentials and secret values from the existing deploy environment. Require `VP_YOUTUBE_CREDENTIALS_HOST_DIR` to exist and default it to `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager/credentials`. Mount it read-only. Keep `VIDEO_USE_GPU=false` unless `docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi -L` exits zero and the operator sets `VP_GPU_RUNTIME_READY=true`; never claim GPU acceleration based only on the host label.

If the service exists, update image, placement, and known worker env keys. If absent, create it. In both cases require `swarm_service_running vp-ffmpeg-worker-gpu-swarm`.

After Steps 3-5, `deploy/swarm/deploy-sync-extension.sh` must contain this complete implementation:

```bash
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
  done < <(vp_service_values "$service" '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}')
  if [[ "$has_runtime" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_CONSTRAINT")
  fi

  local api_args=()
  if [[ "$service" == "vp-api-swarm" ]]; then
    api_args+=(--no-healthcheck)
    if vp_service_values "$service" '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "DATABASE_URL" { found=1 } END { exit found ? 0 : 1 }'; then
      api_args+=(--env-rm DATABASE_URL)
    fi
    api_args+=(--env-add "DATABASE_URL=${VP_API_DATABASE_URL_GO:-postgres://vp:vp_secret@10.0.0.150:5435/videoprocess}")
  fi

  docker service update --detach=false --no-resolve-image \
    --update-order "$order" \
    "${constraint_args[@]}" "${api_args[@]}" \
    --image "$image" "$service" >&2
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
  [[ -d "$credentials_dir" ]] || {
    echo "missing YouTube credentials directory: $credentials_dir" >&2
    return 1
  }
  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null

  local env_key
  local env_value
  local env_args=()
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    if docker service inspect "$VP_PYTHON_WORKER_SERVICE" \
      --format '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' 2>/dev/null \
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
      update_args+=(--mount-add "type=bind,src=$credentials_dir,dst=/app/youtube_credentials,readonly")
    fi
    docker "${update_args[@]}" "${env_args[@]}" "$VP_PYTHON_WORKER_SERVICE" >&2
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
```

- [ ] **Step 6: Verify GREEN and syntax**

Run:

```bash
bash -n deploy/swarm/deploy-sync-extension.sh
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: pass with no call containing `10.0.0.126`.

- [ ] **Step 7: Commit deploy-sync behavior**

```bash
git add deploy/swarm/deploy-sync-extension.sh tests/test_vp_deploy_sync_extension.sh
git commit -m "ops: deploy videoprocess to 150 and 127"
```

---

### Task 3: Retain And Verify A Real Smoke Video

**Files:**
- Modify: `tests/go_migration/test_go_trim_worker_smoke.py`
- Modify: `.gitignore`
- Create: `scripts/run_vp_production_video_smoke.sh`
- Create: `tests/test_vp_production_smoke_script.sh`

**Interfaces:**
- Consumes: `VP_PYTHON_API`, `VP_REDIS_URL`, optional `VP_GO_SMOKE_OUTPUT`, FFmpeg, ffprobe.
- Produces: `<output>.mp4` and `<output>.json` with asset, pipeline, job, artifact, worker, probe, API, and SHA-256 evidence.

- [ ] **Step 1: Write the failing wrapper contract test**

The test requires the wrapper to set strict mode, target 127/150 defaults, create `.runtime/video-smoke`, and avoid publication endpoints:

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/run_vp_production_video_smoke.sh"
bash -n "$SCRIPT"
grep -Fq 'VP_PYTHON_API="${VP_PYTHON_API:-http://10.0.0.127:18080}"' "$SCRIPT"
grep -Fq 'VP_REDIS_URL="${VP_REDIS_URL:-redis://10.0.0.150:6380/0}"' "$SCRIPT"
grep -Fq 'VP_GO_WORKER_SMOKE_STRICT=1' "$SCRIPT"
if grep -Eq 'youtube|bilibili|xiaohongshu|private_upload|public' "$SCRIPT"; then
  echo 'FAIL: production video smoke must not publish' >&2
  exit 1
fi
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
bash tests/test_vp_production_smoke_script.sh
```

Expected: fail because the wrapper does not exist.

- [ ] **Step 3: Extend the strict integration test**

After the job succeeds:

- Require `source_1`, `trim_1`, and `export_1` to be `SUCCEEDED`.
- Require `trim_1` and `export_1` worker IDs to start with `ffmpeg_go-worker@`.
- Download `export_1.output_artifact_id` from `/api/v1/artifacts/{id}/download`.
- Save it when `VP_GO_SMOKE_OUTPUT` is set.
- Run:

```bash
ffprobe -v error -print_format json -show_format -show_streams OUTPUT.mp4
```

- Require one video stream, positive duration, and non-zero file size.
- Write JSON evidence with `hashlib.sha256`, IDs, worker IDs, API URL, UTC timestamp, and parsed probe output.

Add these helpers and imports to the test module:

```python
import hashlib
import json
from datetime import datetime, timezone


def node_by_id(job: dict[str, Any], node_id: str) -> dict[str, Any]:
    matches = [node for node in job["node_executions"] if node["node_id"] == node_id]
    assert len(matches) == 1, job
    return matches[0]


def download_artifact(artifact_id: str, output_path: Path) -> None:
    response = httpx.get(
        f"{PYTHON_API}/api/v1/artifacts/{artifact_id}/download",
        timeout=60,
        follow_redirects=True,
    )
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)
    assert output_path.stat().st_size > 0


def probe_video(output_path: Path) -> dict[str, Any]:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    probe = json.loads(result.stdout)
    assert any(stream.get("codec_type") == "video" for stream in probe.get("streams", []))
    assert float(probe.get("format", {}).get("duration", 0)) > 0
    return probe


def write_smoke_evidence(
    output_path: Path,
    *,
    asset_id: str,
    pipeline_id: str,
    job: dict[str, Any],
    artifact_id: str,
    worker_ids: list[str],
    probe: dict[str, Any],
) -> Path:
    evidence_path = output_path.with_suffix(".json")
    evidence = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_url": PYTHON_API,
        "source_commit": os.environ.get("VP_SMOKE_COMMIT", ""),
        "deployed_commit": os.environ.get("VP_SMOKE_DEPLOYED_COMMIT", ""),
        "asset_id": asset_id,
        "pipeline_id": pipeline_id,
        "job_id": job["id"],
        "job_status": job["status"],
        "artifact_id": artifact_id,
        "worker_ids": worker_ids,
        "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "file_size": output_path.stat().st_size,
        "probe": probe,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence_path
```

Replace the final assertions with:

```python
assert final_job["status"] == "SUCCEEDED", final_job
source_node = node_by_id(final_job, "source_1")
trim_node = node_by_id(final_job, "trim_1")
export_node = node_by_id(final_job, "export_1")
for node in (source_node, trim_node, export_node):
    assert node["status"] == "SUCCEEDED", final_job
for node in (trim_node, export_node):
    assert node["output_artifact_id"]
    assert node["worker_id"]
    assert "ffmpeg_go-worker@" in node["worker_id"]

if output_value := os.environ.get("VP_GO_SMOKE_OUTPUT"):
    output_path = Path(output_value).expanduser().resolve()
    artifact_id = export_node["output_artifact_id"]
    download_artifact(artifact_id, output_path)
    probe = probe_video(output_path)
    evidence_path = write_smoke_evidence(
        output_path,
        asset_id=asset_id,
        pipeline_id=pipeline["id"],
        job=final_job,
        artifact_id=artifact_id,
        worker_ids=[trim_node["worker_id"], export_node["worker_id"]],
        probe=probe,
    )
    print(f"retained_video={output_path}")
    print(f"retained_evidence={evidence_path}")
assert pending_count() == 0
```

- [ ] **Step 4: Implement the wrapper**

Add `.runtime/` to `.gitignore`. Create the wrapper with this complete content:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/.runtime/video-smoke"
TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
OUTPUT_PATH="$OUTPUT_DIR/vp-smoke-$TIMESTAMP.mp4"

mkdir -p "$OUTPUT_DIR"
VP_PYTHON_API="${VP_PYTHON_API:-http://10.0.0.127:18080}"
VP_REDIS_URL="${VP_REDIS_URL:-redis://10.0.0.150:6380/0}"
VP_SMOKE_COMMIT="$(git -C "$ROOT_DIR" rev-parse HEAD)"
VP_SMOKE_DEPLOYED_COMMIT="$(ssh 10.0.0.127 \
  'tr -d "\n" < /Users/wenjieliu/VideoProcess-app/.deploy-sync-source-commit')"

export VP_PYTHON_API VP_REDIS_URL VP_SMOKE_COMMIT VP_SMOKE_DEPLOYED_COMMIT
export VP_GO_WORKER_SMOKE_STRICT=1
export VP_GO_SMOKE_OUTPUT="$OUTPUT_PATH"

cd "$ROOT_DIR/backend"
uv run python -m pytest \
  ../tests/go_migration/test_go_trim_worker_smoke.py::test_trim_worker_mixed_mode_smoke_requires_real_job_completion \
  -q -s

printf 'video=%s\n' "$OUTPUT_PATH"
printf 'evidence=%s\n' "${OUTPUT_PATH%.mp4}.json"
```

It prints the absolute MP4 and JSON paths only after pytest exits zero.

- [ ] **Step 5: Verify the non-live contract**

Run:

```bash
bash tests/test_vp_production_smoke_script.sh
cd backend && uv run python -m pytest ../tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: wrapper contract passes; the strict integration test skips without live env.

- [ ] **Step 6: Commit the smoke evidence path**

```bash
git add .gitignore tests/go_migration/test_go_trim_worker_smoke.py \
  scripts/run_vp_production_video_smoke.sh \
  tests/test_vp_production_smoke_script.sh
git commit -m "test: retain production video smoke evidence"
```

---

### Task 4: Align Runtime Documentation And Deployment Contracts

**Files:**
- Modify: `deploy/four-machine-topology.md`
- Modify: `docs/constructure/infra-services.md`
- Modify: `deploy/macos/README.md`
- Modify: `tests/test_macos_deploy_paths.sh`

**Interfaces:**
- Consumes: approved dual-host design.
- Produces: one documented normal path: GitHub `main` -> scoped 150 deploy-sync -> 127 runtime; 126 explicitly excluded.

- [ ] **Step 1: Add failing documentation assertions**

Add `TOPOLOGY="$ROOT_DIR/deploy/four-machine-topology.md"` and this helper to `tests/test_macos_deploy_paths.sh`:

```bash
assert_file_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq -- "$needle" "$file"; then
    printf 'FAIL: expected %s to contain %s\n' "$file" "$needle" >&2
    exit 1
  fi
}
```

Then require these exact concepts:

```bash
assert_file_contains "$TOPOLOGY" 'node.labels.vp.runtime == true'
assert_file_contains "$TOPOLOGY" '126 is not a VideoProcess automatic failover target'
assert_file_contains "$TOPOLOGY" '--project vp-app --project vp-feature-aggregator'
```

- [ ] **Step 2: Verify RED**

Run:

```bash
bash tests/test_macos_deploy_paths.sh
```

Expected: fail because the current topology docs do not contain the new placement and scoped-sync contract.

- [ ] **Step 3: Update docs**

Document:

- 150 manager/shared/GPU role.
- 127 primary app/worker role and `vp.runtime=true` label.
- 126 ForWin/news role and explicit exclusion from normal VP operations.
- scoped deploy-sync first-manual-then-cron flow.
- commands for node, service placement, markers, host endpoints, Redis consumer, and retained video evidence checks.
- failure behavior: no automatic fallback to 126.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
bash tests/test_macos_deploy_paths.sh
git diff --check
```

Expected: pass.

- [ ] **Step 5: Commit the runtime contract**

```bash
git add deploy/four-machine-topology.md docs/constructure/infra-services.md \
  deploy/macos/README.md tests/test_macos_deploy_paths.sh
git commit -m "docs: define the videoprocess dual-host runtime"
```

---

### Task 5: Verify, Push, And Observe The Exact Commit

**Files:**
- Verify all files changed in Tasks 1-4.

**Interfaces:**
- Consumes: clean committed implementation on `main`.
- Produces: one pushed Git commit observed by the 150 mirror.

- [ ] **Step 1: Run repository verification**

```bash
cd backend
uv run python -m pytest
uv run python -m ruff check \
  ../tests/go_migration/test_go_trim_worker_smoke.py \
  app/services/worker_admission.py worker/main.py
uv run python -m mypy app || true
cd ..
go test ./...
bash tests/test_vp_colima_node.sh
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_vp_production_smoke_script.sh
bash tests/test_macos_deploy_paths.sh
git diff --check
```

Expected: pytest, targeted Ruff, Go tests, and shell contracts exit zero. Existing full mypy baseline errors are recorded without being treated as regressions.

- [ ] **Step 2: Push `main`**

```bash
git status --short --branch
git push origin main
```

Expected: `origin/main` advances to local `HEAD`.

- [ ] **Step 3: Verify GitHub visibility from 150 without deployment**

```bash
ssh 10.0.0.150 \
  '/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --dry-run --project vp-app --project vp-feature-aggregator'
```

Expected: fetch/stage/rsync dry-run references the pushed commit, only host 127, and no service update.

---

### Task 6: Restore The 127 Swarm Node

**Files:**
- Runtime state on 127 and 150; no repository edit.

**Interfaces:**
- Consumes: Task 1 installer and 150 worker join token.
- Produces: Ready node `colima-127`, label `vp.runtime=true`, no VP label on 126.

- [ ] **Step 1: Record pre-change state**

```bash
ssh 10.0.0.150 'docker node ls; docker service ls; docker service ps vp-api-swarm vp-ffmpeg-worker-go-swarm'
colima list
```

- [ ] **Step 2: Run node doctor and install**

```bash
bash deploy/macos/install_videoprocess_colima_node.sh doctor
bash deploy/macos/install_videoprocess_colima_node.sh install
```

Expected: profile `swarmbridged` is Running and manager shows `colima-127` Ready.

- [ ] **Step 3: Verify placement labels before moving services**

```bash
ssh 10.0.0.150 \
  'docker node inspect ccttww-lap colima-127 colima-swarmbridged --format "{{.Description.Hostname}} {{json .Spec.Labels}}"'
```

Expected: only `colima-127` has `vp.runtime=true`; manager has `vp.gpu=true`; 126 node has no VP label.

- [ ] **Step 4: Verify login-time persistence contract**

```bash
plutil -lint "$HOME/Library/LaunchAgents/com.constructure.vp-colima.plist"
launchctl print "gui/$(id -u)/com.constructure.vp-colima"
```

Expected: plist valid and LaunchAgent loaded.

---

### Task 7: Bootstrap The 150 Extension And Perform The First Scoped Deploy

**Files:**
- Modify operationally: `/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh` on 150.
- Backup operationally: timestamped sibling of the installed script.

**Interfaces:**
- Consumes: source-controlled `deploy/swarm/deploy-sync-extension.sh` from the fetched VideoProcess mirror.
- Produces: the installed controller loads current VP overrides after `prepare_repo`; scoped apply moves VP to 127 and starts the managed Python worker on 150.

- [ ] **Step 1: Back up and locally patch a copy of the installed controller**

Copy the file to a local temporary path, use `apply_patch`, then upload it. Add:

```bash
load_vp_extension() {
  local extension="$REPO_ROOT/videoprocess/deploy/swarm/deploy-sync-extension.sh"
  [ -r "$extension" ] || {
    log "missing VideoProcess deploy extension: $extension"
    return 1
  }
  # shellcheck source=/dev/null
  source "$extension"
}
```

Call `load_vp_extension` immediately after `prepare_repo` in `vp-app` and `vp-feature-aggregator`. In `vp-pds`, call it after preparing the PDS repo and require the already-fetched VideoProcess mirror to exist. Do not change ForWin/news branches.

- [ ] **Step 2: Validate the installed controller**

```bash
ssh 10.0.0.150 'bash -n /home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh'
ssh 10.0.0.150 \
  '/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --dry-run --project vp-app --project vp-feature-aggregator'
```

Expected: exact pushed commit, target 127, no 126 reference in VP calls, no service mutation.

- [ ] **Step 3: Run the first scoped apply with a dedicated log**

```bash
ssh 10.0.0.150 \
  '/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --apply --force --project vp-app --project vp-feature-aggregator --project vp-pds' \
  > .runtime/vp-first-dual-host-deploy.log 2>&1
```

Expected: images build on their target hosts, service updates converge, and rollback is not invoked.

- [ ] **Step 4: Verify all service placements and entry points**

```bash
ssh 10.0.0.150 'docker service ls | grep "^vp-"'
ssh 10.0.0.150 'for s in $(docker service ls --format "{{.Name}}" | grep "^vp-"); do docker service ps "$s" --filter desired-state=running --format "{{.Name}} {{.Node}} {{.CurrentState}}"; done'
curl -fsS http://10.0.0.127:18080/health
curl -fsS http://10.0.0.127:3001/ >/dev/null
```

Expected: normal VP services run on `colima-127`, Python GPU worker runs on `ccttww-lap`, and no normal VP task runs on `colima-swarmbridged`.

---

### Task 8: Generate The Video, Prove Restart Persistence, Then Enable Auto-Deploy

**Files:**
- Runtime evidence under `.runtime/video-smoke/`; ignored by git.
- Operationally modify 150 user crontab.

**Interfaces:**
- Consumes: healthy dual-host deployment.
- Produces: playable MP4, JSON evidence, restart persistence proof, and VP-only deploy schedule.

- [ ] **Step 1: Run the real video smoke**

```bash
bash scripts/run_vp_production_video_smoke.sh
```

Expected: pytest passes and prints retained MP4/JSON absolute paths.

- [ ] **Step 2: Inspect the output and evidence**

```bash
ffprobe -v error -show_streams -show_format .runtime/video-smoke/*.mp4
jq . .runtime/video-smoke/*.json
```

Expected: H.264 or another playable video stream, positive duration, non-empty SHA-256, job `SUCCEEDED`, and worker identity on the intended managed node.

- [ ] **Step 3: Restart API and worker services and verify persistence**

```bash
ssh 10.0.0.150 'docker service update --force vp-api-swarm; docker service update --force vp-autoflow-api-swarm; docker service update --force vp-ffmpeg-worker-go-swarm'
```

Wait for `1/1`, then re-fetch the evidence job and artifact URLs recorded in JSON. Expected: job and artifact remain readable and the artifact hash is unchanged.

- [ ] **Step 4: Audit Redis consumers**

```bash
ssh 10.0.0.150 'redis-cli -p 6380 XINFO CONSUMERS vp:tasks:ffmpeg ffmpeg-workers; redis-cli -p 6380 XINFO CONSUMERS vp:tasks:ffmpeg_go ffmpeg_go-workers'
```

Expected: managed identities only, pending count zero for smoke, no stale standalone consumer actively claiming work.

- [ ] **Step 5: Enable a VideoProcess-only cron entry**

Install exactly this scoped schedule while preserving the disabled global line:

```cron
*/15 * * * * /home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh --apply --project vp-app --project vp-feature-aggregator >> /home/taiwei/deploy-github-sync/logs/vp-cron.log 2>&1
```

Do not include `forwin`, `arb`, `news-server`, or an unscoped invocation.

- [ ] **Step 6: Verify one idempotent automatic cycle**

After the next scheduled run:

```bash
ssh 10.0.0.150 'tail -n 80 /home/taiwei/deploy-github-sync/logs/vp-cron.log; crontab -l'
```

Expected: both VP projects report unchanged at the deployed commit; no service is rebuilt; all health and placement checks remain green.

- [ ] **Step 7: Final verification snapshot**

Capture:

```bash
git status --short --branch
git log -8 --oneline --decorate
ssh 10.0.0.150 'docker node ls; docker service ls | grep "^vp-"; grep -E "vp-app|vp-feature-aggregator" /home/taiwei/deploy-github-sync/state/deploy-state.tsv'
curl -fsS http://10.0.0.127:18080/health
```

Expected: local and origin `main` agree, deployed markers match, services are healthy on intended nodes, retained video evidence exists, and the only unrelated local file remains untouched.
