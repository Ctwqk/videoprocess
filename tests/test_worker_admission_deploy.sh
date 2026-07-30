#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
TEST_ROOT="$(cd "$TEST_ROOT" && pwd -P)"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
mkdir -p "$ROOT"
log() {
  :
}
source "$EXTENSION"
mkdir -p "$(vp_worker_admission_root)"
chmod 0700 "$(vp_worker_admission_root)"

if grep -Eq 'chown[[:space:]]+-R' "$EXTENSION"; then
  echo 'FAIL: Python worker one-shot recursively chowns caller state' >&2
  exit 1
fi

(
  helper_root="$TEST_ROOT/helper-bind-guards"
  ROOT="$helper_root/sync"
  REPO_ROOT="$helper_root/repos"
  admission_root="$ROOT/state/vp-worker-admission"
  mkdir -p "$admission_root"
  chmod 0700 "$admission_root"
  secret="$helper_root/secret"
  printf '%s\n' 'guard-secret' >"$secret"
  chmod 0400 "$secret"
  unrelated="$helper_root/unrelated"
  mkdir -p "$unrelated"
  chmod 0700 "$unrelated"
  printf '%s\n' 'preserve' >"$unrelated/preserve"
  chmod 0600 "$unrelated/preserve"

  DOCKER_GUARD_CALLS="$helper_root/docker-calls"
  : >"$DOCKER_GUARD_CALLS"
  docker() {
    printf 'docker|%s\n' "$*" >>"$DOCKER_GUARD_CALLS"
  }

  symlink_source="$helper_root/symlink-state"
  ln -s "$unrelated" "$symlink_source"
  set +e
  vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$symlink_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1
  symlink_status=$?
  set -e
  if [[ "$symlink_status" -eq 0 ]]; then
    echo 'FAIL: symlink bind source was accepted' >&2
    exit 1
  fi
  if [[ "$(<"$unrelated/preserve")" != preserve ]]; then
    echo 'FAIL: symlink rejection changed unrelated content' >&2
    exit 1
  fi
  if [[ -s "$DOCKER_GUARD_CALLS" ]]; then
    echo 'FAIL: symlink bind source reached Docker' >&2
    exit 1
  fi
  if compgen -G "$admission_root/one-shot-runs/run.*" >/dev/null; then
    echo 'FAIL: symlink rejection retained staged one-shot state' >&2
    exit 1
  fi

  first_guard_source="$helper_root/first-guard-state"
  mkdir -p "$first_guard_source"
  chmod 0700 "$first_guard_source"
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state,/requests \
    --mount "type=bind,src=$first_guard_source,dst=/runtime-state" \
    --mount "type=bind,src=$symlink_source,dst=/requests" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: partially guarded bind set was accepted' >&2
    exit 1
  fi
  if compgen -G "$first_guard_source/.vp-python-worker-bind-*" \
    >/dev/null; then
    echo 'FAIL: partial bind validation retained an earlier sentinel' >&2
    exit 1
  fi
  if compgen -G "$admission_root/one-shot-runs/run.*" >/dev/null; then
    echo 'FAIL: partial bind validation retained staged one-shot state' >&2
    exit 1
  fi
  if [[ -s "$DOCKER_GUARD_CALLS" ]]; then
    echo 'FAIL: partially guarded bind set reached Docker' >&2
    exit 1
  fi

  hardlink_source="$helper_root/hardlink-state"
  mkdir -p "$hardlink_source"
  chmod 0700 "$hardlink_source"
  printf '%s\n' 'linked' >"$hardlink_source/state"
  chmod 0600 "$hardlink_source/state"
  ln "$hardlink_source/state" "$helper_root/state-hardlink"
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$hardlink_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: hardlink-bearing bind source was accepted' >&2
    exit 1
  fi
  [[ "$(<"$helper_root/state-hardlink")" == linked ]]
  [[ ! -s "$DOCKER_GUARD_CALLS" ]]

  race_source="$helper_root/race-state"
  displaced_source="$helper_root/race-state-displaced"
  mkdir -p "$race_source"
  chmod 0700 "$race_source"
  docker() {
    printf 'docker|%s\n' "$*" >>"$DOCKER_GUARD_CALLS"
    mv "$race_source" "$displaced_source"
    ln -s "$unrelated" "$race_source"
  }
  if vp_run_python_worker_container \
    synthetic-image "$secret" synthetic-secret /runtime-state \
    --mount "type=bind,src=$race_source,dst=/runtime-state" \
    -- /bin/true >/dev/null 2>&1; then
    echo 'FAIL: bind source replacement was accepted' >&2
    exit 1
  fi
  [[ -L "$race_source" ]]
  [[ "$(<"$unrelated/preserve")" == preserve ]]
  if compgen -G "$unrelated/.vp-python-worker-bind-*" >/dev/null; then
    echo 'FAIL: bind replacement cleanup touched unrelated target' >&2
    exit 1
  fi
)

VP_WORKER_ADMISSION_COMMIT=0123456789abcdef0123456789abcdef01234567
VP_WORKER_FFMPEG_GO_GENERATION=101
VP_WORKER_FFMPEG_GENERATION=102
VP_WORKER_VISION_GENERATION=103
VP_WORKER_YOUTUBE_PUBLISHER_GENERATION=104
VP_WORKER_FFMPEG_GO_DATABASE_SECRET=go-db-secret
VP_WORKER_FFMPEG_GO_ADMISSION_SECRET=go-admission-secret
VP_WORKER_FFMPEG_DATABASE_SECRET=ffmpeg-db-secret
VP_WORKER_FFMPEG_ADMISSION_SECRET=ffmpeg-admission-secret
VP_WORKER_VISION_DATABASE_SECRET=vision-db-secret
VP_WORKER_VISION_ADMISSION_SECRET=vision-admission-secret
VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET=publisher-db-secret
VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET=publisher-admission-secret
VP_WORKER_REDIS_FFMPEG_GO_SECRET=go-redis-secret
VP_WORKER_REDIS_FFMPEG_SECRET=ffmpeg-redis-secret
VP_WORKER_REDIS_VISION_SECRET=vision-redis-secret
VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=publisher-redis-secret
VP_WORKER_MINIO_ACCESS_SECRET=worker-minio-access-secret
VP_WORKER_MINIO_SECRET_SECRET=worker-minio-secret-secret

assert_worker_contract() {
  local service="$1"
  local image="$2"
  local generation="$3"
  local worker_type="$4"
  local host="$5"
  local capabilities="$6"
  local stream="$7"
  local group="$8"
  local expected_db_secret="$9"
  local expected_admission_secret="${10}"
  local expected_redis_secret="${11}"
  local env_file="$TEST_ROOT/$worker_type.env"
  local secret_file="$TEST_ROOT/$worker_type.secrets"

  vp_worker_service_registration_env "$service" "$image" >"$env_file"
  grep -Fxq "DEPLOY_MODE=production" "$env_file"
  grep -Fxq "WORKER_SERVICE_NAME=$service" "$env_file"
  grep -Fxq "WORKER_ADMISSION_GENERATION=$generation" "$env_file"
  grep -Fxq "WORKER_TYPE=$worker_type" "$env_file"
  grep -Fxq "WORKER_HOST=$host" "$env_file"
  grep -Fxq "WORKER_CAPABILITIES=$capabilities" "$env_file"
  grep -Fxq "WORKER_RELEASE_COMMIT=$VP_WORKER_ADMISSION_COMMIT" "$env_file"
  grep -Fxq "WORKER_IMAGE_IDENTITY=$image" "$env_file"
  grep -Fxq "WORKER_REDIS_STREAM=$stream" "$env_file"
  grep -Fxq "WORKER_REDIS_GROUP=$group" "$env_file"
  grep -Fxq "WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url" "$env_file"
  grep -Fxq "WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token" "$env_file"
  grep -Fxq "WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url" "$env_file"
  grep -Fxq "WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key" "$env_file"
  grep -Fxq "WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key" "$env_file"
  grep -Fxq "VP_REQUIRE_STAGING_JANITOR=true" "$env_file"
  if grep -Eq \
    '^(DATABASE_URL|REDIS_URL|WORKER_ADMISSION_TOKEN|MINIO_ACCESS_KEY|MINIO_SECRET_KEY)=' \
    "$env_file"; then
    echo "FAIL: $service exposes a credential in its environment" >&2
    exit 1
  fi

  vp_worker_service_secret_specs "$service" >"$secret_file"
  grep -Fxq \
    "source=$expected_db_secret,target=vp-worker-database-url,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$expected_admission_secret,target=vp-worker-admission-token,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$expected_redis_secret,target=vp-worker-redis-url,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$VP_WORKER_MINIO_ACCESS_SECRET,target=vp-worker-minio-access-key,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  grep -Fxq \
    "source=$VP_WORKER_MINIO_SECRET_SECRET,target=vp-worker-minio-secret-key,uid=10001,gid=10001,mode=0400" \
    "$secret_file"
  [[ "$(wc -l <"$secret_file" | tr -d ' ')" -eq 5 ]]
}

assert_worker_contract \
  vp-ffmpeg-worker-go-swarm \
  vp-ffmpeg-worker-go:deploy-0123456789ab \
  101 ffmpeg_go colima-127 media_cpu \
  vp:tasks:ffmpeg_go ffmpeg_go-workers \
  go-db-secret go-admission-secret go-redis-secret
assert_worker_contract \
  vp-ffmpeg-worker-gpu-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  102 ffmpeg 150-gpu media_gpu \
  vp:tasks:ffmpeg ffmpeg-workers \
  ffmpeg-db-secret ffmpeg-admission-secret ffmpeg-redis-secret
assert_worker_contract \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  103 vision 150-vision vision_gpu \
  vp:tasks:vision vision-workers \
  vision-db-secret vision-admission-secret vision-redis-secret
assert_worker_contract \
  vp-youtube-publisher-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  104 youtube_publisher 150-publisher youtube_publisher \
  vp:tasks:youtube_publisher youtube_publisher-workers \
  publisher-db-secret publisher-admission-secret publisher-redis-secret

DOCKER_SECRET_PAYLOAD="$TEST_ROOT/docker-secret-payload"
docker() {
  if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "secret create" ]]; then
    cat >"$DOCKER_SECRET_PAYLOAD"
    return 0
  fi
  return 90
}

credential_file="$TEST_ROOT/credential"
printf '%s\n' "credential-material" >"$credential_file"
chmod 0600 "$credential_file"
if vp_worker_admission_create_secret \
  test-secret "$credential_file" test-service 105 database \
  >/dev/null 2>&1; then
  echo 'FAIL: mode-0600 credential input was accepted' >&2
  exit 1
fi
chmod 0400 "$credential_file"
vp_worker_admission_create_secret \
  test-secret "$credential_file" test-service 105 database
grep -Fxq "credential-material" "$DOCKER_SECRET_PAYLOAD"

generated_credential="$TEST_ROOT/generated-credential"
vp_worker_admission_write_secret_file \
  "$generated_credential" "generated-material"
[[ "$(vp_worker_redis_marker_file_mode "$generated_credential")" == 400 ]]

read_url_file="$TEST_ROOT/deploy-read-url"
printf '%s\n' \
  'postgresql://deploy_read:credential@vp-postgres/videoprocess' \
  >"$read_url_file"
chmod 0400 "$read_url_file"
VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="$read_url_file"
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
VP_WORKER_ADMISSION_PREPARED=true
SERVICE_SPEC="$TEST_ROOT/service-spec.json"
cat >"$SERVICE_SPEC" <<EOF
{
  "Mode": {"Replicated": {"Replicas": 1}},
  "TaskTemplate": {
    "ContainerSpec": {
      "Image": "vp-ffmpeg-worker-python:deploy-0123456789ab",
      "Env": [
        "DEPLOY_MODE=production",
        "WORKER_SERVICE_NAME=vp-vision-worker-swarm",
        "WORKER_ADMISSION_GENERATION=103",
        "WORKER_SLOT=1",
        "WORKER_TYPE=vision",
        "WORKER_HOST=150-vision",
        "WORKER_CAPABILITIES=vision_gpu",
        "WORKER_RELEASE_COMMIT=$VP_WORKER_ADMISSION_COMMIT",
        "WORKER_IMAGE_IDENTITY=vp-ffmpeg-worker-python:deploy-0123456789ab",
        "WORKER_REDIS_STREAM=vp:tasks:vision",
        "WORKER_REDIS_GROUP=vision-workers",
        "WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url",
        "WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token",
        "WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url",
        "WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key",
        "WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key",
        "VP_REQUIRE_STAGING_JANITOR=true"
      ],
      "Secrets": [
        {"SecretName": "vision-db-secret", "File": {"Name": "vp-worker-database-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "vision-admission-secret", "File": {"Name": "vp-worker-admission-token", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "vision-redis-secret", "File": {"Name": "vp-worker-redis-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "worker-minio-access-secret", "File": {"Name": "vp-worker-minio-access-key", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "worker-minio-secret-secret", "File": {"Name": "vp-worker-minio-secret-key", "UID": "10001", "GID": "10001", "Mode": 256}}
      ]
    },
    "Placement": {
      "Constraints": [
        "node.labels.vp.gpu==true",
        "node.hostname==ccttww-lap"
      ]
    },
    "Networks": [{"Target": "vp-pipeline-network-id"}]
  }
}
EOF
cp "$SERVICE_SPEC" "$TEST_ROOT/valid-worker-service-spec.json"
READINESS_ATTEMPTS="$TEST_ROOT/readiness-attempts"
printf '0\n' >"$READINESS_ATTEMPTS"
docker() {
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    printf '%s\n' \
      "${NETWORK_IDENTITY_OUTPUT:-vp-pipeline-network-id|vp-pipeline-net|overlay|swarm}"
    return
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" \
    && "$*" == *"{{json .Spec}}"* ]]; then
    cat "$SERVICE_SPEC"
    return
  fi
  if [[ "${1:-} ${2:-}" == "service ls" ]]; then
    printf '%s\n' "${CONTROL_SERVICE_NAMES:-}"
    return
  fi
  if [[ "${1:-}" == run \
    && "$*" == *"worker_deployment_cli readiness"* ]]; then
    local attempts
    attempts="$(<"$READINESS_ATTEMPTS")"
    attempts=$((attempts + 1))
    printf '%s\n' "$attempts" >"$READINESS_ATTEMPTS"
    [[ "$attempts" -ge 3 ]]
    return
  fi
  return 90
}
sleep() {
  [[ "$1" == 2 ]]
}
VP_WORKER_DEPLOY_READINESS_ATTEMPTS=3
VP_WORKER_DEPLOY_READINESS_INTERVAL_SECONDS=2
vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab
vp_require_worker_deployment_ready vp-vision-worker-swarm
[[ "$(<"$READINESS_ATTEMPTS")" == 3 ]]

python3 - \
  "$TEST_ROOT/valid-worker-service-spec.json" \
  "$SERVICE_SPEC" <<'PY'
import json
import sys

source, target = sys.argv[1:]
with open(source, encoding="utf-8") as handle:
    spec = json.load(handle)
spec["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["File"]["UID"] = "0"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(spec, handle)
PY
if vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: worker descriptor accepted a root-owned runtime secret' >&2
  exit 1
fi

sed 's/"vision-redis-secret"/"worker-deploy-read-secret"/' \
  "$TEST_ROOT/valid-worker-service-spec.json" \
  >"$TEST_ROOT/bad-service-spec.json"
mv "$TEST_ROOT/bad-service-spec.json" "$SERVICE_SPEC"
if vp_require_worker_service_descriptor \
  vp-vision-worker-swarm \
  vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: worker descriptor accepted a control secret' >&2
  exit 1
fi

VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
VP_STAGING_JANITOR_DATABASE_SECRET=staging-db-secret
VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=staging-minio-access-secret
VP_STAGING_JANITOR_MINIO_SECRET_SECRET=staging-minio-secret-secret
admission_root="$(vp_worker_admission_root)"
mkdir -p "$admission_root"
cat >"$admission_root/staging-object-janitor.conf" <<EOF
VERSION=2
GENERATION=$VP_WORKER_CONTROL_GENERATION
IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
NETWORK=vp-pipeline-net
NETWORK_ID=vp-pipeline-network-id
DATABASE_SECRET=$VP_STAGING_JANITOR_DATABASE_SECRET
MINIO_ACCESS_SECRET=$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET
MINIO_SECRET_SECRET=$VP_STAGING_JANITOR_MINIO_SECRET_SECRET
EVIDENCE_VOLUME=vp-staging-janitor-evidence
MANAGER_NODE=ccttww-lap
EOF
chmod 0600 "$admission_root/staging-object-janitor.conf"
cat >"$SERVICE_SPEC" <<EOF
{
  "Labels": {
    "vp.videoprocess.job": "staging-object-janitor",
    "vp.videoprocess.generation": "$VP_WORKER_CONTROL_GENERATION"
  },
  "Mode": {
    "ReplicatedJob": {"MaxConcurrent": 1, "TotalCompletions": 1}
  },
  "TaskTemplate": {
    "ContainerSpec": {
      "Image": "vp-ffmpeg-worker-python:deploy-0123456789ab",
      "User": "10001:10001",
      "Args": ["python", "-m", "app.channel_agent.staging_object_janitor_cli"],
      "Env": [
        "DEPLOY_MODE=production",
        "VP_STAGING_JANITOR_RUNNER_ID=ccttww-lap",
        "VP_STAGING_JANITOR_DATABASE_URL_FILE=/run/secrets/vp-staging-janitor-database-url",
        "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-staging-janitor-minio-access-key",
        "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE=/run/secrets/vp-staging-janitor-minio-secret-key",
        "VP_STAGING_JANITOR_STATUS_FILE=/run/videoprocess/staging-janitor/status.json",
        "STORAGE_BACKEND=minio",
        "MINIO_ENDPOINT=10.0.0.150:9000",
        "MINIO_BUCKET=videoprocess"
      ],
      "Secrets": [
        {"SecretName": "$VP_STAGING_JANITOR_DATABASE_SECRET", "File": {"Name": "vp-staging-janitor-database-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET", "File": {"Name": "vp-staging-janitor-minio-access-key", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$VP_STAGING_JANITOR_MINIO_SECRET_SECRET", "File": {"Name": "vp-staging-janitor-minio-secret-key", "UID": "10001", "GID": "10001", "Mode": 256}}
      ],
      "Mounts": [
        {"Type": "volume", "Source": "vp-staging-janitor-evidence", "Target": "/run/videoprocess/staging-janitor"}
      ]
    },
    "RestartPolicy": {"Condition": "none"},
    "Placement": {"Constraints": ["node.hostname==ccttww-lap"]},
    "Networks": [{"Target": "vp-pipeline-network-id"}]
  }
}
EOF
vp_require_staging_object_janitor_control \
  "$admission_root" vp-ffmpeg-worker-python:deploy-0123456789ab
sed 's/"TotalCompletions": 1/"TotalCompletions": 2/' \
  "$SERVICE_SPEC" >"$TEST_ROOT/bad-janitor-spec.json"
mv "$TEST_ROOT/bad-janitor-spec.json" "$SERVICE_SPEC"
if vp_require_staging_object_janitor_control \
  "$admission_root" vp-ffmpeg-worker-python:deploy-0123456789ab; then
  echo 'FAIL: control retirement accepted a multi-completion janitor job' >&2
  exit 1
fi

if vp_worker_control_generation_unused \
  c-11111111111111111111 >/dev/null 2>&1; then
  echo 'FAIL: control retirement ignored a dependency inspection failure' >&2
  exit 1
fi
vp_worker_control_generation_unused \
  c-11111111111111111111 true
CONTROL_SERVICE_NAMES=vp-ffmpeg-worker-go-swarm
if vp_worker_control_generation_unused \
  c-11111111111111111111 true >/dev/null 2>&1; then
  echo 'FAIL: candidate cleanup treated an existing service as absent' >&2
  exit 1
fi
CONTROL_SERVICE_NAMES=

VP_PIPELINE_NETWORK_ID=""
NETWORK_IDENTITY_OUTPUT='substitute-id|substitute-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: substitute pipeline network name was accepted' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
vp_require_pipeline_network_identity
VP_PIPELINE_NETWORK_ID=""
NETWORK_IDENTITY_OUTPUT=$'vp-pipeline-network-id|vp-pipeline-net|overlay|swarm\nsecond-id|vp-pipeline-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: ambiguous pipeline network inspection was accepted' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
vp_require_pipeline_network_identity
NETWORK_IDENTITY_OUTPUT='recreated-network-id|vp-pipeline-net|overlay|swarm'
if vp_require_pipeline_network_identity >/dev/null 2>&1; then
  echo 'FAIL: pipeline network identity changed inside one transaction' >&2
  exit 1
fi
NETWORK_IDENTITY_OUTPUT='vp-pipeline-network-id|vp-pipeline-net|overlay|swarm'
if grep -Eq 'docker[[:space:]]+network[[:space:]]+create' "$EXTENSION"; then
  echo 'FAIL: deployment creates or substitutes the pipeline network' >&2
  exit 1
fi

if grep -Eiq 'ACL[[:space:]]+SETUSER|redis-cli[^\n]*ACL' "$EXTENSION"; then
  echo 'FAIL: VideoProcess deployment attempts to own Redis ACL users' >&2
  exit 1
fi
if grep -Eq 'VP_RUNTIME_HOST=.*10\\.0\\.0\\.126|remote_sh[[:space:]]+10\\.0\\.0\\.126' \
  "$EXTENSION"; then
  echo 'FAIL: worker deployment targets host 126' >&2
  exit 1
fi
for worker_cli in \
  app.channel_agent.staging_object_janitor_cli \
  app.services.worker_control_role_cli \
  app.services.worker_deployment_cli \
  app.services.worker_registration_operator_cli \
  app.services.worker_runtime_role_cli; do
  if ! grep -Fq "$worker_cli" "$ROOT_DIR/backend/Dockerfile.worker"; then
    echo "FAIL: Python worker image does not verify CLI: $worker_cli" >&2
    exit 1
  fi
done
python_worker_dockerfile="$ROOT_DIR/backend/Dockerfile.worker"
if ! grep -Fq '/usr/local/share/videoprocess/worker-build-commit' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image does not use a fixed build identity artifact' >&2
  exit 1
fi
if ! grep -Fq 'USER videoprocess-worker' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image does not use its dedicated runtime user' >&2
  exit 1
fi
if ! grep -Fq '["/opt/venv/bin/python", "-I", "-m", "worker.main"]' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker entrypoint does not reject import path injection' >&2
  exit 1
fi
if grep -Fq '/app/worker/_build_identity.py' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker still uses an import-shadowable identity module' >&2
  exit 1
fi
if grep -Eq '^ENV[[:space:]]+VP_BUILD_COMMIT=' \
  "$python_worker_dockerfile"; then
  echo 'FAIL: Python worker image exposes a runtime-overridable build commit' >&2
  exit 1
fi
python_worker_image_test="$ROOT_DIR/tests/test_python_worker_image_identity.sh"
if [[ ! -x "$python_worker_image_test" ]] \
  || ! bash -n "$python_worker_image_test"; then
  echo 'FAIL: Python worker image behavior probe is missing or invalid' >&2
  exit 1
fi

control_manifest="$TEST_ROOT/control-current.conf"
vp_worker_control_write_manifest \
  "$control_manifest" \
  c-0123456789abcdef0123 \
  vp-ffmpeg-worker-python:deploy-0123456789ab
[[ "$(vp_worker_redis_marker_file_mode "$control_manifest")" == 600 ]]
vp_worker_control_read_manifest "$control_manifest"
[[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == \
  c-0123456789abcdef0123 ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET" == \
  vp-wc-operator-c-0123456789abcdef0123 ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET" == \
  vp-wc-worker-minio-secret-c-0123456789abcdef0123 ]]

stale_control_root="$TEST_ROOT/stale-control"
active_control_generation=c-0123456789abcdef0123
active_control_image=vp-ffmpeg-worker-python:deploy-0123456789ab
stale_control_generation=c-11111111111111111111
stale_control_image=vp-ffmpeg-worker-python:deploy-111111111111
vp_worker_control_write_manifest \
  "$stale_control_root/control-candidates/$active_control_generation.conf" \
  "$active_control_generation" "$active_control_image"
vp_worker_control_write_manifest \
  "$stale_control_root/control-candidates/$stale_control_generation.conf" \
  "$stale_control_generation" "$stale_control_image"
STALE_CONTROL_CALLS="$TEST_ROOT/stale-control-calls"
: >"$STALE_CONTROL_CALLS"
VP_WORKER_CONTROL_PRIOR_GENERATION="$active_control_generation"
VP_WORKER_CONTROL_PRIOR_IMAGE="$active_control_image"
vp_worker_control_retire_generation() {
  printf 'retire|%s|%s\n' "$1" "$2" >>"$STALE_CONTROL_CALLS"
}
vp_worker_control_cleanup_stale_candidates "$stale_control_root"
[[ ! -e "$stale_control_root/control-candidates/$active_control_generation.conf" ]]
[[ ! -e "$stale_control_root/control-candidates/$stale_control_generation.conf" ]]
[[ "$(<"$STALE_CONTROL_CALLS")" == \
  "retire|$stale_control_image|$stale_control_generation" ]]

control_retirement_root="$TEST_ROOT/control-retirement"
control_retirement_generation=c-11111111111111111111
control_retirement_image=vp-ffmpeg-worker-python:deploy-111111111111
vp_worker_control_schedule_retirement \
  "$control_retirement_root" \
  "$control_retirement_image" \
  "$control_retirement_generation"
control_retirement_journal="$control_retirement_root/control-retirements/$control_retirement_generation.conf"
[[ "$(vp_worker_redis_marker_file_mode "$control_retirement_journal")" == 600 ]]
vp_worker_control_read_manifest "$control_retirement_journal"
[[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == \
  "$control_retirement_generation" ]]
[[ "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$control_retirement_image" ]]
vp_worker_control_schedule_retirement \
  "$control_retirement_root" \
  "$control_retirement_image" \
  "$control_retirement_generation"

CONTROL_RETIREMENT_CALLS="$TEST_ROOT/control-retirement-calls"
: >"$CONTROL_RETIREMENT_CALLS"
CONTROL_RETIREMENT_FAIL=true
vp_worker_control_retire_generation() {
  printf 'retire|%s|%s\n' "$1" "$2" >>"$CONTROL_RETIREMENT_CALLS"
  [[ "$CONTROL_RETIREMENT_FAIL" == false ]]
}
if vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123; then
  echo 'FAIL: failed control retirement discarded its journal' >&2
  exit 1
fi
[[ -f "$control_retirement_journal" ]]
CONTROL_RETIREMENT_FAIL=false
vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123
[[ ! -e "$control_retirement_journal" ]]
vp_worker_control_process_retirements \
  "$control_retirement_root" c-0123456789abcdef0123
[[ "$(grep -c '^retire|' "$CONTROL_RETIREMENT_CALLS")" -eq 2 ]]

worker_retirement_root="$TEST_ROOT/worker-retirement"
worker_retirement_journal="$worker_retirement_root/retirements/stale-commit.records"
worker_retirement_records='vp-ffmpeg-worker-go-swarm|811|stale-db-811|stale-admission-811'
vp_worker_admission_write_retirement_journal \
  "$worker_retirement_journal" "$worker_retirement_records"
[[ "$(vp_worker_redis_marker_file_mode "$worker_retirement_journal")" == 600 ]]
WORKER_RETIREMENT_CALLS="$TEST_ROOT/worker-retirement-calls"
: >"$WORKER_RETIREMENT_CALLS"
WORKER_RETIREMENT_FAIL=true
vp_worker_admission_retire_records() {
  printf 'retire|%s\n' "$1" >>"$WORKER_RETIREMENT_CALLS"
  [[ "$WORKER_RETIREMENT_FAIL" == false ]]
}
if vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"; then
  echo 'FAIL: failed worker retirement discarded its journal' >&2
  exit 1
fi
[[ -f "$worker_retirement_journal" ]]
WORKER_RETIREMENT_FAIL=false
vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"
[[ ! -e "$worker_retirement_journal" ]]
vp_worker_admission_process_retirement_journals \
  "$worker_retirement_root"
[[ "$(grep -c '^retire|' "$WORKER_RETIREMENT_CALLS")" -eq 2 ]]

VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=partial-generation
VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
vp_require_pipeline_network_identity() {
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
}
vp_worker_admission_new_generation() {
  printf '901\n'
}
vp_worker_admission_required_file() {
  return 1
}
partial_root="$(vp_worker_admission_root)"
if vp_worker_admission_prepare_service \
  vp-ffmpeg-worker-go-swarm \
  vp-ffmpeg-worker-go:deploy-0123456789ab \
  vp-ffmpeg-worker-python:deploy-0123456789ab \
  "$VP_WORKER_ADMISSION_COMMIT" \
  "$partial_root" \
  "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"; then
  echo 'FAIL: partial worker preparation unexpectedly succeeded' >&2
  exit 1
fi
[[ "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" == \
  vp-ffmpeg-worker-go-swarm ]]
partial_records="$(vp_worker_admission_candidate_records)"
[[ "$partial_records" == \
  'vp-ffmpeg-worker-go-swarm|901|vp-wr-ffmpeg-go-db-901|vp-wr-ffmpeg-go-admission-901' ]]

CLEANUP_CALLS="$TEST_ROOT/cleanup-calls"
: >"$CLEANUP_CALLS"
CLEANUP_GRANT_STATE=active
vp_worker_admission_generation_state() {
  printf '%s\n' "$CLEANUP_GRANT_STATE"
}
vp_worker_admission_retirement_ids() {
  :
}
vp_worker_admission_operator() {
  printf 'operator|%s\n' "$*" >>"$CLEANUP_CALLS"
}
vp_worker_admission_required_file() {
  printf '%s\n' "$TEST_ROOT/runtime-owner"
}
printf '%s\n' 'synthetic-runtime-owner' >"$TEST_ROOT/runtime-owner"
chmod 0400 "$TEST_ROOT/runtime-owner"
docker() {
  if [[ "${1:-}" == run ]]; then
    printf 'runtime-role|%s\n' "$*" >>"$CLEANUP_CALLS"
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "secret inspect" ]]; then
    return 1
  fi
  return 90
}
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
vp_worker_admission_retire_generation \
  vp-ffmpeg-worker-go-swarm 901 \
  vp-wr-ffmpeg-go-db-901 vp-wr-ffmpeg-go-admission-901 \
  "$partial_root"
grep -Fq \
  'revoke-grant --service-name vp-ffmpeg-worker-go-swarm --generation 901 --reason replaced' \
  "$CLEANUP_CALLS"
CLEANUP_GRANT_STATE=absent
: >"$CLEANUP_CALLS"
vp_worker_admission_retire_generation \
  vp-ffmpeg-worker-go-swarm 901 \
  vp-wr-ffmpeg-go-db-901 vp-wr-ffmpeg-go-admission-901 \
  "$partial_root"
if grep -Fq 'operator|' "$CLEANUP_CALLS"; then
  echo 'FAIL: absent partial grant was sent to the operator' >&2
  exit 1
fi
grep -Fq 'worker_runtime_role_cli revoke' "$CLEANUP_CALLS"

CONTROL_COMMIT_CALLS="$TEST_ROOT/control-commit-calls"
: >"$CONTROL_COMMIT_CALLS"
VP_WORKER_CONTROL_PREPARED=true
VP_WORKER_ADMISSION_COMMITTED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-0123456789ab
VP_WORKER_CONTROL_PRIOR_GENERATION=c-11111111111111111111
VP_WORKER_CONTROL_PRIOR_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
vp_require_worker_service_descriptor() {
  printf 'descriptor|%s\n' "$1" >>"$CONTROL_COMMIT_CALLS"
}
vp_require_staging_object_janitor_control() {
  printf 'janitor|converged\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_write_manifest() {
  printf 'manifest|current\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_schedule_retirement() {
  printf 'control|journal|%s|%s\n' "$2" "$3" \
    >>"$CONTROL_COMMIT_CALLS"
}
vp_worker_control_process_retirements() {
  printf 'control|process\n' >>"$CONTROL_COMMIT_CALLS"
}
vp_commit_worker_control_generation
[[ "$(grep -c '^descriptor|' "$CONTROL_COMMIT_CALLS")" -eq 4 ]]
janitor_line="$(grep -n '^janitor|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
journal_line="$(grep -n '^control|journal|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
manifest_line="$(grep -n '^manifest|' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
process_line="$(grep -n '^control|process$' "$CONTROL_COMMIT_CALLS" | cut -d: -f1)"
if [[ -z "$janitor_line" || -z "$journal_line" \
  || -z "$manifest_line" || -z "$process_line" \
  || "$janitor_line" -ge "$journal_line" \
  || "$journal_line" -ge "$manifest_line" \
  || "$manifest_line" -ge "$process_line" ]]; then
  echo 'FAIL: control generation retired before dependencies converged' >&2
  exit 1
fi

echo "worker admission deployment contract tests passed"
