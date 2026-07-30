#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="vp-staging-object-janitor"
CONFIG_FILE="${VP_STAGING_JANITOR_CONFIG_FILE:-}"
ACTION="${1:-run}"
[[ "$#" -le 1 && ( "$ACTION" == run || "$ACTION" == retire ) ]] \
  || {
    printf 'staging janitor launcher failed: invalid action\n' >&2
    exit 2
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

fail() {
  printf 'staging janitor launcher failed: %s\n' "$1" >&2
  exit 1
}

[[ "$CONFIG_FILE" = /* && -f "$CONFIG_FILE" && ! -L "$CONFIG_FILE" ]] \
  || fail "configuration is absent"
[[ "$(file_mode "$CONFIG_FILE")" == 600 ]] \
  || fail "configuration mode is invalid"

version=""
generation=""
image=""
network=""
network_id=""
database_secret=""
minio_access_secret=""
minio_secret_secret=""
evidence_volume=""
manager_node=""
while IFS='=' read -r key value; do
  [[ -n "$key" && -n "$value" && "$value" != *$'\r'* ]] \
    || fail "configuration is invalid"
  case "$key" in
    VERSION)
      [[ -z "$version" ]] || fail "configuration has duplicate fields"
      version="$value"
      ;;
    GENERATION)
      [[ -z "$generation" ]] || fail "configuration has duplicate fields"
      generation="$value"
      ;;
    IMAGE)
      [[ -z "$image" ]] || fail "configuration has duplicate fields"
      image="$value"
      ;;
    NETWORK)
      [[ -z "$network" ]] || fail "configuration has duplicate fields"
      network="$value"
      ;;
    NETWORK_ID)
      [[ -z "$network_id" ]] || fail "configuration has duplicate fields"
      network_id="$value"
      ;;
    DATABASE_SECRET)
      [[ -z "$database_secret" ]] || fail "configuration has duplicate fields"
      database_secret="$value"
      ;;
    MINIO_ACCESS_SECRET)
      [[ -z "$minio_access_secret" ]] || fail "configuration has duplicate fields"
      minio_access_secret="$value"
      ;;
    MINIO_SECRET_SECRET)
      [[ -z "$minio_secret_secret" ]] || fail "configuration has duplicate fields"
      minio_secret_secret="$value"
      ;;
    EVIDENCE_VOLUME)
      [[ -z "$evidence_volume" ]] || fail "configuration has duplicate fields"
      evidence_volume="$value"
      ;;
    MANAGER_NODE)
      [[ -z "$manager_node" ]] || fail "configuration has duplicate fields"
      manager_node="$value"
      ;;
    *)
      fail "configuration has an unknown field"
      ;;
  esac
done <"$CONFIG_FILE"

[[ "$version" == 2 ]] || fail "configuration version is invalid"
[[ "$generation" =~ ^c-[0-9a-f]{20}$ ]] \
  || fail "generation is invalid"
[[ "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ ]] \
  || fail "image is invalid"
image_commit="${image##*:deploy-}"
[[ "${generation#c-}" == "$image_commit"* ]] \
  || fail "generation and image do not match"
[[ "$network" == vp-pipeline-net && "$manager_node" == ccttww-lap ]] \
  || fail "topology is invalid"
[[ "$network_id" =~ ^[A-Za-z0-9._:-]+$ ]] \
  || fail "network identity is invalid"
topology="$(
  printf '%s' \
    "$generation $image $network $network_id $database_secret $minio_access_secret $minio_secret_secret $evidence_volume $manager_node" \
    | tr '[:upper:]' '[:lower:]'
)"
case "$topology" in
  *10.0.0.126*|*colima-126*|*colima-swarmbridged*|*caspers-mac-mini*)
    fail "host 126 is forbidden"
    ;;
esac
for name in \
  "$database_secret" \
  "$minio_access_secret" \
  "$minio_secret_secret" \
  "$evidence_volume"; do
  [[ "$name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] \
    || fail "managed object name is invalid"
done
[[ "$database_secret" != "$minio_access_secret" \
  && "$database_secret" != "$minio_secret_secret" \
  && "$minio_access_secret" != "$minio_secret_secret" ]] \
  || fail "secrets are not independent"

network_identity="$(
  docker network inspect "$network" \
    --format '{{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}'
)" \
  || fail "network inspection failed"
[[ "$network_identity" == "$network_id|$network|overlay|swarm" ]] \
  || fail "network identity changed"

validate_existing_job() {
  local spec_json
  spec_json="$(docker service inspect "$JOB_NAME" --format '{{json .Spec}}')" \
    || return 1
  SPEC_JSON="$spec_json" \
    EXPECTED_GENERATION="$generation" \
    EXPECTED_IMAGE="$image" \
    EXPECTED_NETWORK_ID="$network_id" \
    EXPECTED_DATABASE_SECRET="$database_secret" \
    EXPECTED_MINIO_ACCESS_SECRET="$minio_access_secret" \
    EXPECTED_MINIO_SECRET_SECRET="$minio_secret_secret" \
    EXPECTED_EVIDENCE_VOLUME="$evidence_volume" \
    EXPECTED_MANAGER_NODE="$manager_node" \
    python3 - <<'PY'
import json
import os
import sys

try:
    spec = json.loads(os.environ["SPEC_JSON"])
    task = spec["TaskTemplate"]
    container = task["ContainerSpec"]
    labels = spec["Labels"]
    secret_entries = container.get("Secrets", [])
    secrets = {
        (entry["SecretName"], entry["File"]["Name"], entry["File"]["Mode"])
        for entry in secret_entries
    }
    expected_secrets = {
        (
            os.environ["EXPECTED_DATABASE_SECRET"],
            "vp-staging-janitor-database-url",
            0o400,
        ),
        (
            os.environ["EXPECTED_MINIO_ACCESS_SECRET"],
            "vp-staging-janitor-minio-access-key",
            0o400,
        ),
        (
            os.environ["EXPECTED_MINIO_SECRET_SECRET"],
            "vp-staging-janitor-minio-secret-key",
            0o400,
        ),
    }
    expected_env = {
        "DEPLOY_MODE=production",
        "VP_STAGING_JANITOR_RUNNER_ID=ccttww-lap",
        "VP_STAGING_JANITOR_DATABASE_URL_FILE=/run/secrets/"
        "vp-staging-janitor-database-url",
        "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE=/run/secrets/"
        "vp-staging-janitor-minio-access-key",
        "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE=/run/secrets/"
        "vp-staging-janitor-minio-secret-key",
        "VP_STAGING_JANITOR_STATUS_FILE=/run/videoprocess/"
        "staging-janitor/status.json",
        "STORAGE_BACKEND=minio",
        "MINIO_ENDPOINT=10.0.0.150:9000",
        "MINIO_BUCKET=videoprocess",
    }
    valid = (
        labels
        == {
            "vp.videoprocess.job": "staging-object-janitor",
            "vp.videoprocess.generation": os.environ[
                "EXPECTED_GENERATION"
            ],
        }
        and container["Image"] == os.environ["EXPECTED_IMAGE"]
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "MaxConcurrent"
        )
        == 1
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "TotalCompletions"
        )
        == 1
        and task.get("RestartPolicy", {}).get("Condition") == "none"
        and task.get("Placement", {}).get("Constraints")
        == ["node.hostname==" + os.environ["EXPECTED_MANAGER_NODE"]]
        and [entry["Target"] for entry in task.get("Networks", [])]
        == [os.environ["EXPECTED_NETWORK_ID"]]
        and len(secret_entries) == len(expected_secrets)
        and secrets == expected_secrets
        and len(container.get("Env", [])) == len(expected_env)
        and set(container.get("Env", [])) == expected_env
        and container.get("Configs", []) == []
        and container.get("Args")
        == ["python", "-m", "app.channel_agent.staging_object_janitor_cli"]
        and container.get("Mounts")
        == [
            {
                "Type": "volume",
                "Source": os.environ["EXPECTED_EVIDENCE_VOLUME"],
                "Target": "/run/videoprocess/staging-janitor",
            }
        ]
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
sys.exit(0 if valid else 1)
PY
}

if docker service inspect "$JOB_NAME" >/dev/null 2>&1; then
  validate_existing_job || fail "fixed-name service identity mismatch"
  task_state="$(
    docker service ps "$JOB_NAME" \
      --no-trunc \
      --format '{{.DesiredState}}|{{.CurrentState}}'
  )" || fail "task inspection failed"
  [[ -n "$task_state" && "$task_state" != *$'\n'* ]] \
    || fail "task set is invalid"
  case "$task_state" in
    Running\|New*|Running\|Pending*|Running\|Assigned*|Running\|Accepted*|Running\|Preparing*|Running\|Ready*|Running\|Starting*|Running\|Running*)
      [[ "$ACTION" == run ]] \
        || fail "running job cannot be retired"
      printf 'staging janitor job is already running\n'
      exit 0
      ;;
    Shutdown\|Complete*|Shutdown\|Failed*|Shutdown\|Rejected*|Shutdown\|Shutdown*)
      docker service rm "$JOB_NAME" >/dev/null \
        || fail "terminal job removal failed"
      for ((attempt = 1; attempt <= 20; attempt++)); do
        if ! docker service inspect "$JOB_NAME" >/dev/null 2>&1; then
          break
        fi
        sleep 1
      done
      if docker service inspect "$JOB_NAME" >/dev/null 2>&1; then
        fail "terminal job removal did not converge"
      fi
      ;;
    *)
      fail "task is neither running nor terminal"
      ;;
  esac
fi

[[ "$ACTION" == run ]] || exit 0

docker service create \
  --detach=true \
  --name "$JOB_NAME" \
  --label vp.videoprocess.job=staging-object-janitor \
  --label "vp.videoprocess.generation=$generation" \
  --mode replicated-job \
  --replicas 1 \
  --max-concurrent 1 \
  --restart-condition none \
  --constraint "node.hostname==$manager_node" \
  --network "$network_id" \
  --secret "source=$database_secret,target=vp-staging-janitor-database-url,mode=0400" \
  --secret "source=$minio_access_secret,target=vp-staging-janitor-minio-access-key,mode=0400" \
  --secret "source=$minio_secret_secret,target=vp-staging-janitor-minio-secret-key,mode=0400" \
  --mount "type=volume,src=$evidence_volume,dst=/run/videoprocess/staging-janitor" \
  --env DEPLOY_MODE=production \
  --env VP_STAGING_JANITOR_RUNNER_ID=ccttww-lap \
  --env VP_STAGING_JANITOR_DATABASE_URL_FILE=/run/secrets/vp-staging-janitor-database-url \
  --env VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-staging-janitor-minio-access-key \
  --env VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE=/run/secrets/vp-staging-janitor-minio-secret-key \
  --env VP_STAGING_JANITOR_STATUS_FILE=/run/videoprocess/staging-janitor/status.json \
  --env STORAGE_BACKEND=minio \
  --env MINIO_ENDPOINT=10.0.0.150:9000 \
  --env MINIO_BUCKET=videoprocess \
  "$image" \
  python -m app.channel_agent.staging_object_janitor_cli >/dev/null
