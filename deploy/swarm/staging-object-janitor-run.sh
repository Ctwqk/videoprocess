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

validate_holder() {
  local reference="$1"
  local holder_json
  holder_json="$(docker container inspect "$reference" --format '{{json .}}')" \
    || return 1
  HOLDER_JSON="$holder_json" \
    EXPECTED_HOLDER_NAME="$holder_name" \
    EXPECTED_GENERATION="$generation" \
    EXPECTED_IMAGE="$image" \
    EXPECTED_IMAGE_ID="$image_id" \
    EXPECTED_EVIDENCE_VOLUME="$evidence_volume" \
    EXPECTED_VOLUME_IDENTITY="$volume_identity" \
    python3 - <<'PY'
import json
import os
import re
import sys

try:
    holder = json.loads(os.environ["HOLDER_JSON"])
    config = holder["Config"]
    host = holder["HostConfig"]
    state = holder["State"]
    holder_id = holder["Id"]
    labels = config.get("Labels") or {}
    managed_labels = {
        key: value
        for key, value in labels.items()
        if key.startswith("vp.videoprocess.")
    }
    expected_labels = {
        "vp.videoprocess.holder":
            "staging-object-janitor-evidence",
        "vp.videoprocess.generation":
            os.environ["EXPECTED_GENERATION"],
        "vp.videoprocess.transaction":
            "staging-object-janitor:"
            + os.environ["EXPECTED_GENERATION"],
        "vp.videoprocess.volume":
            os.environ["EXPECTED_EVIDENCE_VOLUME"],
        "vp.videoprocess.volume-identity":
            os.environ["EXPECTED_VOLUME_IDENTITY"],
        "vp.videoprocess.image-id":
            os.environ["EXPECTED_IMAGE_ID"],
    }
    mounts = holder.get("Mounts", [])
    mount = mounts[0] if len(mounts) == 1 else {}
    restart = host.get("RestartPolicy") or {}
    tmpfs = host.get("Tmpfs") or {}
    tmp_options = set(filter(None, tmpfs.get("/tmp", "").split(",")))
    cap_add = host.get("CapAdd") or []
    cap_drop = host.get("CapDrop") or []
    security_opt = set(host.get("SecurityOpt") or [])
    status = state["Status"]
    exit_code = state.get("ExitCode", 0)
    valid = (
        re.fullmatch(r"[0-9a-f]{64}", holder_id) is not None
        and holder.get("Name") == "/" + os.environ["EXPECTED_HOLDER_NAME"]
        and holder.get("Image") == os.environ["EXPECTED_IMAGE_ID"]
        and config.get("Image") == os.environ["EXPECTED_IMAGE"]
        and config.get("User") == "0:0"
        and config.get("Entrypoint") in (None, [])
        and config.get("Cmd")
        == [
            "/opt/venv/bin/python",
            "-I",
            "-m",
            "app.channel_agent.staging_object_janitor_cli",
            "prepare-evidence",
        ]
        and managed_labels == expected_labels
        and host.get("AutoRemove") is False
        and host.get("ReadonlyRootfs") is True
        and host.get("NetworkMode") == "none"
        and host.get("Privileged", False) is False
        and host.get("Binds") in (None, [])
        and restart.get("Name") == "no"
        and restart.get("MaximumRetryCount", 0) == 0
        and cap_drop == ["ALL"]
        and len(cap_add) == 3
        and set(cap_add)
        == {"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER"}
        and security_opt
        in (
            {"no-new-privileges"},
            {"no-new-privileges:true"},
        )
        and set(tmpfs) == {"/tmp"}
        and tmp_options
        == {"rw", "nosuid", "nodev", "noexec", "mode=1777"}
        and mount.get("Type") == "volume"
        and mount.get("Name") == os.environ["EXPECTED_EVIDENCE_VOLUME"]
        and mount.get("Destination")
        == "/run/videoprocess/staging-janitor"
        and mount.get("Driver") == "local"
        and mount.get("RW") is True
        and isinstance(mount.get("Source"), str)
        and mount["Source"].startswith("/")
        and status in {"created", "running", "exited"}
        and isinstance(exit_code, int)
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
if not valid:
    sys.exit(1)
print(f"{holder_id}|{status}|{exit_code}")
PY
}

if [[ "${VP_STAGING_JANITOR_LIBRARY_ONLY:-0}" == 1 ]]; then
  [[ "${BASH_SOURCE[0]}" != "$0" ]] \
    || fail "library-only mode must be sourced"
  return 0
fi

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
[[ "$evidence_volume" == vp-staging-janitor-evidence ]] \
  || fail "evidence volume is invalid"
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
  local reference="$1"
  local service_json
  service_json="$(docker service inspect "$reference" --format '{{json .}}')" \
    || return 1
  SERVICE_JSON="$service_json" \
    EXPECTED_JOB_NAME="$JOB_NAME" \
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
import re
import sys

try:
    service = json.loads(os.environ["SERVICE_JSON"])
    service_id = service["ID"]
    spec = service["Spec"]
    task = spec["TaskTemplate"]
    container = task["ContainerSpec"]
    labels = spec["Labels"]
    secret_entries = container.get("Secrets", [])
    secrets = {
        (
            entry["SecretName"],
            entry["File"]["Name"],
            entry["File"]["UID"],
            entry["File"]["GID"],
            entry["File"]["Mode"],
        )
        for entry in secret_entries
    }
    expected_secrets = {
        (
            os.environ["EXPECTED_DATABASE_SECRET"],
            "vp-staging-janitor-database-url",
            "10001",
            "10001",
            0o400,
        ),
        (
            os.environ["EXPECTED_MINIO_ACCESS_SECRET"],
            "vp-staging-janitor-minio-access-key",
            "10001",
            "10001",
            0o400,
        ),
        (
            os.environ["EXPECTED_MINIO_SECRET_SECRET"],
            "vp-staging-janitor-minio-secret-key",
            "10001",
            "10001",
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
        re.fullmatch(r"[a-z0-9]{20,64}", service_id) is not None
        and spec.get("Name") == os.environ["EXPECTED_JOB_NAME"]
        and labels
        == {
            "vp.videoprocess.job": "staging-object-janitor",
            "vp.videoprocess.generation": os.environ[
                "EXPECTED_GENERATION"
            ],
        }
        and container["Image"] == os.environ["EXPECTED_IMAGE"]
        and container.get("User") == "10001:10001"
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
if valid:
    print(service_id)
else:
    sys.exit(1)
PY
}

service_id=""
service_running=0
holder_name="vp-staging-janitor-evidence-holder-$generation"
if docker service inspect "$JOB_NAME" >/dev/null 2>&1; then
  service_id="$(validate_existing_job "$JOB_NAME")" \
    || fail "fixed-name service identity mismatch"
  task_state="$(
    docker service ps "$service_id" \
      --no-trunc \
      --format '{{.DesiredState}}|{{.CurrentState}}'
  )" || fail "task inspection failed"
  [[ -n "$task_state" && "$task_state" != *$'\n'* ]] \
    || fail "task set is invalid"
  case "$task_state" in
    Running\|New*|Running\|Pending*|Running\|Assigned*|Running\|Accepted*|Running\|Preparing*|Running\|Ready*|Running\|Starting*|Running\|Running*)
      [[ "$ACTION" == run ]] \
        || fail "running job cannot be retired"
      if ! docker container inspect "$holder_name" >/dev/null 2>&1; then
        printf 'staging janitor job is already running\n'
        exit 0
      fi
      service_running=1
      ;;
    Shutdown\|Complete*|Shutdown\|Failed*|Shutdown\|Rejected*|Shutdown\|Shutdown*)
      final_service_id="$(
        validate_existing_job "$JOB_NAME"
      )" || fail "terminal job final identity mismatch"
      [[ "$final_service_id" == "$service_id" ]] \
        || fail "fixed-name service was replaced"
      docker service rm "$service_id" >/dev/null \
        || fail "terminal job removal failed"
      for ((attempt = 1; attempt <= 20; attempt++)); do
        if ! docker service inspect "$service_id" >/dev/null 2>&1; then
          break
        fi
        sleep 1
      done
      if docker service inspect "$service_id" >/dev/null 2>&1; then
        fail "terminal job removal did not converge"
      fi
      service_id=""
      ;;
    *)
      fail "task is neither running nor terminal"
      ;;
  esac
fi

[[ "$ACTION" == run ]] || exit 0

inspect_evidence_volume_identity() {
  local created="$1"
  local volume_json
  volume_json="$(
    docker volume inspect "$evidence_volume" --format '{{json .}}'
  )" || return 1
  EVIDENCE_VOLUME_JSON="$volume_json" \
    EXPECTED_EVIDENCE_VOLUME="$evidence_volume" \
    EVIDENCE_VOLUME_CREATED="$created" \
    python3 - <<'PY'
import hashlib
import json
import os
import sys

try:
    volume = json.loads(os.environ["EVIDENCE_VOLUME_JSON"])
    labels = volume.get("Labels")
    expected_labels = {
        "vp.videoprocess.volume": "staging-object-janitor-evidence"
    }
    created = os.environ["EVIDENCE_VOLUME_CREATED"] == "1"
    identity = {
        "CreatedAt": volume["CreatedAt"],
        "Driver": volume["Driver"],
        "Labels": labels,
        "Mountpoint": volume["Mountpoint"],
        "Name": volume["Name"],
        "Options": volume.get("Options"),
        "Scope": volume["Scope"],
    }
    valid = (
        identity["Name"] == os.environ["EXPECTED_EVIDENCE_VOLUME"]
        and identity["Driver"] == "local"
        and identity["Scope"] == "local"
        and identity["Options"] in (None, {})
        and isinstance(identity["CreatedAt"], str)
        and bool(identity["CreatedAt"])
        and isinstance(identity["Mountpoint"], str)
        and identity["Mountpoint"].startswith("/")
        and (
            labels == expected_labels
            or (not created and labels in (None, {}))
        )
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
if not valid:
    sys.exit(1)
encoded = json.dumps(
    identity,
    sort_keys=True,
    separators=(",", ":"),
).encode()
print(hashlib.sha256(encoded).hexdigest())
PY
}

volume_created=0
if ! docker volume inspect "$evidence_volume" >/dev/null 2>&1; then
  docker volume create \
    --driver local \
    --label vp.videoprocess.volume=staging-object-janitor-evidence \
    "$evidence_volume" >/dev/null \
    || fail "evidence volume creation failed"
  volume_created=1
fi
volume_identity="$(
  inspect_evidence_volume_identity "$volume_created"
)" || fail "evidence volume identity is invalid"
[[ "$volume_identity" =~ ^[0-9a-f]{64}$ ]] \
  || fail "evidence volume identity digest is invalid"

image_id="$(docker image inspect "$image" --format '{{.Id}}')" \
  || fail "holder image inspection failed"
[[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || fail "holder image identity is invalid"

holder_details=""
if docker container inspect "$holder_name" >/dev/null 2>&1; then
  holder_details="$(validate_holder "$holder_name")" \
    || fail "stale evidence holder identity mismatch"
else
  holder_id="$(
    docker container create \
      --name "$holder_name" \
      --label vp.videoprocess.holder=staging-object-janitor-evidence \
      --label "vp.videoprocess.generation=$generation" \
      --label "vp.videoprocess.transaction=staging-object-janitor:$generation" \
      --label "vp.videoprocess.volume=$evidence_volume" \
      --label "vp.videoprocess.volume-identity=$volume_identity" \
      --label "vp.videoprocess.image-id=$image_id" \
      --user 0:0 \
      --read-only \
      --network none \
      --restart no \
      --cap-drop ALL \
      --cap-add CHOWN \
      --cap-add DAC_OVERRIDE \
      --cap-add FOWNER \
      --security-opt no-new-privileges \
      --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777 \
      --mount "type=volume,src=$evidence_volume,dst=/run/videoprocess/staging-janitor" \
      --entrypoint "" \
      "$image" \
      /opt/venv/bin/python -I -m \
      app.channel_agent.staging_object_janitor_cli prepare-evidence
  )" || fail "evidence holder creation failed"
  [[ "$holder_id" =~ ^[0-9a-f]{64}$ ]] \
    || fail "created evidence holder identity is invalid"
  holder_details="$(validate_holder "$holder_id")" \
    || fail "created evidence holder descriptor is invalid"
fi

IFS='|' read -r holder_id holder_status holder_exit <<<"$holder_details"
[[ "$holder_id" =~ ^[0-9a-f]{64}$ \
  && -n "$holder_status" \
  && "$holder_exit" =~ ^[0-9]+$ ]] \
  || fail "evidence holder state is invalid"
pinned_volume_identity="$(
  inspect_evidence_volume_identity 0
)" || fail "pinned evidence volume identity is invalid"
[[ "$pinned_volume_identity" == "$volume_identity" ]] \
  || fail "evidence volume changed before holder pin"

case "$holder_status" in
  created)
    docker container start --attach "$holder_id" >/dev/null \
      || fail "evidence volume preparation failed"
    ;;
  running)
    holder_wait_status="$(docker container wait "$holder_id")" \
      || fail "running evidence holder wait failed"
    [[ "$holder_wait_status" == 0 ]] \
      || fail "running evidence holder preparation failed"
    ;;
  exited)
    if [[ "$holder_exit" != 0 ]]; then
      docker container start --attach "$holder_id" >/dev/null \
        || fail "evidence volume preparation retry failed"
    fi
    ;;
  *)
    fail "evidence holder state is not reusable"
    ;;
esac
holder_details="$(validate_holder "$holder_id")" \
  || fail "prepared evidence holder descriptor changed"
IFS='|' read -r prepared_holder_id holder_status holder_exit \
  <<<"$holder_details"
[[ "$prepared_holder_id" == "$holder_id" \
  && "$holder_status" == exited \
  && "$holder_exit" == 0 ]] \
  || fail "evidence holder did not complete preparation"

acquire_service_task_volume() {
  local expected_service_id="$1"
  local task_rows task_id desired_state current_state
  local task_containers task_container_id task_json
  for ((attempt = 1; attempt <= 20; attempt++)); do
    task_rows="$(
      docker service ps "$expected_service_id" \
        --no-trunc \
        --format '{{.ID}}|{{.DesiredState}}|{{.CurrentState}}'
    )" || return 1
    if [[ -n "$task_rows" && "$task_rows" != *$'\n'* ]]; then
      IFS='|' read -r task_id desired_state current_state \
        <<<"$task_rows"
      if [[ "$task_id" =~ ^[a-z0-9]{20,64}$ \
        && ( "$desired_state" == Running \
          || "$desired_state" == Shutdown ) \
        && -n "$current_state" ]]; then
        task_containers="$(
          docker container ls \
            --all \
            --no-trunc \
            --filter "label=com.docker.swarm.service.id=$expected_service_id" \
            --filter "label=com.docker.swarm.task.id=$task_id" \
            --format '{{.ID}}'
        )" || return 1
        if [[ "$task_containers" =~ ^[0-9a-f]{64}$ ]]; then
          task_container_id="$task_containers"
          task_json="$(
            docker container inspect "$task_container_id" \
              --format '{{json .}}'
          )" || return 1
          TASK_CONTAINER_JSON="$task_json" \
            EXPECTED_TASK_CONTAINER_ID="$task_container_id" \
            EXPECTED_SERVICE_ID="$expected_service_id" \
            EXPECTED_TASK_ID="$task_id" \
            EXPECTED_JOB_NAME="$JOB_NAME" \
            EXPECTED_EVIDENCE_VOLUME="$evidence_volume" \
            python3 - <<'PY' \
            && return 0
import json
import os
import sys

try:
    container = json.loads(os.environ["TASK_CONTAINER_JSON"])
    labels = container["Config"].get("Labels") or {}
    mounts = container.get("Mounts", [])
    evidence_mounts = [
        mount
        for mount in mounts
        if mount.get("Destination")
        == "/run/videoprocess/staging-janitor"
    ]
    mount = evidence_mounts[0] if len(evidence_mounts) == 1 else {}
    valid = (
        container.get("Id")
        == os.environ["EXPECTED_TASK_CONTAINER_ID"]
        and labels.get("com.docker.swarm.service.id")
        == os.environ["EXPECTED_SERVICE_ID"]
        and labels.get("com.docker.swarm.task.id")
        == os.environ["EXPECTED_TASK_ID"]
        and labels.get("com.docker.swarm.service.name")
        == os.environ["EXPECTED_JOB_NAME"]
        and mount.get("Type") == "volume"
        and mount.get("Name") == os.environ["EXPECTED_EVIDENCE_VOLUME"]
        and mount.get("Destination")
        == "/run/videoprocess/staging-janitor"
        and mount.get("Driver") == "local"
        and mount.get("RW") is True
        and isinstance(mount.get("Source"), str)
        and mount["Source"].startswith("/")
        and container.get("State", {}).get("Status")
        in {"created", "running", "exited"}
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
sys.exit(0 if valid else 1)
PY
        fi
      fi
    fi
    sleep 1
  done
  return 1
}

if [[ "$service_running" == 0 ]]; then
  service_id="$(
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
  --user 10001:10001 \
  --secret "source=$database_secret,target=vp-staging-janitor-database-url,uid=10001,gid=10001,mode=0400" \
  --secret "source=$minio_access_secret,target=vp-staging-janitor-minio-access-key,uid=10001,gid=10001,mode=0400" \
  --secret "source=$minio_secret_secret,target=vp-staging-janitor-minio-secret-key,uid=10001,gid=10001,mode=0400" \
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
      python -m app.channel_agent.staging_object_janitor_cli
  )" || fail "staging janitor service creation failed"
  [[ "$service_id" =~ ^[a-z0-9]{20,64}$ ]] \
    || fail "created staging janitor service identity is invalid"
fi

validated_service_id="$(validate_existing_job "$service_id")" \
  || fail "created staging janitor service descriptor is invalid"
[[ "$validated_service_id" == "$service_id" ]] \
  || fail "staging janitor service identity changed"
acquire_service_task_volume "$service_id" \
  || fail "staging janitor task did not acquire evidence volume"
validated_service_id="$(validate_existing_job "$service_id")" \
  || fail "staging janitor service changed before holder release"
[[ "$validated_service_id" == "$service_id" ]] \
  || fail "staging janitor service identity changed before holder release"
holder_details="$(validate_holder "$holder_id")" \
  || fail "evidence holder changed before release"
IFS='|' read -r release_holder_id holder_status holder_exit \
  <<<"$holder_details"
[[ "$release_holder_id" == "$holder_id" \
  && "$holder_status" == exited \
  && "$holder_exit" == 0 ]] \
  || fail "evidence holder is not releasable"
release_volume_identity="$(
  inspect_evidence_volume_identity 0
)" || fail "evidence volume changed before holder release"
[[ "$release_volume_identity" == "$volume_identity" ]] \
  || fail "evidence volume identity changed before holder release"
docker container rm "$holder_id" >/dev/null \
  || fail "evidence holder release failed"

if [[ "$service_running" == 1 ]]; then
  printf 'staging janitor job is already running\n'
fi
