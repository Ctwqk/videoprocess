#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$ROOT_DIR/deploy/swarm/staging-object-janitor-run.sh"
TEST_ROOT="$(mktemp -d)"
REAL_DOCKER="$(command -v docker)"
REAL_HOLDER=""
REAL_VOLUME=""

cleanup() {
  local status=$?
  if [[ -n "$REAL_HOLDER" ]]; then
    "$REAL_DOCKER" container rm -f "$REAL_HOLDER" >/dev/null 2>&1 || true
  fi
  if [[ -n "$REAL_VOLUME" ]]; then
    "$REAL_DOCKER" volume rm "$REAL_VOLUME" >/dev/null 2>&1 || true
  fi
  rm -rf "$TEST_ROOT"
  exit "$status"
}
trap cleanup EXIT

FAKE_BIN="$TEST_ROOT/bin"
CALLS="$TEST_ROOT/calls"
SERVICE_STATE="$TEST_ROOT/service-state"
TASK_STATE_FILE="$TEST_ROOT/task-state"
SPEC_FILE="$TEST_ROOT/spec.json"
VOLUME_STATE="$TEST_ROOT/volume-state"
VOLUME_JSON_FILE="$TEST_ROOT/volume.json"
HOLDER_STATE="$TEST_ROOT/holder-state"
HOLDER_ID="$(printf 'a%.0s' {1..64})"
SERVICE_ID="s1234567890abcdefghijklmn"
TASK_ID="t1234567890abcdefghijklmn"
TASK_CONTAINER_ID="$(printf 'b%.0s' {1..64})"
IMAGE_ID="sha256:$(printf 'c%.0s' {1..64})"
mkdir -p "$FAKE_BIN"
export \
  CALLS \
  SERVICE_STATE \
  TASK_STATE_FILE \
  SPEC_FILE \
  VOLUME_STATE \
  VOLUME_JSON_FILE \
  HOLDER_STATE \
  HOLDER_ID \
  SERVICE_ID \
  TASK_ID \
  TASK_CONTAINER_ID \
  IMAGE_ID

cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker|%s\n' "$*" >>"$CALLS"

if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
  printf 'vp-pipeline-network-id|vp-pipeline-net|overlay|swarm\n'
  exit 0
fi
if [[ "${1:-} ${2:-}" == "image inspect" ]]; then
  printf '%s\n' "$IMAGE_ID"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "volume inspect" ]]; then
  [[ -f "$VOLUME_STATE" ]] || exit 1
  if [[ "${FAIL_VOLUME_IDENTITY:-0}" == 1 ]]; then
    sed \
      's/"Name": "vp-staging-janitor-evidence"/"Name": "unexpected-evidence-volume"/' \
      "$VOLUME_JSON_FILE"
  else
    cat "$VOLUME_JSON_FILE"
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "volume create" ]]; then
  : >"$VOLUME_STATE"
  printf '%s\n' "${*: -1}"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container inspect" ]]; then
  reference="${3:-}"
  if [[ "$reference" == "$TASK_CONTAINER_ID" ]]; then
    [[ -f "$SERVICE_STATE" ]] || exit 1
    python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "Id": os.environ["TASK_CONTAINER_ID"],
            "Config": {
                "Labels": {
                    "com.docker.swarm.service.id": os.environ["SERVICE_ID"],
                    "com.docker.swarm.task.id": os.environ["TASK_ID"],
                    "com.docker.swarm.service.name": (
                        "vp-staging-object-janitor"
                    ),
                }
            },
            "Mounts": [
                {
                    "Type": "volume",
                    "Name": "vp-staging-janitor-evidence",
                    "Source": (
                        "/var/lib/docker/volumes/"
                        "vp-staging-janitor-evidence/_data"
                    ),
                    "Destination": (
                        "/run/videoprocess/staging-janitor"
                    ),
                    "Driver": "local",
                    "Mode": "z",
                    "RW": True,
                }
            ],
            "State": {"Status": "running"},
        },
        separators=(",", ":"),
    )
)
PY
    exit 0
  fi
  [[ "$reference" == "$HOLDER_ID" \
    || "$reference" == "$EXPECTED_HOLDER_NAME" ]] || exit 1
  [[ -f "$HOLDER_STATE" ]] || exit 1
  python3 - <<'PY'
import json
import os

status, raw_exit = open(
    os.environ["HOLDER_STATE"],
    encoding="utf-8",
).read().strip().split("|", 1)
generation = os.environ["EXPECTED_GENERATION"]
if os.environ.get("HOLDER_LABEL_MISMATCH") == "1":
    generation = "c-ffffffffffffffffffff"
labels = {
    "vp.videoprocess.holder": "staging-object-janitor-evidence",
    "vp.videoprocess.generation": generation,
    "vp.videoprocess.transaction": (
        "staging-object-janitor:" + generation
    ),
    "vp.videoprocess.volume": "vp-staging-janitor-evidence",
    "vp.videoprocess.volume-identity": os.environ["VOLUME_IDENTITY"],
    "vp.videoprocess.image-id": os.environ["IMAGE_ID"],
}
print(
    json.dumps(
        {
            "Id": os.environ["HOLDER_ID"],
            "Name": "/" + os.environ["EXPECTED_HOLDER_NAME"],
            "Image": os.environ["IMAGE_ID"],
            "Config": {
                "Image": os.environ["EXPECTED_IMAGE"],
                "User": "0:0",
                "Labels": {
                    "org.opencontainers.image.revision": "test",
                    **labels,
                },
                "Cmd": [
                    "/opt/venv/bin/python",
                    "-I",
                    "-m",
                    "app.channel_agent.staging_object_janitor_cli",
                    "prepare-evidence",
                ],
            },
            "HostConfig": {
                "AutoRemove": False,
                "ReadonlyRootfs": True,
                "NetworkMode": "none",
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
                "CapDrop": ["ALL"],
                "CapAdd": [
                    "CAP_CHOWN",
                    "CAP_DAC_OVERRIDE",
                    "CAP_FOWNER",
                ],
                "SecurityOpt": ["no-new-privileges"],
                "Tmpfs": {
                    "/tmp": "rw,nosuid,nodev,noexec,mode=1777"
                },
            },
            "Mounts": [
                {
                    "Type": "volume",
                    "Name": "vp-staging-janitor-evidence",
                    "Source": (
                        "/var/lib/docker/volumes/"
                        "vp-staging-janitor-evidence/_data"
                    ),
                    "Destination": (
                        "/run/videoprocess/staging-janitor"
                    ),
                    "Driver": "local",
                    "Mode": "z",
                    "RW": True,
                }
            ],
            "State": {
                "Status": status,
                "Running": status == "running",
                "ExitCode": int(raw_exit),
            },
        },
        separators=(",", ":"),
    )
)
PY
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container create" ]]; then
  [[ ! -f "$HOLDER_STATE" ]]
  previous=""
  entrypoint_cleared=0
  for argument in "$@"; do
    if [[ "$previous" == "--entrypoint" && -z "$argument" ]]; then
      entrypoint_cleared=1
    fi
    previous="$argument"
  done
  [[ "$entrypoint_cleared" == 1 ]]
  if [[ "${FAIL_HOLDER_CREATE:-0}" == 1 ]]; then
    exit 41
  fi
  printf 'created|0\n' >"$HOLDER_STATE"
  printf '%s\n' "$HOLDER_ID"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container start" ]]; then
  [[ "${3:-}" == "--attach" && "${4:-}" == "$HOLDER_ID" ]]
  if [[ "${FAIL_EVIDENCE_PREPARE:-0}" == 1 ]]; then
    printf 'exited|3\n' >"$HOLDER_STATE"
    exit 3
  fi
  printf 'exited|0\n' >"$HOLDER_STATE"
  printf '{"action":"prepare-evidence","status":"ok"}\n'
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container wait" ]]; then
  [[ "${3:-}" == "$HOLDER_ID" ]]
  printf 'exited|0\n' >"$HOLDER_STATE"
  printf '0\n'
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container rm" ]]; then
  [[ "${3:-}" == "$HOLDER_ID" ]]
  if [[ "${FAIL_HOLDER_RELEASE:-0}" == 1 ]]; then
    exit 42
  fi
  rm -f "$HOLDER_STATE"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "container ls" ]]; then
  if [[ -f "$SERVICE_STATE" \
    && "${FAIL_TASK_ACQUISITION:-0}" != 1 ]]; then
    printf '%s\n' "$TASK_CONTAINER_ID"
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
  [[ -f "$SERVICE_STATE" ]] || exit 1
  if [[ "$*" == *'{{json .}}'* ]]; then
    SERVICE_SPEC_JSON="$(<"$SPEC_FILE")" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "ID": os.environ["SERVICE_ID"],
            "Spec": json.loads(os.environ["SERVICE_SPEC_JSON"]),
        },
        separators=(",", ":"),
    )
)
PY
  elif [[ "$*" == *'{{json .Spec}}'* ]]; then
    cat "$SPEC_FILE"
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service ps" ]]; then
  if [[ "$*" == *'{{.ID}}|'* ]]; then
    printf '%s|' "$TASK_ID"
  fi
  cat "$TASK_STATE_FILE"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service rm" ]]; then
  [[ "${3:-}" == "$SERVICE_ID" ]]
  rm -f "$SERVICE_STATE"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service create" ]]; then
  if [[ "${FAIL_SERVICE_CREATE:-0}" == 1 ]]; then
    exit 43
  fi
  : >"$SERVICE_STATE"
  printf 'Running|Running 1 second ago\n' >"$TASK_STATE_FILE"
  printf '%s\n' "$SERVICE_ID"
  exit 0
fi
if [[ "${1:-}" == run ]]; then
  [[ "${FAIL_EVIDENCE_PREPARE:-0}" != 1 ]]
  exit
fi
exit 90
EOF
chmod +x "$FAKE_BIN/docker"
cat >"$FAKE_BIN/sleep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$FAKE_BIN/sleep"
PATH="$FAKE_BIN:$PATH"
export PATH

generation=c-0123456789abcdef0123
image=vp-ffmpeg-worker-python:deploy-0123456789ab
database_secret=vp-staging-db-test
minio_access_secret=vp-staging-minio-access-test
minio_secret_secret=vp-staging-minio-secret-test
evidence_volume=vp-staging-janitor-evidence
holder_name="vp-staging-janitor-evidence-holder-$generation"
config="$TEST_ROOT/janitor.conf"
cat >"$config" <<EOF
VERSION=2
GENERATION=$generation
IMAGE=$image
NETWORK=vp-pipeline-net
NETWORK_ID=vp-pipeline-network-id
DATABASE_SECRET=$database_secret
MINIO_ACCESS_SECRET=$minio_access_secret
MINIO_SECRET_SECRET=$minio_secret_secret
EVIDENCE_VOLUME=$evidence_volume
MANAGER_NODE=ccttww-lap
EOF
chmod 0600 "$config"
export VP_STAGING_JANITOR_CONFIG_FILE="$config"
cat >"$VOLUME_JSON_FILE" <<EOF
{
  "CreatedAt": "2026-07-29T12:00:00Z",
  "Driver": "local",
  "Labels": {
    "vp.videoprocess.volume": "staging-object-janitor-evidence"
  },
  "Mountpoint": "/var/lib/docker/volumes/$evidence_volume/_data",
  "Name": "$evidence_volume",
  "Options": null,
  "Scope": "local"
}
EOF
VOLUME_IDENTITY="$(
  python3 - "$VOLUME_JSON_FILE" <<'PY'
import hashlib
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    volume = json.load(handle)
identity = {
    "CreatedAt": volume["CreatedAt"],
    "Driver": volume["Driver"],
    "Labels": volume["Labels"],
    "Mountpoint": volume["Mountpoint"],
    "Name": volume["Name"],
    "Options": volume["Options"],
    "Scope": volume["Scope"],
}
encoded = json.dumps(
    identity,
    sort_keys=True,
    separators=(",", ":"),
).encode()
print(hashlib.sha256(encoded).hexdigest())
PY
)"
EXPECTED_HOLDER_NAME="$holder_name"
EXPECTED_GENERATION="$generation"
EXPECTED_IMAGE="$image"
export \
  VOLUME_IDENTITY \
  EXPECTED_HOLDER_NAME \
  EXPECTED_GENERATION \
  EXPECTED_IMAGE

cat >"$SPEC_FILE" <<EOF
{
  "Name": "vp-staging-object-janitor",
  "Labels": {
    "vp.videoprocess.job": "staging-object-janitor",
    "vp.videoprocess.generation": "$generation"
  },
  "Mode": {
    "ReplicatedJob": {
      "MaxConcurrent": 1,
      "TotalCompletions": 1
    }
  },
  "TaskTemplate": {
    "ContainerSpec": {
      "Image": "$image",
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
        {"SecretName": "$database_secret", "File": {"Name": "vp-staging-janitor-database-url", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$minio_access_secret", "File": {"Name": "vp-staging-janitor-minio-access-key", "UID": "10001", "GID": "10001", "Mode": 256}},
        {"SecretName": "$minio_secret_secret", "File": {"Name": "vp-staging-janitor-minio-secret-key", "UID": "10001", "GID": "10001", "Mode": 256}}
      ],
      "Mounts": [
        {"Type": "volume", "Source": "$evidence_volume", "Target": "/run/videoprocess/staging-janitor"}
      ]
    },
    "RestartPolicy": {"Condition": "none"},
    "Placement": {"Constraints": ["node.hostname==ccttww-lap"]},
    "Networks": [{"Target": "vp-pipeline-network-id"}]
  }
}
EOF
cp "$SPEC_FILE" "$TEST_ROOT/valid-spec.json"
printf 'Running|Running 1 second ago\n' >"$TASK_STATE_FILE"

bash "$LAUNCHER"
grep -Fq \
  'docker|volume create --driver local --label vp.videoprocess.volume=staging-object-janitor-evidence vp-staging-janitor-evidence' \
  "$CALLS"
if ! grep -Fq -- \
  "docker|container create --name $holder_name" \
  "$CALLS"; then
  echo 'FAIL: evidence preparation did not create a transaction-owned holder' >&2
  exit 1
fi
grep -Fq -- \
  "--label vp.videoprocess.holder=staging-object-janitor-evidence" \
  "$CALLS"
grep -Fq -- \
  "--label vp.videoprocess.generation=$generation" \
  "$CALLS"
grep -Fq -- \
  "--label vp.videoprocess.transaction=staging-object-janitor:$generation" \
  "$CALLS"
grep -Fq -- \
  "--label vp.videoprocess.volume-identity=$VOLUME_IDENTITY" \
  "$CALLS"
grep -Fq -- '--read-only --network none --restart no' "$CALLS"
grep -Fq -- '--cap-drop ALL --cap-add CHOWN --cap-add DAC_OVERRIDE --cap-add FOWNER' \
  "$CALLS"
grep -Fq \
  "docker|container start --attach $HOLDER_ID" \
  "$CALLS"
grep -Fq 'docker|service create' "$CALLS"
grep -Fq \
  "docker|service inspect $SERVICE_ID --format {{json .}}" \
  "$CALLS"
grep -Fq \
  "docker|service ps $SERVICE_ID --no-trunc --format {{.ID}}|{{.DesiredState}}|{{.CurrentState}}" \
  "$CALLS"
grep -Fq \
  "docker|container ls --all --no-trunc --filter label=com.docker.swarm.service.id=$SERVICE_ID --filter label=com.docker.swarm.task.id=$TASK_ID --format {{.ID}}" \
  "$CALLS"
grep -Fq \
  "docker|container inspect $TASK_CONTAINER_ID --format {{json .}}" \
  "$CALLS"
grep -Fq \
  "docker|container rm $HOLDER_ID" \
  "$CALLS"
if grep -Fq 'docker|run --rm' "$CALLS"; then
  echo 'FAIL: evidence preparation escaped the transaction holder' >&2
  exit 1
fi
holder_create_line="$(
  grep -nF "docker|container create --name $holder_name" "$CALLS" \
    | head -1 | cut -d: -f1
)"
holder_start_line="$(
  grep -nF "docker|container start --attach $HOLDER_ID" "$CALLS" \
    | head -1 | cut -d: -f1
)"
service_create_line="$(
  grep -nF 'docker|service create' "$CALLS" | head -1 | cut -d: -f1
)"
service_inspect_line="$(
  grep -nF "docker|service inspect $SERVICE_ID --format {{json .}}" \
    "$CALLS" | head -1 | cut -d: -f1
)"
task_inspect_line="$(
  grep -nF "docker|container inspect $TASK_CONTAINER_ID --format {{json .}}" \
    "$CALLS" | head -1 | cut -d: -f1
)"
holder_release_line="$(
  grep -nF "docker|container rm $HOLDER_ID" "$CALLS" \
    | head -1 | cut -d: -f1
)"
if [[ "$holder_create_line" -ge "$holder_start_line" \
  || "$holder_start_line" -ge "$service_create_line" \
  || "$service_create_line" -ge "$service_inspect_line" \
  || "$service_inspect_line" -ge "$task_inspect_line" \
  || "$task_inspect_line" -ge "$holder_release_line" ]]; then
  echo 'FAIL: evidence holder did not span prepare through task acquisition' >&2
  exit 1
fi
grep -Fq -- '--user 10001:10001' "$CALLS"
grep -Fq -- '--mode replicated-job --replicas 1 --max-concurrent 1' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--restart-condition none' "$CALLS"
grep -Fq -- '--network vp-pipeline-network-id' "$CALLS"
grep -Fq -- \
  "--secret source=$database_secret,target=vp-staging-janitor-database-url,uid=10001,gid=10001,mode=0400" \
  "$CALLS"
grep -Fq -- \
  "--secret source=$minio_access_secret,target=vp-staging-janitor-minio-access-key,uid=10001,gid=10001,mode=0400" \
  "$CALLS"
grep -Fq -- \
  "--secret source=$minio_secret_secret,target=vp-staging-janitor-minio-secret-key,uid=10001,gid=10001,mode=0400" \
  "$CALLS"
if grep -Fiq '126' "$CALLS"; then
  echo 'FAIL: staging janitor referenced host 126' >&2
  exit 1
fi

printf 'Running|Running 3 seconds ago\n' >"$TASK_STATE_FILE"
python3 - "$TEST_ROOT/valid-spec.json" "$SPEC_FILE" <<'PY'
import json
import sys

source, target = sys.argv[1:]
with open(source, encoding="utf-8") as handle:
    spec = json.load(handle)
spec["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["File"]["UID"] = "0"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(spec, handle)
PY
if bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: root-owned staging janitor secret was accepted' >&2
  exit 1
fi
cp "$TEST_ROOT/valid-spec.json" "$SPEC_FILE"

: >"$CALLS"
python3 - "$TEST_ROOT/valid-spec.json" "$SPEC_FILE" <<'PY'
import json
import sys

source, target = sys.argv[1:]
with open(source, encoding="utf-8") as handle:
    spec = json.load(handle)
spec["TaskTemplate"]["ContainerSpec"]["User"] = "0:0"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(spec, handle)
PY
if bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: root staging janitor service was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service rm' "$CALLS"; then
  echo 'FAIL: root staging janitor service was removed' >&2
  exit 1
fi
cp "$TEST_ROOT/valid-spec.json" "$SPEC_FILE"

: >"$CALLS"
printf 'Running|Running 3 seconds ago\n' >"$TASK_STATE_FILE"
if bash "$LAUNCHER" retire >/dev/null 2>&1; then
  echo 'FAIL: running staging janitor was retired' >&2
  exit 1
fi
if grep -Fq 'docker|service rm' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: running staging janitor retirement mutated the service' >&2
  exit 1
fi

: >"$CALLS"
bash "$LAUNCHER"
if grep -Fq 'docker|service rm' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: running staging janitor was replaced' >&2
  exit 1
fi

: >"$CALLS"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
bash "$LAUNCHER"
grep -Fq "docker|service rm $SERVICE_ID" "$CALLS"
grep -Fq 'docker|service create' "$CALLS"
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

: >"$CALLS"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
bash "$LAUNCHER" retire
grep -Fq "docker|service rm $SERVICE_ID" "$CALLS"
if grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: terminal staging janitor retirement recreated the job' >&2
  exit 1
fi

: >"$SERVICE_STATE"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
sed 's/"Image": "[^"]*"/"Image": "unexpected:image"/' \
  "$TEST_ROOT/valid-spec.json" >"$TEST_ROOT/bad-spec.json"
mv "$TEST_ROOT/bad-spec.json" "$SPEC_FILE"
: >"$CALLS"
if bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: mismatched fixed-name staging janitor was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service rm' "$CALLS"; then
  echo 'FAIL: mismatched fixed-name staging janitor was removed' >&2
  exit 1
fi

: >"$SERVICE_STATE"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
sed 's/"TotalCompletions": 1/"TotalCompletions": 2/' \
  "$TEST_ROOT/valid-spec.json" >"$TEST_ROOT/wrong-completions.json"
mv "$TEST_ROOT/wrong-completions.json" "$SPEC_FILE"
: >"$CALLS"
if bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: multi-completion staging janitor was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service rm' "$CALLS"; then
  echo 'FAIL: multi-completion staging janitor was removed' >&2
  exit 1
fi

: >"$SERVICE_STATE"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
sed \
  's/"vp.videoprocess.job": "staging-object-janitor"/"vp.videoprocess.job": "staging-object-janitor", "unexpected": "label"/' \
  "$TEST_ROOT/valid-spec.json" >"$TEST_ROOT/extra-label.json"
mv "$TEST_ROOT/extra-label.json" "$SPEC_FILE"
: >"$CALLS"
if bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: staging janitor with an extra label was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service rm' "$CALLS"; then
  echo 'FAIL: staging janitor with an extra label was removed' >&2
  exit 1
fi

rm -f "$SERVICE_STATE"
cp "$TEST_ROOT/valid-spec.json" "$SPEC_FILE"
bad_config="$TEST_ROOT/bad-generation.conf"
sed 's/^GENERATION=.*/GENERATION=c-ffffffffffffffffffff/' \
  "$config" >"$bad_config"
chmod 0600 "$bad_config"
if VP_STAGING_JANITOR_CONFIG_FILE="$bad_config" \
  bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: generation/image commit mismatch was accepted' >&2
  exit 1
fi

rm -f "$SERVICE_STATE"
bad_host_config="$TEST_ROOT/bad-host.conf"
sed 's/^EVIDENCE_VOLUME=.*/EVIDENCE_VOLUME=caspers-mac-mini/' \
  "$config" >"$bad_host_config"
chmod 0600 "$bad_host_config"
: >"$CALLS"
if VP_STAGING_JANITOR_CONFIG_FILE="$bad_host_config" \
  bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: lowercase host-126 identity was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: forbidden host-126 configuration created a job' >&2
  exit 1
fi

other_volume_config="$TEST_ROOT/other-volume.conf"
sed 's/^EVIDENCE_VOLUME=.*/EVIDENCE_VOLUME=other-evidence-volume/' \
  "$config" >"$other_volume_config"
chmod 0600 "$other_volume_config"
: >"$CALLS"
if VP_STAGING_JANITOR_CONFIG_FILE="$other_volume_config" \
  bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: non-canonical evidence volume was accepted' >&2
  exit 1
fi
if grep -Eq 'docker\|(volume|run|service create)' "$CALLS"; then
  echo 'FAIL: non-canonical evidence volume caused Docker mutation' >&2
  exit 1
fi

rm -f "$SERVICE_STATE"
rm -f "$HOLDER_STATE"
cp "$TEST_ROOT/valid-spec.json" "$SPEC_FILE"
: >"$CALLS"
if FAIL_EVIDENCE_PREPARE=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: failed evidence preparation was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: failed evidence preparation created the janitor service' >&2
  exit 1
fi
if [[ ! -f "$HOLDER_STATE" || ! -f "$VOLUME_STATE" ]]; then
  echo 'FAIL: failed evidence preparation did not retain holder and evidence' >&2
  exit 1
fi
if grep -Eq 'docker\|(container rm|volume rm)' "$CALLS"; then
  echo 'FAIL: failed evidence preparation released its pin or evidence' >&2
  exit 1
fi

: >"$CALLS"
bash "$LAUNCHER"
if grep -Fq 'docker|container create' "$CALLS"; then
  echo 'FAIL: evidence preparation retry replaced the exact stale holder' >&2
  exit 1
fi
grep -Fq "docker|container start --attach $HOLDER_ID" "$CALLS"
grep -Fq 'docker|service create' "$CALLS"
grep -Fq "docker|container inspect $TASK_CONTAINER_ID --format {{json .}}" \
  "$CALLS"
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

rm -f "$SERVICE_STATE" "$HOLDER_STATE"
: >"$CALLS"
if FAIL_SERVICE_CREATE=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: failed service creation was accepted' >&2
  exit 1
fi
if [[ ! -f "$HOLDER_STATE" || ! -f "$VOLUME_STATE" ]]; then
  echo 'FAIL: failed service creation did not retain holder and evidence' >&2
  exit 1
fi
if grep -Eq 'docker\|(container rm|volume rm)' "$CALLS"; then
  echo 'FAIL: failed service creation released its pin or evidence' >&2
  exit 1
fi

: >"$CALLS"
bash "$LAUNCHER"
if grep -Fq 'docker|container create' "$CALLS"; then
  echo 'FAIL: service creation retry replaced the exact stale holder' >&2
  exit 1
fi
grep -Fq 'docker|service create' "$CALLS"
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

rm -f "$SERVICE_STATE" "$HOLDER_STATE"
: >"$CALLS"
if FAIL_TASK_ACQUISITION=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: missing service task volume acquisition was accepted' >&2
  exit 1
fi
if [[ ! -f "$SERVICE_STATE" || ! -f "$HOLDER_STATE" \
  || ! -f "$VOLUME_STATE" ]]; then
  echo 'FAIL: task acquisition failure did not retain service holder state' >&2
  exit 1
fi
if grep -Eq 'docker\|(container rm|volume rm)' "$CALLS"; then
  echo 'FAIL: task acquisition failure released its pin or evidence' >&2
  exit 1
fi

: >"$CALLS"
bash "$LAUNCHER"
if grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: task acquisition retry replaced the existing service' >&2
  exit 1
fi
grep -Fq "docker|service inspect $SERVICE_ID --format {{json .}}" "$CALLS"
grep -Fq "docker|container inspect $TASK_CONTAINER_ID --format {{json .}}" \
  "$CALLS"
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

rm -f "$SERVICE_STATE" "$HOLDER_STATE"
: >"$CALLS"
if FAIL_HOLDER_RELEASE=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: failed holder release was accepted' >&2
  exit 1
fi
if [[ ! -f "$SERVICE_STATE" || ! -f "$HOLDER_STATE" ]]; then
  echo 'FAIL: failed holder release discarded retry state' >&2
  exit 1
fi
: >"$CALLS"
bash "$LAUNCHER"
if grep -Eq 'docker\|(service create|container create)' "$CALLS"; then
  echo 'FAIL: holder release retry replaced acquired service state' >&2
  exit 1
fi
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

rm -f "$SERVICE_STATE"
printf 'created|0\n' >"$HOLDER_STATE"
: >"$CALLS"
if HOLDER_LABEL_MISMATCH=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: mismatched stale holder was accepted' >&2
  exit 1
fi
if [[ ! -f "$HOLDER_STATE" || ! -f "$VOLUME_STATE" ]]; then
  echo 'FAIL: mismatched stale holder or evidence was removed' >&2
  exit 1
fi
if grep -Eq 'docker\|(container rm|container create|service create|volume rm)' \
  "$CALLS"; then
  echo 'FAIL: mismatched stale holder caused Docker mutation' >&2
  exit 1
fi

rm -f "$SERVICE_STATE"
printf 'running|0\n' >"$HOLDER_STATE"
: >"$CALLS"
bash "$LAUNCHER"
if grep -Eq 'docker\|(container create|container start)' "$CALLS"; then
  echo 'FAIL: running stale holder was replaced or restarted' >&2
  exit 1
fi
grep -Fq "docker|container wait $HOLDER_ID" "$CALLS"
grep -Fq 'docker|service create' "$CALLS"
grep -Fq "docker|container rm $HOLDER_ID" "$CALLS"

rm -f "$SERVICE_STATE"
rm -f "$HOLDER_STATE"
: >"$CALLS"
if FAIL_VOLUME_IDENTITY=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: mismatched evidence volume identity was accepted' >&2
  exit 1
fi
if grep -Eq 'docker\|(run|container create|service create|volume rm)' "$CALLS"; then
  echo 'FAIL: mismatched evidence volume was prepared or launched' >&2
  exit 1
fi

if [[ -n "${VP_STAGING_JANITOR_REAL_DOCKER_IMAGE:-}" ]]; then
  suffix="round5-$$-$RANDOM"
  real_generation="c-$(
    printf '%s' "$suffix" | shasum -a 256 | cut -c1-20
  )"
  REAL_VOLUME="vp-staging-janitor-pin-$suffix"
  REAL_HOLDER="vp-staging-janitor-evidence-holder-$real_generation"
  test_label="vp.videoprocess.test-holder-pin=$suffix"
  "$REAL_DOCKER" volume create \
    --driver local \
    --label "$test_label" \
    "$REAL_VOLUME" >/dev/null
  identity_before="$(
    "$REAL_DOCKER" volume inspect "$REAL_VOLUME" --format '{{json .}}' \
      | python3 -c '
import hashlib
import json
import sys

volume = json.load(sys.stdin)
identity = {
    "CreatedAt": volume["CreatedAt"],
    "Driver": volume["Driver"],
    "Labels": volume.get("Labels"),
    "Mountpoint": volume["Mountpoint"],
    "Name": volume["Name"],
    "Options": volume.get("Options"),
    "Scope": volume["Scope"],
}
encoded = json.dumps(
    identity,
    sort_keys=True,
    separators=(",", ":"),
).encode()
print(hashlib.sha256(encoded).hexdigest())
'
  )"
  real_image_id="$(
    "$REAL_DOCKER" image inspect \
      "$VP_STAGING_JANITOR_REAL_DOCKER_IMAGE" \
      --format '{{.Id}}'
  )"
  "$REAL_DOCKER" container create \
    --name "$REAL_HOLDER" \
    --label vp.videoprocess.holder=staging-object-janitor-evidence \
    --label "vp.videoprocess.generation=$real_generation" \
    --label "vp.videoprocess.transaction=staging-object-janitor:$real_generation" \
    --label "vp.videoprocess.volume=$REAL_VOLUME" \
    --label "vp.videoprocess.volume-identity=$identity_before" \
    --label "vp.videoprocess.image-id=$real_image_id" \
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
    --mount "type=volume,src=$REAL_VOLUME,dst=/run/videoprocess/staging-janitor" \
    --entrypoint "" \
    "$VP_STAGING_JANITOR_REAL_DOCKER_IMAGE" \
    /opt/venv/bin/python -I -m \
    app.channel_agent.staging_object_janitor_cli prepare-evidence >/dev/null
  validate_real_holder() {
    (
      docker() {
        "$REAL_DOCKER" "$@"
      }
      VP_STAGING_JANITOR_LIBRARY_ONLY=1
      VP_STAGING_JANITOR_CONFIG_FILE="$TEST_ROOT/library-only-must-not-read-config"
      source "$LAUNCHER"
      holder_name="$REAL_HOLDER"
      generation="$real_generation"
      image="$VP_STAGING_JANITOR_REAL_DOCKER_IMAGE"
      image_id="$real_image_id"
      evidence_volume="$REAL_VOLUME"
      volume_identity="$identity_before"
      validate_holder "$REAL_HOLDER"
    )
  }
  if ! holder_details="$(validate_real_holder)"; then
    echo 'FAIL: production validator rejected the created real holder' >&2
    exit 1
  fi
  [[ "$holder_details" =~ ^[0-9a-f]{64}\|created\|0$ ]] \
    || {
      echo 'FAIL: production validator returned invalid created state' >&2
      exit 1
    }

  "$REAL_DOCKER" container start --attach "$REAL_HOLDER" >/dev/null
  holder_details="$(validate_real_holder)" \
    || {
      echo 'FAIL: production validator rejected the prepared real holder' >&2
      exit 1
    }
  [[ "$holder_details" =~ ^[0-9a-f]{64}\|exited\|0$ ]] \
    || {
      echo 'FAIL: production validator returned invalid prepared state' >&2
      exit 1
    }

  set +e
  "$REAL_DOCKER" volume rm "$REAL_VOLUME" \
    >"$TEST_ROOT/real-volume-rm.out" 2>&1 &
  volume_rm_pid=$!
  "$REAL_DOCKER" volume prune --force --filter "label=$test_label" \
    >"$TEST_ROOT/real-volume-prune.out" 2>&1 &
  volume_prune_pid=$!
  (
    "$REAL_DOCKER" volume rm "$REAL_VOLUME" \
      && "$REAL_DOCKER" volume create "$REAL_VOLUME"
  ) >"$TEST_ROOT/real-volume-replace.out" 2>&1 &
  volume_replace_pid=$!
  "$REAL_DOCKER" volume create "$REAL_VOLUME" \
    >"$TEST_ROOT/real-volume-create.out" 2>&1 &
  volume_create_pid=$!
  wait "$volume_rm_pid"
  volume_rm_status=$?
  wait "$volume_prune_pid"
  volume_prune_status=$?
  wait "$volume_replace_pid"
  volume_replace_status=$?
  wait "$volume_create_pid"
  volume_create_status=$?
  set -e

  if [[ "$volume_rm_status" -eq 0 ]]; then
    echo 'FAIL: pinned real Docker volume was removed' >&2
    exit 1
  fi
  if [[ "$volume_replace_status" -eq 0 ]]; then
    echo 'FAIL: pinned real Docker volume was replaced' >&2
    exit 1
  fi
  if [[ "$volume_prune_status" -ne 0 || "$volume_create_status" -ne 0 ]]; then
    echo 'FAIL: real Docker pin concurrency probes did not complete' >&2
    exit 1
  fi
  identity_after="$(
    "$REAL_DOCKER" volume inspect "$REAL_VOLUME" --format '{{json .}}' \
      | python3 -c '
import hashlib
import json
import sys

volume = json.load(sys.stdin)
identity = {
    "CreatedAt": volume["CreatedAt"],
    "Driver": volume["Driver"],
    "Labels": volume.get("Labels"),
    "Mountpoint": volume["Mountpoint"],
    "Name": volume["Name"],
    "Options": volume.get("Options"),
    "Scope": volume["Scope"],
}
encoded = json.dumps(
    identity,
    sort_keys=True,
    separators=(",", ":"),
).encode()
print(hashlib.sha256(encoded).hexdigest())
'
  )"
  if [[ "$identity_after" != "$identity_before" ]]; then
    echo 'FAIL: same-name create replaced pinned real Docker volume' >&2
    exit 1
  fi
  "$REAL_DOCKER" container rm "$REAL_HOLDER" >/dev/null
  REAL_HOLDER=""
  "$REAL_DOCKER" volume rm "$REAL_VOLUME" >/dev/null
  REAL_VOLUME=""
  echo "real Docker holder pin/replacement tests passed"
fi

echo "staging object janitor launcher tests passed"
