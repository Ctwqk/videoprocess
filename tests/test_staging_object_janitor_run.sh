#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$ROOT_DIR/deploy/swarm/staging-object-janitor-run.sh"
TEST_ROOT="$(mktemp -d)"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

FAKE_BIN="$TEST_ROOT/bin"
CALLS="$TEST_ROOT/calls"
SERVICE_STATE="$TEST_ROOT/service-state"
TASK_STATE_FILE="$TEST_ROOT/task-state"
SPEC_FILE="$TEST_ROOT/spec.json"
VOLUME_STATE="$TEST_ROOT/volume-state"
mkdir -p "$FAKE_BIN"
export CALLS SERVICE_STATE TASK_STATE_FILE SPEC_FILE VOLUME_STATE

cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker|%s\n' "$*" >>"$CALLS"

if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
  printf 'vp-pipeline-network-id|vp-pipeline-net|overlay|swarm\n'
  exit 0
fi
if [[ "${1:-} ${2:-}" == "volume inspect" ]]; then
  [[ -f "$VOLUME_STATE" ]] || exit 1
  volume_name=vp-staging-janitor-evidence
  if [[ "${FAIL_VOLUME_IDENTITY:-0}" == 1 ]]; then
    volume_name=unexpected-evidence-volume
  fi
  printf '{"Name":"%s","Driver":"local","Scope":"local","Options":null,"Labels":{"vp.videoprocess.volume":"staging-object-janitor-evidence"}}\n' \
    "$volume_name"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "volume create" ]]; then
  : >"$VOLUME_STATE"
  printf '%s\n' "${*: -1}"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
  [[ -f "$SERVICE_STATE" ]] || exit 1
  if [[ "$*" == *'{{json .Spec}}'* ]]; then
    cat "$SPEC_FILE"
  fi
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service ps" ]]; then
  cat "$TASK_STATE_FILE"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service rm" ]]; then
  rm -f "$SERVICE_STATE"
  exit 0
fi
if [[ "${1:-} ${2:-}" == "service create" ]]; then
  : >"$SERVICE_STATE"
  exit 0
fi
if [[ "${1:-}" == run ]]; then
  [[ "${FAIL_EVIDENCE_PREPARE:-0}" != 1 ]]
  exit
fi
exit 90
EOF
chmod +x "$FAKE_BIN/docker"
PATH="$FAKE_BIN:$PATH"
export PATH

generation=c-0123456789abcdef0123
image=vp-ffmpeg-worker-python:deploy-0123456789ab
database_secret=vp-staging-db-test
minio_access_secret=vp-staging-minio-access-test
minio_secret_secret=vp-staging-minio-secret-test
evidence_volume=vp-staging-janitor-evidence
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

cat >"$SPEC_FILE" <<EOF
{
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

bash "$LAUNCHER"
grep -Fq \
  'docker|volume create --driver local --label vp.videoprocess.volume=staging-object-janitor-evidence vp-staging-janitor-evidence' \
  "$CALLS"
grep -Fq -- \
  "docker|run --rm --user 0:0 --mount type=volume,src=$evidence_volume,dst=/run/videoprocess/staging-janitor $image /opt/venv/bin/python -I -m app.channel_agent.staging_object_janitor_cli prepare-evidence" \
  "$CALLS"
grep -Fq 'docker|service create' "$CALLS"
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
grep -Fq 'docker|service rm vp-staging-object-janitor' "$CALLS"
grep -Fq 'docker|service create' "$CALLS"

: >"$CALLS"
printf 'Shutdown|Complete 2 seconds ago\n' >"$TASK_STATE_FILE"
bash "$LAUNCHER" retire
grep -Fq 'docker|service rm vp-staging-object-janitor' "$CALLS"
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
: >"$CALLS"
if FAIL_EVIDENCE_PREPARE=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: failed evidence preparation was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: failed evidence preparation created the janitor service' >&2
  exit 1
fi

: >"$CALLS"
if FAIL_VOLUME_IDENTITY=1 bash "$LAUNCHER" >/dev/null 2>&1; then
  echo 'FAIL: mismatched evidence volume identity was accepted' >&2
  exit 1
fi
if grep -Fq 'docker|run' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: mismatched evidence volume was prepared or launched' >&2
  exit 1
fi

echo "staging object janitor launcher tests passed"
