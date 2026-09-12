#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d "$ROOT_DIR/.task4bc-python-image.XXXXXX")"
LEGACY_EVIDENCE_VOLUME=""
cleanup() {
  local status=$?
  if [[ -n "$LEGACY_EVIDENCE_VOLUME" ]]; then
    docker volume rm -f "$LEGACY_EVIDENCE_VOLUME" >/dev/null 2>&1 || true
  fi
  rm -rf "$TEST_ROOT"
  exit "$status"
}
trap cleanup EXIT

image="${1:?usage: $0 IMAGE EXPECTED_BUILD_COMMIT}"
expected_commit="${2:?usage: $0 IMAGE EXPECTED_BUILD_COMMIT}"
identity_path="/usr/local/share/videoprocess/worker-build-commit"

if [[ ! "$expected_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "FAIL: expected build commit must be 40 lowercase hex characters" >&2
  exit 1
fi

revision="$(
  docker image inspect \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
    "$image"
)"
if [[ "$revision" != "$expected_commit" ]]; then
  echo "FAIL: image revision does not match expected build commit" >&2
  exit 1
fi

docker run --rm \
  --env "EXPECTED_BUILD_COMMIT=$expected_commit" \
  --env "VP_BUILD_COMMIT=ffffffffffffffffffffffffffffffffffffffff" \
  --env "PYTHONPATH=/tmp/forged-import" \
  "$image" \
  /bin/bash \
  -ceu '
    identity_path="/usr/local/share/videoprocess/worker-build-commit"
    if [[ "$(id -u)" -eq 0 ]]; then
      echo "FAIL: Python worker image runs as root" >&2
      exit 1
    fi
    if [[ ! -f "$identity_path" || -L "$identity_path" ]]; then
      echo "FAIL: build identity is not a fixed regular file" >&2
      exit 1
    fi
    if [[ "$(stat -c "%U:%G:%a" "$identity_path")" != "root:root:444" ]]; then
      echo "FAIL: build identity is not root-owned mode 0444" >&2
      exit 1
    fi
    if [[ "$(<"$identity_path")" != "$EXPECTED_BUILD_COMMIT" ]]; then
      echo "FAIL: fixed build identity does not match image revision" >&2
      exit 1
    fi
    for protected_path in \
      /opt/venv/lib/python3.12/site-packages/videoprocess-worker.pth \
      /app/worker/registration.py; do
      if [[ "$(stat -c "%U:%G" "$protected_path")" != "root:root" ]] \
        || [[ -w "$protected_path" ]]; then
        echo "FAIL: isolated import path is runtime-writable" >&2
        exit 1
      fi
    done
    if (
      printf "%s\n" \
        "ffffffffffffffffffffffffffffffffffffffff" \
        >"$identity_path"
    ) 2>/dev/null; then
      echo "FAIL: runtime user can rewrite build identity" >&2
      exit 1
    fi

    mkdir -p /tmp/forged-import
    cp -a /app/worker /tmp/forged-import/worker
    chmod -R u+w /tmp/forged-import/worker
    printf "BUILD_COMMIT = \"%s\"\n" \
      "ffffffffffffffffffffffffffffffffffffffff" \
      >/tmp/forged-import/worker/_build_identity.py

    /opt/venv/bin/python -I - <<"PY"
import os
import uuid

import worker.registration as registration

expected = os.environ["EXPECTED_BUILD_COMMIT"]
assert registration.__file__ == "/app/worker/registration.py"
assert registration.EMBEDDED_BUILD_COMMIT == expected

env = {
    "DEPLOY_MODE": "production",
    "WORKER_SERVICE_NAME": "vp-vision-worker-swarm",
    "WORKER_ADMISSION_GENERATION": "4",
    "WORKER_SLOT": "1",
    "WORKER_TYPE": "vision",
    "WORKER_HOST": "host127",
    "WORKER_CAPABILITIES": "vision_gpu",
    "WORKER_RELEASE_COMMIT": expected,
    "VP_BUILD_COMMIT": "f" * 40,
    "WORKER_IMAGE_IDENTITY": f"vp-vision-worker:deploy-{expected[:12]}",
    "WORKER_REDIS_STREAM": "vp:tasks:vision",
    "WORKER_REDIS_GROUP": "vision-workers",
    "STORAGE_BACKEND": "minio",
    "MINIO_ENDPOINT": "vp-minio:9000",
    "MINIO_BUCKET": "videoprocess",
}
claims = registration.build_worker_registration_claims(
    env,
    database_url=(
        "postgresql+asyncpg://runtime:synthetic@"
        "vp-postgres:5432/videoprocess"
    ),
    redis_url="redis://vp-worker:synthetic@vp-redis:6379/7",
    worker_instance_id=uuid.uuid4(),
)
assert claims.release_commit == expected
print(
    "python-worker-image-identity passed "
    f"uid={os.getuid()} commit={claims.release_commit}"
)
PY
  '

bind_secret="$TEST_ROOT/bind-secret"
printf '%s\n' 'synthetic-bind-secret' >"$bind_secret"
chmod 0400 "$bind_secret"
REPO_ROOT="$ROOT_DIR"
ROOT="$TEST_ROOT/sync"
export REPO_ROOT ROOT
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
caller_uid="$(id -u)"
caller_gid="$(id -g)"
admission_root="$ROOT/state/vp-worker-admission"
mkdir -p "$admission_root"
chmod 0700 "$admission_root"

if ! loader_output="$(
  vp_run_python_worker_container \
    "$image" \
    "$bind_secret" \
    worker-deploy-read-database-url \
    - \
    --env "EXPECTED_CALLER_UID=$caller_uid" \
    --env "EXPECTED_CALLER_GID=$caller_gid" \
    -- \
    /opt/venv/bin/python -I -c '
import os
import stat
from pathlib import Path

from worker.secret_config import read_mode_0400_secret

path = Path("/run/secrets/worker-deploy-read-database-url")
metadata = path.lstat()
expected_uid = int(os.environ["EXPECTED_CALLER_UID"])
expected_gid = int(os.environ["EXPECTED_CALLER_GID"])
assert os.geteuid() == expected_uid
assert os.getegid() == expected_gid
assert metadata.st_uid == expected_uid
assert metadata.st_gid == expected_gid
assert stat.S_IMODE(metadata.st_mode) == 0o400
process_status = {}
for line in Path("/proc/self/status").read_text().splitlines():
    key, separator, value = line.partition(":")
    if separator:
        process_status[key] = value.strip()
assert int(process_status["CapEff"], 16) == 0
assert int(process_status["CapBnd"], 16) == 0
assert process_status["NoNewPrivs"] == "1"
assert read_mode_0400_secret(
    path,
    label="worker deploy-read database URL",
) == "synthetic-bind-secret"
try:
    Path(
        "/usr/local/share/videoprocess/worker-build-commit"
    ).write_text("f" * 40)
except OSError:
    pass
else:
    raise AssertionError("read-only one-shot rewrote build identity")
print(
    "python-worker-bind-secret passed "
    f"uid={expected_uid} gid={expected_gid} mode=0400"
)
'
)"; then
  echo "FAIL: exact Python worker image could not read the transported secret" >&2
  exit 1
fi
if [[ "$loader_output" != *"python-worker-bind-secret passed uid=$caller_uid gid=$caller_gid mode=0400"* ]]; then
  echo "FAIL: exact Python worker image did not execute the real secret loader" >&2
  exit 1
fi
printf '%s\n' "$loader_output"

runtime_state="$admission_root/exact-caller-runtime"
request_state="$admission_root/exact-caller-requests"
mkdir -p "$runtime_state" "$request_state"
chmod 0700 "$runtime_state" "$request_state"
if ! provision_output="$(
  vp_run_python_worker_container \
    "$image" \
    "$bind_secret" \
    worker-runtime-owner-database-url \
    /runtime-state \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state" \
    --env "EXPECTED_CALLER_UID=$caller_uid" \
    --env "EXPECTED_CALLER_GID=$caller_gid" \
    -- \
    /opt/venv/bin/python -I -c '
import os
from pathlib import Path

root = Path("/runtime-state/vp-test-service/701")
root.mkdir(mode=0o700, parents=True)
credential = root / "worker-database-url"
credential.write_text("caller-owned-credential\n")
credential.chmod(0o400)
state = root / "generation-state.json"
state.write_text("{}\n")
state.chmod(0o600)
assert os.geteuid() == int(os.environ["EXPECTED_CALLER_UID"])
assert os.getegid() == int(os.environ["EXPECTED_CALLER_GID"])
print("python-worker-caller-provision passed")
'
)"; then
  echo "FAIL: exact-image caller provision failed" >&2
  exit 1
fi
[[ "$provision_output" == *"python-worker-caller-provision passed"* ]]
credential_file="$runtime_state/vp-test-service/701/worker-database-url"
[[ "$(<"$credential_file")" == caller-owned-credential ]]
[[ "$(stat -f '%u' "$credential_file")" == "$caller_uid" ]]
[[ "$(stat -f '%g' "$credential_file")" == "$caller_gid" ]]
[[ "$(stat -f '%Lp' "$credential_file")" == 400 ]]

if ! render_output="$(
  vp_run_python_worker_container \
    "$image" \
    - \
    - \
    /requests \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state,readonly" \
    --mount "type=bind,src=$request_state,dst=/requests" \
    -- \
    /opt/venv/bin/python -I -c '
from pathlib import Path

credential = Path(
    "/runtime-state/vp-test-service/701/worker-database-url"
).read_text()
assert credential == "caller-owned-credential\n"
request_dir = Path("/requests/701")
request_dir.mkdir(mode=0o700)
request = request_dir / "upsert.json"
request.write_text("{\"generation\":701}\n")
request.chmod(0o600)
print("python-worker-caller-render passed")
'
)"; then
  echo "FAIL: exact-image caller render failed" >&2
  exit 1
fi
[[ "$render_output" == *"python-worker-caller-render passed"* ]]
request_file="$request_state/701/upsert.json"
[[ "$(<"$request_file")" == '{"generation":701}' ]]
[[ "$(stat -f '%u' "$request_file")" == "$caller_uid" ]]
[[ "$(stat -f '%g' "$request_file")" == "$caller_gid" ]]
[[ "$(stat -f '%Lp' "$request_file")" == 600 ]]

if ! revoke_output="$(
  vp_run_python_worker_container \
    "$image" \
    "$bind_secret" \
    worker-runtime-owner-database-url \
    /runtime-state \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state" \
    -- \
    /opt/venv/bin/python -I -c '
from pathlib import Path

generation = Path("/runtime-state/vp-test-service/701")
for filename in ("worker-database-url", "generation-state.json"):
    (generation / filename).unlink()
generation.rmdir()
generation.parent.rmdir()
print("python-worker-caller-revoke passed")
'
)"; then
  echo "FAIL: exact-image caller revoke reuse failed" >&2
  exit 1
fi
[[ "$revoke_output" == *"python-worker-caller-revoke passed"* ]]
[[ -d "$runtime_state" && -r "$runtime_state" && -w "$runtime_state" ]]
if [[ -e "$runtime_state/vp-test-service" ]]; then
  echo "FAIL: caller revoke did not remove its operation state" >&2
  exit 1
fi
printf '%s\n' "$provision_output" "$render_output" "$revoke_output"

run_simulated_controller() {
  local simulated_uid="$1"
  local simulated_gid="$2"
  docker run --rm \
    --user "$simulated_uid:$simulated_gid" \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,size=16777216,mode=1777 \
    --mount \
    "type=bind,src=$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh,dst=/test/deploy-sync-extension.sh,readonly" \
    --env "SIMULATED_UID=$simulated_uid" \
    --env "SIMULATED_GID=$simulated_gid" \
    "$image" \
    /bin/bash -ceu '
      ROOT=/tmp/sync
      REPO_ROOT=/tmp/repos
      mkdir -p "$ROOT/state/vp-worker-admission"
      chmod 0700 "$ROOT/state/vp-worker-admission"
      source /test/deploy-sync-extension.sh

      docker() {
        local joined=" $* "
        [[ "$joined" == *" --user 0:0 "* ]]
        [[ "$joined" == *" --read-only "* ]]
        [[ "$joined" == *" --cap-drop ALL "* ]]
        [[ "$joined" == *" --cap-add CHOWN "* ]]
        [[ "$joined" == *" --cap-add SETPCAP "* ]]
        [[ "$joined" == *" --cap-add SETGID "* ]]
        [[ "$joined" == *" --cap-add SETUID "* ]]
        [[ "$joined" == *" --interactive "* ]]
        [[ "$joined" == *" --entrypoint /bin/bash "* ]]
        [[ "$joined" == *" --bounding-set=-all "* ]]
        [[ "$joined" != *" chown -R "* ]]
        [[ "$joined" != *" simulated-secret "* ]]
        [[ "$joined" != *" bootstrap-secret "* ]]
        local argument
        local state_source=""
        for argument in "$@"; do
          case "$argument" in
            type=bind,src=*,dst=/runtime-state)
              state_source="${argument#type=bind,src=}"
              state_source="${state_source%,dst=/runtime-state}"
              ;;
          esac
        done
        [[ -n "$state_source" ]]
        compgen -G "$state_source/.vp-python-worker-bind-*" >/dev/null
        /opt/venv/bin/python -I -c \
          "import sys; payload = sys.stdin.buffer.read(); \
assert payload.startswith(b\"VPW1\"); \
assert b\"simulated-secret\" in payload"
        mkdir -p "$state_source/controller-output"
        chmod 0700 "$state_source/controller-output"
        printf "%s\n" "simulated-controller" \
          >"$state_source/controller-output/read-back"
        chmod 0600 "$state_source/controller-output/read-back"
      }

      secret="$ROOT/controller-secret"
      state="$ROOT/controller-state"
      printf "%s\n" "simulated-secret" >"$secret"
      chmod 0400 "$secret"
      mkdir -p "$state"
      chmod 0700 "$state"
      vp_run_python_worker_container \
        synthetic-image \
        "$secret" \
        simulated-secret \
        /runtime-state \
        --mount "type=bind,src=$state,dst=/runtime-state" \
        -- \
        /bin/true
      [[ "$(<"$state/controller-output/read-back")" \
        == simulated-controller ]]
      [[ "$(stat -c "%u:%g:%a" "$state")" \
        == "$SIMULATED_UID:$SIMULATED_GID:700" ]]
      [[ "$(stat -c "%u:%g:%a" \
        "$state/controller-output/read-back")" \
        == "$SIMULATED_UID:$SIMULATED_GID:600" ]]
      if grep -R -Fq simulated-secret \
        "$ROOT/state/vp-worker-admission" 2>/dev/null; then
        echo "FAIL: simulated controller retained a host credential copy" >&2
        exit 1
      fi
      if printf "%s\n" forged \
        >/usr/local/share/videoprocess/worker-build-commit 2>/dev/null; then
        echo "FAIL: simulated root controller rewrote image identity" >&2
        exit 1
      fi
      printf "python-worker-simulated-controller passed uid=%s gid=%s\n" \
        "$SIMULATED_UID" "$SIMULATED_GID"
    '
}

run_simulated_controller 1000 1000
run_simulated_controller 0 0

if ! fresh_evidence_output="$(
  docker run --rm \
    --user 0:0 \
    --mount type=volume,dst=/run/videoprocess/staging-janitor \
    "$image" \
    /bin/bash -ceu '
      /opt/venv/bin/python -I -m \
        app.channel_agent.staging_object_janitor_cli prepare-evidence
      exec /usr/bin/setpriv \
        --reuid=10001 \
        --regid=10001 \
        --clear-groups \
        --no-new-privs \
        /opt/venv/bin/python -I - <<"PY"
import os
import stat
from datetime import datetime, timezone
from pathlib import Path

from app.services.staging_object_janitor import StagingObjectJanitor

status_file = Path("/run/videoprocess/staging-janitor/status.json")
janitor = StagingObjectJanitor(
    object(),
    client=object(),
    bucket="videoprocess",
    status_file=status_file,
)
janitor._write_status(
    datetime(2026, 7, 29, tzinfo=timezone.utc),
    {
        "scanned": 0,
        "deleted": 0,
        "protected": 0,
        "too_young": 0,
        "invalid": 0,
        "errors": 0,
    },
)
directory_metadata = status_file.parent.stat()
status_metadata = status_file.stat()
assert os.geteuid() == 10001
assert os.getegid() == 10001
assert directory_metadata.st_uid == 10001
assert directory_metadata.st_gid == 10001
assert stat.S_IMODE(directory_metadata.st_mode) == 0o700
assert status_metadata.st_uid == 10001
assert status_metadata.st_gid == 10001
assert stat.S_IMODE(status_metadata.st_mode) == 0o600
print("python-worker-fresh-evidence passed uid=10001 gid=10001")
PY
    '
)"; then
  echo "FAIL: fresh anonymous evidence volume was not prepared" >&2
  exit 1
fi
if [[ "$fresh_evidence_output" != *"python-worker-fresh-evidence passed uid=10001 gid=10001"* ]]; then
  echo "FAIL: fresh evidence probe did not call the real status writer" >&2
  exit 1
fi
printf '%s\n' "$fresh_evidence_output"

LEGACY_EVIDENCE_VOLUME="vp-task4bc-janitor-legacy-$$"
docker volume create "$LEGACY_EVIDENCE_VOLUME" >/dev/null
docker run --rm \
  --user 0:0 \
  --mount \
  "type=volume,src=$LEGACY_EVIDENCE_VOLUME,dst=/run/videoprocess/staging-janitor" \
  "$image" \
  /bin/bash -ceu '
    evidence=/run/videoprocess/staging-janitor
    printf "%s\n" "legacy evidence" >"$evidence/status.json"
    chown 0:0 "$evidence" "$evidence/status.json"
    chmod 0700 "$evidence"
    chmod 0600 "$evidence/status.json"
  '
for _ in 1 2; do
  docker run --rm \
    --user 0:0 \
    --mount \
    "type=volume,src=$LEGACY_EVIDENCE_VOLUME,dst=/run/videoprocess/staging-janitor" \
    "$image" \
    /opt/venv/bin/python -I -m \
    app.channel_agent.staging_object_janitor_cli prepare-evidence >/dev/null
done
if ! legacy_evidence_output="$(
  docker run --rm \
    --mount \
    "type=volume,src=$LEGACY_EVIDENCE_VOLUME,dst=/run/videoprocess/staging-janitor" \
    "$image" \
    /opt/venv/bin/python -I -c '
import os
import stat
from datetime import datetime, timezone
from pathlib import Path

from app.services.staging_object_janitor import StagingObjectJanitor

status_file = Path("/run/videoprocess/staging-janitor/status.json")
assert status_file.read_text() == "legacy evidence\n"
janitor = StagingObjectJanitor(
    object(),
    client=object(),
    bucket="videoprocess",
    status_file=status_file,
)
janitor._write_status(
    datetime(2026, 7, 29, tzinfo=timezone.utc),
    {
        "scanned": 1,
        "deleted": 0,
        "protected": 1,
        "too_young": 0,
        "invalid": 0,
        "errors": 0,
    },
)
directory_metadata = status_file.parent.stat()
status_metadata = status_file.stat()
assert os.geteuid() == 10001
assert os.getegid() == 10001
assert directory_metadata.st_uid == 10001
assert directory_metadata.st_gid == 10001
assert stat.S_IMODE(directory_metadata.st_mode) == 0o700
assert status_metadata.st_uid == 10001
assert status_metadata.st_gid == 10001
assert stat.S_IMODE(status_metadata.st_mode) == 0o600
print("python-worker-legacy-evidence passed uid=10001 gid=10001")
'
)"; then
  echo "FAIL: legacy named evidence volume was not writable after migration" >&2
  exit 1
fi
if [[ "$legacy_evidence_output" != *"python-worker-legacy-evidence passed uid=10001 gid=10001"* ]]; then
  echo "FAIL: legacy evidence probe did not call the real status writer" >&2
  exit 1
fi
printf '%s\n' "$legacy_evidence_output"
