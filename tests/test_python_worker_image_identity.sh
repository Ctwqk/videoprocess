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

if ! loader_output="$(
  vp_run_python_worker_container \
    "$image" \
    "$bind_secret" \
    worker-deploy-read-database-url \
    - \
    -- \
    /opt/venv/bin/python -I -c '
import os
import stat
from pathlib import Path

from worker.secret_config import read_mode_0400_secret

path = Path("/run/secrets/worker-deploy-read-database-url")
metadata = path.lstat()
assert os.geteuid() == 10001
assert os.getegid() == 10001
assert metadata.st_uid == 10001
assert metadata.st_gid == 10001
assert stat.S_IMODE(metadata.st_mode) == 0o400
assert read_mode_0400_secret(
    path,
    label="worker deploy-read database URL",
) == "synthetic-bind-secret"
print("python-worker-bind-secret passed uid=10001 gid=10001 mode=0400")
'
)"; then
  echo "FAIL: exact Python worker image could not read the transported secret" >&2
  exit 1
fi
if [[ "$loader_output" != *"python-worker-bind-secret passed uid=10001 gid=10001 mode=0400"* ]]; then
  echo "FAIL: exact Python worker image did not execute the real secret loader" >&2
  exit 1
fi
printf '%s\n' "$loader_output"

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
