#!/usr/bin/env bash
set -euo pipefail

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
