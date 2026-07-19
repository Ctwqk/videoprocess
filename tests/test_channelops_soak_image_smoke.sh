#!/usr/bin/env bash
set -euo pipefail

image="${VP_SOAK_SMOKE_IMAGE:-}"
database_url="${VP_SOAK_SMOKE_DATABASE_URL:-}"

if [[ -z "$image" || -z "$database_url" ]]; then
  printf '%s\n' 'SKIP: set VP_SOAK_SMOKE_IMAGE and VP_SOAK_SMOKE_DATABASE_URL'
  exit 0
fi
if [[ "${VP_SOAK_SMOKE_TEST_DATABASE:-}" != "true" ]]; then
  printf '%s\n' 'FAIL: set VP_SOAK_SMOKE_TEST_DATABASE=true only for an isolated test database' >&2
  exit 2
fi
if ! docker image inspect "$image" >/dev/null 2>&1; then
  printf 'FAIL: watcher-matching image is not available locally: %s\n' "$image" >&2
  exit 2
fi

output="$(mktemp "${TMPDIR:-/tmp}/channelops-soak-image-smoke.XXXXXX")"
cleanup() {
  rm -f "$output"
}
trap cleanup EXIT

export DATABASE_URL="$database_url"
set +e
docker run --rm --env DATABASE_URL "$image" \
  python -m app.channel_agent.soak_guard_cli \
  --channel-id 00000000-0000-4000-8000-00000000f117 \
  --started-at 1970-01-01T00:00:00Z >"$output"
status=$?
set -e
unset DATABASE_URL

if [[ "$status" -ne 20 ]]; then
  printf 'FAIL: image CLI exited %d instead of guard exit 20\n' "$status" >&2
  exit 1
fi
if ! grep -Fq '"critical_codes":["channel_missing"]' "$output" \
  || ! grep -Fq '"status":"critical"' "$output"; then
  printf '%s\n' 'FAIL: image CLI did not return the safe missing-channel assessment' >&2
  exit 1
fi

printf 'PASS: real watcher-image CLI reached the test database and returned guard exit 20\n'
