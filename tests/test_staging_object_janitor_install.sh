#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rc=$?; rm -rf "$TEST_ROOT"; exit "$rc"' EXIT

FAKE_BIN="$TEST_ROOT/fake-bin"
FAKE_CRONTAB="$TEST_ROOT/crontab"
FAKE_CALLS="$TEST_ROOT/crontab-calls"
FAKE_FAILURE_USED="$TEST_ROOT/failure-used"
mkdir -p "$FAKE_BIN"
export FAKE_CRONTAB FAKE_CALLS FAKE_FAILURE_USED
FAKE_INSTALL_MODE=normal
export FAKE_INSTALL_MODE

cat >"$FAKE_BIN/crontab" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'crontab|%s\n' "$*" >>"$FAKE_CALLS"
case "${1:-}" in
  -l)
    if [[ -f "$FAKE_CRONTAB" ]]; then
      cat "$FAKE_CRONTAB"
      exit 0
    fi
    echo 'no crontab for test-user' >&2
    exit 1
    ;;
  -r)
    rm -f "$FAKE_CRONTAB"
    exit 0
    ;;
esac
if [[ "$FAKE_INSTALL_MODE" == verify-mismatch \
  && ! -f "$FAKE_FAILURE_USED" ]]; then
  cp "$1" "$FAKE_CRONTAB"
  printf 'injected mismatch\n' >>"$FAKE_CRONTAB"
  : >"$FAKE_FAILURE_USED"
  exit 0
fi
cp "$1" "$FAKE_CRONTAB"
EOF
chmod +x "$FAKE_BIN/crontab"
PATH="$FAKE_BIN:$PATH"
export PATH

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
mkdir -p "$ROOT"
log() {
  :
}
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

vp_require_pipeline_network_identity() {
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
}

VP_STAGING_JANITOR_SOURCE="$ROOT_DIR/deploy/swarm/staging-object-janitor-run.sh"
VP_WORKER_ADMISSION_PREPARED=true
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
VP_STAGING_JANITOR_DATABASE_SECRET=vp-staging-db-test
VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=vp-staging-minio-access-test
VP_STAGING_JANITOR_MINIO_SECRET_SECRET=vp-staging-minio-secret-test
image=vp-ffmpeg-worker-python:deploy-0123456789ab
target="$ROOT/bin/vp-staging-object-janitor-run.sh"
config="$ROOT/state/vp-worker-admission/staging-object-janitor.conf"

UPDATE_SERVICES=0
: >"$FAKE_CALLS"
vp_install_staging_object_janitor "$image"
if [[ -s "$FAKE_CALLS" || -e "$target" || -e "$config" ]]; then
  echo 'FAIL: staging janitor dry-run mutated installation state' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=video-ops@example.com
0 2 * * * /usr/local/bin/backup-video-state
# BEGIN VIDEOPROCESS STAGING JANITOR
0 * * * * $target --legacy
# END VIDEOPROCESS STAGING JANITOR
17 * * * * $target --unmarked-legacy
@reboot /usr/local/bin/restore-video-network
EOF
UPDATE_SERVICES=1
vp_install_staging_object_janitor "$image"

if [[ "$(grep -Fxc '# BEGIN VIDEOPROCESS STAGING JANITOR' "$FAKE_CRONTAB")" -ne 1 \
  || "$(grep -Fxc '# END VIDEOPROCESS STAGING JANITOR' "$FAKE_CRONTAB")" -ne 1 ]]; then
  echo 'FAIL: staging janitor installer did not produce one marked block' >&2
  exit 1
fi
grep -Fqx 'MAILTO=video-ops@example.com' "$FAKE_CRONTAB"
grep -Fqx '0 2 * * * /usr/local/bin/backup-video-state' "$FAKE_CRONTAB"
grep -Fqx '@reboot /usr/local/bin/restore-video-network' "$FAKE_CRONTAB"
grep -Fqx \
  "*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE=$config $target >> $ROOT/logs/vp-staging-object-janitor.log 2>&1" \
  "$FAKE_CRONTAB"
if grep -Fq -- '--legacy' "$FAKE_CRONTAB"; then
  echo 'FAIL: staging janitor installer retained a legacy invocation' >&2
  exit 1
fi
if [[ "$(vp_worker_redis_marker_file_mode "$target")" != 700 \
  || "$(vp_worker_redis_marker_file_mode "$config")" != 600 ]]; then
  echo 'FAIL: staging janitor installation modes are not 0700/0600' >&2
  exit 1
fi
grep -Fxq "GENERATION=$VP_WORKER_CONTROL_GENERATION" "$config"
grep -Fxq "IMAGE=$image" "$config"
grep -Fxq 'NETWORK=vp-pipeline-net' "$config"
grep -Fxq 'NETWORK_ID=vp-pipeline-network-id' "$config"
grep -Fxq 'MANAGER_NODE=ccttww-lap' "$config"
if grep -Eiq '10\.0\.0\.126|DATABASE_URL=|MINIO_(ACCESS|SECRET)_KEY=' \
  "$FAKE_CRONTAB" "$config"; then
  echo 'FAIL: staging janitor installation exposed a secret or host 126' >&2
  exit 1
fi

cp "$FAKE_CRONTAB" "$TEST_ROOT/idempotent-before"
vp_install_staging_object_janitor "$image"
cmp -s "$TEST_ROOT/idempotent-before" "$FAKE_CRONTAB"

printf '#!/usr/bin/env bash\nprintf "prior launcher\\n"\n' >"$target"
chmod 0755 "$target"
printf 'prior-config\n' >"$config"
chmod 0600 "$config"
cp "$FAKE_CRONTAB" "$TEST_ROOT/rollback-cron"
cp "$target" "$TEST_ROOT/rollback-target"
cp "$config" "$TEST_ROOT/rollback-config"
: >"$FAKE_CALLS"
rm -f "$FAKE_FAILURE_USED"
FAKE_INSTALL_MODE=verify-mismatch
export FAKE_INSTALL_MODE
if vp_install_staging_object_janitor "$image" >/dev/null 2>&1; then
  echo 'FAIL: staging janitor crontab verification mismatch succeeded' >&2
  exit 1
fi
cmp -s "$TEST_ROOT/rollback-cron" "$FAKE_CRONTAB"
cmp -s "$TEST_ROOT/rollback-target" "$target"
cmp -s "$TEST_ROOT/rollback-config" "$config"

echo 'staging object janitor installer tests passed'
