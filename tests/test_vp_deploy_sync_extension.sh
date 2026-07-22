#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXTENSION="$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
TEST_ROOT="$(mktemp -d)"
CALLS="$TEST_ROOT/calls"
ROOT="$TEST_ROOT/deploy-github-sync"
FAKE_BIN="$TEST_ROOT/bin"
FAKE_CRONTAB="$TEST_ROOT/crontab"
FAKE_CRONTAB_CALLS="$TEST_ROOT/crontab-calls"
FAKE_CRONTAB_FAILURE_USED="$TEST_ROOT/crontab-failure-used"
FAKE_WATCH_TARGET="$ROOT/bin/channelops-soak-watch.sh"
VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
TEST_COMMIT="0123456789abcdef0123456789abcdef01234567"
trap 'status=$?; rm -rf "$TEST_ROOT"; exit "$status"' EXIT

mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/crontab" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'crontab|%s|lc_all=%s\n' "$*" "${LC_ALL:-}" >>"$CALLS"

if [[ "${1:-}" == "-l" ]]; then
  if [[ "${FAKE_CRONTAB_READ_MODE:-normal}" == "error" ]]; then
    echo 'crontab: permission denied' >&2
    exit 1
  fi
  if [[ -f "$FAKE_CRONTAB" ]]; then
    cat "$FAKE_CRONTAB"
    exit 0
  fi
  echo 'no crontab for video-test' >&2
  exit 1
fi

if [[ "${1:-}" == "-r" ]]; then
  if [[ "${FAKE_CRONTAB_ROLLBACK_FAIL:-false}" == "true" ]]; then
    echo 'crontab: injected rollback removal failure' >&2
    exit 1
  fi
  if [[ ! -f "$FAKE_CRONTAB" ]]; then
    echo 'no crontab for video-test' >&2
    exit 1
  fi
  rm -f "$FAKE_CRONTAB"
  printf 'remove\n' >>"$FAKE_CRONTAB_CALLS"
  exit 0
fi

[[ "$#" -eq 1 ]]
if [[ "${FAKE_CRONTAB_ROLLBACK_FAIL:-false}" == "true" \
  && -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
  echo 'crontab: injected rollback install failure' >&2
  exit 1
fi
case "${FAKE_CRONTAB_INSTALL_MODE:-normal}" in
  fail-before)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      : >"$FAKE_CRONTAB_FAILURE_USED"
      echo 'crontab: injected install failure' >&2
      exit 1
    fi
    ;;
  mutate-then-fail)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      echo 'crontab: injected post-mutation failure' >&2
      exit 1
    fi
    ;;
  verify-mismatch)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      printf '# injected verification mismatch\n' >>"$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      printf 'write-mismatch\n' >>"$FAKE_CRONTAB_CALLS"
      exit 0
    fi
    ;;
  target-verify-mismatch)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      printf '#!/usr/bin/env bash\nprintf "injected target mismatch\\n"\n' >"$FAKE_WATCH_TARGET"
      chmod 0644 "$FAKE_WATCH_TARGET"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      printf 'write-target-mismatch\n' >>"$FAKE_CRONTAB_CALLS"
      exit 0
    fi
    ;;
  signal-term)
    if [[ ! -f "$FAKE_CRONTAB_FAILURE_USED" ]]; then
      cp "$1" "$FAKE_CRONTAB"
      : >"$FAKE_CRONTAB_FAILURE_USED"
      kill -TERM "$PPID"
      exit 0
    fi
    ;;
esac
cp "$1" "$FAKE_CRONTAB"
printf 'write\n' >>"$FAKE_CRONTAB_CALLS"
EOF
chmod +x "$FAKE_BIN/crontab"
export CALLS FAKE_CRONTAB FAKE_CRONTAB_CALLS FAKE_CRONTAB_FAILURE_USED FAKE_WATCH_TARGET
FAKE_CRONTAB_READ_MODE=normal
FAKE_CRONTAB_INSTALL_MODE=normal
FAKE_CRONTAB_ROLLBACK_FAIL=false
export FAKE_CRONTAB_READ_MODE FAKE_CRONTAB_INSTALL_MODE FAKE_CRONTAB_ROLLBACK_FAIL
PATH="$FAKE_BIN:$PATH"
export PATH

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=video-ops@example.com
0 2 * * * /usr/local/bin/backup-video-state
*/10 * * * * $VP_SOAK_WATCH_SOURCE >> $ROOT/logs/channelops-soak-watch.log 2>&1
*/15 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/legacy-soak-watch.log 2>&1
# BEGIN VIDEOPROCESS SOAK WATCH
0 * * * * $ROOT/bin/channelops-soak-watch.sh --legacy
# END VIDEOPROCESS SOAK WATCH
@reboot /usr/local/bin/restore-video-network
5 * * * * /usr/bin/sha256sum $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/watcher-audit.log 2>&1
10 * * * * /usr/local/bin/notify-ops channelops-soak-watch.sh
# audit checksum notification for $ROOT/bin/channelops-soak-watch.sh
EOF

REPO_ROOT=/home/taiwei/deploy-github-sync/repos
BUILD_IMAGES=1
UPDATE_SERVICES=1
HEALTH_CHECKS=1
VP_API_DATABASE_URL_GO=postgres://test:test@10.0.0.150:5435/videoprocess
VP_PYTHON_WORKER_DATABASE_URL=postgresql+asyncpg://test:test@10.0.0.150:5435/videoprocess
VP_MINIO_ACCESS_KEY=test-access
VP_MINIO_SECRET_KEY=test-secret
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
CONSTRAINT_MODE=legacy
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_NETWORK_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials
PUBLISHER_REPLICAS=3
PUBLISHER_LIST_FAILURE=false
PUBLISHER_LIST_NAME=
FAIL_PUBLISHER_INSPECT_FORMAT=
GPU_PREFLIGHT_SUCCEEDS=true
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
FAIL_RUNNING_SERVICE=
FAIL_HEALTH_CHECK=
FAIL_NODE_UPDATE=false
FAIL_NETWORK_INSPECT=false
FAIL_PUBLISHER_CREATE=false
FAIL_MANAGED_CRON_PRINTF=false
FAIL_SOAK_CLEANUP=false

printf() {
  if [[ "$FAIL_MANAGED_CRON_PRINTF" == "true" \
    && "${1:-}" == '%s\n%s\n%s\n' ]]; then
    return 1
  fi
  builtin printf "$@"
}

log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}

gh() {
  printf 'gh|%s\n' "$*" >>"$CALLS"
  printf 'found\tcompleted\tsuccess\t%s\t101\n' "$TEST_COMMIT"
}

mv() {
  printf 'mv|%s\n' "$*" >>"$CALLS"
  command mv "$@"
}

rm() {
  if [[ "$FAIL_SOAK_CLEANUP" == "true" \
    && "$*" == *"vp-soak-watch-cron."* || "$FAIL_SOAK_CLEANUP" == "true" \
    && "$*" == *".channelops-soak-watch.txn."* ]]; then
    printf 'rm-failed|%s\n' "$*" >>"$CALLS"
    return 1
  fi
  command rm "$@"
}

build_image_on_host() {
  printf 'build|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"
}

http_health() {
  printf 'health|%s|%s\n' "$1" "$2" >>"$CALLS"
  [[ "$1" != "$FAIL_HEALTH_CHECK" ]]
}

swarm_service_running() {
  printf 'running|%s\n' "$1" >>"$CALLS"
  [[ "$1" != "$FAIL_RUNNING_SERVICE" ]]
}

docker() {
  printf 'docker|%s\n' "$*" >>"$CALLS"
  if [[ "${1:-}" == "run" && "$GPU_PREFLIGHT_SUCCEEDS" != "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "node update" && "$FAIL_NODE_UPDATE" == "true" ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-ffmpeg-worker-gpu-swarm"* ]]; then
    GPU_SERVICE_EXISTS=true
  fi
  if [[ "${1:-} ${2:-}" == "service create" && "$*" == *"--name vp-youtube-publisher-swarm"* ]]; then
    PUBLISHER_SERVICE_EXISTS=true
    if [[ "$FAIL_PUBLISHER_CREATE" == "true" ]]; then
      return 1
    fi
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-ffmpeg-worker-gpu-swarm" ]]; then
    GPU_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-} ${3:-}" == "service rm vp-youtube-publisher-swarm" ]]; then
    PUBLISHER_SERVICE_EXISTS=false
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && -n "$FAIL_UPDATE_SERVICE" \
    && "$*" == *"--image $FAIL_UPDATE_IMAGE $FAIL_UPDATE_SERVICE"* ]]; then
    return 1
  fi
  if [[ "${1:-} ${2:-}" == "service update" \
    && "$*" == *"vp-youtube-publisher-swarm"* ]]; then
    if [[ "$*" == *"--replicas 0"* ]]; then
      PUBLISHER_REPLICAS=0
    fi
    if [[ "$*" == *"--mount-rm /data/storage"* ]]; then
      PUBLISHER_MOUNT_MODE=scratch_removed
    fi
    if [[ "$*" == *"--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage"* ]]; then
      PUBLISHER_MOUNT_MODE=desired
    fi
    if [[ "$*" == *"--replicas 1"* ]]; then
      PUBLISHER_REPLICAS=1
    fi
  fi
  if [[ "${1:-} ${2:-}" == "network inspect" ]]; then
    if [[ "$FAIL_NETWORK_INSPECT" == "true" ]]; then
      return 1
    fi
    echo vp-pipeline-network-id
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service ls" && "$*" == *"--filter name=vp-youtube-publisher-swarm"* ]]; then
    if [[ "$PUBLISHER_LIST_FAILURE" == "true" ]]; then
      return 1
    fi
    if [[ "$PUBLISHER_SERVICE_EXISTS" == "true" ]]; then
      printf '%s\n' "${PUBLISHER_LIST_NAME:-vp-youtube-publisher-swarm}"
    fi
    return 0
  fi
  if [[ "${1:-} ${2:-}" == "service inspect" ]]; then
    local service="${3:-}"
    if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" && "$GPU_SERVICE_EXISTS" != "true" ]]; then
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" && "$PUBLISHER_SERVICE_EXISTS" != "true" ]]; then
      echo "no such service: $service" >&2
      return 1
    fi
    if [[ "$service" == "vp-youtube-publisher-swarm" \
      && -n "$FAIL_PUBLISHER_INSPECT_FORMAT" \
      && "$*" == *"$FAIL_PUBLISHER_INSPECT_FORMAT"* ]]; then
      return 1
    fi
    case "$*" in
      *ContainerSpec.Image*)
        echo "baseline-$service:stable"
        ;;
      *Spec.Mode.Replicated.Replicas*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo "$PUBLISHER_REPLICAS"
        fi
        ;;
      *Placement.Constraints*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          case "$PUBLISHER_CONSTRAINT_MODE" in
            publisher)
              echo 'node.labels.vp.publisher==true'
              echo 'node.hostname==ccttww-lap'
              ;;
            stale)
              echo 'node.labels.vp.publisher==true'
              echo 'node.labels.vp.runtime==true'
              echo 'node.labels.vp.gpu==true'
              echo 'node.hostname==colima-swarmbridged'
              echo 'node.labels.vp.legacy==true'
              ;;
            *)
              echo 'node.labels.role==app'
              ;;
          esac
        elif [[ "$CONSTRAINT_MODE" == "runtime" ]]; then
          echo 'node.labels.vp.runtime==true'
        else
          echo 'node.labels.role==app'
        fi
        ;;
      *ContainerSpec.Env*)
        if [[ "$service" == "vp-api-swarm" ]]; then
          echo 'DATABASE_URL=legacy'
        elif [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
          echo 'YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials'
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo 'WORKER_HOST=legacy'
          if [[ "$PUBLISHER_ENV_MODE" == "credentials" ]]; then
            echo 'YOUTUBE_MANAGER_URL=http://10.0.0.150:18999'
            echo 'YOUTUBE_PUBLISH_ENABLED=false'
            echo 'YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials'
            echo 'YOUTUBE_CREDENTIALS_JSON=fixture-json'
            echo 'YOUTUBE_LEGACY_MODE=fixture'
            echo 'GOOGLE_CLIENT_SECRETS_FILE=fixture-file'
            echo 'YOUTUBE_REFRESH_TOKEN=fixture-token'
          fi
        fi
        ;;
      *ContainerSpec.Secrets*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo publisher-credential-reference
        fi
        ;;
      *ContainerSpec.Configs*)
        if [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          echo publisher-config-reference
        fi
        ;;
      *TaskTemplate.Networks*)
        if [[ "$service" == "vp-youtube-publisher-swarm" && "$PUBLISHER_NETWORK_MODE" == "pipeline" ]]; then
          echo vp-pipeline-network-id
        else
          echo legacy-network-id
        fi
        ;;
      *ContainerSpec.Mounts*)
        if [[ "$service" == "vp-ffmpeg-worker-gpu-swarm" ]]; then
          echo /app/youtube_credentials
        elif [[ "$service" == "vp-youtube-publisher-swarm" ]]; then
          case "$PUBLISHER_MOUNT_MODE" in
            desired)
              echo 'volume|vp-youtube-publisher-scratch|/data/storage|false'
              ;;
            wrong)
              echo 'volume|vp-youtube-publisher-scratch|/data/storage|true'
              echo 'bind|credential-source|/app/cache|false'
              echo 'bind|/tmp/publisher|/APP/OAUTH|false'
              ;;
            scratch_removed)
              echo 'bind|credential-source|/app/cache|false'
              echo 'bind|/tmp/publisher|/APP/OAUTH|false'
              ;;
            missing)
              echo 'bind|credential-source|/app/cache|false'
              ;;
          esac
        fi
        ;;
    esac
  fi
}

if [[ ! -f "$EXTENSION" ]]; then
  echo "FAIL: missing deploy extension: $EXTENSION" >&2
  exit 1
fi
if grep -Eq 'YOUTUBE_CREDENTIALS_DIR=|VP_YOUTUBE|--mount-add.*youtube_credentials|--mount .*youtube_credentials' "$EXTENSION"; then
  echo 'FAIL: general production worker must not receive publication credentials' >&2
  exit 1
fi
source "$EXTENSION"
images="$(build_vp_app_images "$TEST_COMMIT")"
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: deploy_vp_app_services returned non-zero' >&2
  exit 1
fi

if [[ ! -x "$ROOT/bin/channelops-soak-watch.sh" ]]; then
  echo 'FAIL: successful deployment did not install an executable soak watcher' >&2
  exit 1
fi
if ! cmp -s "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"; then
  echo 'FAIL: installed soak watcher differs from the repository source' >&2
  exit 1
fi
cat >"$TEST_ROOT/expected-crontab" <<EOF
MAILTO=video-ops@example.com
0 2 * * * /usr/local/bin/backup-video-state
@reboot /usr/local/bin/restore-video-network
5 * * * * /usr/bin/sha256sum $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/watcher-audit.log 2>&1
10 * * * * /usr/local/bin/notify-ops channelops-soak-watch.sh
# audit checksum notification for $ROOT/bin/channelops-soak-watch.sh
# BEGIN VIDEOPROCESS SOAK WATCH
*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/channelops-soak-watch.log 2>&1
# END VIDEOPROCESS SOAK WATCH
EOF
if ! cmp -s "$TEST_ROOT/expected-crontab" "$FAKE_CRONTAB"; then
  echo 'FAIL: managed soak watcher cron did not preserve unrelated entries exactly' >&2
  diff -u "$TEST_ROOT/expected-crontab" "$FAKE_CRONTAB" >&2 || true
  exit 1
fi
if [[ "$(grep -Fc '# BEGIN VIDEOPROCESS SOAK WATCH' "$FAKE_CRONTAB" || true)" -ne 1 \
  || "$(grep -Fc '# END VIDEOPROCESS SOAK WATCH' "$FAKE_CRONTAB" || true)" -ne 1 ]]; then
  echo 'FAIL: successful deployment must leave exactly one managed cron block' >&2
  exit 1
fi
if [[ "$(grep -Fc "*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh" "$FAKE_CRONTAB" || true)" -ne 1 ]]; then
  echo 'FAIL: successful deployment must leave exactly one managed soak watcher command' >&2
  exit 1
fi
if grep -Fq '*/10 * * * *' "$FAKE_CRONTAB"; then
  echo 'FAIL: successful deployment retained the historical unmarked watcher line' >&2
  exit 1
fi
if grep -E '^crontab\|-l\|lc_all=' "$CALLS" | grep -Fvq 'lc_all=C'; then
  echo 'FAIL: a crontab read did not force the C locale' >&2
  exit 1
fi
final_health_line="$(grep -nF 'running|vp-youtube-publisher-swarm' "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
watcher_rename_line="$(grep -nE "mv\|-f .*/\.channelops-soak-watch\.txn\.[^/]+/staged-watcher $ROOT/bin/channelops-soak-watch.sh$" "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
cron_install_line="$(grep -nE 'crontab\|/.+\|lc_all=C$' "$CALLS" | tail -n 1 | cut -d: -f1 || true)"
if [[ -z "$final_health_line" || -z "$watcher_rename_line" || -z "$cron_install_line" \
  || "$final_health_line" -ge "$watcher_rename_line" \
  || "$watcher_rename_line" -ge "$cron_install_line" ]]; then
  echo 'FAIL: atomic watcher rename and cron install must follow every VP service health check' >&2
  exit 1
fi

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-after-first-install"
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: repeated deploy_vp_app_services returned non-zero' >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/cron-after-first-install" "$FAKE_CRONTAB"; then
  echo 'FAIL: repeated watcher installation is not idempotent' >&2
  exit 1
fi

TMPDIR="$TEST_ROOT/tmp"
mkdir -p "$TMPDIR"
FAIL_SOAK_CLEANUP=true
if ! vp_install_soak_watch >"$TEST_ROOT/post-commit-cleanup.out" 2>&1; then
  echo 'FAIL: post-commit cleanup failure turned verified watcher install into failure' >&2
  exit 1
fi
FAIL_SOAK_CLEANUP=false
if ! grep -Fq 'cleanup failed' "$TEST_ROOT/post-commit-cleanup.out"; then
  echo 'FAIL: post-commit cleanup warning was not reported' >&2
  exit 1
fi
command rm -rf "$TMPDIR"/vp-soak-watch-cron.* "$ROOT/bin"/.channelops-soak-watch.txn.*

cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-precommit-cleanup"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-precommit-cleanup"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=fail-before
FAIL_SOAK_CLEANUP=true
if vp_install_soak_watch >"$TEST_ROOT/precommit-cleanup.out" 2>&1; then
  echo 'FAIL: pre-commit install failure with cleanup failure unexpectedly succeeded' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
FAIL_SOAK_CLEANUP=false
if ! cmp -s "$TEST_ROOT/target-before-precommit-cleanup" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-precommit-cleanup" "$FAKE_CRONTAB"; then
  echo 'FAIL: pre-commit cleanup failure prevented rollback' >&2
  exit 1
fi
if ! grep -Fq 'cleanup failed' "$TEST_ROOT/precommit-cleanup.out"; then
  echo 'FAIL: pre-commit cleanup failure was not reported' >&2
  exit 1
fi
command rm -rf "$TMPDIR"/vp-soak-watch-cron.* "$ROOT/bin"/.channelops-soak-watch.txn.*

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-no-crontab-test"
rm -f "$FAKE_CRONTAB"
if ! vp_install_soak_watch >/dev/null 2>&1; then
  echo 'FAIL: recognized no-crontab response did not allow first cron install' >&2
  exit 1
fi
cat >"$TEST_ROOT/expected-empty-crontab-install" <<EOF
# BEGIN VIDEOPROCESS SOAK WATCH
*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$ROOT $ROOT/bin/channelops-soak-watch.sh >> $ROOT/logs/channelops-soak-watch.log 2>&1
# END VIDEOPROCESS SOAK WATCH
EOF
if ! cmp -s "$TEST_ROOT/expected-empty-crontab-install" "$FAKE_CRONTAB"; then
  echo 'FAIL: recognized no-crontab response produced unexpected managed cron' >&2
  exit 1
fi
cp "$TEST_ROOT/cron-before-no-crontab-test" "$FAKE_CRONTAB"

printf '#!/usr/bin/env bash\nprintf "prior watcher\\n"\n' >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-read-error"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-read-error"
FAKE_CRONTAB_READ_MODE=error
if vp_install_soak_watch >"$TEST_ROOT/read-error.out" 2>&1; then
  echo 'FAIL: unrecognized crontab read error unexpectedly allowed installation' >&2
  exit 1
fi
FAKE_CRONTAB_READ_MODE=normal
if ! cmp -s "$TEST_ROOT/target-before-read-error" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-read-error" "$FAKE_CRONTAB"; then
  echo 'FAIL: crontab read error changed the prior watcher or crontab' >&2
  exit 1
fi
if ! grep -Fq 'permission denied' "$TEST_ROOT/read-error.out"; then
  echo 'FAIL: crontab read error was not reported' >&2
  exit 1
fi
install -m 0755 "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=transaction-test@example.com
7 1 * * * /usr/local/bin/prior-job
EOF
for install_mode in fail-before mutate-then-fail verify-mismatch target-verify-mismatch; do
  printf '#!/usr/bin/env bash\nprintf "prior-%s\\n"\n' "$install_mode" \
    >"$ROOT/bin/channelops-soak-watch.sh"
  chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
  cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-$install_mode"
  cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-$install_mode"
  rm -f "$FAKE_CRONTAB_FAILURE_USED"
  FAKE_CRONTAB_INSTALL_MODE="$install_mode"
  if vp_install_soak_watch >"$TEST_ROOT/$install_mode.out" 2>&1; then
    echo "FAIL: $install_mode unexpectedly allowed watcher and cron convergence" >&2
    exit 1
  fi
  FAKE_CRONTAB_INSTALL_MODE=normal
  if ! cmp -s "$TEST_ROOT/target-before-$install_mode" "$ROOT/bin/channelops-soak-watch.sh" \
    || [[ ! -x "$ROOT/bin/channelops-soak-watch.sh" ]] \
    || ! cmp -s "$TEST_ROOT/cron-before-$install_mode" "$FAKE_CRONTAB"; then
    echo "FAIL: $install_mode did not restore the prior watcher and crontab" >&2
    exit 1
  fi
done

rm -f "$FAKE_CRONTAB" "$ROOT/bin/channelops-soak-watch.sh" "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=mutate-then-fail
if vp_install_soak_watch >"$TEST_ROOT/absent-rollback.out" 2>&1; then
  echo 'FAIL: mutate-then-fail with absent prior artifacts unexpectedly succeeded' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
if [[ -e "$FAKE_CRONTAB" || -e "$ROOT/bin/channelops-soak-watch.sh" ]]; then
  echo 'FAIL: rollback did not restore absent watcher and no-crontab state' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=rollback-failure@example.com
EOF
printf '#!/usr/bin/env bash\nprintf "rollback-failure-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-rollback-failure"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
FAKE_CRONTAB_INSTALL_MODE=mutate-then-fail
FAKE_CRONTAB_ROLLBACK_FAIL=true
if vp_install_soak_watch >"$TEST_ROOT/rollback-failure.out" 2>&1; then
  echo 'FAIL: rollback failure unexpectedly claimed convergence' >&2
  exit 1
fi
FAKE_CRONTAB_INSTALL_MODE=normal
FAKE_CRONTAB_ROLLBACK_FAIL=false
if ! cmp -s "$TEST_ROOT/target-before-rollback-failure" "$ROOT/bin/channelops-soak-watch.sh"; then
  echo 'FAIL: cron rollback failure prevented watcher rollback' >&2
  exit 1
fi
if ! grep -Fq 'rollback failed' "$TEST_ROOT/rollback-failure.out"; then
  echo 'FAIL: rollback failure was not reported' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=signal-test@example.com
11 4 * * * /usr/local/bin/prior-signal-job
EOF
printf '#!/usr/bin/env bash\nprintf "signal-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-signal"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-signal"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
trap ':' HUP
trap ':' INT
trap ':' TERM
parent_hup_trap="$(trap -p HUP)"
parent_int_trap="$(trap -p INT)"
parent_term_trap="$(trap -p TERM)"
FAKE_CRONTAB_INSTALL_MODE=signal-term
set +e
vp_install_soak_watch >"$TEST_ROOT/signal-term.out" 2>&1
signal_status=$?
set -e
FAKE_CRONTAB_INSTALL_MODE=normal
if [[ "$signal_status" -ne 143 ]]; then
  echo "FAIL: TERM-interrupted installer returned $signal_status instead of 143" >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/target-before-signal" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-signal" "$FAKE_CRONTAB"; then
  echo 'FAIL: TERM-interrupted installer did not restore prior artifacts' >&2
  exit 1
fi
if [[ "$(trap -p HUP)" != "$parent_hup_trap" \
  || "$(trap -p INT)" != "$parent_int_trap" \
  || "$(trap -p TERM)" != "$parent_term_trap" ]]; then
  echo 'FAIL: installer signal handling clobbered a parent trap' >&2
  exit 1
fi
trap - HUP INT TERM
if ! grep -Fq 'interrupted by TERM' "$TEST_ROOT/signal-term.out"; then
  echo 'FAIL: TERM interruption was not reported' >&2
  exit 1
fi

cat >"$FAKE_CRONTAB" <<EOF
MAILTO=malformed-marker@example.com
# BEGIN VIDEOPROCESS SOAK WATCH
3 * * * * /usr/local/bin/unrelated-inside-malformed-block
EOF
printf '#!/usr/bin/env bash\nprintf "malformed-prior\\n"\n' \
  >"$ROOT/bin/channelops-soak-watch.sh"
chmod 0755 "$ROOT/bin/channelops-soak-watch.sh"
cp "$ROOT/bin/channelops-soak-watch.sh" "$TEST_ROOT/target-before-malformed-marker"
cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-malformed-marker"
rm -f "$FAKE_CRONTAB_FAILURE_USED"
if vp_install_soak_watch >"$TEST_ROOT/malformed-marker.out" 2>&1; then
  echo 'FAIL: malformed managed markers unexpectedly allowed convergence' >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/target-before-malformed-marker" "$ROOT/bin/channelops-soak-watch.sh" \
  || ! cmp -s "$TEST_ROOT/cron-before-malformed-marker" "$FAKE_CRONTAB"; then
  echo 'FAIL: malformed managed markers changed the watcher or crontab' >&2
  exit 1
fi

cp "$TEST_ROOT/cron-before-no-crontab-test" "$FAKE_CRONTAB"
install -m 0755 "$VP_SOAK_WATCH_SOURCE" "$ROOT/bin/channelops-soak-watch.sh"
if [[ -n "$(find "$ROOT/bin" -maxdepth 1 -name '.channelops-soak-watch.txn.*' -print -quit)" ]]; then
  echo 'FAIL: transaction failure leaked a watcher staging directory' >&2
  exit 1
fi

cp "$FAKE_CRONTAB" "$TEST_ROOT/cron-before-skips"
cron_writes_before="$(wc -l <"$FAKE_CRONTAB_CALLS" | tr -d ' ')"
UPDATE_SERVICES=0
if ! deploy_vp_app_services $images >/dev/null; then
  echo 'FAIL: UPDATE_SERVICES=0 deployment returned non-zero' >&2
  exit 1
fi
UPDATE_SERVICES=1
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: UPDATE_SERVICES=0 rewrote the crontab' >&2
  exit 1
fi

FAIL_RUNNING_SERVICE=vp-api-swarm
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: failed service convergence unexpectedly succeeded' >&2
  exit 1
fi
FAIL_RUNNING_SERVICE=
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: failed service convergence rewrote the crontab' >&2
  exit 1
fi

printf 'if then\n' >"$TEST_ROOT/invalid-channelops-soak-watch.sh"
VP_SOAK_WATCH_SOURCE="$TEST_ROOT/invalid-channelops-soak-watch.sh"
if deploy_vp_app_services $images >/dev/null 2>&1; then
  echo 'FAIL: invalid watcher syntax unexpectedly allowed deployment' >&2
  exit 1
fi
VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: invalid watcher syntax rewrote the crontab' >&2
  exit 1
fi

TMPDIR="$TEST_ROOT/tmp"
mkdir -p "$TMPDIR"
FAIL_MANAGED_CRON_PRINTF=true
if vp_install_soak_watch >/dev/null 2>&1; then
  echo 'FAIL: managed cron rendering failure unexpectedly succeeded' >&2
  exit 1
fi
FAIL_MANAGED_CRON_PRINTF=false
if ! cmp -s "$TEST_ROOT/cron-before-skips" "$FAKE_CRONTAB"; then
  echo 'FAIL: managed cron rendering failure rewrote the crontab' >&2
  exit 1
fi
if [[ -n "$(find "$TMPDIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo 'FAIL: managed cron rendering failure leaked a temporary path' >&2
  exit 1
fi
cron_writes_after="$(wc -l <"$FAKE_CRONTAB_CALLS" | tr -d ' ')"
if [[ "$cron_writes_after" -ne "$cron_writes_before" ]]; then
  echo 'FAIL: skipped or failed deployment called crontab with an install file' >&2
  exit 1
fi

grep -Fq 'build|10.0.0.127|/Users/wenjieliu/VideoProcess-app|backend/Dockerfile.ffmpeg-worker-go|vp-ffmpeg-worker-go:deploy-0123456789ab' "$CALLS"
grep -Fq 'docker|build -f /home/taiwei/deploy-github-sync/repos/videoprocess/backend/Dockerfile.worker -t vp-ffmpeg-worker-python:deploy-0123456789ab /home/taiwei/deploy-github-sync/repos/videoprocess/backend' "$CALLS"
if grep -Fq 'build|10.0.0.150|' "$CALLS"; then
  echo 'FAIL: manager-local images must use the manager Docker CLI directly' >&2
  exit 1
fi
grep -Fq 'node.labels.vp.runtime==true' "$CALLS"
grep -Fq 'node.labels.vp.gpu==true' "$CALLS"
grep -Fq 'health|vp-api|http://10.0.0.127:18080/health' "$CALLS"
grep -Fq 'health|vp-frontend|http://10.0.0.127:3001/' "$CALLS"
grep -Fq 'vp-autoflow-api-swarm' "$CALLS"
grep -Fq 'vp-ffmpeg-worker-go-swarm' "$CALLS"
grep -Fq 'vp-ffmpeg-worker-gpu-swarm' "$CALLS"
if ! grep -Fq 'vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: deployment must include the dedicated YouTube publisher' >&2
  exit 1
fi
grep -Fq -- '--constraint-rm node.labels.role==app' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--env-rm DATABASE_URL' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_ENABLED=true' "$CALLS"
grep -Fq -- '--env-add VP_GO_ORCHESTRATOR_JOB_WRITES=true' "$CALLS"
grep -Fq -- '--env-add VP_PYTHON_SCHEDULE_URL=http://vp-autoflow-api-swarm:8080' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=colima-127' "$CALLS"
if [[ "$VP_APP_SERVICES" != 'vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm vp-ffmpeg-worker-gpu-swarm vp-youtube-publisher-swarm' ]]; then
  echo 'FAIL: discovery deployment must not add a VP service' >&2
  exit 1
fi
grep -Fq -- '--env-rm YOUTUBE_CREDENTIALS_DIR' "$CALLS"
grep -Fq 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS"
grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--env-add WORKER_TYPE=youtube_publisher' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=150-publisher' "$CALLS"
grep -Fq -- '--env-add YOUTUBE_MANAGER_URL=http://10.0.0.150:18999' "$CALLS"
grep -Fq -- '--env-add YOUTUBE_PUBLISH_ENABLED=true' "$CALLS"
grep -Fq -- '--env-add PUBLIC_PUBLISH_ENABLED=false' "$CALLS"
grep -Fq -- '--env-add WORKER_CONCURRENCY=1' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
grep -Fq -- '--mount-rm /data/storage' "$CALLS"
grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
publisher_mount_remove_line="$(grep -nF 'docker|service update' "$CALLS" \
  | grep -F 'vp-youtube-publisher-swarm' \
  | grep -F -- '--mount-rm /data/storage' \
  | head -n 1 \
  | cut -d: -f1)"
publisher_mount_add_line="$(grep -nF 'docker|service update' "$CALLS" \
  | grep -F 'vp-youtube-publisher-swarm' \
  | grep -F -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' \
  | head -n 1 \
  | cut -d: -f1)"
if [[ -z "$publisher_mount_remove_line" || -z "$publisher_mount_add_line" \
  || "$publisher_mount_remove_line" -ge "$publisher_mount_add_line" ]]; then
  echo 'FAIL: publisher scratch replacement must remove and add the target in separate ordered updates' >&2
  exit 1
fi
publisher_mount_remove_call="$(sed -n "${publisher_mount_remove_line}p" "$CALLS")"
if ! grep -Fq -- '--replicas 0' <<<"$publisher_mount_remove_call" \
  || grep -Fq -- '--mount-add ' <<<"$publisher_mount_remove_call" \
  || grep -Fq -- '--image ' <<<"$publisher_mount_remove_call"; then
  echo 'FAIL: publisher scratch removal must first stop the publisher without applying the release' >&2
  exit 1
fi
publisher_mount_add_call="$(sed -n "${publisher_mount_add_line}p" "$CALLS")"
if grep -Fq -- '--mount-rm /data/storage' <<<"$publisher_mount_add_call" \
  || ! grep -Fq -- '--replicas 1' <<<"$publisher_mount_add_call" \
  || ! grep -Fq -- '--image ' <<<"$publisher_mount_add_call"; then
  echo 'FAIL: publisher release update must add scratch without removing the same target again' >&2
  exit 1
fi
grep -Fq -- '--mount-rm /app/cache' "$CALLS"
grep -Fq -- '--mount-rm /APP/OAUTH' "$CALLS"
grep -Fq -- '--secret-rm publisher-credential-reference' "$CALLS"
grep -Fq -- '--config-rm publisher-config-reference' "$CALLS"
for publisher_env in \
  YOUTUBE_CREDENTIALS_DIR \
  YOUTUBE_CREDENTIALS_JSON \
  YOUTUBE_LEGACY_MODE \
  GOOGLE_CLIENT_SECRETS_FILE \
  YOUTUBE_REFRESH_TOKEN; do
  grep -Fq -- "--env-rm $publisher_env" "$CALLS"
  if grep -Fq -- "--env-add $publisher_env=" "$CALLS"; then
    echo "FAIL: publisher deploy re-added removed credential environment $publisher_env" >&2
    exit 1
  fi
done
grep -Fq -- '--env-add MINIO_SECRET_KEY=' "$CALLS"

publisher_calls="$(grep -F 'vp-youtube-publisher-swarm' "$CALLS" || true)"
if printf '%s\n' "$publisher_calls" | grep -Eq -- 'YOUTUBE_(OAUTH|CLIENT|CREDENTIALS|TOKEN|REFRESH)_[A-Z_]*='; then
  echo 'FAIL: publisher deploy must not add OAuth credential environments' >&2
  exit 1
fi
if printf '%s\n' "$publisher_calls" | grep -Eq -- '--mount(-add)? .*youtube_credentials'; then
  echo 'FAIL: publisher deploy must not add a credentials mount' >&2
  exit 1
fi

publisher_health_line="$(grep -nF 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS" | head -n 1 | cut -d: -f1)"
publisher_update_line="$(grep -nF 'vp-youtube-publisher-swarm' "$CALLS" | grep -F 'docker|service update' | head -n 1 | cut -d: -f1)"
if [[ -z "$publisher_health_line" || -z "$publisher_update_line" || "$publisher_health_line" -ge "$publisher_update_line" ]]; then
  echo 'FAIL: publisher manager auth health must precede publisher updates' >&2
  exit 1
fi

if (
  unset VP_API_DATABASE_URL_GO VP_PYTHON_WORKER_DATABASE_URL \
    VP_MINIO_ACCESS_KEY VP_MINIO_SECRET_KEY
  deploy_vp_app_services $images >/dev/null 2>&1
); then
  echo 'FAIL: production deployment must fail closed when secrets are missing' >&2
  exit 1
fi

CONSTRAINT_MODE=runtime
if ! vp_update_runtime_service vp-frontend-swarm vp-frontend:repeat-test start-first \
  >/dev/null 2>>"$CALLS"; then
  echo 'FAIL: repeat runtime update returned non-zero' >&2
  exit 1
fi
if grep -Fq 'unbound variable' "$CALLS"; then
  echo 'FAIL: repeat runtime update is not compatible with Bash 3.2 set -u' >&2
  exit 1
fi
grep -Fq 'docker|service update --detach=false --no-resolve-image --update-order start-first --image vp-frontend:repeat-test vp-frontend-swarm' "$CALLS"
CONSTRAINT_MODE=legacy

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
if ! vp_deploy_publisher vp-ffmpeg-worker-python:publisher-repeat-test >/dev/null 2>>"$CALLS"; then
  echo 'FAIL: repeat publisher update returned non-zero' >&2
  exit 1
fi
if grep -Fq 'unbound variable' "$CALLS"; then
  echo 'FAIL: repeat publisher update is not compatible with Bash 3.2 set -u' >&2
  exit 1
fi
grep -Fq 'docker|service update --detach=false --no-resolve-image --update-order stop-first' "$CALLS"
grep -Fq -- '--image vp-ffmpeg-worker-python:publisher-repeat-test vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"
if grep -Fq -- '--constraint-add node.labels.vp.publisher==true' "$CALLS" \
  || grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS" \
  || grep -Fq -- '--network-add vp-pipeline-net' "$CALLS" \
  || grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS" \
  || grep -Fq -- '--mount-rm ' "$CALLS"; then
  echo 'FAIL: repeat publisher update must not change the exact desired mount set' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_NETWORK_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials

: >"$CALLS"
FAIL_HEALTH_CHECK=vp-youtube-manager
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-health-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must stop when manager auth health fails' >&2
  exit 1
fi
grep -Fq 'health|vp-youtube-manager|http://10.0.0.150:18999/api/auth/status' "$CALLS"
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS" \
  || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: publisher deploy mutated Swarm after manager auth health failed' >&2
  exit 1
fi
FAIL_HEALTH_CHECK=

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_LIST_FAILURE=true
if deploy_vp_app_services \
  vp-api:publisher-list-daemon-test \
  vp-frontend:publisher-list-daemon-test \
  vp-backend-api:publisher-list-daemon-test \
  vp-channelops-runner-go:publisher-list-daemon-test \
  vp-ffmpeg-worker-go:publisher-list-daemon-test \
  vp-ffmpeg-worker-python:publisher-list-daemon-test >/dev/null 2>&1; then
  echo 'FAIL: publisher list daemon failure unexpectedly allowed deployment' >&2
  exit 1
fi
if ! grep -Fq 'docker|service ls --filter name=vp-youtube-publisher-swarm --format {{.Name}}' "$CALLS"; then
  echo 'FAIL: optional publisher snapshot must use an exact service list probe' >&2
  exit 1
fi
if grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS" \
  || grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: publisher list daemon failure must not omit, create, or delete the existing publisher' >&2
  exit 1
fi
PUBLISHER_LIST_FAILURE=false

: >"$CALLS"
PUBLISHER_LIST_NAME=vp-youtube-publisher-swarm-stale
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-list-name-test >/dev/null 2>&1; then
  echo 'FAIL: non-exact publisher list result unexpectedly selected a service state' >&2
  exit 1
fi
if grep -Fq 'docker|service update' "$CALLS" || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: non-exact publisher list result must not update or create a publisher' >&2
  exit 1
fi
PUBLISHER_LIST_NAME=

: >"$CALLS"
FAIL_PUBLISHER_INSPECT_FORMAT=ContainerSpec.Configs
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-config-inspect-test >/dev/null 2>&1; then
  echo 'FAIL: publisher config inspection failure unexpectedly allowed deployment' >&2
  exit 1
fi
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after config inspection failure' >&2
  exit 1
fi
FAIL_PUBLISHER_INSPECT_FORMAT=

: >"$CALLS"
GPU_SERVICE_EXISTS=false
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_LIST_FAILURE=true
if vp_restore_app_snapshots "" >/dev/null 2>&1; then
  echo 'FAIL: publisher rollback removal accepted a list daemon failure' >&2
  exit 1
fi
if grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS"; then
  echo 'FAIL: publisher rollback removal deleted a publisher after list daemon failure' >&2
  exit 1
fi
PUBLISHER_LIST_FAILURE=false
GPU_SERVICE_EXISTS=true

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
FAIL_NODE_UPDATE=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-node-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when manager label update fails' >&2
  exit 1
fi
grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS"
if grep -Fq 'docker|service update' "$CALLS" || grep -Fq 'docker|service create' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after manager label update failure' >&2
  exit 1
fi
FAIL_NODE_UPDATE=false

: >"$CALLS"
FAIL_NETWORK_INSPECT=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-network-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when network inspection fails' >&2
  exit 1
fi
grep -Fq 'docker|network inspect vp-pipeline-net --format {{.ID}}' "$CALLS"
if grep -Fq 'docker|node update --label-add vp.publisher=true ccttww-lap' "$CALLS" \
  || grep -Fq 'docker|service update' "$CALLS"; then
  echo 'FAIL: publisher deploy continued after pipeline network inspection failure' >&2
  exit 1
fi
FAIL_NETWORK_INSPECT=false

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
FAIL_PUBLISHER_CREATE=true
if vp_deploy_publisher vp-ffmpeg-worker-python:publisher-create-failure-test >/dev/null 2>&1; then
  echo 'FAIL: publisher deploy must return non-zero when service creation fails' >&2
  exit 1
fi
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
FAIL_PUBLISHER_CREATE=false

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_REPLICAS=3
FAIL_UPDATE_SERVICE=vp-youtube-publisher-swarm
FAIL_UPDATE_IMAGE=vp-ffmpeg-worker-python:publisher-update-failure-test
if deploy_vp_app_services \
  vp-api:publisher-update-failure-test \
  vp-frontend:publisher-update-failure-test \
  vp-backend-api:publisher-update-failure-test \
  vp-channelops-runner-go:publisher-update-failure-test \
  vp-ffmpeg-worker-go:publisher-update-failure-test \
  vp-ffmpeg-worker-python:publisher-update-failure-test >/dev/null 2>&1; then
  echo 'FAIL: failed publisher update with an old running service unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-youtube-publisher-swarm:stable vp-youtube-publisher-swarm' "$CALLS"
if [[ "$PUBLISHER_MOUNT_MODE" != desired || "$PUBLISHER_REPLICAS" -ne 1 ]]; then
  echo 'FAIL: publisher rollback did not recover one replica with the desired scratch mount' >&2
  exit 1
fi
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

VP_GPU_RUNTIME_READY=true
GPU_PREFLIGHT_SUCCEEDS=false
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-preflight-test >/dev/null 2>&1; then
  echo 'FAIL: requested GPU mode must fail when the runtime preflight fails' >&2
  exit 1
fi
grep -Fq 'docker|run --rm --gpus all vp-ffmpeg-worker-python:gpu-preflight-test nvidia-smi' "$CALLS"
GPU_PREFLIGHT_SUCCEEDS=true
if vp_deploy_python_worker vp-ffmpeg-worker-python:gpu-swarm-allocation-test \
  >/dev/null 2>&1; then
  echo 'FAIL: GPU mode must remain disabled until Swarm task allocation is configured' >&2
  exit 1
fi
VP_GPU_RUNTIME_READY=false

: >"$CALLS"
GPU_SERVICE_EXISTS=true
FAIL_UPDATE_SERVICE=vp-channel-agent-runner-swarm
FAIL_UPDATE_IMAGE=vp-channelops-runner-go:rollback-test
if deploy_vp_app_services \
  vp-api:rollback-test \
  vp-frontend:rollback-test \
  vp-backend-api:rollback-test \
  vp-channelops-runner-go:rollback-test \
  vp-ffmpeg-worker-go:rollback-test \
  vp-ffmpeg-worker-python:rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected service update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-api-swarm:stable vp-api-swarm' "$CALLS"
grep -Fq -- '--image baseline-vp-channel-agent-runner-swarm:stable vp-channel-agent-runner-swarm' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
if grep -Fq 'docker|service rollback' "$CALLS"; then
  echo 'FAIL: VP rollback must not restore the legacy service specification' >&2
  exit 1
fi
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: rollback must not target host 126' >&2
  exit 1
fi

: >"$CALLS"
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=
GPU_SERVICE_EXISTS=false
FAIL_RUNNING_SERVICE=vp-ffmpeg-worker-gpu-swarm
if deploy_vp_app_services \
  vp-api:create-rollback-test \
  vp-frontend:create-rollback-test \
  vp-backend-api:create-rollback-test \
  vp-channelops-runner-go:create-rollback-test \
  vp-ffmpeg-worker-go:create-rollback-test \
  vp-ffmpeg-worker-python:create-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected new-worker health failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'docker|service rm vp-ffmpeg-worker-gpu-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=true
FAIL_RUNNING_SERVICE=vp-youtube-publisher-swarm
if deploy_vp_app_services \
  vp-api:publisher-rollback-test \
  vp-frontend:publisher-rollback-test \
  vp-backend-api:publisher-rollback-test \
  vp-channelops-runner-go:publisher-rollback-test \
  vp-ffmpeg-worker-go:publisher-rollback-test \
  vp-ffmpeg-worker-python:publisher-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: existing publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-youtube-publisher-swarm:stable vp-youtube-publisher-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
GPU_SERVICE_EXISTS=true
PUBLISHER_SERVICE_EXISTS=false
FAIL_RUNNING_SERVICE=vp-youtube-publisher-swarm
if deploy_vp_app_services \
  vp-api:publisher-create-rollback-test \
  vp-frontend:publisher-create-rollback-test \
  vp-backend-api:publisher-create-rollback-test \
  vp-channelops-runner-go:publisher-create-rollback-test \
  vp-ffmpeg-worker-go:publisher-create-rollback-test \
  vp-ffmpeg-worker-python:publisher-create-rollback-test >/dev/null 2>&1; then
  echo 'FAIL: new publisher convergence failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq 'docker|service rm vp-youtube-publisher-swarm' "$CALLS"
FAIL_RUNNING_SERVICE=

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=true
PUBLISHER_CONSTRAINT_MODE=stale
PUBLISHER_NETWORK_MODE=pipeline
PUBLISHER_MOUNT_MODE=desired
PUBLISHER_ENV_MODE=desired
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-placement-test >/dev/null
grep -Fq -- '--constraint-rm node.labels.vp.runtime==true' "$CALLS"
grep -Fq -- '--constraint-rm node.labels.vp.gpu==true' "$CALLS"
grep -Fq -- '--constraint-rm node.hostname==colima-swarmbridged' "$CALLS"
grep -Fq -- '--constraint-rm node.labels.vp.legacy==true' "$CALLS"
grep -Fq -- '--constraint-add node.hostname==ccttww-lap' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: publisher deployment must never target 126' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy

: >"$CALLS"
PUBLISHER_CONSTRAINT_MODE=publisher
PUBLISHER_MOUNT_MODE=missing
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-missing-scratch-test >/dev/null
grep -Fq -- '--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
if grep -Fq -- '--mount-rm /data/storage' "$CALLS"; then
  echo 'FAIL: publisher deploy removed an absent scratch target' >&2
  exit 1
fi
PUBLISHER_CONSTRAINT_MODE=legacy
PUBLISHER_MOUNT_MODE=wrong
PUBLISHER_ENV_MODE=credentials

: >"$CALLS"
GPU_SERVICE_EXISTS=true
FAIL_UPDATE_SERVICE=vp-pds-swarm
FAIL_UPDATE_IMAGE=vp-pds:rollback-test
if deploy_pds_services vp-pds:rollback-test >/dev/null 2>&1; then
  echo 'FAIL: injected PDS update failure unexpectedly succeeded' >&2
  exit 1
fi
grep -Fq -- '--image baseline-vp-pds-swarm:stable vp-pds-swarm' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.runtime==true' "$CALLS"
FAIL_UPDATE_SERVICE=
FAIL_UPDATE_IMAGE=

GPU_SERVICE_EXISTS=false
vp_deploy_python_worker vp-ffmpeg-worker-python:deploy-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-ffmpeg-worker-gpu-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.gpu==true' "$CALLS"
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: 126 must not be in VP deploy calls' >&2
  exit 1
fi

: >"$CALLS"
PUBLISHER_SERVICE_EXISTS=false
vp_deploy_publisher vp-ffmpeg-worker-python:publisher-create-test >/dev/null
grep -Fq 'docker|service create --detach=false --name vp-youtube-publisher-swarm' "$CALLS"
grep -Fq -- '--constraint node.labels.vp.publisher==true' "$CALLS"
grep -Fq -- '--constraint node.hostname==ccttww-lap' "$CALLS"
grep -Fq -- '--network vp-pipeline-net' "$CALLS"
grep -Fq -- '--mount type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage' "$CALLS"
grep -Fq -- '--replicas 1' "$CALLS"

: >"$CALLS"
vp_update_runtime_service vp-channel-agent-runner-swarm vp-channelops-runner-go:discovery-timeout-test start-first >/dev/null
timeout_calls="$(grep -F -- 'CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120' "$CALLS" || true)"
if [[ "$(printf '%s\n' "$timeout_calls" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1 \
  || "$timeout_calls" != *"vp-channel-agent-runner-swarm"* ]]; then
  echo 'FAIL: discovery timeout must be added exactly once to the Go runner' >&2
  exit 1
fi
if printf '%s\n' "$timeout_calls" | grep -Eq 'vp-(youtube-publisher|ffmpeg-worker|api|frontend|autoflow-api|event-outbox-relay)-swarm'; then
  echo 'FAIL: discovery timeout must not be added to another VP service' >&2
  exit 1
fi
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: discovery timeout deployment must never target 126' >&2
  exit 1
fi
