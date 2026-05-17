#!/usr/bin/env bash
set -euo pipefail

MACOS_DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_PROCESS_ROOT="$(cd "$MACOS_DEPLOY_DIR/../.." && pwd)"
CONSTRUCTURE_REPOS_DIR="${CONSTRUCTURE_REPOS_DIR:-$(cd "$VIDEO_PROCESS_ROOT/.." && pwd)}"
LEGACY_CONSTRUCTURE_ROOT="${LEGACY_CONSTRUCTURE_ROOT:-$HOME/Constructure}"

pick_first_existing_dir() {
  local candidate
  for candidate in "$@"; do
    if [ -d "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  printf '%s\n' "${*: -1}"
}

INFRA_ROOT="${INFRA_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-runtime/infra}"
PLATFORM_UPLOAD_ROOT="${PLATFORM_UPLOAD_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-platform-upload}"
K8S_HOME_ROOT="${K8S_HOME_ROOT:-$(pick_first_existing_dir "$HOME/k8s-Constructure" "$LEGACY_CONSTRUCTURE_ROOT")}"
K8S_CONSTRUCTURE_ROOT="${K8S_CONSTRUCTURE_ROOT:-$(pick_first_existing_dir "$K8S_HOME_ROOT/k8s-constructure" "$LEGACY_CONSTRUCTURE_ROOT/k8s-constructure")}"

SSH_KEY="${SSH_KEY:-/home/taiwei/.ssh/id_mini_wenjie}"
KNOWN_HOSTS="${KNOWN_HOSTS:-/tmp/vp_mac_known_hosts}"
MAIN_HOST="${MAIN_HOST:-192.168.20.4}"
MAIN_SHARED_POSTGRES_PORT="${MAIN_SHARED_POSTGRES_PORT:-5435}"
MAIN_VP_REDIS_PORT="${MAIN_VP_REDIS_PORT:-6380}"
MAIN_MINIO_PORT="${MAIN_MINIO_PORT:-9000}"
MAIN_WATCHDOG_PORT="${MAIN_WATCHDOG_PORT:-8000}"
MAIN_XTTS_PORT="${MAIN_XTTS_PORT:-8010}"
MAIN_QDRANT_PORT="${MAIN_QDRANT_PORT:-6333}"

MAC1_TARGET="${MAC1_TARGET:-wenjieliu@10.0.0.127}"
MAC3_TARGET="${MAC3_TARGET:-magi1@10.0.0.126}"

log_section() {
  echo "== $* =="
}

ssh_run() {
  local target="$1"
  shift
  ssh -i "$SSH_KEY" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile="$KNOWN_HOSTS" \
    "$target" "$@"
}

rsync_push() {
  local source="$1"
  local target="$2"
  local dest="$3"
  rsync -az --delete \
    --exclude '.venv/' \
    --exclude '__pycache__/' \
    -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=$KNOWN_HOSTS" \
    "$source" "$target:$dest"
}

install_user_runtime() {
  local target="$1"
  local need_ffmpeg="${2:-false}"
  log_section "install_user_runtime $target (ffmpeg=$need_ffmpeg)"
  ssh_run "$target" "bash -lc '
    set -euo pipefail
    mkdir -p \$HOME/.local/bin \$HOME/Library/LaunchAgents \$HOME/Library/Logs/constructure \$HOME/Constructure \$HOME/ConstructureData
    export PATH=\$HOME/.local/bin:\$PATH
    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH=\$HOME/.local/bin:\$PATH
    fi
    uv python install 3.12
    ffmpeg_ok=false
    if [ \"$need_ffmpeg\" = true ] && command -v ffmpeg >/dev/null 2>&1; then
      if ffmpeg -hide_banner -encoders 2>/dev/null | grep -q videotoolbox; then
        ffmpeg_ok=true
      fi
    fi
    if [ \"$need_ffmpeg\" = true ] && [ \"\$ffmpeg_ok\" != true ]; then
      tmpdir=\$(mktemp -d)
      curl -L --fail --silent --show-error https://www.osxexperts.net/ffmpeg80arm.zip -o \$tmpdir/ffmpeg.zip
      curl -L --fail --silent --show-error https://www.osxexperts.net/ffprobe80arm.zip -o \$tmpdir/ffprobe.zip
      unzip -oq \$tmpdir/ffmpeg.zip -d \$HOME/.local/bin
      unzip -oq \$tmpdir/ffprobe.zip -d \$HOME/.local/bin
      chmod +x \$HOME/.local/bin/ffmpeg \$HOME/.local/bin/ffprobe
      xattr -dr com.apple.quarantine \$HOME/.local/bin/ffmpeg \$HOME/.local/bin/ffprobe >/dev/null 2>&1 || true
      rm -rf \$tmpdir
    fi
    if [ \"$need_ffmpeg\" = true ]; then
      ffmpeg -hide_banner -encoders 2>/dev/null | grep -q videotoolbox
    fi
  '"
}
