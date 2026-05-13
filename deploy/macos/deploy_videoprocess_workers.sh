#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [all|mac1]

Deploy host-native VideoProcess workers to the active remote Mac video node.

This is a repo-local implementation script.
For normal cluster-wide deploys, prefer:
  /home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh videoprocess

Targets:
  all   Deploy Mac 1 worker
  mac1  Deploy only Mac 1 worker ($MAC1_TARGET)
EOF
}

select_worker_targets() {
  local selection="${1:-all}"
  case "$selection" in
    all|"")
      printf '%s\n' "$MAC1_TARGET"
      ;;
    mac1|wenjie|"$MAC1_TARGET")
      printf '%s\n' "$MAC1_TARGET"
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown target: $selection" >&2
      usage >&2
      exit 1
      ;;
  esac
}

install_vp_worker() {
  local target="$1"
  local worker_host=""
  local vp_database_url="${VP_DATABASE_URL:-postgresql+asyncpg://vp:vp_secret@$MAIN_HOST:$MAIN_SHARED_POSTGRES_PORT/videoprocess}"
  local vp_redis_url="${VP_REDIS_URL:-redis://$MAIN_HOST:$MAIN_VP_REDIS_PORT/0}"
  local vp_minio_endpoint="${VP_MINIO_ENDPOINT:-$MAIN_HOST:$MAIN_MINIO_PORT}"
  local vp_llm_base_url="${VP_LLM_BASE_URL:-http://$MAIN_HOST:$MAIN_WATCHDOG_PORT/v1}"
  local vp_xtts_fallback_url="${VP_XTTS_FALLBACK_BASE_URL:-http://$MAIN_HOST:$MAIN_XTTS_PORT}"
  if [ "$target" = "$MAC1_TARGET" ]; then
    worker_host="wenjie"
  else
    worker_host="$(echo "$target" | cut -d@ -f1)"
  fi
  local minimax_api_key=""
  local minimax_api_key_b64=""
  minimax_api_key="$(kubectl get secret minimax-credentials -n constructure-monitor -o jsonpath='{.data.api-key}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [ -n "$minimax_api_key" ]; then
    minimax_api_key_b64="$(printf '%s' "$minimax_api_key" | base64)"
  fi
  log_section "install_vp_worker $target"
  rsync_push "$VIDEO_PROCESS_ROOT/backend/" "$target" "~/Constructure/VideoProcess/backend/"
  rsync_push "$PLATFORM_UPLOAD_ROOT/YouTubeManager/credentials/" "$target" "~/Constructure/VideoProcess/YouTubeManager/credentials/"
  ssh_run "$target" "bash -lc '
    set -euo pipefail
    export PATH=\$HOME/.local/bin:\$PATH
    mkdir -p ~/Constructure/services/vp-worker ~/ConstructureData/vp-worker-storage
    cd ~/Constructure/VideoProcess/backend
    if [ ! -d .venv ]; then
      uv venv --python 3.12 .venv
    fi
    uv pip install --python .venv/bin/python \".[worker]\"
    uv pip install --python .venv/bin/python yt-dlp
    minimax_api_key=\"\"
    if [ -n \"$minimax_api_key_b64\" ]; then
      minimax_api_key=\$(printf %s \"$minimax_api_key_b64\" | base64 -d)
    fi
    cat > ~/Constructure/services/vp-worker/vp-worker.env <<EOF
PYTHONPATH=\$HOME/Constructure/VideoProcess/backend
DEPLOY_MODE=shared
DATABASE_URL=$vp_database_url
REDIS_URL=$vp_redis_url
STORAGE_BACKEND=minio
STORAGE_LOCAL_ROOT=\$HOME/ConstructureData/vp-worker-storage
MINIO_ENDPOINT=$vp_minio_endpoint
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=videoprocess
WORKER_TYPE=ffmpeg
WORKER_HOST=$worker_host
WORKER_CONCURRENCY=1
WORKER_PEL_MIN_IDLE_MS=900000
WORKER_HEARTBEAT_INTERVAL_SECONDS=15
WORKER_AFFINITY_WAIT_SECONDS=20
WORKER_AFFINITY_MAX_BOUNCES=6
VIDEO_USE_GPU=false
VIDEO_USE_VIDEOTOOLBOX=true
VIDEO_WHISPER_DEVICE=cpu
VIDEO_WHISPER_COMPUTE_TYPE=int8
VIDEO_LLM_BASE_URL=$vp_llm_base_url
VIDEO_TTS_BASE_URL=http://127.0.0.1:8010
VIDEO_TTS_FALLBACK_BASE_URL=$vp_xtts_fallback_url
MINIMAX_API_KEY=\$minimax_api_key
VIDEO_MINIMAX_TTS_BASE_URL=https://api.minimaxi.com/v1
VIDEO_MINIMAX_TTS_MODEL=speech-2.8-hd
YOUTUBE_CREDENTIALS_DIR=\$HOME/Constructure/VideoProcess/YouTubeManager/credentials
PATH=\$HOME/Constructure/VideoProcess/backend/.venv/bin:\$HOME/.local/bin:\$PATH
EOF
    cat > ~/Constructure/services/vp-worker/start.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/vp-worker/vp-worker.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/vp-worker/vp-worker.pid") 2>/dev/null; then
  exit 0
fi
set -a
source "\$HOME/Constructure/services/vp-worker/vp-worker.env"
set +a
cd "\$HOME/Constructure/VideoProcess/backend"
nohup "\$HOME/Constructure/VideoProcess/backend/.venv/bin/python" -m worker.main \
  >> "\$HOME/Library/Logs/constructure/vp-worker.log" \
  2>> "\$HOME/Library/Logs/constructure/vp-worker.err.log" < /dev/null &
echo \$! > "\$HOME/Constructure/services/vp-worker/vp-worker.pid"
EOF
    cat > ~/Constructure/services/vp-worker/stop.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/vp-worker/vp-worker.pid" ]; then
  kill \$(cat "\$HOME/Constructure/services/vp-worker/vp-worker.pid") 2>/dev/null || true
  rm -f "\$HOME/Constructure/services/vp-worker/vp-worker.pid"
fi
pkill -f "python.*worker.main" 2>/dev/null || true
EOF
    cat > ~/Constructure/services/vp-worker/status.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/vp-worker/vp-worker.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/vp-worker/vp-worker.pid") 2>/dev/null; then
  echo running:\$(cat "\$HOME/Constructure/services/vp-worker/vp-worker.pid")
else
  echo stopped
  exit 1
fi
EOF
    chmod +x ~/Constructure/services/vp-worker/start.sh ~/Constructure/services/vp-worker/stop.sh ~/Constructure/services/vp-worker/status.sh
    ~/Constructure/services/vp-worker/stop.sh
    ~/Constructure/services/vp-worker/start.sh
  '"
}

verify_vp_worker() {
  local target="$1"
  log_section "verify_vp_worker $target"
  ssh_run "$target" "bash -lc '
    \$HOME/Constructure/services/vp-worker/status.sh || true
    pgrep -af \"python.*worker.main\" || true
    tail -n 20 \$HOME/Library/Logs/constructure/vp-worker.err.log 2>/dev/null || true
  '"
}

main() {
  local selection="${1:-all}"
  if [[ "$selection" == "-h" || "$selection" == "--help" || "$selection" == "help" ]]; then
    usage
    exit 0
  fi

  mapfile -t targets < <(select_worker_targets "$selection")
  for target in "${targets[@]}"; do
    install_user_runtime "$target" true
    install_vp_worker "$target"
    verify_vp_worker "$target"
  done
}

main "$@"
