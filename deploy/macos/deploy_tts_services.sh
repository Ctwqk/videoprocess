#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [all|mac1]

Deploy host-native TextToAudio XTTS services to the active remote Mac video node.

Targets:
  all   Deploy Mac 1 TTS service
  mac1  Deploy only Mac 1 service ($MAC1_TARGET)
EOF
}

select_tts_targets() {
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

install_tts_service() {
  local target="$1"
  log_section "install_tts_service $target"
  rsync_push "$VIDEO_PROCESS_ROOT/TextToAudio/app/" "$target" "~/Constructure/VideoProcess/TextToAudio/app/"
  rsync_push "$VIDEO_PROCESS_ROOT/TextToAudio/voicesource/" "$target" "~/Constructure/VideoProcess/TextToAudio/voicesource/"
  scp -i "$SSH_KEY" \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile="$KNOWN_HOSTS" \
    "$VIDEO_PROCESS_ROOT/TextToAudio/requirements.txt" \
    "$target:~/Constructure/VideoProcess/TextToAudio/requirements.txt" >/dev/null

  ssh_run "$target" "bash -lc '
    set -euo pipefail
    export PATH=\$HOME/.local/bin:\$PATH
    mkdir -p ~/Constructure/VideoProcess/TextToAudio/output \
      ~/Constructure/VideoProcess/TextToAudio/speaker-cache \
      ~/Library/Logs/constructure \
      ~/Constructure/services/tts-service
    uv python install 3.11
    cd ~/Constructure/VideoProcess/TextToAudio
    if [ ! -d .venv-macos ]; then
      uv venv --python 3.11 .venv-macos
    fi
    uv pip install --python .venv-macos/bin/python --upgrade pip setuptools wheel
    uv pip install --python .venv-macos/bin/python torch==2.3.1 torchaudio==2.3.1
    uv pip install --python .venv-macos/bin/python -r requirements.txt
    cat > ~/Constructure/services/tts-service/tts-service.env <<EOF
PYTHONPATH=\$HOME/Constructure/VideoProcess/TextToAudio
USE_GPU=true
TTS_DEVICE=auto
XTTS_MODEL_NAME=tts_models/multilingual/multi-dataset/xtts_v2
OUTPUT_DIR=\$HOME/Constructure/VideoProcess/TextToAudio/output
SPEAKER_CACHE_DIR=\$HOME/Constructure/VideoProcess/TextToAudio/speaker-cache
DEFAULT_SPEAKER_WAV=\$HOME/Constructure/VideoProcess/TextToAudio/voicesource/sample.wav
COQUI_TOS_AGREED=1
PATH=\$HOME/Constructure/VideoProcess/TextToAudio/.venv-macos/bin:\$HOME/.local/bin:\$PATH
EOF
    cat > ~/Constructure/services/tts-service/start.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/tts-service/tts-service.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/tts-service/tts-service.pid") 2>/dev/null; then
  exit 0
fi
set -a
source "\$HOME/Constructure/services/tts-service/tts-service.env"
set +a
cd "\$HOME/Constructure/VideoProcess/TextToAudio"
nohup "\$HOME/Constructure/VideoProcess/TextToAudio/.venv-macos/bin/python" -m uvicorn app.main:app \
  --host 127.0.0.1 --port 8010 \
  >> "\$HOME/Library/Logs/constructure/tts-service.log" \
  2>> "\$HOME/Library/Logs/constructure/tts-service.err.log" < /dev/null &
echo \$! > "\$HOME/Constructure/services/tts-service/tts-service.pid"
EOF
    cat > ~/Constructure/services/tts-service/stop.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/tts-service/tts-service.pid" ]; then
  kill \$(cat "\$HOME/Constructure/services/tts-service/tts-service.pid") 2>/dev/null || true
  rm -f "\$HOME/Constructure/services/tts-service/tts-service.pid"
fi
pkill -f \"uvicorn app.main:app --host 127.0.0.1 --port 8010\" 2>/dev/null || true
EOF
    cat > ~/Constructure/services/tts-service/status.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/tts-service/tts-service.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/tts-service/tts-service.pid") 2>/dev/null; then
  echo running:\$(cat "\$HOME/Constructure/services/tts-service/tts-service.pid")
else
  echo stopped
  exit 1
fi
EOF
    chmod +x ~/Constructure/services/tts-service/start.sh \
      ~/Constructure/services/tts-service/stop.sh \
      ~/Constructure/services/tts-service/status.sh
    ~/Constructure/services/tts-service/stop.sh
    ~/Constructure/services/tts-service/start.sh
  '"
}

verify_tts_service() {
  local target="$1"
  log_section "verify_tts_service $target"
  ssh_run "$target" "bash -lc '
    \$HOME/Constructure/services/tts-service/status.sh || true
    curl -sS --max-time 120 http://127.0.0.1:8010/health || true
    printf \"\\n\"
    tail -n 20 \$HOME/Library/Logs/constructure/tts-service.err.log 2>/dev/null || true
  '"
}

main() {
  local selection="${1:-all}"
  if [[ "$selection" == "-h" || "$selection" == "--help" || "$selection" == "help" ]]; then
    usage
    exit 0
  fi

  mapfile -t targets < <(select_tts_targets "$selection")
  for target in "${targets[@]}"; do
    install_user_runtime "$target" false
    install_tts_service "$target"
    verify_tts_service "$target"
  done
}

main "$@"
