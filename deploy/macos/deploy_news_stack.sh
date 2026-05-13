#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [mac3]

Deploy the offloaded embedding/news stack to the Mac 3 service node.

This is a repo-local implementation script.
For normal cluster-wide deploys, prefer:
  /home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh news

Targets:
  mac3  Deploy embedding-gateway, news-server, and news-collector ($MAC3_TARGET)
EOF
}

select_news_target() {
  local selection="${1:-mac3}"
  case "$selection" in
    mac3|magi1|"$MAC3_TARGET"|"")
      printf '%s\n' "$MAC3_TARGET"
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

install_news_stack() {
  local target="$1"
  local news_database_url="${NEWS_DATABASE_URL:-postgresql://news:newspass@$MAIN_HOST:$MAIN_SHARED_POSTGRES_PORT/news}"
  local qdrant_host="${NEWS_QDRANT_HOST:-$MAIN_HOST}"
  local qdrant_port="${NEWS_QDRANT_PORT:-$MAIN_QDRANT_PORT}"
  local llm_base_url="${NEWS_LLM_BASE_URL:-http://$MAIN_HOST:$MAIN_WATCHDOG_PORT/v1}"
  local watchdog_url="${NEWS_WATCHDOG_URL:-http://$MAIN_HOST:$MAIN_WATCHDOG_PORT}"
  log_section "install_news_stack $target"
  rsync_push "$INFRA_ROOT/embedding-gateway/" "$target" "~/Constructure/embedding-gateway/"
  rsync_push "$CONSTRUCTURE_ROOT/apps/news/collector/" "$target" "~/Constructure/news/collector/"
  rsync_push "$CONSTRUCTURE_ROOT/apps/news/server/" "$target" "~/Constructure/news/server/"
  rsync_push "$CONSTRUCTURE_ROOT/apps/news/config/" "$target" "~/Constructure/news/config/"
  ssh_run "$target" "bash -lc '
    set -euo pipefail
    export PATH=\$HOME/.local/bin:\$PATH
    mkdir -p ~/Constructure/services/embedding-gateway ~/Constructure/services/news-collector ~/Constructure/services/news-server ~/Constructure/news/config
    cd ~/Constructure/embedding-gateway
    if [ ! -d .venv ]; then
      uv venv --python 3.12 .venv
    fi
    uv pip install --python .venv/bin/python -r requirements.txt
    cd ~/Constructure/news/collector
    if [ ! -d .venv ]; then
      uv venv --python 3.12 .venv
    fi
    uv pip install --python .venv/bin/python -r requirements.txt
    cd ~/Constructure/news/server
    if [ ! -d .venv ]; then
      uv venv --python 3.12 .venv
    fi
    uv pip install --python .venv/bin/python -r requirements.txt
    cat > ~/Constructure/services/embedding-gateway/env <<EOF
EMBED_MODEL=all-MiniLM-L6-v2
EMBED_DEVICE=cpu
    PATH=\$HOME/.local/bin:\$PATH
EOF
    cat > ~/Constructure/services/news-collector/env <<EOF
DEPLOY_MODE=shared
DATABASE_URL=$news_database_url
FETCH_INTERVAL=900
FETCH_CONCURRENCY=8
CONTENT_CONCURRENCY=4
QDRANT_HOST=$qdrant_host
QDRANT_PORT=$qdrant_port
QDRANT_BATCH_SIZE=25
QDRANT_TIMEOUT_SECONDS=120
QDRANT_UPSERT_RETRIES=3
QDRANT_RETRY_BACKOFF_SECONDS=2
QDRANT_UPSERT_WAIT=false
CHUNK_SIZE=512
RETENTION_DAYS=30
EMBED_DEVICE=cpu
EMBEDDING_GATEWAY_URL=http://127.0.0.1:8080
LLM_BASE_URL=$llm_base_url
EXO_WATCHDOG_URL=$watchdog_url
FEED_URLS_FILE=\$HOME/Constructure/news/config/feed-urls.txt
PATH=\$HOME/.local/bin:\$PATH
EOF
    cat > ~/Constructure/services/news-server/env <<EOF
DEPLOY_MODE=shared
DATABASE_URL=$news_database_url
PORT=6551
QDRANT_HOST=$qdrant_host
QDRANT_PORT=$qdrant_port
EMBED_DEVICE=cpu
EMBEDDING_GATEWAY_URL=http://127.0.0.1:8080
PATH=\$HOME/.local/bin:\$PATH
EOF
    cat > ~/Constructure/services/embedding-gateway/start.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid") 2>/dev/null; then
  exit 0
fi
set -a
source "\$HOME/Constructure/services/embedding-gateway/env"
set +a
cd "\$HOME/Constructure/embedding-gateway"
nohup "\$HOME/Constructure/embedding-gateway/.venv/bin/uvicorn" app:app --host 0.0.0.0 --port 8080 \
  >> "\$HOME/Library/Logs/constructure/embedding-gateway.log" \
  2>> "\$HOME/Library/Logs/constructure/embedding-gateway.err.log" < /dev/null &
echo \$! > "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid"
EOF
    cat > ~/Constructure/services/embedding-gateway/stop.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid" ]; then
  kill \$(cat "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid") 2>/dev/null || true
  rm -f "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid"
fi
pkill -f "uvicorn app:app --host 0.0.0.0 --port 8080" 2>/dev/null || true
EOF
    cat > ~/Constructure/services/embedding-gateway/status.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid") 2>/dev/null; then
  echo running:\$(cat "\$HOME/Constructure/services/embedding-gateway/embedding-gateway.pid")
else
  echo stopped
  exit 1
fi
EOF
    cat > ~/Constructure/services/news-collector/start.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-collector/news-collector.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/news-collector/news-collector.pid") 2>/dev/null; then
  exit 0
fi
set -a
source "\$HOME/Constructure/services/news-collector/env"
set +a
cd "\$HOME/Constructure/news/collector/src"
nohup "\$HOME/Constructure/news/collector/.venv/bin/python" main.py \
  >> "\$HOME/Library/Logs/constructure/news-collector.log" \
  2>> "\$HOME/Library/Logs/constructure/news-collector.err.log" < /dev/null &
echo \$! > "\$HOME/Constructure/services/news-collector/news-collector.pid"
EOF
    cat > ~/Constructure/services/news-collector/stop.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-collector/news-collector.pid" ]; then
  kill \$(cat "\$HOME/Constructure/services/news-collector/news-collector.pid") 2>/dev/null || true
  rm -f "\$HOME/Constructure/services/news-collector/news-collector.pid"
fi
pkill -f "Constructure/news/collector/.venv/bin/python.*main.py" 2>/dev/null || true
EOF
    cat > ~/Constructure/services/news-collector/status.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-collector/news-collector.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/news-collector/news-collector.pid") 2>/dev/null; then
  echo running:\$(cat "\$HOME/Constructure/services/news-collector/news-collector.pid")
else
  echo stopped
  exit 1
fi
EOF
    cat > ~/Constructure/services/news-server/start.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-server/news-server.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/news-server/news-server.pid") 2>/dev/null; then
  exit 0
fi
set -a
source "\$HOME/Constructure/services/news-server/env"
set +a
cd "\$HOME/Constructure/news/server/src"
nohup "\$HOME/Constructure/news/server/.venv/bin/python" main.py \
  >> "\$HOME/Library/Logs/constructure/news-server.log" \
  2>> "\$HOME/Library/Logs/constructure/news-server.err.log" < /dev/null &
echo \$! > "\$HOME/Constructure/services/news-server/news-server.pid"
EOF
    cat > ~/Constructure/services/news-server/stop.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-server/news-server.pid" ]; then
  kill \$(cat "\$HOME/Constructure/services/news-server/news-server.pid") 2>/dev/null || true
  rm -f "\$HOME/Constructure/services/news-server/news-server.pid"
fi
pkill -f "Constructure/news/server/.venv/bin/python.*main.py" 2>/dev/null || true
EOF
    cat > ~/Constructure/services/news-server/status.sh <<\\EOF
#!/usr/bin/env bash
set -euo pipefail
if [ -f "\$HOME/Constructure/services/news-server/news-server.pid" ] && kill -0 \$(cat "\$HOME/Constructure/services/news-server/news-server.pid") 2>/dev/null; then
  echo running:\$(cat "\$HOME/Constructure/services/news-server/news-server.pid")
else
  echo stopped
  exit 1
fi
EOF
    chmod +x ~/Constructure/services/embedding-gateway/start.sh ~/Constructure/services/embedding-gateway/stop.sh ~/Constructure/services/embedding-gateway/status.sh
    chmod +x ~/Constructure/services/news-collector/start.sh ~/Constructure/services/news-collector/stop.sh ~/Constructure/services/news-collector/status.sh
    chmod +x ~/Constructure/services/news-server/start.sh ~/Constructure/services/news-server/stop.sh ~/Constructure/services/news-server/status.sh
    ~/Constructure/services/embedding-gateway/stop.sh
    ~/Constructure/services/news-collector/stop.sh
    ~/Constructure/services/news-server/stop.sh
    ~/Constructure/services/embedding-gateway/start.sh
    ~/Constructure/services/news-collector/start.sh
    ~/Constructure/services/news-server/start.sh
  '"
}

verify_news_stack() {
  local target="$1"
  log_section "verify_news_stack $target"
  ssh_run "$target" "bash -lc '
    \$HOME/Constructure/services/embedding-gateway/status.sh || true
    \$HOME/Constructure/services/news-collector/status.sh || true
    \$HOME/Constructure/services/news-server/status.sh || true
    pgrep -af \"uvicorn app:app --host 0.0.0.0 --port 8080\" || true
    pgrep -af \"Constructure/news/collector/.venv/bin/python.*main.py\" || true
    pgrep -af \"Constructure/news/server/.venv/bin/python.*main.py\" || true
    tail -n 20 \$HOME/Library/Logs/constructure/embedding-gateway.err.log 2>/dev/null || true
    tail -n 20 \$HOME/Library/Logs/constructure/news-collector.err.log 2>/dev/null || true
    tail -n 20 \$HOME/Library/Logs/constructure/news-server.err.log 2>/dev/null || true
  '"
}

main() {
  local selection="${1:-mac3}"
  if [[ "$selection" == "-h" || "$selection" == "--help" || "$selection" == "help" ]]; then
    usage
    exit 0
  fi

  local target
  target="$(select_news_target "$selection")"
  install_user_runtime "$target" false
  install_news_stack "$target"
  verify_news_stack "$target"
}

main "$@"
