#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

K8S_NAMESPACE="${K8S_NAMESPACE:-constructure-videoprocess}"
WORKER_DEPLOYMENT="${WORKER_DEPLOYMENT:-videoprocess-worker}"
API_DEPLOYMENT="${API_DEPLOYMENT:-videoprocess-api}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-180s}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Deploy VideoProcess worker code everywhere it currently runs:

- main-host K8s worker
- Mac 1 host-native worker

This is a repo-local convenience script.
For normal cluster-wide deploys, prefer:
  /home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh videoprocess

Options:
  --workers <all|mac1>       Remote Mac worker target subset (default: all)
  --with-api                 Also restart the K8s API deployment
  --with-smoke               Run make smoke-test after deploy
  --skip-compile             Skip python compileall precheck
  -h, --help                 Show this help

Examples:
  $(basename "$0")
  $(basename "$0") --workers mac1
  $(basename "$0") --with-api --with-smoke
EOF
}

compile_check() {
  log_section "compile_check"
  (
    cd "$ROOT_DIR"
    python3 -m compileall backend/app backend/worker
  )
}

restart_k8s_deployment() {
  local deployment="$1"
  log_section "restart_k8s_deployment $deployment"
  kubectl rollout restart "deploy/$deployment" -n "$K8S_NAMESPACE"
  kubectl rollout status "deploy/$deployment" -n "$K8S_NAMESPACE" --timeout="$ROLLOUT_TIMEOUT"
}

verify_k8s_worker() {
  log_section "verify_k8s_worker"
  kubectl get pods -n "$K8S_NAMESPACE" -l "app=$WORKER_DEPLOYMENT" -o wide
}

verify_api_health() {
  log_section "verify_api_health"
  curl -fsS http://localhost:8080/health
}

run_remote_worker_deploy() {
  local selection="$1"
  log_section "run_remote_worker_deploy $selection"
  "$SCRIPT_DIR/deploy_videoprocess_workers.sh" "$selection"
}

run_smoke() {
  log_section "smoke_test"
  (
    cd "$ROOT_DIR"
    make smoke-test
  )
}

main() {
  local worker_selection="all"
  local with_api=false
  local with_smoke=false
  local skip_compile=false

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workers)
        worker_selection="${2:-}"
        if [[ -z "$worker_selection" ]]; then
          echo "--workers requires a value" >&2
          exit 1
        fi
        shift 2
        ;;
      --with-api)
        with_api=true
        shift
        ;;
      --with-smoke)
        with_smoke=true
        shift
        ;;
      --skip-compile)
        skip_compile=true
        shift
        ;;
      -h|--help|help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done

  if [[ "$skip_compile" != true ]]; then
    compile_check
  fi

  restart_k8s_deployment "$WORKER_DEPLOYMENT"
  if [[ "$with_api" == true ]]; then
    restart_k8s_deployment "$API_DEPLOYMENT"
    verify_api_health
  fi

  run_remote_worker_deploy "$worker_selection"
  verify_k8s_worker

  if [[ "$with_smoke" == true ]]; then
    run_smoke
  fi
}

main "$@"
