#!/usr/bin/env bash
set -euo pipefail

PROFILE="${VP_COLIMA_PROFILE:-swarmbridged}"
VM_HOSTNAME="${VP_COLIMA_HOSTNAME:-colima-127}"
MANAGER_HOST="${VP_MANAGER_HOST:-10.0.0.150}"
EXPECTED_HOST_IP="${VP_RUNTIME_HOST_IP:-10.0.0.127}"
NETWORK_INTERFACE="${VP_NETWORK_INTERFACE:-en1}"
PLIST_LABEL="com.constructure.vp-colima"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="$SCRIPT_DIR/$PLIST_LABEL.plist"
PLIST_TARGET="$HOME/Library/LaunchAgents/$PLIST_LABEL.plist"

run() {
  if [[ "${VP_DRY_RUN:-false}" == "true" ]]; then
    printf '%q ' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

host_ip() {
  ipconfig getifaddr "$NETWORK_INTERFACE"
}

doctor() {
  command -v colima >/dev/null
  command -v docker >/dev/null
  command -v ssh >/dev/null

  local current
  current="$(host_ip)"
  if [[ "$current" != "$EXPECTED_HOST_IP" ]]; then
    echo "expected $EXPECTED_HOST_IP on $NETWORK_INTERFACE, got $current" >&2
    return 1
  fi
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$MANAGER_HOST" true
}

install_node() {
  if [[ "${VP_DRY_RUN:-false}" != "true" ]]; then
    doctor
  fi

  run colima start --profile "$PROFILE" --cpus 4 --memory 8 --disk 60 \
    --runtime docker --hostname "$VM_HOSTNAME" --vm-type vz \
    --network-address --network-mode bridged \
    --network-interface "$NETWORK_INTERFACE" --port-forwarder ssh

  if [[ "${VP_DRY_RUN:-false}" == "true" ]]; then
    echo "docker --context colima-$PROFILE swarm join $MANAGER_HOST:2377"
    echo "ssh $MANAGER_HOST docker node update --label-add role=app --label-add vp.runtime=true $VM_HOSTNAME"
    echo "install $PLIST_SOURCE $PLIST_TARGET"
    return 0
  fi

  local swarm_state
  swarm_state="$(docker --context "colima-$PROFILE" info --format '{{.Swarm.LocalNodeState}}')"
  if [[ "$swarm_state" == "inactive" ]]; then
    local token
    token="$(ssh "$MANAGER_HOST" docker swarm join-token -q worker)"
    docker --context "colima-$PROFILE" swarm join --token "$token" "$MANAGER_HOST:2377"
  elif [[ "$swarm_state" != "active" ]]; then
    echo "unexpected Swarm state: $swarm_state" >&2
    return 1
  fi

  ssh "$MANAGER_HOST" docker node update \
    --label-add role=app --label-add vp.runtime=true "$VM_HOSTNAME"

  mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs/constructure"
  install -m 0644 "$PLIST_SOURCE" "$PLIST_TARGET"
  if ! launchctl print "gui/$(id -u)/$PLIST_LABEL" >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST_TARGET"
  fi
}

status_node() {
  colima list
  ssh "$MANAGER_HOST" docker node inspect "$VM_HOSTNAME" \
    --format 'state={{.Status.State}},labels={{.Spec.Labels}}'
}

case "${1:-doctor}" in
  doctor)
    doctor
    ;;
  install)
    install_node
    ;;
  status)
    status_node
    ;;
  *)
    echo "usage: $0 {doctor|install|status}" >&2
    exit 2
    ;;
esac
