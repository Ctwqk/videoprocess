#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMON="$ROOT_DIR/deploy/macos/common.sh"
TOPOLOGY="$ROOT_DIR/deploy/four-machine-topology.md"
COLIMA_INSTALLER="$ROOT_DIR/deploy/macos/install_videoprocess_colima_node.sh"
COLIMA_PLIST="$ROOT_DIR/deploy/macos/com.constructure.vp-colima.plist"

bash -n "$COMMON"
bash -n "$COLIMA_INSTALLER"

assert_contains() {
  local needle="$1"
  if ! grep -Fq "$needle" "$COMMON"; then
    printf 'FAIL: expected macOS common.sh to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_not_contains() {
  local needle="$1"
  if grep -Fq "$needle" "$COMMON"; then
    printf 'FAIL: expected macOS common.sh not to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_file_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq -- "$needle" "$file"; then
    printf 'FAIL: expected %s to contain %s\n' "$file" "$needle" >&2
    exit 1
  fi
}

assert_contains 'CONSTRUCTURE_REPOS_DIR="${CONSTRUCTURE_REPOS_DIR:-$(cd "$VIDEO_PROCESS_ROOT/.." && pwd)}"'
assert_contains 'LEGACY_CONSTRUCTURE_ROOT="${LEGACY_CONSTRUCTURE_ROOT:-$HOME/Constructure}"'
assert_contains 'PLATFORM_UPLOAD_ROOT="${PLATFORM_UPLOAD_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-platform-upload}"'
assert_contains 'INFRA_ROOT="${INFRA_ROOT:-$CONSTRUCTURE_REPOS_DIR/constructure-runtime/infra}"'
assert_contains 'MAIN_HOST="${MAIN_HOST:-10.0.0.150}"'
assert_contains 'MAC1_TARGET="${MAC1_TARGET:-wenjieliu@10.0.0.127}"'
assert_contains 'MAC3_TARGET="${MAC3_TARGET:-magi1@10.0.0.126}"'
assert_not_contains 'CONSTRUCTURE_ROOT="$(cd "$VIDEO_PROCESS_ROOT/../.." && pwd)"'
assert_not_contains 'PLATFORM_UPLOAD_ROOT="${PLATFORM_UPLOAD_ROOT:-$CONSTRUCTURE_ROOT/platform-upload}"'
assert_file_contains "$TOPOLOGY" 'node.labels.vp.runtime == true'
assert_file_contains "$TOPOLOGY" '126 is not a VideoProcess automatic failover target'
assert_file_contains "$TOPOLOGY" '--project vp-app --project vp-feature-aggregator'
assert_file_contains "$COLIMA_PLIST" '<key>EnvironmentVariables</key>'
assert_file_contains "$COLIMA_PLIST" '<string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>'
assert_file_contains "$COLIMA_INSTALLER" '/usr/bin/env PATH=$COLIMA_COMMAND_PATH /opt/homebrew/bin/colima start'
assert_file_contains "$COLIMA_INSTALLER" 'launchctl bootout "$launch_domain/$PLIST_LABEL"'

# Exercise the actual remote build scripts with only Docker replaced. The
# account rename must not redirect builds to another node or arbitrary path.
(
  fixture="$(mktemp -d "${TMPDIR:-/tmp}/vp-build-home.XXXXXX")"
  trap 'rm -rf "$fixture"' EXIT
  mkdir -p "$fixture/bin"
  export VP_BUILD_TRACE="$fixture/build-args"
  cat >"$fixture/bin/docker" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$VP_BUILD_TRACE"
EOF
  chmod +x "$fixture/bin/docker"
  export PATH="$fixture/bin:$PATH"
  REPO_ROOT="$fixture/repos"
  source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
  BUILD_IMAGES=1
  log() { :; }
  remote_sh() {
    [[ "$1" == 10.0.0.127 ]] || return 1
    shift
    "$@"
  }
  commit=0123456789abcdef0123456789abcdef01234567
  for deploy_home in /Users/wenjieliu /Users/magi2; do
    VP_MAC_DEPLOY_HOME="$deploy_home"
    for kind in channelops worker; do
      if [[ "$kind" == channelops ]]; then
        dockerfile=backend/Dockerfile.channelops-runner-go
        tag=vp-channelops-runner-go:deploy-${commit:0:12}
      else
        dockerfile=backend/Dockerfile.ffmpeg-worker-go
        tag=vp-ffmpeg-worker-go:deploy-${commit:0:12}
      fi
      "vp_build_runtime_${kind}_image" "$deploy_home/VideoProcess-app" "$dockerfile" "$tag" "$commit"
      grep -Fxq "$deploy_home/VideoProcess-app/$dockerfile" "$VP_BUILD_TRACE"
      [[ "$(tail -n 1 "$VP_BUILD_TRACE")" == "$deploy_home/VideoProcess-app" ]]
      rm "$VP_BUILD_TRACE"
      if VP_RUNTIME_HOST=10.0.0.126 "vp_build_runtime_${kind}_image" "$deploy_home/VideoProcess-app" "$dockerfile" "$tag" "$commit"; then
        echo 'FAIL: renamed build accepted excluded host' >&2; exit 1
      fi
      [[ ! -e "$VP_BUILD_TRACE" ]]
    done
  done
  for deploy_home in /tmp /Users/magi1 /Users/magi2/../wenjieliu; do
    if VP_MAC_DEPLOY_HOME="$deploy_home" vp_build_runtime_channelops_image "$deploy_home/VideoProcess-app" backend/Dockerfile.channelops-runner-go "vp-channelops-runner-go:deploy-${commit:0:12}" "$commit"; then
      echo 'FAIL: build accepted an unqualified home' >&2; exit 1
    fi
    [[ ! -e "$VP_BUILD_TRACE" ]]
  done
)
