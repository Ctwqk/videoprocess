#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALLER="$ROOT_DIR/deploy/macos/install_videoprocess_colima_node.sh"
PLIST="$ROOT_DIR/deploy/macos/com.constructure.vp-colima.plist"

bash -n "$INSTALLER"
plutil -lint "$PLIST" >/dev/null

plan="$(VP_DRY_RUN=true "$INSTALLER" install)"
grep -Fq 'colima start --profile swarmbridged' <<<"$plan"
grep -Fq -- '--hostname colima-127' <<<"$plan"
grep -Fq -- '--network-mode bridged' <<<"$plan"
grep -Fq -- '--network-interface en1' <<<"$plan"
grep -Fq '10.0.0.150:2377' <<<"$plan"
grep -Fq 'vp.runtime=true' <<<"$plan"
if grep -Fq '10.0.0.126' <<<"$plan"; then
  echo 'FAIL: 126 must not appear in the VP node installer' >&2
  exit 1
fi
