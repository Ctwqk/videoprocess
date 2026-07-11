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

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/bin"

cat >"$tmp_dir/bin/colima" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat >"$tmp_dir/bin/ssh" <<'EOF'
#!/usr/bin/env bash
shift
bash -c "$*"
EOF
cat >"$tmp_dir/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '<%s>\n' "$@" >"$VP_TEST_DOCKER_ARGS"
EOF
chmod +x "$tmp_dir/bin/colima" "$tmp_dir/bin/ssh" "$tmp_dir/bin/docker"

VP_TEST_DOCKER_ARGS="$tmp_dir/docker-args" \
  PATH="$tmp_dir/bin:$PATH" \
  "$INSTALLER" status
grep -Fqx '<state={{.Status.State}},labels={{.Spec.Labels}}>' \
  "$tmp_dir/docker-args"
