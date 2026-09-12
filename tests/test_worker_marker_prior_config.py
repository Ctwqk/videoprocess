from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy/swarm/deploy-sync-extension.sh"
NETWORK_ID = "canonical-network-id"
NETWORK_IDENTITY = f"{NETWORK_ID}|vp-pipeline-net|overlay|swarm"
CONFIG = """GENERATION=m-baseline-1
IMAGE=vp-backend:deploy-baseline
NETWORK=vp-pipeline-net
NETWORK_ID=canonical-network-id
READINESS_DATABASE_SECRET=vp-wrm-readiness-db-m-baseline-1
READINESS_REDIS_SECRET=vp-marker-readiness-redis-baseline
JANITOR_DATABASE_SECRET=vp-wrm-janitor-db-m-baseline-1
JANITOR_REDIS_SECRET=vp-marker-janitor-redis-baseline
"""
READ_CONFIG = r"""
set -euo pipefail
source "$1"
docker() {
  if [[ "$#" != 5 || "$1" != network || "$2" != inspect \
    || "$3" != vp-pipeline-net || "$4" != --format \
    || "$5" != '{{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}' ]]; then
    echo 'unexpected Docker operation' >&2
    return 99
  fi
  [[ "$TEST_DISCOVERY_STATUS" == 0 ]] || return "$TEST_DISCOVERY_STATUS"
  printf '%s\n' "$TEST_NETWORK_IDENTITY"
}
VP_PIPELINE_NETWORK_ID="$3"
VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=stale
VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=stale
VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=stale
VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=stale
status=0
vp_worker_redis_marker_read_prior_config "$2" || status=$?
printf '%s|%s|%s|%s|%s\n' \
  "$VP_PIPELINE_NETWORK_ID" \
  "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
  "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
  "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
  "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET"
exit "$status"
"""


class WorkerMarkerPriorConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = self.root / "control.conf"
        self.config.write_text(CONFIG)
        self.config.chmod(0o600)

    def read_config(self, *, cached_id="", identity=NETWORK_IDENTITY,
                    discovery_status=0):
        return subprocess.run(
            ["bash", "-c", READ_CONFIG, "prior-config-test", str(SCRIPT),
             str(self.config), cached_id],
            env={
                **os.environ,
                "REPO_ROOT": str(ROOT.parent),
                "VP_PIPELINE_NETWORK": "vp-pipeline-net",
                "TEST_NETWORK_IDENTITY": identity,
                "TEST_DISCOVERY_STATUS": str(discovery_status),
            },
            capture_output=True, text=True, timeout=10,
        )

    def assert_rejected(self, result):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(result.stdout.rstrip("\n").split("|")[1:], [""] * 4)

    def test_empty_cached_identity_loads_valid_prior_config(self):
        result = self.read_config()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout,
            "canonical-network-id|m-baseline-1|vp-backend:deploy-baseline|"
            "vp-marker-readiness-redis-baseline|vp-marker-janitor-redis-baseline\n",
        )
        self.assertEqual(result.stderr, "")

    def test_matching_cached_identity_loads_valid_prior_config(self):
        result = self.read_config(cached_id=NETWORK_ID)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split("|")[1], "m-baseline-1")

    def test_wrong_config_network_id_is_rejected(self):
        self.config.write_text(CONFIG.replace(NETWORK_ID, "wrong-network-id"))
        for cached_id in ("", NETWORK_ID, "wrong-network-id"):
            with self.subTest(cached_id=cached_id):
                self.assert_rejected(self.read_config(cached_id=cached_id))

    def test_stale_cached_identity_is_rejected(self):
        self.assert_rejected(self.read_config(cached_id="stale-network-id"))

    def test_discovery_failure_is_rejected_even_with_cached_identity(self):
        for cached_id in ("", NETWORK_ID):
            with self.subTest(cached_id=cached_id):
                result = self.read_config(cached_id=cached_id, discovery_status=1)
                self.assert_rejected(result)
                self.assertIn("required pipeline network is absent", result.stderr)

    def test_noncanonical_live_identity_is_rejected(self):
        for identity in (
            "",
            "|vp-pipeline-net|overlay|swarm",
            f"{NETWORK_ID}|other-network|overlay|swarm",
            f"{NETWORK_ID}|vp-pipeline-net|bridge|swarm",
            f"{NETWORK_ID}|vp-pipeline-net|overlay|local",
            f"{NETWORK_ID}|vp-pipeline-net|overlay|swarm|extra",
            f"{NETWORK_IDENTITY}\n{NETWORK_IDENTITY}",
        ):
            with self.subTest(identity=identity):
                self.assert_rejected(
                    self.read_config(cached_id=NETWORK_ID, identity=identity)
                )

    def test_absent_config_succeeds_without_network_and_clears_prior_state(self):
        self.config.unlink()
        result = self.read_config(discovery_status=1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "||||\n")
        self.assertEqual(result.stderr, "")

    def test_unsafe_config_is_rejected(self):
        for kind in ("mode", "symlink", "directory"):
            with self.subTest(kind=kind):
                path = self.root / kind
                if kind == "mode":
                    path.write_text(CONFIG)
                    path.chmod(0o644)
                elif kind == "symlink":
                    path.symlink_to(self.root / "control.conf")
                else:
                    path.mkdir(mode=0o700)
                self.config = path
                result = self.read_config(cached_id=NETWORK_ID)
                self.assert_rejected(result)
                self.assertIn("active configuration is invalid", result.stderr)

    def test_invalid_config_contents_are_rejected(self):
        for contents in (
            CONFIG + "UNKNOWN=value\n",
            CONFIG + "GENERATION=m-duplicate\n",
            CONFIG.replace("NETWORK=vp-pipeline-net", "NETWORK=other-network"),
            CONFIG.replace(f"NETWORK_ID={NETWORK_ID}\n", ""),
            CONFIG.replace("vp-wrm-readiness-db-m-baseline-1", "wrong-secret"),
            CONFIG.replace("vp-marker-janitor-redis-baseline",
                           "vp-marker-readiness-redis-baseline"),
            CONFIG.replace("vp-backend:deploy-baseline", "10.0.0.126/backend:v1"),
        ):
            with self.subTest(contents=contents):
                self.config.write_text(contents)
                self.assert_rejected(self.read_config(cached_id=NETWORK_ID))


if __name__ == "__main__":
    unittest.main()
