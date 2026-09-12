"""Keep native VP source intact when the host still has a legacy patch overlay."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / "deploy/swarm/deploy-sync-extension.sh"


class NativeStageOverlayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repos/videoprocess"
        self.stage = self.root / "stage/vp-app"
        self.overlay = self.root / "overlays"
        for relative in ("backend/app/main.py", "backend/app/config.py", "frontend/vite.config.ts"):
            target = self.repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        self.stage.mkdir(parents=True)
        (self.stage / "old-release").write_text("keep on failure")
        for relative in (
            "127-vp-backend.dockerignore",
            "127-vp-frontend/.dockerignore",
            "127-vp-frontend/nginx.conf",
        ):
            target = self.overlay / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(relative + "\n")

    def run_stage(self, project="vp-app"):
        # This is the staging entry point present on the deploy host before sourcing VP.
        script = r'''
stage_from_repo() {
  local project="$1" repo_name="$2" source_subdir="$3"
  local stage_tmp="$STAGE_ROOT/$project.tmp" stage_dir="$STAGE_ROOT/$project"
  rm -rf "$stage_tmp"
  mkdir -p "$stage_tmp"
  rsync -a --delete --exclude '.git/' "$REPO_ROOT/$repo_name/$source_subdir"/ "$stage_tmp"/
  apply_overlay "$project" "$stage_tmp"
  rm -rf "$stage_dir"
  mv "$stage_tmp" "$stage_dir"
}
apply_overlay() {
  if [[ "$1" == vp-app ]]; then
    echo 'legacy lifespan patch no longer matches native source' >&2
    return 66
  fi
  printf '%s\n' "$1" > "$2/delegated-overlay"
}
source "$EXTENSION"
stage_from_repo "$PROJECT" videoprocess .
'''
        return subprocess.run(
            ["bash", "-eu", "-c", script],
            env={
                "PATH": os.environ["PATH"],
                "EXTENSION": str(EXTENSION),
                "REPO_ROOT": str(self.root / "repos"),
                "STAGE_ROOT": str(self.root / "stage"),
                "OVERLAY_ROOT": str(self.overlay),
                "PROJECT": project,
            },
            text=True,
            capture_output=True,
            timeout=15,
        )

    def test_native_source_is_preserved_and_packaging_overlay_is_applied(self):
        for _ in range(2):
            result = self.run_stage()
            self.assertEqual(result.returncode, 0, result.stderr)
            for source in self.repo.rglob("*"):
                if source.is_file():
                    self.assertEqual(source.read_bytes(), (self.stage / source.relative_to(self.repo)).read_bytes())
            for source, target in (
                ("127-vp-backend.dockerignore", "backend/.dockerignore"),
                ("127-vp-frontend/.dockerignore", "frontend/.dockerignore"),
                ("127-vp-frontend/nginx.conf", "frontend/nginx.conf"),
            ):
                self.assertEqual((self.overlay / source).read_bytes(), (self.stage / target).read_bytes())
            self.assertFalse((self.stage / "old-release").exists())

    def test_other_projects_keep_their_original_overlay(self):
        result = self.run_stage("vp-pds")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "stage/vp-pds/delegated-overlay").read_text(), "vp-pds\n")

    def test_missing_packaging_input_does_not_replace_previous_stage(self):
        (self.overlay / "127-vp-frontend/nginx.conf").unlink()
        result = self.run_stage()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((self.stage / "old-release").read_text(), "keep on failure")


if __name__ == "__main__":
    unittest.main()
