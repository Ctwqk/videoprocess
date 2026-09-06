from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[1]
OLD_COMMIT = "1" * 40
OLD_CONTROL = "c-" + OLD_COMMIT[:20]
FAILED_CONTROL = "c-" + "2" * 20
OLD_IMAGE = "vp-ffmpeg-worker-python:deploy-" + OLD_COMMIT[:12]
FAILED_IMAGE = "vp-ffmpeg-worker-python:deploy-" + "2" * 12
TRANSACTION = "tx-" + "a" * 32
NAMESPACE = "rollback-123456789012345678"
NETWORK = "network123456789012345678"
WORKERS = (
    ("vp-ffmpeg-worker-go-swarm", "ffmpeg-go", "ffmpeg_go", "colima-127", "media_cpu"),
    ("vp-ffmpeg-worker-gpu-swarm", "ffmpeg", "ffmpeg", "150-gpu", "media_gpu"),
    ("vp-vision-worker-swarm", "vision", "vision", "150-vision", "vision_gpu"),
    ("vp-youtube-publisher-swarm", "youtube-publisher", "youtube_publisher", "150-publisher", "youtube_publisher"),
)


def digest(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def docker_fixture(arguments):
    if arguments[:2] == ["network", "inspect"]:
        print(f"{NETWORK}|vp-pipeline-net|overlay|swarm")
        return 0
    if arguments[:2] != ["service", "inspect"] or len(arguments) != 5:
        return 90
    services = json.loads(Path(os.environ["CASE_ROOT"], "live.json").read_text())
    service = next((item for item in services.values()
                    if arguments[2] in (item["ID"], item["Spec"]["Name"])), None)
    if service is None:
        return 1
    if arguments[4] == "{{.ID}}|{{.Spec.Name}}":
        print(service["ID"] + "|" + service["Spec"]["Name"])
    elif arguments[4] == "{{json .Spec}}":
        print(json.dumps(service["Spec"]))
    elif arguments[4] == "{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}":
        print("\n".join(item["SecretName"] for item in service["Spec"]["TaskTemplate"]["ContainerSpec"]["Secrets"]))
    else:
        return 91
    return 0


class UntouchedRollbackControlTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.root.chmod(0o700)
        self.helper = runpy.run_path(str(REPO / "deploy/swarm/worker-admission-transaction.py"))
        credentials = {}
        for purpose in self.helper["DATABASE_PURPOSES"]:
            path = self.root / purpose
            path.write_text("postgresql://fixture:password@database/videoprocess\n")
            path.chmod(0o400)
            credentials[purpose] = self.helper["_capture_credential"](str(path), "vp_" + purpose)
        self.state = self.helper["_new_document"](
            target_commit="2" * 40, target_backend_image="vp-backend:deploy-" + "2" * 12,
            target_go_image="vp-ffmpeg-worker-go:deploy-" + "2" * 12,
            namespace="2" * 40, baseline_kind="managed", credentials=credentials,
        )
        control = dict(generation=OLD_CONTROL, image=OLD_IMAGE, manifest_sha256="b" * 64, secrets=[
            dict(name=f"vp-wc-{name}-{OLD_CONTROL}", docker_secret_id=f"{i:024d}",
                 service="vp-worker-control", generation=OLD_CONTROL, purpose=purpose)
            for i, (name, purpose) in enumerate((
                ("operator", "operator"), ("orchestrator", "orchestrator"), ("staging", "staging-janitor"),
                ("minio-access", "staging-minio-access"), ("minio-secret", "staging-minio-secret"),
                ("worker-minio-access", "worker-minio-access"), ("worker-minio-secret", "worker-minio-secret"),
            ), 1)
        ])
        self.state.update(transaction_id=TRANSACTION, phase="ROLLBACK_MARKER_PROMOTED", revision=41)
        self.state["baseline"].update(captured=True, control=copy.deepcopy(control))
        self.state["failed_forward"]["captured"] = True
        self.state["rollback"].update(
            attempt=1, namespace=NAMESPACE, marker_generation="m-rb-aaaaaaaaaaaa-1",
            control=control,
        )
        self.state["promotion"].update(workers=True, marker=True, control=False)
        self.live = {}
        for i, worker in enumerate(WORKERS, 1):
            spec = self.worker_spec(worker, 100 + i)
            service = dict(ID=f"{i:024x}", Spec=spec)
            self.live[worker[0]] = service
            self.state["baseline"]["services"].append(dict(
                name=worker[0], existed=True, docker_service_id=service["ID"],
                image=spec["TaskTemplate"]["ContainerSpec"]["Image"], spec_digest=digest(spec),
            ))
        self.live["vp-staging-object-janitor"] = dict(
            ID="9" * 24, Spec=dict(Name="vp-staging-object-janitor", TaskTemplate=dict(ContainerSpec=dict(Secrets=[]))),
        )
        for name in sorted(self.helper["APP_SERVICES"] - {worker[0] for worker in WORKERS}):
            self.state["baseline"]["services"].append(dict(
                name=name, existed=False, docker_service_id=None, image=None, spec_digest=None,
            ))
        self.selected = []
        (self.root / "transactions").mkdir(mode=0o700)
        (self.root / "transactions" / TRANSACTION).mkdir(mode=0o700)
        (self.root / "candidates" / NAMESPACE).mkdir(mode=0o700, parents=True)
        (self.root / "candidates").chmod(0o700)

    def worker_spec(self, worker, generation):
        name, kind, worker_type, host, capabilities = worker
        image = ("vp-ffmpeg-worker-go" if kind == "ffmpeg-go" else "vp-ffmpeg-worker-python") + ":deploy-" + OLD_COMMIT[:12]
        env = dict(
            DEPLOY_MODE="production", WORKER_SERVICE_NAME=name, WORKER_ADMISSION_GENERATION=str(generation),
            WORKER_SLOT="1", WORKER_TYPE=worker_type, WORKER_HOST=host, WORKER_CAPABILITIES=capabilities,
            WORKER_RELEASE_COMMIT=OLD_COMMIT, WORKER_IMAGE_IDENTITY=image,
            WORKER_REDIS_STREAM="vp:tasks:" + worker_type, WORKER_REDIS_GROUP=worker_type + "-workers",
            WORKER_DATABASE_URL_FILE="/run/secrets/vp-worker-database-url",
            WORKER_ADMISSION_TOKEN_FILE="/run/secrets/vp-worker-admission-token",
            WORKER_REDIS_URL_FILE="/run/secrets/vp-worker-redis-url",
            WORKER_MINIO_ACCESS_KEY_FILE="/run/secrets/vp-worker-minio-access-key",
            WORKER_MINIO_SECRET_KEY_FILE="/run/secrets/vp-worker-minio-secret-key", VP_REQUIRE_STAGING_JANITOR="true",
        )
        secrets = [dict(SecretID=f"{generation * 10 + i:024x}", SecretName=source,
                        File=dict(Name=target, UID="10001", GID="10001", Mode=0o400))
                   for i, (source, target) in enumerate((
                       (f"db-{kind}-{generation}", "vp-worker-database-url"),
                       (f"admission-{kind}-{generation}", "vp-worker-admission-token"),
                       (f"redis-{kind}", "vp-worker-redis-url"),
                       (f"vp-wc-worker-minio-access-{OLD_CONTROL}", "vp-worker-minio-access-key"),
                       (f"vp-wc-worker-minio-secret-{OLD_CONTROL}", "vp-worker-minio-secret-key"),
                   ))]
        constraints = ["node.labels.vp.runtime==true", "node.hostname==colima-127"] if kind == "ffmpeg-go" else [
            "node.labels.vp.publisher==true" if kind == "youtube-publisher" else "node.labels.vp.gpu==true",
            "node.hostname==ccttww-lap",
        ]
        return dict(Name=name, Mode=dict(Replicated=dict(Replicas=1)), TaskTemplate=dict(
            ContainerSpec=dict(Image=image, Env=[f"{key}={value}" for key, value in env.items()], Secrets=secrets),
            Placement=dict(Constraints=constraints), Networks=[dict(Target=NETWORK)],
        ))

    def select_worker(self, index):
        worker = WORKERS[index]
        name, kind = worker[:2]
        self.selected.append(name)
        self.state["failed_forward"]["services"].append(copy.deepcopy(self.state["baseline"]["services"][index]))
        generation = 900 + index
        spec = self.worker_spec(worker, generation)
        self.live[name]["Spec"] = spec
        container = spec["TaskTemplate"]["ContainerSpec"]
        references = [dict(name=item["SecretName"], docker_secret_id=item["SecretID"], service=name,
                           generation=str(generation), purpose=purpose)
                      for item, purpose in zip(container["Secrets"], ("database", "admission"))]
        self.state["rollback"]["workers"].append(dict(
            service=name, generation=generation, commit=OLD_COMMIT, image=container["Image"],
            database_secret=references[0], admission_secret=references[1],
            docker_service_id=self.live[name]["ID"], target_spec_digest=digest(spec), applied_stage="verified",
        ))
        fields = dict(VERSION="2", SERVICE=name, COMMIT=OLD_COMMIT, IMAGE=container["Image"], GENERATION=generation,
                      DATABASE_SECRET=references[0]["name"], ADMISSION_SECRET=references[1]["name"],
                      DATABASE_SECRET_ID=references[0]["docker_secret_id"], ADMISSION_SECRET_ID=references[1]["docker_secret_id"])
        path = self.root / "candidates" / NAMESPACE / f"{kind}.conf"
        path.write_text("".join(f"{key}={value}\n" for key, value in fields.items()))
        path.chmod(0o600)

    def finalize(self, success=True):
        self.helper["_validate_document"](self.state)
        active = self.root / "transactions/active.json"
        active.write_bytes(self.helper["_canonical"](self.state))
        active.chmod(0o600)
        (self.root / "live.json").write_text(json.dumps(self.live))
        result = subprocess.run(["bash", "-c", r'''
set -euo pipefail
REPO_ROOT="$CASE_REPO"
source "$CASE_REPO/deploy/swarm/deploy-sync-extension.sh"
vp_worker_admission_root() { printf '%s\n' "$CASE_ROOT"; }
docker() { python3 "$CASE_TEST" docker "$@"; }
vp_worker_admission_lock_acquire "$CASE_ROOT"
trap 'vp_worker_admission_lock_release' EXIT
VP_WORKER_ADMISSION_TRANSACTION_ID="$CASE_TRANSACTION"
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$CASE_NAMESPACE"
VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$CASE_SELECTED"
VP_WORKER_ADMISSION_COMMIT=2222222222222222222222222222222222222222
VP_WORKER_ADMISSION_PREPARED=true
VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_CONTROL_PREPARED=true
VP_WORKER_CONTROL_GENERATION="$CASE_OLD_CONTROL"
VP_WORKER_ADMISSION_CONTROL_IMAGE="$CASE_OLD_IMAGE"
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$CASE_FAILED_CONTROL"
VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$CASE_FAILED_IMAGE"
VP_WORKER_MINIO_ACCESS_SECRET="vp-wc-worker-minio-access-$CASE_OLD_CONTROL"
VP_WORKER_MINIO_SECRET_SECRET="vp-wc-worker-minio-secret-$CASE_OLD_CONTROL"
VP_WORKER_REDIS_FFMPEG_GO_SECRET=redis-ffmpeg-go
VP_WORKER_REDIS_FFMPEG_SECRET=redis-ffmpeg
VP_WORKER_REDIS_VISION_SECRET=redis-vision
VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=redis-youtube-publisher
ids=()
for n in 1 2 3 4 5 6 7; do ids+=("$(printf '%024d' "$n")"); done
vp_worker_control_write_manifest "$CASE_ROOT/control-current.conf" "$CASE_OLD_CONTROL" "$CASE_OLD_IMAGE" "${ids[@]}"
ids=()
for n in 11 12 13 14 15 16 17; do ids+=("$(printf '%024d' "$n")"); done
vp_worker_control_write_manifest "$CASE_ROOT/control-candidates/$CASE_FAILED_CONTROL.conf" "$CASE_FAILED_CONTROL" "$CASE_FAILED_IMAGE" "${ids[@]}"
vp_require_staging_object_janitor_control() {
  [[ "$1" == "$CASE_ROOT" && "$2" == "$CASE_OLD_IMAGE" ]] || return 1
  printf 'janitor\n' >>"$CASE_ROOT/effects"
}
vp_worker_control_revoke_authority() { printf 'revoke|%s|%s\n' "$1" "$2" >>"$CASE_ROOT/effects"; }
vp_remove_managed_secret_if_absent_exact() { printf 'secret|%s\n' "$*" >>"$CASE_ROOT/effects"; }
vp_finalize_worker_control_rollback
[[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" && "$VP_WORKER_CONTROL_PREPARED" == false ]]
[[ "$VP_WORKER_ADMISSION_COMMIT" == 2222222222222222222222222222222222222222 ]]
'''], env=dict(os.environ, CASE_ROOT=str(self.root), CASE_REPO=str(REPO), CASE_TEST=__file__,
              CASE_TRANSACTION=TRANSACTION, CASE_NAMESPACE=NAMESPACE, CASE_SELECTED=" ".join(self.selected),
              CASE_OLD_CONTROL=OLD_CONTROL, CASE_OLD_IMAGE=OLD_IMAGE,
              CASE_FAILED_CONTROL=FAILED_CONTROL, CASE_FAILED_IMAGE=FAILED_IMAGE),
            capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        effects = (self.root / "effects").read_text().splitlines() if (self.root / "effects").exists() else []
        if success:
            self.assertEqual(effects[:2], ["janitor", f"revoke|{FAILED_IMAGE}|{FAILED_CONTROL}"])
            self.assertEqual(len(effects), 9)
            self.assertTrue(all(FAILED_CONTROL in line for line in effects[2:]))
            self.assertFalse((self.root / "control-candidates" / f"{FAILED_CONTROL}.conf").exists())
        else:
            self.assertEqual(effects, [])

    def test_zero_attempted_workers_finalize_using_unchanged_baseline(self):
        self.finalize()

    def test_mixed_rollback_selects_candidate_only_for_attempted_worker(self):
        self.select_worker(2)
        self.finalize()

    def test_untouched_worker_rejects_recreated_service(self):
        self.live[WORKERS[0][0]]["ID"] = "f" * 24
        self.finalize(success=False)

    def test_untouched_worker_rejects_database_secret_id_drift(self):
        self.live[WORKERS[0][0]]["Spec"]["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["SecretID"] = "f" * 24
        self.finalize(success=False)

    def test_mixed_rollback_rejects_untouched_worker_env_drift(self):
        self.select_worker(2)
        self.live[WORKERS[0][0]]["Spec"]["TaskTemplate"]["ContainerSpec"]["Env"].append("DATABASE_URL=unsafe")
        self.finalize(success=False)

    def test_selected_worker_rejects_secret_id_drift(self):
        self.select_worker(0)
        self.live[WORKERS[0][0]]["Spec"]["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["SecretID"] = "f" * 24
        self.finalize(success=False)

    def test_attempted_worker_without_rollback_selection_is_not_untouched(self):
        self.state["failed_forward"]["services"].append(copy.deepcopy(self.state["baseline"]["services"][0]))
        self.finalize(success=False)


if __name__ == "__main__":
    if sys.argv[1:2] == ["docker"]:
        raise SystemExit(docker_fixture(sys.argv[2:]))
    unittest.main()
