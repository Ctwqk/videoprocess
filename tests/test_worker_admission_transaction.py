from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest


HELPER_PATH = (
    Path(__file__).resolve().parents[1]
    / "deploy/swarm/worker-admission-transaction.py"
)
HELPER = runpy.run_path(str(HELPER_PATH))
TRANSACTION_ID = "tx-e84fb31f632be927e6abe9ffb642fc79"
MARKER_GENERATION = "m-rb-e84fb31f632b-1"
OLD_COMMIT = "fab36e3a818bef6717c1" + "a" * 20
OLD_IMAGE = "vp-ffmpeg-worker-python:deploy-fab36e3a818b"


def secret(service, generation, purpose, name, serial):
    return dict(
        service=service, generation=str(generation), purpose=purpose,
        name=name, docker_secret_id=f"{serial:024x}",
    )


def marker_secret(purpose="readiness", serial=500):
    return secret(
        "worker-redis-marker-control", MARKER_GENERATION,
        purpose + "-database", f"vp-wrm-{purpose}-db-{MARKER_GENERATION}", serial,
    )


class RollbackPreparedSecretTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.root.chmod(0o700)
        self.active = self.root / "transactions/active.json"
        self.active.parent.mkdir(mode=0o700)
        credentials = {}
        for purpose in HELPER["DATABASE_PURPOSES"]:
            path = self.root / purpose
            path.write_text("postgresql://fixture:password@database/videoprocess\n")
            path.chmod(0o400)
            credentials[purpose] = HELPER["_capture_credential"](
                str(path), "vp_" + purpose,
            )
        self.state = HELPER["_new_document"](
            target_commit="2" * 40,
            target_backend_image="vp-backend:deploy-222222222222",
            target_go_image="vp-ffmpeg-worker-go:deploy-222222222222",
            namespace="2" * 40, baseline_kind="managed", credentials=credentials,
        )
        self.state.update(
            transaction_id=TRANSACTION_ID, phase="ROLLBACK_PREPARING", revision=71,
        )
        control_generation = "c-fab36e3a818bef6717c1"
        control = dict(
            generation=control_generation, image=OLD_IMAGE, manifest_sha256="a" * 64,
            secrets=[
                secret("vp-worker-control", control_generation, purpose, purpose, i)
                for i, purpose in enumerate((
                    "operator", "orchestrator", "staging-janitor",
                    "staging-minio-access", "staging-minio-secret",
                    "worker-minio-access", "worker-minio-secret",
                ), 100)
            ],
        )
        self.state["baseline"].update(
            captured=True, control=copy.deepcopy(control),
            services=[
                dict(name=name, existed=True, docker_service_id=f"{i:024x}",
                     image=OLD_IMAGE, spec_digest="b" * 64)
                for i, name in enumerate(sorted(HELPER["APP_SERVICES"]), 200)
            ],
        )
        self.state["failed_forward"]["captured"] = True
        self.state["rollback"].update(
            attempt=1, namespace="rollback-123456789012345678",
            marker_generation=MARKER_GENERATION, control=control,
        )
        self.worker = dict(
            service="vp-ffmpeg-worker-go-swarm", generation=900,
            commit=OLD_COMMIT, image="vp-ffmpeg-worker-go:deploy-fab36e3a818b",
            database_secret=secret(
                "vp-ffmpeg-worker-go-swarm", 900, "database", "vp-wr-ffmpeg-go-db-900", 600,
            ),
            admission_secret=secret(
                "vp-ffmpeg-worker-go-swarm", 900, "admission",
                "vp-wr-ffmpeg-go-admission-900", 601,
            ),
            docker_service_id=None, target_spec_digest=None, applied_stage="pending",
        )
        self.write_state()
        lock = self.root / "transaction.lock"
        self.lock_fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, self.lock_fd)

    def write_state(self, *, validate=True):
        if validate:
            HELPER["_validate_document"](self.state)
        self.active.write_bytes(HELPER["_canonical"](self.state))
        self.active.chmod(0o600)

    def cli(self, command, *arguments, data=None, success=True):
        result = subprocess.run(
            [sys.executable, str(HELPER_PATH), command, str(self.root),
             str(self.lock_fd), *map(str, arguments)],
            input=None if data is None else HELPER["_canonical"](data),
            capture_output=True, pass_fds=(self.lock_fd,), timeout=10,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stderr.decode())
        else:
            self.assertNotEqual(result.returncode, 0)
        return result.stdout.decode().strip()

    def lookup(self, reference, *, success=True):
        return self.cli(
            "lookup-prepared-secret",
            *(reference[key] for key in ("name", "service", "generation", "purpose")),
            success=success,
        )

    def record(self, reference, *, success=True):
        return self.cli(
            "record-prepared-secret",
            *(reference[key] for key in (
                "name", "docker_secret_id", "service", "generation", "purpose",
            )), success=success,
        )

    def read_state(self):
        return json.loads(self.active.read_bytes())

    def assert_rejected_without_write(self, reference):
        before = self.active.read_bytes()
        self.lookup(reference, success=False)
        self.record(reference, success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def test_lookup_before_first_marker_secret_creation(self):
        before = self.active.read_bytes()
        self.assertEqual(self.lookup(marker_secret()), "-")
        self.assertEqual(self.active.read_bytes(), before)

    def test_marker_recording_and_interrupted_retry_preserve_exact_ids(self):
        references = [marker_secret(purpose, i) for i, purpose in enumerate(
            ("readiness", "janitor", "repair"), 500,
        )]
        for reference in references:
            self.record(reference)
            self.assertEqual(self.lookup(reference), reference["docker_secret_id"])
            self.record(reference)
        state = self.read_state()
        self.assertEqual(state["prepared_secrets"], references)
        self.assertEqual(state["rollback"], self.state["rollback"])
        self.assertEqual(state["authorities"], [])
        self.assertEqual(state["pending_retirements"], [])
        conflicting = dict(references[0], docker_secret_id="f" * 24)
        before = self.active.read_bytes()
        self.record(conflicting, success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def test_worker_secrets_require_exact_existing_rollback_plan(self):
        self.state["rollback"]["workers"] = [self.worker]
        self.write_state()
        for field in ("database_secret", "admission_secret"):
            reference = self.worker[field]
            self.assertEqual(self.lookup(reference), "-")
            self.record(reference)
            self.record(reference)
            self.assertEqual(self.lookup(reference), reference["docker_secret_id"])
        self.assertEqual(self.read_state()["prepared_secrets"], [
            self.worker["database_secret"], self.worker["admission_secret"],
        ])

    def prepare_fresh_worker(self):
        self.state["failed_forward"]["services"] = [
            dict(item, image="vp-ffmpeg-worker-go:deploy-222222222222")
            for item in self.state["baseline"]["services"]
            if item["name"] == self.worker["service"]
        ]
        self.state["authorities"] = [dict(
            kind="marker", service="worker-redis-marker-control",
            generation="m-222222222222-1700000000-0001", state="provisioned",
            control_image="vp-ffmpeg-worker-python:deploy-222222222222",
            control_generation="c-22222222222222222222",
            operator_reference="marker/m-222222222222-1700000000-0001/worker-marker-owner-database-url",
        )]
        self.write_state()
        control = self.state["rollback"]["control"]
        return [
            "runtime", self.worker["service"], self.worker["generation"],
            control["image"], control["generation"],
            f"control/{control['generation']}/worker-registration-operator-database-url",
        ]

    def test_fresh_runtime_intent_through_secret_creation_and_worker_selection(self):
        arguments = self.prepare_fresh_worker()
        self.assertEqual(self.read_state()["rollback"]["workers"], [])
        self.cli("record-authority-intent", *arguments)
        self.assertEqual(self.read_state()["authorities"][-1]["state"], "planned")
        self.assert_rejected_without_write(self.worker["database_secret"])
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.assertEqual(self.read_state()["authorities"][-1]["state"], "provisioning")
        self.assert_rejected_without_write(self.worker["database_secret"])
        self.cli("mark-authority-provisioned", *arguments[:3])
        self.assertEqual(self.read_state()["authorities"][-1]["state"], "provisioned")
        for field in ("database_secret", "admission_secret"):
            reference = self.worker[field]
            self.assertEqual(self.lookup(reference), "-")
            self.record(reference)
            self.assertEqual(self.lookup(reference), reference["docker_secret_id"])
        plan = {key: value for key, value in self.worker.items()
                if key not in {"docker_service_id", "applied_stage"}}
        self.cli("record-worker-plan", self.read_state()["revision"], "rollback", data=plan)
        self.cli("record-authority-intent", *arguments)
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.cli("mark-authority-provisioned", *arguments[:3])
        for field in ("database_secret", "admission_secret"):
            self.record(self.worker[field])
            self.assertEqual(self.lookup(self.worker[field]), self.worker[field]["docker_secret_id"])
        self.assertEqual(len(self.read_state()["authorities"]), 2)
        self.assertEqual(len(self.read_state()["prepared_secrets"]), 2)
        self.assertEqual(self.read_state()["rollback"]["workers"], [self.worker])

    def test_fresh_runtime_intent_rejects_unaffected_service_or_wrong_control(self):
        arguments = self.prepare_fresh_worker()
        for index, value in (
            (0, "control"), (1, "vp-vision-worker-swarm"),
            (3, "vp-ffmpeg-worker-python:deploy-222222222222"),
            (4, "c-22222222222222222222"), (5, "control/foreign/operator"),
        ):
            with self.subTest(index=index):
                invalid = list(arguments)
                invalid[index] = value
                before = self.active.read_bytes()
                self.cli("record-authority-intent", *invalid, success=False)
                self.assertEqual(self.active.read_bytes(), before)

    def test_fresh_runtime_secrets_reject_noncanonical_names_and_purposes(self):
        arguments = self.prepare_fresh_worker()
        self.cli("record-authority-intent", *arguments)
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.cli("mark-authority-provisioned", *arguments[:3])
        reference = self.worker["database_secret"]
        for invalid in (
            dict(reference, name="vp-wr-go-db-900"),
            dict(reference, name="vp-wr-ffmpeg-go-db-901", generation="901"),
            dict(reference, purpose="operator"),
            dict(reference, purpose="admission"),
        ):
            with self.subTest(reference=invalid):
                self.assert_rejected_without_write(invalid)
        competing = list(arguments)
        competing[2] = 901
        self.cli("record-authority-intent", *competing, success=False)

    def test_applying_replays_only_existing_provisioned_authority_and_secret_ids(self):
        arguments = self.prepare_fresh_worker()
        self.cli("record-authority-intent", *arguments)
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.cli("mark-authority-provisioned", *arguments[:3])
        self.record(self.worker["database_secret"])
        self.cli("transition", self.read_state()["revision"], "ROLLBACK_APPLYING")
        before = self.active.read_bytes()
        self.cli("record-authority-intent", *arguments)
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.cli("mark-authority-provisioned", *arguments[:3])
        self.assertEqual(self.lookup(self.worker["database_secret"]), "000000000000000000000258")
        self.record(self.worker["database_secret"])
        self.assertEqual(self.active.read_bytes(), before)
        self.assert_rejected_without_write(self.worker["admission_secret"])
        self.assert_rejected_without_write(marker_secret())
        arguments[2] = 901
        self.cli("record-authority-intent", *arguments, success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def test_forward_preparation_still_requires_target_control_authority(self):
        arguments = self.prepare_fresh_worker()
        for phase in ("PREPARING", "FORWARD_APPLYING"):
            with self.subTest(phase=phase):
                self.state["phase"] = phase
                self.write_state()
                self.cli("record-authority-intent", *arguments, success=False)
                self.record(marker_secret(), success=False)
                target = list(arguments)
                target[3:] = [
                    "vp-ffmpeg-worker-python:deploy-222222222222",
                    "c-22222222222222222222",
                    "control/c-22222222222222222222/worker-registration-operator-database-url",
                ]
                self.cli("record-authority-intent", *target)

    def test_duplicate_secret_ids_are_rejected_without_losing_prior_record(self):
        self.record(marker_secret())
        before = self.active.read_bytes()
        self.record(marker_secret("janitor"), success=False)
        self.assertEqual(self.active.read_bytes(), before)
        self.assertEqual(self.lookup(marker_secret()), "0000000000000000000001f4")

    def test_rollback_worker_selection_cannot_replace_prepared_secret_ids(self):
        arguments = self.prepare_fresh_worker()
        self.cli("record-authority-intent", *arguments)
        self.cli("mark-authority-provisioning", *arguments[:3])
        self.cli("mark-authority-provisioned", *arguments[:3])
        self.record(self.worker["database_secret"])
        plan = {key: value for key, value in self.worker.items()
                if key not in {"docker_service_id", "applied_stage"}}
        plan["database_secret"] = dict(plan["database_secret"], docker_secret_id="f" * 24)
        before = self.active.read_bytes()
        self.cli("record-worker-plan", self.read_state()["revision"], "rollback",
                 data=plan, success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def test_unbound_names_generations_services_and_purposes_fail_before_creation(self):
        self.state["rollback"]["workers"] = [self.worker]
        self.write_state()
        marker = marker_secret()
        worker = self.worker["database_secret"]
        invalid = [
            dict(marker, generation="m-rb-aaaaaaaaaaaa-1"),
            dict(marker, generation="m-rb-e84fb31f632b-2"),
            dict(marker, generation="m-222222222222-1700000000-0001"),
            dict(marker, name="unbound-marker-secret"),
            dict(marker, purpose="owner-database"),
            dict(marker, purpose="readiness-redis"),
            dict(marker, service="vp-worker-control"),
            dict(marker, service="vision-cutover", generation=TRANSACTION_ID,
                 purpose="safety-database"),
            dict(worker, generation="901"),
            dict(worker, name="unbound-worker-secret"),
            dict(worker, purpose="operator"),
            dict(worker, service="vp-vision-worker-swarm"),
        ]
        for reference in invalid:
            with self.subTest(reference=reference):
                self.assert_rejected_without_write(reference)

    def test_worker_id_cannot_disagree_with_rollback_plan(self):
        self.state["rollback"]["workers"] = [self.worker]
        self.write_state()
        before = self.active.read_bytes()
        self.record(dict(self.worker["database_secret"], docker_secret_id="f" * 24),
                    success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def test_missing_or_foreign_rollback_allocation_and_control_are_rejected(self):
        original = copy.deepcopy(self.state)
        for case in ("unallocated", "foreign-transaction", "foreign-attempt",
                     "no-control", "foreign-control", "no-workers"):
            with self.subTest(case=case):
                self.state = copy.deepcopy(original)
                rollback = self.state["rollback"]
                reference = marker_secret()
                if case == "unallocated":
                    rollback.update(attempt=0, namespace=None, marker_generation=None)
                elif case == "foreign-transaction":
                    self.state["transaction_id"] = "tx-" + "a" * 32
                elif case == "foreign-attempt":
                    rollback["attempt"] = 2
                elif case == "no-control":
                    rollback["control"] = None
                elif case == "foreign-control":
                    rollback["control"]["image"] = "vp-ffmpeg-worker-python:deploy-222222222222"
                else:
                    reference = self.worker["database_secret"]
                self.write_state()
                self.assert_rejected_without_write(reference)

    def test_recorded_rollback_secrets_survive_promotion_and_retirement(self):
        self.record(marker_secret())
        self.state = self.read_state()
        self.state["rollback"]["marker"] = dict(
            generation=MARKER_GENERATION, image=OLD_IMAGE,
            config_sha256="c" * 64, cron_sha256="d" * 64,
            secrets=[marker_secret()],
        )
        self.write_state()
        for phase in ("ROLLBACK_APPLYING", "ROLLBACK_VERIFIED",
                      "ROLLBACK_WORKERS_PROMOTED", "ROLLBACK_MARKER_PROMOTED",
                      "ROLLBACK_CONTROL_PROMOTED", "RETIRING", "DONE"):
            arguments = [self.read_state()["revision"], phase]
            if phase == "DONE":
                arguments.append("rolled_back")
            self.cli("transition", *arguments)
            state = self.read_state()
            self.assertEqual(state["prepared_secrets"], [marker_secret()])
            self.assertEqual(state["pending_retirements"], [])
            self.assert_rejected_without_write(marker_secret("repair", 502))

    def test_marker_selection_cannot_replace_a_recorded_secret_identity(self):
        self.record(marker_secret())
        before = self.active.read_bytes()
        selection = dict(
            generation=MARKER_GENERATION, image=OLD_IMAGE,
            config_sha256="c" * 64, cron_sha256="d" * 64,
            secrets=[dict(marker_secret(), docker_secret_id="f" * 24)],
        )
        self.cli("record-marker-selection", self.read_state()["revision"], "rollback",
                 data=selection, success=False)
        self.assertEqual(self.active.read_bytes(), before)

    def shell(self, body, *, success=True):
        result = subprocess.run(
            ["bash", "-c", r'''
set -euo pipefail
REPO_ROOT="$CASE_REPO"
source "$CASE_REPO/deploy/swarm/deploy-sync-extension.sh"
vp_worker_admission_root() { printf '%s\n' "$CASE_ROOT"; }
vp_worker_admission_lock_acquire "$CASE_ROOT"
trap 'vp_worker_admission_lock_release' EXIT
VP_WORKER_ADMISSION_TRANSACTION_ID=tx-e84fb31f632be927e6abe9ffb642fc79
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=rollback-123456789012345678
VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
VP_WORKER_ADMISSION_PREPARED=true
''' + body],
            env=dict(os.environ, CASE_ROOT=str(self.root),
                     CASE_REPO=str(HELPER_PATH.parents[2])),
            capture_output=True, text=True, timeout=20,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def prepare_empty_worker_promotion(self):
        self.state["phase"] = "ROLLBACK_VERIFIED"
        self.state["rollback"]["marker"] = dict(
            generation=MARKER_GENERATION, image=OLD_IMAGE,
            config_sha256="c" * 64, cron_sha256="d" * 64,
            secrets=[marker_secret()],
        )
        self.write_state()
        (self.active.parent / TRANSACTION_ID).mkdir(mode=0o700)
        (self.root / "candidates").mkdir(mode=0o700)
        current = self.root / "current"
        current.mkdir(mode=0o700)
        for name in ("ffmpeg-go", "ffmpeg", "vision", "youtube-publisher"):
            path = current / f"{name}.conf"
            path.write_text(f"preserved-{name}\n")
            path.chmod(0o600)
        return {path.name: path.read_bytes() for path in current.iterdir()}

    def test_empty_rollback_worker_promotion_preserves_all_current_manifests(self):
        before = self.prepare_empty_worker_promotion()
        self.shell("vp_worker_admission_promote_phase PROMOTE_ROLLBACK_WORKERS\n")
        state = self.read_state()
        self.assertEqual(state["phase"], "ROLLBACK_WORKERS_PROMOTED")
        self.assertEqual(state["promotion"], dict(workers=True, marker=False, control=False))
        self.assertIsNone(state["operation"])
        self.assertEqual(state["pending_retirements"], [])
        self.assertEqual(before, {path.name: path.read_bytes()
                                 for path in (self.root / "current").iterdir()})
        retirements = self.root / "retirements"
        self.assertTrue(not retirements.exists() or not list(retirements.iterdir()))

    def test_empty_worker_commit_does_not_drain_other_retirement_journals(self):
        self.prepare_empty_worker_promotion()
        self.shell(r'''
vp_worker_admission_process_retirement_journals() { return 77; }
vp_worker_admission_promote_phase PROMOTE_ROLLBACK_WORKERS
''')

    def test_empty_worker_selection_requires_all_preserved_manifests(self):
        self.prepare_empty_worker_promotion()
        (self.root / "current/vision.conf").unlink()
        self.shell("vp_worker_admission_require_promotion_selection PROMOTE_ROLLBACK_WORKERS\n",
                   success=False)

    def test_empty_worker_pending_promotion_rejects_current_manifest_drift(self):
        self.prepare_empty_worker_promotion()
        self.shell(r'''
vp_worker_admission_promotion_identity PROMOTE_ROLLBACK_WORKERS >/dev/null
vp_worker_admission_capture_promotion_precondition PROMOTE_ROLLBACK_WORKERS "$VP_WORKER_ADMISSION_PROMOTION_IDENTITY"
vp_worker_admission_load_replay_plan
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" intent "$CASE_ROOT" "$VP_WORKER_ADMISSION_LOCK_FD" "$VP_WORKER_ADMISSION_REPLAY_REVISION" PROMOTE_ROLLBACK_WORKERS "$VP_WORKER_ADMISSION_PROMOTION_IDENTITY"
''')
        current = self.root / "current/vision.conf"
        original = current.read_bytes()
        extra = self.root / "current/unexpected.conf"
        for change in ("modify", "delete", "add"):
            with self.subTest(change=change):
                if change == "modify":
                    current.write_text("changed\n")
                elif change == "delete":
                    current.unlink()
                else:
                    extra.write_text("extra\n")
                    extra.chmod(0o600)
                self.shell(r'''
vp_worker_admission_load_replay_plan
vp_worker_admission_current_promotion_matches PROMOTE_ROLLBACK_WORKERS
''', success=False)
                current.write_bytes(original)
                current.chmod(0o600)
                extra.unlink(missing_ok=True)
        self.assertEqual(self.read_state()["phase"], "ROLLBACK_VERIFIED")

    def test_empty_rollback_worker_promotion_replays_pending_intent(self):
        before = self.prepare_empty_worker_promotion()
        self.shell(r'''
vp_worker_admission_promotion_identity PROMOTE_ROLLBACK_WORKERS >/dev/null
vp_worker_admission_capture_promotion_precondition PROMOTE_ROLLBACK_WORKERS "$VP_WORKER_ADMISSION_PROMOTION_IDENTITY"
vp_worker_admission_load_replay_plan
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" intent "$CASE_ROOT" "$VP_WORKER_ADMISSION_LOCK_FD" "$VP_WORKER_ADMISSION_REPLAY_REVISION" PROMOTE_ROLLBACK_WORKERS "$VP_WORKER_ADMISSION_PROMOTION_IDENTITY"
''')
        self.assertIsNotNone(self.read_state()["operation"])
        self.shell(r'''
vp_worker_admission_load_replay_plan
vp_worker_admission_complete_pending_promotion PROMOTE_ROLLBACK_WORKERS "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID"
''')
        self.assertEqual(self.read_state()["phase"], "ROLLBACK_WORKERS_PROMOTED")
        self.assertEqual(before, {path.name: path.read_bytes()
                                 for path in (self.root / "current").iterdir()})

    def test_empty_worker_selection_rejects_forward_and_attempted_worker(self):
        self.prepare_empty_worker_promotion()
        self.shell("vp_worker_admission_require_promotion_selection PROMOTE_WORKERS\n",
                   success=False)
        self.state["failed_forward"]["services"] = [
            item for item in self.state["baseline"]["services"]
            if item["name"] == self.worker["service"]
        ]
        self.write_state()
        self.shell("vp_worker_admission_require_promotion_selection PROMOTE_ROLLBACK_WORKERS\n",
                   success=False)
        self.assertFalse((self.root / "candidates" / self.state["rollback"]["namespace"]).exists())

    def test_empty_worker_selection_rejects_unexpected_candidate(self):
        self.prepare_empty_worker_promotion()
        directory = self.root / "candidates" / self.state["rollback"]["namespace"]
        directory.mkdir(mode=0o700)
        (directory / "vision.conf").write_text("unselected\n")
        self.shell("vp_worker_admission_require_promotion_selection PROMOTE_ROLLBACK_WORKERS\n",
                   success=False)

    def test_reconcile_retires_failed_candidates_before_archiving_rollback(self):
        self.prepare_empty_worker_promotion()
        self.state.update(phase="RETIRING", retiring_outcome="rolled_back",
                          promotion=dict(workers=True, marker=True, control=True))
        self.write_state()
        result = self.shell(r'''
vp_worker_admission_verify_active_database_credentials() { :; }
vp_worker_admission_hydrate_recovery_context() {
  VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
  VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS=failed-records
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=failed-namespace
}
vp_worker_admission_stale_rollback_records() { printf 'stale-records\n'; }
vp_worker_admission_stale_rollback_namespaces() { printf 'stale-namespace\n'; }
vp_worker_admission_retire_records() { printf 'retire|%s\n' "$1"; }
vp_worker_admission_discard_namespace() { printf 'discard|%s\n' "$2"; }
vp_worker_admission_retire_transaction() { printf 'transaction-cleanup\n'; }
vp_reconcile_worker_admission_transaction
''')
        self.assertEqual(result.stdout.splitlines(), [
            "retire|failed-records", "stale-records", "discard|failed-namespace",
            "discard|stale-namespace", "transaction-cleanup",
        ])
        self.assertFalse(self.active.exists())
        done = json.loads((self.active.parent / TRANSACTION_ID / "done.json").read_bytes())
        self.assertEqual((done["phase"], done["outcome"]), ("DONE", "rolled_back"))


if __name__ == "__main__":
    unittest.main()
