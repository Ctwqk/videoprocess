from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


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

    def prepare_absent_forward_worker(self):
        self.state.update(phase="FORWARD_APPLYING", revision=0)
        self.state["rollback"] = dict(
            attempt=0, namespace=None, marker_generation=None,
            control=None, marker=None, workers=[],
        )
        self.state["failed_forward"] = dict(captured=False, control=None, services=[])
        self.worker.update(applied_stage="prepared", commit="2" * 40,
                           image="vp-ffmpeg-worker-go:deploy-222222222222")
        self.state["forward"]["workers"] = [self.worker]
        absent = dict(name=self.worker["service"], existed=False,
                      docker_service_id=None, image=None, spec_digest=None)
        self.state["baseline"]["services"] = [
            copy.deepcopy(absent) if item["name"] == absent["name"] else item
            for item in self.state["baseline"]["services"]
        ]
        self.write_state()
        directory = self.active.parent / TRANSACTION_ID
        directory.mkdir(mode=0o700)
        self.progress_path = directory / "app-progress.json"
        self.progress_path.write_bytes(HELPER["_canonical"](dict(
            schema=1, transaction_id=TRANSACTION_ID, target_commit="2" * 40,
            attempted_services=[], migration_state="pending",
        )))
        self.progress_path.chmod(0o600)

    def test_existing_worker_activation_intent_survives_restart(self):
        self.prepare_absent_forward_worker()
        self.state["baseline"]["services"] = [
            dict(item, existed=True, docker_service_id="d" * 24,
                 image=OLD_IMAGE, spec_digest="e" * 64)
            if item["name"] == self.worker["service"] else item
            for item in self.state["baseline"]["services"]
        ]
        self.write_state()
        for _ in range(2):
            self.shell("vp_record_worker_activation_attempt vp-ffmpeg-worker-go-swarm\n")
            progress = json.loads(self.progress_path.read_bytes())
            self.assertEqual(progress["attempted_services"], [self.worker["service"]])

    def test_absent_worker_activation_preserves_precreation_cleanup(self):
        self.prepare_absent_forward_worker()
        before = (self.active.read_bytes(), self.progress_path.read_bytes())
        self.shell("vp_record_worker_activation_attempt vp-ffmpeg-worker-go-swarm\n")
        self.assertEqual((self.active.read_bytes(), self.progress_path.read_bytes()), before)
        self.assertEqual(self.read_state()["forward"]["workers"], [self.worker])

    def test_activation_baseline_lookup_fails_closed_without_progress_write(self):
        self.prepare_absent_forward_worker()
        original = copy.deepcopy(self.state)
        for invalid in ("missing", "duplicate", "invalid-bool", "uncaptured", "wrong-phase"):
            with self.subTest(invalid=invalid):
                self.state = copy.deepcopy(original)
                baseline = self.state["baseline"]
                worker = next(item for item in baseline["services"]
                              if item["name"] == self.worker["service"])
                if invalid == "missing":
                    baseline["services"] = [
                        item for item in baseline["services"] if item is not worker
                    ]
                elif invalid == "duplicate":
                    baseline["services"].append(copy.deepcopy(worker))
                elif invalid == "invalid-bool":
                    worker["existed"] = "false"
                elif invalid == "uncaptured":
                    baseline["captured"] = False
                else:
                    self.state["phase"] = "PREPARING"
                self.write_state(validate=False)
                before = self.progress_path.read_bytes()
                self.shell("vp_record_worker_activation_attempt vp-ffmpeg-worker-go-swarm\n",
                           success=False)
                self.assertEqual(self.progress_path.read_bytes(), before)
        self.state = original
        self.write_state()
        self.shell("vp_worker_admission_recovery_state() { return 1; }; "
                   "vp_record_worker_activation_attempt vp-ffmpeg-worker-go-swarm\n",
                   success=False)

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


class RegisteredReconcileJournalTests(unittest.TestCase):
    def setUp(self):
        self.assertIn("prepare_registered_reconcile", HELPER, "Task1 journal operations missing")
        fixture = RollbackPreparedSecretTests(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.root, self.fd = fixture.root, fixture.lock_fd
        self.protocol = HELPER["_registered_protocol"]()
        state = fixture.state
        state.update(phase="FORWARD_APPLYING", revision=71)
        state["failed_forward"] = dict(captured=False, control=None, services=[])
        state["rollback"] = dict(attempt=0, namespace=None, marker_generation=None,
                                  control=None, marker=None, workers=[])
        control = copy.deepcopy(state["baseline"]["control"])
        control["secrets"][0].update(docker_secret_id="a" * 25,
                                     name="vp-wc-operator-" + control["generation"])
        state["forward"]["control"] = control
        state["runtime_redis"]["control"] = dict(
            runtime_generation="redis-1", secret_name="vp-control-redis-redis-1",
            docker_secret_id="b" * 25,
        )
        state["forward"]["workers"] = []
        for index, service in enumerate(sorted(HELPER["RUNTIME_AUTHORITY_SERVICES"])):
            worker = copy.deepcopy(fixture.worker)
            worker.update(service=service, generation=900 + index, commit="2" * 40,
                          image="vp-worker:deploy-222222222222", applied_stage="verified",
                          docker_service_id=f"{index + 900:024x}", target_spec_digest="a" * 64)
            for purpose, serial in (("database", 700 + index), ("admission", 800 + index)):
                worker[purpose + "_secret"] = secret(service, worker["generation"], purpose,
                                                     service + "-" + purpose, serial)
            state["forward"]["workers"].append(worker)
        fixture.write_state()
        attempt = self.root / "handshake"
        attempt.mkdir(mode=0o700)
        files = self.protocol["prepare_files"](attempt)
        workers = state["forward"]["workers"]
        # Host journal fixture tests the independently validated pin projection;
        # actual Unit1 pin decoding is exercised by the backend callback tests.
        pins = dict(version=1, transaction_id=TRANSACTION_ID, revision=71,
                    release_commit="2" * 40, workers=[dict(current=dict(
                        service_name=w["service"], generation=w["generation"],
                        release_commit=w["commit"], image_identity=w["image"]),
                        predecessor={}) for w in workers])
        pin_json = json.dumps(pins, sort_keys=True, separators=(",", ":"))
        self.binding = dict(
            version=1, attempt_id="00000000-0000-0000-0000-000000000987", replay_only=False,
            transaction_id=TRANSACTION_ID, binding_revision=71, release_commit="2" * 40,
            pin_json=pin_json, pin_sha256=hashlib.sha256(pin_json.encode()).hexdigest(),
            targets={w["service"]: [w["generation"], w["image"]] for w in workers},
            commands={s: "e" * 64 for s in self.protocol["STREAMS"].values()},
            credentials=dict(control_generation=control["generation"], redis_generation="redis-1",
                             redis_username="vp_control_1", database_secret_id="a" * 25,
                             redis_secret_id="b" * 25, database_secret_sha256="c" * 64,
                             redis_secret_sha256="d" * 64), files=files, descriptor_sha256="f" * 64,
        )
        self.checked = []
        transaction_dir = self.root / "transactions" / TRANSACTION_ID
        transaction_dir.mkdir(mode=0o700, exist_ok=True)
        self.progress_path = transaction_dir / "app-progress.json"
        self.progress_path.write_bytes(
            HELPER["_canonical"](
                dict(
                    schema=1,
                    transaction_id=TRANSACTION_ID,
                    target_commit=state["target_commit"],
                    attempted_services=[],
                    migration_state="applied",
                )
            )
        )
        self.progress_path.chmod(0o600)

    def verify(self, binding, service_id):
        self.assertEqual(binding, self.binding)
        HELPER["acquire_lock"](str(self.root), str(self.fd))
        self.checked.append(service_id)

    def prepare(self):
        HELPER["prepare_registered_reconcile"](
            str(self.root), str(self.fd), "71", self.binding, verify_owner=self.verify)
        HELPER["bind_registered_reconcile_job"](
            str(self.root), str(self.fd), "71", self.binding["attempt_id"],
            "j" * 25, self.binding["descriptor_sha256"], verify_owner=self.verify)

    def record(self):
        path = self.root / "transactions" / TRANSACTION_ID / "registered-reconcile.json"
        return json.loads(path.read_bytes())

    def request(self, action="before_eval", *, sequence=1, outcome=None, **changes):
        value = dict(version=1, attempt_id=self.binding["attempt_id"], sequence=sequence,
                     nonce=f"{sequence:032x}", binding_sha256=self.protocol["digest"](self.binding),
                     action=action, stream="vp:tasks:ffmpeg_go", command_sha256="e" * 64,
                     outcome=outcome)
        if action == "revalidate":
            value.update(stream=None, command_sha256=None)
        value.update(changes)
        with open(self.binding["files"]["request"]["path"], "ab") as handle:
            handle.write(self.protocol["canonical"](value))
            handle.flush()
            os.fsync(handle.fileno())
        return value

    def answer(self, revision="71", verify=None):
        return HELPER["answer_registered_reconcile"](
            str(self.root), str(self.fd), revision, verify_owner=verify or self.verify)

    def test_intent_is_durable_before_reply_and_no_second_authorization(self):
        self.prepare()
        request = self.request()
        observed = []
        original = self.protocol["write_reply"]

        def reply(files, value):
            observed.append(self.record()["streams"]["vp:tasks:ffmpeg_go"])
            original(files, value)

        with patch.dict(self.protocol, write_reply=reply):
            self.assertTrue(self.answer())
        self.assertEqual(observed, ["consumed"])
        self.assertTrue(self.protocol["read_reply"](self.binding["files"], request))
        self.request(sequence=2)
        with self.assertRaises(HELPER["TransactionError"]):
            self.answer()
        self.assertEqual(self.record()["sequence"], 1)

    def test_lost_reply_survives_reload_without_reissuing(self):
        self.prepare()
        self.request()
        with patch.dict(self.protocol, write_reply=lambda *args: (_ for _ in ()).throw(OSError())):
            with self.assertRaises(HELPER["TransactionError"]):
                self.answer()
        self.assertEqual(self.record()["streams"]["vp:tasks:ffmpeg_go"], "consumed")
        self.assertFalse(self.answer())
        self.assertFalse((Path(self.binding["files"]["replies"]["path"]) / "reply.json").exists())

    def test_unknown_is_durable_and_blocks_other_streams(self):
        self.prepare()
        self.request()
        self.answer()
        self.request("after_eval", sequence=2, outcome="unknown")
        self.answer()
        self.assertEqual(self.record()["streams"]["vp:tasks:ffmpeg_go"], "unknown")
        self.request(sequence=3, stream="vp:tasks:ffmpeg")
        with self.assertRaises(HELPER["TransactionError"]):
            self.answer()

    def test_failed_fsync_cannot_publish_reply(self):
        self.prepare()
        self.request()
        with patch.dict(HELPER["answer_registered_reconcile"].__globals__,
                        _write_document=lambda *args, **kw: (_ for _ in ()).throw(OSError())):
            with self.assertRaises(HELPER["TransactionError"]):
                self.answer()
        self.assertEqual(self.record()["sequence"], 0)
        self.assertFalse((Path(self.binding["files"]["replies"]["path"]) / "reply.json").exists())

    def test_binding_revision_is_frozen_but_journal_revision_can_advance(self):
        self.prepare()
        self.fixture.state["revision"] = 72
        self.fixture.write_state()
        self.request("revalidate")
        self.assertTrue(self.answer("72"))
        self.assertEqual(self.record()["binding"]["binding_revision"], 71)

    def test_fresh_owner_refusal_prevents_write(self):
        self.prepare()
        self.request()
        with self.assertRaises(HELPER["TransactionError"]):
            self.answer(verify=lambda *args: (_ for _ in ()).throw(ValueError("private")))
        self.assertEqual(self.record()["sequence"], 0)

    def test_no_overwrite_or_rebind(self):
        self.prepare()
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["prepare_registered_reconcile"](
                str(self.root), str(self.fd), "71", self.binding, verify_owner=self.verify)
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["bind_registered_reconcile_job"](
                str(self.root), str(self.fd), "71", self.binding["attempt_id"],
                "k" * 25, self.binding["descriptor_sha256"], verify_owner=self.verify)

    def test_fresh_phase_target_credentials_and_revision_refusal(self):
        for change in ("phase", "target", "credentials", "revision"):
            with self.subTest(change=change):
                original = copy.deepcopy(self.fixture.state)
                if change == "phase":
                    self.fixture.state["phase"] = "FORWARD_VERIFIED"
                elif change == "target":
                    self.fixture.state["forward"]["workers"][0]["generation"] += 1
                elif change == "credentials":
                    self.fixture.state["runtime_redis"]["control"]["docker_secret_id"] = "z" * 25
                else:
                    self.fixture.state["revision"] += 1
                self.fixture.write_state(validate=False)
                with self.assertRaises(HELPER["TransactionError"]):
                    HELPER["prepare_registered_reconcile"](
                        str(self.root), str(self.fd), "71", self.binding, verify_owner=self.verify)
                self.fixture.state = original
                self.fixture.write_state()

    def test_new_contract_cannot_verify_without_settled_registered_job(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1, baseline=None, current=None, run=None
        )
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["_set_phase"](state, "FORWARD_VERIFIED", None)

    def test_success_gate_binds_complete_receipt_pins_and_secret_cleanup(self):
        captured = dict(
            credentials=self.binding["credentials"],
            pins={
                key: self.binding[key] for key in ("pin_json", "pin_sha256", "commands")
            },
        )
        job = dict(
            state="removed",
            exit_code=0,
            service_id="j" * 25,
            task_id="t" * 25,
            pins_secret_id="p" * 25,
            result=captured,
        )
        run = {
            **job,
            "result": dict(
                outcome="already_absent",
                pin_sha256=self.binding["pin_sha256"],
                attempted_streams=[],
                request_bytes=400,
                request_sha256="a" * 64,
                service_id="j" * 25,
                task_id="t" * 25,
            ),
        }
        valid = dict(
            version=1,
            baseline=copy.deepcopy(job),
            current=copy.deepcopy(job),
            run=run,
            pins=dict(
                state="removed",
                id="p" * 25,
                name="vp-registered-pins-" + self.binding["attempt_id"],
                sha256=self.binding["pin_sha256"],
            ),
            capture_read={**self.reader_record(), "state": "removed"},
        )
        for fault in (
            None,
            "pin",
            "secret",
            "missing_secret",
            "task",
            "unknown",
            "credentials",
            "partial",
            "reader_live",
            "reader_missing",
        ):
            with self.subTest(fault=fault):
                state = copy.deepcopy(self.fixture.state)
                value = copy.deepcopy(valid)
                state["registered_reconcile"] = value
                if fault == "pin":
                    value["run"]["result"]["pin_sha256"] = "b" * 64
                elif fault == "secret":
                    value["pins"]["id"] = "b" * 25
                elif fault == "missing_secret":
                    value["pins"] = None
                elif fault == "task":
                    value["run"]["result"]["task_id"] = "b" * 25
                elif fault == "unknown":
                    value["run"]["result"]["outcome"] = "unknown"
                elif fault == "credentials":
                    value["baseline"]["result"]["credentials"][
                        "redis_secret_sha256"
                    ] = "e" * 64
                elif fault == "partial":
                    value["run"]["result"].pop("request_sha256")
                elif fault == "reader_live":
                    value["capture_read"]["state"] = "present"
                elif fault == "reader_missing":
                    value.pop("capture_read")
                if fault is None:
                    HELPER["_registered_gate"](state, success=True)
                else:
                    with self.assertRaises(HELPER["TransactionError"]):
                        HELPER["_registered_gate"](state, success=True)

    def test_older_journal_is_not_relabelled_as_registered_cleanup(self):
        state = self.fixture.state
        self.assertNotIn("registered_reconcile", state)
        HELPER["_set_phase"](state, "FORWARD_VERIFIED", None)
        self.assertNotIn("registered_reconcile", state)

    def test_unknown_live_registered_job_blocks_rollback_before_any_mutation(self):
        state = self.fixture.state
        state["failed_forward"]["captured"] = True
        state["registered_reconcile"] = dict(
            version=1,
            baseline=None,
            current=None,
            run=dict(state="created", exit_code=None),
        )
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["_set_phase"](state, "ROLLBACK_PREPARING", None)

    def test_schema_accepts_explicit_required_contract_without_legacy_rewrite(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1, baseline=None, current=None, run=None
        )
        self.assertIs(HELPER["_validate_document"](state), state)

    def test_capture_intent_precedes_launch_and_cannot_be_replaced(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1,
            baseline=None,
            current=None,
            run=None,
            capture_read=self.reader_record(),
        )
        for worker in state["forward"]["workers"]:
            worker["applied_stage"] = "prepared"
            worker["docker_service_id"] = worker["target_spec_digest"] = None
        self.fixture.write_state()
        planned = HELPER["prepare_registered_capture"](
            str(self.root),
            str(self.fd),
            "71",
            "baseline",
            "n" * 25,
            "ccttww-lap",
            "m" * 25,
        )
        job = planned["registered_reconcile"]["baseline"]
        self.assertEqual(job["state"], "planned")
        self.assertIsNone(job["service_id"])
        self.assertEqual(
            job["spec"]["TaskTemplate"]["ContainerSpec"]["Args"][-1], "--capture"
        )
        self.assertTrue(Path(job["input_file"]["path"]).is_file())
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["prepare_registered_capture"](
                str(self.root),
                str(self.fd),
                "72",
                "baseline",
                "n" * 25,
                "ccttww-lap",
                "m" * 25,
            )

    def test_baseline_cannot_be_captured_after_worker_mutation(self):
        self.fixture.state["registered_reconcile"] = dict(
            version=1, baseline=None, current=None, run=None
        )
        self.fixture.write_state()
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["prepare_registered_capture"](
                str(self.root),
                str(self.fd),
                "71",
                "baseline",
                "n" * 25,
                "ccttww-lap",
                "m" * 25,
            )

    def test_baseline_rejects_recorded_activation_even_before_stage_advance(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1, baseline=None, current=None, run=None
        )
        for worker in state["forward"]["workers"]:
            worker.update(
                applied_stage="prepared",
                docker_service_id=None,
                target_spec_digest=None,
            )
        self.fixture.write_state()
        progress = json.loads(self.progress_path.read_bytes())
        progress["attempted_services"] = ["vp-ffmpeg-worker-go-swarm"]
        self.progress_path.write_bytes(HELPER["_canonical"](progress))
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["prepare_registered_capture"](
                str(self.root),
                str(self.fd),
                "71",
                "baseline",
                "n" * 25,
                "ccttww-lap",
                "m" * 25,
            )

    def test_outer_lock_requires_held_exact_inode_and_immediate_parent(self):
        path = self.root / "sync.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, descriptor)
        function = HELPER["require_registered_outer_lock"]
        with self.assertRaises(HELPER["TransactionError"]):
            function(str(path), str(descriptor), str(os.getppid()))
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        function(str(path), str(descriptor), str(os.getppid()))
        with self.assertRaises(HELPER["TransactionError"]):
            function(str(path), str(descriptor), str(os.getpid()))
        path.unlink()
        path.touch(mode=0o600)
        with self.assertRaises(HELPER["TransactionError"]):
            function(str(path), str(descriptor), str(os.getppid()))

    def test_outer_lock_rejects_foreign_holder_on_same_inode(self):
        path = self.root / "sync.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, descriptor)
        holder = subprocess.Popen(
            [
                sys.executable, "-B", "-c",
                "import fcntl,sys; f=open(sys.argv[1], 'r+'); "
                "fcntl.flock(f, fcntl.LOCK_EX); "
                "print('locked', flush=True); sys.stdin.read()",
                str(path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(holder.stdout.readline().strip(), "locked")
            with self.assertRaises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["require_registered_outer_lock"](
                    str(path), str(descriptor), str(os.getppid())
                )
            with self.assertRaises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            _stdout, stderr = holder.communicate("", timeout=5)
        self.assertEqual(holder.returncode, 0, stderr)

    def capture_job(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1,
            baseline=None,
            current=None,
            run=None,
            capture_read=self.reader_record(),
        )
        for worker in state["forward"]["workers"]:
            worker.update(
                applied_stage="prepared",
                docker_service_id=None,
                target_spec_digest=None,
            )
        self.fixture.write_state()
        document = HELPER["prepare_registered_capture"](
            str(self.root),
            str(self.fd),
            "71",
            "baseline",
            "n" * 25,
            "ccttww-lap",
            "m" * 25,
        )
        return document["registered_reconcile"]["baseline"]

    def reader_record(self):
        return dict(
            state="present",
            id="r" * 25,
            name="vp-registered-read-" + self.fixture.state["transaction_id"],
            sha256="d" * 64,
            principal="vp_deploy_read",
        )

    def reader_intent_fixture(self):
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1, baseline=None, current=None, run=None
        )
        source = state["database_credentials"]["deploy_read"]
        path = Path(source["canonical_path"])
        path.chmod(0o600)
        path.write_text("postgresql://vp_deploy_read:private@fixture.invalid/db\n")
        path.chmod(0o400)
        for worker in state["forward"]["workers"]:
            worker.update(
                applied_stage="prepared",
                docker_service_id=None,
                target_spec_digest=None,
            )
        self.fixture.write_state()
        return path.read_bytes()

    def test_capture_reader_create_consumes_intent_before_unknown_no_retry(self):
        raw = self.reader_intent_fixture()
        calls = []

        def docker(arguments, **kwargs):
            current = self.fixture.read_state()["registered_reconcile"]["capture_read"]
            self.assertEqual(current["state"], "creating")
            self.assertEqual(current["sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(kwargs["input_bytes"], raw)
            calls.append(arguments)
            raise HELPER["TransactionError"]

        with patch.dict(
            HELPER["_registered_docker"].__globals__, _registered_docker=docker
        ):
            for _ in range(2):
                with self.assertRaises(HELPER["TransactionError"]):
                    HELPER["create_registered_read"](str(self.root), str(self.fd))
        self.assertEqual(len(calls), 1)

    def test_capture_reader_exact_create_and_cleanup_before_success(self):
        raw = self.reader_intent_fixture()
        namespace = HELPER["_registered_docker"].__globals__
        with patch.dict(namespace, _registered_docker=lambda *a, **k: "r" * 25):
            document = HELPER["create_registered_read"](str(self.root), str(self.fd))
        reader = document["registered_reconcile"]["capture_read"]
        self.assertEqual(reader["state"], "present")
        self.assertEqual(reader["sha256"], hashlib.sha256(raw).hexdigest())
        present = True
        removals = []

        def docker(arguments, **kwargs):
            nonlocal present
            if arguments[:2] == ["secret", "ls"]:
                return reader["id"] + " " + reader["name"] if present else ""
            if arguments[:2] == ["secret", "inspect"]:
                return json.dumps(
                    [
                        dict(
                            ID=reader["id"],
                            Spec=dict(
                                Name=reader["name"],
                                Labels={
                                    "vp.transaction": document["transaction_id"],
                                    "vp.credential_sha256": reader["sha256"],
                                },
                            ),
                        )
                    ]
                )
            self.assertEqual(arguments, ["secret", "rm", reader["id"]])
            removals.append(arguments)
            present = False
            return ""

        with patch.dict(namespace, _registered_docker=docker):
            result = HELPER["cleanup_registered_read"](str(self.root), str(self.fd))
            HELPER["cleanup_registered_read"](str(self.root), str(self.fd))
        self.assertEqual(
            result["registered_reconcile"]["capture_read"]["state"], "removed"
        )
        self.assertEqual(len(removals), 1)

    def test_capture_reader_cannot_retire_while_capture_job_live(self):
        self.capture_job()
        calls = []
        with patch.dict(
            HELPER["_registered_docker"].__globals__,
            _registered_docker=lambda *a, **k: calls.append(a),
        ):
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["cleanup_registered_read"](str(self.root), str(self.fd))
        self.assertEqual(calls, [])

    def test_capture_reader_rejects_replaced_original_credential_inode(self):
        raw = self.reader_intent_fixture()
        path = Path(self.fixture.state["database_credentials"]["deploy_read"]["canonical_path"])
        replacement = path.with_name("replacement-read")
        replacement.write_bytes(raw)
        replacement.chmod(0o400)
        replacement.replace(path)
        with patch.dict(HELPER["_registered_docker"].__globals__,
                        _registered_docker=lambda *a, **k: self.fail("unexpected transport")):
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["create_registered_read"](str(self.root), str(self.fd))
        self.assertNotIn("capture_read", self.fixture.read_state()["registered_reconcile"])

    def test_capture_reader_rejects_wrong_credential_principal_before_create(self):
        self.reader_intent_fixture()
        path = Path(self.fixture.state["database_credentials"]["deploy_read"]["canonical_path"])
        path.chmod(0o600)
        path.write_text("postgresql://wrong:private@fixture.invalid/db\n")
        path.chmod(0o400)
        with patch.dict(HELPER["_registered_docker"].__globals__,
                        _registered_docker=lambda *a, **k: self.fail("unexpected transport")):
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["create_registered_read"](str(self.root), str(self.fd))
        self.assertNotIn("capture_read", self.fixture.read_state()["registered_reconcile"])

    def test_capture_reader_cleanup_rejects_identity_or_label_drift(self):
        self.reader_intent_fixture()
        namespace = HELPER["_registered_docker"].__globals__
        with patch.dict(namespace, _registered_docker=lambda *a, **k: "r" * 25):
            document = HELPER["create_registered_read"](str(self.root), str(self.fd))
        reader = document["registered_reconcile"]["capture_read"]
        for fault in ("id", "name", "hash"):
            with self.subTest(fault=fault):
                def docker(arguments, **kwargs):
                    if arguments[:2] == ["secret", "ls"]:
                        return ("x" * 25 if fault == "id" else reader["id"]) + " " + reader["name"]
                    self.assertEqual(arguments, ["secret", "inspect", reader["id"]])
                    return json.dumps([dict(ID=reader["id"], Spec=dict(
                        Name="unrelated" if fault == "name" else reader["name"],
                        Labels={"vp.transaction": document["transaction_id"],
                                "vp.credential_sha256": "f" * 64 if fault == "hash" else reader["sha256"]}))])
                with patch.dict(namespace, _registered_docker=docker):
                    with self.assertRaises(HELPER["TransactionError"]):
                        HELPER["cleanup_registered_read"](str(self.root), str(self.fd))
                self.assertEqual(self.fixture.read_state()["registered_reconcile"]["capture_read"]["state"], "present")

    def test_create_intent_survives_unknown_transport_and_cannot_retry(self):
        job = self.capture_job()
        observed = []

        def docker(arguments, **kwargs):
            observed.append(arguments)
            current = self.fixture.read_state()["registered_reconcile"]["baseline"]
            self.assertEqual(current["state"], "launching")
            raise HELPER["TransactionError"]

        namespace = HELPER["launch_registered_job"].__globals__
        with patch.dict(namespace, _registered_docker=docker):
            for _ in range(2):
                with self.assertRaises(HELPER["TransactionError"]):
                    HELPER["launch_registered_job"](
                        str(self.root), str(self.fd), "baseline"
                    )
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0][:2], ["service", "create"])
        self.assertIn("--read-only", observed[0])
        self.assertIn("10001:10001", observed[0])
        self.assertIn(job["spec"]["Name"], observed[0])

    def test_observation_error_never_becomes_terminal_or_cleanup_success(self):
        self.capture_job()
        state = self.fixture.read_state()
        state["registered_reconcile"]["baseline"].update(
            state="created", service_id="s" * 25
        )
        self.fixture.state = state
        self.fixture.write_state()
        before = self.fixture.active.read_bytes()
        namespace = HELPER["observe_registered_job"].__globals__
        with patch.dict(
            namespace,
            _registered_docker=lambda *args, **kwargs: (_ for _ in ()).throw(
                HELPER["TransactionError"]()
            ),
        ):
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["observe_registered_job"](
                    str(self.root), str(self.fd), "baseline"
                )
        self.assertEqual(self.fixture.active.read_bytes(), before)

    def test_run_binding_uses_captured_pins_and_cannot_rearm(self):
        job = self.capture_job()
        state = self.fixture.read_state()
        for index, worker in enumerate(state["forward"]["workers"]):
            worker.update(
                applied_stage="verified",
                docker_service_id=f"{index + 900:025x}",
                target_spec_digest="a" * 64,
            )
        captured = dict(
            snapshot={"observed_at": "2026-09-11T06:00:00+00:00", "workers": []},
            credentials=self.binding["credentials"],
            pins={
                key: self.binding[key] for key in ("pin_json", "pin_sha256", "commands")
            },
        )
        job.update(
            state="removed",
            exit_code=0,
            service_id="s" * 25,
            task_id="t" * 25,
            result=captured,
            pins_secret_id="p" * 25,
        )
        state["registered_reconcile"].update(
            baseline=copy.deepcopy(job), current=copy.deepcopy(job)
        )
        self.fixture.state = state
        self.fixture.write_state()
        prepared = HELPER["prepare_registered_run"](
            str(self.root), str(self.fd), verify_owner=lambda *_: None
        )
        run = prepared["registered_reconcile"]["run"]
        value = self.protocol["read_input"](
            Path(run["input_file"]["path"]), run["input_file"]
        )
        self.assertEqual(value["binding"]["pin_json"], self.binding["pin_json"])
        self.assertEqual(
            value["binding"]["descriptor_sha256"], self.protocol["digest"](run["spec"])
        )
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["prepare_registered_run"](
                str(self.root), str(self.fd), verify_owner=lambda *_: None
            )

    def test_cleanup_waits_for_actual_service_and_container_absence(self):
        job = self.capture_job()
        state = self.fixture.read_state()
        job.update(state="terminal", exit_code=1, service_id="s" * 25, task_id="t" * 25)
        state["registered_reconcile"]["baseline"] = job
        self.fixture.state = state
        self.fixture.write_state()
        removed = []
        present = [True]
        running = [True]

        def docker(arguments, **kwargs):
            if arguments[:2] == ["service", "inspect"]:
                return json.dumps([{"ID": "s" * 25, "Spec": job["spec"]}])
            if arguments[:2] == ["service", "rm"]:
                present[0] = False
                removed.append(arguments[-1])
                return ""
            if arguments[:2] == ["service", "ls"]:
                return ("s" * 25 + " " + job["spec"]["Name"]) if present[0] else ""
            if arguments[:2] == ["container", "ls"]:
                return "c" * 64
            if arguments[:2] == ["container", "inspect"]:
                return json.dumps(
                    [
                        {
                            "Id": "c" * 64,
                            "Config": {
                                "Labels": {"com.docker.swarm.service.id": "s" * 25}
                            },
                            "State": {
                                "Running": running[0],
                                "Status": "running" if running[0] else "exited",
                            },
                        }
                    ]
                )
            raise AssertionError(arguments)

        namespace = HELPER["cleanup_registered_job"].__globals__
        with patch.dict(namespace, _registered_docker=docker):
            with self.assertRaises(HELPER["TransactionError"]):
                HELPER["cleanup_registered_job"](
                    str(self.root), str(self.fd), "baseline"
                )
            self.assertTrue(Path(job["input_file"]["path"]).exists())
            running[0] = False
            HELPER["cleanup_registered_job"](str(self.root), str(self.fd), "baseline")
        self.assertFalse(Path(job["input_file"]["path"]).exists())
        self.assertEqual(
            self.fixture.read_state()["registered_reconcile"]["baseline"]["state"],
            "removed",
        )
        self.assertEqual(removed, ["s" * 25])

    def test_cleanup_container_inspect_requires_actual_identity_and_terminal_state(self):
        job = {"service_id": "s" * 25, "spec": {"Name": "vp-registered-fixture"}}
        container = {
            "Id": "c" * 64,
            "Config": {"Labels": {"com.docker.swarm.service.id": "s" * 25}},
            "State": {"Running": False, "Status": "exited"},
        }

        def docker(arguments, **kwargs):
            if arguments[:2] == ["service", "ls"]:
                return ""
            if arguments[:2] == ["container", "ls"]:
                return "c" * 64
            if arguments == ["container", "inspect", "c" * 64]:
                return json.dumps([actual])
            raise AssertionError(arguments)

        function = HELPER["_registered_absent"]
        with patch.dict(function.__globals__, _registered_docker=docker):
            for status in ("exited", "dead"):
                with self.subTest(status=status):
                    actual = copy.deepcopy(container)
                    actual["State"]["Status"] = status
                    function(job)
            for fault in ("wrong_id", "swarm_ID", "wrong_service", "running", "paused"):
                with self.subTest(fault=fault):
                    actual = copy.deepcopy(container)
                    if fault == "wrong_id":
                        actual["Id"] = "d" * 64
                    elif fault == "swarm_ID":
                        actual["ID"] = actual.pop("Id")
                    elif fault == "wrong_service":
                        actual["Config"]["Labels"]["com.docker.swarm.service.id"] = "x" * 25
                    elif fault == "running":
                        actual["State"]["Running"] = True
                    else:
                        actual["State"]["Status"] = "paused"
                    with self.assertRaises(HELPER["TransactionError"]):
                        function(job)

    def test_pin_secret_uncertain_creation_is_durably_consumed_once(self):
        job = self.capture_job()
        state = self.fixture.read_state()
        job.update(
            state="removed",
            exit_code=0,
            result={
                "pins": {
                    key: self.binding[key]
                    for key in ("pin_json", "pin_sha256", "commands")
                }
            },
        )
        state["registered_reconcile"]["current"] = job
        self.fixture.state = state
        self.fixture.write_state()
        calls = []

        def docker(arguments, **kwargs):
            calls.append(arguments)
            raise HELPER["TransactionError"]

        namespace = HELPER["create_registered_pins"].__globals__
        with patch.dict(namespace, _registered_docker=docker):
            for _ in range(2):
                with self.assertRaises(HELPER["TransactionError"]):
                    HELPER["create_registered_pins"](str(self.root), str(self.fd))
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            self.fixture.read_state()["registered_reconcile"]["pins"]["state"],
            "creating",
        )

    def test_unknown_create_can_only_adopt_exact_existing_job_for_cleanup(self):
        job = self.capture_job()
        state = self.fixture.read_state()
        job["state"] = "launching"
        state["registered_reconcile"]["baseline"] = job
        self.fixture.state = state
        self.fixture.write_state()
        present = [True]
        actions = []

        def docker(arguments, **kwargs):
            actions.append(arguments[:2])
            if arguments[:2] == ["service", "ls"]:
                return ("s" * 25 + " " + job["spec"]["Name"]) if present[0] else ""
            if arguments[:2] == ["service", "inspect"]:
                return json.dumps([{"ID": "s" * 25, "Spec": job["spec"]}])
            if arguments[:2] == ["service", "rm"]:
                self.assertEqual(arguments[-1], "s" * 25)
                present[0] = False
                return ""
            if arguments[:2] == ["container", "ls"]:
                return ""
            raise AssertionError(arguments)

        with patch.dict(
            HELPER["cleanup_registered_job"].__globals__, _registered_docker=docker
        ):
            result = HELPER["cleanup_registered_job"](
                str(self.root), str(self.fd), "baseline"
            )
        self.assertEqual(result["registered_reconcile"]["baseline"]["state"], "removed")
        self.assertIsNone(result["registered_reconcile"]["baseline"]["exit_code"])
        self.assertNotIn(["service", "create"], actions)

    def test_immutable_older_pin_revision_requires_exact_managed_run_input(self):
        self.capture_job()
        state = self.fixture.read_state()
        files = self.binding["files"]
        state["registered_reconcile"]["run"] = dict(
            attempt_id=self.binding["attempt_id"],
            files=files,
            input_file=self.protocol["write_input"](
                Path(files["request"]["path"]).parent / "input.json",
                {"binding": self.binding},
            ),
            input_sha256=self.protocol["digest"]({"binding": self.binding}),
            spec={},
            service_id=None,
            task_id=None,
            state="planned",
            exit_code=None,
            result=None,
            pins_secret_id="p" * 25,
        )
        for index, worker in enumerate(state["forward"]["workers"]):
            worker.update(
                applied_stage="verified",
                docker_service_id=f"{index + 900:025x}",
                target_spec_digest="a" * 64,
            )
        self.fixture.state = state
        self.fixture.write_state()
        HELPER["prepare_registered_reconcile"](
            str(self.root),
            str(self.fd),
            str(state["revision"]),
            self.binding,
            verify_owner=lambda *_: None,
        )

    def test_finish_receipt_requires_exact_full_retained_request_bytes(self):
        self.prepare()
        self.request(
            action="finished",
            outcome="already_absent",
            stream=None,
            command_sha256=None,
        )
        self.answer()
        job = dict(
            attempt_id=self.binding["attempt_id"],
            service_id="j" * 25,
            task_id="t" * 25,
            files=self.binding["files"],
            state="terminal",
        )
        result = HELPER["registered_finish_receipt"](
            str(self.root), self.fixture.read_state(), job
        )
        self.assertEqual(result["outcome"], "already_absent")
        self.assertEqual(result["pin_sha256"], self.binding["pin_sha256"])
        with Path(self.binding["files"]["request"]["path"]).open("ab") as stream:
            stream.write(b"partial")
        with self.assertRaises(HELPER["TransactionError"]):
            HELPER["registered_finish_receipt"](
                str(self.root), self.fixture.read_state(), job
            )

    def test_ten_callbacks_use_real_owning_shell_journal_and_file_processes(self):
        self.callback_harness(owner_exit=False)

    def test_owner_death_after_durable_intent_does_not_release_consumption(self):
        self.callback_harness(owner_exit=True)

    def callback_harness(self, *, owner_exit):
        files = self.binding["files"]
        spec = self.protocol["managed_spec"](
            self.binding,
            image="vp-worker:deploy-222222222222",
            network_id="n" * 25,
            manager_node="ccttww-lap",
            manager_node_id="m" * 25,
            pins_secret_id="p" * 25,
        )
        self.binding["descriptor_sha256"] = self.protocol["digest"](spec)
        payload = {"binding": self.binding}
        input_file = self.protocol["write_input"](
            Path(files["request"]["path"]).parent / "input.json", payload
        )
        state = self.fixture.state
        state["registered_reconcile"] = dict(
            version=1,
            baseline=None,
            current=None,
            run=dict(
                attempt_id=self.binding["attempt_id"],
                files=files,
                input_file=input_file,
                input_sha256=self.protocol["digest"](payload),
                spec=spec,
                service_id="j" * 25,
                task_id="t" * 25,
                state="created",
                exit_code=None,
                result=None,
                pins_secret_id="p" * 25,
            ),
        )
        self.fixture.write_state()
        self.prepare()
        driver = self.root / "driver.py"
        driver.write_text("""import json,os,runpy,sys
h=runpy.run_path(os.environ["REAL_HELPER"])
def docker(args,**kwargs):
    doc=json.load(open(os.environ["ADMISSION_ROOT"]+"/transactions/active.json"))
    job=doc["registered_reconcile"]["run"]
    if args[:2]==["service","inspect"]: return json.dumps([{"ID":job["service_id"],"Spec":job["spec"]}])
    if args[:2]==["service","ps"]: return job["task_id"]
    if args[0]=="inspect":
        done=os.path.exists(os.environ["ADMISSION_ROOT"]+"/client.done")
        return json.dumps([{"ID":job["task_id"],"ServiceID":job["service_id"],"NodeID":"m"*25,"Spec":job["spec"]["TaskTemplate"],
                           "Status":{"State":"complete" if done else "running","ContainerStatus":{"ExitCode":0}}}])
    raise h["TransactionError"]()
h["registered_job_action"].__globals__["_registered_docker"]=docker
try:
    status=h["main"](sys.argv[1:])
    if os.environ.get("OWNER_EXIT")=="true" and sys.argv[1:2]==["registered-job"]:
        root=os.environ["ADMISSION_ROOT"]
        doc=json.load(open(root+"/transactions/active.json"))
        record=json.load(open(root+"/transactions/"+doc["transaction_id"]+"/registered-reconcile.json"))
        if record["sequence"]==2: os.kill(os.getppid(),9)
    sys.exit(status)
except h["TransactionError"]: sys.exit(1)
""")
        client = self.root / "client.py"
        client.write_text("""import json,os,runpy,subprocess,sys,time
from pathlib import Path
p=runpy.run_path(os.environ["PROTOCOL"])
b=json.load(open(os.environ["INPUT"]))["binding"]
actions=[]
for stream in b["commands"]:
    actions.extend([("revalidate",None,None),("before_eval",stream,None),("after_eval",stream,"retired")])
actions.append(("revalidate",None,None))
offset=0
started=time.monotonic()
for sequence,(action,stream,outcome) in enumerate(actions+[("finished",None,"reconciled")],1):
    request=dict(version=1,attempt_id=b["attempt_id"],sequence=sequence,nonce=f"{sequence:032x}",
                 binding_sha256=p["digest"](b),action=action,stream=stream,
                 command_sha256=None if stream is None else b["commands"][stream],outcome=outcome)
    value=p["canonical"](dict(files=b["files"],request=request,offset=offset))
    result=subprocess.run([sys.executable,os.environ["PROTOCOL"],"--file-exchange"],input=value,
                          stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=2)
    assert result.returncode==0 and result.stdout==b"ok\\n"
    offset+=len(p["canonical"](request))
    if sequence==10:
        Path(os.environ["ADMISSION_ROOT"]+"/elapsed").write_text(str(time.monotonic()-started))
Path(os.environ["ADMISSION_ROOT"]+"/client.done").touch()
""")
        environment = dict(
            PATH=os.environ["PATH"],
            ADMISSION_ROOT=str(self.root),
            REAL_HELPER=str(HELPER_PATH),
            DRIVER=str(driver),
            CLIENT=str(client),
            EXTENSION=str(HELPER_PATH.with_name("deploy-sync-extension.sh")),
            INPUT=str(input_file["path"]),
            LOCK_FD=str(self.fd),
            PROTOCOL=str(
                HELPER_PATH.parents[2]
                / "backend/app/services/registered_consumer_reconcile_job.py"
            ),
            OWNER_EXIT="true" if owner_exit else "false",
        )
        script = r"""
set -eu
REPO_ROOT=/unused
log() { :; }
source "$EXTENSION"
VP_WORKER_ADMISSION_TRANSACTION_HELPER="$DRIVER"
ROOT="$ADMISSION_ROOT"
exec 9<>"$ROOT/sync.lock"
chmod 600 "$ROOT/sync.lock"
python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)'
eval "exec 19<&$LOCK_FD"
VP_WORKER_ADMISSION_LOCK_HELD=true
VP_WORKER_ADMISSION_LOCK_DEPTH=1
VP_WORKER_ADMISSION_LOCK_ROOT="$ADMISSION_ROOT"
VP_WORKER_ADMISSION_LOCK_FD=19
vp_worker_admission_capture_bashpid
VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID="$VP_WORKER_ADMISSION_CURRENT_BASHPID"
VP_WORKER_ADMISSION_LOCK_TOKEN="$(python3 "$DRIVER" lock-token "$ADMISSION_ROOT" 19)"
status=0
vp_registered_reconcile_wait run || status=$?
[[ "$status" -eq 0 ]] || exit "$status"
"""
        parent = subprocess.Popen(
            ["bash", "-c", script],
            env=environment,
            pass_fds=(self.fd,),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        client_process = subprocess.Popen(
            [sys.executable, str(client)],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            _, errors = parent.communicate(timeout=12)
            client_process.wait(timeout=3)
            if owner_exit:
                self.assertEqual(parent.returncode, -9)
                self.assertNotEqual(client_process.returncode, 0)
                self.assertEqual(self.record()["sequence"], 2)
                self.assertEqual(
                    list(self.record()["streams"].values()).count("consumed"), 1
                )
                with self.assertRaises(HELPER["TransactionError"]):
                    HELPER["launch_registered_job"](str(self.root), str(self.fd), "run")
                return
            self.assertEqual(parent.returncode, 0, errors)
            self.assertEqual(client_process.returncode, 0)
        finally:
            for process in (parent, client_process):
                if process.poll() is None:
                    process.kill()
                process.wait()
        elapsed = float((self.root / "elapsed").read_text())
        self.assertLess(elapsed, 6)
        self.assertEqual(self.record()["sequence"], 11)
        self.assertEqual(set(self.record()["streams"].values()), {"retired"})
        self.assertEqual(
            self.fixture.read_state()["registered_reconcile"]["run"]["state"],
            "terminal",
        )
        print(f"ten owning-shell callback exchanges: {elapsed:.4f}s")


if __name__ == "__main__":
    unittest.main()
