"""Failed-control observations and bounded recovery, with no external services."""

import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys

import pytest


HELPER_PATH = Path(__file__).resolve().parents[1] / "deploy/swarm/worker-admission-transaction.py"
HELPER = runpy.run_path(str(HELPER_PATH))
ROOT = "/owned/sync"
NETWORK = "h" * 25
WORKERS = (
    "vp-ffmpeg-worker-go-swarm", "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm", "vp-youtube-publisher-swarm",
)
ATTEMPTED = ["vp-api-swarm", "vp-frontend-swarm", "vp-autoflow-api-swarm"]


def sha(value):
    return hashlib.sha256(value).hexdigest()


def control(commit, serial):
    generation = "c-" + commit[:20]
    purposes = (
        "operator", "orchestrator", "staging-janitor", "staging-minio-access",
        "staging-minio-secret", "worker-minio-access", "worker-minio-secret",
    )
    return dict(
        generation=generation, image="vp-worker:deploy-" + commit[:12],
        manifest_sha256="f" * 64,
        secrets=[dict(
            name=f"vp-wc-{purpose}-{generation}", docker_secret_id=f"{serial+i:025x}",
            service="vp-worker-control", generation=generation, purpose=purpose,
        ) for i, purpose in enumerate(purposes)],
    )


def config(selection):
    names = {item["purpose"]: item["name"] for item in selection["secrets"]}
    return (
        "VERSION=2\n"
        f"GENERATION={selection['generation']}\nIMAGE={selection['image']}\n"
        f"NETWORK=vp-pipeline-net\nNETWORK_ID={NETWORK}\n"
        f"DATABASE_SECRET={names['staging-janitor']}\n"
        f"MINIO_ACCESS_SECRET={names['staging-minio-access']}\n"
        f"MINIO_SECRET_SECRET={names['staging-minio-secret']}\n"
        "EVIDENCE_VOLUME=vp-staging-janitor-evidence\nMANAGER_NODE=ccttww-lap\n"
    ).encode()


def blocks():
    marker_root = ROOT + "/state/worker-redis-marker-control"
    prefix = (
        f"VP_WORKER_REDIS_MARKER_CONFIG_FILE={marker_root}/control.conf "
        f"VP_WORKER_REDIS_MARKER_STATE_DIR={marker_root}/status "
        f"VP_WORKER_REDIS_MARKER_LOCK_DIR={marker_root}/locks "
        f"{ROOT}/bin/worker-redis-marker-control.sh"
    )
    marker = (
        "# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL\n"
        f"* * * * * {prefix} readiness >> {ROOT}/logs/worker-redis-marker-readiness.log 2>&1\n"
        f"*/5 * * * * {prefix} janitor >> {ROOT}/logs/worker-redis-marker-janitor.log 2>&1\n"
        "# END VIDEOPROCESS WORKER REDIS MARKER CONTROL\n"
    ).encode()
    staging = (
        "# BEGIN VIDEOPROCESS STAGING JANITOR\n"
        f"*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE={ROOT}/state/vp-worker-admission/staging-object-janitor.conf "
        f"{ROOT}/bin/vp-staging-object-janitor-run.sh >> {ROOT}/logs/vp-staging-object-janitor.log 2>&1\n"
        "# END VIDEOPROCESS STAGING JANITOR\n"
    ).encode()
    return marker, staging


@pytest.fixture
def case():
    baseline, forward = control("1" * 40, 100), control("2" * 40, 200)
    marker, staging = blocks()
    foreign = b"# retained operator entry\n15 * * * * /owned/audit\n"
    baseline_cron = marker + foreign + staging
    captured_cron = foreign + staging + marker
    document = dict(
        phase="CANDIDATE_RESTORE_REQUIRED", operation=None,
        baseline=dict(kind="managed", captured=True, control=baseline),
        forward=dict(control=forward, workers=[dict(
            service=name, applied_stage="prepared", docker_service_id=None,
            target_spec_digest=None,
        ) for name in WORKERS]),
        failed_forward=dict(captured=True, control=dict(
            generation=forward["generation"], image=forward["image"],
            config_sha256=sha(config(baseline)), cron_sha256=sha(captured_cron),
        ), services=[dict(name=name) for name in ATTEMPTED]),
    )
    progress = dict(attempted_services=ATTEMPTED.copy(), migration_state="applied")
    return dict(document=document, progress=progress, sync_root=ROOT,
                network_id=NETWORK, baseline_cron=baseline_cron,
                current_cron=foreign + marker + staging,
                current_config=config(forward), observation=None)


def recover(case, **changes):
    return HELPER["_recover_failed_control"](**(case | changes))


def test_capture_observes_baseline_not_prepared_forward_labels(case):
    document = case["document"]
    identity = HELPER["_observe_failed_control"](
        document, ROOT, NETWORK, config(document["baseline"]["control"]),
        case["baseline_cron"],
    )
    assert identity["generation"] == document["baseline"]["control"]["generation"]
    assert identity["image"] == document["baseline"]["control"]["image"]
    assert identity["cron_sha256"] == sha(case["baseline_cron"])


def test_capture_requires_managed_block_markers_at_line_boundaries(case):
    with pytest.raises(HELPER["TransactionError"]):
        HELPER["_observe_failed_control"](
            case["document"], ROOT, NETWORK, config(case["document"]["baseline"]["control"]),
            b"# not a block boundary " + case["baseline_cron"],
        )


def test_exact_legacy_hybrid_selects_baseline_without_changing_history(case):
    before = copy.deepcopy(case)
    assert recover(case) == case["document"]["baseline"]["control"]
    assert case == before
    assert recover(case) == recover(case)


def test_correct_baseline_capture_uses_same_narrow_cron_proof(case):
    selected = case["document"]["baseline"]["control"]
    case["document"]["failed_forward"]["control"].update(
        generation=selected["generation"], image=selected["image"],
    )
    assert recover(case) == selected


@pytest.mark.parametrize("fault", [
    "config_hash", "image", "generation", "worker_attempted", "worker_applied",
    "missing_worker", "extra_attempt", "service_set", "migration", "phase",
    "operation", "foreign_change", "foreign_reorder", "duplicate_block",
    "unknown_command", "altered_block", "baseline_tamper", "wrong_full_hash",
    "unknown_current_config", "wrong_network",
])
def test_legacy_compatibility_rejects_drift(case, fault):
    state = case["document"]
    expected = state["failed_forward"]["control"]
    if fault in {"config_hash", "wrong_full_hash"}:
        expected["config_sha256" if fault == "config_hash" else "cron_sha256"] = "0" * 64
    elif fault in {"image", "generation"}:
        expected[fault] = "unrelated"
    elif fault == "worker_attempted":
        case["progress"]["attempted_services"].append(WORKERS[0])
    elif fault == "worker_applied":
        state["forward"]["workers"][0]["applied_stage"] = "applied"
    elif fault == "missing_worker":
        state["forward"]["workers"].pop()
    elif fault == "extra_attempt":
        case["progress"]["attempted_services"].append("vp-channel-agent-runner-swarm")
    elif fault == "service_set":
        state["failed_forward"]["services"].pop()
    elif fault == "migration":
        case["progress"]["migration_state"] = "pending"
    elif fault == "phase":
        state["phase"] = "FORWARD_VERIFIED"
    elif fault == "operation":
        state["operation"] = {"kind": "PROMOTE_WORKERS"}
    elif fault == "foreign_change":
        case["current_cron"] += b"* * * * * /unknown\n"
    elif fault == "foreign_reorder":
        case["current_cron"] = case["current_cron"].replace(
            b"# retained operator entry\n15 * * * * /owned/audit\n",
            b"15 * * * * /owned/audit\n# retained operator entry\n",
        )
    elif fault == "duplicate_block":
        case["current_cron"] += blocks()[1]
    elif fault == "unknown_command":
        case["current_cron"] += f"* * * * * {ROOT}/bin/vp-staging-object-janitor-run.sh\n".encode()
    elif fault == "altered_block":
        case["current_cron"] = case["current_cron"].replace(b"*/5", b"*/2")
    elif fault == "baseline_tamper":
        case["baseline_cron"] += b"# unknown\n"
    elif fault == "unknown_current_config":
        case["current_config"] += b"UNKNOWN=yes\n"
    elif fault == "wrong_network":
        case["network_id"] = "z" * 25
    with pytest.raises(HELPER["TransactionError"]):
        recover(case)


def test_new_observation_binds_full_original_cron_with_later_owned_reordering(case):
    state = case["document"]
    selected = state["forward"]["control"]
    original = case["current_cron"] + b"# normal installed soak watch\n"
    state["failed_forward"]["control"].update(
        generation=selected["generation"], image=selected["image"],
        config_sha256=sha(config(selected)), cron_sha256=sha(original),
    )
    observation = dict(config=config(selected).decode(), cron=original.decode())
    current = original.replace(blocks()[1], b"") + blocks()[1]
    assert recover(case, current_cron=current, observation=observation) == selected
    with pytest.raises(HELPER["TransactionError"]):
        recover(case, current_cron=current + b"# foreign drift\n", observation=observation)


@pytest.fixture
def native(tmp_path, case):
    sync = tmp_path / "sync"
    root = sync / "state/vp-worker-admission"
    root.mkdir(parents=True, mode=0o700)
    credentials = {}
    for purpose in HELPER["DATABASE_PURPOSES"]:
        path = root / purpose
        path.write_text("postgresql://fixture:unused@invalid/fixture\n")
        path.chmod(0o400)
        credentials[purpose] = HELPER["_capture_credential"](str(path), "vp_" + purpose)
    state = HELPER["_new_document"](
        target_commit="2" * 40, target_backend_image="vp-backend:deploy-222222222222",
        target_go_image="vp-go:deploy-222222222222", namespace="2" * 40,
        baseline_kind="managed", credentials=credentials,
    )
    state.update(phase="FORWARD_APPLYING", revision=77)
    state["baseline"].update(captured=True, control=case["document"]["baseline"]["control"])
    state["baseline"]["services"] = [dict(
        name=name, existed=True, docker_service_id=f"{index:025x}",
        image="vp-worker:deploy-111111111111", spec_digest="a" * 64,
    ) for index, name in enumerate(sorted(HELPER["APP_SERVICES"]), 300)]
    state["forward"]["control"] = case["document"]["forward"]["control"]
    for index, name in enumerate(WORKERS, 400):
        worker = dict(
            service=name, generation=index, commit="2" * 40,
            image="vp-worker:deploy-222222222222", applied_stage="prepared",
            docker_service_id=None, target_spec_digest=None,
        )
        for offset, purpose in enumerate(("database", "admission")):
            worker[purpose + "_secret"] = dict(
                name=f"vp-{purpose}-{index}", docker_secret_id=f"{index*2+offset:025x}",
                purpose=purpose, service=name, generation=str(index),
            )
        state["forward"]["workers"].append(worker)
    HELPER["_validate_document"](state)
    transaction = root / "transactions" / state["transaction_id"]
    transaction.mkdir(parents=True, mode=0o700)
    transaction.parent.chmod(0o700)
    active = transaction.parent / "active.json"
    active.write_bytes(HELPER["_canonical"](state))
    active.chmod(0o600)
    progress = case["progress"] | dict(
        schema=1, transaction_id=state["transaction_id"], target_commit="2" * 40,
    )
    path = transaction / "app-progress.json"
    path.write_bytes(HELPER["_canonical"](progress))
    path.chmod(0o600)
    cron = case["current_cron"].replace(ROOT.encode(), str(sync).encode())
    config_path = root / "staging-object-janitor.conf"
    config_path.write_bytes(config(state["baseline"]["control"]))
    config_path.chmod(0o600)
    descriptor = os.open(root / "transaction.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    def call(mode, data=cron):
        return subprocess.run(
            [sys.executable, "-I", "-B", str(HELPER_PATH), "failed-control",
             str(root), str(descriptor), mode, NETWORK],
            input=data, capture_output=True, pass_fds=(descriptor,), timeout=10,
            env={"PATH": os.environ["PATH"]},
        )
    yield dict(call=call, root=root, sync=sync, state=state, active=active,
               transaction=transaction, config_path=config_path, fd=descriptor, cron=cron)
    os.close(descriptor)


def test_native_observation_is_private_immutable_and_does_not_change_journal(native):
    before = native["active"].read_bytes()
    result = native["call"]("observe")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["generation"] == native["state"]["baseline"]["control"]["generation"]
    observation = native["transaction"] / "failed-control-observation.json"
    original = observation.read_bytes()
    assert observation.stat().st_mode & 0o777 == 0o600
    assert native["call"]("observe").returncode == 0
    native["config_path"].write_bytes(config(native["state"]["forward"]["control"]))
    assert native["call"]("observe").returncode != 0
    assert observation.read_bytes() == original
    assert native["active"].read_bytes() == before


def test_native_observation_rejects_a_contending_writer_lock(native):
    fcntl.flock(native["fd"], fcntl.LOCK_UN)
    other = os.open(native["root"] / "transaction.lock", os.O_RDWR)
    try:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert native["call"]("observe").returncode == 75
    finally:
        os.close(other)
    assert not (native["transaction"] / "failed-control-observation.json").exists()


def test_native_recovery_preserves_capture_through_real_transition_cas(native):
    result = native["call"]("observe")
    assert result.returncode == 0, result.stderr
    captured = json.loads(result.stdout)
    state = native["state"]
    state["failed_forward"] = dict(
        captured=True, control=captured,
        services=[item for item in state["baseline"]["services"] if item["name"] in ATTEMPTED],
    )
    state["phase"] = "CANDIDATE_RESTORE_REQUIRED"
    HELPER["_validate_document"](state)
    native["active"].write_bytes(HELPER["_canonical"](state))
    assert native["call"]("select").returncode == 0
    assert native["call"]("verify").returncode == 0
    result = subprocess.run(
        [sys.executable, "-I", "-B", str(HELPER_PATH), "transition", str(native["root"]),
         str(native["fd"]), "77", "CANDIDATE_RESTORING"],
        capture_output=True, pass_fds=(native["fd"],), timeout=10,
        env={"PATH": os.environ["PATH"]},
    )
    assert result.returncode == 0, result.stderr
    after = json.loads(native["active"].read_bytes())
    assert after["revision"] == 78
    assert after["failed_forward"] == state["failed_forward"]
    assert native["call"]("verify").returncode == 0


def shell(native, script):
    cron_path = native["transaction"] / "fixture-crontab"
    cron_path.write_bytes(native["cron"])
    env = {"PATH": os.environ["PATH"], "TMPDIR": str(native["transaction"])}
    env.update(
        EXTENSION=str(HELPER_PATH.with_name("deploy-sync-extension.sh")),
        ROOT=str(native["sync"]), LOCK_FD=str(native["fd"]),
        CRON_FILE=str(cron_path), CONFIG_FILE=str(native["config_path"]),
        BASELINE=json.dumps(native["state"]["baseline"]["control"]),
        FORWARD=json.dumps(native["state"]["forward"]["control"]),
    )
    setup = r'''
REPO_ROOT=/unused
source "$EXTENSION"
log() { :; }
docker() { echo unexpected_docker >&2; return 91; }
ssh() { return 92; }
remote_sh() { return 93; }
curl() { return 94; }
crontab() { [[ "$1" == -l ]] && cat "$CRON_FILE"; }
vp_require_pipeline_network_identity() { VP_PIPELINE_NETWORK_ID=hhhhhhhhhhhhhhhhhhhhhhhhh; }
exec 19>&$LOCK_FD
VP_WORKER_ADMISSION_LOCK_FD=19
VP_WORKER_ADMISSION_LOCK_ROOT="$ROOT/state/vp-worker-admission"
VP_WORKER_ADMISSION_LOCK_HELD=true
VP_WORKER_ADMISSION_LOCK_DEPTH=1
vp_worker_admission_capture_bashpid
VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID="$VP_WORKER_ADMISSION_CURRENT_BASHPID"
VP_WORKER_ADMISSION_LOCK_TOKEN="$(python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" lock-token "$VP_WORKER_ADMISSION_LOCK_ROOT" 19)"
VP_WORKER_CONTROL_PREPARED=true
VP_WORKER_CONTROL_GENERATION=c-22222222222222222222
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-worker:deploy-222222222222
UPDATE_SERVICES=1
'''
    return subprocess.run(
        ["bash", "-eu", "-c", setup + script], env=env, text=True,
        capture_output=True, pass_fds=(native["fd"],), timeout=20,
    )


def test_real_apply_failure_before_janitor_capture_reports_installed_baseline(native):
    result = shell(native, r'''
vp_worker_admission_transition_to() { :; }
vp_worker_admission_advance_migration_state() { :; }
vp_update_app_runtime_service() { [[ "$1" != vp-autoflow-api-swarm ]]; }
http_health() { :; }
vp_run_worker_registration_migration() { :; }
vp_require_channelops_migration_head() { :; }
vp_prepare_worker_redis_marker_controls() { :; }
vp_prepare_worker_admission() { :; }
vp_install_staging_object_janitor() { echo unexpected_install >&2; return 97; }
if vp_apply_app_services api frontend backend runner go worker; then exit 98; fi
vp_worker_admission_failed_forward_control_json
''')
    assert result.returncode == 0, result.stderr
    assert not result.stderr
    assert json.loads(result.stdout)["generation"] == native["state"]["baseline"]["control"]["generation"]


def test_shell_reinstall_uses_actual_baseline_with_forward_authority_unchanged(native):
    result = native["call"]("observe")
    assert result.returncode == 0
    state = native["state"]
    state["failed_forward"].update(captured=True, control=json.loads(result.stdout))
    state["phase"] = "CANDIDATE_RESTORE_REQUIRED"
    native["active"].write_bytes(HELPER["_canonical"](state))
    original = native["active"].read_bytes()
    result = shell(native, r'''
vp_managed_secret_id() {
  python3 -c 'import json,os,sys; print(next(x["docker_secret_id"] for x in json.loads(os.environ["BASELINE"])["secrets"] if x["name"] == sys.argv[1]))' "$1"
}
vp_install_staging_object_janitor() {
  [[ "$1" == vp-worker:deploy-111111111111 ]]
  [[ "$VP_WORKER_CONTROL_GENERATION" == c-11111111111111111111 ]]
  [[ "$VP_STAGING_JANITOR_DATABASE_SECRET" == vp-wc-staging-janitor-c-11111111111111111111 ]]
  echo installed_baseline
}
vp_reinstall_failed_forward_control
[[ "$VP_WORKER_CONTROL_GENERATION" == c-22222222222222222222 ]]
[[ "$VP_WORKER_ADMISSION_CONTROL_IMAGE" == vp-worker:deploy-222222222222 ]]
''')
    assert result.returncode == 0, result.stderr
    assert result.stdout == "installed_baseline\n"
    assert native["active"].read_bytes() == original


def test_native_legacy_preimage_and_current_control_are_checked_independently(native, case):
    state = native["state"]
    baseline_cron = case["baseline_cron"].replace(ROOT.encode(), str(native["sync"]).encode())
    marker = blocks()[0].replace(ROOT.encode(), str(native["sync"]).encode())
    captured_cron = baseline_cron.replace(marker, b"", 1) + marker
    state["failed_forward"] = dict(
        captured=True, control=dict(
            generation=state["forward"]["control"]["generation"],
            image=state["forward"]["control"]["image"],
            config_sha256=sha(config(state["baseline"]["control"])),
            cron_sha256=sha(captured_cron),
        ), services=[item for item in state["baseline"]["services"] if item["name"] in ATTEMPTED],
    )
    state["phase"] = "CANDIDATE_RESTORE_REQUIRED"
    HELPER["_validate_document"](state)
    native["active"].write_bytes(HELPER["_canonical"](state))
    original = native["active"].read_bytes()
    marker_root = native["root"].parent / "worker-redis-marker-control"
    marker_root.mkdir(mode=0o700)
    directory = marker_root
    for child in ("transactions", state["transaction_id"], "baseline-managed-state"):
        directory = directory / child
        directory.mkdir(mode=0o700)
    (directory / "captured").write_bytes(b"VERSION=1\n")
    (directory / "captured").chmod(0o600)
    (directory / "crontab").write_bytes(baseline_cron)
    (directory / "crontab").chmod(0o664)
    native["config_path"].write_bytes(config(state["forward"]["control"]))
    selected = native["call"]("select")
    assert selected.returncode == 0, selected.stderr
    assert json.loads(selected.stdout) == state["baseline"]["control"]
    assert native["call"]("verify").returncode != 0
    native["config_path"].write_bytes(config(state["baseline"]["control"]))
    assert native["call"]("verify").returncode == 0
    (directory / "crontab").write_bytes(baseline_cron + b"# foreign drift\n")
    assert native["call"]("select").returncode != 0
    assert native["active"].read_bytes() == original
    assert not (native["transaction"] / "failed-control-observation.json").exists()


def test_shell_secret_id_drift_denies_install_before_effect(native):
    observed = native["call"]("observe")
    state = native["state"]
    state["failed_forward"].update(captured=True, control=json.loads(observed.stdout))
    state["phase"] = "CANDIDATE_RESTORE_REQUIRED"
    native["active"].write_bytes(HELPER["_canonical"](state))
    result = shell(native, r'''
vp_managed_secret_id() { printf '%s\n' zzzzzzzzzzzzzzzzzzzzzzzzz; }
vp_install_staging_object_janitor() { echo unexpected_install; }
vp_reinstall_failed_forward_control
''')
    assert result.returncode != 0
    assert not result.stdout


def test_native_observation_rejects_config_symlink(native):
    path = native["config_path"]
    saved = path.with_suffix(".saved")
    path.rename(saved)
    path.symlink_to(saved)
    result = native["call"]("observe")
    assert result.returncode != 0
    assert not result.stdout


def test_native_observation_cannot_attach_different_bytes_to_existing_capture(native):
    state = native["state"]
    state["failed_forward"].update(captured=True, control=dict(
        generation=state["forward"]["control"]["generation"],
        image=state["forward"]["control"]["image"],
        config_sha256=sha(config(state["baseline"]["control"])),
        cron_sha256=sha(native["cron"]),
    ))
    native["active"].write_bytes(HELPER["_canonical"](state))
    assert native["call"]("observe").returncode != 0
    assert not (native["transaction"] / "failed-control-observation.json").exists()


def test_missing_config_fails_without_raw_error_or_snapshot(native):
    native["config_path"].unlink()
    result = native["call"]("observe")
    assert result.returncode != 0
    assert not result.stdout
    assert not result.stderr


def test_observation_rejects_json_expansion_before_creating_unreadable_snapshot(native):
    cron = native["cron"] + b"# " + b"\t" * (HELPER["MAX_DOCUMENT_BYTES"] // 2) + b"\n"
    assert len(cron) < HELPER["MAX_DOCUMENT_BYTES"]
    result = native["call"]("observe", cron)
    assert result.returncode != 0
    assert not result.stdout
    assert not result.stderr
    assert not (native["transaction"] / "failed-control-observation.json").exists()


@pytest.mark.parametrize("start,end", [
    ('\n(\n  failed_control_root=', '\n(\n  VP_WORKER_CONTROL_GENERATION='),
    ('\n(\n  compensation_calls=', '\n(\n  candidate_calls='),
])
def test_existing_rollback_control_fixtures(tmp_path, start, end):
    source = (HELPER_PATH.parents[2] / "tests/test_worker_admission_rollback.sh").read_text()
    offset = source.index(start)
    block = source[offset:source.index(end, offset + len(start))]
    header = r'''
REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
UPDATE_SERVICES=1
mkdir -p "$ROOT"
log() { :; }
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"
docker() { echo unexpected_docker >&2; return 91; }
ssh() { return 92; }
remote_sh() { return 93; }
curl() { return 94; }
'''
    result = subprocess.run(
        ["bash", "-eu", "-c", header + block], capture_output=True, text=True,
        env={"PATH": os.environ["PATH"], "TMPDIR": str(tmp_path),
             "TEST_ROOT": str(tmp_path), "ROOT_DIR": str(HELPER_PATH.parents[2])},
        timeout=40,
    )
    assert result.returncode == 0, result.stderr
