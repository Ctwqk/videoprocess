"""Execute deployment shell functions with no external operations available."""

import json
import os
from pathlib import Path
import subprocess
import shlex
import sys

import pytest


EXTENSION = (
    Path(__file__).resolve().parents[1] / "deploy/swarm/deploy-sync-extension.sh"
)
SERVICE_ID = "a" * 25
SECRET_ID = "b" * 25
GENERATION = "c-0123456789abcdef0123"
SECRET = f"vp-wc-orchestrator-{GENERATION}"


def run(script, **data):
    env = {"PATH": os.environ["PATH"], "EXTENSION": str(EXTENSION)}
    env.update(
        {
            key: json.dumps(value) if not isinstance(value, str) else value
            for key, value in data.items()
        }
    )
    return subprocess.run(
        [
            "bash",
            "-eu",
            "-c",
            r"""
REPO_ROOT=/unused
log() { :; }
source "$EXTENSION"
identity_function="$(declare -f vp_autoflow_control_identity)"
docker() { echo 'unexpected docker call' >&2; return 91; }
remote_sh() { echo 'unexpected remote call' >&2; return 92; }
ssh() { return 93; }
curl() { return 94; }
UPDATE_SERVICES=1
VP_WORKER_ADMISSION_LOCK_HELD=true
VP_WORKER_CONTROL_GENERATION=c-0123456789abcdef0123
vp_autoflow_control_identity() { printf '%s\n' 'vp-wc-orchestrator-c-0123456789abcdef0123|bbbbbbbbbbbbbbbbbbbbbbbbb|c-0123456789abcdef0123'; }
"""
            + script,
        ],
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )


def spec():
    return {
        "User": "",
        "Env": ["KEEP=yes", "WORKER_ORCHESTRATOR_CONTROL_GENERATION=old"],
        "Secrets": [
            {
                "SecretName": "unrelated",
                "SecretID": "x" * 25,
                "File": {"Name": "other", "UID": "0", "GID": "0", "Mode": 256},
            },
            {
                "SecretName": "vp-wc-orchestrator-old",
                "SecretID": "y" * 25,
                "File": {
                    "Name": "worker-orchestrator-database-url",
                    "UID": "0",
                    "GID": "0",
                    "Mode": 256,
                },
            },
        ],
    }


def test_update_binds_only_selected_secret_and_preserves_unrelated_runtime():
    result = run(
        r"""
vp_service_values() { printf '%s\n' "$SPEC"; }
docker() { [[ "$1 $2" == 'image inspect' ]] && printf '\n'; }
vp_autoflow_runtime_update_args aaaaaaaaaaaaaaaaaaaaaaaaa vp-backend-api:deploy-0123456789ab
""",
        SPEC=spec(),
    )
    assert result.returncode == 0, result.stderr
    args = result.stdout.splitlines()
    assert (
        args[args.index("--secret-add") + 1]
        == f"source={SECRET_ID},target=worker-orchestrator-database-url,uid=0,gid=0,mode=0400"
    )
    assert args[args.index("--secret-rm") + 1] == "vp-wc-orchestrator-old"
    assert "unrelated" not in args and "KEEP" not in args
    assert "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + GENERATION in args


@pytest.mark.parametrize(
    "fault", ["duplicate_target", "unknown_target", "unknown_user", "inspect_error"]
)
def test_update_rejects_ambiguous_mount_or_identity(fault):
    value = spec()
    if fault == "duplicate_target":
        value["Secrets"].append(value["Secrets"][-1])
    if fault == "unknown_target":
        value["Secrets"][-1]["SecretName"] = "operator-credential"
    if fault == "unknown_user":
        value["User"] = "unexpected-user"
    result = run(
        r"""
vp_service_values() { [[ "$FAULT" != inspect_error ]] && printf '%s\n' "$SPEC"; }
docker() { [[ "$1 $2" == 'image inspect' ]] && printf '\n'; }
vp_autoflow_runtime_update_args aaaaaaaaaaaaaaaaaaaaaaaaa vp-backend-api:deploy-0123456789ab
""",
        SPEC=value,
        FAULT=fault,
    )
    assert result.returncode == 1


@pytest.mark.parametrize(
    "state,allowed",
    [("running", False), ("shutdown", True), ("orphaned", False), ("pending", False)],
)
def test_retirement_accounts_for_old_autoflow_tasks(state, allowed):
    result = run(
        r"""
docker() {
  if [[ "$1 $2" == 'service inspect' ]]; then printf '\n'
  elif [[ "$1 $2" == 'service ps' ]]; then printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaa
  elif [[ "$1" == inspect ]]; then printf '%s\n' "$TASKS"
  else return 91; fi
}
vp_worker_control_generation_unused c-0123456789abcdef0123
""",
        TASKS=[
            {
                "ServiceID": SERVICE_ID,
                "Status": {"State": state},
                "Spec": {
                    "ContainerSpec": {
                        "Secrets": [{"SecretName": SECRET, "SecretID": SECRET_ID}]
                    }
                },
            }
        ],
    )
    assert (result.returncode == 0) == allowed, result.stderr


def test_candidate_provision_precedes_autoflow_update_and_readiness_precedes_activation():
    result = run(r"""
vp_worker_admission_transition_to() { :; }
vp_worker_admission_advance_migration_state() { :; }
vp_run_worker_registration_migration() { echo migrate; }
vp_require_channelops_migration_head() { :; }
vp_prepare_worker_redis_marker_controls() { :; }
vp_prepare_worker_admission() { echo provision; }
vp_update_app_runtime_service() { echo "update:$1"; }
vp_require_autoflow_control_ready() { echo qualify; }
vp_install_staging_object_janitor() { :; }
vp_run_staging_object_janitor_once() { :; }
vp_worker_admission_record_janitor_service() { :; }
vp_require_worker_redis_marker_status() { :; }
vp_record_worker_activation_attempt() { :; }
vp_activate_worker_admission() { echo activate; return 1; }
http_health() { :; }
vp_apply_app_services api frontend backend runner go worker
""")
    assert result.returncode != 0
    events = result.stdout.splitlines()
    assert (
        events.index("migrate")
        < events.index("provision")
        < events.index("update:vp-autoflow-api-swarm")
    )
    assert (
        events.index("update:vp-autoflow-api-swarm")
        < events.index("qualify")
        < events.index("activate")
    )


def test_failed_autoflow_qualification_never_activates_workers():
    result = run(r"""
vp_worker_admission_transition_to() { :; }
vp_worker_admission_advance_migration_state() { :; }
vp_run_worker_registration_migration() { :; }
vp_require_channelops_migration_head() { :; }
vp_prepare_worker_redis_marker_controls() { :; }
vp_prepare_worker_admission() { :; }
vp_update_app_runtime_service() { :; }
vp_require_autoflow_control_ready() { return 1; }
vp_install_staging_object_janitor() { echo unsafe-after-failure; return 1; }
http_health() { :; }
vp_apply_app_services api frontend backend runner go worker
""")
    assert result.returncode != 0 and "unsafe-after-failure" not in result.stdout


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "argv_secret",
        "secret_id",
        "mode",
        "generation",
        "task_image",
        "old_task",
        "health",
    ],
)
def test_exact_ready_descriptor_and_current_task_qualification(fault):
    container = {
        "Image": "vp-backend-api:deploy-0123456789ab",
        "User": "",
        "Env": [
            "WORKER_ORCHESTRATOR_DATABASE_URL_FILE=/run/secrets/worker-orchestrator-database-url",
            f"WORKER_ORCHESTRATOR_CONTROL_GENERATION={GENERATION}",
        ],
        "Secrets": [
            {
                "SecretName": SECRET,
                "SecretID": SECRET_ID,
                "File": {
                    "Name": "worker-orchestrator-database-url",
                    "UID": "0",
                    "GID": "0",
                    "Mode": 256,
                },
            }
        ],
        "Healthcheck": {"Test": ["CMD-SHELL", "true"]},
    }
    value = {
        "Name": "vp-autoflow-api-swarm",
        "Mode": {"Replicated": {"Replicas": 1}},
        "TaskTemplate": {"ContainerSpec": container},
    }
    tasks = [
        {
            "ServiceID": SERVICE_ID,
            "Status": {
                "State": "running",
                "ContainerStatus": {"ContainerID": "c" * 64},
            },
            "Spec": {"ContainerSpec": json.loads(json.dumps(container))},
        }
    ]
    if fault == "argv_secret":
        container["Env"].append("PRIVATE=SENTINEL_CREDENTIAL")
        tasks[0]["Spec"]["ContainerSpec"]["Env"].append("PRIVATE=SENTINEL_CREDENTIAL")
    if fault == "secret_id":
        container["Secrets"][0]["SecretID"] = "z" * 25
    if fault == "mode":
        container["Secrets"][0]["File"]["Mode"] = 292
    if fault == "generation":
        container["Env"][-1] = "WORKER_ORCHESTRATOR_CONTROL_GENERATION=other"
    if fault == "task_image":
        tasks[0]["Spec"]["ContainerSpec"]["Image"] = "old-image"
    if fault == "old_task":
        tasks.append(tasks[0])
    result = run(
        r"""
vp_app_service_durable_identity() { echo 'aaaaaaaaaaaaaaaaaaaaaaaaa|dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'; }
vp_autoflow_health_command() { echo true; }
vp_require_service_node() { :; }
docker() {
  if [[ "$1 $2" == 'service inspect' ]]; then printf '%s\n' "$SPEC"
  elif [[ "$1 $2" == 'service ps' ]]; then echo aaaaaaaaaaaaaaaaaaaaaaaaa
  elif [[ "$1 $2" == 'image inspect' ]]; then printf '\n'
  elif [[ "$1" == inspect ]]; then printf '%s\n' "$TASKS"
  else return 91; fi
}
remote_sh() { [[ "$FAULT" != health ]]; }
python3() {
  for arg in "$@"; do
    [[ "$arg" != *SENTINEL_CREDENTIAL* ]] || return 1
  done
  command python3 "$@"
}
vp_require_autoflow_control_ready vp-backend-api:deploy-0123456789ab
""",
        SPEC=value,
        TASKS=tasks,
        FAULT=fault or "none",
    )
    assert result.returncode == (0 if fault in {None, "argv_secret"} else 1), (
        result.stderr
    )


def test_rollback_rebinds_retained_migration_compatible_image_before_readiness():
    result = run(r"""
VP_BACKEND_MIGRATION_APPLIED=true
vp_validate_app_snapshot_identities() { :; }
vp_autoflow_selected_image() { echo vp-backend-api:deploy-0123456789ab; }
vp_update_runtime_service() { echo "update:$1:$2:$4"; }
vp_require_autoflow_control_ready() { echo "qualify:$1"; return 1; }
vp_restore_app_snapshots 'vp-autoflow-api-swarm|aaaaaaaaaaaaaaaaaaaaaaaaa|vp-backend-api:deploy-000000000000|dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' vp-autoflow-api-swarm true
""")
    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        "update:vp-autoflow-api-swarm:vp-backend-api:deploy-0123456789ab:" + SERVICE_ID,
        "qualify:vp-backend-api:deploy-0123456789ab",
    ]


@pytest.mark.parametrize(
    "action",
    [
        "vp_commit_worker_control_generation",
        "vp_finalize_worker_control_rollback",
        "vp_worker_admission_retire_transaction",
    ],
)
def test_control_cannot_promote_or_retire_before_selected_autoflow_ready(action):
    result = run(
        r"""
VP_WORKER_CONTROL_PREPARED=true
VP_WORKER_ADMISSION_COMMITTED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=c-11111111111111111111
VP_WORKER_ADMISSION_LOCK_ROOT=/unused
vp_require_selected_autoflow_control_ready() { echo blocked; return 1; }
vp_require_pipeline_network_identity() { echo bypassed; return 1; }
vp_worker_control_require_rollback_workers() { echo bypassed; return 1; }
vp_worker_admission_process_retirement_journals() { echo bypassed; return 1; }
"$ACTION"
""",
        ACTION=action,
    )
    assert result.returncode == 1 and result.stdout == "blocked\n"


@pytest.mark.parametrize(
    "fault",
    [None, "lock", "secret_id", "selection", "read_error", "service_id", "image"],
)
def test_control_identity_requires_current_locked_journal_and_immutable_secret(fault):
    selection = {"generation": GENERATION, "secrets": [{"docker_secret_id": SECRET_ID}]}
    state = {
        "phase": "FORWARD_APPLYING",
        "forward": {"control": selection},
        "target_commit": "0123456789abcdef0123456789abcdef01234567",
        "baseline": {
            "services": [
                {
                    "name": "vp-autoflow-api-swarm",
                    "existed": True,
                    "docker_service_id": SERVICE_ID,
                }
            ]
        },
    }
    if fault == "selection":
        state["forward"]["control"] = {"generation": "old"}
    if fault == "service_id":
        state["baseline"]["services"][0]["docker_service_id"] = "other"
    if fault == "image":
        state["target_commit"] = "1" * 40
    result = run(
        r"""
eval "$identity_function"
vp_worker_admission_load_replay_plan() { [[ "$FAULT" != lock ]]; }
vp_worker_admission_root() { echo /fixture; }
vp_worker_control_find_v2_manifest() { echo /fixture/manifest; }
vp_worker_control_read_manifest() {
  VP_WORKER_CONTROL_MANIFEST_VERSION=2
  VP_WORKER_CONTROL_MANIFEST_GENERATION=c-0123456789abcdef0123
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET=vp-wc-orchestrator-c-0123456789abcdef0123
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID=bbbbbbbbbbbbbbbbbbbbbbbbb
}
vp_managed_secret_id() { if [[ "$FAULT" == secret_id ]]; then echo changed; else echo bbbbbbbbbbbbbbbbbbbbbbbbb; fi; }
vp_worker_admission_control_selection_json() { printf '%s\n' "$SELECTION"; }
vp_worker_admission_recovery_state() { [[ "$FAULT" != read_error ]] && printf '%s\n' "$STATE"; }
vp_autoflow_control_identity aaaaaaaaaaaaaaaaaaaaaaaaa vp-backend-api:deploy-0123456789ab
""",
        FAULT=fault or "none",
        STATE=state,
        SELECTION=selection,
    )
    assert result.returncode == (0 if fault is None else 1)
    if fault is None:
        assert result.stdout.strip() == f"{SECRET}|{SECRET_ID}|{GENERATION}"


@pytest.mark.parametrize("fault", [None, "generation", "principal", "ready", "status"])
def test_actual_health_command_qualifies_body_without_extra_requests(fault):
    result = run("vp_autoflow_health_command\n")
    assert result.returncode == 0
    command = shlex.split(result.stdout)
    assert command[:2] == ["python", "-c"]
    code = """
import io,json,os,urllib.request
from app.services.worker_control_role_cli import role_names_for_generation
g=os.environ["WORKER_ORCHESTRATOR_CONTROL_GENERATION"]
body={"status":"ok","registered_runtime":{"ready":True,"generation":g,"principal":role_names_for_generation(g).versioned["orchestrator"]}}
fault=os.environ["FAULT"]
if fault=="status": body["status"]="unavailable"
elif fault!="none": body["registered_runtime"][fault]=False if fault=="ready" else "wrong"
calls=[]
def request(url,timeout):
    assert url=="http://127.0.0.1:8080/health" and timeout==2
    calls.append(url)
    return io.StringIO(json.dumps(body))
urllib.request.urlopen=request
"""
    code += "\nexec(" + repr(command[2]) + ")\nassert len(calls)==1\n"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        env={
            "PATH": os.environ["PATH"],
            "PYTHONPATH": str(EXTENSION.parents[2] / "backend"),
            "WORKER_ORCHESTRATOR_CONTROL_GENERATION": GENERATION,
            "FAULT": fault or "none",
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == (0 if fault is None else 1), completed.stderr
