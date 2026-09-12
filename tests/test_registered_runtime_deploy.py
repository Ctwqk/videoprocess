"""Execute deployment shell functions with no external operations available."""

import json
import os
from pathlib import Path
import runpy
import subprocess
import shlex
import sys
from unittest.mock import patch

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
vp_autoflow_control_identity() { VP_AUTOFLOW_CONTROL_IDENTITY='vp-wc-orchestrator-c-0123456789abcdef0123|bbbbbbbbbbbbbbbbbbbbbbbbb|c-0123456789abcdef0123'; }
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
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    desired = helper["_autoflow_update_spec"](
        {"ID": SERVICE_ID, "Spec": {"Name": "vp-autoflow-api-swarm", "TaskTemplate": {"ContainerSpec": spec()}}},
        SERVICE_ID, "vp-backend-api:deploy-0123456789ab", "start-first",
        f"{SECRET}|{SECRET_ID}|{GENERATION}", "", "true", "colima-127",
    )
    container = desired["TaskTemplate"]["ContainerSpec"]
    assert container["Secrets"][0] == spec()["Secrets"][0]
    assert container["Secrets"][1] == {"SecretID": SECRET_ID, "SecretName": SECRET,
        "File": {"Name": "worker-orchestrator-database-url", "UID": "0", "GID": "0", "Mode": 256}}
    assert "KEEP=yes" in container["Env"]
    assert "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + GENERATION in container["Env"]


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
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    actual = {"ID": SERVICE_ID, "Spec": {"Name": "vp-autoflow-api-swarm", "TaskTemplate": {"ContainerSpec": value}}}
    with pytest.raises(helper["TransactionError"]):
        helper["_autoflow_update_spec"](
            {} if fault == "inspect_error" else actual, SERVICE_ID,
            "vp-backend-api:deploy-0123456789ab", "start-first",
            f"{SECRET}|{SECRET_ID}|{GENERATION}", "", "true", "colima-127",
        )


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
vp_registered_reconcile_capture() { :; }
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
VP_WORKER_DEPLOY_READINESS_ATTEMPTS=2
VP_WORKER_DEPLOY_READINESS_INTERVAL_SECONDS=1
sleep() { :; }
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
printf '%s\n' "$VP_AUTOFLOW_CONTROL_IDENTITY"
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


def locked_runtime_fixture(tmp_path, action):
    """A real, validated journal/lock; Docker and mounted metadata remain fake."""
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    root = tmp_path.resolve()
    root.chmod(0o700)
    credentials = {}
    for purpose in helper["DATABASE_PURPOSES"]:
        path = root / purpose
        path.write_text("postgresql://fixture:password@database.invalid/fixture\n")
        path.chmod(0o400)
        credentials[purpose] = helper["_capture_credential"](str(path), "vp_" + purpose)
    commit = "0123456789abcdef0123" + "4" * 20
    image = "vp-backend-api:deploy-0123456789ab"
    generation = "c-11111111111111111111" if action == "rollback" else GENERATION
    control = {
        "generation": generation,
        "image": "vp-ffmpeg-worker-python:deploy-" + generation[2:14],
        "manifest_sha256": "d" * 64,
        "secrets": [
            {
                "service": "vp-worker-control", "generation": generation,
                "purpose": purpose, "name": f"vp-wc-{purpose}-{generation}",
                "docker_secret_id": SECRET_ID if purpose == "orchestrator" else f"{i:025x}",
            }
            for i, purpose in enumerate((
                "operator", "orchestrator", "staging-janitor", "staging-minio-access",
                "staging-minio-secret", "worker-minio-access", "worker-minio-secret",
            ), 100)
        ],
    }
    state = helper["_new_document"](
        target_commit=commit, target_backend_image=image,
        target_go_image="vp-ffmpeg-worker-go:deploy-0123456789ab",
        namespace=commit, baseline_kind="managed", credentials=credentials,
    )
    state["phase"] = "ROLLBACK_APPLYING" if action == "rollback" else "FORWARD_APPLYING"
    state["baseline"].update(captured=True, control=control, services=[
        {
            "name": name, "existed": True,
            "docker_service_id": SERVICE_ID if name == "vp-autoflow-api-swarm" else f"{i:025x}",
            "image": "vp-backend-api:deploy-111111111111", "spec_digest": "d" * 64,
        }
        for i, name in enumerate(sorted(helper["APP_SERVICES"]), 200)
    ])
    state["forward"]["control"] = control
    if action == "rollback":
        state["rollback"].update(
            attempt=1, namespace="rollback-123456789012345678",
            marker_generation="m-rb-0123456789ab-1", control=control,
        )
        state["forward"]["control"] = dict(
            control, generation=GENERATION,
            image="vp-ffmpeg-worker-python:deploy-0123456789ab",
            secrets=[
                dict(secret, generation=GENERATION,
                     name=f"vp-wc-{secret['purpose']}-{GENERATION}",
                     docker_secret_id=f"{i:025x}")
                for i, secret in enumerate(control["secrets"], 300)
            ],
        )
        state["failed_forward"]["captured"] = True
    helper["_validate_document"](state)
    transactions = root / "transactions"
    transactions.mkdir(mode=0o700)
    (transactions / state["transaction_id"]).mkdir(mode=0o700)
    active = transactions / "active.json"
    active.write_bytes(helper["_canonical"](state))
    active.chmod(0o600)
    container = {
        "Image": image, "User": "", "Env": [
            "KEEP=yes",
            "WORKER_ORCHESTRATOR_DATABASE_URL_FILE=/run/secrets/worker-orchestrator-database-url",
            "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + generation,
        ],
        "Secrets": [{
            "SecretName": "vp-wc-orchestrator-" + generation, "SecretID": SECRET_ID,
            "File": {"Name": "worker-orchestrator-database-url", "UID": "0", "GID": "0", "Mode": 256},
        }],
        "Healthcheck": {"Test": ["CMD-SHELL", "true"]},
    }
    spec = {"Name": "vp-autoflow-api-swarm", "Mode": {"Replicated": {"Replicas": 1}},
            "TaskTemplate": {"ContainerSpec": container}}
    tasks = [{"ServiceID": SERVICE_ID, "Spec": {"ContainerSpec": container}, "Status": {
        "State": "running", "ContainerStatus": {"ContainerID": "c" * 64},
    }}]
    fake_bin = root / "bin"
    fake_bin.mkdir()
    docker = fake_bin / "docker"
    docker.write_text("#!" + sys.executable + "\n" + r'''
import json,os,pathlib,sys
root=pathlib.Path(os.environ["ADMISSION_ROOT"])
stored=root/"engine-spec.json"
spec=json.loads(stored.read_text() if stored.exists() else os.environ["SPEC"])
args=sys.argv[1:]
if args[:2]==["image","inspect"]:
    print("")
elif args==["service","inspect","aaaaaaaaaaaaaaaaaaaaaaaaa"]:
    print(json.dumps([{"ID":"a"*25,"Version":{"Index":72 if stored.exists() else 71},"Spec":spec,"UpdateStatus":{"State":"completed"}}]))
elif args==["system","dial-stdio"]:
    headers,body=sys.stdin.buffer.read().split(b"\r\n\r\n",1)
    assert headers.startswith(b"POST /v1.52/services/aaaaaaaaaaaaaaaaaaaaaaaaa/update?version=71&registryAuthFrom=spec HTTP/1.1\r\n")
    value=json.loads(body)
    with open(os.environ["AUDIT"],"a") as audit: audit.write("UPDATE|"+json.dumps(value,separators=(",",":"))+"\n")
    if os.environ["FAULT"] in {"engine_id_replaced","engine_version_conflict"}:
        sys.stdout.buffer.write(b"HTTP/1.1 409 Conflict\r\nContent-Length: 2\r\n\r\n{}")
    else:
        stored.write_text(json.dumps(value))
        if os.environ["FAULT"]!="engine_lost":
            sys.stdout.buffer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
else:
    raise SystemExit(91)
''')
    docker.chmod(0o700)
    return dict(ADMISSION_ROOT=str(root), AUDIT=str(root / "operations.log"), IMAGE=image, SELECTED_GENERATION=generation,
                CONTROL=control, SPEC=spec, CONTAINER=container, TASKS=tasks, ACTION=action,
                PATH=str(fake_bin) + os.pathsep + os.environ["PATH"])


LOCKED_RUNTIME_BOUNDARY = r"""
eval "$identity_function"
: > "$AUDIT"
VP_WORKER_ADMISSION_LOCK_HELD=false
vp_worker_admission_lock_acquire "$ADMISSION_ROOT"
trap 'vp_worker_admission_lock_release' EXIT
vp_worker_admission_lock_assert
vp_worker_admission_load_replay_plan
[[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true ]]
VP_WORKER_CONTROL_GENERATION="$SELECTED_GENERATION"
if [[ "$FAULT" == generation ]]; then VP_WORKER_CONTROL_GENERATION=c-22222222222222222222; fi
vp_worker_admission_root() { printf '%s\n' "$ADMISSION_ROOT"; }
vp_worker_control_find_v2_manifest() { printf '%s/fixture.conf\n' "$ADMISSION_ROOT"; }
vp_worker_control_read_manifest() {
  VP_WORKER_CONTROL_MANIFEST_VERSION=2
  VP_WORKER_CONTROL_MANIFEST_GENERATION="$SELECTED_GENERATION"
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET="vp-wc-orchestrator-$SELECTED_GENERATION"
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID=bbbbbbbbbbbbbbbbbbbbbbbbb
}
vp_managed_secret_id() {
  if [[ "$FAULT" == secret ]]; then echo zzzzzzzzzzzzzzzzzzzzzzzzz
  else echo bbbbbbbbbbbbbbbbbbbbbbbbb; fi
}
vp_worker_admission_control_selection_json() { printf '%s\n' "$CONTROL"; }
vp_registered_worker_service_current_id() {
  if [[ "$FAULT" == service ]]; then echo zzzzzzzzzzzzzzzzzzzzzzzzz
  else echo aaaaaaaaaaaaaaaaaaaaaaaaa; fi
}
vp_service_values() {
  case "$2" in
    *Placement.Constraints*) printf '%s\n' "$VP_RUNTIME_CONSTRAINT" "$VP_RUNTIME_NODE_CONSTRAINT" ;;
    '{{json .Spec.TaskTemplate.ContainerSpec}}') printf '%s\n' "$CONTAINER" ;;
    *) return 91 ;;
  esac
}
vp_autoflow_health_command() { echo true; }
vp_require_service_node() { [[ "$2" == colima-127 ]]; }
docker() {
  case "$1 $2" in
    'image inspect') [[ "$3" == "$IMAGE" ]] && printf '\n' ;;
    'service inspect')
      case "$5" in
        '{{.ID}}|{{.Spec.Name}}')
          printf '%s|vp-autoflow-api-swarm\n' "$(vp_registered_worker_service_current_id)" ;;
        '{{json .Spec}}')
          if [[ -f "$ADMISSION_ROOT/engine-spec.json" ]]; then cat "$ADMISSION_ROOT/engine-spec.json"
          else printf '%s\n' "$SPEC"; fi ;;
        *) return 91 ;;
      esac ;;
    'service ps') echo aaaaaaaaaaaaaaaaaaaaaaaaa ;;
    'inspect --type')
      python3 -c 'import json,os,pathlib; t=json.loads(os.environ["TASKS"]); p=pathlib.Path(os.environ["ADMISSION_ROOT"])/"engine-spec.json"; t[0]["Spec"]["ContainerSpec"]=json.loads(p.read_text())["TaskTemplate"]["ContainerSpec"] if p.exists() else t[0]["Spec"]["ContainerSpec"]; print(json.dumps(t))' ;;
    'service update')
      echo ATTEMPT_UPDATE >> "$AUDIT"
      vp_worker_admission_lock_assert || return 91
      { printf UPDATE; printf '|%s' "$@"; printf '\n'; } >> "$AUDIT" ;;
    *) return 91 ;;
  esac
}
remote_sh() {
  echo ATTEMPT_HEALTH >> "$AUDIT"
  vp_worker_admission_lock_assert || return 92
  [[ "$1" == 10.0.0.127 && "$5" == cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc ]]
  echo HEALTH >> "$AUDIT"
}
boundary() {
  case "$ACTION" in
    forward)
      vp_update_runtime_service vp-autoflow-api-swarm "$IMAGE" start-first || return $?
      vp_require_selected_autoflow_control_ready ;;
    readiness) vp_require_selected_autoflow_control_ready ;;
    rollback)
      VP_BACKEND_MIGRATION_APPLIED=true
      vp_restore_app_snapshots \
        'vp-autoflow-api-swarm|aaaaaaaaaaaaaaaaaaaaaaaaa|vp-backend-api:deploy-111111111111|dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' \
        vp-autoflow-api-swarm true ;;
    *) return 93 ;;
  esac
}
status=0
if [[ "$FAULT" == child ]]; then ( boundary ) || status=$?
else boundary || status=$?; fi
vp_worker_admission_lock_assert
exit "$status"
"""


@pytest.mark.parametrize("action", ["forward", "readiness", "rollback"])
@pytest.mark.parametrize("fault", ["none", "child", "generation", "secret", "service"])
def test_real_owning_shell_lock_and_replay_boundary(tmp_path, action, fault):
    data = locked_runtime_fixture(tmp_path, action)
    active = Path(data["ADMISSION_ROOT"]) / "transactions/active.json"
    before = active.read_bytes()
    result = run(LOCKED_RUNTIME_BOUNDARY, **data, FAULT=fault)
    assert result.returncode == (0 if fault == "none" else 2 if action == "forward" else 1), result.stderr
    assert active.read_bytes() == before
    operations = Path(data["AUDIT"]).read_text().splitlines()
    updates = [line for line in operations if line.startswith("UPDATE|")]
    health = operations.count("HEALTH")
    if fault != "none":
        assert not operations, operations
    else:
        assert len(updates) == (0 if action == "readiness" else 1)
        assert health == 1
        if updates:
            posted = json.loads(updates[0].split("|", 1)[1])
            container = posted["TaskTemplate"]["ContainerSpec"]
            assert "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + data["SELECTED_GENERATION"] in container["Env"]
            assert container["Secrets"][0]["SecretID"] == SECRET_ID
            assert container["Secrets"][0]["SecretName"] == "vp-wc-orchestrator-" + data["SELECTED_GENERATION"]
            assert container["Image"] == data["IMAGE"]
            assert posted["UpdateConfig"]["Order"] == ("stop-first" if action == "rollback" else "start-first")


@pytest.mark.parametrize("action", ["forward", "rollback"])
@pytest.mark.parametrize("fault", ["engine_id_replaced", "engine_version_conflict", "engine_lost"])
def test_owning_shell_engine_failure_keeps_pin_and_never_retries(tmp_path, action, fault):
    data = locked_runtime_fixture(tmp_path, action)
    result = run(LOCKED_RUNTIME_BOUNDARY, **data, FAULT=fault)
    assert result.returncode == 1
    operations = Path(data["AUDIT"]).read_text().splitlines()
    updates = [entry for entry in operations if entry.startswith("UPDATE|")]
    assert len(updates) == 1
    assert json.loads(updates[0].split("|", 1)[1])["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["SecretID"] == SECRET_ID
    assert "HEALTH" not in operations


@pytest.mark.parametrize("fixture", ["test_worker_admission_deploy.sh", "test_vp_deploy_sync_extension.sh"])
@pytest.mark.parametrize("order", ["start-first", "stop-first"])
def test_legacy_flow_fixture_covers_new_atomic_transport_entry(fixture, order):
    result = run(r'''
vp_registered_worker_service_current_id() { printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaa; }
eval "$(sed -n '/^vp_autoflow_update_runtime_service() {/,/^}/p' "$FIXTURE")"
docker() { printf '%s\n' "$@"; }
vp_update_runtime_service vp-autoflow-api-swarm vp-backend-api:deploy-0123456789ab "$ORDER"
''', FIXTURE=str(EXTENSION.parents[2] / "tests" / fixture), ORDER=order)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "service", "update", "--detach=false", "--no-resolve-image", "--update-order", order,
        "--constraint-add", "node.labels.vp.runtime==true", "--constraint-add", "node.hostname==colima-127",
        "--env-add", "WORKER_ORCHESTRATOR_CONTROL_GENERATION=fixture", "--image",
        "vp-backend-api:deploy-0123456789ab", SERVICE_ID,
    ]


@pytest.mark.parametrize("fault", ["none", "paused", "spec_drift", "timeout"])
def test_atomic_update_acceptance_waits_for_convergence(tmp_path, fault):
    data = locked_runtime_fixture(tmp_path, "forward")
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    document = json.loads((Path(data["ADMISSION_ROOT"]) / "transactions/active.json").read_text())
    posted = []
    polls = []
    now = [0.0]

    def docker(args, **kwargs):
        if args[:2] == ["image", "inspect"]:
            return ""
        assert args == ["service", "inspect", SERVICE_ID]
        value = json.loads(json.dumps(posted[0] if posted else data["SPEC"]))
        state = "completed"
        if posted:
            polls.append(1)
            state = "updating" if fault == "timeout" or len(polls) == 1 else "completed"
            if fault == "paused":
                state = "paused"
            if fault == "spec_drift":
                value["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["SecretID"] = "z" * 25
        return json.dumps([{"ID": SERVICE_ID, "Version": {"Index": 71}, "Spec": value, "UpdateStatus": {"State": state}}])

    def post(path, spec, status):
        assert path == f"/services/{SERVICE_ID}/update?version=71&registryAuthFrom=spec" and status == 200
        posted.append(spec)
        return {}

    def sleep(seconds):
        now[0] += 60 if fault == "timeout" else seconds

    with patch.dict(helper["autoflow_update"].__globals__,
                    acquire_lock=lambda *args: "token", _registered_document=lambda *args: document,
                    _registered_docker=docker, _engine_service_post=post), \
         patch("time.monotonic", lambda: now[0]), patch("time.sleep", sleep):
        result = helper["autoflow_update"]([
            data["ADMISSION_ROOT"], "19", str(os.getppid()), "token", str(document["revision"]),
            SERVICE_ID, data["IMAGE"], "start-first", f"{SECRET}|{SECRET_ID}|{GENERATION}", "colima-127", "true",
        ])
    assert result == (0 if fault == "none" else 1)
    assert len(posted) == 1
    if fault == "none":
        assert len(polls) == 2


@pytest.mark.parametrize("runner", [False, True])
@pytest.mark.parametrize("action", ["forward", "rollback"])
@pytest.mark.parametrize("fault", ["none", "secret_replaced", "name_changed", "pin_missing", "journal_changed", "wrong_owner", "lost"])
def test_owned_history_entry_uses_journal_pin_and_one_cas(tmp_path, runner, action, fault):
    data = locked_runtime_fixture(tmp_path, action)
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    document = json.loads((Path(data["ADMISSION_ROOT"]) / "transactions/active.json").read_text())
    reference = {"runtime_generation": "eeb8593f43dc5709d0191a06c528a9d35b22785e", "secret_name": "vp-control-redis-eeb8593f43dc",
                 "docker_secret_id": "r" * 25}
    document["runtime_redis"] = {} if fault == "pin_missing" else {"control": reference}
    before = json.dumps(document, sort_keys=True)
    service = "vp-channel-agent-runner-swarm" if runner else "vp-autoflow-api-swarm"
    baseline = next(row for row in document["baseline"]["services"] if row["name"] == service)
    service_id = baseline["docker_service_id"]
    image = data["IMAGE"]
    if runner:
        image = baseline["image"] if action == "rollback" else "vp-channelops-runner-go:deploy-0123456789ab"
    original = json.loads(json.dumps(data["SPEC"]))
    original["Name"] = service
    posted, reads = [], []

    def docker(args, **kwargs):
        if args[:2] == ["secret", "inspect"]:
            assert args == ["secret", "inspect", "r" * 25, "--format", "{{.ID}}|{{.Spec.Name}}"]
            name = "other-qualified-name" if fault == "name_changed" else reference["secret_name"]
            return ("z" * 25 if fault == "secret_replaced" else "r" * 25) + "|" + name
        if args[:2] == ["image", "inspect"]:
            assert args[2] == image
            return ""
        assert args == ["service", "inspect", service_id]
        return json.dumps([{"ID": service_id, "Version": {"Index": 71}, "Spec": posted[0] if posted else original,
                            "UpdateStatus": {"State": "completed"}}])

    def read(*args):
        reads.append(1)
        if fault == "journal_changed" and len(reads) > 1:
            return {**document, "revision": document["revision"] + 1}
        return document

    def post(path, spec, status):
        assert path == f"/services/{service_id}/update?version=71&registryAuthFrom=spec" and status == 200
        posted.append(spec)
        if fault == "lost":
            raise TimeoutError("private-error")
        return {}

    args = [data["ADMISSION_ROOT"], "19", str(os.getppid() + (fault == "wrong_owner")), "token",
            str(document["revision"]), service_id, image, "stop-first" if action == "rollback" else "start-first",
            "-" if runner else f"vp-wc-orchestrator-{data['SELECTED_GENERATION']}|{SECRET_ID}|{data['SELECTED_GENERATION']}",
            "colima-127", "-" if runner else "true", "owned-history-file"]
    with patch.dict(helper["autoflow_update"].__globals__, acquire_lock=lambda *args: "token",
                    _registered_document=read, _registered_docker=docker, _engine_service_post=post):
        result = helper["main"](["owned-history-runner-update" if runner else "autoflow-update", *args])
    assert result == (0 if fault == "none" else 1 if fault == "lost" else 2)
    assert len(posted) == (1 if fault in {"none", "lost"} else 0)
    assert json.dumps(document, sort_keys=True) == before
    if posted:
        c = posted[0]["TaskTemplate"]["ContainerSpec"]
        assert c["Image"] == image
        assert [s for s in c["Secrets"] if s["File"]["Name"] == "owned-history-redis-url"] == [
            {"SecretID": "r" * 25, "SecretName": reference["secret_name"],
             "File": {"Name": "owned-history-redis-url", "UID": "0", "GID": "0", "Mode": 256}}]


@pytest.mark.parametrize("size", [255, 256])
def test_owned_history_reference_name_limit_matches_journal(size):
    reference = {"runtime_generation": "a" * 40, "secret_name": "a" * size,
                 "docker_secret_id": "r" * 25}
    result = run(r'''
vp_worker_admission_recovery_state() { printf '%s\n' "$STATE"; }
vp_owned_history_redis_identity
''', STATE=json.dumps({"runtime_redis": {"control": reference}}))
    assert result.returncode == (0 if size == 255 else 1)
    assert result.stdout.strip() == (reference["secret_name"] + "|" + "r" * 25 if size == 255 else "")


@pytest.mark.parametrize("service", ["vp-autoflow-api-swarm", "vp-channel-agent-runner-swarm"])
def test_explicit_owned_history_shell_routes_through_atomic_entry(tmp_path, service):
    audit = tmp_path / "args"
    result = run(r'''
OWNED_HISTORY_REDIS_URL_FILE=/protected/host-file
VP_WORKER_ADMISSION_LOCK_ROOT=/unused
VP_WORKER_ADMISSION_CURRENT_BASHPID=${BASHPID:-$$}
vp_worker_admission_load_replay_plan() { VP_WORKER_ADMISSION_REPLAY_REVISION=71; }
VP_WORKER_ADMISSION_REPLAY_REVISION=71
vp_registered_worker_service_current_id() { printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaa; }
python3() { printf '%s\n' "$@" >"$AUDIT"; }
vp_update_runtime_service "$SERVICE" "$IMAGE" stop-first
''', AUDIT=str(audit), SERVICE=service,
        IMAGE="vp-backend-api:deploy-0123456789ab" if service == "vp-autoflow-api-swarm" else "vp-channelops-runner-go:deploy-0123456789ab")
    assert result.returncode == 0, result.stderr
    args = audit.read_text().splitlines()
    assert args[1] == ("autoflow-update" if service == "vp-autoflow-api-swarm" else "owned-history-runner-update")
    assert args[-1] == "owned-history-file"
    assert "/protected/host-file" not in args


@pytest.mark.parametrize("fault", ["none", "missing", "id", "mode", "env"])
def test_owned_history_readiness_rechecks_fixed_mount_before_health(tmp_path, fault):
    data = locked_runtime_fixture(tmp_path, "readiness")
    active = Path(data["ADMISSION_ROOT"]) / "transactions/active.json"
    state = json.loads(active.read_text())
    reference = {"runtime_generation": "eeb8593f43dc5709d0191a06c528a9d35b22785e", "secret_name": "vp-control-redis-eeb8593f43dc",
                 "docker_secret_id": "r" * 25}
    state["runtime_redis"] = {"control": reference}
    helper = runpy.run_path(str(EXTENSION.with_name("worker-admission-transaction.py")))
    helper["_validate_document"](state)
    active.write_bytes(helper["_canonical"](state))
    before = active.read_bytes()
    c = data["CONTAINER"]
    c["Env"].append("OWNED_HISTORY_REDIS_URL_FILE=" + ("/wrong" if fault == "env" else "/run/secrets/owned-history-redis-url"))
    if fault != "missing":
        c["Secrets"].append({"SecretID": "z" * 25 if fault == "id" else "r" * 25,
                             "SecretName": reference["secret_name"],
                             "File": {"Name": "owned-history-redis-url", "UID": "0", "GID": "0",
                                      "Mode": 292 if fault == "mode" else 256}})
    result = run(LOCKED_RUNTIME_BOUNDARY, **data, FAULT=fault, OWNED_HISTORY_REDIS_URL_FILE="/protected/host-file")
    assert result.returncode == (0 if fault == "none" else 1), result.stderr
    assert active.read_bytes() == before
    assert ("HEALTH" in Path(data["AUDIT"]).read_text().splitlines()) == (fault == "none")


@pytest.mark.parametrize("fault", ["settle", "two_forever", "health_forever", "body_pending",
                                  "service_drift", "spec_drift", "secret_drift", "journal_drift", "node_drift"])
def test_completed_start_first_waits_for_exact_single_healthy_task(tmp_path, fault):
    data = locked_runtime_fixture(tmp_path, "forward")
    final = json.loads(json.dumps(data["TASKS"]))
    old = json.loads(json.dumps(final[0]))
    old["Spec"]["ContainerSpec"]["Image"] = "vp-backend-api:deploy-111111111111"
    old["Status"]["ContainerStatus"]["ContainerID"] = "d" * 64
    data["TASKS"] = [final[0], old]
    transition = r'''
eval "$node_function"
export POLL=0
VP_WORKER_DEPLOY_READINESS_ATTEMPTS=4
VP_WORKER_DEPLOY_READINESS_INTERVAL_SECONDS=1
sleep() {
  [[ "$1" == 1 ]] || return 91
  POLL=$((POLL + 1))
  echo "WAIT:$POLL" >> "$AUDIT"
  if [[ "$FAULT" != two_forever ]]; then TASKS="$FINAL_TASKS"; fi
  if [[ "$FAULT" == service_drift ]]; then
    vp_registered_worker_service_current_id() { echo zzzzzzzzzzzzzzzzzzzzzzzzz; }
  elif [[ "$FAULT" == spec_drift || "$FAULT" == secret_drift || "$FAULT" == journal_drift ]]; then
    python3 -c 'import json,os,pathlib
root=pathlib.Path(os.environ["ADMISSION_ROOT"]); fault=os.environ["FAULT"]
p=root/("transactions/active.json" if fault=="journal_drift" else "engine-spec.json")
v=json.loads(p.read_text())
if fault=="journal_drift": v["phase"]="ROLLBACK_PREPARING"
elif fault=="spec_drift": v["Labels"]={"drift":"unexpected"}
else: v["TaskTemplate"]["ContainerSpec"]["Secrets"][0]["SecretID"]="z"*25
p.write_text(json.dumps(v,sort_keys=True,separators=(",",":"))+"\n")'
  fi
}
remote_sh() {
  vp_worker_admission_lock_assert || return 92
  [[ "$1" == 10.0.0.127 && "$5" == cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc ]] || return 92
  echo "HEALTH_PROBE:$POLL" >> "$AUDIT"
  local script
  script="$(cat)"
  (
    docker() {
      case "$1" in
        inspect)
          local health=healthy
          if [[ "$POLL" == 1 || "$FAULT" == health_forever ]]; then health=starting; fi
          printf '%s|%s|true|%s\n' aaaaaaaaaaaaaaaaaaaaaaaaa "$IMAGE" "$health" ;;
        exec)
          echo "HEALTH_BODY:$POLL" >> "$AUDIT"
          if [[ "$FAULT" == body_pending && "$POLL" == 2 ]]; then return 1; fi ;;
        *) return 91 ;;
      esac
    }
    export -f docker
    SCRIPT="$script" bash -c 'eval "$SCRIPT"' -- "${@:5}"
  )
}
'''
    script = 'node_function="$(declare -f vp_require_service_node)"\n' + LOCKED_RUNTIME_BOUNDARY
    script = script.replace("status=0\n", transition + "\nstatus=0\n", 1)
    script = script.replace("'service ps') echo aaaaaaaaaaaaaaaaaaaaaaaaa ;;", r''' 'service ps')
      if [[ "$4" == --filter ]]; then
        if [[ "$FAULT" == node_drift ]]; then echo 'wrong-node|Running 1 second'
        else
          echo 'colima-127|Running 1 second'
          if [[ "$POLL" == 0 || "$FAULT" == two_forever ]]; then echo 'colima-127|Running 5 minutes'; fi
        fi
      else echo aaaaaaaaaaaaaaaaaaaaaaaaa; fi ;;''')
    result = run(script, **data, FINAL_TASKS=final, FAULT=fault)
    assert result.returncode == (0 if fault in {"settle", "body_pending"} else 1), result.stderr
    operations = Path(data["AUDIT"]).read_text().splitlines()
    assert sum(line.startswith("UPDATE|") for line in operations) == 1
    waits = [line for line in operations if line.startswith("WAIT:")]
    expected_waits = 3 if fault in {"two_forever", "health_forever", "body_pending"} else 2 if fault == "settle" else 1
    assert len(waits) == expected_waits
    assert "HEALTH_PROBE:0" not in operations
    if fault in {"settle", "body_pending"}:
        assert operations[-1] == ("HEALTH_BODY:3" if fault == "body_pending" else "HEALTH_BODY:2")
    else:
        assert not any(line.startswith("HEALTH_BODY:") for line in operations)


@pytest.mark.parametrize(
    "fault", ["none", "child", "substitution", "outer_unlocked", "outer_replaced"]
)
def test_registered_action_requires_real_owning_shell_and_both_lock_inodes(
    tmp_path, fault
):
    data = locked_runtime_fixture(tmp_path, "forward")
    result = run(
        r"""
ROOT="$ADMISSION_ROOT"
exec 9<>"$ROOT/sync.lock"
chmod 600 "$ROOT/sync.lock"
python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)'
VP_WORKER_ADMISSION_LOCK_HELD=false
vp_worker_admission_lock_acquire "$ADMISSION_ROOT"
if [[ "$FAULT" == outer_unlocked ]]; then
  python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_UN)'
elif [[ "$FAULT" == outer_replaced ]]; then
  mv "$ROOT/sync.lock" "$ROOT/original.lock"
  : >"$ROOT/sync.lock"
  chmod 600 "$ROOT/sync.lock"
fi
if [[ "$FAULT" == child ]]; then ( vp_registered_reconcile_action verify all )
elif [[ "$FAULT" == substitution ]]; then value="$(vp_registered_reconcile_action verify all)"
else vp_registered_reconcile_action verify all; fi
""",
        **data,
        FAULT=fault,
    )
    assert result.returncode == (0 if fault == "none" else 1), result.stderr
    assert not result.stdout and not result.stderr


@pytest.mark.parametrize("terminal", [10, 11, 1])
def test_registered_waiter_does_not_confuse_observation_failure_with_success(terminal):
    result = run(
        r"""
vp_registered_reconcile_action() {
  echo "$1:$2"
  if [[ "$1" == observe ]]; then return "$TERMINAL"; fi
}
vp_registered_reconcile_wait run
""",
        TERMINAL=str(terminal),
    )
    assert result.returncode == (0 if terminal == 10 else 1), result.stderr
    assert result.stdout.splitlines() == ["observe:run"]


def test_registered_capture_finishes_cleanup_before_returning_failure():
    result = run(r"""
vp_require_pipeline_network_identity() { :; }
vp_registered_reconcile_action() { echo "$1:$2"; }
vp_registered_reconcile_wait() { echo wait; return 1; }
vp_registered_reconcile_capture baseline
""")
    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        "prepare:baseline",
        "launch:baseline",
        "wait",
        "cleanup:baseline",
    ]


@pytest.mark.parametrize("boundary", ["forward_failure", "abort", "pending_promotion"])
def test_unsettled_registered_job_blocks_recovery_before_other_effects(boundary):
    result = run(
        r"""
vp_worker_admission_hydrate_recovery_context() { echo hydrate; }
vp_registered_reconcile_cleanup() { echo cleanup; return 1; }
vp_registered_reconcile_action() { echo verify; return 1; }
vp_worker_admission_current_promotion_matches() { echo unsafe; return 1; }
vp_restore_app_snapshots() { echo unsafe; }
vp_worker_admission_abort_vision_jobs() { echo unsafe; }
vp_worker_admission_abort_preparing_transaction() { echo unsafe; }
if [[ "$BOUNDARY" == forward_failure ]]; then
  vp_worker_admission_resume_forward_failure
elif [[ "$BOUNDARY" == abort ]]; then
  vp_worker_admission_abort_transaction preparing_failed
else
  vp_worker_admission_complete_pending_promotion PROMOTE_WORKERS operation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
fi
""",
        BOUNDARY=boundary,
    )
    assert result.returncode == 1, result.stderr
    assert "unsafe" not in result.stdout
    assert result.stdout.splitlines() == (
        ["verify"] if boundary == "pending_promotion" else ["cleanup"]
    )


def test_normal_reconcile_gate_follows_readiness_and_precedes_first_promotion():
    result = run(r"""
for function in vp_worker_admission_advance_migration_state vp_run_worker_registration_migration \
  vp_require_channelops_migration_head vp_prepare_worker_redis_marker_controls vp_prepare_worker_admission \
  vp_require_autoflow_control_ready vp_install_staging_object_janitor vp_run_staging_object_janitor_once \
  vp_worker_admission_record_janitor_service vp_require_worker_redis_marker_status vp_record_worker_activation_attempt \
  vp_worker_admission_advance_live_worker_stage vp_record_app_service_attempt vp_deploy_python_worker \
  vp_deploy_vision_worker vp_run_vision_cutover_job vp_reconcile_vision_consumers vp_deploy_publisher \
  http_health vp_update_app_runtime_service vp_require_worker_deployment_ready; do
  eval "$function() { :; }"
done
VP_VISION_CUTOVER_REQUIRED=false
vp_registered_reconcile_capture() { echo "capture:$1"; }
vp_activate_worker_admission() { echo "activate:$1"; }
swarm_service_running() { echo "ready:$1"; }
vp_install_soak_watch() { echo soak; }
vp_registered_reconcile_forward() { echo reconcile; }
vp_worker_admission_transition_to() { echo "phase:$1"; }
vp_worker_admission_promote_phase() { echo "promote:$1"; }
vp_worker_admission_finish_transaction() { echo finish; }
vp_apply_app_services api frontend backend runner go worker
""")
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines.index("capture:baseline") < lines.index(
        "activate:vp-ffmpeg-worker-go-swarm"
    )
    assert lines.index("soak") + 1 == lines.index("reconcile")
    assert lines.index("reconcile") + 1 == lines.index("phase:FORWARD_VERIFIED")
    assert lines.index("phase:FORWARD_VERIFIED") < lines.index(
        "promote:PROMOTE_WORKERS"
    )


@pytest.mark.parametrize("boundary", ["before_capture", "after_capture", "after_run"])
def test_pending_signal_prevents_new_attempt_or_success_but_allows_cleanup(boundary):
    result = run(
        r"""
vp_require_pipeline_network_identity() { :; }
vp_registered_reconcile_action() { echo "$1:$2"; }
vp_registered_reconcile_wait() { :; }
vp_registered_reconcile_cleanup() {
  echo "cleanup:$1"
  if [[ "$BOUNDARY" == after_capture && "$1" == current \
    || "$BOUNDARY" == after_run && "$1" == all ]]; then
    VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS=143
  fi
}
if [[ "$BOUNDARY" == before_capture ]]; then VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS=143; fi
vp_registered_reconcile_forward
""",
        BOUNDARY=boundary,
    )
    assert result.returncode == 1, result.stderr
    lines = result.stdout.splitlines()
    assert "verify:all" not in lines
    if boundary == "before_capture":
        assert lines == []
    elif boundary == "after_capture":
        assert lines == ["prepare:current", "launch:current", "cleanup:current"]
    else:
        assert lines[-1] == "cleanup:all" and lines.count("launch:run") == 1


@pytest.mark.parametrize("mode", ["skip", "unlocked", "normal", "writer_failure", "journal_failure"])
def test_broad_preparation_fake_matches_skip_and_authority_contract(mode):
    source = (EXTENSION.parents[2] / "tests/test_vp_deploy_sync_extension.sh").read_text()
    marker = "vp_prepare_worker_admission() {\n"
    definition = marker + source.split(marker, 1)[1].split("\n}\n", 1)[0] + "\n}\n"
    result = run(r'''
eval "$DEFINITION"
UPDATE_SERVICES=1
VP_WORKER_ADMISSION_LOCK_ROOT=/fixture-only/admission
if [[ "$MODE" == skip ]]; then UPDATE_SERVICES=0; VP_WORKER_ADMISSION_LOCK_ROOT=; fi
VP_WORKER_ADMISSION_PREPARED=unchanged
VP_WORKER_CONTROL_PREPARED=unchanged
TEST_COMMIT=0123456789abcdef0123456789abcdef01234567
VP_WORKER_ADMISSION_COMMIT="$TEST_COMMIT"
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$TEST_COMMIT"
CALLS=/dev/null
vp_worker_admission_lock_assert() { echo lock; [[ "$MODE" != unlocked ]]; }
vp_worker_control_write_manifest() { echo "control:$1"; [[ "$MODE" != writer_failure ]]; }
vp_worker_admission_write_manifest() { echo "worker:$1"; }
vp_worker_admission_record_authority_intent() { echo intent; [[ "$MODE" != journal_failure ]]; }
vp_worker_admission_mark_authority_provisioning() { echo provisioning; }
vp_worker_admission_mark_authority_provisioned() { echo provisioned; }
vp_worker_admission_record_control_selection() { echo selection; }
vp_worker_admission_set_candidate() { :; }
vp_worker_admission_track_candidate() { :; }
vp_worker_admission_record_prepared_worker_plan() { :; }
status=0
vp_prepare_worker_admission vp-python:fixture vp-go:fixture || status=$?
printf 'status:%s\nflags:%s:%s\n' "$status" "$VP_WORKER_ADMISSION_PREPARED" "$VP_WORKER_CONTROL_PREPARED"
''', MODE=mode, DEFINITION=definition)
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    if mode == "skip":
        assert lines == ["status:0", "flags:unchanged:unchanged"]
    elif mode == "unlocked":
        assert lines == ["lock", "status:1", "flags:unchanged:unchanged"]
    elif mode in {"writer_failure", "journal_failure"}:
        assert "status:1" in lines
        assert not any(line.startswith("worker:") for line in lines)
        assert ("intent" in lines) == (mode == "journal_failure")
        assert "provisioning" not in lines
    else:
        assert lines[0] == "lock" and lines[-2:] == ["status:0", "flags:true:false"]
        paths = [line.split(":", 1)[1] for line in lines if line.startswith(("control:", "worker:"))]
        assert len(paths) == 5 and len(set(paths)) == 5
        assert all(path.startswith("/fixture-only/admission/") for path in paths)
