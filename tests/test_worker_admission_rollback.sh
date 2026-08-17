#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
cleanup_test_root() {
  local rc=$?
  if [[ "${KEEP_TEST_ROOT:-false}" == true ]]; then
    printf 'preserved test root: %s\n' "$TEST_ROOT" >&2
  else
    rm -rf "$TEST_ROOT"
  fi
  exit "$rc"
}
trap cleanup_test_root EXIT

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
UPDATE_SERVICES=1
mkdir -p "$ROOT"
log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

(
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS=stale-recovery-authority
  vp_worker_admission_reset_forward_context
  if [[ -n "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" ]]; then
    echo 'FAIL: a new forward transaction retained stale recovery service authority' >&2
    exit 1
  fi
)

(
  mutation_calls="$TEST_ROOT/worker-restore-identity-race-mutations"
  : >"$mutation_calls"
  baseline_service_id=aaaaaaaaaaaaaaaaaaaaaaaa
  replacement_service_id=eeeeeeeeeeeeeeeeeeeeeeee
  VP_WORKER_ADMISSION_PREPARED=true

  vp_worker_service_registration_env() { return 0; }
  vp_worker_service_secret_specs() { return 0; }
  vp_require_pipeline_network_identity() { return 0; }
  vp_worker_service_contract() { printf '%s|||||304\n' "$1"; }
  vp_resolve_gpu_mode() { printf 'cpu\n'; }
  vp_python_worker_env() { return 0; }
  vp_vision_worker_env() { return 0; }
  vp_publisher_env() { return 0; }
  vp_publisher_env_is_sensitive() { return 1; }
  vp_publisher_service_state() { printf 'exists\n'; }
  vp_registered_worker_service_current_id() {
    printf '%s\n' "$replacement_service_id"
  }
  vp_service_values() { return 0; }
  vp_gpu_constraint_update_args() { return 0; }
  vp_mutate_registered_worker_service() {
    printf '%s\n' "$*" >>"$mutation_calls"
  }
  vp_require_worker_redis_marker_status() { return 0; }
  swarm_service_running() { return 0; }
  vp_require_service_node() { return 0; }
  vp_require_managed_worker_storage_ready() { return 0; }
  http_health() { return 0; }
  docker() {
    if [[ "$1" == node && "$2" == update ]]; then
      printf 'docker|%s\n' "$*" >>"$mutation_calls"
    fi
    return 0
  }

  local_deploy=""
  for local_deploy in \
    vp_deploy_python_worker \
    vp_deploy_vision_worker \
    vp_deploy_publisher; do
    if "$local_deploy" vp-worker:rollback "$baseline_service_id"; then
      echo "FAIL: $local_deploy accepted a replaced rollback service identity" >&2
      exit 1
    fi
  done
  if [[ -s "$mutation_calls" ]]; then
    echo 'FAIL: replaced rollback service identity reached a mutation' >&2
    exit 1
  fi
)

(
  head_helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  legacy_schema_one_commit=a0e1afa1cb837ad89eca8fa1eb61ed568eb44a6a
  legacy_fixture="$TEST_ROOT/legacy-schema-1"
  legacy_archive="$legacy_fixture/archive"
  mkdir -p "$legacy_archive"
  if ! git -C "$ROOT_DIR" cat-file -e \
    "$legacy_schema_one_commit^{commit}" 2>/dev/null; then
    echo 'FAIL: legacy schema-1 journal commit is unavailable' >&2
    exit 1
  fi
  git -C "$ROOT_DIR" archive --format=tar "$legacy_schema_one_commit" \
    deploy/swarm/worker-admission-transaction.py \
    | tar -xf - -C "$legacy_archive"
  old_helper="$legacy_archive/deploy/swarm/worker-admission-transaction.py"
  [[ -f "$old_helper" && ! -L "$old_helper" ]]

  credentials=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$legacy_fixture/credential-$index"
    printf 'postgresql://legacy-%s:credential@database/videoprocess\n' \
      "$index" >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    python3 "$old_helper" validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"
  commit=7123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend:deploy-${commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${commit:0:12}"
  control_image="vp-ffmpeg-worker-python:deploy-${commit:0:12}"
  control_generation="c-${commit:0:20}"
  operator_reference="control/$control_generation/worker-registration-operator-database-url"
  control_secret_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  runtime_secret_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

  create_legacy_journal() {
    local phase="$1"
    local case_root="$2"
    local transaction_root="$case_root/sync/state/vp-worker-admission"
    mkdir -p "$transaction_root"
    chmod 0700 "$transaction_root"
    local lock_path
    lock_path="$(
      python3 "$old_helper" lock-prepare "$transaction_root"
    )"
    exec 18<>"$lock_path"
    python3 "$old_helper" lock-acquire \
      "$transaction_root" 18 >/dev/null
    python3 "$old_helper" begin \
      "$transaction_root" 18 \
      "$commit" "$backend_image" "$go_image" "$commit" \
      legacy_no_control <<<"$credential_records" >/dev/null
    if [[ "$phase" == ABORTING ]]; then
      python3 "$old_helper" record-prepared-secret \
        "$transaction_root" 18 \
        "vp-wc-operator-$control_generation" "$control_secret_id" \
        vp-worker-control "$control_generation" operator >/dev/null
      python3 "$old_helper" record-prepared-secret \
        "$transaction_root" 18 \
        vp-wr-ffmpeg-go-db-971 "$runtime_secret_id" \
        vp-ffmpeg-worker-go-swarm 971 database >/dev/null
      python3 "$old_helper" begin-abort \
        "$transaction_root" 18 2 preparing_failed >/dev/null
    fi
    exec 18>&-
  }

  for legacy_phase in PREPARING ABORTING; do
    case_root="$legacy_fixture/$legacy_phase"
    create_legacy_journal "$legacy_phase" "$case_root"
    transaction_root="$case_root/sync/state/vp-worker-admission"
    active="$transaction_root/transactions/active.json"
    original="$case_root/active.original.json"
    cp "$active" "$original"
    chmod 0600 "$original"
    python3 - "$active" "$legacy_phase" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
phase = sys.argv[2]
raw = path.read_bytes()
document = json.loads(raw)
if raw != (
    json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
).encode("utf-8"):
    raise SystemExit("legacy helper did not emit canonical JSON")
if (
    type(document["schema"]) is not int
    or document["schema"] != 1
    or document["phase"] != phase
):
    raise SystemExit("legacy helper emitted the wrong schema or phase")
if "authorities" in document:
    raise SystemExit("legacy helper unexpectedly emitted authority WAL")
if phase == "ABORTING":
    authorities = document["abort"]["authorities"]
    if (
        len(authorities) != 2
        or any(
            set(authority) != {"generation", "kind", "service"}
            for authority in authorities
        )
    ):
        raise SystemExit("legacy abort authority shape drifted")
PY

    plan_one="$case_root/quarantine-one.json"
    plan_two="$case_root/quarantine-two.json"
    if ! python3 "$head_helper" replay-plan \
      "$transaction_root" >"$plan_one"; then
      echo "FAIL: HEAD did not type quarantine legacy $legacy_phase" >&2
      exit 1
    fi
    python3 "$head_helper" replay-plan \
      "$transaction_root" >"$plan_two"
    cmp -s "$plan_one" "$plan_two"
    python3 - "$plan_one" "$active" "$legacy_phase" <<'PY'
import hashlib
import json
import pathlib
import sys

plan_path = pathlib.Path(sys.argv[1])
active_path = pathlib.Path(sys.argv[2])
phase = sys.argv[3]
raw_plan = plan_path.read_bytes()
plan = json.loads(raw_plan)
if raw_plan != (
    json.dumps(plan, sort_keys=True, separators=(",", ":")) + "\n"
).encode("utf-8"):
    raise SystemExit("legacy quarantine plan is not canonical")
if plan != {
    "active": True,
    "allow_new_candidate": False,
    "allow_stale_cleanup": False,
    "journal_sha256": hashlib.sha256(active_path.read_bytes()).hexdigest(),
    "namespace": None,
    "next_action": "QUARANTINE_LEGACY_SCHEMA_1",
    "pending_operation": None,
    "phase": phase,
    "reason_code": "legacy_schema_1_authority_context_unavailable",
    "retirements": [],
    "revision": plan["revision"],
    "transaction_id": plan["transaction_id"],
}:
    raise SystemExit("legacy quarantine plan is not deterministic or closed")
if (
    not isinstance(plan["revision"], int)
    or isinstance(plan["revision"], bool)
    or plan["revision"] < 0
    or not isinstance(plan["transaction_id"], str)
    or not plan["transaction_id"].startswith("tx-")
):
    raise SystemExit("legacy quarantine identity is not typed")
if b"postgresql://" in raw_plan or b"credential@" in raw_plan:
    raise SystemExit("legacy quarantine disclosed credential material")

probe = b'{"transaction_id":"tx-postgresql://credential@sentinel"}'
try:
    if b"postgresql://" in probe or b"credential@" in probe:
        raise SystemExit("legacy quarantine disclosed credential material")
except SystemExit as error:
    if str(error) != "legacy quarantine disclosed credential material":
        raise
else:
    raise SystemExit("legacy quarantine credential assertion is unreachable")
PY
    cmp -s "$active" "$original"

    if [[ "$legacy_phase" == PREPARING ]]; then
      for schema_literal in 1.0 2.0 -0.0 true; do
        schema_case="${schema_literal//[^A-Za-z0-9]/_}"
        forged="$case_root/legacy-schema-$schema_case.json"
        python3 - "$active" "$schema_literal" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
document = json.loads(path.read_bytes())
schema = json.loads(sys.argv[2])
if type(schema) is int:
    raise SystemExit("forged legacy schema unexpectedly has integer type")
document["schema"] = schema
path.write_bytes(
    (
        json.dumps(document, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
)
PY
        cp "$active" "$forged"
        if python3 "$head_helper" replay-plan \
          "$transaction_root" >/dev/null 2>&1; then
          echo \
            "FAIL: legacy active reader accepted non-integer schema $schema_literal" \
            >&2
          exit 1
        fi
        if ! cmp -s "$active" "$forged"; then
          echo \
            "FAIL: rejected legacy schema $schema_literal mutated active bytes" \
            >&2
          exit 1
        fi
        cp "$original" "$active"
        chmod 0600 "$active"
      done
    fi

    lock_path="$transaction_root/transaction.lock"
    exec 18<>"$lock_path"
    python3 "$head_helper" lock-acquire \
      "$transaction_root" 18 >/dev/null
    if python3 "$head_helper" begin \
      "$transaction_root" 18 \
      "$commit" "$backend_image" "$go_image" replacement-namespace \
      legacy_no_control <<<"$credential_records" >/dev/null 2>&1; then
      echo "FAIL: legacy $legacy_phase quarantine allowed begin" >&2
      exit 1
    fi
    if [[ "$legacy_phase" == PREPARING ]]; then
      if python3 "$head_helper" begin-abort \
        "$transaction_root" 18 0 preparing_failed \
        >/dev/null 2>&1; then
        echo 'FAIL: legacy PREPARING quarantine allowed normal mutation' >&2
        exit 1
      fi
    else
      if python3 "$head_helper" intent-prepared-secret-removal \
        "$transaction_root" 18 3 "$runtime_secret_id" \
        >/dev/null 2>&1; then
        echo 'FAIL: legacy ABORTING quarantine allowed abort mutation' >&2
        exit 1
      fi
    fi
    retirement_identity="$case_root/retirement.json"
    printf '%s\n' \
      '{"docker_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","generation":"971","kind":"secret","name":"legacy-retirement","purpose":"database","service":"vp-ffmpeg-worker-go-swarm","spec_digest":null}' \
      >"$retirement_identity"
    chmod 0600 "$retirement_identity"
    if python3 "$head_helper" queue-retirement \
      "$transaction_root" 18 0 \
      retirement-cccccccccccccccccccccccccccccccc \
      "$retirement_identity" >/dev/null 2>&1; then
      echo "FAIL: legacy $legacy_phase quarantine allowed retirement" >&2
      exit 1
    fi
    if python3 "$head_helper" archive \
      "$transaction_root" 18 0 >/dev/null 2>&1; then
      echo "FAIL: legacy $legacy_phase quarantine allowed archive" >&2
      exit 1
    fi
    exec 18>&-
    cmp -s "$active" "$original"

    calls="$case_root/operator-calls"
    : >"$calls"
    ROOT="$case_root/sync"
    REPO_ROOT="$case_root/repos"
    VP_WORKER_DATABASE_CREDENTIAL_RECORDS="$credential_records"
    docker() {
      printf 'docker|%s\n' "$*" >>"$calls"
    }
    vp_worker_admission_operator() {
      printf 'operator|%s\n' "$*" >>"$calls"
    }
    vp_worker_admission_lock_acquire "$transaction_root"
    if vp_worker_admission_prepare_transaction \
      "$backend_image" "$go_image" "$control_image" \
      >/dev/null 2>&1; then
      echo "FAIL: shell admission resumed legacy $legacy_phase quarantine" >&2
      exit 1
    fi
    vp_worker_admission_lock_release
    if [[ -s "$calls" ]]; then
      echo "FAIL: legacy $legacy_phase quarantine reached Docker or operator" >&2
      exit 1
    fi
    cmp -s "$active" "$original"

    VP_RUNTIME_HOST=10.0.0.127
    VP_RUNTIME_NODE=colima-127
    VP_MANAGER_NODE=ccttww-lap
    VP_PIPELINE_NETWORK=vp-pipeline-net
    VP_PIPELINE_NETWORK_ID=""
    VP_API_DATABASE_URL_GO=synthetic-go-database-url
    VP_PYTHON_WORKER_DATABASE_URL=synthetic-python-database-url
    VP_MINIO_ACCESS_KEY=synthetic-access-key
    VP_MINIO_SECRET_KEY=synthetic-secret-key
    VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE="${credentials[0]}"
    VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE="${credentials[1]}"
    VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE="${credentials[2]}"
    VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE="${credentials[3]}"
    VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL="${principals[0]}"
    VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL="${principals[1]}"
    VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL="${principals[2]}"
    VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL="${principals[3]}"
    python3 - "${credentials[@]}" <<'PY'
import os
import stat
import sys

identities = set()
for raw_path in sys.argv[1:]:
    metadata = os.stat(raw_path)
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o400:
        raise SystemExit("full-entry credential identity is invalid")
    identities.add((metadata.st_dev, metadata.st_ino))
if len(sys.argv[1:]) != 4 or len(identities) != 4:
    raise SystemExit("full-entry credentials are not four distinct files")
PY
    docker() {
      printf 'docker|%s\n' "$*" >>"$calls"
      if [[ "${1:-} ${2:-}" == 'network inspect' ]]; then
        printf 'network-id|vp-pipeline-net|overlay|swarm\n'
        return 0
      fi
      if [[ "${1:-}" == run ]]; then
        local principal=""
        local index
        for index in 0 1 2 3; do
          if [[ " $* " == *"/credential-$index,"* ]]; then
            principal="${principals[$index]}"
            break
          fi
        done
        [[ -n "$principal" ]] || return 97
        printf '{"current_user":"%s","session_user":"%s"}\n' \
          "$principal" "$principal"
        return 0
      fi
      return 98
    }
    : >"$calls"
    full_entry_error="$case_root/full-entry.stderr"
    set +e
    deploy_vp_app_services \
      synthetic-pds synthetic-frontend "$backend_image" \
      synthetic-channelops "$go_image" "$control_image" \
      >/dev/null 2>"$full_entry_error"
    full_entry_status=$?
    set -e
    docker_call_count="$(grep -c '^docker|' "$calls" || true)"
    operator_call_count="$(grep -c '^operator|' "$calls" || true)"
    if [[ "$full_entry_status" -eq 0 \
      || "$docker_call_count" -ne 0 \
      || "$operator_call_count" -ne 0 \
      || "$(<"$full_entry_error")" \
        != "worker admission transaction quarantined: legacy_schema_1_authority_context_unavailable ($legacy_phase)" ]]; then
      echo \
        "FAIL: full deploy entry touched Docker/operator before legacy $legacy_phase quarantine (docker=$docker_call_count operator=$operator_call_count)" \
        >&2
      exit 1
    fi
    cmp -s "$active" "$original"
  done
)

(
  helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  credential_records=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  credentials=()
  for index in 0 1 2 3; do
    credential="$TEST_ROOT/vision-replay-credential-$index"
    printf 'postgresql://vision-%s:credential@database/videoprocess\n' \
      "$index" >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    python3 "$helper" validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"

  for interrupted_state in planned created terminal; do
    transaction_root="$TEST_ROOT/vision-replay-$interrupted_state"
    mkdir -p "$transaction_root"
    chmod 0700 "$transaction_root"
    lock_path="$(python3 "$helper" lock-prepare "$transaction_root")"
    exec 18<>"$lock_path"
    python3 "$helper" lock-acquire "$transaction_root" 18 >/dev/null
    commit=3123456789abcdef0123456789abcdef01234567
    python3 "$helper" begin \
      "$transaction_root" 18 "$commit" \
      "vp-backend:deploy-${commit:0:12}" \
      "vp-ffmpeg-worker-go:deploy-${commit:0:12}" \
      "vision-$interrupted_state" legacy_no_control \
      <<<"$credential_records" >/dev/null
    python3 "$helper" record-runtime-secret \
      "$transaction_root" 18 0 watcher runtime-vision \
      vp-watcher-redis-runtime-vision \
      aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa >/dev/null
    python3 "$helper" prepare-vision-job \
      "$transaction_root" 18 1 check \
      vp-vision-cutover-check-0123456789ab \
      "vp-ffmpeg-worker-python:deploy-${commit:0:12}" \
      watcher - - >/dev/null
    revision=2
    if [[ "$interrupted_state" != planned ]]; then
      python3 "$helper" record-vision-job-service \
        "$transaction_root" 18 "$revision" check \
        bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb >/dev/null
      revision="$((revision + 1))"
    fi
    if [[ "$interrupted_state" == terminal ]]; then
      python3 "$helper" record-vision-job-terminal \
        "$transaction_root" 18 "$revision" check 0 >/dev/null
      revision="$((revision + 1))"
    fi
    python3 "$helper" abort-vision-job-removal \
      "$transaction_root" 18 "$revision" check >/dev/null
    revision="$((revision + 1))"
    vision_job="$(
      python3 "$helper" lookup-vision-job "$transaction_root" check
    )"
    python3 - "$vision_job" "$interrupted_state" <<'PY'
import json
import re
import sys

job = json.loads(sys.argv[1])
interrupted_state = sys.argv[2]
if (
    job["state"] != "removed"
    or job["exit_code"] != 255
    or (interrupted_state == "planned")
    != (job["docker_service_id"] is None)
):
    raise SystemExit("interrupted vision job did not become replay-safe")
PY
    python3 "$helper" begin-abort \
      "$transaction_root" 18 "$revision" interrupted_vision >/dev/null
    revision="$((revision + 1))"
    python3 "$helper" finish-abort \
      "$transaction_root" 18 "$revision" >/dev/null
    python3 - "$transaction_root/transactions/active.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
if document["phase"] != "DONE" or document["outcome"] != "aborted":
    raise SystemExit("vision replay did not allow abort completion")
PY
    exec 18>&-
  done

  transaction_root="$TEST_ROOT/vision-final-safety"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  lock_path="$(python3 "$helper" lock-prepare "$transaction_root")"
  exec 18<>"$lock_path"
  python3 "$helper" lock-acquire "$transaction_root" 18 >/dev/null
  commit=4123456789abcdef0123456789abcdef01234567
  python3 "$helper" begin \
    "$transaction_root" 18 "$commit" \
    "vp-backend:deploy-${commit:0:12}" \
    "vp-ffmpeg-worker-go:deploy-${commit:0:12}" \
    vision-final-safety legacy_no_control \
    <<<"$credential_records" >/dev/null
  transaction_id="$(
    python3 "$helper" replay-plan "$transaction_root" \
      | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["transaction_id"])'
  )"
  python3 "$helper" record-runtime-secret \
    "$transaction_root" 18 0 watcher runtime-vision-final \
    vp-watcher-redis-runtime-vision-final \
    cccccccccccccccccccccccccccccccc >/dev/null
  final_database_name=vp-vision-cutover-final-read-db-0123456789ab
  final_database_id=dddddddddddddddddddddddddddddddd
  if ! python3 "$helper" record-prepared-secret \
    "$transaction_root" 18 \
    "$final_database_name" "$final_database_id" \
    vision-cutover "$transaction_id" final-safety-database >/dev/null; then
    echo 'FAIL: transaction rejected the final vision safety secret' >&2
    exit 1
  fi
  if ! python3 "$helper" prepare-vision-job \
    "$transaction_root" 18 2 final-safety \
    vp-vision-cutover-final-safety-0123456789ab \
    "vp-ffmpeg-worker-python:deploy-${commit:0:12}" \
    watcher "$final_database_name" "$final_database_id" >/dev/null; then
    echo 'FAIL: transaction rejected the final vision safety job' >&2
    exit 1
  fi
  final_job="$(
    python3 "$helper" lookup-vision-job "$transaction_root" final-safety
  )"
  python3 -I - "$final_database_name" "$final_database_id" \
    "$final_job" <<'PY'
import json
import sys

name, docker_id, raw = sys.argv[1:]
job = json.loads(raw)
if (
    job["mode"] != "final-safety"
    or job["name"] != "vp-vision-cutover-final-safety-0123456789ab"
    or job["database_secret"]["name"] != name
    or job["database_secret"]["docker_secret_id"] != docker_id
    or job["database_secret"]["purpose"] != "final-safety-database"
):
    raise SystemExit("final vision safety identity was not durable")
PY
  exec 18>&-
)

(
  transaction_helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  transaction_root="$TEST_ROOT/durable-stage2/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  transaction_cli() {
    python3 "$transaction_helper" "$@"
  }

  lock_path="$(transaction_cli lock-prepare "$transaction_root")"
  exec 18<>"$lock_path"
  transaction_cli lock-acquire "$transaction_root" 18 >/dev/null
  credentials=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$TEST_ROOT/durable-stage2/credential-$index"
    printf 'postgresql://stage2-%s:credential@database/videoprocess\n' "$index" \
      >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    transaction_cli validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"
  commit=2123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend:deploy-${commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${commit:0:12}"
  transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" "$commit" \
    legacy_no_control <<<"$credential_records" >/dev/null

  transaction_cli init-app-progress "$transaction_root" 18 >/dev/null
  transaction_cli init-app-progress "$transaction_root" 18 >/dev/null

  assert_transition_rejected() {
    local revision="$1"
    local target_phase="$2"
    local reason="$3"
    local active="$transaction_root/transactions/active.json"
    local before
    before="$(shasum -a 256 "$active")"
    if transaction_cli transition \
      "$transaction_root" 18 "$revision" "$target_phase" \
      >/dev/null 2>&1; then
      echo "FAIL: transaction entered $target_phase $reason" >&2
      exit 1
    fi
    if [[ "$(shasum -a 256 "$active")" != "$before" ]]; then
      echo "FAIL: rejected $target_phase transition mutated the journal" >&2
      exit 1
    fi
  }

  assert_transition_rejected \
    0 FORWARD_APPLYING 'without a captured baseline'

  baseline="$TEST_ROOT/durable-stage2/baseline.json"
  python3 - "$baseline" <<'PY'
import json
import pathlib
import sys

services = [
    "vp-api-swarm",
    "vp-frontend-swarm",
    "vp-autoflow-api-swarm",
    "vp-event-outbox-relay-swarm",
    "vp-channel-agent-runner-swarm",
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
]
payload = {
    "control": None,
    "kind": "legacy_no_control",
    "services": [
        {
            "docker_service_id": None,
            "existed": False,
            "image": None,
            "name": service,
            "spec_digest": None,
        }
        for service in services
    ],
}
pathlib.Path(sys.argv[1]).write_text(
    json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  transaction_cli capture-baseline \
    "$transaction_root" 18 0 <"$baseline" >/dev/null

  worker_plan="$TEST_ROOT/durable-stage2/worker-plan.json"
  printf '%s\n' \
    "{\"admission_secret\":{\"docker_secret_id\":\"bbbbbbbbbbbbbbbbbbbbbbbb\",\"generation\":\"901\",\"name\":\"vp-stage2-admission-901\",\"purpose\":\"admission\",\"service\":\"vp-ffmpeg-worker-go-swarm\"},\"commit\":\"$commit\",\"database_secret\":{\"docker_secret_id\":\"cccccccccccccccccccccccc\",\"generation\":\"901\",\"name\":\"vp-stage2-database-901\",\"purpose\":\"database\",\"service\":\"vp-ffmpeg-worker-go-swarm\"},\"generation\":901,\"image\":\"vp-ffmpeg-worker-go:rollback\",\"service\":\"vp-ffmpeg-worker-go-swarm\",\"target_spec_digest\":null}" \
    >"$worker_plan"
  transaction_cli record-worker-plan \
    "$transaction_root" 18 1 forward <"$worker_plan" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 2 FORWARD_APPLYING >/dev/null
  transaction_cli record-app-attempt \
    "$transaction_root" 18 vp-api-swarm >/dev/null
  transaction_cli record-app-attempt \
    "$transaction_root" 18 vp-api-swarm >/dev/null
  transaction_cli record-app-attempt \
    "$transaction_root" 18 vp-frontend-swarm >/dev/null
  transaction_cli remove-app-attempt \
    "$transaction_root" 18 vp-frontend-swarm >/dev/null
  transaction_cli remove-app-attempt \
    "$transaction_root" 18 vp-frontend-swarm >/dev/null
  if transaction_cli advance-migration-state \
    "$transaction_root" 18 pending applied >/dev/null 2>&1; then
    echo 'FAIL: durable app progress skipped the migration applying boundary' >&2
    exit 1
  fi
  transaction_cli advance-migration-state \
    "$transaction_root" 18 pending applying >/dev/null
  transaction_cli advance-migration-state \
    "$transaction_root" 18 pending applying >/dev/null
  transaction_cli advance-migration-state \
    "$transaction_root" 18 applying applied >/dev/null
  app_progress="$TEST_ROOT/durable-stage2/app-progress.json"
  transaction_cli read-app-progress "$transaction_root" >"$app_progress"
  python3 - "$app_progress" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    progress = json.load(handle)
if (
    progress["schema"] != 1
    or progress["attempted_services"] != ["vp-api-swarm"]
    or progress["migration_state"] != "applied"
    or re.fullmatch(r"tx-[0-9a-f]{32}", progress["transaction_id"]) is None
):
    raise SystemExit("durable app progress did not replay monotonically")
PY
  assert_transition_rejected \
    3 FORWARD_VERIFIED 'with a pending forward worker'
  assert_transition_rejected \
    3 ROLLBACK_PREPARING 'without a failed-forward snapshot'

  failed_forward="$TEST_ROOT/durable-stage2/failed-forward.json"
  printf '%s\n' \
    '{"control":null,"services":[{"docker_service_id":"aaaaaaaaaaaaaaaaaaaaaaaa","existed":true,"image":"vp-ffmpeg-worker-go:failed","name":"vp-ffmpeg-worker-go-swarm","spec_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}' \
    >"$failed_forward"
  transaction_cli capture-failed-forward \
    "$transaction_root" 18 3 <"$failed_forward" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 4 ROLLBACK_PREPARING >/dev/null
  assert_transition_rejected \
    5 ROLLBACK_APPLYING 'without an allocated rollback attempt'

  rollback_one="$TEST_ROOT/durable-stage2/rollback-one.json"
  rollback_two="$TEST_ROOT/durable-stage2/rollback-two.json"
  transaction_cli allocate-rollback-attempt \
    "$transaction_root" 18 5 >"$rollback_one"
  transaction_cli allocate-rollback-attempt \
    "$transaction_root" 18 6 >"$rollback_two"
  python3 - "$rollback_one" "$rollback_two" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    first = json.load(handle)
with open(sys.argv[2], encoding="utf-8") as handle:
    second = json.load(handle)
for value in (first, second):
    rollback = value["rollback"]
    if (
        rollback["attempt"] != 1
        or re.fullmatch(r"rollback-[1-9][0-9]{1,19}", rollback["namespace"])
        is None
        or re.fullmatch(r"m-rb-[0-9a-f]{12}-1", rollback["marker_generation"])
        is None
    ):
        raise SystemExit("rollback allocation identity is invalid")
if first["rollback"] != second["rollback"]:
    raise SystemExit("rollback replay minted a second identity")
if first["revision"] != 6 or second["revision"] != 6:
    raise SystemExit("idempotent rollback replay changed the revision")
PY

  transaction_cli record-worker-plan \
    "$transaction_root" 18 6 rollback <"$worker_plan" >/dev/null
  transaction_cli record-worker-plan \
    "$transaction_root" 18 7 rollback <"$worker_plan" >"$TEST_ROOT/durable-stage2/worker-plan-replay.json"
  if [[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["revision"])' \
      "$TEST_ROOT/durable-stage2/worker-plan-replay.json")" -ne 7 ]]; then
    echo 'FAIL: durable worker plan replay changed the revision' >&2
    exit 1
  fi
  transaction_cli transition \
    "$transaction_root" 18 7 ROLLBACK_APPLYING >/dev/null
  assert_transition_rejected \
    8 ROLLBACK_VERIFIED 'with a pending rollback worker'
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 8 rollback \
    vp-ffmpeg-worker-go-swarm 901 pending prepared - - >/dev/null
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 9 rollback \
    vp-ffmpeg-worker-go-swarm 901 pending prepared - - \
    >"$TEST_ROOT/durable-stage2/worker-prepared-replay.json"
  if [[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["revision"])' \
      "$TEST_ROOT/durable-stage2/worker-prepared-replay.json")" -ne 9 ]]; then
    echo 'FAIL: durable prepared-stage replay changed the revision' >&2
    exit 1
  fi

  active="$transaction_root/transactions/active.json"
  before_skip="$(shasum -a 256 "$active")"
  if transaction_cli advance-worker-stage \
    "$transaction_root" 18 9 rollback \
    vp-ffmpeg-worker-go-swarm 901 prepared verified \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    >/dev/null 2>&1; then
    echo 'FAIL: durable worker stage accepted a skipped apply' >&2
    exit 1
  fi
  [[ "$(shasum -a 256 "$active")" == "$before_skip" ]]

  transaction_cli advance-worker-stage \
    "$transaction_root" 18 9 rollback \
    vp-ffmpeg-worker-go-swarm 901 prepared applied \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    >/dev/null
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 10 rollback \
    vp-ffmpeg-worker-go-swarm 901 prepared applied \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    >"$TEST_ROOT/durable-stage2/worker-applied-replay.json"
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 10 rollback \
    vp-ffmpeg-worker-go-swarm 901 applied verified \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    >/dev/null
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 11 rollback \
    vp-ffmpeg-worker-go-swarm 901 pending prepared - - \
    >"$TEST_ROOT/durable-stage2/worker-stale-prepared-replay.json"
  transaction_cli advance-worker-stage \
    "$transaction_root" 18 11 rollback \
    vp-ffmpeg-worker-go-swarm 901 prepared applied \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    >"$TEST_ROOT/durable-stage2/worker-stale-applied-replay.json"
  for replay in \
    "$TEST_ROOT/durable-stage2/worker-stale-prepared-replay.json" \
    "$TEST_ROOT/durable-stage2/worker-stale-applied-replay.json"; do
    if [[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["revision"])' \
        "$replay")" -ne 11 ]]; then
      echo 'FAIL: stale durable worker stage replay changed the revision' >&2
      exit 1
    fi
  done
  transaction_cli transition \
    "$transaction_root" 18 11 ROLLBACK_VERIFIED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 12 ROLLBACK_WORKERS_PROMOTED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 13 ROLLBACK_MARKER_PROMOTED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 14 ROLLBACK_CONTROL_PROMOTED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 15 RETIRING >/dev/null

  replay_state="$TEST_ROOT/durable-stage2/replay-state.json"
  transaction_cli replay-state "$transaction_root" >"$replay_state"
  python3 - "$replay_state" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    state = json.load(handle)
worker = state["rollback"]["workers"][0]
if (
    state["schema"] != 3
    or state["phase"] != "RETIRING"
    or state["retiring_outcome"] != "rolled_back"
    or not state["baseline"]["captured"]
    or not state["failed_forward"]["captured"]
    or worker["applied_stage"] != "verified"
    or worker["target_spec_digest"]
    != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    or state["app_progress"]["attempted_services"] != ["vp-api-swarm"]
    or state["app_progress"]["migration_state"] != "applied"
):
    raise SystemExit("fresh-process rollback replay state is incomplete")
PY
  transaction_cli transition \
    "$transaction_root" 18 16 DONE rolled_back >/dev/null
  transaction_cli archive "$transaction_root" 18 17 >/dev/null
  exec 18>&-
)

(
  transaction_helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  transaction_root="$TEST_ROOT/durable-core/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  transaction_cli() {
    python3 "$transaction_helper" "$@"
  }

  lock_path="$(
    transaction_cli lock-prepare "$transaction_root"
  )"
  [[ "$lock_path" == "$transaction_root/transaction.lock" ]]
  exec 18<>"$lock_path"
  transaction_cli lock-acquire "$transaction_root" 18

  credentials=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  purposes=(
    deploy_migrator
    deploy_read
    control_role_owner
    runtime_role_owner
  )
  for purpose in "${purposes[@]}"; do
    credential="$TEST_ROOT/durable-core/$purpose"
    printf 'postgresql://%s:credential@database/videoprocess\n' "$purpose" \
      >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    transaction_cli validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"

  commit=0123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend:deploy-${commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${commit:0:12}"
  namespace="$commit"
  if ! transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" "$namespace" \
    legacy_no_control \
    <<<"$credential_records" \
    >"$TEST_ROOT/durable-core/begin.json"; then
    echo 'FAIL: durable transaction PREPARING begin is unavailable' >&2
    exit 1
  fi

  active="$transaction_root/transactions/active.json"
  [[ -f "$active" && ! -L "$active" ]]
  [[ "$(vp_worker_redis_marker_file_mode "$active")" == 600 ]]
  python3 - "$active" "$transaction_root" <<'PY'
import json
import os
import pathlib
import re
import stat
import sys

active = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
raw = active.read_bytes()
document = json.loads(raw)
expected = (
    json.dumps(document, sort_keys=True, separators=(",", ":"))
    + "\n"
).encode("utf-8")
if raw != expected:
    raise SystemExit("active transaction is not canonical JSON")
if type(document["schema"]) is not int or document["schema"] != 3:
    raise SystemExit("unexpected transaction schema")
if re.fullmatch(r"tx-[0-9a-f]{32}", document["transaction_id"]) is None:
    raise SystemExit("transaction id is not 128-bit lowercase hex")
if document["phase"] != "PREPARING" or document["revision"] != 0:
    raise SystemExit("new transaction is not PREPARING revision zero")
if document["promotion"] != {
    "control": False,
    "marker": False,
    "workers": False,
}:
    raise SystemExit("promotion state is not split")
if document["pending_retirements"] or document["operation"] is not None:
    raise SystemExit("new transaction has destructive work")
credentials = document["database_credentials"]
if sorted(credentials) != [
    "control_role_owner",
    "deploy_migrator",
    "deploy_read",
    "runtime_role_owner",
]:
    raise SystemExit("database purposes are incomplete")
identities = {
    (entry["device"], entry["inode"]) for entry in credentials.values()
}
paths = {entry["canonical_path"] for entry in credentials.values()}
principals = {
    entry["expected_principal"] for entry in credentials.values()
}
if len(identities) != 4 or len(paths) != 4 or len(principals) != 4:
    raise SystemExit("database identities are not pairwise distinct")
if any(entry["mode"] != 0o400 for entry in credentials.values()):
    raise SystemExit("database credential mode was not captured")
tx_dir = root / "transactions" / document["transaction_id"]
metadata = tx_dir.lstat()
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
):
    raise SystemExit("transaction directory mode is invalid")
snapshots_path = tx_dir / "snapshots.json"
snapshots_metadata = snapshots_path.lstat()
if (
    not stat.S_ISREG(snapshots_metadata.st_mode)
    or snapshots_path.is_symlink()
    or stat.S_IMODE(snapshots_metadata.st_mode) != 0o600
):
    raise SystemExit("snapshot journal identity or mode is invalid")
snapshots_raw = snapshots_path.read_bytes()
snapshots = json.loads(snapshots_raw)
snapshots_expected = (
    json.dumps(snapshots, sort_keys=True, separators=(",", ":"))
    + "\n"
).encode("utf-8")
if snapshots_raw != snapshots_expected:
    raise SystemExit("snapshots journal is not canonical JSON")
if type(snapshots["schema"]) is not int or snapshots["schema"] != 1:
    raise SystemExit("snapshot schema is not an exact integer")
if snapshots != {
    "baseline": {"control": None, "services": []},
    "failed_forward": {"control": None, "services": []},
    "forward": {"control": None, "marker": None, "workers": []},
    "janitor": {"service": None},
    "revision": 0,
    "rollback": {"control": None, "marker": None, "workers": []},
    "schema": 1,
    "transaction_id": document["transaction_id"],
}:
    raise SystemExit("unexpected initial snapshots journal")
if b"credential@" in raw or b"postgresql://" in raw:
    raise SystemExit("transaction persisted credential material")
if b"credential@" in snapshots_raw or b"postgresql://" in snapshots_raw:
    raise SystemExit("snapshots journal persisted credential material")
PY

  valid_active="$TEST_ROOT/durable-core/active-valid.json"
  cp "$active" "$valid_active"
  chmod 0600 "$valid_active"
  schema_failures=0
  for schema_literal in 1.0 3.0 -0.0 true; do
    schema_case="${schema_literal//[^A-Za-z0-9]/_}"
    forged_active="$TEST_ROOT/durable-core/active-forged-$schema_case.json"
    python3 - "$active" "$schema_literal" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
document = json.loads(path.read_bytes())
schema = json.loads(sys.argv[2])
if type(schema) is int:
    raise SystemExit("forged current schema unexpectedly has integer type")
document["schema"] = schema
path.write_bytes(
    (
        json.dumps(document, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
)
PY
    cp "$active" "$forged_active"

    if transaction_cli replay-plan "$transaction_root" \
      >/dev/null 2>&1; then
      printf 'RED: replay accepted non-integer current schema %s\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi
    if ! cmp -s "$active" "$forged_active"; then
      printf 'RED: replay mutated forged current schema %s\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi

    cp "$forged_active" "$active"
    chmod 0600 "$active"
    if transaction_cli transition \
      "$transaction_root" 18 0 FORWARD_APPLYING \
      >/dev/null 2>&1; then
      printf 'RED: mutation accepted non-integer current schema %s\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi
    if ! cmp -s "$active" "$forged_active"; then
      printf 'RED: mutation changed forged current schema %s bytes\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi

    cp "$forged_active" "$active"
    chmod 0600 "$active"
    if transaction_cli archive "$transaction_root" 18 0 \
      >/dev/null 2>&1; then
      printf 'RED: archive accepted non-integer current schema %s\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi
    if ! cmp -s "$active" "$forged_active"; then
      printf 'RED: archive changed forged current schema %s bytes\n' \
        "$schema_literal" >&2
      schema_failures=$((schema_failures + 1))
    fi
  done
  cp "$valid_active" "$active"
  chmod 0600 "$active"
  if [[ "$schema_failures" -ne 0 ]]; then
    echo \
      "FAIL: current active schema discriminator accepted or mutated $schema_failures non-integer cases" \
      >&2
    exit 1
  fi

  transaction_id="$(
    python3 -c \
      'import json,sys; print(json.load(open(sys.argv[1]))["transaction_id"])' \
      "$active"
  )"
  snapshots_path="$transaction_root/transactions/$transaction_id/snapshots.json"
  python3 - "$transaction_helper" "$snapshots_path" <<'PY'
import importlib.util
import json
import pathlib
import sys

helper_path = pathlib.Path(sys.argv[1])
snapshots_path = pathlib.Path(sys.argv[2])
spec = importlib.util.spec_from_file_location(
    "worker_admission_transaction_schema_test",
    helper_path,
)
if spec is None or spec.loader is None:
    raise SystemExit("transaction helper could not be imported")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

valid_bytes = snapshots_path.read_bytes()
valid = json.loads(valid_bytes)
if type(valid["schema"]) is not int or valid["schema"] != 1:
    raise SystemExit("valid snapshot schema is not an exact integer")
for raw_schema in ("1.0", "2.0", "-0.0", "true"):
    forged = dict(valid)
    forged["schema"] = json.loads(raw_schema)
    if type(forged["schema"]) is int:
        raise SystemExit("forged snapshot schema unexpectedly has integer type")
    forged_bytes = (
        json.dumps(forged, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    snapshots_path.write_bytes(forged_bytes)
    try:
        module._validate_snapshots(
            module._decode_canonical(snapshots_path.read_bytes())
        )
    except module.TransactionError:
        pass
    else:
        raise SystemExit(
            f"snapshot discriminator accepted non-integer schema {raw_schema}"
        )
    if snapshots_path.read_bytes() != forged_bytes:
        raise SystemExit(
            f"rejected snapshot schema {raw_schema} mutated bytes"
        )
snapshots_path.write_bytes(valid_bytes)
PY

  before_invalid="$(shasum -a 256 "$active")"
  if transaction_cli transition \
    "$transaction_root" 18 0 CONTROL_PROMOTED >/dev/null 2>&1; then
    echo 'FAIL: transaction accepted an illegal phase transition' >&2
    exit 1
  fi
  [[ "$(shasum -a 256 "$active")" == "$before_invalid" ]]

  preparing_plan="$TEST_ROOT/durable-core/preparing-plan.json"
  transaction_cli replay-plan "$transaction_root" >"$preparing_plan"
  transaction_cli replay-plan "$transaction_root" \
    >"$TEST_ROOT/durable-core/preparing-plan-second.json"
  cmp -s \
    "$preparing_plan" \
    "$TEST_ROOT/durable-core/preparing-plan-second.json"
  python3 - "$preparing_plan" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    plan = json.load(handle)
if (
    plan["phase"] != "PREPARING"
    or plan["next_action"] != "RESUME_PREPARING"
    or plan["allow_new_candidate"]
    or plan["allow_stale_cleanup"]
    or plan["retirements"]
):
    raise SystemExit("PREPARING replay plan is unsafe")
PY

  transaction_cli capture-baseline \
    "$transaction_root" 18 0 \
    <"$TEST_ROOT/durable-stage2/baseline.json" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 1 FORWARD_APPLYING >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 2 FORWARD_VERIFIED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 3 WORKERS_PROMOTED >/dev/null

  retirement_identity="$TEST_ROOT/durable-core/retirement.json"
  retirement_id=retirement-0123456789abcdef0123456789abcdef
  secret_id=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  printf '%s\n' \
    "{\"docker_id\":\"$secret_id\",\"generation\":\"901\",\"kind\":\"secret\",\"name\":\"vp-worker-candidate\",\"purpose\":\"database\",\"service\":\"vp-ffmpeg-worker-go-swarm\",\"spec_digest\":null}" \
    >"$retirement_identity"
  chmod 0600 "$retirement_identity"
  transaction_cli queue-retirement \
    "$transaction_root" 18 4 "$retirement_id" "$retirement_identity" \
    >/dev/null

  replay_one="$TEST_ROOT/durable-core/workers-promoted-one.json"
  replay_two="$TEST_ROOT/durable-core/workers-promoted-two.json"
  transaction_cli replay-plan "$transaction_root" >"$replay_one"
  transaction_cli replay-plan "$transaction_root" >"$replay_two"
  cmp -s "$replay_one" "$replay_two"
  python3 - "$replay_one" "$namespace" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    plan = json.load(handle)
if plan != {
    "active": True,
    "allow_new_candidate": False,
    "allow_stale_cleanup": False,
    "namespace": sys.argv[2],
    "next_action": "PROMOTE_MARKER",
    "pending_operation": None,
    "phase": "WORKERS_PROMOTED",
    "retirements": [],
    "revision": 5,
    "transaction_id": plan["transaction_id"],
}:
    raise SystemExit("workers-promoted replay plan is not deterministic")
PY
  if transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" replacement-namespace \
    legacy_no_control \
    <<<"$credential_records" \
    >/dev/null 2>&1; then
    echo 'FAIL: active transaction allowed a new candidate namespace' >&2
    exit 1
  fi

  marker_identity="$TEST_ROOT/durable-core/marker-identity.json"
  printf '%s\n' \
    '{"docker_id":null,"generation":"marker-901","kind":"manifest","name":"marker-current.conf","purpose":"promotion","service":"worker-redis-marker-control","spec_digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}' \
    >"$marker_identity"
  chmod 0600 "$marker_identity"
  marker_intent="$(
    transaction_cli intent \
      "$transaction_root" 18 5 PROMOTE_MARKER "$marker_identity"
  )"
  marker_operation_id="$(
    python3 -c \
      'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
      <<<"$marker_intent"
  )"
  transaction_cli replay-plan "$transaction_root" \
    >"$TEST_ROOT/durable-core/marker-intent-plan.json"
  python3 - "$TEST_ROOT/durable-core/marker-intent-plan.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    plan = json.load(handle)
if (
    plan["next_action"] != "VERIFY_PROMOTE_MARKER"
    or plan["pending_operation"]["kind"] != "PROMOTE_MARKER"
    or plan["retirements"]
):
    raise SystemExit("operation-before-phase replay intent was lost")
PY
  transaction_cli complete-intent \
    "$transaction_root" 18 6 "$marker_operation_id" >/dev/null

  control_identity="$TEST_ROOT/durable-core/control-identity.json"
  printf '%s\n' \
    '{"docker_id":null,"generation":"control-901","kind":"manifest","name":"control-current.conf","purpose":"promotion","service":"worker-admission-control","spec_digest":"1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}' \
    >"$control_identity"
  chmod 0600 "$control_identity"
  control_intent="$(
    transaction_cli intent \
      "$transaction_root" 18 7 PROMOTE_CONTROL "$control_identity"
  )"
  control_operation_id="$(
    python3 -c \
      'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
      <<<"$control_intent"
  )"
  transaction_cli complete-intent \
    "$transaction_root" 18 8 "$control_operation_id" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 9 RETIRING >/dev/null
  transaction_cli replay-plan "$transaction_root" \
    >"$TEST_ROOT/durable-core/retiring-plan.json"
  python3 - "$TEST_ROOT/durable-core/retiring-plan.json" \
    "$retirement_id" "$secret_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    plan = json.load(handle)
if (
    plan["phase"] != "RETIRING"
    or plan["next_action"] != "RETIRE_EXACT_IDENTITIES"
    or len(plan["retirements"]) != 1
    or plan["retirements"][0]["retirement_id"] != sys.argv[2]
    or plan["retirements"][0]["identity"]["docker_id"] != sys.argv[3]
):
    raise SystemExit("RETIRING did not expose the exact queued identity")
PY
  transaction_cli complete-retirement \
    "$transaction_root" 18 10 "$retirement_id" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 11 DONE succeeded >/dev/null
  valid_done="$TEST_ROOT/durable-core/done-valid.json"
  forged_done="$TEST_ROOT/durable-core/done-schema-3_0.json"
  cp "$active" "$valid_done"
  chmod 0600 "$valid_done"
  python3 - "$active" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
document = json.loads(path.read_bytes())
document["schema"] = json.loads("3.0")
if type(document["schema"]) is int:
    raise SystemExit("forged DONE schema unexpectedly has integer type")
path.write_bytes(
    (
        json.dumps(document, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
)
PY
  cp "$active" "$forged_done"
  if transaction_cli archive "$transaction_root" 18 12 \
    >/dev/null 2>&1; then
    echo 'FAIL: archive accepted non-integer current schema 3.0' >&2
    exit 1
  fi
  if ! cmp -s "$active" "$forged_done"; then
    echo 'FAIL: rejected DONE schema 3.0 mutated active bytes' >&2
    exit 1
  fi
  cp "$valid_done" "$active"
  chmod 0600 "$active"
  done_path="$(
    transaction_cli archive "$transaction_root" 18 12
  )"
  [[ ! -e "$active" ]]
  [[ -f "$done_path" && ! -L "$done_path" ]]
  [[ "$(vp_worker_redis_marker_file_mode "$done_path")" == 600 ]]
  if compgen -G "$transaction_root/transactions/.active.json.tmp.*" \
    >/dev/null; then
    echo 'FAIL: durable transaction retained an atomic-write temp file' >&2
    exit 1
  fi
  exec 18>&-

  strict_root="$TEST_ROOT/durable-core-strict/state/vp-worker-admission"
  mkdir -p "$strict_root/transactions"
  chmod 0700 "$strict_root" "$strict_root/transactions"
  printf '%s\n' '{"schema":1,"unknown":true}' \
    >"$strict_root/transactions/active.json"
  chmod 0600 "$strict_root/transactions/active.json"
  if transaction_cli replay-plan "$strict_root" >/dev/null 2>&1; then
    echo 'FAIL: transaction reader accepted an unknown schema field' >&2
    exit 1
  fi
  python3 - \
    "$TEST_ROOT/durable-core/begin.json" \
    "$strict_root/transactions/active.json" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
service = {
    "docker_service_id": "0123456789abcdef01234567",
    "existed": True,
    "image": "vp-ffmpeg-worker-go:baseline",
    "name": "vp-ffmpeg-worker-go-swarm",
    "spec_digest": "0123456789abcdef" * 4,
}
document["baseline"]["services"] = [service, service.copy()]
pathlib.Path(sys.argv[2]).write_text(
    json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  chmod 0600 "$strict_root/transactions/active.json"
  if transaction_cli replay-plan "$strict_root" >/dev/null 2>&1; then
    echo 'FAIL: transaction reader accepted a duplicate service identity' >&2
    exit 1
  fi

  relational_failures=0
  for relational_case in \
    preparing-all-promoted \
    skipped-marker-promotion \
    rollback-promotion-contradiction \
    duplicate-logical-retirement; do
    python3 - \
      "$TEST_ROOT/durable-core/begin.json" \
      "$strict_root/transactions/active.json" \
      "$relational_case" <<'PY'
import json
import pathlib
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
case = sys.argv[3]
if case == "preparing-all-promoted":
    document["promotion"] = {
        "control": True,
        "marker": True,
        "workers": True,
    }
elif case == "skipped-marker-promotion":
    document["phase"] = "MARKER_PROMOTED"
    document["promotion"] = {
        "control": False,
        "marker": False,
        "workers": True,
    }
elif case == "rollback-promotion-contradiction":
    document["phase"] = "ROLLBACK_PREPARING"
    document["promotion"] = {
        "control": False,
        "marker": False,
        "workers": True,
    }
elif case == "duplicate-logical-retirement":
    document["phase"] = "FORWARD_APPLYING"
    identity = {
        "docker_id": "1" * 64,
        "generation": "901",
        "kind": "secret",
        "name": "vp-worker-candidate",
        "purpose": "database",
        "service": "vp-ffmpeg-worker-go-swarm",
        "spec_digest": None,
    }
    replacement = identity.copy()
    replacement["docker_id"] = "2" * 64
    document["pending_retirements"] = [
        {
            "identity": identity,
            "retirement_id": "retirement-" + "1" * 32,
        },
        {
            "identity": replacement,
            "retirement_id": "retirement-" + "2" * 32,
        },
    ]
else:
    raise SystemExit("unknown relational schema case")
pathlib.Path(sys.argv[2]).write_text(
    json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
    chmod 0600 "$strict_root/transactions/active.json"
    if transaction_cli replay-plan "$strict_root" >/dev/null 2>&1; then
      printf 'RED: relational schema accepted %s\n' "$relational_case" >&2
      relational_failures=$((relational_failures + 1))
    fi
  done
  if [[ "$relational_failures" -ne 0 ]]; then
    echo "FAIL: relational schema accepted $relational_failures contradictions" >&2
    exit 1
  fi
)

(
  transaction_helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  transaction_root="$TEST_ROOT/abort-core/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  transaction_cli() {
    python3 "$transaction_helper" "$@"
  }

  lock_path="$(transaction_cli lock-prepare "$transaction_root")"
  exec 18<>"$lock_path"
  transaction_cli lock-acquire "$transaction_root" 18 >/dev/null
  credentials=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$TEST_ROOT/abort-core/credential-$index"
    printf 'postgresql://abort-%s:credential@database/videoprocess\n' "$index" \
      >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    transaction_cli validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"
  commit=1123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend:deploy-${commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${commit:0:12}"
  control_image="vp-ffmpeg-worker-python:deploy-${commit:0:12}"
  control_generation=c-${commit:0:20}
  operator_reference="control/$control_generation/worker-registration-operator-database-url"
  transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" "$commit" \
    legacy_no_control <<<"$credential_records" >/dev/null

  control_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  worker_database_id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  worker_admission_id=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  transaction_cli record-authority-intent \
    "$transaction_root" 18 control vp-worker-control \
    "$control_generation" "$control_image" "$control_generation" \
    "$operator_reference" >/dev/null
  transaction_cli mark-authority-provisioning \
    "$transaction_root" 18 control vp-worker-control \
    "$control_generation" >/dev/null
  transaction_cli mark-authority-provisioned \
    "$transaction_root" 18 control vp-worker-control \
    "$control_generation" >/dev/null
  transaction_cli record-authority-intent \
    "$transaction_root" 18 runtime vp-ffmpeg-worker-go-swarm 901 \
    "$control_image" "$control_generation" "$operator_reference" >/dev/null
  transaction_cli mark-authority-provisioning \
    "$transaction_root" 18 runtime vp-ffmpeg-worker-go-swarm 901 \
    >/dev/null
  transaction_cli mark-authority-provisioned \
    "$transaction_root" 18 runtime vp-ffmpeg-worker-go-swarm 901 \
    >/dev/null
  transaction_cli record-prepared-secret \
    "$transaction_root" 18 \
    vp-wc-operator-c-1123456789abcdef0123 "$control_id" \
    vp-worker-control c-1123456789abcdef0123 operator >/dev/null
  transaction_cli record-prepared-secret \
    "$transaction_root" 18 \
    vp-wr-ffmpeg-go-db-901 "$worker_database_id" \
    vp-ffmpeg-worker-go-swarm 901 database >/dev/null
  transaction_cli record-prepared-secret \
    "$transaction_root" 18 \
    vp-wr-ffmpeg-go-admission-901 "$worker_admission_id" \
    vp-ffmpeg-worker-go-swarm 901 admission >/dev/null

  transaction_cli begin-abort \
    "$transaction_root" 18 9 preparing_failed >/dev/null
  abort_state="$TEST_ROOT/abort-core/list.json"
  transaction_cli list-abort "$transaction_root" 18 >"$abort_state"
  python3 - \
    "$abort_state" "$control_id" "$worker_database_id" "$worker_admission_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    state = json.load(handle)
if (
    state["phase"] != "ABORTING"
    or state["revision"] != 10
    or state["operation"] is not None
    or [
        item["docker_secret_id"]
        for item in state["prepared_secrets"]
    ] != [sys.argv[4], sys.argv[3], sys.argv[2]]
    or state["authorities"] != [
        {
            "control_generation": "c-1123456789abcdef0123",
            "control_image": "vp-ffmpeg-worker-python:deploy-1123456789ab",
            "generation": "901",
            "kind": "runtime",
            "operator_reference": "control/c-1123456789abcdef0123/worker-registration-operator-database-url",
            "service": "vp-ffmpeg-worker-go-swarm",
            "state": "provisioned",
        },
        {
            "control_generation": "c-1123456789abcdef0123",
            "control_image": "vp-ffmpeg-worker-python:deploy-1123456789ab",
            "generation": "c-1123456789abcdef0123",
            "kind": "control",
            "operator_reference": "control/c-1123456789abcdef0123/worker-registration-operator-database-url",
            "service": "vp-worker-control",
            "state": "provisioned",
        },
    ]
):
    raise SystemExit("abort state is not deterministic")
PY
  if transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" replacement \
    legacy_no_control <<<"$credential_records" >/dev/null 2>&1; then
    echo 'FAIL: ABORTING transaction allowed a new candidate' >&2
    exit 1
  fi

  intent="$(
    transaction_cli intent-prepared-secret-removal \
      "$transaction_root" 18 10 "$worker_admission_id"
  )"
  operation_id="$(
    python3 -c \
      'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
      <<<"$intent"
  )"
  transaction_cli replay-plan "$transaction_root" \
    >"$TEST_ROOT/abort-core/replay.json"
  python3 - "$TEST_ROOT/abort-core/replay.json" "$worker_admission_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    plan = json.load(handle)
if (
    plan["phase"] != "ABORTING"
    or plan["next_action"] != "VERIFY_REMOVE_PREPARED_SECRET"
    or plan["pending_operation"]["identity"]["docker_id"] != sys.argv[2]
    or plan["allow_new_candidate"]
    or plan["allow_stale_cleanup"]
):
    raise SystemExit("prepared-secret removal intent is not replayable")
PY
  before_wrong_complete="$(shasum -a 256 \
    "$transaction_root/transactions/active.json")"
  if transaction_cli complete-prepared-secret-removal \
    "$transaction_root" 18 11 operation-00000000000000000000000000000000 \
    >/dev/null 2>&1; then
    echo 'FAIL: abort core accepted the wrong operation identity' >&2
    exit 1
  fi
  [[ "$(shasum -a 256 "$transaction_root/transactions/active.json")" \
    == "$before_wrong_complete" ]]
  transaction_cli complete-prepared-secret-removal \
    "$transaction_root" 18 11 "$operation_id" >/dev/null

  revision=12
  for secret_id in "$worker_database_id" "$control_id"; do
    intent="$(
      transaction_cli intent-prepared-secret-removal \
        "$transaction_root" 18 "$revision" "$secret_id"
    )"
    operation_id="$(
      python3 -c \
        'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
        <<<"$intent"
    )"
    revision=$((revision + 1))
    transaction_cli complete-prepared-secret-removal \
      "$transaction_root" 18 "$revision" "$operation_id" >/dev/null
    revision=$((revision + 1))
  done

  transaction_cli complete-abort-authority \
    "$transaction_root" 18 16 runtime \
    vp-ffmpeg-worker-go-swarm 901 >/dev/null
  transaction_cli complete-abort-authority \
    "$transaction_root" 18 17 control \
    vp-worker-control c-1123456789abcdef0123 >/dev/null
  transaction_cli finish-abort "$transaction_root" 18 18 >/dev/null
  if transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" replacement \
    legacy_no_control <<<"$credential_records" >/dev/null 2>&1; then
    echo 'FAIL: unarchived DONE transaction allowed a new candidate' >&2
    exit 1
  fi
  done_path="$(transaction_cli archive "$transaction_root" 18 19)"
  [[ -f "$done_path" && ! -e "$transaction_root/transactions/active.json" ]]
  python3 - "$done_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
if (
    document["phase"] != "DONE"
    or document["outcome"] != "aborted"
    or document["prepared_secrets"]
    or document["operation"] is not None
    or any(
        authority["state"] != "revoked"
        for authority in document["authorities"]
    )
    or document["abort"] != {
        "authorities": [],
        "reason": "preparing_failed",
    }
):
    raise SystemExit("aborted DONE evidence is incomplete")
PY
  transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" replacement \
    legacy_no_control <<<"$credential_records" >/dev/null
  exec 18>&-
)

(
  transaction_helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  transaction_root="$TEST_ROOT/authority-wal/state/vp-worker-admission"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  transaction_cli() {
    python3 "$transaction_helper" "$@"
  }

  lock_path="$(transaction_cli lock-prepare "$transaction_root")"
  exec 18<>"$lock_path"
  transaction_cli lock-acquire "$transaction_root" 18 >/dev/null
  credentials=()
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$TEST_ROOT/authority-wal/credential-$index"
    printf 'postgresql://authority-%s:credential@database/videoprocess\n' \
      "$index" >"$credential"
    chmod 0400 "$credential"
    credentials+=("$credential")
  done
  credential_records="$(
    transaction_cli validate-credentials \
      "${credentials[0]}" "${principals[0]}" \
      "${credentials[1]}" "${principals[1]}" \
      "${credentials[2]}" "${principals[2]}" \
      "${credentials[3]}" "${principals[3]}"
  )"
  commit=4123456789abcdef0123456789abcdef01234567
  control_generation=c-${commit:0:20}
  control_image="vp-ffmpeg-worker-python:deploy-${commit:0:12}"
  operator_reference="control/$control_generation/worker-registration-operator-database-url"
  transaction_cli begin \
    "$transaction_root" 18 \
    "$commit" "vp-backend:deploy-${commit:0:12}" \
    "vp-ffmpeg-worker-go:deploy-${commit:0:12}" "$commit" \
    legacy_no_control <<<"$credential_records" >/dev/null
  before_wrong_control_image="$(
    shasum -a 256 "$transaction_root/transactions/active.json"
  )"
  if transaction_cli record-authority-intent \
    "$transaction_root" 18 runtime vp-ffmpeg-worker-go-swarm 999 \
    "alternate-control:deploy-${commit:0:12}" \
    "$control_generation" "$operator_reference" >/dev/null 2>&1; then
    echo 'FAIL: authority WAL accepted an alternate control image' >&2
    exit 1
  fi
  [[ "$(shasum -a 256 "$transaction_root/transactions/active.json")" \
    == "$before_wrong_control_image" ]]

  authority_services=(
    vp-worker-control
    vp-ffmpeg-worker-go-swarm
    vp-ffmpeg-worker-gpu-swarm
    vp-vision-worker-swarm
    vp-youtube-publisher-swarm
  )
  authority_generations=(
    "$control_generation"
    921
    922
    923
    924
  )
  for index in 0 1 2 3 4; do
    kind=runtime
    [[ "$index" -eq 0 ]] && kind=control
    transaction_cli record-authority-intent \
      "$transaction_root" 18 \
      "$kind" "${authority_services[$index]}" \
      "${authority_generations[$index]}" \
      "$control_image" "$control_generation" "$operator_reference" \
      >/dev/null
  done
  transaction_cli mark-authority-provisioning \
    "$transaction_root" 18 runtime \
    "${authority_services[1]}" "${authority_generations[1]}" >/dev/null
  transaction_cli mark-authority-provisioning \
    "$transaction_root" 18 runtime \
    "${authority_services[2]}" "${authority_generations[2]}" >/dev/null
  transaction_cli mark-authority-provisioned \
    "$transaction_root" 18 runtime \
    "${authority_services[2]}" "${authority_generations[2]}" >/dev/null

  transaction_cli begin-abort \
    "$transaction_root" 18 8 preparing_failed >/dev/null
  transaction_cli list-abort "$transaction_root" 18 \
    >"$TEST_ROOT/authority-wal/abort.json"
  python3 - "$TEST_ROOT/authority-wal/abort.json" \
    "$control_image" "$control_generation" "$operator_reference" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    state = json.load(handle)
authorities = state["authorities"]
if (
    state["phase"] != "ABORTING"
    or state["prepared_secrets"]
    or len(authorities) != 5
    or {item["state"] for item in authorities}
    != {"planned", "provisioning", "provisioned"}
    or any(item["control_image"] != sys.argv[2] for item in authorities)
    or any(item["control_generation"] != sys.argv[3] for item in authorities)
    or any(item["operator_reference"] != sys.argv[4] for item in authorities)
):
    raise SystemExit("authority WAL did not preserve zero-secret intents")
PY
  if transaction_cli finish-abort "$transaction_root" 18 9 \
    >/dev/null 2>&1; then
    echo 'FAIL: authority WAL archived without revoke evidence' >&2
    exit 1
  fi

  revision=9
  for index in 4 3 2 1 0; do
    kind=runtime
    [[ "$index" -eq 0 ]] && kind=control
    transaction_cli complete-abort-authority \
      "$transaction_root" 18 "$revision" \
      "$kind" "${authority_services[$index]}" \
      "${authority_generations[$index]}" >/dev/null
    revision=$((revision + 1))
  done
  transaction_cli finish-abort \
    "$transaction_root" 18 "$revision" >/dev/null
  revision=$((revision + 1))
  done_path="$(
    transaction_cli archive "$transaction_root" 18 "$revision"
  )"
  python3 - "$done_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
if (
    document["phase"] != "DONE"
    or document["outcome"] != "aborted"
    or len(document["authorities"]) != 5
    or any(item["state"] != "revoked" for item in document["authorities"])
    or document["abort"]["authorities"]
):
    raise SystemExit("authority WAL lost completion evidence")
PY
  exec 18>&-
)

CALLS="$TEST_ROOT/calls"
GENERATION_SEQUENCE="$TEST_ROOT/generation-sequence"
printf '700\n' >"$GENERATION_SEQUENCE"
: >"$CALLS"

VP_WORKER_ADMISSION_PREPARED=true
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa
VP_WORKER_CONTROL_GENERATION=c-aaaaaaaaaaaaaaaaaaaa

old_commit=1111111111111111111111111111111111111111
old_short="${old_commit:0:12}"
new_commit=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
VP_WORKER_CONTROL_PRIOR_GENERATION=c-${old_commit:0:20}
VP_WORKER_CONTROL_PRIOR_IMAGE=vp-ffmpeg-worker-python:deploy-$old_short
VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=prior-operator-secret
VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=prior-orchestrator-secret
VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=prior-staging-secret
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=prior-staging-minio-access
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=prior-staging-minio-secret
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=prior-worker-minio-access
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=prior-worker-minio-secret
snapshots="$(
  printf '%s|%s|%s|%s\n' \
    vp-ffmpeg-worker-go-swarm \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    "vp-ffmpeg-worker-go:deploy-$old_short" \
    1111111111111111111111111111111111111111111111111111111111111111 \
    "$VP_PYTHON_WORKER_SERVICE" \
    bbbbbbbbbbbbbbbbbbbbbbbb \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    2222222222222222222222222222222222222222222222222222222222222222 \
    "$VP_VISION_WORKER_SERVICE" \
    cccccccccccccccccccccccc \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    3333333333333333333333333333333333333333333333333333333333333333 \
    "$VP_PUBLISHER_SERVICE" \
    dddddddddddddddddddddddd \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    4444444444444444444444444444444444444444444444444444444444444444
)"
attempted_services="vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE"
test_secret_id() {
  printf '%064x\n' "$1"
}

for service in $attempted_services; do
  kind="$(vp_worker_admission_kind "$service")"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image="vp-ffmpeg-worker-go:deploy-$old_short"
      old_generation=101
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=102
      ;;
    "$VP_VISION_WORKER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=103
      ;;
    "$VP_PUBLISHER_SERVICE")
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      old_generation=104
      ;;
  esac
  vp_worker_admission_write_manifest \
    "$ROOT/state/vp-worker-admission/current/$kind.conf" \
    "$service" "$old_commit" "$image" "$old_generation" \
    "old-$kind-db-$old_generation" \
    "old-$kind-admission-$old_generation" \
    "$(test_secret_id "$old_generation")" \
    "$(test_secret_id "$((old_generation + 1000))")"
done

VP_WORKER_ADMISSION_COMMIT="$new_commit"
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$new_commit"
VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$attempted_services"
failed_forward_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
failed_records="$(
  printf '%s|%s|%s|%s|%s|%s\n' \
    vp-ffmpeg-worker-go-swarm 201 failed-go-db-201 "$(test_secret_id 201)" failed-go-admission-201 "$(test_secret_id 1201)" \
    "$VP_PYTHON_WORKER_SERVICE" 202 failed-ffmpeg-db-202 "$(test_secret_id 202)" failed-ffmpeg-admission-202 "$(test_secret_id 1202)" \
    "$VP_VISION_WORKER_SERVICE" 203 failed-vision-db-203 "$(test_secret_id 203)" failed-vision-admission-203 "$(test_secret_id 1203)" \
    "$VP_PUBLISHER_SERVICE" 204 failed-publisher-db-204 "$(test_secret_id 204)" failed-publisher-admission-204 "$(test_secret_id 1204)"
)"
while IFS='|' read -r \
  service generation database_secret database_secret_id \
  admission_secret admission_secret_id; do
  kind="$(vp_worker_admission_kind "$service")"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image=vp-ffmpeg-worker-go:deploy-${new_commit:0:12}
      ;;
    *)
      image=vp-ffmpeg-worker-python:deploy-${new_commit:0:12}
      ;;
  esac
  vp_worker_admission_write_manifest \
    "$ROOT/state/vp-worker-admission/candidates/$failed_forward_namespace/$kind.conf" \
    "$service" "$new_commit" "$image" "$generation" \
    "$database_secret" "$admission_secret" \
    "$database_secret_id" "$admission_secret_id"
done <<<"$failed_records"

admission_root="$ROOT/state/vp-worker-admission"
chmod 0700 "$admission_root"
vp_worker_admission_lock_acquire "$admission_root"
rollback_credentials=()
rollback_principals=(
  vp_deploy_migrator
  vp_deploy_read
  vp_control_role_owner
  vp_runtime_role_owner
)
for index in 0 1 2 3; do
  credential="$TEST_ROOT/rollback-credential-$index"
  printf 'postgresql://rollback-%s:credential@database/videoprocess\n' \
    "$index" >"$credential"
  chmod 0400 "$credential"
  rollback_credentials+=("$credential")
done
rollback_credential_records="$(
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" validate-credentials \
    "${rollback_credentials[0]}" "${rollback_principals[0]}" \
    "${rollback_credentials[1]}" "${rollback_principals[1]}" \
    "${rollback_credentials[2]}" "${rollback_principals[2]}" \
    "${rollback_credentials[3]}" "${rollback_principals[3]}"
)"
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" begin \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
  "$new_commit" "vp-backend:deploy-${new_commit:0:12}" \
  "vp-ffmpeg-worker-go:deploy-${new_commit:0:12}" \
  "$new_commit" legacy_no_control \
  <<<"$rollback_credential_records" >/dev/null
rollback_baseline="$TEST_ROOT/rollback-baseline.json"
python3 - "$rollback_baseline" <<'PY'
import json
import pathlib
import sys

services = [
    "vp-api-swarm",
    "vp-frontend-swarm",
    "vp-autoflow-api-swarm",
    "vp-event-outbox-relay-swarm",
    "vp-channel-agent-runner-swarm",
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
]
payload = {
    "control": None,
    "kind": "legacy_no_control",
    "services": [
        {
            "docker_service_id": None,
            "existed": False,
            "image": None,
            "name": service,
            "spec_digest": None,
        }
        for service in services
    ],
}
pathlib.Path(sys.argv[1]).write_text(
    json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" capture-baseline \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" 0 \
  <"$rollback_baseline" >/dev/null
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" transition \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
  1 FORWARD_APPLYING >/dev/null
printf '%s\n' '{"control":null,"services":[]}' \
  | python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    capture-failed-forward \
    "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" 2 >/dev/null

vp_worker_admission_new_generation() {
  local value
  value="$(<"$GENERATION_SEQUENCE")"
  printf '%s\n' "$((value + 1))" >"$GENERATION_SEQUENCE"
  printf '%s\n' "$value"
}

vp_worker_admission_image_commit() {
  [[ "$1" == "vp-ffmpeg-worker-python:deploy-$old_short" ]]
  printf '%s\n' "$old_commit"
}

vp_worker_admission_record_control_selection() {
  printf 'control-selection|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_prepare_worker_redis_marker_rollback_candidate() {
  printf 'marker-selection|rollback\n' >>"$CALLS"
}

vp_worker_admission_prepare_service() {
  local service="$1"
  local image="$2"
  local _control_image="$3"
  local commit="$4"
  local root="$5"
  local namespace="$6"
  local kind
  kind="$(vp_worker_admission_kind "$service")"
  local candidate="$root/candidates/$namespace/$kind.conf"
  local generation
  local database_secret
  local admission_secret
  if vp_worker_admission_read_manifest "$candidate" "$service"; then
    [[ "$VP_WORKER_MANIFEST_COMMIT" == "$commit" \
      && "$VP_WORKER_MANIFEST_IMAGE" == "$image" ]]
    generation="$VP_WORKER_MANIFEST_GENERATION"
    database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
    admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
  else
    [[ ! -e "$candidate" ]]
    generation="$(vp_worker_admission_new_generation)"
    database_secret="fresh-$kind-db-$generation"
    admission_secret="fresh-$kind-admission-$generation"
    vp_worker_admission_write_manifest \
      "$candidate" \
      "$service" "$commit" "$image" "$generation" \
      "$database_secret" "$admission_secret" \
      "$(test_secret_id "$generation")" \
      "$(test_secret_id "$((generation + 1000))")"
  fi
  vp_worker_admission_set_candidate \
    "$service" "$generation" "$database_secret" "$admission_secret"
  printf 'prepare|%s|%s|%s|%s|%s\n' \
    "$service" "$commit" "$generation" "$namespace" \
    "$_control_image" >>"$CALLS"
}

vp_require_worker_redis_marker_status() {
  printf 'marker|status\n' >>"$CALLS"
}

vp_activate_worker_admission() {
  local service="$1"
  local contract
  contract="$(vp_worker_service_contract "$service")"
  printf 'activate|%s|%s|%s\n' \
    "$service" "$(cut -d'|' -f6 <<<"$contract")" \
    "$VP_WORKER_ADMISSION_COMMIT" >>"$CALLS"
}

vp_update_runtime_service() {
  printf 'restore|%s|%s\n' "$1" "$2" >>"$CALLS"
}

vp_deploy_python_worker() {
  printf 'restore|%s|%s|%s\n' \
    "$VP_PYTHON_WORKER_SERVICE" "$1" "${2:-}" >>"$CALLS"
}

vp_deploy_vision_worker() {
  printf 'restore|%s|%s|%s\n' \
    "$VP_VISION_WORKER_SERVICE" "$1" "${2:-}" >>"$CALLS"
}

vp_deploy_publisher() {
  printf 'restore|%s|%s|%s\n' \
    "$VP_PUBLISHER_SERVICE" "$1" "${2:-}" >>"$CALLS"
}

vp_registered_worker_service_current_id() {
  case "$1" in
    vp-ffmpeg-worker-go-swarm)
      printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaa
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      printf '%s\n' bbbbbbbbbbbbbbbbbbbbbbbb
      ;;
    "$VP_VISION_WORKER_SERVICE")
      printf '%s\n' cccccccccccccccccccccccc
      ;;
    "$VP_PUBLISHER_SERVICE")
      printf '%s\n' dddddddddddddddddddddddd
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_live_worker_identity() {
  local service="$1"
  local _image="$2"
  local service_id
  local digest
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      service_id=aaaaaaaaaaaaaaaaaaaaaaaa
      digest=1111111111111111111111111111111111111111111111111111111111111111
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      service_id=bbbbbbbbbbbbbbbbbbbbbbbb
      digest=2222222222222222222222222222222222222222222222222222222222222222
      ;;
    "$VP_VISION_WORKER_SERVICE")
      service_id=cccccccccccccccccccccccc
      digest=3333333333333333333333333333333333333333333333333333333333333333
      ;;
    "$VP_PUBLISHER_SERVICE")
      service_id=dddddddddddddddddddddddd
      digest=4444444444444444444444444444444444444444444444444444444444444444
      ;;
    *) return 1 ;;
  esac
  printf '%s|%s\n' "$service_id" "$digest"
}

vp_install_staging_object_janitor() {
  printf 'janitor|install|%s\n' "$1" >>"$CALLS"
}

vp_run_staging_object_janitor_once() {
  printf 'janitor|ready\n' >>"$CALLS"
}

vp_worker_admission_clear_janitor_service() {
  printf 'janitor|clear\n' >>"$CALLS"
}

vp_worker_admission_record_janitor_service() {
  printf 'janitor|record\n' >>"$CALLS"
}

FAIL_READY_SERVICE="$VP_VISION_WORKER_SERVICE"
vp_require_worker_deployment_ready() {
  local service="$1"
  local contract
  contract="$(vp_worker_service_contract "$service")"
  printf 'ready|%s|%s\n' \
    "$service" "$(cut -d'|' -f6 <<<"$contract")" >>"$CALLS"
  [[ "$service" != "$FAIL_READY_SERVICE" ]]
}

vp_worker_admission_retire_generation() {
  printf 'retire|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$CALLS"
}

vp_worker_admission_promote_phase() {
  case "$1" in
    PROMOTE_ROLLBACK_WORKERS)
      vp_commit_worker_admission
      vp_worker_admission_transition_to ROLLBACK_WORKERS_PROMOTED
      ;;
    PROMOTE_ROLLBACK_MARKER)
      vp_worker_admission_transition_to ROLLBACK_MARKER_PROMOTED
      ;;
    PROMOTE_ROLLBACK_CONTROL)
      vp_worker_admission_transition_to ROLLBACK_CONTROL_PROMOTED
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_retire_transaction() {
  return 0
}

if vp_restore_worker_admission_transaction \
  "$snapshots" "$attempted_services" "$failed_records"; then
  echo 'FAIL: rollback readiness failure unexpectedly committed' >&2
  exit 1
fi
first_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
if grep -Fq 'retire|' "$CALLS"; then
  echo 'FAIL: failed rollback retired a generation before convergence' >&2
  exit 1
fi
if [[ ! -d "$ROOT/state/vp-worker-admission/candidates/$first_namespace" ]]; then
  echo 'FAIL: failed rollback did not preserve its managed candidate state' >&2
  exit 1
fi
rollback_active="$admission_root/transactions/active.json"
rollback_transaction_id="$(
  python3 - "$rollback_active" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
stages = {
    worker["service"]: worker["applied_stage"]
    for worker in document["rollback"]["workers"]
}
expected = {
    "vp-ffmpeg-worker-go-swarm": "verified",
    "vp-ffmpeg-worker-gpu-swarm": "verified",
    "vp-vision-worker-swarm": "applied",
    "vp-youtube-publisher-swarm": "verified",
}
if (
    document["phase"] != "CANDIDATE_RESTORE_REQUIRED"
    or stages != expected
    or any(
        worker["docker_service_id"] is None
        or worker["target_spec_digest"] is None
        for worker in document["rollback"]["workers"]
    )
):
    raise SystemExit("failed rollback worker stages were not durable")
print(document["transaction_id"])
PY
)" || exit 1

vp_worker_admission_load_replay_plan
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" transition \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
  "$VP_WORKER_ADMISSION_REPLAY_REVISION" CANDIDATE_RESTORING >/dev/null
vp_worker_admission_load_replay_plan
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" transition \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
  "$VP_WORKER_ADMISSION_REPLAY_REVISION" CANDIDATE_RESTORED >/dev/null
vp_worker_admission_load_replay_plan
python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" transition \
  "$admission_root" "$VP_WORKER_ADMISSION_LOCK_FD" \
  "$VP_WORKER_ADMISSION_REPLAY_REVISION" ROLLBACK_PREPARING >/dev/null

: >"$CALLS"
FAIL_READY_SERVICE=
vp_restore_worker_admission_transaction \
  "$snapshots" "$attempted_services" "$failed_records"
second_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
if [[ -z "$first_namespace" || -z "$second_namespace" \
  || "$first_namespace" != "$second_namespace" ]]; then
  echo 'FAIL: rollback replay minted a second credential namespace' >&2
  exit 1
fi
if [[ -e "$ROOT/state/vp-worker-admission/candidates/$first_namespace" ]]; then
  echo 'FAIL: converged rollback retained a superseded rollback namespace' >&2
  exit 1
fi
if [[ -e "$ROOT/state/vp-worker-admission/candidates/$failed_forward_namespace" ]]; then
  echo 'FAIL: converged rollback retained the failed forward namespace' >&2
  exit 1
fi
rollback_done="$admission_root/transactions/$rollback_transaction_id/done.json"
python3 - "$rollback_done" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
workers = document["rollback"]["workers"]
if (
    document["phase"] != "DONE"
    or document["outcome"] != "rolled_back"
    or len(workers) != 4
    or any(worker["applied_stage"] != "verified" for worker in workers)
):
    raise SystemExit("converged rollback worker stages were not archived")
PY

assert_order() {
  local first="$1"
  local second="$2"
  local first_line
  local second_line
  first_line="$(grep -nF "$first" "$CALLS" | head -1 | cut -d: -f1)"
  second_line="$(grep -nF "$second" "$CALLS" | head -1 | cut -d: -f1)"
  if [[ -z "$first_line" || -z "$second_line" \
    || "$first_line" -ge "$second_line" ]]; then
    echo "FAIL: rollback order is invalid: $first -> $second" >&2
    exit 1
  fi
}

for service in $attempted_services; do
  kind="$(vp_worker_admission_kind "$service")"
  current="$ROOT/state/vp-worker-admission/current/$kind.conf"
  vp_worker_admission_read_manifest "$current" "$service"
  if [[ "$VP_WORKER_MANIFEST_COMMIT" != "$old_commit" \
    || "$VP_WORKER_MANIFEST_GENERATION" =~ ^10[1-4]$ \
    || "$VP_WORKER_MANIFEST_GENERATION" =~ ^20[1-4]$ \
    || "$VP_WORKER_MANIFEST_DATABASE_SECRET" != fresh-* \
    || "$VP_WORKER_MANIFEST_ADMISSION_SECRET" != fresh-* ]]; then
    echo "FAIL: rollback did not commit fresh prior-image state: $service" >&2
    exit 1
  fi
  generation="$VP_WORKER_MANIFEST_GENERATION"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      image="vp-ffmpeg-worker-go:deploy-$old_short"
      ;;
    *)
      image="vp-ffmpeg-worker-python:deploy-$old_short"
      ;;
  esac
  assert_order "activate|$service|$generation|$old_commit" \
    "restore|$service|$image"
  assert_order "restore|$service|$image" "ready|$service|$generation"
  grep -Eq \
    "^prepare\\|$service\\|$old_commit\\|$generation\\|rollback-[^|]+\\|vp-ffmpeg-worker-python:deploy-$old_short$" \
    "$CALLS"
done
if ! grep -Fxq \
    "restore|$VP_PYTHON_WORKER_SERVICE|vp-ffmpeg-worker-python:deploy-$old_short|bbbbbbbbbbbbbbbbbbbbbbbb" \
    "$CALLS" \
  || ! grep -Fxq \
    "restore|$VP_VISION_WORKER_SERVICE|vp-ffmpeg-worker-python:deploy-$old_short|cccccccccccccccccccccccc" \
    "$CALLS" \
  || ! grep -Fxq \
    "restore|$VP_PUBLISHER_SERVICE|vp-ffmpeg-worker-python:deploy-$old_short|dddddddddddddddddddddddd" \
    "$CALLS"; then
  echo 'FAIL: rollback restore did not carry baseline service IDs to worker helpers' >&2
  exit 1
fi
rollback_control_manifest="$ROOT/state/vp-worker-admission/control-current.conf"
assert_order \
  "control-selection|rollback|$rollback_control_manifest" \
  'prepare|vp-ffmpeg-worker-go-swarm|'

for generation in 201 202 203 204; do
  grep -Eq "^retire\\|[^|]+\\|$generation\\|" "$CALLS"
done
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: rollback referenced host 126' >&2
  exit 1
fi
vp_worker_admission_lock_release

: >"$CALLS"
VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=c-aaaaaaaaaaaaaaaaaaaa
VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa
VP_WORKER_CONTROL_GENERATION=c-11111111111111111111
VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
vp_require_pipeline_network_identity() {
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
}
vp_require_worker_service_descriptor() {
  printf 'descriptor|%s\n' "$1" >>"$CALLS"
}
vp_require_staging_object_janitor_control() {
  printf 'janitor|converged\n' >>"$CALLS"
}
vp_worker_control_schedule_retirement() {
  printf 'control|journal|%s|%s\n' "$2" "$3" >>"$CALLS"
}
vp_worker_control_write_manifest() {
  printf 'control|current|%s|%s\n' "$2" "$3" >>"$CALLS"
}
vp_worker_control_process_retirements() {
  printf 'control|process|%s\n' "$2" >>"$CALLS"
}
vp_finalize_worker_control_rollback
assert_order 'janitor|converged' \
  'control|journal|vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa|c-aaaaaaaaaaaaaaaaaaaa'
assert_order \
  'control|journal|vp-ffmpeg-worker-python:deploy-aaaaaaaaaaaa|c-aaaaaaaaaaaaaaaaaaaa' \
  'control|current|c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111'
assert_order \
  'control|current|c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111' \
  'control|process|c-11111111111111111111'
[[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" ]]
[[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" ]]

(
  legacy_calls="$TEST_ROOT/legacy-first-deploy-calls"
  : >"$legacy_calls"
  legacy_control_current="$TEST_ROOT/legacy-first-deploy-control-current.conf"
  rm -f "$legacy_control_current"
  legacy_baseline="$({
    printf '%s|true|%s|%s|%s\n' \
      vp-ffmpeg-worker-go-swarm \
      aaaaaaaaaaaaaaaaaaaaaaaa \
      vp-ffmpeg-worker-go:legacy-baseline \
      0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    printf '%s|false|-|-|-\n' \
      "$VP_VISION_WORKER_SERVICE"
  })"
  legacy_candidates="$({
    printf '%s|201|%s\n' \
      vp-ffmpeg-worker-go-swarm bbbbbbbbbbbbbbbbbbbbbbbb
    printf '%s|203|%s\n' \
      "$VP_VISION_WORKER_SERVICE" cccccccccccccccccccccccc
  })"
  legacy_candidate_secrets="$({
    printf '%s|201|go-database|%s|go-admission|%s\n' \
      vp-ffmpeg-worker-go-swarm \
      dddddddddddddddddddddddd eeeeeeeeeeeeeeeeeeeeeeee
    printf '%s|203|vision-database|%s|vision-admission|%s\n' \
      "$VP_VISION_WORKER_SERVICE" \
      ffffffffffffffffffffffff 111111111111111111111111
  })"
  FAIL_LEGACY_RESTORE=true
  vp_restore_legacy_worker_service() {
    printf 'restore|%s|%s|%s|%s\n' "$@" >>"$legacy_calls"
    [[ "$FAIL_LEGACY_RESTORE" != true ]]
  }
  vp_remove_legacy_candidate_worker() {
    printf 'rm-id|%s|%s|%s\n' "$@" >>"$legacy_calls"
  }
  vp_require_legacy_worker_service_ready() {
    printf 'legacy-ready|%s|%s\n' "$@" >>"$legacy_calls"
  }
  vp_require_legacy_worker_service_absent() {
    printf 'legacy-absent|%s\n' "$1" >>"$legacy_calls"
  }
  vp_retire_legacy_forward_candidate() {
    printf 'retire-candidate|%s\n' "$1" >>"$legacy_calls"
  }
  vp_worker_admission_new_generation() {
    printf 'minted-generation\n' >>"$legacy_calls"
    return 1
  }

  if vp_restore_legacy_worker_admission_baseline \
    "$legacy_baseline" "$legacy_candidates" \
    "$legacy_candidate_secrets" "$legacy_control_current"; then
    echo 'FAIL: injected first-deploy baseline restore failure converged' >&2
    exit 1
  fi
  if grep -Eq '^(rm-id|retire-candidate|minted-generation)' "$legacy_calls"; then
    echo 'FAIL: failed first-deploy restore retired evidence or minted authority' >&2
    exit 1
  fi
  [[ ! -e "$legacy_control_current" ]]

  : >"$legacy_calls"
  FAIL_LEGACY_RESTORE=false
  vp_restore_legacy_worker_admission_baseline \
    "$legacy_baseline" "$legacy_candidates" \
    "$legacy_candidate_secrets" "$legacy_control_current"
  grep -Fqx \
    'restore|vp-ffmpeg-worker-go-swarm|aaaaaaaaaaaaaaaaaaaaaaaa|vp-ffmpeg-worker-go:legacy-baseline|0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' \
    "$legacy_calls"
  grep -Fqx \
    "rm-id|$VP_VISION_WORKER_SERVICE|cccccccccccccccccccccccc|203" \
    "$legacy_calls"
  restore_line="$(grep -n '^restore|' "$legacy_calls" | cut -d: -f1)"
  remove_line="$(grep -n '^rm-id|' "$legacy_calls" | cut -d: -f1)"
  retire_line="$(grep -n '^retire-candidate|' "$legacy_calls" | head -1 | cut -d: -f1)"
  if [[ -z "$restore_line" || -z "$remove_line" || -z "$retire_line" \
    || "$restore_line" -ge "$retire_line" \
    || "$remove_line" -ge "$retire_line" ]]; then
    echo 'FAIL: first-deploy candidate authority retired before baseline fencing' >&2
    exit 1
  fi
  if [[ "$(sed -n "${retire_line},$((retire_line + 1))p" "$legacy_calls")" \
    != "retire-candidate|$legacy_candidate_secrets" ]]; then
    echo 'FAIL: first-deploy retirement did not use immutable secret records' >&2
    exit 1
  fi
  if grep -Fqx minted-generation "$legacy_calls"; then
    echo 'FAIL: first-deploy rollback minted a fake managed generation' >&2
    exit 1
  fi
  [[ ! -e "$legacy_control_current" ]]
)

(
  marker_selection_root="$TEST_ROOT/marker-selection"
  ROOT="$marker_selection_root/sync"
  VP_PIPELINE_NETWORK=vp-pipeline-net
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  mkdir -p "$ROOT/state/worker-redis-marker-control/status" \
    "$ROOT/state/worker-redis-marker-control/locks" "$ROOT/bin" "$ROOT/logs"
  marker_control_root="$ROOT/state/worker-redis-marker-control"
  marker_generation=m-222222222222-1700000000-0001
  marker_image=vp-ffmpeg-worker-python:deploy-222222222222
  marker_runtime_generation=2222222222222222222222222222222222222222
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$marker_generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$marker_image"
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$marker_runtime_generation"
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=runtime-marker-readiness
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID=111111111111111111111111
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=runtime-marker-janitor
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID=222222222222222222222222
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=333333333333333333333333
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=444444444444444444444444
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=555555555555555555555555
  marker_config="$marker_control_root/control.conf"
  printf '%s\n' \
    "GENERATION=$marker_generation" \
    "IMAGE=$marker_image" \
    "NETWORK=$VP_PIPELINE_NETWORK" \
    "NETWORK_ID=$VP_PIPELINE_NETWORK_ID" \
    "READINESS_DATABASE_SECRET=vp-wrm-readiness-db-$marker_generation" \
    "READINESS_REDIS_SECRET=$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
    "JANITOR_DATABASE_SECRET=vp-wrm-janitor-db-$marker_generation" \
    "JANITOR_REDIS_SECRET=$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" \
    >"$marker_config"
  chmod 0600 "$marker_config"
  marker_cron="$marker_selection_root/crontab"
  marker_launcher="$ROOT/bin/worker-redis-marker-control.sh"
  printf '%s\n' \
    '# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL' \
    "* * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$marker_config VP_WORKER_REDIS_MARKER_STATE_DIR=$marker_control_root/status VP_WORKER_REDIS_MARKER_LOCK_DIR=$marker_control_root/locks $marker_launcher readiness >> $ROOT/logs/worker-redis-marker-readiness.log 2>&1" \
    "*/5 * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$marker_config VP_WORKER_REDIS_MARKER_STATE_DIR=$marker_control_root/status VP_WORKER_REDIS_MARKER_LOCK_DIR=$marker_control_root/locks $marker_launcher janitor >> $ROOT/logs/worker-redis-marker-janitor.log 2>&1" \
    '# END VIDEOPROCESS WORKER REDIS MARKER CONTROL' \
    >"$marker_cron"
  crontab() {
    [[ "$#" -eq 1 && "$1" == -l ]] || return 1
    command cat "$marker_cron"
  }

  durable_marker_state="$marker_control_root/transactions/tx-22222222222222222222222222222222/baseline-managed-state"
  vp_worker_redis_marker_capture_managed_state \
    "$marker_control_root" "$durable_marker_state"
  if [[ "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" != "$durable_marker_state" \
    || ! -f "$durable_marker_state/captured" \
    || ! -f "$durable_marker_state/control.conf" \
    || ! -f "$durable_marker_state/crontab" ]]; then
    echo 'FAIL: marker baseline state was not captured at its durable path' >&2
    exit 1
  fi
  durable_marker_hash_before="$(
    shasum -a 256 "$durable_marker_state/control.conf" \
      "$durable_marker_state/crontab"
  )"
  vp_worker_redis_marker_capture_managed_state \
    "$marker_control_root" "$durable_marker_state"
  durable_marker_hash_after="$(
    shasum -a 256 "$durable_marker_state/control.conf" \
      "$durable_marker_state/crontab"
  )"
  if [[ "$durable_marker_hash_before" != "$durable_marker_hash_after" ]]; then
    echo 'FAIL: marker baseline replay overwrote its durable backup' >&2
    exit 1
  fi

  marker_payload="$(vp_worker_admission_marker_selection_json)"
  python3 - "$marker_config" "$marker_cron" "$marker_payload" <<'PY'
import hashlib
import json
import pathlib
import sys

config_path, cron_path, raw_payload = sys.argv[1:]
payload = json.loads(raw_payload)
expected_refs = {
    ("vp-wrm-readiness-db-m-222222222222-1700000000-0001", "333333333333333333333333", "worker-redis-marker-control", "m-222222222222-1700000000-0001", "readiness-database"),
    ("vp-wrm-janitor-db-m-222222222222-1700000000-0001", "444444444444444444444444", "worker-redis-marker-control", "m-222222222222-1700000000-0001", "janitor-database"),
    ("vp-wrm-repair-db-m-222222222222-1700000000-0001", "555555555555555555555555", "worker-redis-marker-control", "m-222222222222-1700000000-0001", "repair-database"),
    ("runtime-marker-readiness", "111111111111111111111111", "vp-worker-redis-runtime", "2222222222222222222222222222222222222222", "readiness-redis"),
    ("runtime-marker-janitor", "222222222222222222222222", "vp-worker-redis-runtime", "2222222222222222222222222222222222222222", "janitor-redis"),
}
actual_refs = {
    (
        ref["name"], ref["docker_secret_id"], ref["service"],
        ref["generation"], ref["purpose"],
    )
    for ref in payload["secrets"]
}
if (
    payload["generation"] != "m-222222222222-1700000000-0001"
    or payload["image"] != "vp-ffmpeg-worker-python:deploy-222222222222"
    or payload["config_sha256"] != hashlib.sha256(pathlib.Path(config_path).read_bytes()).hexdigest()
    or payload["cron_sha256"] != hashlib.sha256(pathlib.Path(cron_path).read_bytes()).hexdigest()
    or actual_refs != expected_refs
):
    raise SystemExit("marker selection did not preserve immutable control identity")
PY

  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=
  if vp_worker_admission_marker_selection_json >/dev/null 2>&1; then
    echo 'FAIL: marker selection accepted an incomplete secret identity' >&2
    exit 1
  fi

  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=m-rb-222222222222-1
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=666666666666666666666666
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=777777777777777777777777
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=888888888888888888888888
  if vp_worker_admission_marker_selection_json >/dev/null 2>&1; then
    echo 'FAIL: active marker selection accepted a prospective rollback config' >&2
    exit 1
  fi
  prospective_cron_sha256="$(shasum -a 256 "$marker_cron" | awk '{print $1}')"
  printf '0 0 * * * %s readiness\n' \
    "$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE" >>"$marker_cron"
  rollback_marker_payload="$(vp_worker_admission_marker_selection_json expected)"
  python3 - \
    "$marker_config" "$prospective_cron_sha256" \
    "$rollback_marker_payload" <<'PY'
import hashlib
import json
import pathlib
import sys

config_path, expected_cron_sha256, raw_payload = sys.argv[1:]
payload = json.loads(raw_payload)
forward = pathlib.Path(config_path).read_text(encoding="ascii")
expected = forward.replace(
    "GENERATION=m-222222222222-1700000000-0001",
    "GENERATION=m-rb-222222222222-1",
).replace(
    "IMAGE=vp-ffmpeg-worker-python:deploy-222222222222",
    "IMAGE=vp-ffmpeg-worker-python:deploy-111111111111",
).replace(
    "vp-wrm-readiness-db-m-222222222222-1700000000-0001",
    "vp-wrm-readiness-db-m-rb-222222222222-1",
).replace(
    "vp-wrm-janitor-db-m-222222222222-1700000000-0001",
    "vp-wrm-janitor-db-m-rb-222222222222-1",
)
if (
    payload["generation"] != "m-rb-222222222222-1"
    or payload["image"] != "vp-ffmpeg-worker-python:deploy-111111111111"
    or payload["config_sha256"]
    != hashlib.sha256(expected.encode("ascii")).hexdigest()
    or payload["cron_sha256"] != expected_cron_sha256
):
    raise SystemExit("prospective rollback marker identity was not deterministic")
PY
  promotion_selection_payload="$(
    printf '%s\n' "$rollback_marker_payload" | python3 -I -c '
import json
import sys

value = json.load(sys.stdin)
value["config_sha256"] = "f" * 64
print(json.dumps(value, sort_keys=True, separators=(",", ":")))
'
  )"
  vp_worker_admission_recovery_state() {
    printf '{"rollback":{"marker":%s}}\n' "$promotion_selection_payload"
  }
  vp_worker_admission_lock_assert() {
    :
  }

  VP_WORKER_ADMISSION_LOCK_ROOT="$ROOT/state/vp-worker-admission"
  VP_WORKER_ADMISSION_TRANSACTION_ID=tx-22222222222222222222222222222222
  mkdir -p "$VP_WORKER_ADMISSION_LOCK_ROOT/transactions/$VP_WORKER_ADMISSION_TRANSACTION_ID"
  if vp_worker_admission_promotion_identity PROMOTE_ROLLBACK_MARKER \
    >/dev/null 2>&1; then
    echo 'FAIL: promotion intent trusted a marker outside its durable selection' >&2
    exit 1
  fi
  promotion_selection_payload="$rollback_marker_payload"
  rollback_promotion_identity="$(
    vp_worker_admission_promotion_identity PROMOTE_ROLLBACK_MARKER
  )"
  expected_rollback_config="$marker_selection_root/expected-rollback-control.conf"
  vp_worker_redis_marker_render_config \
    "$expected_rollback_config" \
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
  expected_rollback_digest="$(
    vp_worker_admission_promotion_digest "$expected_rollback_config"
  )"
  active_forward_digest="$(vp_worker_admission_promotion_digest "$marker_config")"
  python3 - \
    "$rollback_promotion_identity" "$expected_rollback_digest" \
    "$active_forward_digest" <<'PY'
import json
import sys

path, expected_digest, active_digest = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    identity = json.load(handle)
if (
    identity["generation"] != "m-rb-222222222222-1"
    or identity["spec_digest"] != expected_digest
    or identity["spec_digest"] == active_digest
):
    raise SystemExit("rollback marker promotion intent hashed the active forward config")
PY

  vp_worker_admission_capture_promotion_precondition \
    PROMOTE_ROLLBACK_MARKER "$rollback_promotion_identity"
  rollback_precondition="$VP_WORKER_ADMISSION_LOCK_ROOT/transactions/$VP_WORKER_ADMISSION_TRANSACTION_ID/promotion-preconditions/PROMOTE_ROLLBACK_MARKER.json"
  if [[ ! -f "$rollback_precondition" \
    || "$(vp_worker_redis_marker_file_mode "$rollback_precondition")" != 600 ]]; then
    echo 'FAIL: rollback marker promotion precondition was not durable' >&2
    exit 1
  fi
  VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND=PROMOTE_ROLLBACK_MARKER
  VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME=control.conf
  VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE=worker-redis-marker-control
  VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION=m-rb-222222222222-1
  VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST="$expected_rollback_digest"
  rollback_replay_calls="$marker_selection_root/rollback-replay-calls"
  : >"$rollback_replay_calls"
  vp_restore_worker_redis_marker_controls() {
    printf 'restore-rollback-marker\n' >>"$rollback_replay_calls"
    vp_worker_redis_marker_render_config \
      "$marker_config" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
  }
  vp_worker_admission_write_marker_promotion_receipt() {
    printf 'receipt|%s\n' "$1" >>"$rollback_replay_calls"
  }

  printf 'tampered-marker\n' >"$marker_config"
  if vp_worker_admission_replay_pending_promotion_effect \
    PROMOTE_ROLLBACK_MARKER >/dev/null 2>&1; then
    echo 'FAIL: rollback marker replay accepted a precondition mismatch' >&2
    exit 1
  fi
  if [[ -s "$rollback_replay_calls" ]]; then
    echo 'FAIL: rollback marker replay mutated a tampered current config' >&2
    exit 1
  fi

  vp_worker_redis_marker_render_config \
    "$marker_config" \
    vp-ffmpeg-worker-python:deploy-222222222222 \
    m-222222222222-1700000000-0001
  vp_worker_admission_replay_pending_promotion_effect \
    PROMOTE_ROLLBACK_MARKER
  if [[ "$(cat "$rollback_replay_calls")" \
      != $'restore-rollback-marker\nreceipt|PROMOTE_ROLLBACK_MARKER' \
    || "$(vp_worker_admission_promotion_digest "$marker_config")" \
      != "$expected_rollback_digest" ]]; then
    echo 'FAIL: rollback marker pre-side-effect crash did not replay exactly once' >&2
    exit 1
  fi

  : >"$rollback_replay_calls"
  vp_worker_admission_replay_pending_promotion_effect \
    PROMOTE_ROLLBACK_MARKER
  if [[ "$(cat "$rollback_replay_calls")" \
      != $'restore-rollback-marker\nreceipt|PROMOTE_ROLLBACK_MARKER' ]]; then
    echo 'FAIL: rollback marker target without a receipt was not replayable' >&2
    exit 1
  fi
)

(
  crash_replay_root="$TEST_ROOT/fixed-temporary-crash-replay"
  VP_WORKER_ADMISSION_LOCK_ROOT="$crash_replay_root"
  VP_WORKER_ADMISSION_TRANSACTION_ID=tx-66666666666666666666666666666666
  mkdir -p "$crash_replay_root"
  chmod 0700 "$crash_replay_root"

  promotion_identity="$crash_replay_root/promotion-control.json"
  printf '%s\n' \
    '{"docker_id":null,"generation":"control-666","kind":"manifest","name":"control-current.conf","purpose":"promotion","service":"worker-admission-control","spec_digest":"6666666666666666666666666666666666666666666666666666666666666666"}' \
    >"$promotion_identity"
  chmod 0600 "$promotion_identity"
  precondition_path="$(
    vp_worker_admission_promotion_precondition_path PROMOTE_CONTROL
  )"
  mkdir -p "${precondition_path%/*}"
  printf 'stale-precondition-temporary\n' >"$precondition_path.tmp"
  if ! vp_worker_admission_capture_promotion_precondition \
    PROMOTE_CONTROL "$promotion_identity"; then
    echo 'FAIL: stale fixed precondition temporary blocked crash replay' >&2
    exit 1
  fi
  if [[ ! -f "$precondition_path" \
    || "$(command cat "$precondition_path.tmp")" \
      != 'stale-precondition-temporary' ]]; then
    echo 'FAIL: precondition crash replay did not preserve the stale temporary' >&2
    exit 1
  fi

  VP_WORKER_ADMISSION_REPLAY_ACTIVE=true
  VP_WORKER_ADMISSION_REPLAY_OPERATION_ID=operation-66666666666666666666666666666666
  VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND=PROMOTE_MARKER
  VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME=control.conf
  VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE=worker-redis-marker-control
  VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION=marker-666
  VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST=6666666666666666666666666666666666666666666666666666666666666666
  vp_worker_admission_load_replay_plan() {
    :
  }
  receipt_path="$(
    vp_worker_admission_marker_promotion_receipt_path PROMOTE_MARKER
  )"
  mkdir -p "${receipt_path%/*}"
  printf 'stale-receipt-temporary\n' >"$receipt_path.tmp"
  if ! vp_worker_admission_write_marker_promotion_receipt PROMOTE_MARKER; then
    echo 'FAIL: stale fixed receipt temporary blocked crash replay' >&2
    exit 1
  fi
  if [[ ! -f "$receipt_path" \
    || "$(command cat "$receipt_path.tmp")" != 'stale-receipt-temporary' ]]; then
    echo 'FAIL: receipt crash replay did not preserve the stale temporary' >&2
    exit 1
  fi
)

(
  worker_promotion_root="$TEST_ROOT/partial-worker-promotion"
  VP_WORKER_ADMISSION_LOCK_ROOT="$worker_promotion_root"
  VP_WORKER_ADMISSION_TRANSACTION_ID=tx-77777777777777777777777777777777
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=7777777777777777777777777777777777777777
  current_dir="$worker_promotion_root/current"
  candidate_dir="$worker_promotion_root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  mkdir -p "$current_dir" "$candidate_dir"
  chmod 0700 "$worker_promotion_root" "$current_dir" \
    "$worker_promotion_root/candidates" "$candidate_dir"

  vp_worker_admission_write_manifest \
    "$current_dir/ffmpeg-go.conf" vp-ffmpeg-worker-go-swarm \
    1111111111111111111111111111111111111111 vp-go:old 101 \
    go-db-old go-admission-old \
    aaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbb
  vp_worker_admission_write_manifest \
    "$current_dir/vision.conf" vp-vision-worker-swarm \
    1111111111111111111111111111111111111111 vp-vision:old 102 \
    vision-db-old vision-admission-old \
    cccccccccccccccccccccccc dddddddddddddddddddddddd
  vp_worker_admission_write_manifest \
    "$current_dir/youtube-publisher.conf" vp-youtube-publisher-swarm \
    1111111111111111111111111111111111111111 vp-publisher:untouched 103 \
    publisher-db-old publisher-admission-old \
    333333333333333333333333 444444444444444444444444
  vp_worker_admission_write_manifest \
    "$candidate_dir/ffmpeg-go.conf" vp-ffmpeg-worker-go-swarm \
    7777777777777777777777777777777777777777 vp-go:new 201 \
    go-db-new go-admission-new \
    eeeeeeeeeeeeeeeeeeeeeeee ffffffffffffffffffffffff
  vp_worker_admission_write_manifest \
    "$candidate_dir/vision.conf" vp-vision-worker-swarm \
    7777777777777777777777777777777777777777 vp-vision:new 202 \
    vision-db-new vision-admission-new \
    111111111111111111111111 222222222222222222222222

  worker_target_digest="$(
    vp_worker_admission_promotion_digest "$candidate_dir"
  )"
  worker_identity="$worker_promotion_root/worker-promotion.json"
  printf '%s\n' \
    "{\"docker_id\":null,\"generation\":\"$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE\",\"kind\":\"manifest\",\"name\":\"worker-manifests\",\"purpose\":\"promotion\",\"service\":\"worker-admission\",\"spec_digest\":\"$worker_target_digest\"}" \
    >"$worker_identity"
  chmod 0600 "$worker_identity"
  vp_worker_admission_capture_promotion_precondition \
    PROMOTE_WORKERS "$worker_identity"

  # Crash after the first manifest replacement but before the second.
  cp "$candidate_dir/ffmpeg-go.conf" "$current_dir/ffmpeg-go.conf"
  chmod 0600 "$current_dir/ffmpeg-go.conf"
  VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND=PROMOTE_WORKERS
  VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME=worker-manifests
  VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE=worker-admission
  VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST="$worker_target_digest"
  worker_replay_calls="$worker_promotion_root/replay-calls"
  : >"$worker_replay_calls"
  vp_worker_admission_pending_promotion_target_matches() {
    :
  }
  vp_worker_admission_apply_promotion_effect() {
    printf 'apply|%s\n' "$1" >>"$worker_replay_calls"
  }
  vp_worker_admission_replay_pending_promotion_effect PROMOTE_WORKERS
  if [[ "$(command cat "$worker_replay_calls")" \
    != 'apply|PROMOTE_WORKERS' ]]; then
    echo 'FAIL: partial worker manifest promotion was not replayable' >&2
    exit 1
  fi

  : >"$worker_replay_calls"
  printf 'tampered\n' >"$current_dir/vision.conf"
  chmod 0600 "$current_dir/vision.conf"
  if vp_worker_admission_replay_pending_promotion_effect \
    PROMOTE_WORKERS >/dev/null 2>&1; then
    echo 'FAIL: worker promotion replay accepted a tampered manifest' >&2
    exit 1
  fi
  if [[ -s "$worker_replay_calls" ]]; then
    echo 'FAIL: tampered worker promotion replay reached the mutation' >&2
    exit 1
  fi
)

(
  partial_prepare_calls="$TEST_ROOT/same-process-partial-prepare-calls"
  : >"$partial_prepare_calls"
  VP_WORKER_ADMISSION_PREPARED=false
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$TEST_ROOT/baseline-managed-state"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=vp-ffmpeg-worker-python:partial
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=m-0123456789ab-1780000000-0001
  vp_worker_admission_root() {
    printf '%s\n' "$TEST_ROOT/same-process-partial-root"
  }
  vp_restore_app_snapshots() {
    printf 'restore-apps\n' >>"$partial_prepare_calls"
  }
  vp_worker_redis_marker_restore_managed_state() {
    printf 'restore-marker\n' >>"$partial_prepare_calls"
  }
  vp_worker_redis_marker_remove_generation_jobs() {
    printf 'remove-marker-jobs|%s|%s\n' "$1" "$2" \
      >>"$partial_prepare_calls"
  }
  vp_worker_admission_abort_preparing_transaction() {
    printf 'abort|%s\n' "$1" >>"$partial_prepare_calls"
  }
  vp_worker_admission_abort_vision_jobs() {
    printf 'abort-vision-jobs\n' >>"$partial_prepare_calls"
  }
  vp_worker_redis_marker_discard_managed_state() {
    printf 'discard-marker\n' >>"$partial_prepare_calls"
  }

  vp_restore_worker_admission_transaction '' '' ''
  expected_partial_prepare="$({
    printf '%s\n' \
      'restore-apps' \
      'restore-marker' \
      'remove-marker-jobs|vp-ffmpeg-worker-python:partial|m-0123456789ab-1780000000-0001' \
      'abort-vision-jobs' \
      'abort|preparing_failed' \
      'discard-marker'
  })"
  if [[ "$(command cat "$partial_prepare_calls")" \
    != "$expected_partial_prepare" ]]; then
    echo 'FAIL: same-process partial preparation did not restore marker before abort' >&2
    exit 1
  fi
)

(
  failed_control_root="$TEST_ROOT/failed-forward-control"
  ROOT="$failed_control_root/sync"
  mkdir -p "$ROOT/state/vp-worker-admission"
  failed_control_config="$ROOT/state/vp-worker-admission/staging-object-janitor.conf"
  printf '%s\n' \
    'VERSION=2' \
    'GENERATION=c-22222222222222222222' \
    'IMAGE=vp-ffmpeg-worker-python:deploy-222222222222' \
    >"$failed_control_config"
  chmod 0600 "$failed_control_config"
  failed_control_cron="$failed_control_root/crontab"
  printf '%s\n' \
    '# unrelated operator entry' \
    '# BEGIN VIDEOPROCESS STAGING JANITOR' \
    "*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE=$failed_control_config $ROOT/bin/vp-staging-object-janitor-run.sh >> $ROOT/logs/vp-staging-object-janitor.log 2>&1" \
    '# END VIDEOPROCESS STAGING JANITOR' \
    >"$failed_control_cron"
  crontab() {
    [[ "$#" -eq 1 && "$1" == -l ]] || return 1
    command cat "$failed_control_cron"
  }
  VP_WORKER_CONTROL_PREPARED=true
  VP_WORKER_CONTROL_GENERATION=c-22222222222222222222
  VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-222222222222

  failed_control_payload="$(vp_worker_admission_failed_forward_payload '')"
  python3 - \
    "$failed_control_config" "$failed_control_cron" \
    "$failed_control_payload" <<'PY'
import hashlib
import json
import pathlib
import sys

config_path, cron_path, raw_payload = sys.argv[1:]
payload = json.loads(raw_payload)
expected = {
    "config_sha256": hashlib.sha256(pathlib.Path(config_path).read_bytes()).hexdigest(),
    "cron_sha256": hashlib.sha256(pathlib.Path(cron_path).read_bytes()).hexdigest(),
    "generation": "c-22222222222222222222",
    "image": "vp-ffmpeg-worker-python:deploy-222222222222",
}
if payload != {"control": expected, "services": []}:
    raise SystemExit("failed-forward payload lost installed control identity")
PY

  VP_WORKER_CONTROL_PREPARED=false
  failed_control_payload="$(vp_worker_admission_failed_forward_payload '')"
  python3 - "$failed_control_payload" <<'PY'
import json
import sys

if json.loads(sys.argv[1]) != {"control": None, "services": []}:
    raise SystemExit("unprepared failed-forward payload claimed control authority")
PY

  VP_WORKER_CONTROL_PREPARED=true
  rm -f "$failed_control_config"
  if vp_worker_admission_failed_forward_payload '' >/dev/null 2>&1; then
    echo 'FAIL: failed-forward payload accepted missing prepared control config' >&2
    exit 1
  fi

  VP_WORKER_CONTROL_PREPARED=false
  vp_service_values() {
    return 1
  }
  if vp_worker_admission_failed_forward_payload \
    'vp-api-swarm' >/dev/null 2>&1; then
    echo 'FAIL: failed-forward payload silently omitted an attempted service' >&2
    exit 1
  fi
)

(
  VP_WORKER_CONTROL_GENERATION=c-22222222222222222222
  VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-222222222222
  vp_require_staging_object_janitor_control() {
    :
  }
  vp_app_service_durable_identity() {
    printf '%s|%s\n' \
      aaaaaaaaaaaaaaaaaaaaaaaa \
      0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  }
  janitor_service_payload="$(vp_worker_admission_janitor_service_json)"
  python3 - "$janitor_service_payload" <<'PY'
import json
import sys

if json.loads(sys.argv[1]) != {
    "docker_service_id": "aaaaaaaaaaaaaaaaaaaaaaaa",
    "generation": "c-22222222222222222222",
    "name": "vp-staging-object-janitor",
    "spec_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
}:
    raise SystemExit("janitor service payload lost immutable identity")
PY
)

(
  compensation_calls="$TEST_ROOT/failed-forward-compensation-calls"
  : >"$compensation_calls"
  VP_WORKER_CONTROL_GENERATION=c-22222222222222222222
  VP_WORKER_ADMISSION_CONTROL_IMAGE=vp-ffmpeg-worker-python:deploy-222222222222
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$VP_WORKER_CONTROL_GENERATION"
  VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$VP_WORKER_ADMISSION_CONTROL_IMAGE"
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256="$(printf 'a%.0s' {1..64})"
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256="$(printf 'b%.0s' {1..64})"
  CONTROL_IDENTITY_MATCH=true
  vp_install_staging_object_janitor() {
    printf 'control|install|%s\n' "$1" >>"$compensation_calls"
  }
  vp_worker_admission_failed_forward_control_json() {
    local config_hash="$VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256"
    [[ "$CONTROL_IDENTITY_MATCH" == true ]] || config_hash="$(printf 'f%.0s' {1..64})"
    printf '{"config_sha256":"%s","cron_sha256":"%s","generation":"%s","image":"%s"}\n' \
      "$config_hash" \
      "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256" \
      "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
      "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE"
  }
  vp_reinstall_failed_forward_control
  grep -Fqx \
    "control|install|$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" \
    "$compensation_calls"
  CONTROL_IDENTITY_MATCH=false
  if vp_reinstall_failed_forward_control >/dev/null 2>&1; then
    echo 'FAIL: failed-forward control accepted a mismatched installed identity' >&2
    exit 1
  fi

  : >"$compensation_calls"
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=m-rb-222222222222-1
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=vp-ffmpeg-worker-python:deploy-111111111111
  VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION=m-222222222222-1700000000-0001
  VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE=vp-ffmpeg-worker-python:deploy-222222222222
  VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256="$(printf 'c%.0s' {1..64})"
  VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256="$(printf 'd%.0s' {1..64})"
  VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID=111111111111111111111111
  VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID=222222222222222222222222
  VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID=333333333333333333333333
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=runtime-readiness
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID=444444444444444444444444
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=runtime-janitor
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID=555555555555555555555555
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION=2222222222222222222222222222222222222222
  MARKER_MANIFEST_MATCH=true
  vp_worker_redis_marker_control_root() {
    printf '%s\n' "$TEST_ROOT/failed-forward-marker-control"
  }
  vp_python_worker_prepare_controlled_directory() {
    printf '%s\n' "$1"
  }
  vp_worker_redis_marker_remove_generation_jobs() {
    printf 'marker|remove|%s|%s\n' "$1" "$2" >>"$compensation_calls"
  }
  vp_worker_redis_marker_require_secret_manifest() {
    printf 'marker|manifest|%s\n' "$2" >>"$compensation_calls"
    VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=111111111111111111111111
    VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=222222222222222222222222
    VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=333333333333333333333333
    if [[ "$MARKER_MANIFEST_MATCH" != true ]]; then
      VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=999999999999999999999999
    fi
  }
  vp_install_worker_redis_marker_control() {
    printf 'marker|install|%s|%s\n' "$1" "$2" >>"$compensation_calls"
  }
  vp_run_worker_redis_marker_readiness() {
    printf 'marker|ready\n' >>"$compensation_calls"
  }
  vp_worker_admission_marker_selection_json() {
    python3 - <<'PY'
import json
import os

print(json.dumps({
    "config_sha256": os.environ["VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256"],
    "cron_sha256": os.environ["VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256"],
    "generation": os.environ["VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION"],
    "image": os.environ["VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE"],
    "secrets": [
        {
            "docker_secret_id": os.environ[f"VP_WORKER_ROLLBACK_FAILED_MARKER_{purpose.upper()}_DATABASE_SECRET_ID"],
            "generation": os.environ["VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION"],
            "name": f"vp-wrm-{purpose}-db-{os.environ['VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION']}",
            "purpose": f"{purpose}-database",
            "service": "worker-redis-marker-control",
        }
        for purpose in ("readiness", "janitor", "repair")
    ] + [
        {
            "docker_secret_id": os.environ[f"VP_WORKER_REDIS_MARKER_{purpose.upper()}_REDIS_SECRET_ID"],
            "generation": os.environ["VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION"],
            "name": os.environ[f"VP_WORKER_REDIS_MARKER_{purpose.upper()}_REDIS_SECRET"],
            "purpose": f"{purpose}-redis",
            "service": "vp-worker-redis-runtime",
        }
        for purpose in ("readiness", "janitor")
    ],
}, sort_keys=True, separators=(",", ":")))
PY
  }
  export \
    VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256 \
    VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256 \
    VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION \
    VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE \
    VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID \
    VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID \
    VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID \
    VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET \
    VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID \
    VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET \
    VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID \
    VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION
  vp_restore_failed_forward_marker_controls
  marker_remove_line="$(grep -n '^marker|remove|' "$compensation_calls" | cut -d: -f1)"
  marker_install_line="$(grep -n '^marker|install|' "$compensation_calls" | cut -d: -f1)"
  marker_ready_line="$(grep -n '^marker|ready$' "$compensation_calls" | cut -d: -f1)"
  if [[ -z "$marker_remove_line" || -z "$marker_install_line" \
    || -z "$marker_ready_line" || "$marker_remove_line" -ge "$marker_install_line" \
    || "$marker_install_line" -ge "$marker_ready_line" ]]; then
    echo 'FAIL: failed-forward marker compensation order is invalid' >&2
    exit 1
  fi
  : >"$compensation_calls"
  MARKER_MANIFEST_MATCH=false
  if vp_restore_failed_forward_marker_controls >/dev/null 2>&1 \
    || grep -Fq 'marker|install|' "$compensation_calls"; then
    echo 'FAIL: marker compensation installed with drifted secret IDs' >&2
    exit 1
  fi

  : >"$compensation_calls"
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=2222222222222222222222222222222222222222
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=vp-vision-worker-swarm
  VP_WORKER_ADMISSION_PREPARED=true
  vp_worker_admission_root() {
    printf '%s\n' "$TEST_ROOT/failed-forward-workers"
  }
  vp_worker_admission_kind() {
    [[ "$1" == vp-vision-worker-swarm ]]
    printf 'vision\n'
  }
  vp_worker_admission_require_v2_manifest() {
    VP_WORKER_MANIFEST_SERVICE=vp-vision-worker-swarm
    VP_WORKER_MANIFEST_GENERATION=203
    VP_WORKER_MANIFEST_IMAGE=vp-ffmpeg-worker-python:deploy-222222222222
    VP_WORKER_MANIFEST_DATABASE_SECRET=failed-vision-db-203
    VP_WORKER_MANIFEST_ADMISSION_SECRET=failed-vision-admission-203
  }
  vp_registered_worker_service_current_id() {
    printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaa
  }
  vp_worker_admission_set_candidate() {
    printf 'worker|select|%s|%s\n' "$1" "$2" >>"$compensation_calls"
  }
  vp_activate_worker_admission() {
    printf 'worker|activate|%s\n' "$1" >>"$compensation_calls"
  }
  vp_deploy_vision_worker() {
    printf 'worker|deploy|%s\n' "$1" >>"$compensation_calls"
  }
  vp_app_service_durable_identity() {
    printf '%s|%s\n' \
      aaaaaaaaaaaaaaaaaaaaaaaa \
      0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  }
  vp_require_worker_deployment_ready() {
    printf 'worker|ready|%s\n' "$1" >>"$compensation_calls"
  }
  vp_restore_failed_forward_worker_service \
    vp-vision-worker-swarm 203 aaaaaaaaaaaaaaaaaaaaaaaa \
    0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  expected_worker_calls="$({
    printf '%s\n' \
      'worker|select|vp-vision-worker-swarm|203' \
      'worker|activate|vp-vision-worker-swarm' \
      'worker|deploy|vp-ffmpeg-worker-python:deploy-222222222222' \
      'worker|ready|vp-vision-worker-swarm'
  })"
  if [[ "$(command cat "$compensation_calls")" != "$expected_worker_calls" ]]; then
    echo 'FAIL: failed-forward worker compensation order or identity drifted' >&2
    exit 1
  fi

  : >"$compensation_calls"
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES='vp-vision-worker-swarm|203|aaaaaaaaaaaaaaaaaaaaaaaa|0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
  vp_failed_forward_control_identity_matches() {
    printf 'verify|control\n' >>"$compensation_calls"
  }
  vp_failed_forward_marker_identity_matches() {
    printf 'verify|marker\n' >>"$compensation_calls"
  }
  vp_run_staging_object_janitor_once() {
    printf 'verify|janitor-run\n' >>"$compensation_calls"
  }
  vp_require_staging_object_janitor_control() {
    printf 'verify|janitor-control\n' >>"$compensation_calls"
  }
  vp_require_worker_redis_marker_status() {
    printf 'verify|marker-status\n' >>"$compensation_calls"
  }
  vp_require_worker_service_descriptor() {
    printf 'verify|descriptor|%s\n' "$1" >>"$compensation_calls"
  }
  vp_verify_failed_forward_candidate
  expected_verify_calls="$({
    printf '%s\n' \
      'verify|control' \
      'verify|marker' \
      'verify|janitor-run' \
      'verify|janitor-control' \
      'verify|marker-status' \
      'worker|ready|vp-vision-worker-swarm' \
      'verify|descriptor|vp-vision-worker-swarm'
  })"
  if [[ "$(command cat "$compensation_calls")" != "$expected_verify_calls" ]]; then
    echo 'FAIL: failed-forward candidate verification gates drifted' >&2
    exit 1
  fi
)

(
  candidate_calls="$TEST_ROOT/failed-rollback-candidate-calls"
  : >"$candidate_calls"
  STAGE2_TEST_PHASE=CANDIDATE_RESTORE_REQUIRED
  CANDIDATE_INSTALL_FAIL=true
  candidate_records="$({
    printf '%s|201|%s|%s\n' \
      vp-ffmpeg-worker-go-swarm aaaaaaaaaaaaaaaaaaaaaaaa \
      0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    printf '%s|203|%s|%s\n' \
      "$VP_VISION_WORKER_SERVICE" bbbbbbbbbbbbbbbbbbbbbbbb \
      1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  })"
  vp_reinstall_failed_forward_control() {
    printf 'candidate-control|install\n' >>"$candidate_calls"
    [[ "$CANDIDATE_INSTALL_FAIL" != true ]]
  }
  vp_restore_failed_forward_marker_controls() {
    printf 'candidate-marker|restore\n' >>"$candidate_calls"
  }
  vp_restore_failed_forward_worker_service() {
    printf 'candidate-worker|%s|%s|%s|%s\n' "$@" \
      >>"$candidate_calls"
  }
  vp_verify_failed_forward_candidate() {
    printf 'candidate|verify\n' >>"$candidate_calls"
  }
  vp_worker_admission_transition_to() {
    printf 'phase|%s\n' "$1" >>"$candidate_calls"
    STAGE2_TEST_PHASE="$1"
  }
  vp_worker_admission_load_replay_plan() {
    VP_WORKER_ADMISSION_REPLAY_PHASE="$STAGE2_TEST_PHASE"
  }
  vp_worker_admission_retire_generation() {
    printf 'unexpected-retire\n' >>"$candidate_calls"
    return 1
  }

  if vp_restore_failed_forward_candidate "$candidate_records"; then
    echo 'FAIL: failed candidate reinstall reported restoration' >&2
    exit 1
  fi
  if [[ "$STAGE2_TEST_PHASE" != CANDIDATE_RESTORE_REQUIRED ]] \
    || grep -Eq '^(candidate-marker|candidate-worker|unexpected-retire|phase)' \
      "$candidate_calls"; then
    echo 'FAIL: failed candidate reinstall advanced phase or destroyed authority' >&2
    exit 1
  fi

  : >"$candidate_calls"
  CANDIDATE_INSTALL_FAIL=false
  vp_restore_failed_forward_candidate "$candidate_records"
  if [[ "$STAGE2_TEST_PHASE" != CANDIDATE_RESTORED ]]; then
    echo 'FAIL: compensated rollback did not reach CANDIDATE_RESTORED' >&2
    exit 1
  fi
  install_line="$(grep -n '^candidate-control|install$' "$candidate_calls" | cut -d: -f1)"
  marker_line="$(grep -n '^candidate-marker|restore$' "$candidate_calls" | cut -d: -f1)"
  worker_line="$(grep -n '^candidate-worker|' "$candidate_calls" | tail -1 | cut -d: -f1)"
  verify_line="$(grep -n '^candidate|verify$' "$candidate_calls" | cut -d: -f1)"
  restored_line="$(grep -n '^phase|CANDIDATE_RESTORED$' "$candidate_calls" | cut -d: -f1)"
  if [[ -z "$install_line" || -z "$marker_line" || -z "$worker_line" \
    || -z "$verify_line" || -z "$restored_line" \
    || "$install_line" -ge "$marker_line" \
    || "$marker_line" -ge "$worker_line" \
    || "$worker_line" -ge "$verify_line" \
    || "$verify_line" -ge "$restored_line" ]]; then
    echo 'FAIL: failed-forward candidate compensation order is invalid' >&2
    exit 1
  fi
  if grep -Fqx unexpected-retire "$candidate_calls"; then
    echo 'FAIL: candidate compensation retired candidate or rollback credentials' >&2
    exit 1
  fi

  : >"$candidate_calls"
  STAGE2_TEST_PHASE=CANDIDATE_RESTORING
  vp_restore_failed_forward_candidate "$candidate_records"
  if grep -Fqx 'phase|CANDIDATE_RESTORING' "$candidate_calls" \
    || [[ "$STAGE2_TEST_PHASE" != CANDIDATE_RESTORED ]]; then
    echo 'FAIL: CANDIDATE_RESTORING replay repeated its phase transition' >&2
    exit 1
  fi
)

(
  removal_calls="$TEST_ROOT/durable-candidate-removal-calls"
  : >"$removal_calls"
  durable_candidate_id=bbbbbbbbbbbbbbbbbbbbbbbb
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS="${VP_PUBLISHER_SERVICE}|304|${durable_candidate_id}"

  vp_worker_service_contract() {
    echo 'FAIL: fresh recovery unexpectedly used the process-local worker contract' >&2
    return 1
  }
  vp_registered_worker_service_current_id() {
    [[ "$1" == "$VP_PUBLISHER_SERVICE" ]] || return 1
    printf '%s\n' "$durable_candidate_id"
  }
  vp_registered_worker_service_identity() {
    [[ "$1" == "$durable_candidate_id" \
      && "$2" == "$VP_PUBLISHER_SERVICE" \
      && "$3" == 304 ]] || return 1
    printf '%s\n' "$durable_candidate_id"
  }
  vp_require_worker_redis_marker_status() {
    return 0
  }
  docker() {
    if [[ "$1" == service && "$2" == rm \
      && "$3" == "$durable_candidate_id" ]]; then
      printf 'docker|%s\n' "$*" >>"$removal_calls"
      return 0
    fi
    [[ "$1" == service && "$2" == inspect ]] && return 1
    return 1
  }

  if ! vp_remove_new_registered_worker "$VP_PUBLISHER_SERVICE"; then
    echo 'FAIL: fresh recovery could not remove a durable baseline-absent worker' >&2
    exit 1
  fi
  expected_removal="docker|service rm $durable_candidate_id"
  if [[ "$(command cat "$removal_calls")" != "$expected_removal" ]]; then
    echo 'FAIL: durable baseline-absent worker removal used the wrong authority' >&2
    exit 1
  fi
)

(
  removal_calls="$TEST_ROOT/volatile-candidate-removal-calls"
  : >"$removal_calls"
  partial_candidate_id=cccccccccccccccccccccccc
  unrelated_candidate_records="vp-ffmpeg-worker-go-swarm|101|bbbbbbbbbbbbbbbbbbbbbbbb"
  process_candidate_records="$VP_PUBLISHER_SERVICE|304|$partial_candidate_id"

  vp_worker_service_contract() {
    echo 'FAIL: explicit process candidate authority consulted hydrated globals' >&2
    return 1
  }
  vp_registered_worker_service_current_id() {
    [[ "$1" == "$VP_PUBLISHER_SERVICE" ]] || return 1
    printf '%s\n' "$partial_candidate_id"
  }
  vp_registered_worker_service_identity() {
    [[ "$1" == "$partial_candidate_id" \
      && "$2" == "$VP_PUBLISHER_SERVICE" \
      && "$3" == 304 ]] || return 1
    printf '%s\n' "$partial_candidate_id"
  }
  vp_require_worker_redis_marker_status() {
    return 0
  }
  docker() {
    if [[ "$1" == service && "$2" == rm \
      && "$3" == "$partial_candidate_id" ]]; then
      printf 'docker|%s\n' "$*" >>"$removal_calls"
      return 0
    fi
    [[ "$1" == service && "$2" == inspect ]] && return 1
    return 1
  }

  if ! vp_remove_new_registered_worker \
      "$VP_PUBLISHER_SERVICE" \
      "$unrelated_candidate_records" "$process_candidate_records"; then
    echo 'FAIL: same-process rollback could not remove a partial worker create' >&2
    exit 1
  fi
  expected_removal="docker|service rm $partial_candidate_id"
  if [[ "$(command cat "$removal_calls")" != "$expected_removal" ]]; then
    echo 'FAIL: same-process partial worker removal used the wrong authority' >&2
    exit 1
  fi
  : >"$removal_calls"
  if vp_remove_new_registered_worker \
      "$VP_PUBLISHER_SERVICE" "$unrelated_candidate_records" ""; then
    echo 'FAIL: fresh recovery accepted process-local partial worker authority' >&2
    exit 1
  fi
  if [[ -s "$removal_calls" ]]; then
    echo 'FAIL: rejected fresh partial worker recovery reached mutation' >&2
    exit 1
  fi
)

(
  replay_calls="$TEST_ROOT/stage2-replay-dispatch-calls"
  : >"$replay_calls"
  REPLAY_TEST_PHASE=
  REPLAY_TEST_ACTIVE=true

  vp_worker_admission_lock_assert() {
    return 0
  }
  vp_worker_admission_load_replay_plan() {
    VP_WORKER_ADMISSION_REPLAY_ACTIVE="$REPLAY_TEST_ACTIVE"
    VP_WORKER_ADMISSION_REPLAY_PHASE="$REPLAY_TEST_PHASE"
    VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND=-
  }
  vp_worker_admission_verify_active_database_credentials() {
    printf 'verify|' >>"$replay_calls"
  }
  vp_worker_admission_resume_preparing_transaction() {
    printf 'resume|preparing\n' >>"$replay_calls"
    REPLAY_TEST_ACTIVE=false
  }
  vp_worker_admission_resume_forward_failure() {
    printf 'resume|forward-failure\n' >>"$replay_calls"
    REPLAY_TEST_ACTIVE=false
  }
  vp_worker_admission_resume_durable_rollback() {
    printf 'resume|rollback|%s\n' "$REPLAY_TEST_PHASE" >>"$replay_calls"
    REPLAY_TEST_ACTIVE=false
  }
  vp_worker_admission_resume_candidate_restore() {
    printf 'resume|candidate|%s\n' "$REPLAY_TEST_PHASE" >>"$replay_calls"
    REPLAY_TEST_ACTIVE=false
  }

  while IFS='|' read -r phase expected; do
    : >"$replay_calls"
    REPLAY_TEST_PHASE="$phase"
    REPLAY_TEST_ACTIVE=true
    if ! vp_reconcile_worker_admission_transaction; then
      echo "FAIL: $phase replay dispatcher returned non-zero" >&2
      exit 1
    fi
    if [[ "$(command cat "$replay_calls")" != "$expected" ]]; then
      echo "FAIL: $phase replay did not dispatch its durable recovery action" >&2
      exit 1
    fi
  done <<'EOF'
PREPARING|verify|resume|preparing
FORWARD_APPLYING|verify|resume|forward-failure
ROLLBACK_PREPARING|verify|resume|rollback|ROLLBACK_PREPARING
ROLLBACK_APPLYING|verify|resume|rollback|ROLLBACK_APPLYING
CANDIDATE_RESTORE_REQUIRED|verify|resume|candidate|CANDIDATE_RESTORE_REQUIRED
CANDIDATE_RESTORING|verify|resume|candidate|CANDIDATE_RESTORING
CANDIDATE_RESTORED|verify|resume|candidate|CANDIDATE_RESTORED
EOF
)

(
  resume_calls="$TEST_ROOT/stage2-resume-contract-calls"
  : >"$resume_calls"
  RESUME_TEST_PHASE=FORWARD_APPLYING
  VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=false
  VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS='vp-api-swarm|aaaaaaaaaaaaaaaaaaaaaaaa|baseline-api|1111111111111111111111111111111111111111111111111111111111111111'
  VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES='vp-api-swarm'
  VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS='candidate-records'
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES='candidate-identities'
  VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=false
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=marker-image
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=marker-generation

  vp_worker_admission_hydrate_recovery_context() {
    printf 'hydrate|%s\n' "$RESUME_TEST_PHASE" >>"$resume_calls"
    VP_WORKER_ADMISSION_RECOVERY_PHASE="$RESUME_TEST_PHASE"
  }
  vp_worker_admission_abort_preparing_transaction() {
    printf 'abort|%s\n' "$1" >>"$resume_calls"
  }
  vp_worker_admission_abort_vision_jobs() {
    printf 'abort-vision-jobs\n' >>"$resume_calls"
  }
  vp_worker_admission_capture_failed_forward() {
    printf 'capture-failed|%s\n' "$1" >>"$resume_calls"
    VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=true
  }
  vp_worker_admission_transition_to() {
    printf 'transition|%s\n' "$1" >>"$resume_calls"
    RESUME_TEST_PHASE="$1"
  }
  vp_restore_worker_admission_transaction() {
    printf 'restore-baseline|%s|%s|%s\n' "$1" "$2" "$3" \
      >>"$resume_calls"
  }
  vp_restore_app_snapshots() {
    printf 'restore-apps|%s|%s|%s\n' "$1" "$2" "$3" >>"$resume_calls"
  }
  vp_worker_redis_marker_discard_managed_state() {
    printf 'discard-marker-baseline\n' >>"$resume_calls"
  }
  vp_worker_redis_marker_restore_managed_state() {
    printf 'restore-marker-baseline\n' >>"$resume_calls"
  }
  vp_worker_redis_marker_remove_generation_jobs() {
    printf 'remove-marker-jobs|%s|%s\n' "$1" "$2" >>"$resume_calls"
  }
  vp_restore_failed_forward_candidate() {
    printf 'restore-candidate|%s\n' "$1" >>"$resume_calls"
    RESUME_TEST_PHASE=CANDIDATE_RESTORED
  }
  vp_verify_failed_forward_candidate() {
    printf 'verify-candidate\n' >>"$resume_calls"
  }

  : >"$resume_calls"
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  vp_worker_admission_resume_preparing_transaction
  if [[ "$(command cat "$resume_calls")" != \
      $'abort-vision-jobs\nabort|interrupted_preparing' \
    || "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" != true ]]; then
    echo 'FAIL: interrupted PREPARING did not enter durable abort' >&2
    exit 1
  fi

  : >"$resume_calls"
  RESUME_TEST_PHASE=FORWARD_APPLYING
  VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=false
  VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=false
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=false
  vp_worker_admission_resume_forward_failure
  expected_forward="$({
    printf '%s\n' \
      'hydrate|FORWARD_APPLYING' \
      'abort-vision-jobs' \
      'capture-failed|vp-api-swarm' \
      'transition|ROLLBACK_PREPARING' \
      'hydrate|ROLLBACK_PREPARING' \
      "restore-baseline|$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS|vp-api-swarm|candidate-records"
  })"
  if [[ "$(command cat "$resume_calls")" != "$expected_forward" ]]; then
    echo 'FAIL: interrupted forward did not capture state before rollback replay' >&2
    exit 1
  fi

  : >"$resume_calls"
  RESUME_TEST_PHASE=FORWARD_APPLYING
  VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=false
  VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=true
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=true
  vp_worker_admission_resume_forward_failure
  expected_early_forward="$({
    printf '%s\n' \
      'hydrate|FORWARD_APPLYING' \
      "restore-apps|$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS|vp-api-swarm|false" \
      'abort-vision-jobs' \
      'abort|preparing_failed' \
      'discard-marker-baseline'
  })"
  if [[ "$(command cat "$resume_calls")" != "$expected_early_forward" ]]; then
    echo 'FAIL: early forward crash did not restore apps before durable abort' >&2
    exit 1
  fi
  VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=false

  : >"$resume_calls"
  RESUME_TEST_PHASE=FORWARD_APPLYING
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=true
  vp_worker_admission_resume_forward_failure
  expected_partial_forward="$({
    printf '%s\n' \
      'hydrate|FORWARD_APPLYING' \
      "restore-apps|$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS|vp-api-swarm|false" \
      'restore-marker-baseline' \
      'remove-marker-jobs|marker-image|marker-generation' \
      'abort-vision-jobs' \
      'abort|preparing_failed' \
      'discard-marker-baseline'
  })"
  if [[ "$(command cat "$resume_calls")" != "$expected_partial_forward" ]]; then
    echo 'FAIL: partial forward crash did not restore marker before durable abort' >&2
    exit 1
  fi
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=false

  for phase in \
    CANDIDATE_RESTORE_REQUIRED \
    CANDIDATE_RESTORING \
    CANDIDATE_RESTORED; do
    : >"$resume_calls"
    RESUME_TEST_PHASE="$phase"
    vp_worker_admission_resume_candidate_restore
    if [[ "$phase" == CANDIDATE_RESTORED ]]; then
      expected_candidate="$({
        printf '%s\n' \
          "hydrate|$phase" \
          'verify-candidate' \
          'transition|ROLLBACK_PREPARING' \
          'hydrate|ROLLBACK_PREPARING' \
          "restore-baseline|$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS|vp-api-swarm|candidate-records"
      })"
    else
      expected_candidate="$({
        printf '%s\n' \
          "hydrate|$phase" \
          'restore-candidate|candidate-identities' \
          'verify-candidate' \
          'transition|ROLLBACK_PREPARING' \
          'hydrate|ROLLBACK_PREPARING' \
          "restore-baseline|$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS|vp-api-swarm|candidate-records"
      })"
    fi
    if [[ "$(command cat "$resume_calls")" != "$expected_candidate" ]]; then
      echo "FAIL: $phase did not complete candidate compensation before rollback" >&2
      exit 1
    fi
  done
)

(
  vp_worker_admission_lock_assert() {
    :
  }
  recovery_state="$TEST_ROOT/stage2-recovery-state.json"
  cat >"$recovery_state" <<'JSON'
{"baseline":{"captured":true,"kind":"managed","services":[{"docker_service_id":"aaaaaaaaaaaaaaaaaaaaaaaa","existed":true,"image":"vp-api:deploy-111111111111","name":"vp-api-swarm","spec_digest":"1111111111111111111111111111111111111111111111111111111111111111"},{"docker_service_id":null,"existed":false,"image":null,"name":"vp-youtube-publisher-swarm","spec_digest":null}]},"failed_forward":{"captured":true,"services":[{"docker_service_id":"aaaaaaaaaaaaaaaaaaaaaaaa","existed":true,"image":"vp-api:deploy-222222222222","name":"vp-api-swarm","spec_digest":"2222222222222222222222222222222222222222222222222222222222222222"},{"docker_service_id":"bbbbbbbbbbbbbbbbbbbbbbbb","existed":true,"image":"vp-worker:deploy-222222222222","name":"vp-youtube-publisher-swarm","spec_digest":"3333333333333333333333333333333333333333333333333333333333333333"}]},"forward":{"namespace":"2222222222222222222222222222222222222222","workers":[{"admission_secret":{"docker_secret_id":"dddddddddddddddddddddddd","name":"go-admission","purpose":"admission"},"applied_stage":"prepared","database_secret":{"docker_secret_id":"cccccccccccccccccccccccc","name":"go-database","purpose":"database"},"docker_service_id":null,"generation":301,"service":"vp-ffmpeg-worker-go-swarm","target_spec_digest":null},{"admission_secret":{"docker_secret_id":"ffffffffffffffffffffffff","name":"publisher-admission","purpose":"admission"},"applied_stage":"applied","database_secret":{"docker_secret_id":"eeeeeeeeeeeeeeeeeeeeeeee","name":"publisher-database","purpose":"database"},"docker_service_id":"bbbbbbbbbbbbbbbbbbbbbbbb","generation":304,"service":"vp-youtube-publisher-swarm","target_spec_digest":"3333333333333333333333333333333333333333333333333333333333333333"}]},"phase":"ROLLBACK_APPLYING","rollback":{"marker_generation":"m-rb-222222222222-1","namespace":"rollback-123456789012345678"},"schema":3,"target_commit":"2222222222222222222222222222222222222222"}
JSON
  python3 - "$recovery_state" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
generation = state["target_commit"]
state["transaction_id"] = "tx-22222222222222222222222222222222"
state["app_progress"] = {
    "schema": 1,
    "transaction_id": state["transaction_id"],
    "target_commit": generation,
    "attempted_services": [
        "vp-api-swarm",
        "vp-youtube-publisher-swarm",
    ],
    "migration_state": "applied",
}
roles = (
    "control",
    "ffmpeg_go",
    "ffmpeg",
    "vision",
    "youtube_publisher",
    "watcher",
    "readiness",
    "janitor",
    "repair",
)
state["runtime_redis"] = {
    role: {
        "docker_secret_id": f"{index:024x}",
        "runtime_generation": generation,
        "secret_name": f"runtime-{role}-secret",
    }
    for index, role in enumerate(roles, start=1)
}
state["authorities"] = []
state["prepared_secrets"] = []


def control(generation, image, id_offset, digest_character):
    purposes = (
        "operator",
        "orchestrator",
        "staging-janitor",
        "staging-minio-access",
        "staging-minio-secret",
        "worker-minio-access",
        "worker-minio-secret",
    )
    return {
        "generation": generation,
        "image": image,
        "manifest_sha256": digest_character * 64,
        "secrets": [
            {
                "docker_secret_id": f"{id_offset + index:024x}",
                "generation": generation,
                "name": f"{generation}-{purpose}",
                "purpose": purpose,
                "service": "vp-worker-control",
            }
            for index, purpose in enumerate(purposes, start=1)
        ],
    }


baseline_control = control(
    "c-11111111111111111111",
    "vp-ffmpeg-worker-python:deploy-111111111111",
    10,
    "a",
)
forward_control = control(
    "c-22222222222222222222",
    "vp-ffmpeg-worker-python:deploy-222222222222",
    20,
    "b",
)
state["baseline"]["control"] = baseline_control
state["forward"]["control"] = forward_control
state["rollback"]["control"] = baseline_control


def marker(generation, image, id_offset, digest_character):
    database_purposes = ("readiness", "janitor", "repair")
    secrets = [
        {
            "docker_secret_id": f"{id_offset + index:024x}",
            "generation": generation,
            "name": f"vp-wrm-{purpose}-db-{generation}",
            "purpose": f"{purpose}-database",
            "service": "worker-redis-marker-control",
        }
        for index, purpose in enumerate(database_purposes, start=1)
    ]
    for purpose in ("readiness", "janitor"):
        runtime_reference = state["runtime_redis"][purpose]
        secrets.append({
            "docker_secret_id": runtime_reference["docker_secret_id"],
            "generation": runtime_reference["runtime_generation"],
            "name": runtime_reference["secret_name"],
            "purpose": f"{purpose}-redis",
            "service": "vp-worker-redis-runtime",
        })
    return {
        "config_sha256": digest_character * 64,
        "cron_sha256": digest_character.upper().lower() * 64,
        "generation": generation,
        "image": image,
        "secrets": secrets,
    }


forward_marker = marker(
    "m-222222222222-1700000000-0001",
    forward_control["image"],
    30,
    "c",
)
rollback_marker = marker(
    state["rollback"]["marker_generation"],
    baseline_control["image"],
    40,
    "d",
)
state["forward"]["marker"] = forward_marker
state["rollback"]["marker"] = rollback_marker
state["janitor"] = {
    "service": {
        "docker_service_id": "999999999999999999999999",
        "generation": baseline_control["generation"],
        "name": "vp-staging-object-janitor",
        "spec_digest": "9" * 64,
    }
}
state["failed_forward"]["control"] = {
    "config_sha256": "a" * 64,
    "cron_sha256": "b" * 64,
    "generation": forward_control["generation"],
    "image": forward_control["image"],
}
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  VP_PIPELINE_NETWORK=vp-pipeline-net
  VP_PIPELINE_NETWORK_ID=vp-pipeline-network-id
  recovery_marker_state="$ROOT/state/worker-redis-marker-control/transactions/tx-22222222222222222222222222222222/baseline-managed-state"
  mkdir -p "$recovery_marker_state"
  chmod 0700 "$recovery_marker_state"
  printf '%s\n' \
    'GENERATION=m-111111111111-1699999999-0001' \
    'IMAGE=vp-ffmpeg-worker-python:deploy-111111111111' \
    "NETWORK=$VP_PIPELINE_NETWORK" \
    "NETWORK_ID=$VP_PIPELINE_NETWORK_ID" \
    'READINESS_DATABASE_SECRET=vp-wrm-readiness-db-m-111111111111-1699999999-0001' \
    'READINESS_REDIS_SECRET=runtime-readiness-secret' \
    'JANITOR_DATABASE_SECRET=vp-wrm-janitor-db-m-111111111111-1699999999-0001' \
    'JANITOR_REDIS_SECRET=runtime-janitor-secret' \
    >"$recovery_marker_state/control.conf"
  chmod 0600 "$recovery_marker_state/control.conf"
  printf 'VERSION=1\n' >"$recovery_marker_state/captured"
  chmod 0600 "$recovery_marker_state/captured"
  : >"$recovery_marker_state/crontab"
  chmod 0600 "$recovery_marker_state/crontab"
  vp_worker_admission_recovery_state() {
    command cat "$recovery_state"
  }

  vp_worker_admission_hydrate_recovery_context
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_PHASE" != ROLLBACK_APPLYING \
    || "$VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED" != true \
    || "$VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND" != managed \
    || "$VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS" \
      != 'vp-youtube-publisher-swarm|false|-|-|-' \
    || "$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS" \
      != 'vp-api-swarm|aaaaaaaaaaaaaaaaaaaaaaaa|vp-api:deploy-111111111111|1111111111111111111111111111111111111111111111111111111111111111' \
    || "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" \
      != 'vp-api-swarm vp-youtube-publisher-swarm' \
    || "$VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE" != applied \
    || "$VP_BACKEND_MIGRATION_APPLIED" != true \
    || "$VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS" != "$({
      printf '%s\n' \
        'vp-ffmpeg-worker-go-swarm|301|go-database|cccccccccccccccccccccccc|go-admission|dddddddddddddddddddddddd' \
        'vp-youtube-publisher-swarm|304|publisher-database|eeeeeeeeeeeeeeeeeeeeeeee|publisher-admission|ffffffffffffffffffffffff'
    })" \
    || "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES" \
      != 'vp-youtube-publisher-swarm|304|bbbbbbbbbbbbbbbbbbbbbbbb|3333333333333333333333333333333333333333333333333333333333333333' \
    || "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" \
      != 'vp-youtube-publisher-swarm|304|bbbbbbbbbbbbbbbbbbbbbbbb' \
    || "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
      != 2222222222222222222222222222222222222222 \
    || "$VP_WORKER_REDIS_CONTROL_SECRET|$VP_WORKER_REDIS_CONTROL_SECRET_ID" \
      != 'runtime-control-secret|000000000000000000000001' \
    || "$VP_WORKER_REDIS_WATCHER_SECRET|$VP_WORKER_REDIS_WATCHER_SECRET_ID" \
      != 'runtime-watcher-secret|000000000000000000000006' \
    || "$VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET|$VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID" \
      != 'runtime-repair-secret|000000000000000000000009' \
    || "$VP_WORKER_CONTROL_PRIOR_GENERATION|$VP_WORKER_CONTROL_PRIOR_IMAGE|$VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET" \
      != 'c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111|c-11111111111111111111-operator' \
    || "$VP_WORKER_CONTROL_GENERATION|$VP_WORKER_ADMISSION_CONTROL_IMAGE|$VP_WORKER_OPERATOR_DATABASE_SECRET" \
      != 'c-11111111111111111111|vp-ffmpeg-worker-python:deploy-111111111111|c-11111111111111111111-operator' \
    || "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION|$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" \
      != 'c-22222222222222222222|vp-ffmpeg-worker-python:deploy-222222222222' \
    || "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256|$VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256" \
      != "$(printf 'a%.0s' {1..64})|$(printf 'b%.0s' {1..64})" \
    || "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION|$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      != 'm-rb-222222222222-1|vp-ffmpeg-worker-python:deploy-111111111111' \
    || "$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID|$VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID|$VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID" \
      != '000000000000000000000029|00000000000000000000002a|00000000000000000000002b' \
    || "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION|$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
      != 'm-222222222222-1700000000-0001|vp-ffmpeg-worker-python:deploy-222222222222' \
    || "$VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID|$VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID|$VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID" \
      != '00000000000000000000001f|000000000000000000000020|000000000000000000000021' \
    || "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID|$VP_WORKER_ADMISSION_JANITOR_GENERATION|$VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST" \
      != "999999999999999999999999|c-11111111111111111111|$(printf '9%.0s' {1..64})" \
    || "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION|$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
      != 'm-111111111111-1699999999-0001|vp-ffmpeg-worker-python:deploy-111111111111' \
    || "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" != "$recovery_marker_state" \
    || "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
      != rollback-123456789012345678 \
    || "$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE" \
      != 2222222222222222222222222222222222222222 ]]; then
    echo 'FAIL: durable recovery hydration lost baseline or candidate identity' >&2
    exit 1
  fi

  forward_recovery_state="${recovery_state%/*}/forward-applying-state.json"
  cp "$recovery_state" "$forward_recovery_state"
  python3 - "$forward_recovery_state" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
state["phase"] = "FORWARD_APPLYING"
state["failed_forward"] = {
    "captured": False,
    "control": None,
    "services": [],
}
state["app_progress"]["attempted_services"] = [
    "vp-api-swarm",
    "vp-frontend-swarm",
]
state["app_progress"]["migration_state"] = "applying"
state["rollback"] = {
    "control": None,
    "marker": None,
    "marker_generation": None,
    "namespace": None,
}
state["janitor"] = {"service": None}
for worker in state["forward"]["workers"]:
    worker["applied_stage"] = "prepared"
    worker["docker_service_id"] = None
    worker["target_spec_digest"] = None
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  vp_worker_admission_recovery_state() {
    command cat "$forward_recovery_state"
  }
  vp_worker_admission_hydrate_recovery_context
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_PHASE" != FORWARD_APPLYING \
    || "$VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED" != false \
    || "$VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD" != true \
    || "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" \
      != 'vp-api-swarm vp-frontend-swarm' \
    || "$VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE" != applying \
    || "$VP_BACKEND_MIGRATION_APPLIED" != true ]]; then
    echo 'FAIL: uncaptured forward crash lost exact durable app progress' >&2
    exit 1
  fi

  early_forward_state="${recovery_state%/*}/early-forward-applying-state.json"
  cp "$forward_recovery_state" "$early_forward_state"
  python3 - "$early_forward_state" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
state["transaction_id"] = "tx-77777777777777777777777777777777"
state["app_progress"]["transaction_id"] = state["transaction_id"]
state["forward"] = {
    "control": None,
    "marker": None,
    "namespace": state["target_commit"],
    "workers": [],
}
state["janitor"] = {"service": None}
state["authorities"] = []
state["prepared_secrets"] = []
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  vp_worker_admission_recovery_state() {
    command cat "$early_forward_state"
  }
  if ! vp_worker_admission_hydrate_recovery_context; then
    echo 'FAIL: valid early forward crash state was rejected' >&2
    exit 1
  fi
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" != true \
    || "$VP_WORKER_ADMISSION_PREPARED" != false \
    || "$VP_WORKER_CONTROL_PREPARED" != false \
    || "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != false \
    || -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" \
    || "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" \
      != 'vp-api-swarm vp-frontend-swarm' ]]; then
    echo 'FAIL: early forward crash was not hydrated as an abort-only recovery' >&2
    exit 1
  fi

  partial_forward_state="${recovery_state%/*}/partial-forward-applying-state.json"
  cp "$forward_recovery_state" "$partial_forward_state"
  python3 - "$partial_forward_state" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
state["transaction_id"] = "tx-66666666666666666666666666666666"
state["app_progress"]["transaction_id"] = state["transaction_id"]
state["forward"]["control"] = None
state["forward"]["workers"] = []
state["janitor"] = {"service": None}
state["authorities"] = [
    {
        "control_generation": "c-22222222222222222222",
        "control_image": "vp-ffmpeg-worker-python:deploy-222222222222",
        "generation": "m-222222222222-1700000000-0001",
        "kind": "marker",
        "operator_reference": (
            "marker/m-222222222222-1700000000-0001/"
            "worker-marker-owner-database-url"
        ),
        "service": "worker-redis-marker-control",
        "state": "provisioned",
    }
]
state["prepared_secrets"] = []
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  partial_marker_state="$ROOT/state/worker-redis-marker-control/transactions/tx-66666666666666666666666666666666/baseline-managed-state"
  mkdir -p "${partial_marker_state%/*}"
  cp -R "$recovery_marker_state" "$partial_marker_state"
  vp_worker_admission_recovery_state() {
    command cat "$partial_forward_state"
  }
  if ! vp_worker_admission_hydrate_recovery_context; then
    echo 'FAIL: valid partial forward crash state was rejected' >&2
    exit 1
  fi
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" != false \
    || "$VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD" != true \
    || "$VP_WORKER_ADMISSION_PREPARED" != false \
    || "$VP_WORKER_CONTROL_PREPARED" != false \
    || "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != true \
    || "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" != "$partial_marker_state" ]]; then
    echo 'FAIL: partial forward crash was not hydrated as an abort-only recovery' >&2
    exit 1
  fi
  vp_worker_admission_recovery_state() {
    command cat "$recovery_state"
  }

  promotion_recovery_state="${recovery_state%/*}/promotion-state.json"
  for promotion_contract in \
    'FORWARD_VERIFIED|c-22222222222222222222|m-222222222222-1700000000-0001|2222222222222222222222222222222222222222' \
    'WORKERS_PROMOTED|c-22222222222222222222|m-222222222222-1700000000-0001|2222222222222222222222222222222222222222' \
    'MARKER_PROMOTED|c-22222222222222222222|m-222222222222-1700000000-0001|2222222222222222222222222222222222222222' \
    'ROLLBACK_VERIFIED|c-11111111111111111111|m-rb-222222222222-1|rollback-123456789012345678' \
    'ROLLBACK_WORKERS_PROMOTED|c-11111111111111111111|m-rb-222222222222-1|rollback-123456789012345678' \
    'ROLLBACK_MARKER_PROMOTED|c-11111111111111111111|m-rb-222222222222-1|rollback-123456789012345678'; do
    IFS='|' read -r \
      promotion_phase promotion_control promotion_marker \
      promotion_namespace <<<"$promotion_contract"
    cp "$recovery_state" "$promotion_recovery_state"
    python3 - "$promotion_recovery_state" "$promotion_phase" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
state["phase"] = sys.argv[2]
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
    vp_worker_admission_recovery_state() {
      command cat "$promotion_recovery_state"
    }
    hidden_marker_state="${recovery_marker_state}.hidden"
    if [[ "$promotion_phase" == MARKER_PROMOTED ]]; then
      mv "$recovery_marker_state" "$hidden_marker_state"
    fi
    promotion_status=0
    vp_worker_admission_hydrate_recovery_context || promotion_status=$?
    if [[ "$promotion_phase" == MARKER_PROMOTED ]]; then
      mv "$hidden_marker_state" "$recovery_marker_state"
    fi
    if [[ "$promotion_status" -ne 0 \
      || "$VP_WORKER_ADMISSION_RECOVERY_PHASE" != "$promotion_phase" \
      || "$VP_WORKER_CONTROL_GENERATION" != "$promotion_control" \
      || "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
        != "$promotion_marker" \
      || "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
        != "$promotion_namespace" ]]; then
      echo "FAIL: fresh promotion hydration lost $promotion_phase context" >&2
      exit 1
    fi
  done
  vp_worker_admission_recovery_state() {
    command cat "$recovery_state"
  }

  python3 - "$recovery_state" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
state = json.loads(path.read_text(encoding="utf-8"))
state["rollback"]["marker"] = None
path.write_text(
    json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  vp_worker_admission_hydrate_recovery_context
  if [[ "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION|$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
    != 'm-222222222222-1700000000-0001|vp-ffmpeg-worker-python:deploy-222222222222' ]]; then
    echo 'FAIL: rollback applying without a rollback marker did not retain the forward marker' >&2
    exit 1
  fi
)

(
  vp_worker_admission_lock_assert() {
    :
  }
  VP_WORKER_ADMISSION_RECOVERY_PHASE=ROLLBACK_APPLYING
  VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=true
  VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND=managed
  VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS=stale-baseline
  VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS=stale-snapshots
  VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES=stale-attempts
  VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS=stale-secrets
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES=stale-identities
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS=stale-services
  VP_WORKER_ADMISSION_PREPARED=true
  VP_WORKER_ADMISSION_COMMIT=ffffffffffffffffffffffffffffffffffffffff
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=stale-namespace
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=stale-service
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=stale-forward
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION=stale-generation
  VP_WORKER_REDIS_CONTROL_SECRET=stale-control
  VP_WORKER_REDIS_CONTROL_SECRET_ID=stale-control-id
  VP_WORKER_REDIS_WATCHER_SECRET=stale-watcher
  VP_WORKER_REDIS_WATCHER_SECRET_ID=stale-watcher-id
  VP_WORKER_CONTROL_PRIOR_GENERATION=stale-prior-control
  VP_WORKER_CONTROL_GENERATION=stale-current-control
  VP_WORKER_ADMISSION_CONTROL_IMAGE=stale-control-image
  VP_WORKER_OPERATOR_DATABASE_SECRET=stale-operator
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=stale-failed-control
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256=stale-failed-config
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256=stale-failed-cron
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=stale-marker
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=stale-marker-image
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=stale-marker-readiness
  VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION=stale-failed-marker
  VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE=stale-failed-marker-image
  VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=stale-janitor-id
  VP_WORKER_ADMISSION_JANITOR_GENERATION=stale-janitor-generation
  VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST=stale-janitor-digest
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=stale-prior-marker
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=stale-prior-marker-image
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=stale-managed-state
  vp_worker_admission_recovery_state() {
    printf '%s\n' '{"schema":3.0}'
  }

  if vp_worker_admission_hydrate_recovery_context; then
    echo 'FAIL: recovery hydration accepted a non-integer schema' >&2
    exit 1
  fi
  if [[ -n "$VP_WORKER_ADMISSION_RECOVERY_PHASE" \
    || "$VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED" != false \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES" \
    || -n "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" \
    || "$VP_WORKER_ADMISSION_PREPARED" != false \
    || -n "$VP_WORKER_ADMISSION_COMMIT" \
    || -n "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
    || -n "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE" \
    || -n "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
    || -n "$VP_WORKER_REDIS_CONTROL_SECRET" \
    || -n "$VP_WORKER_REDIS_CONTROL_SECRET_ID" \
    || -n "$VP_WORKER_REDIS_WATCHER_SECRET" \
    || -n "$VP_WORKER_REDIS_WATCHER_SECRET_ID" \
    || -n "$VP_WORKER_CONTROL_PRIOR_GENERATION" \
    || -n "$VP_WORKER_CONTROL_GENERATION" \
    || -n "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
    || -n "$VP_WORKER_OPERATOR_DATABASE_SECRET" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256" \
    || -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
    || -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
    || -n "$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
    || -n "$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
    || -n "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" \
    || -n "$VP_WORKER_ADMISSION_JANITOR_GENERATION" \
    || -n "$VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST" \
    || -n "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    || -n "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    || -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" ]]; then
    echo 'FAIL: rejected recovery state retained stale shell authority' >&2
    exit 1
  fi
)

(
  marker_intent_root="$TEST_ROOT/marker-intent-forward-applying"
  transaction_root="$marker_intent_root/state/vp-worker-admission"
  helper="$ROOT_DIR/deploy/swarm/worker-admission-transaction.py"
  mkdir -p "$transaction_root"
  chmod 0700 "$transaction_root"
  lock_path="$(python3 "$helper" lock-prepare "$transaction_root")"
  exec 18<>"$lock_path"
  python3 "$helper" lock-acquire "$transaction_root" 18 >/dev/null
  credential_arguments=()
  credential_records=""
  principals=(
    vp_deploy_migrator
    vp_deploy_read
    vp_control_role_owner
    vp_runtime_role_owner
  )
  for index in 0 1 2 3; do
    credential="$marker_intent_root/credential-$index"
    printf 'postgresql://marker-intent-%s@database/videoprocess\n' "$index" \
      >"$credential"
    chmod 0400 "$credential"
    credential_arguments+=("$credential" "${principals[$index]}")
  done
  credential_records="$(
    python3 "$helper" validate-credentials "${credential_arguments[@]}"
  )"
  commit=8123456789abcdef0123456789abcdef01234567
  backend_image="vp-backend:deploy-${commit:0:12}"
  go_image="vp-ffmpeg-worker-go:deploy-${commit:0:12}"
  control_image="vp-ffmpeg-worker-python:deploy-${commit:0:12}"
  control_generation="c-${commit:0:20}"
  marker_generation=m-8123456789ab-1700000000-0001
  marker_operator_reference="marker/$marker_generation/worker-marker-owner-database-url"
  python3 "$helper" begin \
    "$transaction_root" 18 \
    "$commit" "$backend_image" "$go_image" "$commit" \
    legacy_no_control <<<"$credential_records" >/dev/null
  baseline="$marker_intent_root/baseline.json"
  python3 - "$baseline" <<'PY'
import json
import pathlib
import sys

services = (
    "vp-api-swarm",
    "vp-frontend-swarm",
    "vp-autoflow-api-swarm",
    "vp-event-outbox-relay-swarm",
    "vp-channel-agent-runner-swarm",
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
)
payload = {
    "control": None,
    "kind": "legacy_no_control",
    "services": [
        {
            "docker_service_id": None,
            "existed": False,
            "image": None,
            "name": service,
            "spec_digest": None,
        }
        for service in services
    ],
}
pathlib.Path(sys.argv[1]).write_text(
    json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  python3 "$helper" capture-baseline \
    "$transaction_root" 18 0 <"$baseline" >/dev/null
  python3 "$helper" transition \
    "$transaction_root" 18 1 FORWARD_APPLYING >/dev/null
  python3 "$helper" record-authority-intent \
    "$transaction_root" 18 \
    marker worker-redis-marker-control "$marker_generation" \
    "$control_image" "$control_generation" \
    "$marker_operator_reference" >/dev/null
  python3 "$helper" mark-authority-provisioning \
    "$transaction_root" 18 \
    marker worker-redis-marker-control "$marker_generation" >/dev/null
  python3 "$helper" mark-authority-provisioned \
    "$transaction_root" 18 \
    marker worker-redis-marker-control "$marker_generation" >/dev/null
  python3 "$helper" record-prepared-secret \
    "$transaction_root" 18 \
    "vp-wrm-readiness-db-$marker_generation" \
    aaaaaaaaaaaaaaaaaaaaaaaa \
    worker-redis-marker-control "$marker_generation" \
    readiness-database >/dev/null
  marker_selection="$marker_intent_root/marker-selection.json"
  python3 - "$marker_selection" "$marker_generation" "$control_image" <<'PY'
import json
import pathlib
import sys

path, generation, image = sys.argv[1:]
payload = {
    "config_sha256": "a" * 64,
    "cron_sha256": "b" * 64,
    "generation": generation,
    "image": image,
    "secrets": [],
}
pathlib.Path(path).write_text(
    json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
PY
  python3 "$helper" record-marker-selection \
    "$transaction_root" 18 6 forward <"$marker_selection" >/dev/null
  python3 "$helper" begin-abort \
    "$transaction_root" 18 7 interrupted_partial_forward >/dev/null
  python3 "$helper" list-abort \
    "$transaction_root" 18 >"$marker_intent_root/abort.json"
  python3 - "$marker_intent_root/abort.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    state = json.load(handle)
authority = state["authorities"][0]
secret = state["prepared_secrets"][0]
if (
    state["phase"] != "ABORTING"
    or state["reason"] != "interrupted_partial_forward"
    or authority["kind"] != "marker"
    or authority["state"] != "provisioned"
    or secret["service"] != "worker-redis-marker-control"
):
    raise SystemExit("marker preparation intent was not recoverable")
PY
)

echo 'worker admission rollback transaction tests passed'
