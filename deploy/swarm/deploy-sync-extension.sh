#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "deploy-sync-extension.sh must be sourced by deploy-github-sync.sh" >&2
  exit 2
fi

: "${REPO_ROOT:?REPO_ROOT must be set by deploy-github-sync.sh}"

VP_WORKER_ADMISSION_TRANSACTION_HELPER="$(
  cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P
)/worker-admission-transaction.py"
VP_WORKER_ADMISSION_LOCK_FD=19
VP_WORKER_ADMISSION_LOCK_HELD=false
VP_WORKER_ADMISSION_LOCK_DEPTH=0
VP_WORKER_ADMISSION_LOCK_ROOT=""
VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID=""
VP_WORKER_ADMISSION_LOCK_TOKEN=""
VP_WORKER_ADMISSION_CURRENT_BASHPID=""
VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
VP_WORKER_ADMISSION_TRANSACTION_ID=""
VP_WORKER_ADMISSION_TRANSACTION_REPLAYED=false
VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME="-"
VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE="-"
VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION="-"
VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST="-"
VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="-"
VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT=""
VP_PYTHON_WORKER_ACTIVE_OPERATION_UID=""
VP_PYTHON_WORKER_ACTIVE_OPERATION_GID=""
VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
VP_PYTHON_WORKER_SIGNAL_STATUS=0
VP_PYTHON_WORKER_SIGNAL_NAME=""
VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS=0
VP_PYTHON_WORKER_PENDING_SIGNAL_NAME=""
VP_PYTHON_WORKER_LAUNCH_GATE_FD=16
VP_PYTHON_WORKER_LAUNCH_GATE_READ_FD=17
VP_PYTHON_WORKER_LAUNCH_GATE_OPEN=false
VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
VP_PYTHON_WORKER_LAUNCH_GATE_PATH=""
VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY=""
VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN=""
VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE=false
VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS=0
VP_WORKER_DATABASE_CREDENTIAL_RECORDS=""
VP_WORKER_PREPARED_SECRET_ID=""
VP_WORKER_ADMISSION_QUERY_READ_FD=14
VP_WORKER_ADMISSION_QUERY_WRITE_FD=15
VP_WORKER_ADMISSION_QUERY_READ_OPEN=false
VP_WORKER_ADMISSION_QUERY_WRITE_OPEN=false
VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE=""
VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY=""
VP_WORKER_ADMISSION_GENERATION_STATE=""
VP_WORKER_ADMISSION_RETIREMENT_IDS=""
VP_WORKER_ADMISSION_HYDRATED_RETIREMENT_RECORDS=""
VP_WORKER_ABORT_REVISION=""
VP_WORKER_ABORT_REASON=""
VP_WORKER_ABORT_OPERATION_ID=""
VP_WORKER_ABORT_SECRET_NAME=""
VP_WORKER_ABORT_SECRET_ID=""
VP_WORKER_ABORT_SECRET_SERVICE=""
VP_WORKER_ABORT_SECRET_GENERATION=""
VP_WORKER_ABORT_SECRET_PURPOSE=""
VP_WORKER_ABORT_AUTHORITY_KIND=""
VP_WORKER_ABORT_AUTHORITY_SERVICE=""
VP_WORKER_ABORT_AUTHORITY_GENERATION=""
VP_WORKER_ABORT_AUTHORITY_STATE=""
VP_WORKER_ABORT_AUTHORITY_CONTROL_IMAGE=""
VP_WORKER_ABORT_AUTHORITY_CONTROL_GENERATION=""
VP_WORKER_ABORT_AUTHORITY_OPERATOR_REFERENCE=""

VP_RUNTIME_HOST="${VP_RUNTIME_HOST:-10.0.0.127}"
VP_RUNTIME_NODE="${VP_RUNTIME_NODE:-colima-127}"
VP_RUNTIME_CONSTRAINT="node.labels.vp.runtime==true"
VP_RUNTIME_NODE_CONSTRAINT="node.hostname==$VP_RUNTIME_NODE"
VP_GPU_CONSTRAINT="node.labels.vp.gpu==true"
VP_MANAGER_NODE="${VP_MANAGER_NODE:-ccttww-lap}"
VP_GPU_MANAGER_CONSTRAINT="node.hostname==$VP_MANAGER_NODE"
VP_PUBLISHER_CONSTRAINT="node.labels.vp.publisher==true"
VP_PUBLISHER_MANAGER_CONSTRAINT="node.hostname==$VP_MANAGER_NODE"
VP_PIPELINE_NETWORK="${VP_PIPELINE_NETWORK:-vp-pipeline-net}"
VP_PIPELINE_NETWORK_ID=""
VP_APP_CI_REPOSITORY="Ctwqk/videoprocess"
VP_APP_CI_WORKFLOW="ci.yml"
VP_PDS_CI_REPOSITORY="Ctwqk/policy-decision-service"
VP_PDS_CI_WORKFLOW="ci.yml"
VP_PDS_SERVICE="vp-pds-swarm"
VP_PDS_HTTP_ADDR=":8080"
VP_PDS_HEALTH_TEST='["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]'
VP_SERVICE_UPDATE_NOT_ATTEMPTED=2
VP_PYTHON_WORKER_SERVICE="vp-ffmpeg-worker-gpu-swarm"
VP_VISION_WORKER_SERVICE="vp-vision-worker-swarm"
VP_PUBLISHER_SERVICE="vp-youtube-publisher-swarm"
VP_APP_SERVICES="vp-api-swarm vp-frontend-swarm vp-autoflow-api-swarm vp-event-outbox-relay-swarm vp-channel-agent-runner-swarm vp-ffmpeg-worker-go-swarm $VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE"
VP_WORKER_REDIS_RUNTIME_ACL_IDENTITY="vp-marker-acl-v1"
VP_WORKER_REDIS_MARKER_CONTROL_SOURCE="${VP_WORKER_REDIS_MARKER_CONTROL_SOURCE:-$REPO_ROOT/videoprocess/deploy/swarm/worker-redis-marker-control.sh}"
VP_STAGING_JANITOR_SOURCE="${VP_STAGING_JANITOR_SOURCE:-$REPO_ROOT/videoprocess/deploy/swarm/staging-object-janitor-run.sh}"
VP_APP_ATTEMPTED_SERVICES=""
VP_BACKEND_MIGRATION_APPLIED=false
VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE=""
VP_VISION_CUTOVER_REQUIRED=false
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
VP_WORKER_REDIS_MARKER_CANDIDATE_CONFIG_SHA256=""
VP_WORKER_REDIS_MARKER_CANDIDATE_CRON_SHA256=""
VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=""
VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=""
VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=""
VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION=""
VP_WORKER_REDIS_CONTROL_SECRET=""
VP_WORKER_REDIS_FFMPEG_GO_SECRET=""
VP_WORKER_REDIS_FFMPEG_SECRET=""
VP_WORKER_REDIS_VISION_SECRET=""
VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=""
VP_WORKER_REDIS_WATCHER_SECRET=""
VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET=""
VP_WORKER_REDIS_CONTROL_SECRET_ID=""
VP_WORKER_REDIS_FFMPEG_GO_SECRET_ID=""
VP_WORKER_REDIS_FFMPEG_SECRET_ID=""
VP_WORKER_REDIS_VISION_SECRET_ID=""
VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET_ID=""
VP_WORKER_REDIS_WATCHER_SECRET_ID=""
VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID=""
VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID=""
VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID=""
VP_VISION_CUTOVER_JOB_SERVICE_ID=""
VP_WORKER_ADMISSION_PREPARED=false
VP_WORKER_ADMISSION_COMMIT=""
VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=""
VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
VP_WORKER_FFMPEG_GO_GENERATION=""
VP_WORKER_FFMPEG_GENERATION=""
VP_WORKER_VISION_GENERATION=""
VP_WORKER_YOUTUBE_PUBLISHER_GENERATION=""
VP_WORKER_FFMPEG_GO_DATABASE_SECRET=""
VP_WORKER_FFMPEG_GO_ADMISSION_SECRET=""
VP_WORKER_FFMPEG_DATABASE_SECRET=""
VP_WORKER_FFMPEG_ADMISSION_SECRET=""
VP_WORKER_VISION_DATABASE_SECRET=""
VP_WORKER_VISION_ADMISSION_SECRET=""
VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET=""
VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET=""
VP_WORKER_CONTROL_GENERATION=""
VP_WORKER_OPERATOR_DATABASE_SECRET=""
VP_WORKER_ORCHESTRATOR_DATABASE_SECRET=""
VP_STAGING_JANITOR_DATABASE_SECRET=""
VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=""
VP_STAGING_JANITOR_MINIO_SECRET_SECRET=""
VP_WORKER_MINIO_ACCESS_SECRET=""
VP_WORKER_MINIO_SECRET_SECRET=""
VP_WORKER_ADMISSION_CONTROL_IMAGE=""
VP_WORKER_ADMISSION_COMMITTED=false
VP_WORKER_CONTROL_PREPARED=false
VP_WORKER_CONTROL_PRIOR_GENERATION=""
VP_WORKER_CONTROL_PRIOR_IMAGE=""
VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=""
VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=""
VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=""
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=""
VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=""
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=""
VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=""
VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=false
VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION=""
VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256=""
VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION=""
VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE=""
VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256=""
VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256=""
VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID=""
VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID=""
VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID=""
VP_WORKER_ADMISSION_RECOVERY_PHASE=""
VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=false
VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=false
VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=false
VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND=""
VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS=""
VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS=""
VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES=""
VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS=""
VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES=""
VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS=""
VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=""
VP_WORKER_ADMISSION_JANITOR_GENERATION=""
VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST=""

vp_validate_topology() {
  if [[ "${BUILD_IMAGES:-1}" -eq 0 && "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi

  if [[ "$VP_RUNTIME_HOST" != "10.0.0.127" ]] \
    || [[ "$VP_RUNTIME_NODE" != "colima-127" ]] \
    || [[ "$VP_MANAGER_NODE" != "ccttww-lap" ]]; then
    echo "VideoProcess deployment topology must remain fixed to 127 and 150" >&2
    return 1
  fi
}

vp_require_pipeline_network_identity() {
  [[ "$VP_PIPELINE_NETWORK" == vp-pipeline-net ]] || return 1
  local identity
  identity="$(
    docker network inspect "$VP_PIPELINE_NETWORK" \
      --format '{{.ID}}|{{.Name}}|{{.Driver}}|{{.Scope}}'
  )" || {
    echo "required pipeline network is absent" >&2
    return 1
  }
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local network_id
  local network_name
  local network_driver
  local network_scope
  local extra
  IFS='|' read -r \
    network_id network_name network_driver network_scope extra \
    <<<"$identity"
  if [[ -n "$extra" \
    || ! "$network_id" =~ ^[A-Za-z0-9._:-]+$ \
    || "$network_name" != "$VP_PIPELINE_NETWORK" \
    || "$network_driver" != overlay \
    || "$network_scope" != swarm \
    || ( -n "$VP_PIPELINE_NETWORK_ID" \
      && "$VP_PIPELINE_NETWORK_ID" != "$network_id" ) ]]; then
    echo "pipeline network identity is invalid" >&2
    return 1
  fi
  VP_PIPELINE_NETWORK_ID="$network_id"
}

vp_validate_worker_database_identities() {
  VP_WORKER_DATABASE_CREDENTIAL_RECORDS=""
  local credential_records
  credential_records="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      validate-credentials \
      "${VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE:-}" \
      "${VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL:-}" \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "${VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL:-}" \
      "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "${VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL:-}" \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL:-}" \
      2>/dev/null
  )" || {
      echo "worker database credential identity validation failed" >&2
      return 1
    }
  [[ -n "$credential_records" \
    && "$credential_records" != *$'\n'* ]] || return 1
  VP_WORKER_DATABASE_CREDENTIAL_RECORDS="$credential_records"
}

vp_verify_worker_database_credential_record() {
  local purpose="$1"
  local credential_file="$2"
  local expected_principal="$3"
  [[ -n "$VP_WORKER_DATABASE_CREDENTIAL_RECORDS" ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    verify-credential-record \
    "$purpose" "$credential_file" "$expected_principal" \
    <<<"$VP_WORKER_DATABASE_CREDENTIAL_RECORDS"
}

vp_probe_worker_database_principal() {
  local image="$1"
  local credential_file="$2"
  local expected_principal="$3"
  local purpose="$4"
  local principal_pattern='^[A-Za-z_][A-Za-z0-9_.$@-]{0,127}$'
  [[ "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$credential_file" = /* \
    && "$expected_principal" =~ $principal_pattern ]] || {
    echo "database_principal_probe_failed" >&2
    return 1
  }
  credential_file="$(
    vp_verify_worker_database_credential_record \
      "$purpose" "$credential_file" "$expected_principal" \
      2>/dev/null
  )" || {
    echo "database_principal_probe_failed" >&2
    return 1
  }
  local caller_uid
  local caller_gid
  caller_uid="$(id -u)" || return 1
  caller_gid="$(id -g)" || return 1
  local probe_output
  local probe_status=0
  if ! probe_output="$(
      docker run --rm \
      --user "$caller_uid:$caller_gid" \
      --read-only \
      --cap-drop ALL \
      --security-opt no-new-privileges \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --mount "type=bind,src=$credential_file,dst=/run/secrets/vp-database-identity-url,readonly" \
      --env VP_DATABASE_IDENTITY_URL_FILE=/run/secrets/vp-database-identity-url \
      --entrypoint /opt/venv/bin/python3 \
      "$image" \
      -I -c '
import asyncio
import json
import os
import stat

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def probe() -> None:
    descriptor = -1
    engine = None
    try:
        path = os.environ.get("VP_DATABASE_IDENTITY_URL_FILE", "")
        if path != "/run/secrets/vp-database-identity-url":
            raise RuntimeError
        before = os.lstat(path)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o400
        ):
            raise RuntimeError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o400
            or (before.st_dev, before.st_ino)
            != (opened.st_dev, opened.st_ino)
        ):
            raise RuntimeError
        payload = os.read(descriptor, 65537)
        if not payload or len(payload) > 65536:
            raise RuntimeError
        database_url = payload.decode("utf-8").strip()
        if not database_url:
            raise RuntimeError
        engine = create_async_engine(database_url)
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT session_user::text, "
                        "current_user::text"
                    )
                )
            ).one()
        print(
            json.dumps(
                {
                    "current_user": row[1],
                    "session_user": row[0],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    except Exception:
        raise SystemExit(1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if engine is not None:
            await engine.dispose()


asyncio.run(probe())
' 2>/dev/null
    )"; then
    probe_status=1
  fi
  local identity_status=0
  vp_verify_worker_database_credential_record \
    "$purpose" "$credential_file" "$expected_principal" \
    >/dev/null 2>&1 || identity_status=1
  if [[ "$probe_status" -ne 0 || "$identity_status" -ne 0 ]]; then
    echo "database_principal_probe_failed" >&2
    return 1
  fi
  if ! printf '%s\n' "$probe_output" \
    | python3 -I -c '
import json
import re
import sys

expected = sys.argv[1]
raw = sys.stdin.buffer.read()
try:
    pairs = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=lambda value: (
            dict(value)
            if len(value) == len(dict(value))
            else (_ for _ in ()).throw(ValueError())
        ),
    )
    canonical = (
        json.dumps(pairs, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    if (
        raw != canonical
        or set(pairs) != {"current_user", "session_user"}
        or not all(isinstance(value, str) for value in pairs.values())
        or any(
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.$@-]{0,127}", value)
            is None
            for value in pairs.values()
        )
        or pairs["current_user"] != expected
        or pairs["session_user"] != expected
    ):
        raise ValueError
except Exception:
    raise SystemExit(1)
' "$expected_principal" >/dev/null 2>&1; then
    echo "database_principal_probe_failed" >&2
    return 1
  fi
}

vp_validate_deploy_config() {
  local principal_probe_image="${1:-}"
  vp_validate_topology || return 1
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  vp_require_pipeline_network_identity || return 1

  local missing=""
  [[ -n "${VP_API_DATABASE_URL_GO:-}" ]] || missing="$missing VP_API_DATABASE_URL_GO"
  [[ -n "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]] || missing="$missing VP_PYTHON_WORKER_DATABASE_URL"
  [[ -n "${VP_MINIO_ACCESS_KEY:-}" ]] || missing="$missing VP_MINIO_ACCESS_KEY"
  [[ -n "${VP_MINIO_SECRET_KEY:-}" ]] || missing="$missing VP_MINIO_SECRET_KEY"
  [[ -n "${VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL:-}" ]] \
    || missing="$missing VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL"
  [[ -n "${VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL:-}" ]] \
    || missing="$missing VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL"
  [[ -n "${VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL:-}" ]] \
    || missing="$missing VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL"
  [[ -n "${VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL:-}" ]] \
    || missing="$missing VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL"
  if [[ -n "$missing" ]]; then
    echo "missing required VideoProcess deploy settings:$missing" >&2
    return 1
  fi
  if [[ ! "$principal_probe_image" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]]; then
    echo "database_principal_probe_failed" >&2
    return 1
  fi
  vp_validate_worker_database_identities || return 1
  vp_probe_worker_database_principal \
    "$principal_probe_image" \
    "$VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE" \
    "$VP_WORKER_DEPLOY_MIGRATOR_EXPECTED_PRINCIPAL" \
    deploy_migrator || return 1
  vp_probe_worker_database_principal \
    "$principal_probe_image" \
    "$VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE" \
    "$VP_WORKER_DEPLOY_READ_EXPECTED_PRINCIPAL" \
    deploy_read || return 1
  vp_probe_worker_database_principal \
    "$principal_probe_image" \
    "$VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE" \
    "$VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL" \
    control_role_owner || return 1
  vp_probe_worker_database_principal \
    "$principal_probe_image" \
    "$VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE" \
    "$VP_WORKER_RUNTIME_ROLE_OWNER_EXPECTED_PRINCIPAL" \
    runtime_role_owner || return 1
  vp_worker_redis_marker_owner_file >/dev/null || return 1
}

vp_worker_service_contract() {
  local service="$1"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
        ffmpeg_go "$VP_RUNTIME_NODE" media_cpu \
        vp:tasks:ffmpeg_go ffmpeg_go-workers \
        "$VP_WORKER_FFMPEG_GO_GENERATION" \
        "$VP_WORKER_FFMPEG_GO_DATABASE_SECRET" \
        "$VP_WORKER_FFMPEG_GO_ADMISSION_SECRET"
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
        ffmpeg 150-gpu media_gpu \
        vp:tasks:ffmpeg ffmpeg-workers \
        "$VP_WORKER_FFMPEG_GENERATION" \
        "$VP_WORKER_FFMPEG_DATABASE_SECRET" \
        "$VP_WORKER_FFMPEG_ADMISSION_SECRET"
      ;;
    "$VP_VISION_WORKER_SERVICE")
      printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
        vision 150-vision vision_gpu \
        vp:tasks:vision vision-workers \
        "$VP_WORKER_VISION_GENERATION" \
        "$VP_WORKER_VISION_DATABASE_SECRET" \
        "$VP_WORKER_VISION_ADMISSION_SECRET"
      ;;
    "$VP_PUBLISHER_SERVICE")
      printf '%s|%s|%s|%s|%s|%s|%s|%s\n' \
        youtube_publisher 150-publisher youtube_publisher \
        vp:tasks:youtube_publisher youtube_publisher-workers \
        "$VP_WORKER_YOUTUBE_PUBLISHER_GENERATION" \
        "$VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET" \
        "$VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET"
      ;;
    *)
      return 1
      ;;
  esac
}

vp_worker_service_redis_secret() {
  case "$1" in
    vp-ffmpeg-worker-go-swarm)
      printf '%s\n' "$VP_WORKER_REDIS_FFMPEG_GO_SECRET"
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      printf '%s\n' "$VP_WORKER_REDIS_FFMPEG_SECRET"
      ;;
    "$VP_VISION_WORKER_SERVICE")
      printf '%s\n' "$VP_WORKER_REDIS_VISION_SECRET"
      ;;
    "$VP_PUBLISHER_SERVICE")
      printf '%s\n' "$VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET"
      ;;
    *)
      return 1
      ;;
  esac
}

vp_worker_service_registration_env() {
  local service="$1"
  local image="$2"
  local worker_type
  local worker_host
  local capabilities
  local redis_stream
  local redis_group
  local generation
  local database_secret
  local admission_secret
  local extra
  IFS='|' read -r \
    worker_type worker_host capabilities redis_stream redis_group \
    generation database_secret admission_secret extra \
    <<<"$(vp_worker_service_contract "$service")" || return 1
  if [[ -n "$extra" || ! "$generation" =~ ^[1-9][0-9]*$ \
    || ! "$VP_WORKER_ADMISSION_COMMIT" =~ ^[0-9a-f]{40}$ \
    || "$image" != *":deploy-${VP_WORKER_ADMISSION_COMMIT:0:12}" ]]; then
    return 1
  fi
  printf '%s\n' \
    "DEPLOY_MODE=production" \
    "WORKER_SERVICE_NAME=$service" \
    "WORKER_ADMISSION_GENERATION=$generation" \
    "WORKER_SLOT=1" \
    "WORKER_TYPE=$worker_type" \
    "WORKER_HOST=$worker_host" \
    "WORKER_CAPABILITIES=$capabilities" \
    "WORKER_RELEASE_COMMIT=$VP_WORKER_ADMISSION_COMMIT" \
    "WORKER_IMAGE_IDENTITY=$image" \
    "WORKER_REDIS_STREAM=$redis_stream" \
    "WORKER_REDIS_GROUP=$redis_group" \
    "WORKER_DATABASE_URL_FILE=/run/secrets/vp-worker-database-url" \
    "WORKER_ADMISSION_TOKEN_FILE=/run/secrets/vp-worker-admission-token" \
    "WORKER_REDIS_URL_FILE=/run/secrets/vp-worker-redis-url" \
    "WORKER_MINIO_ACCESS_KEY_FILE=/run/secrets/vp-worker-minio-access-key" \
    "WORKER_MINIO_SECRET_KEY_FILE=/run/secrets/vp-worker-minio-secret-key" \
    "VP_REQUIRE_STAGING_JANITOR=true"
}

vp_worker_service_secret_specs() {
  local service="$1"
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local worker_type
  local worker_host
  local capabilities
  local redis_stream
  local redis_group
  local generation
  local database_secret
  local admission_secret
  local extra
  IFS='|' read -r \
    worker_type worker_host capabilities redis_stream redis_group \
    generation database_secret admission_secret extra <<<"$contract"
  local redis_secret
  redis_secret="$(vp_worker_service_redis_secret "$service")" || return 1
  if [[ -n "$extra" || -z "$database_secret" || -z "$admission_secret" \
    || -z "$redis_secret" || -z "$VP_WORKER_MINIO_ACCESS_SECRET" \
    || -z "$VP_WORKER_MINIO_SECRET_SECRET" ]]; then
    return 1
  fi
  printf '%s\n' \
    "source=$database_secret,target=vp-worker-database-url,uid=10001,gid=10001,mode=0400" \
    "source=$admission_secret,target=vp-worker-admission-token,uid=10001,gid=10001,mode=0400" \
    "source=$redis_secret,target=vp-worker-redis-url,uid=10001,gid=10001,mode=0400" \
    "source=$VP_WORKER_MINIO_ACCESS_SECRET,target=vp-worker-minio-access-key,uid=10001,gid=10001,mode=0400" \
    "source=$VP_WORKER_MINIO_SECRET_SECRET,target=vp-worker-minio-secret-key,uid=10001,gid=10001,mode=0400"
}

vp_python_worker_host_guard() {
  python3 - "$@" <<'PY'
import json
import os
import re
import secrets
import stat
import struct
import sys
from pathlib import Path


def fail() -> None:
    raise RuntimeError("python worker host bind validation failed")


def numeric(value: str) -> int:
    if not value.isascii() or not value.isdigit():
        fail()
    parsed = int(value)
    if parsed < 0 or parsed > 2**31 - 1:
        fail()
    return parsed


def exact_path(raw: str) -> Path:
    path = Path(raw)
    if (
        not path.is_absolute()
        or "\n" in raw
        or "\r" in raw
        or "\t" in raw
        or str(path.resolve(strict=True)) != raw
    ):
        fail()
    return path


def identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def directory_flags() -> int:
    return (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )


def file_flags() -> int:
    return os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)


def require_directory(
    metadata: os.stat_result,
    uid: int,
    gid: int,
) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        fail()


def require_file(
    metadata: os.stat_result,
    uid: int,
    gid: int,
    mode: int,
) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_nlink != 1
    ):
        fail()


def open_exact_directory(
    path: Path,
    uid: int,
    gid: int,
) -> tuple[int, os.stat_result]:
    before = path.lstat()
    require_directory(before, uid, gid)
    descriptor = os.open(path, directory_flags())
    try:
        opened = os.fstat(descriptor)
        require_directory(opened, uid, gid)
        if identity(before) != identity(opened):
            fail()
        after = path.lstat()
        if identity(opened) != identity(after):
            fail()
    except Exception:
        os.close(descriptor)
        raise
    return descriptor, opened


def capture_file(
    path: Path,
    uid: int,
    gid: int,
    mode: int,
) -> tuple[int, os.stat_result]:
    before = path.lstat()
    require_file(before, uid, gid, mode)
    descriptor = os.open(path, file_flags())
    try:
        opened = os.fstat(descriptor)
        require_file(opened, uid, gid, mode)
        if identity(before) != identity(opened):
            fail()
    except Exception:
        os.close(descriptor)
        raise
    return descriptor, opened


def verify_file_record(
    path: Path,
    uid: int,
    gid: int,
    mode: int,
    record: str,
) -> None:
    descriptor, opened = capture_file(path, uid, gid, mode)
    try:
        if record != ",".join(str(value) for value in identity(opened)):
            fail()
        after = path.lstat()
        if identity(opened) != identity(after):
            fail()
    finally:
        os.close(descriptor)


def scan_tree(
    descriptor: int,
    root_device: int,
    uid: int,
    gid: int,
) -> None:
    for name in sorted(os.listdir(descriptor)):
        metadata = os.stat(
            name,
            dir_fd=descriptor,
            follow_symlinks=False,
        )
        if metadata.st_dev != root_device:
            fail()
        if stat.S_ISDIR(metadata.st_mode):
            require_directory(metadata, uid, gid)
            child = os.open(name, directory_flags(), dir_fd=descriptor)
            try:
                opened = os.fstat(child)
                require_directory(opened, uid, gid)
                if identity(metadata) != identity(opened):
                    fail()
                scan_tree(child, root_device, uid, gid)
                after = os.stat(
                    name,
                    dir_fd=descriptor,
                    follow_symlinks=False,
                )
                if identity(opened) != identity(after):
                    fail()
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            mode = stat.S_IMODE(metadata.st_mode)
            if mode not in {0o400, 0o600}:
                fail()
            child = os.open(name, file_flags(), dir_fd=descriptor)
            try:
                opened = os.fstat(child)
                require_file(opened, uid, gid, mode)
                if identity(metadata) != identity(opened):
                    fail()
                after = os.stat(
                    name,
                    dir_fd=descriptor,
                    follow_symlinks=False,
                )
                if identity(opened) != identity(after):
                    fail()
            finally:
                os.close(child)
        else:
            fail()


def prepare_controlled_directory(
    anchor_raw: str,
    target_raw: str,
    uid: int,
    gid: int,
) -> None:
    if (
        not anchor_raw.startswith("/")
        or anchor_raw == "/"
        or len(anchor_raw) > 4096
        or os.path.normpath(anchor_raw) != anchor_raw
        or any(character in anchor_raw for character in "\n\r\t")
    ):
        fail()
    anchor_components = [
        component
        for component in anchor_raw.split("/")
        if component
    ]
    if any(component in {".", ".."} for component in anchor_components):
        fail()

    lexical_anchor_raw = anchor_raw
    lexical_anchor = Path(anchor_raw)
    lexical_current = Path("/")
    caller_owned_ancestor = False
    for index, component in enumerate(anchor_components):
        lexical_current /= component
        try:
            lexical_metadata = lexical_current.lstat()
        except FileNotFoundError:
            break
        if stat.S_ISLNK(lexical_metadata.st_mode):
            parent_metadata = lexical_current.parent.stat()
            if (
                index == len(anchor_components) - 1
                or caller_owned_ancestor
                or lexical_metadata.st_uid != 0
                or parent_metadata.st_uid != 0
                or stat.S_IMODE(parent_metadata.st_mode) & 0o022
            ):
                fail()
        elif not stat.S_ISDIR(lexical_metadata.st_mode):
            fail()
        elif uid != 0 and lexical_metadata.st_uid == uid:
            caller_owned_ancestor = True
    try:
        anchor_path = lexical_anchor.resolve(strict=False)
    except (OSError, RuntimeError):
        fail()
    anchor_raw = str(anchor_path)
    if (
        not anchor_raw.startswith("/")
        or anchor_raw == "/"
        or os.path.normpath(anchor_raw) != anchor_raw
    ):
        fail()
    anchor_components = [
        component
        for component in anchor_raw.split("/")
        if component
    ]
    if target_raw.startswith(lexical_anchor_raw + "/"):
        relative = target_raw[len(lexical_anchor_raw) + 1 :]
    elif target_raw.startswith(anchor_raw + "/"):
        relative = target_raw[len(anchor_raw) + 1 :]
    else:
        fail()
    if (
        not relative
        or len(relative) > 1024
        or relative.startswith("/")
        or relative.endswith("/")
    ):
        fail()
    components = relative.split("/")
    if any(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", component)
        is None
        for component in components
    ):
        fail()

    current = os.open("/", directory_flags())
    try:
        for component in anchor_components:
            created = False
            try:
                metadata = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                parent = os.fstat(current)
                if (
                    not stat.S_ISDIR(parent.st_mode)
                    or parent.st_uid != uid
                    or parent.st_gid != gid
                    or stat.S_IMODE(parent.st_mode) & 0o022
                ):
                    fail()
                os.mkdir(component, mode=0o700, dir_fd=current)
                os.fsync(current)
                metadata = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
                created = True
            if not stat.S_ISDIR(metadata.st_mode):
                fail()
            child = os.open(
                component,
                directory_flags(),
                dir_fd=current,
            )
            try:
                opened = os.fstat(child)
                if identity(metadata) != identity(opened):
                    fail()
                if created:
                    require_directory(opened, uid, gid)
                after = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
                if identity(opened) != identity(after):
                    fail()
            except Exception:
                os.close(child)
                raise
            os.close(current)
            current = child

        opened_anchor = os.fstat(current)
        if (
            not stat.S_ISDIR(opened_anchor.st_mode)
            or opened_anchor.st_uid != uid
            or opened_anchor.st_gid != gid
            or stat.S_IMODE(opened_anchor.st_mode) & 0o022
        ):
            fail()
        for index, component in enumerate(components):
            created = False
            try:
                metadata = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                os.mkdir(component, mode=0o700, dir_fd=current)
                os.fsync(current)
                metadata = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
                created = True
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != uid
                or metadata.st_gid != gid
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                fail()
            child = os.open(
                component,
                directory_flags(),
                dir_fd=current,
            )
            try:
                opened = os.fstat(child)
                if identity(metadata) != identity(opened):
                    fail()
                if index == len(components) - 1:
                    os.fchmod(child, 0o700)
                    os.fsync(child)
                    opened = os.fstat(child)
                    require_directory(opened, uid, gid)
                elif created:
                    require_directory(opened, uid, gid)
                after = os.stat(
                    component,
                    dir_fd=current,
                    follow_symlinks=False,
                )
                if identity(opened) != identity(after):
                    fail()
            except Exception:
                os.close(child)
                raise
            os.close(current)
            current = child

        current_anchor = anchor_path.lstat()
        if (
            identity(opened_anchor)[:5]
            != identity(current_anchor)[:5]
        ):
            fail()
        target = anchor_path.joinpath(*components)
        target_metadata = os.fstat(current)
        require_directory(target_metadata, uid, gid)
        print(
            f"{target}|"
            + ",".join(str(value) for value in identity(target_metadata))
        )
    finally:
        os.close(current)


def write_all(descriptor: int, payload: bytes | bytearray) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            fail()
        view = view[written:]


def read_limited(descriptor: int, limit: int) -> bytes:
    payload = bytearray()
    while True:
        chunk = os.read(descriptor, 65536)
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > limit:
            fail()


def metadata_record(metadata: os.stat_result) -> str:
    return ",".join(str(value) for value in identity(metadata))


def capture_file_record(
    path: Path,
    uid: int,
    gid: int,
    mode: int,
) -> None:
    descriptor, metadata = capture_file(path, uid, gid, mode)
    try:
        if identity(metadata) != identity(path.lstat()):
            fail()
        print(metadata_record(metadata))
    finally:
        os.close(descriptor)


def stream_payloads(
    uid: int,
    gid: int,
    arguments: list[str],
) -> None:
    if not arguments or len(arguments) % 3 or len(arguments) > 6:
        fail()
    payloads: list[bytes] = []
    for index in range(0, len(arguments), 3):
        path = exact_path(arguments[index])
        mode = int(arguments[index + 1], 8)
        record = arguments[index + 2]
        if mode not in {0o400, 0o600}:
            fail()
        descriptor, metadata = capture_file(path, uid, gid, mode)
        try:
            if metadata_record(metadata) != record:
                fail()
            payload = read_limited(descriptor, 1048576)
            if identity(metadata) != identity(os.fstat(descriptor)):
                fail()
            if identity(metadata) != identity(path.lstat()):
                fail()
            payloads.append(payload)
        finally:
            os.close(descriptor)
    output = sys.stdout.buffer
    output.write(b"VPW1")
    output.write(bytes([len(payloads)]))
    for payload in payloads:
        output.write(struct.pack(">Q", len(payload)))
        output.write(payload)
    output.flush()


def remove_legacy_sentinels(
    descriptor: int,
    uid: int,
    gid: int,
) -> None:
    for name in sorted(os.listdir(descriptor)):
        matched = re.fullmatch(
            r"\.vp-python-worker-bind-([0-9a-f]{32})",
            name,
        )
        if matched is None:
            continue
        metadata = os.stat(
            name,
            dir_fd=descriptor,
            follow_symlinks=False,
        )
        require_file(metadata, uid, gid, 0o400)
        sentinel = os.open(
            name,
            file_flags(),
            dir_fd=descriptor,
        )
        try:
            opened = os.fstat(sentinel)
            require_file(opened, uid, gid, 0o400)
            if identity(metadata) != identity(opened):
                fail()
            payload = read_limited(sentinel, 1024)
            expected = (
                f"vp-python-worker-bind-v1:{matched.group(1)}:"
                f"{uid}:{gid}"
            ).encode("ascii")
            if payload != expected:
                fail()
        finally:
            os.close(sentinel)
        os.unlink(name, dir_fd=descriptor)
        os.fsync(descriptor)


def operation_root(
    admission_descriptor: int,
    uid: int,
    gid: int,
) -> int:
    name = "one-shot-operations"
    try:
        os.mkdir(name, mode=0o700, dir_fd=admission_descriptor)
        os.fsync(admission_descriptor)
    except FileExistsError:
        pass
    metadata = os.stat(
        name,
        dir_fd=admission_descriptor,
        follow_symlinks=False,
    )
    require_directory(metadata, uid, gid)
    descriptor = os.open(
        name,
        directory_flags(),
        dir_fd=admission_descriptor,
    )
    opened = os.fstat(descriptor)
    require_directory(opened, uid, gid)
    if identity(metadata) != identity(opened):
        os.close(descriptor)
        fail()
    return descriptor


def reconcile_legacy_runs(
    admission_descriptor: int,
    uid: int,
    gid: int,
) -> None:
    name = "one-shot-runs"
    try:
        metadata = os.stat(
            name,
            dir_fd=admission_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    require_directory(metadata, uid, gid)
    runs = os.open(
        name,
        directory_flags(),
        dir_fd=admission_descriptor,
    )
    try:
        if identity(metadata) != identity(os.fstat(runs)):
            fail()
        for run_name in sorted(os.listdir(runs)):
            if re.fullmatch(r"run\.[0-9a-f]{32}", run_name) is None:
                fail()
            run_metadata = os.stat(
                run_name,
                dir_fd=runs,
                follow_symlinks=False,
            )
            require_directory(run_metadata, uid, gid)
            run = os.open(
                run_name,
                directory_flags(),
                dir_fd=runs,
            )
            try:
                if identity(run_metadata) != identity(os.fstat(run)):
                    fail()
                for entry in sorted(os.listdir(run)):
                    if re.fullmatch(
                        r"(bootstrap-secret|bind-file-[0-9]+)",
                        entry,
                    ) is None:
                        fail()
                    entry_metadata = os.stat(
                        entry,
                        dir_fd=run,
                        follow_symlinks=False,
                    )
                    mode = stat.S_IMODE(entry_metadata.st_mode)
                    if mode not in {0o400, 0o600}:
                        fail()
                    require_file(entry_metadata, uid, gid, mode)
                    os.unlink(entry, dir_fd=run)
                os.fsync(run)
            finally:
                os.close(run)
            os.rmdir(run_name, dir_fd=runs)
            os.fsync(runs)
        if os.listdir(runs):
            fail()
    finally:
        os.close(runs)
    os.rmdir(name, dir_fd=admission_descriptor)
    os.fsync(admission_descriptor)


def parse_operation(
    payload: bytes,
    expected_name: str,
    uid: int,
    gid: int,
) -> dict[str, object]:
    try:
        operation = json.loads(payload)
        if (
            not isinstance(operation, dict)
            or set(operation)
            != {"bindings", "gid", "operation_id", "uid", "version"}
            or operation["version"] != 1
            or operation["operation_id"] != expected_name
            or operation["uid"] != uid
            or operation["gid"] != gid
            or not isinstance(operation["bindings"], list)
            or not 1 <= len(operation["bindings"]) <= 3
        ):
            fail()
        seen_paths: set[str] = set()
        seen_targets: set[str] = set()
        for binding in operation["bindings"]:
            if (
                not isinstance(binding, dict)
                or set(binding)
                != {
                    "device",
                    "inode",
                    "marker",
                    "path",
                    "sentinel",
                    "target",
                }
                or not isinstance(binding["device"], int)
                or binding["device"] < 0
                or not isinstance(binding["inode"], int)
                or binding["inode"] < 1
                or binding["target"]
                not in {"/control-state", "/runtime-state", "/requests"}
                or not isinstance(binding["path"], str)
            ):
                fail()
            exact_path(binding["path"])
            sentinel_match = re.fullmatch(
                r"\.vp-python-worker-bind-([0-9a-f]{32})",
                binding["sentinel"],
            )
            if sentinel_match is None:
                fail()
            expected_marker = (
                f"vp-python-worker-bind-v2:{expected_name}:"
                f"{sentinel_match.group(1)}:{uid}:{gid}:"
                f"{binding['target']}"
            )
            if binding["marker"] != expected_marker:
                fail()
            if (
                binding["path"] in seen_paths
                or binding["target"] in seen_targets
            ):
                fail()
            seen_paths.add(binding["path"])
            seen_targets.add(binding["target"])
        return operation
    except (
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ):
        fail()


def read_operation(
    operation_descriptor: int,
    operation_name: str,
    uid: int,
    gid: int,
) -> dict[str, object]:
    entries = sorted(os.listdir(operation_descriptor))
    if entries != ["operation.json"]:
        fail()
    metadata = os.stat(
        "operation.json",
        dir_fd=operation_descriptor,
        follow_symlinks=False,
    )
    require_file(metadata, uid, gid, 0o600)
    descriptor = os.open(
        "operation.json",
        file_flags(),
        dir_fd=operation_descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        require_file(opened, uid, gid, 0o600)
        if identity(metadata) != identity(opened):
            fail()
        payload = read_limited(descriptor, 65536)
    finally:
        os.close(descriptor)
    return parse_operation(payload, operation_name, uid, gid)


def cleanup_operation(
    operations: int,
    operation_name: str,
    uid: int,
    gid: int,
    require_sentinels: bool,
) -> None:
    if re.fullmatch(r"op\.[0-9a-f]{32}", operation_name) is None:
        fail()
    metadata = os.stat(
        operation_name,
        dir_fd=operations,
        follow_symlinks=False,
    )
    require_directory(metadata, uid, gid)
    operation_descriptor = os.open(
        operation_name,
        directory_flags(),
        dir_fd=operations,
    )
    try:
        opened = os.fstat(operation_descriptor)
        require_directory(opened, uid, gid)
        if identity(metadata) != identity(opened):
            fail()
        try:
            operation = read_operation(
                operation_descriptor,
                operation_name,
                uid,
                gid,
            )
        except Exception:
            if require_sentinels:
                raise
            entries = sorted(os.listdir(operation_descriptor))
            if entries == []:
                pass
            elif entries == ["operation.json"]:
                record = os.stat(
                    "operation.json",
                    dir_fd=operation_descriptor,
                    follow_symlinks=False,
                )
                require_file(record, uid, gid, 0o600)
                os.unlink("operation.json", dir_fd=operation_descriptor)
                os.fsync(operation_descriptor)
            else:
                raise
            operation = None
        if operation is not None:
            for binding in operation["bindings"]:
                path = exact_path(binding["path"])
                descriptor, path_metadata = open_exact_directory(
                    path,
                    uid,
                    gid,
                )
                try:
                    if (
                        path_metadata.st_dev != binding["device"]
                        or path_metadata.st_ino != binding["inode"]
                    ):
                        fail()
                    scan_tree(
                        descriptor,
                        path_metadata.st_dev,
                        uid,
                        gid,
                    )
                    try:
                        sentinel_metadata = os.stat(
                            binding["sentinel"],
                            dir_fd=descriptor,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        if require_sentinels:
                            fail()
                        continue
                    require_file(
                        sentinel_metadata,
                        uid,
                        gid,
                        0o400,
                    )
                    sentinel = os.open(
                        binding["sentinel"],
                        file_flags(),
                        dir_fd=descriptor,
                    )
                    try:
                        opened_sentinel = os.fstat(sentinel)
                        require_file(
                            opened_sentinel,
                            uid,
                            gid,
                            0o400,
                        )
                        if identity(sentinel_metadata) != identity(
                            opened_sentinel
                        ):
                            fail()
                        sentinel_payload = read_limited(sentinel, 1024)
                        expected = binding["marker"].encode("ascii")
                        if require_sentinels:
                            if sentinel_payload != expected:
                                fail()
                        elif not expected.startswith(sentinel_payload):
                            fail()
                    finally:
                        os.close(sentinel)
                    os.unlink(
                        binding["sentinel"],
                        dir_fd=descriptor,
                    )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            os.unlink("operation.json", dir_fd=operation_descriptor)
            os.fsync(operation_descriptor)
    finally:
        os.close(operation_descriptor)
    os.rmdir(operation_name, dir_fd=operations)
    os.fsync(operations)


def reconcile_operations(
    admission_path: Path,
    uid: int,
    gid: int,
) -> tuple[int, int]:
    admission, _metadata = open_exact_directory(
        admission_path,
        uid,
        gid,
    )
    try:
        reconcile_legacy_runs(admission, uid, gid)
        operations = operation_root(admission, uid, gid)
    except Exception:
        os.close(admission)
        raise
    for operation_name in sorted(os.listdir(operations)):
        cleanup_operation(
            operations,
            operation_name,
            uid,
            gid,
            False,
        )
    return admission, operations


def prepare_operation(
    admission_path: Path,
    uid: int,
    gid: int,
    arguments: list[str],
) -> None:
    if len(arguments) % 2 or len(arguments) > 6:
        fail()
    admission, operations = reconcile_operations(
        admission_path,
        uid,
        gid,
    )
    binding_descriptors: list[int] = []
    try:
        bindings: list[dict[str, object]] = []
        seen_targets: set[str] = set()
        seen_paths: set[str] = set()
        for index in range(0, len(arguments), 2):
            path = exact_path(arguments[index])
            target = arguments[index + 1]
            if (
                target
                not in {"/control-state", "/runtime-state", "/requests"}
                or str(path) in seen_paths
                or target in seen_targets
            ):
                fail()
            descriptor, metadata = open_exact_directory(path, uid, gid)
            try:
                remove_legacy_sentinels(descriptor, uid, gid)
                scan_tree(descriptor, metadata.st_dev, uid, gid)
            except Exception:
                os.close(descriptor)
                raise
            binding_descriptors.append(descriptor)
            seen_paths.add(str(path))
            seen_targets.add(target)
            bindings.append(
                {
                    "device": metadata.st_dev,
                    "inode": metadata.st_ino,
                    "path": str(path),
                    "target": target,
                }
            )
        if not bindings:
            print("-")
            return

        operation_name = f"op.{secrets.token_hex(16)}"
        os.mkdir(operation_name, mode=0o700, dir_fd=operations)
        operation_descriptor = os.open(
            operation_name,
            directory_flags(),
            dir_fd=operations,
        )
        try:
            require_directory(
                os.fstat(operation_descriptor),
                uid,
                gid,
            )
            for binding in bindings:
                token = secrets.token_hex(16)
                binding["sentinel"] = (
                    f".vp-python-worker-bind-{token}"
                )
                binding["marker"] = (
                    f"vp-python-worker-bind-v2:{operation_name}:"
                    f"{token}:{uid}:{gid}:{binding['target']}"
                )
            operation = {
                "bindings": bindings,
                "gid": gid,
                "operation_id": operation_name,
                "uid": uid,
                "version": 1,
            }
            encoded = json.dumps(
                operation,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
            record = os.open(
                "operation.json",
                (
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0)
                ),
                0o600,
                dir_fd=operation_descriptor,
            )
            try:
                write_all(record, encoded)
                os.fchmod(record, 0o600)
                os.fsync(record)
                require_file(os.fstat(record), uid, gid, 0o600)
            finally:
                os.close(record)
            os.fsync(operation_descriptor)
            os.fsync(operations)

            for binding, descriptor in zip(
                bindings,
                binding_descriptors,
                strict=True,
            ):
                sentinel = os.open(
                    binding["sentinel"],
                    (
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | os.O_CLOEXEC
                        | getattr(os, "O_NOFOLLOW", 0)
                    ),
                    0o400,
                    dir_fd=descriptor,
                )
                try:
                    write_all(
                        sentinel,
                        binding["marker"].encode("ascii"),
                    )
                    os.fchmod(sentinel, 0o400)
                    os.fsync(sentinel)
                    require_file(
                        os.fstat(sentinel),
                        uid,
                        gid,
                        0o400,
                    )
                finally:
                    os.close(sentinel)
                os.fsync(descriptor)
        finally:
            os.close(operation_descriptor)
        print(operation_name)
        for binding in bindings:
            print(
                f"{binding['target']}|{binding['sentinel']}|"
                f"{binding['marker']}"
            )
    finally:
        for descriptor in binding_descriptors:
            os.close(descriptor)
        os.close(operations)
        os.close(admission)


def finish_operation(
    admission_path: Path,
    operation_name: str,
    uid: int,
    gid: int,
) -> None:
    admission, _metadata = open_exact_directory(
        admission_path,
        uid,
        gid,
    )
    try:
        operations = operation_root(admission, uid, gid)
        try:
            cleanup_operation(
                operations,
                operation_name,
                uid,
                gid,
                True,
            )
        finally:
            os.close(operations)
    finally:
        os.close(admission)


try:
    action = sys.argv[1]
    if action == "prepare-controlled-directory" and len(sys.argv) == 6:
        prepare_controlled_directory(
            sys.argv[2],
            sys.argv[3],
            numeric(sys.argv[4]),
            numeric(sys.argv[5]),
        )
    elif action == "capture-file-record" and len(sys.argv) == 6:
        mode = int(sys.argv[5], 8)
        if mode not in {0o400, 0o600}:
            fail()
        capture_file_record(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
            mode,
        )
    elif action == "stream-payloads" and len(sys.argv) >= 7:
        stream_payloads(
            numeric(sys.argv[2]),
            numeric(sys.argv[3]),
            sys.argv[4:],
        )
    elif action == "prepare-operation" and len(sys.argv) >= 5:
        prepare_operation(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
            sys.argv[5:],
        )
    elif action == "finish-operation" and len(sys.argv) == 6:
        finish_operation(
            exact_path(sys.argv[2]),
            sys.argv[3],
            numeric(sys.argv[4]),
            numeric(sys.argv[5]),
        )
    elif action == "verify-file" and len(sys.argv) == 7:
        mode = int(sys.argv[5], 8)
        if mode not in {0o400, 0o600}:
            fail()
        verify_file_record(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
            mode,
            sys.argv[6],
        )
    else:
        fail()
except Exception:
    sys.exit(1)
PY
}

vp_python_worker_prepare_controlled_directory() {
  local target="$1"
  [[ -n "${ROOT:-}" && "$ROOT" = /* && "$target" = /* ]] \
    || return 1
  local caller_uid
  local caller_gid
  caller_uid="$(id -u)" || return 1
  caller_gid="$(id -g)" || return 1
  [[ "$caller_uid" =~ ^[0-9]+$ && "$caller_gid" =~ ^[0-9]+$ ]] \
    || return 1
  local result
  if ! result="$(
    vp_python_worker_host_guard \
      prepare-controlled-directory \
      "$ROOT" "$target" "$caller_uid" "$caller_gid"
  )"; then
    echo 'controlled directory preparation failed: deployment root and existing state path components must be caller-owned and not group/world-writable' >&2
    return 1
  fi
  local path="${result%%|*}"
  local record="${result#*|}"
  [[ "$path" = /* && "$record" != "$result" \
    && "$record" =~ ^[0-9]+,[0-9]+,[0-9]+,[0-9]+,448,[0-9]+,[0-9]+,[0-9]+,[0-9]+$ ]] \
    || return 1
  printf '%s\n' "$path"
}

vp_worker_admission_capture_bashpid() {
  if [[ "${BASHPID:-}" =~ ^[1-9][0-9]*$ ]]; then
    VP_WORKER_ADMISSION_CURRENT_BASHPID="$BASHPID"
  else
    VP_WORKER_ADMISSION_CURRENT_BASHPID="$(
      exec sh -c 'printf "%s\n" "$PPID"'
    )" || return 1
  fi
  [[ "$VP_WORKER_ADMISSION_CURRENT_BASHPID" =~ ^[1-9][0-9]*$ ]]
}

vp_worker_admission_lock_assert() {
  vp_worker_admission_capture_bashpid || return 1
  [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_LOCK_DEPTH" =~ ^[1-9][0-9]*$ \
    && "$VP_WORKER_ADMISSION_LOCK_ROOT" = /* \
    && "$VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID" \
      == "$VP_WORKER_ADMISSION_CURRENT_BASHPID" \
    && "$VP_WORKER_ADMISSION_LOCK_TOKEN" \
      =~ ^[0-9]+:[1-9][0-9]*$ \
    && "$VP_WORKER_ADMISSION_LOCK_FD" -eq 19 ]] || return 1
  local current_token
  current_token="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      lock-token \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" 2>/dev/null
  )" || return 1
  [[ "$current_token" == "$VP_WORKER_ADMISSION_LOCK_TOKEN" ]]
}

vp_worker_admission_lock_drop_inherited() {
  vp_worker_admission_capture_bashpid || return 1
  [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID" \
      != "$VP_WORKER_ADMISSION_CURRENT_BASHPID" ]] \
    || return 1
  local status=0
  { exec 19>&-; } 2>/dev/null || status=1
  VP_WORKER_ADMISSION_LOCK_HELD=false
  VP_WORKER_ADMISSION_LOCK_DEPTH=0
  VP_WORKER_ADMISSION_LOCK_ROOT=""
  VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID=""
  VP_WORKER_ADMISSION_LOCK_TOKEN=""
  VP_WORKER_ADMISSION_CURRENT_BASHPID=""
  return "$status"
}

vp_worker_admission_lock_acquire() {
  local admission_root="$1"
  [[ "$admission_root" = /* \
    && -f "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    && ! -L "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" ]] || return 1

  vp_worker_admission_capture_bashpid || return 1
  if [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID" \
      != "$VP_WORKER_ADMISSION_CURRENT_BASHPID" ]]; then
    vp_worker_admission_lock_drop_inherited || return 1
  fi
  if [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true ]]; then
    [[ "$VP_WORKER_ADMISSION_LOCK_ROOT" == "$admission_root" ]] \
      || return 1
    vp_worker_admission_lock_assert || return 1
    VP_WORKER_ADMISSION_LOCK_DEPTH=$((VP_WORKER_ADMISSION_LOCK_DEPTH + 1))
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_LOCK_DEPTH" -eq 0 \
    && -z "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    && -z "$VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID" \
    && -z "$VP_WORKER_ADMISSION_LOCK_TOKEN" \
    && "$VP_WORKER_ADMISSION_LOCK_FD" -eq 19 ]] || return 1
  if [[ -e /dev/fd/19 ]]; then
    echo "worker admission transaction lock descriptor is unavailable" >&2
    return 1
  fi

  local lock_path
  lock_path="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      lock-prepare "$admission_root" 2>/dev/null
  )" || return 1
  [[ "$lock_path" == "$admission_root/transaction.lock" \
    && -f "$lock_path" && ! -L "$lock_path" \
    && "$(vp_worker_redis_marker_file_mode "$lock_path")" == 600 ]] \
    || return 1
  exec 19<>"$lock_path" || return 1
  VP_WORKER_ADMISSION_LOCK_ROOT="$admission_root"
  if ! python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    lock-acquire "$admission_root" 19 >/dev/null 2>&1; then
    exec 19>&-
    VP_WORKER_ADMISSION_LOCK_ROOT=""
    return 1
  fi
  local lock_token
  lock_token="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      lock-token "$admission_root" 19 2>/dev/null
  )" || {
    exec 19>&-
    VP_WORKER_ADMISSION_LOCK_ROOT=""
    return 1
  }
  [[ "$lock_token" =~ ^[0-9]+:[1-9][0-9]*$ ]] || {
    exec 19>&-
    VP_WORKER_ADMISSION_LOCK_ROOT=""
    return 1
  }
  VP_WORKER_ADMISSION_LOCK_HELD=true
  VP_WORKER_ADMISSION_LOCK_DEPTH=1
  VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID="$VP_WORKER_ADMISSION_CURRENT_BASHPID"
  VP_WORKER_ADMISSION_LOCK_TOKEN="$lock_token"
}

vp_worker_admission_lock_release() {
  [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_LOCK_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || return 1
  if [[ "$VP_WORKER_ADMISSION_LOCK_DEPTH" -gt 1 ]]; then
    VP_WORKER_ADMISSION_LOCK_DEPTH=$((VP_WORKER_ADMISSION_LOCK_DEPTH - 1))
    return 0
  fi
  local status=0
  vp_worker_admission_lock_assert || status=1
  exec 19>&- || status=1
  VP_WORKER_ADMISSION_LOCK_HELD=false
  VP_WORKER_ADMISSION_LOCK_DEPTH=0
  VP_WORKER_ADMISSION_LOCK_ROOT=""
  VP_WORKER_ADMISSION_LOCK_OWNER_BASHPID=""
  VP_WORKER_ADMISSION_LOCK_TOKEN=""
  return "$status"
}

vp_worker_admission_record_deploy_signal() {
  local signal_status="$1"
  [[ "$signal_status" =~ ^(129|130|143)$ ]] || return 1
  if [[ "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS" -eq 0 ]]; then
    VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS="$signal_status"
  fi
}

vp_python_worker_pending_signal_handler() {
  local signal_status="$1"
  local signal_name="$2"
  if [[ "$VP_PYTHON_WORKER_SIGNAL_STATUS" -eq 0 ]]; then
    VP_PYTHON_WORKER_SIGNAL_STATUS="$signal_status"
    VP_PYTHON_WORKER_SIGNAL_NAME="$signal_name"
  fi
  if [[ "$VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS" -eq 0 ]]; then
    VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS="$signal_status"
    VP_PYTHON_WORKER_PENDING_SIGNAL_NAME="$signal_name"
  fi
  vp_worker_admission_record_deploy_signal \
    "$VP_PYTHON_WORKER_SIGNAL_STATUS" || true
}

vp_python_worker_cleanup_active_operation() {
  if [[ "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME" \
      =~ ^op\.[0-9a-f]{32}$ \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT" = /* \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_UID" =~ ^[0-9]+$ \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_GID" =~ ^[0-9]+$ ]]; then
    if vp_worker_admission_lock_assert \
      && vp_python_worker_host_guard \
        finish-operation \
        "$VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT" \
        "$VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME" \
        "$VP_PYTHON_WORKER_ACTIVE_OPERATION_UID" \
        "$VP_PYTHON_WORKER_ACTIVE_OPERATION_GID" >/dev/null 2>&1; then
      VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
      VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="-"
      return 0
    fi
    return 1
  fi
  [[ "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" == true \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME" == - ]]
}

vp_python_worker_signal_handler() {
  local signal_status="$1"
  local signal_name="$2"
  vp_python_worker_pending_signal_handler \
    "$signal_status" "$signal_name"
  signal_status="$VP_PYTHON_WORKER_SIGNAL_STATUS"
  signal_name="$VP_PYTHON_WORKER_SIGNAL_NAME"
  if [[ ! "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" =~ ^[1-9][0-9]*$ ]]; then
    return 0
  fi
  local child_pid="$VP_PYTHON_WORKER_ACTIVE_CHILD_PID"
  VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS=0
  VP_PYTHON_WORKER_PENDING_SIGNAL_NAME=""
  kill "-$signal_name" "$child_pid" >/dev/null 2>&1 || true
  wait "$child_pid" >/dev/null 2>&1 || true
  if [[ "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" == "$child_pid" ]]; then
    VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
  fi
  vp_python_worker_cleanup_active_operation || true
}

vp_worker_admission_raise_if_signaled() {
  local signal_status=0
  local signal_name=""
  if [[ "$VP_PYTHON_WORKER_SIGNAL_STATUS" =~ ^(129|130|143)$ ]]; then
    signal_status="$VP_PYTHON_WORKER_SIGNAL_STATUS"
    signal_name="$VP_PYTHON_WORKER_SIGNAL_NAME"
  elif [[ "$VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS" \
    =~ ^(129|130|143)$ ]]; then
    signal_status="$VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS"
    signal_name="$VP_PYTHON_WORKER_PENDING_SIGNAL_NAME"
  elif [[ "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS" \
      =~ ^(129|130|143)$ ]]; then
    signal_status="$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS"
  fi
  [[ "$signal_status" -ne 0 ]] || return 0
  if [[ "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" =~ ^[1-9][0-9]*$ \
    && "$signal_name" =~ ^(HUP|INT|TERM)$ ]]; then
    vp_python_worker_signal_handler "$signal_status" "$signal_name"
  elif [[ "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true ]]; then
    vp_python_worker_cleanup_active_operation || true
  fi
  return "$signal_status"
}

vp_python_worker_consume_pending_signal() {
  local signal_status="$VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS"
  local signal_name="$VP_PYTHON_WORKER_PENDING_SIGNAL_NAME"
  [[ "$signal_status" =~ ^(129|130|143)$ \
    && "$signal_name" =~ ^(HUP|INT|TERM)$ ]] || return 0
  if [[ "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" =~ ^[1-9][0-9]*$ ]]; then
    vp_python_worker_signal_handler "$signal_status" "$signal_name"
  fi
  return "$signal_status"
}

vp_python_worker_install_pending_signal_traps() {
  trap 'vp_python_worker_pending_signal_handler 129 HUP' HUP
  trap 'vp_python_worker_pending_signal_handler 130 INT' INT
  trap 'vp_python_worker_pending_signal_handler 143 TERM' TERM
}

vp_python_worker_install_full_signal_traps() {
  trap 'vp_python_worker_signal_handler 129 HUP' HUP
  trap 'vp_python_worker_signal_handler 130 INT' INT
  trap 'vp_python_worker_signal_handler 143 TERM' TERM
}

vp_python_worker_restore_trap() {
  local saved_trap="$1"
  local signal_name="$2"
  if [[ -n "$saved_trap" ]]; then
    eval "$saved_trap"
  else
    trap - "$signal_name"
  fi
}

vp_python_worker_prepare_launch_gate() {
  local admission_root="$1"
  local caller_uid="$2"
  local caller_gid="$3"
  vp_worker_admission_lock_assert || return 1
  [[ "$admission_root" == "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    && "$caller_uid" =~ ^[0-9]+$ \
    && "$caller_gid" =~ ^[0-9]+$ \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_FD" -eq 16 \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_FD" -eq 17 \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_OPEN" == false \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN" == false \
    && -z "$VP_PYTHON_WORKER_LAUNCH_GATE_PATH" \
    && -z "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY" \
    && -z "$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN" \
    && ! -e /dev/fd/16 \
    && ! -e /dev/fd/17 ]] || return 1
  local directory
  directory="$(
    vp_python_worker_prepare_controlled_directory \
      "$admission_root/launch-gates"
  )" || return 1
  local gate_record
  gate_record="$(
    python3 -I - "$directory" "$caller_uid" "$caller_gid" <<'PY'
import os
import secrets
import stat
import sys

try:
    directory = os.path.abspath(sys.argv[1])
    expected_uid = int(sys.argv[2])
    expected_gid = int(sys.argv[3])
    directory_metadata = os.lstat(directory)
    if (
        not stat.S_ISDIR(directory_metadata.st_mode)
        or stat.S_IMODE(directory_metadata.st_mode) != 0o700
        or directory_metadata.st_uid != expected_uid
        or directory_metadata.st_gid != expected_gid
    ):
        raise ValueError
    for _attempt in range(16):
        path = os.path.join(directory, f".gate.{secrets.token_hex(16)}")
        try:
            os.mkfifo(path, 0o600)
        except FileExistsError:
            continue
        metadata = os.lstat(path)
        if (
            not stat.S_ISFIFO(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or metadata.st_nlink != 1
        ):
            raise ValueError
        print(f"{path}|{metadata.st_dev}:{metadata.st_ino}")
        break
    else:
        raise ValueError
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
PY
  )" || return 1
  local path="${gate_record%%|*}"
  local identity="${gate_record#*|}"
  [[ "$path" == "$directory/".gate.* \
    && "$identity" != "$gate_record" \
    && "$identity" =~ ^[0-9]+:[1-9][0-9]*$ ]] || return 1
  exec 16<>"$path" || return 1
  exec 17<"$path" || {
    exec 16>&-
    return 1
  }
  if ! python3 -I - "$path" 16 17 "$identity" \
      "$caller_uid" "$caller_gid" <<'PY'
import fcntl
import os
import stat
import sys

try:
    path = os.path.abspath(sys.argv[1])
    writer_descriptor = int(sys.argv[2])
    reader_descriptor = int(sys.argv[3])
    expected_device, expected_inode = (
        int(value) for value in sys.argv[4].split(":", 1)
    )
    expected_uid = int(sys.argv[5])
    expected_gid = int(sys.argv[6])
    path_metadata = os.lstat(path)
    writer_metadata = os.fstat(writer_descriptor)
    reader_metadata = os.fstat(reader_descriptor)
    for metadata in (path_metadata, writer_metadata, reader_metadata):
        if (
            not stat.S_ISFIFO(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino)
            != (expected_device, expected_inode)
        ):
            raise ValueError
    writer_flags = fcntl.fcntl(writer_descriptor, fcntl.F_GETFL)
    reader_flags = fcntl.fcntl(reader_descriptor, fcntl.F_GETFL)
    if (
        writer_flags & os.O_ACCMODE != os.O_RDWR
        or reader_flags & os.O_ACCMODE != os.O_RDONLY
    ):
        raise ValueError
    os.unlink(path)
    if (
        os.fstat(writer_descriptor).st_nlink != 0
        or os.fstat(reader_descriptor).st_nlink != 0
    ):
        raise ValueError
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
PY
  then
    exec 17>&-
    exec 16>&-
    return 1
  fi
  local token
  token="$(
    python3 -I -c 'import secrets; print(secrets.token_hex(16))'
  )" || {
    exec 17>&-
    exec 16>&-
    return 1
  }
  [[ "$token" =~ ^[0-9a-f]{32}$ ]] || {
    exec 17>&-
    exec 16>&-
    return 1
  }
  VP_PYTHON_WORKER_LAUNCH_GATE_OPEN=true
  VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=true
  VP_PYTHON_WORKER_LAUNCH_GATE_PATH="$path"
  VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY="$identity"
  VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN="$token"
}

vp_python_worker_verify_launch_gate() {
  [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_OPEN" == true \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_FD" -eq 16 \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_PATH" = /* \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY" \
      =~ ^[0-9]+:[1-9][0-9]*$ \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN" \
      =~ ^[0-9a-f]{32}$ ]] || return 1
  python3 -I -c '
import os
import stat
import sys

try:
    descriptor = int(sys.argv[1])
    expected_device, expected_inode = (
        int(value) for value in sys.argv[2].split(":", 1)
    )
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISFIFO(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != os.getuid()
        or metadata.st_gid != os.getgid()
        or metadata.st_nlink != 0
        or (metadata.st_dev, metadata.st_ino)
        != (expected_device, expected_inode)
    ):
        raise ValueError
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
' "$VP_PYTHON_WORKER_LAUNCH_GATE_FD" \
    "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY"
}

vp_python_worker_verify_launch_gate_reader() {
  [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN" == true \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_FD" -eq 17 \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_PATH" = /* \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY" \
      =~ ^[0-9]+:[1-9][0-9]*$ \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN" \
      =~ ^[0-9a-f]{32}$ ]] || return 1
  python3 -I -c '
import fcntl
import os
import stat
import sys

try:
    descriptor = int(sys.argv[1])
    expected_device, expected_inode = (
        int(value) for value in sys.argv[2].split(":", 1)
    )
    metadata = os.fstat(descriptor)
    flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
    if (
        not stat.S_ISFIFO(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != os.getuid()
        or metadata.st_gid != os.getgid()
        or metadata.st_nlink != 0
        or (metadata.st_dev, metadata.st_ino)
        != (expected_device, expected_inode)
        or flags & os.O_ACCMODE != os.O_RDONLY
    ):
        raise ValueError
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
' "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_FD" \
    "$VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY"
}

vp_python_worker_close_parent_launch_gate_reader() {
  [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN" == true ]] \
    || return 1
  local status=0
  vp_python_worker_verify_launch_gate_reader || status=1
  exec 17>&- || status=1
  VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
  return "$status"
}

vp_python_worker_prepare_launch_supervisor() {
  [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_OPEN" == true \
    && "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN" == true ]] \
    || return 1
  local status=0
  exec 16>&- || status=1
  VP_PYTHON_WORKER_LAUNCH_GATE_OPEN=false
  vp_worker_admission_lock_drop_inherited || status=1
  if [[ "$status" -ne 0 ]] \
    || ! vp_python_worker_verify_launch_gate_reader; then
    exec 17>&- 2>/dev/null || true
    VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
    return 1
  fi
}

vp_python_worker_wait_for_launch_gate() {
  local expected_token="$1"
  [[ "$expected_token" =~ ^[0-9a-f]{32}$ ]] || return 1
  vp_python_worker_prepare_launch_supervisor || return 1
  local received_token=""
  local status=0
  IFS= read -r received_token <&17 || status=1
  exec 17>&- || status=1
  VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
  VP_PYTHON_WORKER_LAUNCH_GATE_PATH=""
  VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY=""
  VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN=""
  [[ "$status" -eq 0 \
    && "$received_token" == "VP-LAUNCH-1:$expected_token" ]]
}

vp_python_worker_drop_inherited_launch_resources() {
  local status=0
  exec 16>&- || status=1
  exec 17>&- || status=1
  VP_PYTHON_WORKER_LAUNCH_GATE_OPEN=false
  VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
  VP_PYTHON_WORKER_LAUNCH_GATE_PATH=""
  VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY=""
  VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN=""
  vp_worker_admission_lock_drop_inherited || status=1
  return "$status"
}

vp_python_worker_release_launch_gate() {
  vp_worker_admission_raise_if_signaled || return $?
  vp_python_worker_verify_launch_gate || return 1
  printf 'VP-LAUNCH-1:%s\n' \
    "$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN" >&16 || return 1
}

vp_python_worker_discard_launch_gate() {
  local status=0
  if [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN" == true ]]; then
    vp_python_worker_verify_launch_gate_reader || status=1
    exec 17>&- || status=1
  fi
  if [[ "$VP_PYTHON_WORKER_LAUNCH_GATE_OPEN" == true ]]; then
    vp_python_worker_verify_launch_gate || status=1
    exec 16>&- || status=1
  fi
  VP_PYTHON_WORKER_LAUNCH_GATE_OPEN=false
  VP_PYTHON_WORKER_LAUNCH_GATE_READ_OPEN=false
  VP_PYTHON_WORKER_LAUNCH_GATE_PATH=""
  VP_PYTHON_WORKER_LAUNCH_GATE_IDENTITY=""
  VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN=""
  return "$status"
}

_vp_run_python_worker_container_locked() {
  local image="$1"
  local secret_source="$2"
  local secret_target="$3"
  local prepare_dirs="$4"
  shift 4
  vp_worker_admission_raise_if_signaled || return $?

  [[ "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  local caller_uid
  local caller_gid
  caller_uid="$(id -u)" || return 1
  caller_gid="$(id -g)" || return 1
  [[ "$caller_uid" =~ ^[0-9]+$ && "$caller_gid" =~ ^[0-9]+$ ]] \
    || return 1
  if [[ "$secret_source" == - && "$secret_target" == - ]]; then
    secret_source=""
    secret_target=""
  elif [[ "$secret_source" != /* \
    || ! "$secret_target" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    return 1
  fi
  case "$prepare_dirs" in
    -)
      prepare_dirs=""
      ;;
    /control-state|/runtime-state|/requests|\
    /control-state,/requests|/runtime-state,/requests)
      ;;
    *)
      return 1
      ;;
  esac

  local passthrough_args=()
  local bind_sources=()
  local bind_targets=()
  local bind_readonly=()
  local payload_sources=()
  local payload_targets=()
  local payload_modes=()
  local capture_query_output=false
  if [[ -n "$secret_source" ]]; then
    payload_sources+=("$secret_source")
    payload_targets+=("/run/secrets/$secret_target")
    payload_modes+=(0400)
  fi
  local seen_targets="|"
  while [[ "$#" -gt 0 && "$1" != -- ]]; do
    case "$1" in
      --network)
        [[ "$#" -ge 2 \
          && "$2" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$ ]] \
          || return 1
        passthrough_args+=("$1" "$2")
        shift 2
        ;;
      --env)
        [[ "$#" -ge 2 \
          && "$2" =~ ^[A-Za-z_][A-Za-z0-9_]*=.*$ \
          && "$2" != *$'\n'* && "$2" != *$'\r'* \
          && "$2" != VP_PYTHON_WORKER_* ]] || return 1
        passthrough_args+=("$1" "$2")
        shift 2
        ;;
      --mount)
        [[ "$#" -ge 2 ]] || return 1
        local mount_spec="$2"
        if [[ ! "$mount_spec" =~ ^type=bind,src=([^,]+),dst=([^,]+)(,readonly)?$ ]]; then
          return 1
        fi
        local bind_source="${BASH_REMATCH[1]}"
        local bind_target="${BASH_REMATCH[2]}"
        local readonly_suffix="${BASH_REMATCH[3]}"
        [[ "$bind_source" = /* && "$seen_targets" != *"|$bind_target|"* ]] \
          || return 1
        case "$bind_target" in
          /control-state|/runtime-state|/requests)
            if [[ ",$prepare_dirs," == *",$bind_target,"* ]]; then
              [[ -z "$readonly_suffix" ]] || return 1
            else
              [[ "$readonly_suffix" == ,readonly ]] || return 1
            fi
            bind_sources+=("$bind_source")
            bind_targets+=("$bind_target")
            bind_readonly+=("$readonly_suffix")
            ;;
          /run/control/upsert.json)
            [[ "$readonly_suffix" == ,readonly ]] || return 1
            payload_sources+=("$bind_source")
            payload_targets+=("$bind_target")
            payload_modes+=(0600)
            ;;
          *)
            return 1
            ;;
        esac
        seen_targets+="$bind_target|"
        shift 2
        ;;
      --query-output)
        [[ "$capture_query_output" == false ]] || return 1
        capture_query_output=true
        shift
        ;;
      *)
        return 1
        ;;
    esac
  done
  [[ "$#" -gt 1 && "$1" == -- ]] || return 1
  shift
  local command=("$@")
  local required_path
  local required_paths="${prepare_dirs//,/ }"
  for required_path in $required_paths; do
    [[ "$seen_targets" == *"|$required_path|"* ]] || return 1
  done

  local admission_root
  admission_root="$(vp_worker_admission_root)" || return 1
  admission_root="$(
    vp_python_worker_prepare_controlled_directory "$admission_root"
  )" || return 1

  local payload_records=()
  local stream_arguments=()
  local index
  for ((index = 0; index < ${#payload_sources[@]}; index++)); do
    local payload_record
    payload_record="$(
      vp_python_worker_host_guard \
        capture-file-record \
        "${payload_sources[$index]}" \
        "$caller_uid" \
        "$caller_gid" \
        "${payload_modes[$index]}"
    )" || return 1
    payload_records[$index]="$payload_record"
    stream_arguments+=(
      "${payload_sources[$index]}"
      "${payload_modes[$index]}"
      "$payload_record"
    )
  done

  local operation_arguments=()
  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    operation_arguments+=(
      "${bind_sources[$index]}"
      "${bind_targets[$index]}"
    )
  done
  vp_worker_admission_raise_if_signaled || return $?
  local operation_output
  if (( ${#operation_arguments[@]} > 0 )); then
    operation_output="$(
      vp_python_worker_host_guard \
        prepare-operation "$admission_root" "$caller_uid" "$caller_gid" \
        "${operation_arguments[@]}"
    )" || return 1
  else
    operation_output="$(
      vp_python_worker_host_guard \
        prepare-operation "$admission_root" "$caller_uid" "$caller_gid"
    )" || return 1
  fi
  local operation_name="$operation_output"
  local bind_manifest=""
  if [[ "$operation_output" == *$'\n'* ]]; then
    operation_name="${operation_output%%$'\n'*}"
    bind_manifest="${operation_output#*$'\n'}"
  fi
  if [[ "$operation_name" == - ]]; then
    [[ -z "$bind_manifest" && ${#bind_sources[@]} -eq 0 ]] || return 1
  elif [[ ! "$operation_name" =~ ^op\.[0-9a-f]{32}$ \
    || -z "$bind_manifest" \
    || ${#bind_sources[@]} -eq 0 ]]; then
    return 1
  fi
  VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="$operation_name"
  VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT="$admission_root"
  VP_PYTHON_WORKER_ACTIVE_OPERATION_UID="$caller_uid"
  VP_PYTHON_WORKER_ACTIVE_OPERATION_GID="$caller_gid"
  if [[ "$operation_name" == - ]]; then
    VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
  else
    VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=false
  fi
  vp_python_worker_install_full_signal_traps
  vp_python_worker_consume_pending_signal || {
    local pending_status=$?
    vp_python_worker_cleanup_active_operation || true
    return "$pending_status"
  }

  local payload_manifest=""
  local needs_control_tmpfs=false
  for ((index = 0; index < ${#payload_targets[@]}; index++)); do
    payload_manifest+="${payload_manifest:+;}${payload_targets[$index]}:${payload_modes[$index]}"
    if [[ "${payload_targets[$index]}" == /run/control/upsert.json ]]; then
      needs_control_tmpfs=true
    fi
  done

  local run_args=(
    run
    --rm
    --user 0:0
    --read-only
    --cap-drop ALL
    --cap-add CHOWN
    --cap-add SETPCAP
    --cap-add SETGID
    --cap-add SETUID
    --security-opt no-new-privileges
    --tmpfs
    "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=1777"
    --tmpfs
    "/run/secrets:rw,nosuid,nodev,noexec,size=65536,mode=0700,uid=0,gid=0"
    --entrypoint
    /bin/bash
  )
  if [[ "$needs_control_tmpfs" == true ]]; then
    run_args+=(
      --tmpfs
      "/run/control:rw,nosuid,nodev,noexec,size=1048576,mode=0700,uid=0,gid=0"
    )
  fi
  if (( ${#payload_sources[@]} > 0 )); then
    run_args+=(--interactive)
  fi
  if (( ${#passthrough_args[@]} > 0 )); then
    run_args+=("${passthrough_args[@]}")
  fi
  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    run_args+=(
      --mount
      "type=bind,src=${bind_sources[$index]},dst=${bind_targets[$index]}${bind_readonly[$index]}"
    )
  done

  local docker_status=0
  local docker_command=(
    docker
    "${run_args[@]}"
    --env "VP_PYTHON_WORKER_SECRET_TARGET=$secret_target" \
    --env "VP_PYTHON_WORKER_CALLER_UID=$caller_uid" \
    --env "VP_PYTHON_WORKER_CALLER_GID=$caller_gid" \
    --env "VP_PYTHON_WORKER_STDIN_TARGETS=$payload_manifest" \
    --env "VP_PYTHON_WORKER_BIND_SENTINELS=$bind_manifest" \
    "$image"
    -ceu
    '
      runtime_uid="${VP_PYTHON_WORKER_CALLER_UID:?}"
      runtime_gid="${VP_PYTHON_WORKER_CALLER_GID:?}"
      [[ "$runtime_uid" =~ ^[0-9]+$ && "$runtime_gid" =~ ^[0-9]+$ ]]
      [[ "$(id -u)" == 0 && "$(id -g)" == 0 ]]
      umask 077
      export HOME=/tmp/vp-python-worker-home
      mkdir -p "$HOME"
      chmod 0700 "$HOME"
      chown "$runtime_uid:$runtime_gid" "$HOME"

      payload_manifest="${VP_PYTHON_WORKER_STDIN_TARGETS:-}"
      if [[ -n "$payload_manifest" ]]; then
        /opt/venv/bin/python -I -c '"'"'
import os
import stat
import struct
import sys

uid = int(sys.argv[1])
gid = int(sys.argv[2])
raw_manifest = sys.argv[3]
specs = []
seen = set()
for raw_spec in raw_manifest.split(";"):
    target, separator, raw_mode = raw_spec.rpartition(":")
    if not separator or target in seen:
        raise RuntimeError("invalid payload manifest")
    mode = int(raw_mode, 8)
    if (
        target.startswith("/run/secrets/")
        and target.count("/") == 3
        and mode == 0o400
    ):
        name = target.rsplit("/", 1)[1]
        if (
            not name
            or len(name) > 128
            or any(
                character
                not in (
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "0123456789._-"
                )
                for character in name
            )
        ):
            raise RuntimeError("invalid secret target")
    elif target == "/run/control/upsert.json" and mode == 0o600:
        pass
    else:
        raise RuntimeError("invalid payload target")
    seen.add(target)
    specs.append((target, mode))

stream = sys.stdin.buffer


def read_exact(length):
    payload = bytearray()
    while len(payload) < length:
        chunk = stream.read(length - len(payload))
        if not chunk:
            raise RuntimeError("truncated payload stream")
        payload.extend(chunk)
    return bytes(payload)


if read_exact(4) != b"VPW1":
    raise RuntimeError("invalid payload stream")
count = read_exact(1)[0]
if count != len(specs):
    raise RuntimeError("payload count mismatch")

created = []
parents = set()
try:
    for target, mode in specs:
        length = struct.unpack(">Q", read_exact(8))[0]
        if length > 1048576:
            raise RuntimeError("payload too large")
        parent = os.path.dirname(target)
        parent_metadata = os.lstat(parent)
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or parent_metadata.st_uid != 0
            or parent_metadata.st_gid != 0
            or stat.S_IMODE(parent_metadata.st_mode) != 0o700
        ):
            raise RuntimeError("payload tmpfs is invalid")
        descriptor = os.open(
            target,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        created.append(target)
        try:
            remaining = length
            while remaining:
                chunk = read_exact(min(remaining, 65536))
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    if written < 1:
                        raise RuntimeError("payload write failed")
                    view = view[written:]
                remaining -= len(chunk)
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, uid, gid)
            os.fsync(descriptor)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != uid
                or metadata.st_gid != gid
                or stat.S_IMODE(metadata.st_mode) != mode
                or metadata.st_nlink != 1
            ):
                raise RuntimeError("payload identity mismatch")
        finally:
            os.close(descriptor)
        parents.add(parent)
    if stream.read(1):
        raise RuntimeError("trailing payload data")
    for parent in parents:
        os.chmod(parent, 0o700)
        os.chown(parent, uid, gid)
except Exception:
    for target in reversed(created):
        try:
            os.unlink(target)
        except OSError:
            pass
    raise
'"'"' "$runtime_uid" "$runtime_gid" "$payload_manifest"
      fi

      while IFS="|" read -r path sentinel marker extra; do
        [[ -n "$path" || -n "$sentinel" || -n "$marker" || -n "$extra" ]] \
          || continue
        [[ -z "$extra" ]]
        case "$path" in
          /control-state|/runtime-state|/requests) ;;
          *) exit 1 ;;
        esac
        [[ "$sentinel" =~ ^\.vp-python-worker-bind-[0-9a-f]{32}$ ]]
        [[ -d "$path" && ! -L "$path" ]]
        [[ "$(stat -c "%a" "$path")" == 700 ]]
        [[ -f "$path/$sentinel" && ! -L "$path/$sentinel" ]]
        sentinel_identity="$(
          stat -c "%u:%g:%a:%h" "$path/$sentinel"
        )"
        [[ "$sentinel_identity" == "$runtime_uid:$runtime_gid:400:1" \
          || "$sentinel_identity" == 0:0:400:1 ]]
        [[ "$(<"$path/$sentinel")" == "$marker" ]]
      done <<<"${VP_PYTHON_WORKER_BIND_SENTINELS:-}"

      unset \
        VP_PYTHON_WORKER_SECRET_TARGET \
        VP_PYTHON_WORKER_CALLER_UID \
        VP_PYTHON_WORKER_CALLER_GID \
        VP_PYTHON_WORKER_STDIN_TARGETS \
        VP_PYTHON_WORKER_BIND_SENTINELS
      exec /usr/bin/setpriv \
        --reuid="$runtime_uid" \
        --regid="$runtime_gid" \
        --clear-groups \
        --inh-caps=-all \
        --ambient-caps=-all \
        --bounding-set=-all \
        --no-new-privs \
        -- "$@"
    '
    vp-python-worker-bootstrap
    "${command[@]}"
  )
  if [[ "$capture_query_output" == true ]]; then
    [[ "$VP_WORKER_ADMISSION_QUERY_WRITE_FD" -eq 15 \
      && "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" == true \
      && "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
        =~ ^[0-9]+:[1-9][0-9]*$ ]] \
      && vp_worker_admission_verify_query_output_fd \
        "$VP_WORKER_ADMISSION_QUERY_WRITE_FD" \
        "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
      || return 1
  fi
  vp_worker_admission_lock_assert || return 1
  vp_worker_admission_raise_if_signaled || return $?
  local caller_pipefail=false
  local launched_child_pid=""
  vp_worker_admission_raise_if_signaled || return $?
  vp_python_worker_prepare_launch_gate \
    "$admission_root" "$caller_uid" "$caller_gid" || return 1
  local launch_token="$VP_PYTHON_WORKER_LAUNCH_GATE_TOKEN"
  if (( ${#payload_sources[@]} > 0 )); then
    if set -o | awk '$1 == "pipefail" && $2 == "on" { found=1 }
      END { exit found ? 0 : 1 }'; then
      caller_pipefail=true
    fi
    set -o pipefail
    (
      trap - HUP INT TERM
      vp_python_worker_drop_inherited_launch_resources || exit 1
      vp_python_worker_host_guard \
        stream-payloads "$caller_uid" "$caller_gid" \
        "${stream_arguments[@]}"
    ) \
      | (
          trap - HUP INT TERM
          vp_python_worker_wait_for_launch_gate "$launch_token" \
            || exit 1
          if [[ "$capture_query_output" == true ]]; then
            if declare -F docker >/dev/null 2>&1; then
              "${docker_command[@]}" \
                >&"$VP_WORKER_ADMISSION_QUERY_WRITE_FD"
            else
              exec "${docker_command[@]}" \
                >&"$VP_WORKER_ADMISSION_QUERY_WRITE_FD"
            fi
          elif declare -F docker >/dev/null 2>&1; then
            "${docker_command[@]}"
          else
            exec "${docker_command[@]}"
          fi
        ) &
  else
    (
      trap - HUP INT TERM
      vp_python_worker_wait_for_launch_gate "$launch_token" \
        || exit 1
      if [[ "$capture_query_output" == true ]]; then
        if declare -F docker >/dev/null 2>&1; then
          "${docker_command[@]}" </dev/null \
            >&"$VP_WORKER_ADMISSION_QUERY_WRITE_FD"
        else
          exec "${docker_command[@]}" </dev/null \
            >&"$VP_WORKER_ADMISSION_QUERY_WRITE_FD"
        fi
      elif declare -F docker >/dev/null 2>&1; then
        "${docker_command[@]}" </dev/null
      else
        exec "${docker_command[@]}" </dev/null
      fi
    ) &
  fi
  VP_PYTHON_WORKER_ACTIVE_CHILD_PID=$!
  launched_child_pid="$VP_PYTHON_WORKER_ACTIVE_CHILD_PID"
  local launch_status=0
  vp_python_worker_close_parent_launch_gate_reader \
    || launch_status=1
  vp_python_worker_consume_pending_signal || true
  if [[ "$launch_status" -eq 0 ]]; then
    vp_python_worker_release_launch_gate || launch_status=$?
  fi
  vp_python_worker_discard_launch_gate || {
    [[ "$launch_status" -ne 0 ]] || launch_status=1
  }
  if [[ "$launch_status" -ne 0 \
    && "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" == "$launched_child_pid" \
    && "$launched_child_pid" =~ ^[1-9][0-9]*$ ]]; then
    kill -TERM "$launched_child_pid" >/dev/null 2>&1 || true
    wait "$launched_child_pid" >/dev/null 2>&1 || true
    if [[ "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" == "$launched_child_pid" ]]; then
      VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
    fi
  fi
  if [[ "$launch_status" -eq 0 \
    && "$VP_PYTHON_WORKER_ACTIVE_CHILD_PID" == "$launched_child_pid" \
    && "$launched_child_pid" =~ ^[1-9][0-9]*$ ]] \
    && wait "$launched_child_pid" 2>/dev/null; then
    docker_status=0
  elif [[ "$launch_status" -ne 0 ]]; then
    docker_status="$launch_status"
  else
    docker_status=$?
  fi
  if [[ "$caller_pipefail" != true ]]; then
    set +o pipefail
  fi
  VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
  vp_worker_admission_raise_if_signaled || return $?

  local validation_status=0
  for ((index = 0; index < ${#payload_sources[@]}; index++)); do
    vp_python_worker_host_guard \
      verify-file \
      "${payload_sources[$index]}" \
      "$caller_uid" \
      "$caller_gid" \
      "${payload_modes[$index]}" \
      "${payload_records[$index]}" >/dev/null \
      || validation_status=1
  done
  if [[ "$operation_name" != - \
    && "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true ]]; then
    if vp_worker_admission_lock_assert \
      && vp_python_worker_host_guard \
        finish-operation \
        "$admission_root" "$operation_name" \
        "$caller_uid" "$caller_gid" >/dev/null; then
      VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
      VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="-"
    else
      validation_status=1
    fi
  fi
  [[ "$docker_status" -eq 0 && "$validation_status" -eq 0 ]]
}

vp_run_python_worker_container() {
  local caller_hup_trap
  local caller_int_trap
  local caller_term_trap
  caller_hup_trap="$(trap -p HUP)"
  caller_int_trap="$(trap -p INT)"
  caller_term_trap="$(trap -p TERM)"
  VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="-"
  VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_UID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_GID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
  VP_PYTHON_WORKER_SIGNAL_STATUS=0
  VP_PYTHON_WORKER_SIGNAL_NAME=""
  VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS=0
  VP_PYTHON_WORKER_PENDING_SIGNAL_NAME=""
  vp_python_worker_install_pending_signal_traps

  local operation_status=0
  local release_status=0
  local lock_acquired=false
  local admission_root=""
  if vp_worker_admission_raise_if_signaled; then
    admission_root="$(vp_worker_admission_root)" || operation_status=1
  else
    operation_status=$?
  fi
  if [[ "$operation_status" -eq 0 ]]; then
    admission_root="$(
      vp_python_worker_prepare_controlled_directory "$admission_root"
    )" || operation_status=1
  fi
  if [[ "$operation_status" -eq 0 ]]; then
    if vp_worker_admission_raise_if_signaled; then
      :
    else
      operation_status=$?
    fi
  fi
  if [[ "$operation_status" -eq 0 ]]; then
    if vp_worker_admission_lock_acquire "$admission_root"; then
      lock_acquired=true
    else
      operation_status=1
    fi
  fi
  if [[ "$operation_status" -eq 0 ]]; then
    if vp_worker_admission_raise_if_signaled; then
      :
    else
      operation_status=$?
    fi
  fi
  if [[ "$operation_status" -eq 0 ]]; then
    if _vp_run_python_worker_container_locked "$@"; then
      operation_status=0
    else
      operation_status=$?
    fi
  fi
  if vp_worker_admission_raise_if_signaled; then
    :
  else
    operation_status=$?
  fi
  if [[ "$VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED" != true ]]; then
    vp_python_worker_cleanup_active_operation || true
  fi

  if [[ "$lock_acquired" == true ]]; then
    vp_worker_admission_lock_release || release_status=1
  fi
  vp_python_worker_restore_trap "$caller_hup_trap" HUP
  vp_python_worker_restore_trap "$caller_int_trap" INT
  vp_python_worker_restore_trap "$caller_term_trap" TERM
  local signal_status="$VP_PYTHON_WORKER_SIGNAL_STATUS"
  if [[ "$signal_status" -eq 0 \
    && "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS" \
      =~ ^(129|130|143)$ ]]; then
    signal_status="$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS"
  fi
  VP_PYTHON_WORKER_ACTIVE_CHILD_PID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_NAME="-"
  VP_PYTHON_WORKER_ACTIVE_OPERATION_ROOT=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_UID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_GID=""
  VP_PYTHON_WORKER_ACTIVE_OPERATION_CLEANED=true
  VP_PYTHON_WORKER_SIGNAL_STATUS=0
  VP_PYTHON_WORKER_SIGNAL_NAME=""
  VP_PYTHON_WORKER_PENDING_SIGNAL_STATUS=0
  VP_PYTHON_WORKER_PENDING_SIGNAL_NAME=""

  if [[ "$signal_status" -ne 0 ]]; then
    return "$signal_status"
  fi
  [[ "$operation_status" -eq 0 && "$release_status" -eq 0 ]]
}

vp_worker_admission_root() {
  local sync_root="${DEPLOY_GITHUB_SYNC_ROOT:-${ROOT:-}}"
  if [[ -z "$sync_root" || ! "$sync_root" = /* \
    || ( -n "${DEPLOY_GITHUB_SYNC_ROOT:-}" \
      && -n "${ROOT:-}" \
      && "$DEPLOY_GITHUB_SYNC_ROOT" != "$ROOT" ) ]]; then
    return 1
  fi
  printf '%s\n' "$sync_root/state/vp-worker-admission"
}

vp_worker_admission_required_file() {
  local path="$1"
  local label="$2"
  if [[ ! "$path" = /* || ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 400 ]]; then
    echo "$label is absent or invalid" >&2
    return 1
  fi
  printf '%s\n' "$path"
}

vp_worker_admission_database_credential_file() {
  local purpose="$1"
  local path="$2"
  local label="$3"
  case "$purpose" in
    deploy_migrator|deploy_read|control_role_owner|runtime_role_owner)
      ;;
    *)
      return 1
      ;;
  esac
  path="$(
    vp_worker_admission_required_file "$path" "$label"
  )" || return 1
  if [[ "$VP_WORKER_ADMISSION_LOCK_HELD" == true \
    && "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
    path="$(
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        verify-credential \
        "$VP_WORKER_ADMISSION_LOCK_ROOT" \
        "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$purpose" \
        "$path" 2>/dev/null
    )" || {
      echo "worker database credential identity drifted" >&2
      return 1
    }
  fi
  printf '%s\n' "$path"
}

vp_worker_admission_new_generation() {
  local epoch
  epoch="$(date +%s)" || return 1
  [[ "$epoch" =~ ^[1-9][0-9]{9,10}$ ]] || return 1
  printf '%s%05d\n' "$epoch" "$((RANDOM % 100000))"
}

vp_worker_admission_kind() {
  case "$1" in
    vp-ffmpeg-worker-go-swarm) printf 'ffmpeg-go\n' ;;
    "$VP_PYTHON_WORKER_SERVICE") printf 'ffmpeg\n' ;;
    "$VP_VISION_WORKER_SERVICE") printf 'vision\n' ;;
    "$VP_PUBLISHER_SERVICE") printf 'youtube-publisher\n' ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_set_candidate() {
  local service="$1"
  local generation="$2"
  local database_secret="$3"
  local admission_secret="$4"
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      VP_WORKER_FFMPEG_GO_GENERATION="$generation"
      VP_WORKER_FFMPEG_GO_DATABASE_SECRET="$database_secret"
      VP_WORKER_FFMPEG_GO_ADMISSION_SECRET="$admission_secret"
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      VP_WORKER_FFMPEG_GENERATION="$generation"
      VP_WORKER_FFMPEG_DATABASE_SECRET="$database_secret"
      VP_WORKER_FFMPEG_ADMISSION_SECRET="$admission_secret"
      ;;
    "$VP_VISION_WORKER_SERVICE")
      VP_WORKER_VISION_GENERATION="$generation"
      VP_WORKER_VISION_DATABASE_SECRET="$database_secret"
      VP_WORKER_VISION_ADMISSION_SECRET="$admission_secret"
      ;;
    "$VP_PUBLISHER_SERVICE")
      VP_WORKER_YOUTUBE_PUBLISHER_GENERATION="$generation"
      VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET="$database_secret"
      VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET="$admission_secret"
      ;;
    *)
      return 1
      ;;
  esac
}

vp_worker_admission_track_candidate() {
  local service="$1"
  vp_worker_admission_kind "$service" >/dev/null || return 1
  case " $VP_WORKER_ADMISSION_CANDIDATE_SERVICES " in
    *" $service "*)
      return 0
      ;;
  esac
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES="${VP_WORKER_ADMISSION_CANDIDATE_SERVICES:+$VP_WORKER_ADMISSION_CANDIDATE_SERVICES }$service"
}

vp_worker_admission_write_manifest() {
  local path="$1"
  local service="$2"
  local commit="$3"
  local image="$4"
  local generation="$5"
  local database_secret="$6"
  local admission_secret="$7"
  local database_secret_id="${8:-}"
  local admission_secret_id="${9:-}"
  local version=1
  if [[ -n "$database_secret_id" || -n "$admission_secret_id" ]]; then
    [[ "$database_secret_id" =~ ^[a-z0-9]{20,64}$ \
      && "$admission_secret_id" =~ ^[a-z0-9]{20,64}$ \
      && "$database_secret_id" != "$admission_secret_id" ]] || return 1
    version=2
  fi
  local directory
  directory="$(dirname "$path")" || return 1
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  local temporary
  temporary="$(mktemp "$directory/.manifest.XXXXXX")" || return 1
  chmod 0600 "$temporary" || {
    rm -f "$temporary"
    return 1
  }
  if ! printf '%s\n' \
    "VERSION=$version" \
    "SERVICE=$service" \
    "COMMIT=$commit" \
    "IMAGE=$image" \
    "GENERATION=$generation" \
    "DATABASE_SECRET=$database_secret" \
    "ADMISSION_SECRET=$admission_secret" \
    ${database_secret_id:+"DATABASE_SECRET_ID=$database_secret_id"} \
    ${admission_secret_id:+"ADMISSION_SECRET_ID=$admission_secret_id"} \
    >"$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
  [[ "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]]
}

vp_worker_admission_read_manifest() {
  local path="$1"
  local expected_service="$2"
  if [[ ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    return 1
  fi
  VP_WORKER_MANIFEST_VERSION=""
  VP_WORKER_MANIFEST_SERVICE=""
  VP_WORKER_MANIFEST_COMMIT=""
  VP_WORKER_MANIFEST_IMAGE=""
  VP_WORKER_MANIFEST_GENERATION=""
  VP_WORKER_MANIFEST_DATABASE_SECRET=""
  VP_WORKER_MANIFEST_ADMISSION_SECRET=""
  VP_WORKER_MANIFEST_DATABASE_SECRET_ID=""
  VP_WORKER_MANIFEST_ADMISSION_SECRET_ID=""
  local key
  local value
  while IFS='=' read -r key value; do
    [[ -n "$key" && -n "$value" ]] || return 1
    case "$key" in
      VERSION) [[ -z "$VP_WORKER_MANIFEST_VERSION" ]] || return 1
        VP_WORKER_MANIFEST_VERSION="$value" ;;
      SERVICE) [[ -z "$VP_WORKER_MANIFEST_SERVICE" ]] || return 1
        VP_WORKER_MANIFEST_SERVICE="$value" ;;
      COMMIT) [[ -z "$VP_WORKER_MANIFEST_COMMIT" ]] || return 1
        VP_WORKER_MANIFEST_COMMIT="$value" ;;
      IMAGE) [[ -z "$VP_WORKER_MANIFEST_IMAGE" ]] || return 1
        VP_WORKER_MANIFEST_IMAGE="$value" ;;
      GENERATION) [[ -z "$VP_WORKER_MANIFEST_GENERATION" ]] || return 1
        VP_WORKER_MANIFEST_GENERATION="$value" ;;
      DATABASE_SECRET)
        [[ -z "$VP_WORKER_MANIFEST_DATABASE_SECRET" ]] || return 1
        VP_WORKER_MANIFEST_DATABASE_SECRET="$value" ;;
      ADMISSION_SECRET)
        [[ -z "$VP_WORKER_MANIFEST_ADMISSION_SECRET" ]] || return 1
        VP_WORKER_MANIFEST_ADMISSION_SECRET="$value" ;;
      DATABASE_SECRET_ID)
        [[ -z "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" ]] || return 1
        VP_WORKER_MANIFEST_DATABASE_SECRET_ID="$value" ;;
      ADMISSION_SECRET_ID)
        [[ -z "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" ]] || return 1
        VP_WORKER_MANIFEST_ADMISSION_SECRET_ID="$value" ;;
      *) return 1 ;;
    esac
  done <"$path"
  [[ "$VP_WORKER_MANIFEST_VERSION" =~ ^[12]$ \
    && "$VP_WORKER_MANIFEST_SERVICE" == "$expected_service" \
    && "$VP_WORKER_MANIFEST_COMMIT" =~ ^[0-9a-f]{40}$ \
    && "$VP_WORKER_MANIFEST_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "$VP_WORKER_MANIFEST_GENERATION" =~ ^[1-9][0-9]*$ \
    && "$VP_WORKER_MANIFEST_DATABASE_SECRET" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    && "$VP_WORKER_MANIFEST_ADMISSION_SECRET" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    && ( "$VP_WORKER_MANIFEST_VERSION" == 1 \
      && -z "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
      && -z "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" \
      || "$VP_WORKER_MANIFEST_VERSION" == 2 \
      && "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" =~ ^[a-z0-9]{20,64}$ \
      && "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" =~ ^[a-z0-9]{20,64}$ \
      && "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
        != "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" ) ]]
}

vp_managed_secret_id() {
  local reference="$1"
  local expected_name="$2"
  local expected_service="$3"
  local expected_generation="$4"
  local expected_purpose="$5"
  [[ "$reference" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$ \
    && "$expected_name" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$ \
    && "$expected_service" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$ \
    && "$expected_generation" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    && "$expected_purpose" =~ ^[a-z][a-z0-9-]{0,63}$ ]] \
    || return 1
  local identity
  identity="$(
    docker secret inspect "$reference" \
      --format '{{.ID}}|{{.Spec.Name}}|{{index .Spec.Labels "vp.service"}}|{{index .Spec.Labels "vp.generation"}}|{{index .Spec.Labels "vp.purpose"}}'
  )" || return 1
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local secret_id
  local name
  local service
  local generation
  local purpose
  local extra
  IFS='|' read -r \
    secret_id name service generation purpose extra \
    <<<"$identity"
  [[ -z "$extra" \
    && "$secret_id" =~ ^[a-z0-9]{20,64}$ \
    && "$name" == "$expected_name" \
    && "$service" == "$expected_service" \
    && "$generation" == "$expected_generation" \
    && "$purpose" == "$expected_purpose" ]] || return 1
  printf '%s\n' "$secret_id"
}

vp_worker_admission_prepared_secret_id() {
  local name="$1"
  local service="$2"
  local generation="$3"
  local purpose="$4"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]] \
    || return 1
  vp_worker_admission_lock_assert || return 1
  VP_WORKER_PREPARED_SECRET_ID="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      lookup-prepared-secret \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$name" "$service" "$generation" "$purpose"
  )" || {
    VP_WORKER_PREPARED_SECRET_ID=""
    return 1
  }
  [[ "$VP_WORKER_PREPARED_SECRET_ID" == - \
    || "$VP_WORKER_PREPARED_SECRET_ID" =~ ^[a-z0-9]{20,64}$ ]]
}

vp_worker_admission_record_prepared_secret() {
  local name="$1"
  local secret_id="$2"
  local service="$3"
  local generation="$4"
  local purpose="$5"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]] \
    || return 1
  vp_worker_admission_lock_assert || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-prepared-secret \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$name" "$secret_id" "$service" "$generation" "$purpose" \
    >/dev/null
}

vp_worker_admission_record_authority_intent() {
  local kind="$1"
  local service="$2"
  local generation="$3"
  local control_image="$4"
  local control_generation="$5"
  local operator_reference="$6"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]] \
    || return 1
  vp_worker_admission_lock_assert || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-authority-intent \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$kind" "$service" "$generation" \
    "$control_image" "$control_generation" "$operator_reference" \
    >/dev/null
}

vp_worker_admission_mark_authority_provisioning() {
  local kind="$1"
  local service="$2"
  local generation="$3"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]] \
    || return 1
  vp_worker_admission_lock_assert || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioning \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$kind" "$service" "$generation" >/dev/null
}

vp_worker_admission_mark_authority_provisioned() {
  local kind="$1"
  local service="$2"
  local generation="$3"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]] \
    || return 1
  vp_worker_admission_lock_assert || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    mark-authority-provisioned \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$kind" "$service" "$generation" >/dev/null
}

vp_worker_admission_load_abort_state() {
  vp_worker_admission_lock_assert || return 1
  local state
  state="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      list-abort \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" 2>/dev/null
  )" || return 1
  local fields
  fields="$(
    printf '%s\n' "$state" | python3 -I -c '
import json
import re
import sys


def require_string(value, pattern):
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise ValueError
    return value


try:
    state = json.load(sys.stdin)
    if set(state) != {
        "authorities",
        "operation",
        "phase",
        "prepared_secrets",
        "reason",
        "revision",
    }:
        raise ValueError
    if (
        state["phase"] != "ABORTING"
        or isinstance(state["revision"], bool)
        or not isinstance(state["revision"], int)
        or state["revision"] < 0
    ):
        raise ValueError
    reason = require_string(state["reason"], r"[a-z][a-z0-9_]{0,63}")
    if not isinstance(state["prepared_secrets"], list):
        raise ValueError
    if not isinstance(state["authorities"], list):
        raise ValueError
    secret = None
    operation_id = "-"
    operation = state["operation"]
    if operation is not None:
        if set(operation) != {
            "identity",
            "kind",
            "operation_id",
            "target_phase",
        }:
            raise ValueError
        if (
            operation["kind"] != "REMOVE_PREPARED_SECRET"
            or operation["target_phase"] != "ABORTING"
        ):
            raise ValueError
        operation_id = require_string(
            operation["operation_id"],
            r"operation-[0-9a-f]{32}",
        )
        identity = operation["identity"]
        if set(identity) != {
            "docker_id",
            "generation",
            "kind",
            "name",
            "purpose",
            "service",
            "spec_digest",
        } or identity["kind"] != "secret" or identity["spec_digest"] is not None:
            raise ValueError
        secret = {
            "docker_secret_id": identity["docker_id"],
            "generation": identity["generation"],
            "name": identity["name"],
            "purpose": identity["purpose"],
            "service": identity["service"],
        }
    elif state["prepared_secrets"]:
        secret = state["prepared_secrets"][0]
    if secret is not None:
        if set(secret) != {
            "docker_secret_id",
            "generation",
            "name",
            "purpose",
            "service",
        }:
            raise ValueError
        secret_fields = [
            require_string(secret["name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}"),
            require_string(secret["docker_secret_id"], r"[a-z0-9]{20,64}"),
            require_string(secret["service"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}"),
            require_string(secret["generation"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"),
            require_string(secret["purpose"], r"[a-z][a-z0-9_-]{0,63}"),
        ]
    else:
        secret_fields = ["-", "-", "-", "-", "-"]
    authority_fields = ["-", "-", "-", "-", "-", "-", "-"]
    if secret is None and state["authorities"]:
        authority = state["authorities"][0]
        if set(authority) != {
            "control_generation",
            "control_image",
            "generation",
            "kind",
            "operator_reference",
            "service",
            "state",
        }:
            raise ValueError
        kind = require_string(authority["kind"], r"(control|marker|runtime)")
        service = require_string(
            authority["service"],
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
        )
        generation = require_string(
            authority["generation"],
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
        )
        authority_state = require_string(
            authority["state"],
            r"(planned|provisioning|provisioned)",
        )
        control_image = require_string(
            authority["control_image"],
            r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
        )
        control_generation = require_string(
            authority["control_generation"],
            r"c-[0-9a-f]{20}",
        )
        operator_reference = require_string(
            authority["operator_reference"],
            r"[A-Za-z0-9][A-Za-z0-9_./-]{0,254}",
        )
        expected_reference = (
            "marker/"
            + generation
            + "/worker-marker-owner-database-url"
            if kind == "marker"
            else (
                "control/"
                + control_generation
                + "/worker-registration-operator-database-url"
            )
        )
        if operator_reference != expected_reference:
            raise ValueError
        authority_fields = [
            kind,
            service,
            generation,
            authority_state,
            control_image,
            control_generation,
            operator_reference,
        ]
    print(
        "|".join(
            [
                str(state["revision"]),
                reason,
                operation_id,
                *secret_fields,
                *authority_fields,
            ]
        )
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local extra
  IFS='|' read -r \
    VP_WORKER_ABORT_REVISION \
    VP_WORKER_ABORT_REASON \
    VP_WORKER_ABORT_OPERATION_ID \
    VP_WORKER_ABORT_SECRET_NAME \
    VP_WORKER_ABORT_SECRET_ID \
    VP_WORKER_ABORT_SECRET_SERVICE \
    VP_WORKER_ABORT_SECRET_GENERATION \
    VP_WORKER_ABORT_SECRET_PURPOSE \
    VP_WORKER_ABORT_AUTHORITY_KIND \
    VP_WORKER_ABORT_AUTHORITY_SERVICE \
    VP_WORKER_ABORT_AUTHORITY_GENERATION \
    VP_WORKER_ABORT_AUTHORITY_STATE \
    VP_WORKER_ABORT_AUTHORITY_CONTROL_IMAGE \
    VP_WORKER_ABORT_AUTHORITY_CONTROL_GENERATION \
    VP_WORKER_ABORT_AUTHORITY_OPERATOR_REFERENCE \
    extra <<<"$fields"
  [[ -z "$extra" \
    && "$VP_WORKER_ABORT_REVISION" =~ ^(0|[1-9][0-9]*)$ \
    && "$VP_WORKER_ABORT_REASON" =~ ^[a-z][a-z0-9_]{0,63}$ ]] \
    || return 1
}

vp_worker_admission_secret_unused() {
  local secret_id="$1"
  [[ "$secret_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local service_ids
  service_ids="$(
    docker service ls --quiet --no-trunc 2>/dev/null
  )" || return 1
  local seen_services="|"
  local service_id
  while IFS= read -r service_id; do
    [[ -n "$service_id" ]] || continue
    [[ "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$seen_services" != *"|$service_id|"* ]] || return 1
    seen_services+="$service_id|"
    local mounted_ids
    mounted_ids="$(
      docker service inspect "$service_id" \
        --format '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretID}}{{end}}' \
        2>/dev/null
    )" || return 1
    local mounted_id
    while IFS= read -r mounted_id; do
      [[ -n "$mounted_id" ]] || continue
      [[ "$mounted_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
      if [[ "$mounted_id" == "$secret_id" ]]; then
        echo "prepared worker secret is still mounted" >&2
        return 1
      fi
    done <<<"$mounted_ids"
  done <<<"$service_ids"
}

vp_worker_admission_secret_id_absent() {
  local secret_id="$1"
  [[ "$secret_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local visible_ids
  visible_ids="$(
    docker secret ls \
      --filter "id=$secret_id" \
      --format '{{.ID}}' 2>/dev/null
  )" || return 1
  local visible_id
  while IFS= read -r visible_id; do
    [[ -n "$visible_id" ]] || continue
    [[ "$visible_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
    return 1
  done <<<"$visible_ids"
}

vp_worker_admission_abort_preparing_transaction() {
  local reason="$1"
  [[ "$reason" =~ ^[a-z][a-z0-9_]{0,63}$ ]] || return 1
  vp_worker_admission_lock_assert || return 1
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local replay
  replay="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      replay-plan "$root" 2>/dev/null
  )" || return 1
  local replay_fields
  replay_fields="$(
    printf '%s\n' "$replay" | python3 -I -c '
import json
import sys

try:
    plan = json.load(sys.stdin)
    if set(plan) != {
        "active",
        "allow_new_candidate",
        "allow_stale_cleanup",
        "namespace",
        "next_action",
        "pending_operation",
        "phase",
        "retirements",
        "revision",
        "transaction_id",
    }:
        raise ValueError
    if (
        not plan["active"]
        or plan["allow_new_candidate"]
        or plan["allow_stale_cleanup"]
        or plan["phase"] not in {
            "PREPARING", "FORWARD_APPLYING", "ABORTING", "DONE"
        }
        or isinstance(plan["revision"], bool)
        or not isinstance(plan["revision"], int)
        or plan["revision"] < 0
    ):
        raise ValueError
    print("{}|{}".format(plan["phase"], plan["revision"]))
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local phase
  local revision
  local extra
  IFS='|' read -r phase revision extra <<<"$replay_fields"
  [[ -z "$extra" && "$revision" =~ ^(0|[1-9][0-9]*)$ ]] || return 1
  case "$phase" in
    PREPARING|FORWARD_APPLYING)
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        begin-abort \
        "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$revision" "$reason" >/dev/null || return 1
      ;;
    ABORTING)
      ;;
    DONE)
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        archive \
        "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$revision" >/dev/null || return 1
      VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
      return 0
      ;;
    *)
      return 1
      ;;
  esac

  while :; do
    vp_worker_admission_load_abort_state || return 1
    [[ "$VP_WORKER_ABORT_REASON" == "$reason" ]] || return 1
    if [[ "$VP_WORKER_ABORT_SECRET_ID" != - ]]; then
      if [[ "$VP_WORKER_ABORT_OPERATION_ID" == - ]]; then
        local expected_secret_id="$VP_WORKER_ABORT_SECRET_ID"
        python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
          intent-prepared-secret-removal \
          "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
          "$VP_WORKER_ABORT_REVISION" \
          "$VP_WORKER_ABORT_SECRET_ID" >/dev/null || return 1
        vp_worker_admission_load_abort_state || return 1
        [[ "$VP_WORKER_ABORT_SECRET_ID" == "$expected_secret_id" \
          && "$VP_WORKER_ABORT_OPERATION_ID" \
            =~ ^operation-[0-9a-f]{32}$ ]] || return 1
      fi
      vp_worker_admission_secret_unused \
        "$VP_WORKER_ABORT_SECRET_ID" || return 1
      local inspected_id
      if inspected_id="$(
        vp_managed_secret_id \
          "$VP_WORKER_ABORT_SECRET_ID" \
          "$VP_WORKER_ABORT_SECRET_NAME" \
          "$VP_WORKER_ABORT_SECRET_SERVICE" \
          "$VP_WORKER_ABORT_SECRET_GENERATION" \
          "$VP_WORKER_ABORT_SECRET_PURPOSE" 2>/dev/null
      )"; then
        [[ "$inspected_id" == "$VP_WORKER_ABORT_SECRET_ID" ]] || return 1
        docker secret rm "$VP_WORKER_ABORT_SECRET_ID" >/dev/null \
          || return 1
      else
        vp_worker_admission_secret_id_absent \
          "$VP_WORKER_ABORT_SECRET_ID" || return 1
      fi
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        complete-prepared-secret-removal \
        "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$VP_WORKER_ABORT_REVISION" \
        "$VP_WORKER_ABORT_OPERATION_ID" >/dev/null || return 1
      continue
    fi
    if [[ "$VP_WORKER_ABORT_AUTHORITY_KIND" != - ]]; then
      case "$VP_WORKER_ABORT_AUTHORITY_KIND" in
        control)
          vp_worker_control_revoke_authority \
            "$VP_WORKER_ABORT_AUTHORITY_CONTROL_IMAGE" \
            "$VP_WORKER_ABORT_AUTHORITY_GENERATION" \
            "$root" || return 1
          ;;
        marker)
          local marker_control_root
          marker_control_root="$(vp_worker_redis_marker_control_root)" \
            || return 1
          vp_worker_redis_marker_revoke_roles \
            "$VP_WORKER_ABORT_AUTHORITY_CONTROL_IMAGE" \
            "$VP_WORKER_ABORT_AUTHORITY_GENERATION" \
            "$marker_control_root" || return 1
          local marker_secret_manifest="$marker_control_root/secret-manifests/$VP_WORKER_ABORT_AUTHORITY_GENERATION.conf"
          if [[ -e "$marker_secret_manifest" ]]; then
            vp_worker_redis_marker_require_secret_manifest \
              "$marker_control_root" \
              "$VP_WORKER_ABORT_AUTHORITY_GENERATION" || return 1
            rm -f "$marker_secret_manifest" || return 1
          fi
          ;;
        runtime)
          vp_worker_admission_revoke_generation_authority \
            "$VP_WORKER_ABORT_AUTHORITY_SERVICE" \
            "$VP_WORKER_ABORT_AUTHORITY_GENERATION" \
            "$root" \
            "$VP_WORKER_ABORT_AUTHORITY_CONTROL_IMAGE" \
            "$VP_WORKER_ABORT_AUTHORITY_CONTROL_GENERATION" \
            "$VP_WORKER_ABORT_AUTHORITY_OPERATOR_REFERENCE" \
            || return 1
          ;;
        *)
          return 1
          ;;
      esac
      python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
        complete-abort-authority \
        "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
        "$VP_WORKER_ABORT_REVISION" \
        "$VP_WORKER_ABORT_AUTHORITY_KIND" \
        "$VP_WORKER_ABORT_AUTHORITY_SERVICE" \
        "$VP_WORKER_ABORT_AUTHORITY_GENERATION" >/dev/null || return 1
      continue
    fi
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      finish-abort \
      "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$VP_WORKER_ABORT_REVISION" >/dev/null || return 1
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      archive \
      "$root" "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$((VP_WORKER_ABORT_REVISION + 1))" >/dev/null || return 1
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
    VP_WORKER_ADMISSION_PREPARED=false
    VP_WORKER_CONTROL_PREPARED=false
    return 0
  done
}

vp_remove_managed_secret() {
  local secret_id="$1"
  local expected_name="$2"
  local expected_service="$3"
  local expected_generation="$4"
  local expected_purpose="$5"
  [[ "$secret_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local inspected_id
  inspected_id="$(
    vp_managed_secret_id \
      "$secret_id" \
      "$expected_name" \
      "$expected_service" \
      "$expected_generation" \
      "$expected_purpose"
  )" || return 1
  [[ "$inspected_id" == "$secret_id" ]] || return 1
  docker secret rm "$secret_id" >/dev/null
}

vp_remove_managed_secret_if_absent_exact() {
  local secret_id="$1"
  local expected_name="$2"
  local expected_service="$3"
  local expected_generation="$4"
  local expected_purpose="$5"
  [[ "$secret_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local inspected_id
  if inspected_id="$(
    vp_managed_secret_id \
      "$secret_id" \
      "$expected_name" \
      "$expected_service" \
      "$expected_generation" \
      "$expected_purpose"
  )"; then
    [[ "$inspected_id" == "$secret_id" ]] || return 1
    docker secret rm "$secret_id" >/dev/null
    return
  fi

  local inventory
  inventory="$(
    docker secret ls --format '{{.ID}}|{{.Name}}'
  )" || return 1
  local listed_id
  local listed_name
  local extra
  while IFS='|' read -r listed_id listed_name extra; do
    [[ -n "$listed_id$listed_name$extra" ]] || continue
    [[ -z "$extra" \
      && "$listed_id" =~ ^[a-z0-9]{12,64}$ \
      && "$listed_name" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ ]] \
      || return 1
    if [[ "$listed_id" == "$secret_id" \
      || "$listed_name" == "$expected_name" ]]; then
      return 1
    fi
  done <<<"$inventory"
}

vp_worker_admission_require_v2_manifest() {
  local path="$1"
  local service="$2"
  vp_worker_admission_read_manifest "$path" "$service" || return 1
  if [[ "$VP_WORKER_MANIFEST_VERSION" == 2 ]]; then
    return 0
  fi
  local database_secret_id
  database_secret_id="$(
    vp_managed_secret_id \
      "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
      "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
      "$service" "$VP_WORKER_MANIFEST_GENERATION" database
  )" || {
    echo "worker v1 manifest requires manual secret identity evidence" >&2
    return 1
  }
  local admission_secret_id
  admission_secret_id="$(
    vp_managed_secret_id \
      "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
      "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
      "$service" "$VP_WORKER_MANIFEST_GENERATION" admission
  )" || {
    echo "worker v1 manifest requires manual secret identity evidence" >&2
    return 1
  }
  vp_worker_admission_write_manifest \
    "$path" \
    "$service" \
    "$VP_WORKER_MANIFEST_COMMIT" \
    "$VP_WORKER_MANIFEST_IMAGE" \
    "$VP_WORKER_MANIFEST_GENERATION" \
    "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
    "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
    "$database_secret_id" \
    "$admission_secret_id" || return 1
  vp_worker_admission_read_manifest "$path" "$service" || return 1
  [[ "$VP_WORKER_MANIFEST_VERSION" == 2 \
    && "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" == "$database_secret_id" \
    && "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID" == "$admission_secret_id" ]]
}

vp_worker_admission_create_secret() {
  local secret_name="$1"
  local credential_file="$2"
  local service="$3"
  local generation="$4"
  local purpose="$5"
  local mode
  mode="$(vp_worker_redis_marker_file_mode "$credential_file")" || return 1
  if [[ ! -f "$credential_file" || -L "$credential_file" \
    || "$mode" != 400 ]]; then
    echo "worker credential file is absent or invalid" >&2
    return 1
  fi
  VP_WORKER_CREATED_SECRET_ID=""
  if [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
    local recorded_id
    vp_worker_admission_prepared_secret_id \
      "$secret_name" "$service" "$generation" "$purpose" \
      || return 1
    recorded_id="$VP_WORKER_PREPARED_SECRET_ID"
    if [[ "$recorded_id" != - ]]; then
      local verified_recorded_id
      verified_recorded_id="$(
        vp_managed_secret_id \
          "$recorded_id" "$secret_name" \
          "$service" "$generation" "$purpose"
      )" || return 1
      [[ "$verified_recorded_id" == "$recorded_id" ]] || return 1
      VP_WORKER_CREATED_SECRET_ID="$recorded_id"
      return 0
    fi
  fi
  local existing_id
  if existing_id="$(
    vp_managed_secret_id \
      "$secret_name" "$secret_name" \
      "$service" "$generation" "$purpose" 2>/dev/null
  )"; then
    if [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
      vp_worker_admission_record_prepared_secret \
        "$secret_name" "$existing_id" \
        "$service" "$generation" "$purpose" || return 1
    fi
    VP_WORKER_CREATED_SECRET_ID="$existing_id"
    return 0
  fi
  local created_id
  created_id="$(
    docker secret create \
      --label "vp.service=$service" \
      --label "vp.generation=$generation" \
      --label "vp.purpose=$purpose" \
      "$secret_name" - <"$credential_file"
  )" || return 1
  [[ "$created_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local inspected_id
  inspected_id="$(
    vp_managed_secret_id \
      "$created_id" "$secret_name" \
      "$service" "$generation" "$purpose"
  )" || return 1
  [[ "$inspected_id" == "$created_id" ]] || return 1
  if [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
    vp_worker_admission_record_prepared_secret \
      "$secret_name" "$created_id" \
      "$service" "$generation" "$purpose" || return 1
  fi
  VP_WORKER_CREATED_SECRET_ID="$created_id"
}

vp_worker_admission_write_secret_file() {
  local path="$1"
  local value="$2"
  local directory
  directory="$(dirname "$path")" || return 1
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  if [[ -e "$path" ]]; then
    [[ -f "$path" && ! -L "$path" \
      && "$(vp_worker_redis_marker_file_mode "$path")" == 400 \
      && "$(<"$path")" == "$value" ]] || return 1
    return 0
  fi
  local temporary
  temporary="$(mktemp "$directory/.secret.XXXXXX")" || return 1
  if ! chmod 0600 "$temporary" \
    || ! printf '%s\n' "$value" >"$temporary" \
    || ! chmod 0400 "$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
}

vp_worker_admission_operator() {
  local operator_file="$1"
  local image="$2"
  shift 2
  vp_require_pipeline_network_identity || return 1
  local request_mount_option=""
  local request_mount_value=""
  if [[ "${1:-}" == upsert ]]; then
    local request_file="${3:-}"
    [[ "$request_file" = /* && -f "$request_file" && ! -L "$request_file" \
      && "$(vp_worker_redis_marker_file_mode "$request_file")" == 600 ]] \
      || return 1
    request_mount_option="--mount"
    request_mount_value="type=bind,src=$request_file,dst=/run/control/upsert.json,readonly"
    set -- upsert --request-file /run/control/upsert.json
  fi
  if [[ -n "$request_mount_option" ]]; then
    vp_run_python_worker_container \
      "$image" \
      "$operator_file" \
      worker-operator-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      "$request_mount_option" "$request_mount_value" \
      --env WORKER_REGISTRATION_OPERATOR_DATABASE_URL_FILE=/run/secrets/worker-operator-database-url \
      -- \
      python -m app.services.worker_registration_operator_cli \
      "$@" >/dev/null
  else
    vp_run_python_worker_container \
      "$image" \
      "$operator_file" \
      worker-operator-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_REGISTRATION_OPERATOR_DATABASE_URL_FILE=/run/secrets/worker-operator-database-url \
      -- \
      python -m app.services.worker_registration_operator_cli \
      "$@" >/dev/null
  fi
}

vp_worker_control_secret_names() {
  local generation="$1"
  [[ "$generation" =~ ^c-[0-9a-f]{20}$ ]] || return 1
  printf '%s\n' \
    "vp-wc-operator-$generation" \
    "vp-wc-orchestrator-$generation" \
    "vp-wc-staging-$generation" \
    "vp-wc-minio-access-$generation" \
    "vp-wc-minio-secret-$generation" \
    "vp-wc-worker-minio-access-$generation" \
    "vp-wc-worker-minio-secret-$generation"
}

vp_worker_control_secret_refs() {
  local generation="$1"
  [[ "$generation" =~ ^c-[0-9a-f]{20}$ ]] || return 1
  printf '%s|%s\n' \
    "vp-wc-operator-$generation" operator \
    "vp-wc-orchestrator-$generation" orchestrator \
    "vp-wc-staging-$generation" staging-janitor \
    "vp-wc-minio-access-$generation" staging-minio-access \
    "vp-wc-minio-secret-$generation" staging-minio-secret \
    "vp-wc-worker-minio-access-$generation" worker-minio-access \
    "vp-wc-worker-minio-secret-$generation" worker-minio-secret
}

vp_worker_control_write_manifest() {
  local path="$1"
  local generation="$2"
  local image="$3"
  local operator_secret_id="${4:-}"
  local orchestrator_secret_id="${5:-}"
  local staging_secret_id="${6:-}"
  local staging_minio_access_secret_id="${7:-}"
  local staging_minio_secret_secret_id="${8:-}"
  local worker_minio_access_secret_id="${9:-}"
  local worker_minio_secret_secret_id="${10:-}"
  local manifest_version=1
  local provided_ids="$operator_secret_id $orchestrator_secret_id $staging_secret_id $staging_minio_access_secret_id $staging_minio_secret_secret_id $worker_minio_access_secret_id $worker_minio_secret_secret_id"
  if [[ -n "${provided_ids// }" ]]; then
    local candidate_id
    local seen_ids=""
    for candidate_id in $provided_ids; do
      [[ "$candidate_id" =~ ^[a-z0-9]{20,64}$ \
        && " $seen_ids " != *" $candidate_id "* ]] || return 1
      seen_ids="${seen_ids:+$seen_ids }$candidate_id"
    done
    [[ "$(wc -w <<<"$provided_ids" | tr -d ' ')" -eq 7 ]] || return 1
    manifest_version=2
  fi
  vp_worker_control_secret_names "$generation" >/dev/null || return 1
  local operator_secret="vp-wc-operator-$generation"
  local orchestrator_secret="vp-wc-orchestrator-$generation"
  local staging_secret="vp-wc-staging-$generation"
  local staging_minio_access_secret="vp-wc-minio-access-$generation"
  local staging_minio_secret_secret="vp-wc-minio-secret-$generation"
  local worker_minio_access_secret="vp-wc-worker-minio-access-$generation"
  local worker_minio_secret_secret="vp-wc-worker-minio-secret-$generation"
  [[ "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${generation#c-}" == "${image##*:deploy-}"* ]] || return 1
  local directory
  directory="$(dirname "$path")" || return 1
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  local temporary
  temporary="$(mktemp "$directory/.control-manifest.XXXXXX")" \
    || return 1
  if ! chmod 0600 "$temporary" \
    || ! printf '%s\n' \
      "VERSION=$manifest_version" \
      "GENERATION=$generation" \
      "IMAGE=$image" \
      "OPERATOR_DATABASE_SECRET=$operator_secret" \
      "ORCHESTRATOR_DATABASE_SECRET=$orchestrator_secret" \
      "STAGING_DATABASE_SECRET=$staging_secret" \
      "STAGING_MINIO_ACCESS_SECRET=$staging_minio_access_secret" \
      "STAGING_MINIO_SECRET_SECRET=$staging_minio_secret_secret" \
      "WORKER_MINIO_ACCESS_SECRET=$worker_minio_access_secret" \
      "WORKER_MINIO_SECRET_SECRET=$worker_minio_secret_secret" \
      ${operator_secret_id:+"OPERATOR_DATABASE_SECRET_ID=$operator_secret_id"} \
      ${orchestrator_secret_id:+"ORCHESTRATOR_DATABASE_SECRET_ID=$orchestrator_secret_id"} \
      ${staging_secret_id:+"STAGING_DATABASE_SECRET_ID=$staging_secret_id"} \
      ${staging_minio_access_secret_id:+"STAGING_MINIO_ACCESS_SECRET_ID=$staging_minio_access_secret_id"} \
      ${staging_minio_secret_secret_id:+"STAGING_MINIO_SECRET_SECRET_ID=$staging_minio_secret_secret_id"} \
      ${worker_minio_access_secret_id:+"WORKER_MINIO_ACCESS_SECRET_ID=$worker_minio_access_secret_id"} \
      ${worker_minio_secret_secret_id:+"WORKER_MINIO_SECRET_SECRET_ID=$worker_minio_secret_secret_id"} \
      >"$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
  [[ "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]]
}

vp_worker_control_read_manifest() {
  local path="$1"
  if [[ ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    return 1
  fi
  VP_WORKER_CONTROL_MANIFEST_VERSION=""
  VP_WORKER_CONTROL_MANIFEST_GENERATION=""
  VP_WORKER_CONTROL_MANIFEST_IMAGE=""
  VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET=""
  VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID=""
  VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID=""
  local key
  local value
  while IFS='=' read -r key value; do
    [[ -n "$key" && -n "$value" && "$value" != *$'\r'* ]] \
      || return 1
    case "$key" in
      VERSION)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_VERSION" ]] || return 1
        VP_WORKER_CONTROL_MANIFEST_VERSION="$value"
        ;;
      GENERATION)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_GENERATION" ]] || return 1
        VP_WORKER_CONTROL_MANIFEST_GENERATION="$value"
        ;;
      IMAGE)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_IMAGE" ]] || return 1
        VP_WORKER_CONTROL_MANIFEST_IMAGE="$value"
        ;;
      OPERATOR_DATABASE_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET="$value"
        ;;
      ORCHESTRATOR_DATABASE_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET="$value"
        ;;
      STAGING_DATABASE_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET="$value"
        ;;
      STAGING_MINIO_ACCESS_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET="$value"
        ;;
      STAGING_MINIO_SECRET_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET="$value"
        ;;
      WORKER_MINIO_ACCESS_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET="$value"
        ;;
      WORKER_MINIO_SECRET_SECRET)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET="$value"
        ;;
      OPERATOR_DATABASE_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID="$value"
        ;;
      ORCHESTRATOR_DATABASE_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID="$value"
        ;;
      STAGING_DATABASE_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID="$value"
        ;;
      STAGING_MINIO_ACCESS_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID="$value"
        ;;
      STAGING_MINIO_SECRET_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID="$value"
        ;;
      WORKER_MINIO_ACCESS_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID="$value"
        ;;
      WORKER_MINIO_SECRET_SECRET_ID)
        [[ -z "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID" ]] \
          || return 1
        VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID="$value"
        ;;
      *)
        return 1
        ;;
    esac
  done <"$path"
  local expected
  expected="$(
    vp_worker_control_secret_names \
      "$VP_WORKER_CONTROL_MANIFEST_GENERATION"
  )" || return 1
  local actual
  actual="$(
    printf '%s\n' \
      "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET" \
      "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET"
  )"
  local identity_values="$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID $VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID"
  local identity_count=0
  local identity_value
  local seen_identity_values=""
  for identity_value in $identity_values; do
    [[ "$identity_value" =~ ^[a-z0-9]{20,64}$ \
      && " $seen_identity_values " != *" $identity_value "* ]] \
      || return 1
    seen_identity_values="${seen_identity_values:+$seen_identity_values }$identity_value"
    identity_count=$((identity_count + 1))
  done
  [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" =~ ^[12]$ \
    && "$VP_WORKER_CONTROL_MANIFEST_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${VP_WORKER_CONTROL_MANIFEST_GENERATION#c-}" \
      == "${VP_WORKER_CONTROL_MANIFEST_IMAGE##*:deploy-}"* \
    && "$actual" == "$expected" \
    && ( "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 1 \
      && "$identity_count" -eq 0 \
      || "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 2 \
      && "$identity_count" -eq 7 ) ]]
}

vp_worker_control_manifest_secret_refs() {
  [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 2 ]] || return 1
  printf '%s|%s|%s\n' \
    "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID" \
    operator \
    "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID" \
    orchestrator \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID" \
    staging-janitor \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID" \
    staging-minio-access \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID" \
    staging-minio-secret \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID" \
    worker-minio-access \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET" \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID" \
    worker-minio-secret
}

vp_worker_control_require_v2_manifest() {
  local path="$1"
  vp_worker_control_read_manifest "$path" || return 1
  if [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 2 ]]; then
    return 0
  fi
  local generation="$VP_WORKER_CONTROL_MANIFEST_GENERATION"
  local image="$VP_WORKER_CONTROL_MANIFEST_IMAGE"
  local ids=()
  local secret_name
  local purpose
  while IFS='|' read -r secret_name purpose; do
    [[ -n "$secret_name" && -n "$purpose" ]] || return 1
    local secret_id
    secret_id="$(
      vp_managed_secret_id \
        "$secret_name" "$secret_name" \
        vp-worker-control "$generation" "$purpose"
    )" || {
      echo "worker control v1 manifest requires manual secret identity evidence" >&2
      return 1
    }
    ids+=("$secret_id")
  done < <(vp_worker_control_secret_refs "$generation")
  [[ "${#ids[@]}" -eq 7 ]] || return 1
  vp_worker_control_write_manifest \
    "$path" "$generation" "$image" "${ids[@]}" || return 1
  vp_worker_control_read_manifest "$path" || return 1
  [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 2 ]]
}

vp_worker_control_find_v2_manifest() {
  local root="$1"
  local generation="$2"
  local candidate
  for candidate in \
    "$root/control-retirements/$generation.conf" \
    "$root/control-candidates/$generation.conf" \
    "$root/control-current.conf"; do
    [[ -e "$candidate" ]] || continue
    vp_worker_control_require_v2_manifest "$candidate" || return 1
    if [[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == "$generation" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

vp_worker_control_schedule_retirement() {
  local root="$1"
  local image="$2"
  local generation="$3"
  [[ "$root" = /* ]] || return 1
  local directory="$root/control-retirements"
  local journal="$directory/$generation.conf"
  if [[ -e "$journal" ]]; then
    vp_worker_control_require_v2_manifest "$journal" || return 1
    [[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == "$generation" \
      && "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$image" ]]
    return
  fi
  vp_worker_control_find_v2_manifest \
    "$root" "$generation" >/dev/null || return 1
  [[ "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$image" ]] || return 1
  vp_worker_control_write_manifest \
    "$journal" "$generation" "$image" \
    "$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID" \
    "$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID"
}

vp_worker_control_process_retirements() {
  local root="$1"
  local protected_generation="$2"
  [[ "$root" = /* ]] || return 1
  local directory="$root/control-retirements"
  [[ -e "$directory" ]] || return 0
  if [[ ! -d "$directory" || -L "$directory" \
    || "$(vp_worker_redis_marker_file_mode "$directory")" != 700 ]]; then
    return 1
  fi
  local journal
  for journal in "$directory"/*.conf; do
    [[ -e "$journal" ]] || continue
    local basename="${journal##*/}"
    local journal_generation="${basename%.conf}"
    [[ "$basename" == "$journal_generation.conf" \
      && "$journal_generation" =~ ^c-[0-9a-f]{20}$ ]] || return 1
    vp_worker_control_read_manifest "$journal" || return 1
    local image="$VP_WORKER_CONTROL_MANIFEST_IMAGE"
    local generation="$VP_WORKER_CONTROL_MANIFEST_GENERATION"
    [[ "$generation" == "$journal_generation" \
      && "$generation" != "$protected_generation" ]] || return 1
    vp_worker_control_retire_generation \
      "$image" "$generation" "$root" || return 1
    rm -f "$journal" || return 1
  done
}

vp_worker_control_capture_prior() {
  local root="$1"
  VP_WORKER_CONTROL_PRIOR_GENERATION=""
  VP_WORKER_CONTROL_PRIOR_IMAGE=""
  VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=""
  local current="$root/control-current.conf"
  [[ -e "$current" ]] || return 0
  vp_worker_control_require_v2_manifest "$current" || {
    echo "worker control current manifest is invalid" >&2
    return 1
  }
  VP_WORKER_CONTROL_PRIOR_GENERATION="$VP_WORKER_CONTROL_MANIFEST_GENERATION"
  VP_WORKER_CONTROL_PRIOR_IMAGE="$VP_WORKER_CONTROL_MANIFEST_IMAGE"
  VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET="$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET"
  VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET="$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET"
  VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET="$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET"
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET="$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET"
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET="$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET"
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET="$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET"
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET="$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET"
}

vp_worker_admission_prepare_control_roles() {
  local image="$1"
  local commit="$2"
  local root="$3"
  vp_require_pipeline_network_identity || return 1
  local owner_file
  owner_file="$(
    vp_worker_admission_database_credential_file \
      control_role_owner \
      "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker control-role owner database URL file"
  )" || return 1
  local generation="c-${commit:0:20}"
  local state
  state="$(
    vp_python_worker_prepare_controlled_directory "$root/control"
  )" || return 1
  VP_WORKER_CONTROL_GENERATION="$generation"
  VP_WORKER_OPERATOR_DATABASE_SECRET="vp-wc-operator-$generation"
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET="vp-wc-orchestrator-$generation"
  VP_STAGING_JANITOR_DATABASE_SECRET="vp-wc-staging-$generation"
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET="vp-wc-minio-access-$generation"
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET="vp-wc-minio-secret-$generation"
  VP_WORKER_MINIO_ACCESS_SECRET="vp-wc-worker-minio-access-$generation"
  VP_WORKER_MINIO_SECRET_SECRET="vp-wc-worker-minio-secret-$generation"
  VP_WORKER_CONTROL_PREPARED=true
  local candidate="$root/control-candidates/$generation.conf"
  local operator_secret_id=""
  local orchestrator_secret_id=""
  local staging_secret_id=""
  local staging_minio_access_secret_id=""
  local staging_minio_secret_secret_id=""
  local worker_minio_access_secret_id=""
  local worker_minio_secret_secret_id=""
  if [[ -e "$candidate" ]]; then
    vp_worker_control_read_manifest "$candidate" || return 1
    [[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == "$generation" \
      && "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$image" ]] || return 1
    if [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 2 ]]; then
      operator_secret_id="$VP_WORKER_CONTROL_MANIFEST_OPERATOR_DATABASE_SECRET_ID"
      orchestrator_secret_id="$VP_WORKER_CONTROL_MANIFEST_ORCHESTRATOR_DATABASE_SECRET_ID"
      staging_secret_id="$VP_WORKER_CONTROL_MANIFEST_STAGING_DATABASE_SECRET_ID"
      staging_minio_access_secret_id="$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_ACCESS_SECRET_ID"
      staging_minio_secret_secret_id="$VP_WORKER_CONTROL_MANIFEST_STAGING_MINIO_SECRET_SECRET_ID"
      worker_minio_access_secret_id="$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_ACCESS_SECRET_ID"
      worker_minio_secret_secret_id="$VP_WORKER_CONTROL_MANIFEST_WORKER_MINIO_SECRET_SECRET_ID"
    else
      [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 1 ]] || return 1
    fi
  else
    vp_worker_control_write_manifest \
      "$candidate" "$generation" "$image" || return 1
  fi
  local operator_reference="control/$generation/worker-registration-operator-database-url"
  vp_worker_admission_record_authority_intent \
    control vp-worker-control "$generation" \
    "$image" "$generation" "$operator_reference" || return 1
  vp_worker_admission_mark_authority_provisioning \
    control vp-worker-control "$generation" || return 1
  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-control-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$state,dst=/control-state" \
    --env WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-control-owner-database-url \
    -- \
    python -m app.services.worker_control_role_cli \
      provision --generation "$generation" \
      --state-dir /control-state >/dev/null || return 1
  vp_worker_admission_mark_authority_provisioned \
    control vp-worker-control "$generation" || return 1

  vp_worker_admission_create_secret \
    "$VP_WORKER_OPERATOR_DATABASE_SECRET" \
    "$state/$generation/worker-registration-operator-database-url" \
    vp-worker-control "$generation" operator || return 1
  [[ -z "$operator_secret_id" \
    || "$operator_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  operator_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$VP_WORKER_ORCHESTRATOR_DATABASE_SECRET" \
    "$state/$generation/worker-orchestrator-database-url" \
    vp-worker-control "$generation" orchestrator || return 1
  [[ -z "$orchestrator_secret_id" \
    || "$orchestrator_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  orchestrator_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$VP_STAGING_JANITOR_DATABASE_SECRET" \
    "$state/$generation/vp-staging-janitor-database-url" \
    vp-worker-control "$generation" staging-janitor || return 1
  [[ -z "$staging_secret_id" \
    || "$staging_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  staging_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  local minio_access_file="$state/$generation/vp-staging-janitor-minio-access-key"
  local minio_secret_file="$state/$generation/vp-staging-janitor-minio-secret-key"
  vp_worker_admission_write_secret_file \
    "$minio_access_file" "$VP_MINIO_ACCESS_KEY" || return 1
  vp_worker_admission_write_secret_file \
    "$minio_secret_file" "$VP_MINIO_SECRET_KEY" || return 1
  vp_worker_admission_create_secret \
    "$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET" \
    "$minio_access_file" vp-worker-control "$generation" \
    staging-minio-access || return 1
  [[ -z "$staging_minio_access_secret_id" \
    || "$staging_minio_access_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  staging_minio_access_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$VP_STAGING_JANITOR_MINIO_SECRET_SECRET" \
    "$minio_secret_file" vp-worker-control "$generation" \
    staging-minio-secret || return 1
  [[ -z "$staging_minio_secret_secret_id" \
    || "$staging_minio_secret_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  staging_minio_secret_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$VP_WORKER_MINIO_ACCESS_SECRET" \
    "$minio_access_file" vp-worker-control "$generation" \
    worker-minio-access || return 1
  [[ -z "$worker_minio_access_secret_id" \
    || "$worker_minio_access_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  worker_minio_access_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$VP_WORKER_MINIO_SECRET_SECRET" \
    "$minio_secret_file" vp-worker-control "$generation" \
    worker-minio-secret || return 1
  [[ -z "$worker_minio_secret_secret_id" \
    || "$worker_minio_secret_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  worker_minio_secret_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_control_write_manifest \
    "$candidate" \
    "$generation" "$image" \
    "$operator_secret_id" \
    "$orchestrator_secret_id" \
    "$staging_secret_id" \
    "$staging_minio_access_secret_id" \
    "$staging_minio_secret_secret_id" \
    "$worker_minio_access_secret_id" \
    "$worker_minio_secret_secret_id" || return 1
}

vp_worker_admission_prepare_service() {
  local service="$1"
  local image="$2"
  local control_image="$3"
  local commit="$4"
  local root="$5"
  local namespace="${6:-$commit}"
  vp_require_pipeline_network_identity || return 1
  [[ "$namespace" =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  local runtime_state
  runtime_state="$(
    vp_python_worker_prepare_controlled_directory "$root/runtime"
  )" || return 1
  local request_root
  request_root="$(
    vp_python_worker_prepare_controlled_directory \
      "$root/requests/$service"
  )" || return 1
  local candidate_dir="$root/candidates/$namespace"
  local candidate="$candidate_dir/$kind.conf"
  local generation=""
  local database_secret=""
  local admission_secret=""
  local database_secret_id=""
  local admission_secret_id=""
  if vp_worker_admission_read_manifest "$candidate" "$service" \
    && [[ "$VP_WORKER_MANIFEST_COMMIT" == "$commit" \
      && "$VP_WORKER_MANIFEST_IMAGE" == "$image" ]]; then
    generation="$VP_WORKER_MANIFEST_GENERATION"
    database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
    admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
    if [[ "$VP_WORKER_MANIFEST_VERSION" == 2 ]]; then
      database_secret_id="$VP_WORKER_MANIFEST_DATABASE_SECRET_ID"
      admission_secret_id="$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
    else
      [[ "$VP_WORKER_MANIFEST_VERSION" == 1 ]] || return 1
    fi
  else
    [[ ! -e "$candidate" ]] || return 1
    generation="$(vp_worker_admission_new_generation)" || return 1
    database_secret="vp-wr-$kind-db-$generation"
    admission_secret="vp-wr-$kind-admission-$generation"
    vp_worker_admission_write_manifest \
      "$candidate" "$service" "$commit" "$image" "$generation" \
      "$database_secret" "$admission_secret" || return 1
  fi
  vp_worker_admission_track_candidate "$service" || return 1

  local owner_file
  owner_file="$(
    vp_worker_admission_database_credential_file \
      runtime_role_owner \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker runtime-role owner database URL file"
  )" || return 1
  local control_generation="$VP_WORKER_CONTROL_GENERATION"
  local operator_reference="control/$control_generation/worker-registration-operator-database-url"
  vp_worker_admission_record_authority_intent \
    runtime "$service" "$generation" \
    "$control_image" "$control_generation" "$operator_reference" \
    || return 1
  vp_worker_admission_mark_authority_provisioning \
    runtime "$service" "$generation" || return 1
  vp_run_python_worker_container \
    "$control_image" \
    "$owner_file" \
    worker-runtime-owner-database-url \
    /runtime-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state" \
    --env WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-runtime-owner-database-url \
    -- \
    python -m app.services.worker_runtime_role_cli \
      provision --service-name "$service" \
      --generation "$generation" \
      --state-dir /runtime-state >/dev/null || return 1
  vp_worker_admission_mark_authority_provisioned \
    runtime "$service" "$generation" || return 1

  local credential_dir="$runtime_state/$service/$generation"
  vp_worker_admission_create_secret \
    "$database_secret" "$credential_dir/worker-database-url" \
    "$service" "$generation" database || return 1
  [[ -z "$database_secret_id" \
    || "$database_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  database_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_create_secret \
    "$admission_secret" "$credential_dir/worker-admission-token" \
    "$service" "$generation" admission || return 1
  [[ -z "$admission_secret_id" \
    || "$admission_secret_id" == "$VP_WORKER_CREATED_SECRET_ID" ]] \
    || return 1
  admission_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  vp_worker_admission_write_manifest \
    "$candidate" "$service" "$commit" "$image" "$generation" \
    "$database_secret" "$admission_secret" \
    "$database_secret_id" "$admission_secret_id" || return 1
  vp_worker_admission_set_candidate \
    "$service" "$generation" "$database_secret" "$admission_secret" \
    || return 1

  local request_file="$request_root/$generation/upsert.json"
  vp_run_python_worker_container \
    "$control_image" \
    - \
    - \
    /requests \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state,readonly" \
    --mount "type=bind,src=$request_root,dst=/requests" \
    -- \
    python -m app.services.worker_deployment_cli render-request \
      --service-name "$service" \
      --generation "$generation" \
      --release-commit "$commit" \
      --image-identity "$image" \
      --state-dir /runtime-state \
      --request-file "/requests/$generation/upsert.json" \
      --redis-host 10.0.0.150 \
      --redis-port 6380 \
      --redis-database 0 \
      --storage-host 10.0.0.150 \
      --storage-port 9000 \
      --storage-bucket videoprocess >/dev/null || return 1

  local operator_file="$root/control/$VP_WORKER_CONTROL_GENERATION/worker-registration-operator-database-url"
  vp_worker_admission_operator \
    "$operator_file" "$control_image" \
    upsert --request-file "$request_file"
}

vp_worker_admission_load_replay_plan() {
  vp_worker_admission_lock_assert || return 1
  local replay
  replay="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      replay-plan "$VP_WORKER_ADMISSION_LOCK_ROOT" 2>/dev/null
  )" || return 1
  local fields
  fields="$(
    printf '%s\n' "$replay" | python3 -I -c '
import json
import re
import sys

try:
    plan = json.load(sys.stdin)
    required = {
        "active", "allow_new_candidate", "allow_stale_cleanup",
        "namespace", "next_action", "pending_operation", "phase",
        "retirements", "revision", "transaction_id",
    }
    if set(plan) != required:
        raise ValueError
    if plan["active"] is False:
        if plan != {
            "active": False,
            "allow_new_candidate": True,
            "allow_stale_cleanup": True,
            "namespace": None,
            "next_action": "BEGIN",
            "pending_operation": None,
            "phase": None,
            "retirements": [],
            "revision": None,
            "transaction_id": None,
        }:
            raise ValueError
        print("false|-|-|-|BEGIN|-|-|-|-|-|-")
        raise SystemExit(0)
    if (
        plan["active"] is not True
        or plan["allow_new_candidate"] is not False
        or plan["allow_stale_cleanup"] is not False
        or not isinstance(plan["phase"], str)
        or type(plan["revision"]) is not int
        or plan["revision"] < 0
        or re.fullmatch(r"tx-[0-9a-f]{32}", plan["transaction_id"] or "")
        is None
        or not isinstance(plan["next_action"], str)
        or not isinstance(plan["retirements"], list)
    ):
        raise ValueError
    operation = plan["pending_operation"]
    operation_kind = "-"
    operation_id = "-"
    operation_name = "-"
    operation_service = "-"
    operation_generation = "-"
    operation_digest = "-"
    if operation is not None:
        if (
            not isinstance(operation, dict)
            or set(operation) != {
                "operation_id", "kind", "target_phase", "identity"
            }
            or re.fullmatch(
                r"operation-[0-9a-f]{32}",
                operation["operation_id"] or "",
            )
            is None
            or not isinstance(operation["kind"], str)
        ):
            raise ValueError
        identity = operation["identity"]
        if (
            not isinstance(identity, dict)
            or set(identity) != {
                "docker_id", "generation", "kind", "name", "purpose",
                "service", "spec_digest",
            }
            or identity["docker_id"] is not None
            or identity["kind"] != "manifest"
            or identity["purpose"] != "promotion"
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
                identity["name"] or "",
            )
            is None
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
                identity["service"] or "",
            )
            is None
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
                identity["generation"] or "",
            )
            is None
            or re.fullmatch(r"[0-9a-f]{64}", identity["spec_digest"] or "")
            is None
        ):
            raise ValueError
        operation_kind = operation["kind"]
        operation_id = operation["operation_id"]
        operation_name = identity["name"]
        operation_service = identity["service"]
        operation_generation = identity["generation"]
        operation_digest = identity["spec_digest"]
    print("|".join([
        "true", plan["phase"], str(plan["revision"]),
        plan["transaction_id"], plan["next_action"],
        operation_kind, operation_id, operation_name, operation_service,
        operation_generation, operation_digest,
    ]))
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local extra
  IFS='|' read -r \
    VP_WORKER_ADMISSION_REPLAY_ACTIVE \
    VP_WORKER_ADMISSION_REPLAY_PHASE \
    VP_WORKER_ADMISSION_REPLAY_REVISION \
    VP_WORKER_ADMISSION_REPLAY_TRANSACTION_ID \
    VP_WORKER_ADMISSION_REPLAY_NEXT_ACTION \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_ID \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION \
    VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST \
    extra <<<"$fields"
  [[ -z "$extra" ]] || return 1
  if [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true ]]; then
    VP_WORKER_ADMISSION_TRANSACTION_ID="${VP_WORKER_ADMISSION_REPLAY_TRANSACTION_ID}"
  else
    VP_WORKER_ADMISSION_TRANSACTION_ID=""
  fi
}

vp_worker_admission_transition_to() {
  local target_phase="$1"
  local outcome="${2:-}"
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true ]] || return 1
  local arguments=(
    "$VP_WORKER_ADMISSION_LOCK_ROOT"
    "$VP_WORKER_ADMISSION_LOCK_FD"
    "$VP_WORKER_ADMISSION_REPLAY_REVISION"
    "$target_phase"
  )
  if [[ -n "$outcome" ]]; then
    arguments+=("$outcome")
  fi
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" transition \
    "${arguments[@]}" >/dev/null
}

vp_worker_admission_load_durable_state() {
  vp_worker_admission_lock_assert || return 1
  local state
  state="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      replay-state "$VP_WORKER_ADMISSION_LOCK_ROOT" 2>/dev/null
  )" || return 1
  local fields
  fields="$(
    printf '%s\n' "$state" | python3 -I -c '
import json
import re
import sys

try:
    state = json.load(sys.stdin)
    rollback = state["rollback"]
    if (
        state["schema"] != 3
        or type(state["revision"]) is not int
        or state["revision"] < 0
        or not isinstance(state["phase"], str)
        or state["retiring_outcome"]
        not in {None, "succeeded", "rolled_back", "manual"}
        or set(rollback) != {
            "attempt", "namespace", "marker_generation",
            "control", "marker", "workers",
        }
        or type(rollback["attempt"]) is not int
        or rollback["attempt"] < 0
    ):
        raise ValueError
    namespace = rollback["namespace"] or "-"
    marker_generation = rollback["marker_generation"] or "-"
    retiring_outcome = state["retiring_outcome"] or "-"
    if rollback["attempt"] == 0:
        if namespace != "-" or marker_generation != "-":
            raise ValueError
    elif (
        re.fullmatch(r"rollback-[1-9][0-9]{1,19}", namespace) is None
        or re.fullmatch(
            r"m-rb-[0-9a-f]{12}-[1-9][0-9]*",
            marker_generation,
        )
        is None
    ):
        raise ValueError
    print("|".join([
        str(state["revision"]), state["phase"], retiring_outcome,
        str(rollback["attempt"]), namespace, marker_generation,
    ]))
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local extra
  IFS='|' read -r \
    VP_WORKER_ADMISSION_DURABLE_REVISION \
    VP_WORKER_ADMISSION_DURABLE_PHASE \
    VP_WORKER_ADMISSION_DURABLE_RETIRING_OUTCOME \
    VP_WORKER_ADMISSION_DURABLE_ROLLBACK_ATTEMPT \
    VP_WORKER_ADMISSION_DURABLE_ROLLBACK_NAMESPACE \
    VP_WORKER_ADMISSION_DURABLE_ROLLBACK_MARKER_GENERATION \
    extra <<<"$fields"
  [[ -z "$extra" ]]
}

vp_worker_admission_recovery_state() {
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    replay-state "$VP_WORKER_ADMISSION_LOCK_ROOT"
}

vp_worker_admission_hydrate_recovery_context() {
  vp_worker_admission_lock_assert || return 1
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  VP_WORKER_ADMISSION_PREPARED=false
  VP_WORKER_ADMISSION_COMMIT=""
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=""
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION=""
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION=""
  VP_WORKER_REDIS_CONTROL_SECRET=""
  VP_WORKER_REDIS_FFMPEG_GO_SECRET=""
  VP_WORKER_REDIS_FFMPEG_SECRET=""
  VP_WORKER_REDIS_VISION_SECRET=""
  VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET=""
  VP_WORKER_REDIS_WATCHER_SECRET=""
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET=""
  VP_WORKER_REDIS_CONTROL_SECRET_ID=""
  VP_WORKER_REDIS_FFMPEG_GO_SECRET_ID=""
  VP_WORKER_REDIS_FFMPEG_SECRET_ID=""
  VP_WORKER_REDIS_VISION_SECRET_ID=""
  VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET_ID=""
  VP_WORKER_REDIS_WATCHER_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID=""
  VP_WORKER_CONTROL_PREPARED=false
  VP_WORKER_CONTROL_GENERATION=""
  VP_WORKER_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET=""
  VP_STAGING_JANITOR_DATABASE_SECRET=""
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=""
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET=""
  VP_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_MINIO_SECRET_SECRET=""
  VP_WORKER_ADMISSION_CONTROL_IMAGE=""
  VP_WORKER_CONTROL_PRIOR_GENERATION=""
  VP_WORKER_CONTROL_PRIOR_IMAGE=""
  VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256=""
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_CONFIG_SHA256=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_CRON_SHA256=""
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
  VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID=""
  VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID=""
  VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=""
  VP_WORKER_ADMISSION_JANITOR_GENERATION=""
  VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST=""
  VP_WORKER_ADMISSION_RECOVERY_PHASE=""
  VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED=false
  VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD=false
  VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD=false
  VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND=""
  VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS=""
  VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS=""
  VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES=""
  VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE=""
  VP_BACKEND_MIGRATION_APPLIED=false
  VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS=""
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES=""
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS=""
  local state
  state="$(vp_worker_admission_recovery_state)" || return 1
  local records
  records="$(
    printf '%s\n' "$state" | python3 -I -c '
import json
import re
import sys

APP_SERVICES = (
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
WORKER_SERVICES = {
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
}
RECOVERY_PHASES = {
    "FORWARD_APPLYING",
    "FORWARD_VERIFIED",
    "WORKERS_PROMOTED",
    "MARKER_PROMOTED",
    "CONTROL_PROMOTED",
    "ROLLBACK_PREPARING",
    "ROLLBACK_APPLYING",
    "ROLLBACK_VERIFIED",
    "ROLLBACK_WORKERS_PROMOTED",
    "ROLLBACK_MARKER_PROMOTED",
    "ROLLBACK_CONTROL_PROMOTED",
    "CANDIDATE_RESTORE_REQUIRED",
    "CANDIDATE_RESTORING",
    "CANDIDATE_RESTORED",
    "RETIRING",
}
ROLLBACK_CONTEXT_PHASES = {
    "ROLLBACK_APPLYING",
    "ROLLBACK_VERIFIED",
    "ROLLBACK_WORKERS_PROMOTED",
    "ROLLBACK_MARKER_PROMOTED",
    "ROLLBACK_CONTROL_PROMOTED",
}
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}")
DOCKER_ID = re.compile(r"[a-z0-9]{12,64}")
DIGEST = re.compile(r"[0-9a-f]{64}")
NAMESPACE = re.compile(r"[a-z0-9][a-z0-9-]{0,127}")
ROLLBACK_NAMESPACE = re.compile(r"rollback-[1-9][0-9]{1,19}")
MARKER_GENERATION = re.compile(r"m-rb-[0-9a-f]{12}-[1-9][0-9]*")
RUNTIME_ROLES = (
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
CONTROL_PURPOSES = (
    "operator",
    "orchestrator",
    "staging-janitor",
    "staging-minio-access",
    "staging-minio-secret",
    "worker-minio-access",
    "worker-minio-secret",
)


def matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def service_snapshots(value):
    if not isinstance(value, list):
        raise ValueError
    result = {}
    for item in value:
        if not isinstance(item, dict):
            raise ValueError
        name = item.get("name")
        existed = item.get("existed")
        if name not in APP_SERVICES or name in result or type(existed) is not bool:
            raise ValueError
        image = item.get("image")
        docker_id = item.get("docker_service_id")
        digest = item.get("spec_digest")
        if existed:
            if (
                not matches(IMAGE, image)
                or not matches(DOCKER_ID, docker_id)
                or not matches(DIGEST, digest)
            ):
                raise ValueError
        elif image is not None or docker_id is not None or digest is not None:
            raise ValueError
        result[name] = item
    return result


def secret_identity(value, purpose, service, generation):
    if not isinstance(value, dict):
        raise ValueError
    name = value.get("name")
    docker_id = value.get("docker_secret_id")
    if (
        not matches(NAME, name)
        or not matches(DOCKER_ID, docker_id)
        or value.get("purpose") != purpose
    ):
        raise ValueError
    if "service" in value and value["service"] != service:
        raise ValueError
    if "generation" in value and value["generation"] != str(generation):
        raise ValueError
    return name, docker_id


def workers(value):
    if not isinstance(value, list):
        raise ValueError
    result = {}
    for worker in value:
        if not isinstance(worker, dict):
            raise ValueError
        service = worker.get("service")
        generation = worker.get("generation")
        stage = worker.get("applied_stage")
        key = (service, generation)
        if (
            service not in APP_SERVICES
            or type(generation) is not int
            or generation < 1
            or stage not in {"pending", "prepared", "applied", "verified"}
            or key in result
        ):
            raise ValueError
        database = secret_identity(
            worker.get("database_secret"), "database", service, generation
        )
        admission = secret_identity(
            worker.get("admission_secret"), "admission", service, generation
        )
        if database[1] == admission[1]:
            raise ValueError
        service_id = worker.get("docker_service_id")
        digest = worker.get("target_spec_digest")
        applied = stage in {"applied", "verified"}
        if applied:
            if not matches(DOCKER_ID, service_id) or not matches(DIGEST, digest):
                raise ValueError
        elif service_id is not None or digest is not None:
            raise ValueError
        result[key] = (worker, database, admission)
    return result


def control_selection(value):
    if not isinstance(value, dict) or set(value) != {
        "generation", "image", "manifest_sha256", "secrets"
    }:
        raise ValueError
    generation = value["generation"]
    image = value["image"]
    if (
        not matches(NAME, generation)
        or not matches(IMAGE, image)
        or not matches(DIGEST, value["manifest_sha256"])
        or not isinstance(value["secrets"], list)
    ):
        raise ValueError
    references = {}
    secret_names = set()
    secret_ids = set()
    for reference in value["secrets"]:
        if not isinstance(reference, dict) or set(reference) != {
            "name", "docker_secret_id", "service", "generation", "purpose"
        }:
            raise ValueError
        purpose = reference["purpose"]
        name = reference["name"]
        secret_id = reference["docker_secret_id"]
        if (
            purpose not in CONTROL_PURPOSES
            or purpose in references
            or reference["service"] != "vp-worker-control"
            or reference["generation"] != generation
            or not matches(NAME, name)
            or not matches(DOCKER_ID, secret_id)
            or name in secret_names
            or secret_id in secret_ids
        ):
            raise ValueError
        references[purpose] = (name, secret_id)
        secret_names.add(name)
        secret_ids.add(secret_id)
    if set(references) != set(CONTROL_PURPOSES):
        raise ValueError
    return generation, image, references


def failed_control_selection(value):
    if not isinstance(value, dict) or set(value) != {
        "generation", "image", "config_sha256", "cron_sha256"
    }:
        raise ValueError
    generation = value["generation"]
    image = value["image"]
    if (
        not matches(NAME, generation)
        or not matches(IMAGE, image)
        or not matches(DIGEST, value["config_sha256"])
        or not matches(DIGEST, value["cron_sha256"])
    ):
        raise ValueError
    return generation, image, value["config_sha256"], value["cron_sha256"]


def marker_selection(value):
    if not isinstance(value, dict) or set(value) != {
        "generation", "image", "config_sha256", "cron_sha256", "secrets"
    }:
        raise ValueError
    generation = value["generation"]
    image = value["image"]
    if (
        not matches(NAME, generation)
        or not matches(IMAGE, image)
        or not matches(DIGEST, value["config_sha256"])
        or not matches(DIGEST, value["cron_sha256"])
        or not isinstance(value["secrets"], list)
    ):
        raise ValueError
    expected_purposes = {
        "readiness-database",
        "janitor-database",
        "repair-database",
        "readiness-redis",
        "janitor-redis",
    }
    references = {}
    secret_names = set()
    secret_ids = set()
    for reference in value["secrets"]:
        if not isinstance(reference, dict) or set(reference) != {
            "name", "docker_secret_id", "service", "generation", "purpose"
        }:
            raise ValueError
        purpose = reference["purpose"]
        name = reference["name"]
        secret_id = reference["docker_secret_id"]
        if (
            purpose not in expected_purposes
            or purpose in references
            or not matches(NAME, name)
            or not matches(DOCKER_ID, secret_id)
            or name in secret_names
            or secret_id in secret_ids
        ):
            raise ValueError
        if purpose.endswith("-database"):
            database_purpose = purpose.removesuffix("-database")
            if (
                reference["service"] != "worker-redis-marker-control"
                or reference["generation"] != generation
                or name != f"vp-wrm-{database_purpose}-db-{generation}"
            ):
                raise ValueError
        references[purpose] = reference
        secret_names.add(name)
        secret_ids.add(secret_id)
    if set(references) != expected_purposes:
        raise ValueError
    return (
        generation,
        image,
        value["config_sha256"],
        value["cron_sha256"],
        references,
    )


def janitor_service_identity(value):
    if not isinstance(value, dict) or set(value) != {
        "name", "docker_service_id", "generation", "spec_digest"
    }:
        raise ValueError
    if (
        value["name"] != "vp-staging-object-janitor"
        or not matches(DOCKER_ID, value["docker_service_id"])
        or not matches(NAME, value["generation"])
        or not matches(DIGEST, value["spec_digest"])
    ):
        raise ValueError
    return (
        value["docker_service_id"],
        value["generation"],
        value["spec_digest"],
    )


try:
    state = json.load(sys.stdin)
    if type(state.get("schema")) is not int or state["schema"] != 3:
        raise ValueError
    phase = state.get("phase")
    commit = state.get("target_commit")
    transaction_id = state.get("transaction_id")
    if (
        phase not in RECOVERY_PHASES
        or not matches(re.compile(r"[0-9a-f]{40}"), commit)
        or not matches(re.compile(r"tx-[0-9a-f]{32}"), transaction_id)
    ):
        raise ValueError
    retiring_outcome = state.get("retiring_outcome")
    if phase == "RETIRING":
        if retiring_outcome not in {"succeeded", "rolled_back"}:
            raise ValueError
    elif retiring_outcome is not None:
        raise ValueError
    rollback_context = phase in ROLLBACK_CONTEXT_PHASES or (
        phase == "RETIRING" and retiring_outcome == "rolled_back"
    )

    baseline = state.get("baseline")
    failed_forward = state.get("failed_forward")
    forward = state.get("forward")
    rollback = state.get("rollback")
    if not all(isinstance(value, dict) for value in (
        baseline, failed_forward, forward, rollback
    )):
        raise ValueError
    janitor = state.get("janitor")
    if not isinstance(janitor, dict) or set(janitor) != {"service"}:
        raise ValueError
    janitor_service = janitor.get("service")
    if janitor_service is not None:
        janitor_service = janitor_service_identity(janitor_service)
    authorities = state.get("authorities")
    prepared_secrets = state.get("prepared_secrets")
    if not isinstance(authorities, list) or not isinstance(prepared_secrets, list):
        raise ValueError
    if baseline.get("captured") is not True:
        raise ValueError
    baseline_kind = baseline.get("kind")
    if baseline_kind not in {"managed", "legacy_no_control"}:
        raise ValueError
    failed_captured = failed_forward.get("captured")
    if type(failed_captured) is not bool:
        raise ValueError

    app_progress = state.get("app_progress")
    if not isinstance(app_progress, dict) or set(app_progress) != {
        "schema",
        "transaction_id",
        "target_commit",
        "attempted_services",
        "migration_state",
    }:
        raise ValueError
    attempted_services = app_progress["attempted_services"]
    migration_state = app_progress["migration_state"]
    if (
        app_progress["schema"] != 1
        or app_progress["transaction_id"] != transaction_id
        or app_progress["target_commit"] != commit
        or not isinstance(attempted_services, list)
        or len(attempted_services) != len(set(attempted_services))
        or any(service not in APP_SERVICES for service in attempted_services)
        or migration_state not in {"pending", "applying", "applied"}
    ):
        raise ValueError

    baseline_services = service_snapshots(baseline.get("services"))
    failed_services = service_snapshots(failed_forward.get("services"))
    if failed_captured and set(failed_services) != set(attempted_services):
        raise ValueError
    forward_workers = workers(forward.get("workers"))
    rollback_workers = workers(rollback.get("workers", []))
    baseline_control = baseline.get("control")
    forward_control = forward.get("control")
    rollback_control = rollback.get("control")
    failed_control = failed_forward.get("control")
    forward_marker = forward.get("marker")
    rollback_marker = rollback.get("marker")
    early_forward = phase == "FORWARD_APPLYING" and forward_marker is None
    if baseline_kind == "managed":
        if baseline_control is None:
            raise ValueError
        baseline_control = control_selection(baseline_control)
    elif baseline_control is not None:
        raise ValueError
    if forward_control is not None:
        forward_control = control_selection(forward_control)
    if rollback_control is not None:
        rollback_control = control_selection(rollback_control)
    if rollback_context and rollback_control is None:
        raise ValueError
    if failed_control is not None:
        failed_control = failed_control_selection(failed_control)
    if forward_marker is not None:
        forward_marker = marker_selection(forward_marker)
    if rollback_marker is not None:
        rollback_marker = marker_selection(rollback_marker)
    runtime_redis = state.get("runtime_redis")
    if not isinstance(runtime_redis, dict) or set(runtime_redis) != set(RUNTIME_ROLES):
        raise ValueError
    runtime_names = set()
    runtime_ids = set()
    runtime_generations = set()
    runtime_records = []
    runtime_identities = {}
    for role in RUNTIME_ROLES:
        reference = runtime_redis[role]
        if not isinstance(reference, dict) or set(reference) != {
            "runtime_generation", "secret_name", "docker_secret_id"
        }:
            raise ValueError
        runtime_generation = reference["runtime_generation"]
        secret_name = reference["secret_name"]
        secret_id = reference["docker_secret_id"]
        if (
            not matches(re.compile(r"[0-9a-f]{40}"), runtime_generation)
            or not matches(NAME, secret_name)
            or not matches(DOCKER_ID, secret_id)
            or secret_name in runtime_names
            or secret_id in runtime_ids
        ):
            raise ValueError
        runtime_names.add(secret_name)
        runtime_ids.add(secret_id)
        runtime_generations.add(runtime_generation)
        runtime_records.append((role, runtime_generation, secret_name, secret_id))
        runtime_identities[role] = (runtime_generation, secret_name, secret_id)
    if len(runtime_generations) != 1:
        raise ValueError
    for selection in (forward_marker, rollback_marker):
        if selection is None:
            continue
        marker_generation, _image, _config, _cron, references = selection
        for role in ("readiness", "janitor"):
            reference = references[f"{role}-redis"]
            if (
                reference["service"] != "vp-worker-redis-runtime"
                or (
                    reference["generation"],
                    reference["name"],
                    reference["docker_secret_id"],
                ) != runtime_identities[role]
            ):
                raise ValueError
    forward_namespace = forward.get("namespace")
    rollback_namespace = rollback.get("namespace")
    marker_generation = rollback.get("marker_generation")
    if not matches(NAMESPACE, forward_namespace) or forward_namespace != commit:
        raise ValueError
    if rollback_namespace is not None and not matches(
        ROLLBACK_NAMESPACE, rollback_namespace
    ):
        raise ValueError
    if marker_generation is not None and not matches(
        MARKER_GENERATION, marker_generation
    ):
        raise ValueError
    if (rollback_namespace is None) != (marker_generation is None):
        raise ValueError
    if rollback_marker is not None and rollback_marker[0] != marker_generation:
        raise ValueError
    if phase == "ROLLBACK_APPLYING" and rollback_namespace is None:
        raise ValueError
    if phase.startswith("ROLLBACK") or phase.startswith("CANDIDATE"):
        if failed_captured is not True:
            raise ValueError
    if early_forward:
        if (
            forward_control is not None
            or forward_workers
            or janitor_service is not None
        ):
            raise ValueError
        for authority in authorities:
            if (
                not isinstance(authority, dict)
                or authority.get("kind") != "marker"
                or authority.get("service") != "worker-redis-marker-control"
                or authority.get("state")
                not in {"planned", "provisioning", "provisioned"}
                or not matches(NAME, authority.get("generation"))
            ):
                raise ValueError
        for reference in prepared_secrets:
            if not isinstance(reference, dict):
                raise ValueError
            marker_secret = (
                reference.get("service") == "worker-redis-marker-control"
                and reference.get("purpose")
                in {
                    "readiness-database",
                    "janitor-database",
                    "repair-database",
                }
            )
            vision_secret = (
                reference.get("service") == "vision-cutover"
                and reference.get("purpose")
                in {"safety-database", "final-safety-database"}
            )
            if not marker_secret and not vision_secret:
                raise ValueError
    elif forward_marker is None:
        raise ValueError
    if phase != "FORWARD_APPLYING" and (
        forward_control is None or forward_marker is None
    ):
        raise ValueError

    forward_worker_services = {
        service for service, _generation in forward_workers
    }
    fully_prepared_forward = (
        forward_control is not None
        and forward_marker is not None
        and forward_worker_services == WORKER_SERVICES
        and all(
            authority.get("state") == "provisioned"
            for authority in authorities
        )
    )
    partial_forward = (
        phase == "FORWARD_APPLYING" and not fully_prepared_forward
    )
    if partial_forward and (
        janitor_service is not None
        or any(
            worker[0]["applied_stage"] in {"applied", "verified"}
            for worker in forward_workers.values()
        )
    ):
        raise ValueError

    print("|".join((
        "meta",
        phase,
        str(failed_captured).lower(),
        commit,
        forward_namespace,
        rollback_namespace or "-",
        marker_generation or "-",
        baseline_kind,
        transaction_id,
        "rollback" if rollback_context else "forward",
        str(early_forward).lower(),
        str(partial_forward).lower(),
    )))
    for role, runtime_generation, secret_name, secret_id in runtime_records:
        print("|".join((
            "runtime", role, runtime_generation, secret_name, secret_id
        )))
    for scope, selection in (
        ("baseline", baseline_control),
        ("forward", forward_control),
        ("rollback", rollback_control),
    ):
        if selection is None:
            continue
        generation, image, references = selection
        print("|".join((
            "control",
            scope,
            generation,
            image,
            *(references[purpose][0] for purpose in CONTROL_PURPOSES),
        )))
    if failed_control is not None:
        print("failed-control|{}|{}|{}|{}".format(*failed_control))
    if janitor_service is not None:
        print("janitor|{}|{}|{}".format(*janitor_service))
    for scope, selection in (
        ("forward", forward_marker),
        ("rollback", rollback_marker),
    ):
        if selection is None:
            continue
        generation, image, config_sha256, cron_sha256, references = selection
        print("|".join((
            "marker",
            scope,
            generation,
            image,
            config_sha256,
            cron_sha256,
            references["readiness-database"]["docker_secret_id"],
            references["janitor-database"]["docker_secret_id"],
            references["repair-database"]["docker_secret_id"],
        )))
    print(f"migration|{migration_state}")
    attempted = set(attempted_services)
    for service in APP_SERVICES:
        snapshot = baseline_services.get(service)
        if snapshot is not None and snapshot["existed"]:
            print("|".join((
                "snapshot",
                service,
                snapshot["docker_service_id"],
                snapshot["image"],
                snapshot["spec_digest"],
            )))
        if snapshot is not None and service in WORKER_SERVICES:
            if snapshot["existed"]:
                print("|".join((
                    "baseline-worker",
                    service,
                    "true",
                    snapshot["docker_service_id"],
                    snapshot["image"],
                    snapshot["spec_digest"],
                )))
            else:
                print(f"baseline-worker|{service}|false|-|-|-")
        if service in attempted:
            print(f"attempted|{service}")
    for service in APP_SERVICES:
        for (worker_service, generation), (worker, database, admission) in (
            forward_workers.items()
        ):
            if worker_service != service:
                continue
            print("|".join((
                "failed-candidate",
                service,
                str(generation),
                database[0],
                database[1],
                admission[0],
                admission[1],
            )))
            if worker["applied_stage"] in {"applied", "verified"}:
                print("|".join((
                    "candidate-identity",
                    service,
                    str(generation),
                    worker["docker_service_id"],
                    worker["target_spec_digest"],
                )))
                print("|".join((
                    "candidate-service-record",
                    service,
                    str(generation),
                    worker["docker_service_id"],
                )))
            print(f"candidate-service|forward|{service}")
    for service in APP_SERVICES:
        for worker_service, _generation in rollback_workers:
            if worker_service == service:
                print(f"candidate-service|rollback|{service}")
except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1

  local recovery_commit=""
  local recovery_forward_namespace=""
  local recovery_rollback_namespace=""
  local recovery_marker_generation=""
  local recovery_transaction_id=""
  local recovery_promotion_context=""
  local forward_candidate_services=""
  local rollback_candidate_services=""
  local recovery_forward_control=""
  local recovery_rollback_control=""
  local recovery_forward_marker=""
  local recovery_rollback_marker=""
  local seen_meta=false
  local line
  while IFS= read -r line; do
    [[ -n "$line" && "$line" == *'|'* ]] || return 1
    local record_type="${line%%|*}"
    local payload="${line#*|}"
    local first=""
    local second=""
    local third=""
    local fourth=""
    local fifth=""
    local sixth=""
    local seventh=""
    local eighth=""
    local ninth=""
    local tenth=""
    local eleventh=""
    local extra=""
    case "$record_type" in
      meta)
        [[ "$seen_meta" == false ]] || return 1
        IFS='|' read -r \
          first second third fourth fifth sixth seventh eighth ninth tenth \
          eleventh extra <<<"$payload"
        [[ -z "$extra" \
          && "$first" =~ ^(FORWARD_APPLYING|FORWARD_VERIFIED|WORKERS_PROMOTED|MARKER_PROMOTED|CONTROL_PROMOTED|ROLLBACK_PREPARING|ROLLBACK_APPLYING|ROLLBACK_VERIFIED|ROLLBACK_WORKERS_PROMOTED|ROLLBACK_MARKER_PROMOTED|ROLLBACK_CONTROL_PROMOTED|CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING|CANDIDATE_RESTORED|RETIRING)$ \
          && "$second" =~ ^(true|false)$ \
          && "$third" =~ ^[0-9a-f]{40}$ \
          && "$fourth" =~ ^[a-z0-9][a-z0-9-]{0,127}$ \
          && ( "$fifth" == - \
            || "$fifth" =~ ^rollback-[1-9][0-9]{1,19}$ ) \
          && ( "$sixth" == - \
            || "$sixth" =~ ^m-rb-[0-9a-f]{12}-[1-9][0-9]*$ ) \
          && "$seventh" =~ ^(managed|legacy_no_control)$ \
          && "$eighth" =~ ^tx-[0-9a-f]{32}$ \
          && "$ninth" =~ ^(forward|rollback)$ \
          && "$tenth" =~ ^(true|false)$ \
          && "$eleventh" =~ ^(true|false)$ ]] \
          || return 1
        VP_WORKER_ADMISSION_RECOVERY_PHASE="$first"
        VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED="$second"
        recovery_commit="$third"
        recovery_forward_namespace="$fourth"
        recovery_rollback_namespace="$fifth"
        recovery_marker_generation="$sixth"
        VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND="$seventh"
        recovery_transaction_id="$eighth"
        recovery_promotion_context="$ninth"
        VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD="$tenth"
        VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD="$eleventh"
        seen_meta=true
        ;;
      baseline-worker)
        IFS='|' read -r first second third fourth fifth extra <<<"$payload"
        [[ -z "$extra" ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS="${VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS:+$VP_WORKER_ADMISSION_RECOVERY_BASELINE_WORKER_RECORDS$'\n'}$payload"
        ;;
      snapshot)
        IFS='|' read -r first second third fourth extra <<<"$payload"
        [[ -z "$extra" \
          && -n "$first" \
          && "$second" =~ ^[a-z0-9]{12,64}$ \
          && -n "$third" \
          && "$fourth" =~ ^[0-9a-f]{64}$ ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS="${VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS:+$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS$'\n'}$first|$second|$third|$fourth"
        ;;
      runtime)
        IFS='|' read -r first second third fourth extra <<<"$payload"
        [[ -z "$extra" \
          && "$second" =~ ^[0-9a-f]{40}$ \
          && "$third" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
          && "$fourth" =~ ^[a-z0-9]{12,64}$ ]] || return 1
        if [[ -n "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
          && "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" != "$second" ]]; then
          return 1
        fi
        VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$second"
        case "$first" in
          control)
            VP_WORKER_REDIS_CONTROL_SECRET="$third"
            VP_WORKER_REDIS_CONTROL_SECRET_ID="$fourth"
            ;;
          ffmpeg_go)
            VP_WORKER_REDIS_FFMPEG_GO_SECRET="$third"
            VP_WORKER_REDIS_FFMPEG_GO_SECRET_ID="$fourth"
            ;;
          ffmpeg)
            VP_WORKER_REDIS_FFMPEG_SECRET="$third"
            VP_WORKER_REDIS_FFMPEG_SECRET_ID="$fourth"
            ;;
          vision)
            VP_WORKER_REDIS_VISION_SECRET="$third"
            VP_WORKER_REDIS_VISION_SECRET_ID="$fourth"
            ;;
          youtube_publisher)
            VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET="$third"
            VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET_ID="$fourth"
            ;;
          watcher)
            VP_WORKER_REDIS_WATCHER_SECRET="$third"
            VP_WORKER_REDIS_WATCHER_SECRET_ID="$fourth"
            ;;
          readiness)
            VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET="$third"
            VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID="$fourth"
            ;;
          janitor)
            VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET="$third"
            VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID="$fourth"
            ;;
          repair)
            VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET="$third"
            VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID="$fourth"
            ;;
          *) return 1 ;;
        esac
        ;;
      control)
        local control_scope=""
        local control_generation=""
        local control_image=""
        local control_operator=""
        local control_orchestrator=""
        local control_staging=""
        local control_staging_minio_access=""
        local control_staging_minio_secret=""
        local control_worker_minio_access=""
        local control_worker_minio_secret=""
        IFS='|' read -r \
          control_scope control_generation control_image \
          control_operator control_orchestrator control_staging \
          control_staging_minio_access control_staging_minio_secret \
          control_worker_minio_access control_worker_minio_secret extra \
          <<<"$payload"
        [[ -z "$extra" \
          && "$control_scope" =~ ^(baseline|forward|rollback)$ \
          && -n "$control_generation" \
          && -n "$control_image" \
          && -n "$control_operator" \
          && -n "$control_orchestrator" \
          && -n "$control_staging" \
          && -n "$control_staging_minio_access" \
          && -n "$control_staging_minio_secret" \
          && -n "$control_worker_minio_access" \
          && -n "$control_worker_minio_secret" ]] || return 1
        local control_selection="$control_generation|$control_image|$control_operator|$control_orchestrator|$control_staging|$control_staging_minio_access|$control_staging_minio_secret|$control_worker_minio_access|$control_worker_minio_secret"
        case "$control_scope" in
          baseline)
            VP_WORKER_CONTROL_PRIOR_GENERATION="$control_generation"
            VP_WORKER_CONTROL_PRIOR_IMAGE="$control_image"
            VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET="$control_operator"
            VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET="$control_orchestrator"
            VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET="$control_staging"
            VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET="$control_staging_minio_access"
            VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET="$control_staging_minio_secret"
            VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET="$control_worker_minio_access"
            VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET="$control_worker_minio_secret"
            ;;
          forward) recovery_forward_control="$control_selection" ;;
          rollback) recovery_rollback_control="$control_selection" ;;
        esac
        ;;
      failed-control)
        IFS='|' read -r first second third fourth extra <<<"$payload"
        [[ -z "$extra" && -n "$first" && -n "$second" \
          && "$third" =~ ^[0-9a-f]{64}$ \
          && "$fourth" =~ ^[0-9a-f]{64}$ ]] || return 1
        VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$first"
        VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$second"
        VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256="$third"
        VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256="$fourth"
        ;;
      janitor)
        IFS='|' read -r first second third extra <<<"$payload"
        [[ -z "$extra" \
          && "$first" =~ ^[a-z0-9]{12,64}$ \
          && "$second" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
          && "$third" =~ ^[0-9a-f]{64}$ ]] || return 1
        VP_WORKER_ADMISSION_JANITOR_SERVICE_ID="$first"
        VP_WORKER_ADMISSION_JANITOR_GENERATION="$second"
        VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST="$third"
        ;;
      marker)
        local marker_scope=""
        local marker_generation=""
        local marker_image=""
        local marker_config_sha256=""
        local marker_cron_sha256=""
        local marker_readiness_database_id=""
        local marker_janitor_database_id=""
        local marker_repair_database_id=""
        IFS='|' read -r \
          marker_scope marker_generation marker_image \
          marker_config_sha256 marker_cron_sha256 \
          marker_readiness_database_id marker_janitor_database_id \
          marker_repair_database_id extra <<<"$payload"
        [[ -z "$extra" \
          && "$marker_scope" =~ ^(forward|rollback)$ \
          && "$marker_generation" \
            =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
          && "$marker_image" \
            =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
          && "$marker_config_sha256" =~ ^[0-9a-f]{64}$ \
          && "$marker_cron_sha256" =~ ^[0-9a-f]{64}$ \
          && "$marker_readiness_database_id" =~ ^[a-z0-9]{12,64}$ \
          && "$marker_janitor_database_id" =~ ^[a-z0-9]{12,64}$ \
          && "$marker_repair_database_id" =~ ^[a-z0-9]{12,64}$ ]] \
          || return 1
        local marker_selection="$marker_generation|$marker_image|$marker_config_sha256|$marker_cron_sha256|$marker_readiness_database_id|$marker_janitor_database_id|$marker_repair_database_id"
        case "$marker_scope" in
          forward) recovery_forward_marker="$marker_selection" ;;
          rollback) recovery_rollback_marker="$marker_selection" ;;
        esac
        ;;
      attempted)
        IFS='|' read -r first extra <<<"$payload"
        [[ -z "$extra" && -n "$first" ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES="${VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES:+$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES }$first"
        ;;
      migration)
        IFS='|' read -r first extra <<<"$payload"
        [[ -z "$extra" && "$first" =~ ^(pending|applying|applied)$ \
          && -z "$VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE" ]] \
          || return 1
        VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE="$first"
        case "$first" in
          pending) VP_BACKEND_MIGRATION_APPLIED=false ;;
          applying|applied) VP_BACKEND_MIGRATION_APPLIED=true ;;
        esac
        ;;
      failed-candidate)
        IFS='|' read -r \
          first second third fourth fifth sixth extra <<<"$payload"
        [[ -z "$extra" ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS="${VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS:+$VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS$'\n'}$payload"
        ;;
      candidate-identity)
        IFS='|' read -r first second third fourth extra <<<"$payload"
        [[ -z "$extra" ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES="${VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES:+$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES$'\n'}$payload"
        ;;
      candidate-service-record)
        IFS='|' read -r first second third extra <<<"$payload"
        [[ -z "$extra" \
          && "$first" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
          && "$second" =~ ^[1-9][0-9]*$ \
          && "$third" =~ ^[a-z0-9]{12,64}$ ]] || return 1
        VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS="${VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS:+$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS$'\n'}$payload"
        ;;
      candidate-service)
        IFS='|' read -r first second extra <<<"$payload"
        [[ -z "$extra" && -n "$second" ]] || return 1
        case "$first" in
          forward)
            forward_candidate_services="${forward_candidate_services:+$forward_candidate_services }$second"
            ;;
          rollback)
            rollback_candidate_services="${rollback_candidate_services:+$rollback_candidate_services }$second"
            ;;
          *) return 1 ;;
        esac
        ;;
      *) return 1 ;;
    esac
  done <<<"$records"
  [[ "$seen_meta" == true \
    && -n "$VP_WORKER_ADMISSION_RECOVERY_MIGRATION_STATE" ]] || return 1

  local selected_control=""
  if [[ "$recovery_promotion_context" == rollback ]]; then
    selected_control="$recovery_rollback_control"
  else
    selected_control="$recovery_forward_control"
  fi
  local selected_extra=""
  if [[ -n "$selected_control" ]]; then
    IFS='|' read -r \
      VP_WORKER_CONTROL_GENERATION \
      VP_WORKER_ADMISSION_CONTROL_IMAGE \
      VP_WORKER_OPERATOR_DATABASE_SECRET \
      VP_WORKER_ORCHESTRATOR_DATABASE_SECRET \
      VP_STAGING_JANITOR_DATABASE_SECRET \
      VP_STAGING_JANITOR_MINIO_ACCESS_SECRET \
      VP_STAGING_JANITOR_MINIO_SECRET_SECRET \
      VP_WORKER_MINIO_ACCESS_SECRET \
      VP_WORKER_MINIO_SECRET_SECRET \
      selected_extra <<<"$selected_control"
    [[ -z "$selected_extra" ]] || return 1
    VP_WORKER_CONTROL_PREPARED=true
    if [[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" ]]; then
      VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$VP_WORKER_CONTROL_GENERATION"
      VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$VP_WORKER_ADMISSION_CONTROL_IMAGE"
    fi
  elif [[ "$recovery_promotion_context" != forward \
    || "$VP_WORKER_ADMISSION_RECOVERY_PHASE" != FORWARD_APPLYING ]]; then
    return 1
  fi

  local selected_marker="$recovery_forward_marker"
  if [[ "$recovery_promotion_context" == rollback \
    && -n "$recovery_rollback_marker" ]]; then
    selected_marker="$recovery_rollback_marker"
  fi
  if [[ -n "$selected_marker" ]]; then
    selected_extra=""
    IFS='|' read -r \
      VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION \
      VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE \
      VP_WORKER_REDIS_MARKER_CANDIDATE_CONFIG_SHA256 \
      VP_WORKER_REDIS_MARKER_CANDIDATE_CRON_SHA256 \
      VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID \
      VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID \
      VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID \
      selected_extra <<<"$selected_marker"
    [[ -z "$selected_extra" ]] || return 1
    selected_extra=""
    IFS='|' read -r \
      VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION \
      VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE \
      VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256 \
      VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256 \
      VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID \
      VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID \
      VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID \
      selected_extra <<<"$recovery_forward_marker"
    [[ -z "$selected_extra" ]] || return 1
    VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
    VP_WORKER_REDIS_MARKER_CANDIDATE_READY=true
    if [[ -z "$selected_control" ]]; then
      VP_WORKER_CONTROL_GENERATION="c-${recovery_commit:0:20}"
      VP_WORKER_ADMISSION_CONTROL_IMAGE="$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE"
      VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$VP_WORKER_CONTROL_GENERATION"
      VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$VP_WORKER_ADMISSION_CONTROL_IMAGE"
    fi
  elif [[ "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" != true ]]; then
    return 1
  fi

  local require_baseline_marker_state=false
  case "$VP_WORKER_ADMISSION_RECOVERY_PHASE" in
    FORWARD_APPLYING|FORWARD_VERIFIED|WORKERS_PROMOTED|ROLLBACK_PREPARING|ROLLBACK_APPLYING|ROLLBACK_VERIFIED|ROLLBACK_WORKERS_PROMOTED|CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING|CANDIDATE_RESTORED)
      require_baseline_marker_state=true
      ;;
  esac
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" == true ]]; then
    require_baseline_marker_state=false
  fi
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_BASELINE_KIND" == managed \
    && ( "$require_baseline_marker_state" == true \
      || "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" == true ) ]]; then
    local marker_control_root
    marker_control_root="$(vp_worker_redis_marker_control_root)" || return 1
    local baseline_marker_state="$marker_control_root/transactions/$recovery_transaction_id/baseline-managed-state"
    if [[ ! -e "$baseline_marker_state" \
      && "$require_baseline_marker_state" == false ]]; then
      baseline_marker_state=""
    elif [[ -d "$baseline_marker_state" && ! -L "$baseline_marker_state" \
      && "$(vp_worker_redis_marker_file_mode "$baseline_marker_state")" == 700 \
      && -f "$baseline_marker_state/captured" \
      && ! -L "$baseline_marker_state/captured" \
      && "$(vp_worker_redis_marker_file_mode "$baseline_marker_state/captured")" == 600 \
      && "$(command cat "$baseline_marker_state/captured")" == VERSION=1 \
      && -f "$baseline_marker_state/crontab" \
      && ! -L "$baseline_marker_state/crontab" ]]; then
      :
    else
      return 1
    fi
    if [[ -n "$baseline_marker_state" ]]; then
      VP_WORKER_REDIS_MARKER_MANAGED_STATE="$baseline_marker_state"
      vp_worker_redis_marker_read_prior_config \
        "$baseline_marker_state/control.conf" || return 1
      [[ -n "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
        && -n "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" ]] || return 1
    fi
  fi

  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  VP_WORKER_ADMISSION_PREPARED=true
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD" == true ]]; then
    VP_WORKER_ADMISSION_PREPARED=false
  fi
  VP_WORKER_ADMISSION_COMMIT="$recovery_commit"
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE="$recovery_forward_namespace"
  if [[ "$recovery_promotion_context" == rollback ]]; then
      [[ "$recovery_rollback_namespace" != - \
        && "$recovery_marker_generation" != - ]] || return 1
      VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$recovery_rollback_namespace"
      VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$rollback_candidate_services"
      VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION="$recovery_marker_generation"
  else
    case "$VP_WORKER_ADMISSION_RECOVERY_PHASE" in
      ROLLBACK_PREPARING)
      VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$recovery_forward_namespace"
      VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$forward_candidate_services"
      VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION=""
      ;;
      FORWARD_APPLYING|FORWARD_VERIFIED|WORKERS_PROMOTED|MARKER_PROMOTED|CONTROL_PROMOTED|RETIRING|CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING|CANDIDATE_RESTORED)
      VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$recovery_forward_namespace"
      VP_WORKER_ADMISSION_CANDIDATE_SERVICES="$forward_candidate_services"
      VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION=""
      ;;
      *) return 1 ;;
    esac
  fi
}

vp_worker_admission_allocate_rollback_attempt() {
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_REPLAY_PHASE" == ROLLBACK_PREPARING ]] \
    || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    allocate-rollback-attempt \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" >/dev/null || return 1
  vp_worker_admission_load_durable_state || return 1
  [[ "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_ATTEMPT" \
      =~ ^[1-9][0-9]*$ \
    && "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_NAMESPACE" != - \
    && "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_MARKER_GENERATION" != - ]] \
    || return 1
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_NAMESPACE"
  VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION="$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_MARKER_GENERATION"
}

vp_worker_admission_load_stage_direction() {
  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    PREPARING|FORWARD_APPLYING)
      VP_WORKER_ADMISSION_STAGE_DIRECTION=forward
      ;;
    ROLLBACK_PREPARING|ROLLBACK_APPLYING)
      VP_WORKER_ADMISSION_STAGE_DIRECTION=rollback
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_worker_plan_payload() {
  local service="$1"
  local expected_image="$2"
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  [[ "$root" = /* \
    && "$namespace" =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  local manifest="$root/candidates/$namespace/$kind.conf"
  vp_worker_admission_require_v2_manifest \
    "$manifest" "$service" || return 1
  [[ "$VP_WORKER_MANIFEST_IMAGE" == "$expected_image" \
    && "$VP_WORKER_MANIFEST_IMAGE" \
      == *":deploy-${VP_WORKER_MANIFEST_COMMIT:0:12}" ]] || return 1
  python3 -I -c '
import json
import sys

(
    service,
    generation,
    commit,
    image,
    database_name,
    database_id,
    admission_name,
    admission_id,
) = sys.argv[1:]
try:
    generation_number = int(generation, 10)
    if generation_number < 1:
        raise ValueError
    payload = {
        "admission_secret": {
            "docker_secret_id": admission_id,
            "generation": generation,
            "name": admission_name,
            "purpose": "admission",
            "service": service,
        },
        "commit": commit,
        "database_secret": {
            "docker_secret_id": database_id,
            "generation": generation,
            "name": database_name,
            "purpose": "database",
            "service": service,
        },
        "generation": generation_number,
        "image": image,
        "service": service,
        "target_spec_digest": None,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
except (TypeError, ValueError):
    raise SystemExit(1)
' \
    "$service" \
    "$VP_WORKER_MANIFEST_GENERATION" \
    "$VP_WORKER_MANIFEST_COMMIT" \
    "$VP_WORKER_MANIFEST_IMAGE" \
    "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
    "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
    "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
    "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
}

vp_worker_admission_record_prepared_worker_plan() {
  local service="$1"
  local image="$2"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  vp_worker_admission_lock_assert || return 1
  vp_worker_admission_load_stage_direction || return 1
  local direction="$VP_WORKER_ADMISSION_STAGE_DIRECTION"
  local payload
  payload="$(
    vp_worker_admission_worker_plan_payload "$service" "$image"
  )" || return 1
  local plan_identity
  plan_identity="$(
    printf '%s\n' "$payload" | python3 -I -c '
import json
import re
import sys

try:
    plan = json.load(sys.stdin)
    service = plan["service"]
    generation = plan["generation"]
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", service)
        is None
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise ValueError
    print(f"{service}|{generation}")
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local plan_service
  local plan_generation
  local extra
  IFS='|' read -r plan_service plan_generation extra <<<"$plan_identity"
  [[ -z "$extra" \
    && "$plan_service" == "$service" \
    && "$plan_generation" =~ ^[1-9][0-9]*$ ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-worker-plan \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$direction" <<<"$payload" >/dev/null || return 1
  vp_worker_admission_load_stage_direction || return 1
  [[ "$VP_WORKER_ADMISSION_STAGE_DIRECTION" == "$direction" ]] \
    || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    advance-worker-stage \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$direction" \
    "$plan_service" \
    "$plan_generation" \
    pending prepared - - >/dev/null
}

vp_worker_admission_live_worker_identity() {
  local service="$1"
  local image="$2"
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local generation
  generation="$(cut -d'|' -f6 <<<"$contract")"
  [[ "$generation" =~ ^[1-9][0-9]*$ ]] || return 1
  vp_require_worker_service_descriptor "$service" "$image" || return 1
  local registered_id
  registered_id="$(
    vp_registered_worker_service_identity \
      "$service" "$service" "$generation"
  )" || return 1
  local durable_identity
  durable_identity="$(
    vp_app_service_durable_identity "$service" "$image"
  )" || return 1
  local durable_id
  local spec_digest
  local extra
  IFS='|' read -r durable_id spec_digest extra <<<"$durable_identity"
  [[ -z "$extra" \
    && "$durable_id" == "$registered_id" \
    && "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  printf '%s|%s\n' "$registered_id" "$spec_digest"
}

vp_worker_admission_advance_live_worker_stage() {
  local service="$1"
  local image="$2"
  local expected_stage="$3"
  local target_stage="$4"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  vp_worker_admission_lock_assert || return 1
  vp_worker_admission_load_stage_direction || return 1
  local direction="$VP_WORKER_ADMISSION_STAGE_DIRECTION"
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local generation
  generation="$(cut -d'|' -f6 <<<"$contract")"
  [[ "$generation" =~ ^[1-9][0-9]*$ ]] || return 1
  local live_identity
  live_identity="$(
    vp_worker_admission_live_worker_identity "$service" "$image"
  )" || return 1
  local service_id
  local spec_digest
  local extra
  IFS='|' read -r service_id spec_digest extra <<<"$live_identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    advance-worker-stage \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$direction" "$service" "$generation" \
    "$expected_stage" "$target_stage" \
    "$service_id" "$spec_digest" >/dev/null
}

vp_worker_admission_promotion_digest() {
  local source="$1"
  python3 -I - "$source" <<'PY'
import hashlib
import os
import stat
import sys


def read_regular(path: str) -> bytes:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise ValueError
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ValueError
        chunks = []
        total = 0
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > 1024 * 1024:
                raise ValueError
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError
        return b"".join(chunks)
    finally:
        os.close(descriptor)


source = sys.argv[1]
metadata = os.lstat(source)
digest = hashlib.sha256()
if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
    payload = read_regular(source)
    digest.update(b"file\0")
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
elif stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
    entries = sorted(os.scandir(source), key=lambda entry: entry.name)
    if not entries:
        raise ValueError
    digest.update(b"directory\0")
    for entry in entries:
        if (
            not entry.name.endswith(".conf")
            or entry.is_symlink()
            or not entry.is_file(follow_symlinks=False)
        ):
            raise ValueError
        name = entry.name.encode("ascii")
        payload = read_regular(entry.path)
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
else:
    raise ValueError
print(digest.hexdigest())
PY
}

vp_worker_admission_promotion_destination() {
  local kind="$1"
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      printf '%s\n' "$root/current"
      ;;
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      printf '%s\n' "$(vp_worker_redis_marker_control_root)/control.conf"
      ;;
    PROMOTE_CONTROL|PROMOTE_ROLLBACK_CONTROL)
      printf '%s\n' "$root/control-current.conf"
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_promotion_precondition_path() {
  local kind="$1"
  local transaction_id="$VP_WORKER_ADMISSION_TRANSACTION_ID"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_MARKER|PROMOTE_CONTROL|PROMOTE_ROLLBACK_WORKERS|PROMOTE_ROLLBACK_MARKER|PROMOTE_ROLLBACK_CONTROL) ;;
    *) return 1 ;;
  esac
  [[ "$transaction_id" =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  printf '%s\n' \
    "$VP_WORKER_ADMISSION_LOCK_ROOT/transactions/$transaction_id/promotion-preconditions/$kind.json"
}

vp_worker_admission_capture_promotion_precondition() {
  local kind="$1"
  local identity="$2"
  [[ "$identity" = /* && -f "$identity" && ! -L "$identity" \
    && "$(vp_worker_redis_marker_file_mode "$identity")" == 600 ]] \
    || return 1
  local target_fields
  target_fields="$(python3 -I -c '
import json
import os
import re
import stat
import sys

path = sys.argv[1]
metadata = os.lstat(path)
if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit(1)
with open(path, encoding="utf-8") as handle:
    value = json.load(handle)
if (
    not isinstance(value, dict)
    or set(value) != {
        "docker_id", "generation", "kind", "name", "purpose",
        "service", "spec_digest",
    }
    or value["docker_id"] is not None
    or value["kind"] != "manifest"
    or value["purpose"] != "promotion"
    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value["name"] or "") is None
    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value["service"] or "") is None
    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value["generation"] or "") is None
    or re.fullmatch(r"[0-9a-f]{64}", value["spec_digest"] or "") is None
):
    raise SystemExit(1)
print("|".join((
    value["name"], value["service"], value["generation"],
    value["spec_digest"],
)))
' "$identity")" || return 1
  local target_name
  local target_service
  local target_generation
  local target_digest
  local extra
  IFS='|' read -r \
    target_name target_service target_generation target_digest extra \
    <<<"$target_fields"
  [[ -z "$extra" ]] || return 1

  local destination
  destination="$(vp_worker_admission_promotion_destination "$kind")" \
    || return 1
  local current_state=absent
  local current_digest=-
  if [[ -L "$destination" ]]; then
    return 1
  fi
  if [[ -e "$destination" ]]; then
    current_state=present
    current_digest="$(
      vp_worker_admission_promotion_digest "$destination"
    )" || return 1
  fi
  local path
  path="$(vp_worker_admission_promotion_precondition_path "$kind")" \
    || return 1
  local directory="${path%/*}"
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  python3 -I -c '
import json
import hashlib
import os
import pathlib
import stat
import sys
import tempfile

(
    path,
    transaction_id,
    operation_kind,
    target_name,
    target_service,
    target_generation,
    target_digest,
    current_state,
    current_digest,
    admission_root,
    candidate_namespace,
) = sys.argv[1:]


def read_regular(file_path):
    before = os.lstat(file_path)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_uid != os.getuid()
        or before.st_gid != os.getgid()
        or before.st_nlink != 1
    ):
        raise SystemExit(1)
    descriptor = os.open(
        file_path,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise SystemExit(1)
        payload = b""
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            payload += chunk
            if len(payload) > 1024 * 1024:
                raise SystemExit(1)
        after = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise SystemExit(1)
        return payload
    finally:
        os.close(descriptor)


def file_digest(file_path):
    payload = read_regular(file_path)
    digest = hashlib.sha256()
    digest.update(b"file\0")
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


worker_manifests = []
if operation_kind in {"PROMOTE_WORKERS", "PROMOTE_ROLLBACK_WORKERS"}:
    root = pathlib.Path(admission_root)
    candidate = root / "candidates" / candidate_namespace
    candidate_metadata = os.lstat(candidate)
    if (
        not stat.S_ISDIR(candidate_metadata.st_mode)
        or stat.S_ISLNK(candidate_metadata.st_mode)
        or stat.S_IMODE(candidate_metadata.st_mode) != 0o700
    ):
        raise SystemExit(1)
    candidate_entries = sorted(os.scandir(candidate), key=lambda item: item.name)
    if not candidate_entries or any(
        not item.name.endswith(".conf")
        or item.is_symlink()
        or not item.is_file(follow_symlinks=False)
        for item in candidate_entries
    ):
        raise SystemExit(1)
    current = root / "current"
    if current.exists() or current.is_symlink():
        current_metadata = os.lstat(current)
        if (
            not stat.S_ISDIR(current_metadata.st_mode)
            or stat.S_ISLNK(current_metadata.st_mode)
            or stat.S_IMODE(current_metadata.st_mode) != 0o700
        ):
            raise SystemExit(1)
        current_entries = {
            item.name: item
            for item in os.scandir(current)
        }
        if any(
            not item.name.endswith(".conf")
            or item.is_symlink()
            or not item.is_file(follow_symlinks=False)
            for item in current_entries.values()
        ):
            raise SystemExit(1)
    else:
        current_entries = {}
    candidate_by_name = {item.name: item for item in candidate_entries}
    for name in sorted(set(current_entries) | set(candidate_by_name)):
        prior_path = current / name
        selected = name in candidate_by_name
        prior_digest = (
            file_digest(prior_path) if name in current_entries else None
        )
        worker_manifests.append({
            "name": name,
            "prior_spec_digest": prior_digest,
            "selected": selected,
            "target_spec_digest": (
                file_digest(pathlib.Path(candidate_by_name[name].path))
                if selected
                else prior_digest
            ),
        })
elif operation_kind not in {
    "PROMOTE_MARKER",
    "PROMOTE_ROLLBACK_MARKER",
    "PROMOTE_CONTROL",
    "PROMOTE_ROLLBACK_CONTROL",
}:
    raise SystemExit(1)

payload = {
    "schema": 1,
    "transaction_id": transaction_id,
    "operation_kind": operation_kind,
    "target": {
        "name": target_name,
        "service": target_service,
        "generation": target_generation,
        "spec_digest": target_digest,
    },
    "current": {
        "state": current_state,
        "spec_digest": None if current_digest == "-" else current_digest,
    },
    "worker_manifests": worker_manifests,
}
destination = pathlib.Path(path)
if destination.exists() or destination.is_symlink():
    metadata = os.lstat(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise SystemExit(1)
    with open(path, encoding="utf-8") as handle:
        existing = json.load(handle)
    if existing != payload:
        raise SystemExit(1)
    raise SystemExit(0)
descriptor, temporary = tempfile.mkstemp(
    dir=str(destination.parent),
    prefix=f".{destination.name}.tmp.",
)
try:
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        descriptor = -1
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
finally:
    if descriptor >= 0:
        os.close(descriptor)
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
directory_fd = os.open(str(destination.parent), os.O_RDONLY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
' \
    "$path" "$VP_WORKER_ADMISSION_TRANSACTION_ID" "$kind" \
    "$target_name" "$target_service" "$target_generation" \
    "$target_digest" "$current_state" "$current_digest" \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
}

vp_worker_admission_promotion_precondition_matches() {
  local kind="$1"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      vp_worker_admission_worker_promotion_precondition_matches \
        "$kind" prior
      return
      ;;
  esac
  local path
  path="$(vp_worker_admission_promotion_precondition_path "$kind")" \
    || return 1
  [[ -f "$path" && ! -L "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
    || return 1
  local destination
  destination="$(vp_worker_admission_promotion_destination "$kind")" \
    || return 1
  local current_state=absent
  local current_digest=-
  if [[ -L "$destination" ]]; then
    return 1
  fi
  if [[ -e "$destination" ]]; then
    current_state=present
    current_digest="$(
      vp_worker_admission_promotion_digest "$destination"
    )" || return 1
  fi
  python3 -I -c '
import json
import os
import re
import stat
import sys

(
    path,
    transaction_id,
    operation_kind,
    operation_name,
    operation_service,
    operation_generation,
    operation_digest,
    current_state,
    current_digest,
) = sys.argv[1:]
metadata = os.lstat(path)
if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
    raise SystemExit(1)
with open(path, encoding="utf-8") as handle:
    value = json.load(handle)
expected = {
    "schema": 1,
    "transaction_id": transaction_id,
    "operation_kind": operation_kind,
    "target": {
        "name": operation_name,
        "service": operation_service,
        "generation": operation_generation,
        "spec_digest": operation_digest,
    },
    "current": {
        "state": current_state,
        "spec_digest": None if current_digest == "-" else current_digest,
    },
    "worker_manifests": [],
}
if (
    value != expected
    or re.fullmatch(r"tx-[0-9a-f]{32}", transaction_id) is None
    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", operation_generation or "") is None
    or re.fullmatch(r"[0-9a-f]{64}", operation_digest or "") is None
):
    raise SystemExit(1)
' \
    "$path" "$VP_WORKER_ADMISSION_TRANSACTION_ID" "$kind" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" \
    "$current_state" "$current_digest"
}

vp_worker_admission_promotion_precondition_record_matches() {
  local kind="$1"
  local path
  path="$(vp_worker_admission_promotion_precondition_path "$kind")" \
    || return 1
  [[ -f "$path" && ! -L "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
    || return 1
  python3 -I -c '
import json
import re
import sys

(
    path,
    transaction_id,
    operation_kind,
    operation_name,
    operation_service,
    operation_generation,
    operation_digest,
) = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    value = json.load(handle)
if not isinstance(value, dict) or set(value) != {
    "schema", "transaction_id", "operation_kind", "target", "current",
    "worker_manifests",
}:
    raise SystemExit(1)
target = value["target"]
current = value["current"]
expected_target = {
    "name": operation_name,
    "service": operation_service,
    "generation": operation_generation,
    "spec_digest": operation_digest,
}
if (
    value["schema"] != 1
    or value["transaction_id"] != transaction_id
    or value["operation_kind"] != operation_kind
    or target != expected_target
    or not isinstance(current, dict)
    or set(current) != {"state", "spec_digest"}
    or current["state"] not in {"absent", "present"}
    or (
        current["state"] == "absent"
        and current["spec_digest"] is not None
    )
    or (
        current["state"] == "present"
        and re.fullmatch(r"[0-9a-f]{64}", current["spec_digest"] or "")
        is None
    )
    or re.fullmatch(r"tx-[0-9a-f]{32}", transaction_id) is None
    or re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
        operation_generation or "",
    ) is None
    or re.fullmatch(r"[0-9a-f]{64}", operation_digest or "") is None
    or value["worker_manifests"] != []
):
    raise SystemExit(1)
' \
    "$path" "$VP_WORKER_ADMISSION_TRANSACTION_ID" "$kind" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST"
}

vp_worker_admission_worker_promotion_precondition_matches() {
  local kind="$1"
  local accepted_state="${2:-mixed}"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS) ;;
    *) return 1 ;;
  esac
  case "$accepted_state" in
    prior|mixed|target) ;;
    *) return 1 ;;
  esac
  local path
  path="$(vp_worker_admission_promotion_precondition_path "$kind")" \
    || return 1
  [[ -f "$path" && ! -L "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
    || return 1
  python3 -I - \
    "$path" "$VP_WORKER_ADMISSION_TRANSACTION_ID" "$kind" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
    "$accepted_state" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import stat
import sys

(
    precondition_path,
    transaction_id,
    operation_kind,
    operation_name,
    operation_service,
    operation_generation,
    operation_digest,
    admission_root,
    candidate_namespace,
    accepted_state,
) = sys.argv[1:]


def read_regular(path):
    before = os.lstat(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_uid != os.getuid()
        or before.st_gid != os.getgid()
        or before.st_nlink != 1
    ):
        raise SystemExit(1)
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise SystemExit(1)
        payload = b""
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            payload += chunk
            if len(payload) > 1024 * 1024:
                raise SystemExit(1)
        after = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise SystemExit(1)
        return payload
    finally:
        os.close(descriptor)


def promotion_digest(path):
    metadata = os.lstat(path)
    digest = hashlib.sha256()
    if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        payload = read_regular(path)
        digest.update(b"file\0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    elif stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        entries = sorted(os.scandir(path), key=lambda item: item.name)
        if not entries:
            raise SystemExit(1)
        digest.update(b"directory\0")
        for item in entries:
            if (
                not item.name.endswith(".conf")
                or item.is_symlink()
                or not item.is_file(follow_symlinks=False)
            ):
                raise SystemExit(1)
            name = item.name.encode("ascii")
            payload = read_regular(pathlib.Path(item.path))
            digest.update(len(name).to_bytes(4, "big"))
            digest.update(name)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    else:
        raise SystemExit(1)
    return digest.hexdigest()


with open(precondition_path, encoding="utf-8") as handle:
    value = json.load(handle)
if not isinstance(value, dict) or set(value) != {
    "schema",
    "transaction_id",
    "operation_kind",
    "target",
    "current",
    "worker_manifests",
}:
    raise SystemExit(1)
if (
    value["schema"] != 1
    or value["transaction_id"] != transaction_id
    or value["operation_kind"] != operation_kind
    or value["target"] != {
        "name": operation_name,
        "service": operation_service,
        "generation": operation_generation,
        "spec_digest": operation_digest,
    }
    or operation_kind not in {"PROMOTE_WORKERS", "PROMOTE_ROLLBACK_WORKERS"}
    or operation_name != "worker-manifests"
    or operation_service != "worker-admission"
    or operation_generation != candidate_namespace
    or re.fullmatch(r"tx-[0-9a-f]{32}", transaction_id) is None
    or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", candidate_namespace) is None
    or re.fullmatch(r"[0-9a-f]{64}", operation_digest or "") is None
):
    raise SystemExit(1)

records = value["worker_manifests"]
if not isinstance(records, list) or not records:
    raise SystemExit(1)
by_name = {}
for item in records:
    if not isinstance(item, dict) or set(item) != {
        "name", "prior_spec_digest", "selected", "target_spec_digest"
    }:
        raise SystemExit(1)
    name = item["name"]
    prior_digest = item["prior_spec_digest"]
    selected = item["selected"]
    target_digest = item["target_spec_digest"]
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,250}\.conf", name or "")
        is None
        or name in by_name
        or not isinstance(selected, bool)
        or (
            prior_digest is not None
            and re.fullmatch(r"[0-9a-f]{64}", prior_digest or "") is None
        )
        or re.fullmatch(r"[0-9a-f]{64}", target_digest or "") is None
        or (not selected and (prior_digest is None or target_digest != prior_digest))
    ):
        raise SystemExit(1)
    by_name[name] = item

root = pathlib.Path(admission_root)
candidate = root / "candidates" / candidate_namespace
candidate_metadata = os.lstat(candidate)
if (
    not stat.S_ISDIR(candidate_metadata.st_mode)
    or stat.S_ISLNK(candidate_metadata.st_mode)
    or stat.S_IMODE(candidate_metadata.st_mode) != 0o700
    or promotion_digest(candidate) != operation_digest
):
    raise SystemExit(1)
candidate_names = {item.name for item in os.scandir(candidate)}
selected_names = {
    name for name, record in by_name.items() if record["selected"]
}
if candidate_names != selected_names:
    raise SystemExit(1)
for name in selected_names:
    record = by_name[name]
    if promotion_digest(candidate / name) != record["target_spec_digest"]:
        raise SystemExit(1)

current = root / "current"
if current.exists() or current.is_symlink():
    current_metadata = os.lstat(current)
    if (
        not stat.S_ISDIR(current_metadata.st_mode)
        or stat.S_ISLNK(current_metadata.st_mode)
        or stat.S_IMODE(current_metadata.st_mode) != 0o700
    ):
        raise SystemExit(1)
    current_names = {item.name for item in os.scandir(current)}
else:
    current_names = set()
if not current_names.issubset(set(by_name)):
    raise SystemExit(1)
for name, record in by_name.items():
    actual = promotion_digest(current / name) if name in current_names else None
    prior = record["prior_spec_digest"]
    target = record["target_spec_digest"]
    if accepted_state == "prior":
        accepted = {prior}
    elif accepted_state == "target":
        accepted = {target}
    else:
        accepted = {prior, target}
    if actual not in accepted:
        raise SystemExit(1)
PY
}

vp_worker_admission_replay_promotion_precondition_matches() {
  local kind="$1"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      vp_worker_admission_worker_promotion_precondition_matches \
        "$kind" mixed
      return
      ;;
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      local destination
      destination="$(vp_worker_admission_promotion_destination "$kind")" \
        || return 1
      if [[ -e "$destination" && ! -L "$destination" ]]; then
        local current_digest
        current_digest="$(
          vp_worker_admission_promotion_digest "$destination"
        )" || return 1
        if [[ "$current_digest" \
          == "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" ]]; then
          vp_worker_admission_promotion_precondition_record_matches "$kind"
          return
        fi
      fi
      ;;
  esac
  vp_worker_admission_promotion_precondition_matches "$kind"
}

vp_worker_admission_pending_promotion_target_matches() {
  local kind="$1"
  vp_worker_admission_require_promotion_selection "$kind" || return 1
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local expected_name
  local expected_service
  local expected_generation
  local source
  local rendered_source=""
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      expected_name=worker-manifests
      expected_service=worker-admission
      expected_generation="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
      source="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
      ;;
    PROMOTE_MARKER)
      expected_name=control.conf
      expected_service=worker-redis-marker-control
      expected_generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
      source="$(vp_worker_redis_marker_control_root)/control.conf"
      ;;
    PROMOTE_ROLLBACK_MARKER)
      expected_name=control.conf
      expected_service=worker-redis-marker-control
      expected_generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
      rendered_source="$(
        mktemp "${TMPDIR:-/tmp}/vp-rollback-marker-replay.XXXXXX"
      )" || return 1
      if ! vp_worker_redis_marker_render_config \
        "$rendered_source" \
        "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
        "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"; then
        rm -f "$rendered_source"
        return 1
      fi
      source="$rendered_source"
      ;;
    PROMOTE_CONTROL)
      expected_name=control-current.conf
      expected_service=worker-admission-control
      expected_generation="$VP_WORKER_CONTROL_GENERATION"
      source="$root/control-candidates/$VP_WORKER_CONTROL_GENERATION.conf"
      ;;
    PROMOTE_ROLLBACK_CONTROL)
      expected_name=control-current.conf
      expected_service=worker-admission-control
      expected_generation="$VP_WORKER_CONTROL_GENERATION"
      source="$root/control-current.conf"
      ;;
    *) return 1 ;;
  esac
  if [[ "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" != "$kind" \
    || "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" != "$expected_name" \
    || "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" != "$expected_service" \
    || "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
      != "$expected_generation" ]]; then
    [[ -z "$rendered_source" ]] || rm -f "$rendered_source"
    return 1
  fi
  local digest
  if ! digest="$(vp_worker_admission_promotion_digest "$source")"; then
    [[ -z "$rendered_source" ]] || rm -f "$rendered_source"
    return 1
  fi
  [[ -z "$rendered_source" ]] || rm -f "$rendered_source"
  [[ "$digest" == "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" ]]
}

vp_worker_admission_apply_promotion_effect() {
  local kind="$1"
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      vp_commit_worker_admission
      ;;
    PROMOTE_MARKER)
      vp_commit_worker_redis_marker_controls || return 1
      vp_worker_admission_write_marker_promotion_receipt "$kind"
      ;;
    PROMOTE_CONTROL)
      vp_commit_worker_control_generation
      ;;
    PROMOTE_ROLLBACK_MARKER)
      vp_restore_worker_redis_marker_controls || return 1
      vp_worker_admission_write_marker_promotion_receipt "$kind"
      ;;
    PROMOTE_ROLLBACK_CONTROL)
      vp_finalize_worker_control_rollback
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_replay_pending_promotion_effect() {
  local kind="$1"
  vp_worker_admission_replay_promotion_precondition_matches \
    "$kind" || return 1
  vp_worker_admission_pending_promotion_target_matches "$kind" || return 1
  vp_worker_admission_replay_promotion_precondition_matches \
    "$kind" || return 1
  vp_worker_admission_apply_promotion_effect "$kind"
}

vp_worker_admission_require_promotion_selection() {
  local kind="$1"
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  vp_worker_admission_lock_assert || return 1
  local state
  state="$(vp_worker_admission_recovery_state)" || return 1
  case "$kind" in
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      local direction=forward
      local selection_mode=active
      if [[ "$kind" == PROMOTE_ROLLBACK_MARKER ]]; then
        direction=rollback
        selection_mode=expected
      fi
      local durable_selection
      durable_selection="$(printf '%s\n' "$state" | python3 -I -c '
import json
import sys

direction = sys.argv[1]
state = json.load(sys.stdin)
selection = state[direction]["marker"]
if not isinstance(selection, dict):
    raise SystemExit(1)
print(json.dumps(selection, sort_keys=True, separators=(",", ":")))
' "$direction")" || return 1
      local actual_selection
      actual_selection="$(
        vp_worker_admission_marker_selection_json "$selection_mode"
      )" || return 1
      [[ "$actual_selection" == "$durable_selection" ]]
      ;;
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS|PROMOTE_CONTROL|PROMOTE_ROLLBACK_CONTROL)
      printf '%s\n' "$state" | python3 -I -c '
import hashlib
import json
import os
import pathlib
import stat
import sys

kind, raw_root = sys.argv[1:]
root = pathlib.Path(raw_root)
state = json.load(sys.stdin)
direction = "rollback" if "ROLLBACK" in kind else "forward"
selection = state[direction]


def read_regular(path: pathlib.Path) -> bytes:
    before = os.lstat(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
    ):
        raise ValueError
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ValueError
        payload = b""
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            payload += chunk
            if len(payload) > 1024 * 1024:
                raise ValueError
        after = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError
        return payload
    finally:
        os.close(descriptor)


if kind in {"PROMOTE_CONTROL", "PROMOTE_ROLLBACK_CONTROL"}:
    control = selection["control"]
    if not isinstance(control, dict):
        raise SystemExit(1)
    generation = control["generation"]
    if kind == "PROMOTE_CONTROL":
        source = root / "control-candidates" / f"{generation}.conf"
    else:
        source = root / "control-current.conf"
    if hashlib.sha256(read_regular(source)).hexdigest() != control["manifest_sha256"]:
        raise SystemExit(1)
    raise SystemExit(0)

namespace = selection["namespace"]
workers = selection["workers"]
if not isinstance(namespace, str) or not isinstance(workers, list) or not workers:
    raise SystemExit(1)
directory = root / "candidates" / namespace
metadata = os.lstat(directory)
if (
    not stat.S_ISDIR(metadata.st_mode)
    or stat.S_ISLNK(metadata.st_mode)
    or stat.S_IMODE(metadata.st_mode) != 0o700
):
    raise SystemExit(1)
filenames = {
    "vp-ffmpeg-worker-go-swarm": "ffmpeg-go.conf",
    "vp-ffmpeg-worker-gpu-swarm": "ffmpeg.conf",
    "vp-vision-worker-swarm": "vision.conf",
    "vp-youtube-publisher-swarm": "youtube-publisher.conf",
}
expected_files = {filenames[worker["service"]] for worker in workers}
actual_files = {entry.name for entry in os.scandir(directory)}
if actual_files != expected_files:
    raise SystemExit(1)
for worker in workers:
    path = directory / filenames[worker["service"]]
    lines = read_regular(path).decode("ascii").splitlines()
    fields = {}
    for line in lines:
        key, separator, value = line.partition("=")
        if not separator or not key or not value or key in fields:
            raise SystemExit(1)
        fields[key] = value
    database = worker["database_secret"]
    admission = worker["admission_secret"]
    expected = {
        "VERSION": "2",
        "SERVICE": worker["service"],
        "COMMIT": worker["commit"],
        "IMAGE": worker["image"],
        "GENERATION": str(worker["generation"]),
        "DATABASE_SECRET": database["name"],
        "ADMISSION_SECRET": admission["name"],
        "DATABASE_SECRET_ID": database["docker_secret_id"],
        "ADMISSION_SECRET_ID": admission["docker_secret_id"],
    }
    if fields != expected:
        raise SystemExit(1)
' "$kind" "$root"
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_promotion_identity() {
  local kind="$1"
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local transaction_id="$VP_WORKER_ADMISSION_TRANSACTION_ID"
  [[ "$transaction_id" =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  vp_worker_admission_require_promotion_selection "$kind" || return 1
  local identity_dir="$root/transactions/$transaction_id/promotion-identities"
  mkdir -p "$identity_dir" || return 1
  chmod 0700 "$identity_dir" || return 1
  local name
  local service
  local generation
  local source
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      name=worker-manifests
      service=worker-admission
      generation="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
      source="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
      ;;
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      name=control.conf
      service=worker-redis-marker-control
      generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
      source="$(vp_worker_redis_marker_control_root)/control.conf"
      ;;
    PROMOTE_CONTROL)
      name=control-current.conf
      service=worker-admission-control
      generation="$VP_WORKER_CONTROL_GENERATION"
      source="$root/control-candidates/$VP_WORKER_CONTROL_GENERATION.conf"
      ;;
    PROMOTE_ROLLBACK_CONTROL)
      name=control-current.conf
      service=worker-admission-control
      generation="$VP_WORKER_CONTROL_GENERATION"
      source="$root/control-current.conf"
      ;;
    *) return 1 ;;
  esac
  [[ -n "$generation" && -e "$source" && ! -L "$source" ]] || return 1
  local digest_source="$source"
  local rendered_source=""
  if [[ "$kind" == PROMOTE_ROLLBACK_MARKER ]]; then
    rendered_source="$(
      mktemp "${TMPDIR:-/tmp}/vp-rollback-marker-promotion.XXXXXX"
    )" || return 1
    if ! vp_worker_redis_marker_render_config \
      "$rendered_source" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"; then
      rm -f "$rendered_source"
      return 1
    fi
    digest_source="$rendered_source"
  fi
  local digest
  if ! digest="$(vp_worker_admission_promotion_digest "$digest_source")"; then
    [[ -z "$rendered_source" ]] || rm -f "$rendered_source"
    return 1
  fi
  [[ -z "$rendered_source" ]] || rm -f "$rendered_source"
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  local path="$identity_dir/$kind.json"
  python3 -I -c '
import json
import os
import sys

path, name, service, generation, digest = sys.argv[1:]
payload = {
    "docker_id": None,
    "generation": generation,
    "kind": "manifest",
    "name": name,
    "purpose": "promotion",
    "service": service,
    "spec_digest": digest,
}
temporary = path + ".tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
' "$path" "$name" "$service" "$generation" "$digest" || return 1
  printf '%s\n' "$path"
}

vp_worker_admission_marker_promotion_receipt_path() {
  local kind="$1"
  case "$kind" in
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER) ;;
    *) return 1 ;;
  esac
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  printf '%s\n' \
    "$VP_WORKER_ADMISSION_LOCK_ROOT/transactions/$VP_WORKER_ADMISSION_TRANSACTION_ID/promotion-receipts/$kind.json"
}

vp_worker_admission_write_marker_promotion_receipt() {
  local kind="$1"
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" == "$kind" \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID" \
      =~ ^operation-[0-9a-f]{32}$ \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" == control.conf \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
      == worker-redis-marker-control \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" \
      =~ ^[0-9a-f]{64}$ ]] || return 1
  local path
  path="$(vp_worker_admission_marker_promotion_receipt_path "$kind")" \
    || return 1
  local directory="${path%/*}"
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  python3 -I -c '
import json
import os
import pathlib
import stat
import sys
import tempfile

(
    path,
    transaction_id,
    operation_id,
    operation_kind,
    name,
    service,
    generation,
    digest,
) = sys.argv[1:]
payload = {
    "cleanup_completed": True,
    "operation_id": operation_id,
    "operation_kind": operation_kind,
    "schema": 1,
    "target": {
        "generation": generation,
        "name": name,
        "service": service,
        "spec_digest": digest,
    },
    "transaction_id": transaction_id,
}
destination = pathlib.Path(path)
if os.path.lexists(path):
    metadata = os.lstat(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise SystemExit(1)
    with open(path, encoding="utf-8") as handle:
        if json.load(handle) != payload:
            raise SystemExit(1)
    raise SystemExit(0)
descriptor, temporary = tempfile.mkstemp(
    dir=str(destination.parent),
    prefix=f".{destination.name}.tmp.",
)
try:
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        descriptor = -1
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
finally:
    if descriptor >= 0:
        os.close(descriptor)
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
directory_fd = os.open(str(destination.parent), os.O_RDONLY)
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
' \
    "$path" \
    "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST"
}

vp_worker_admission_marker_promotion_receipt_matches() {
  local kind="$1"
  local path
  path="$(vp_worker_admission_marker_promotion_receipt_path "$kind")" \
    || return 1
  [[ -f "$path" && ! -L "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
    || return 1
  python3 -I -c '
import json
import sys

(
    path,
    transaction_id,
    operation_id,
    operation_kind,
    name,
    service,
    generation,
    digest,
) = sys.argv[1:]
expected = {
    "cleanup_completed": True,
    "operation_id": operation_id,
    "operation_kind": operation_kind,
    "schema": 1,
    "target": {
        "generation": generation,
        "name": name,
        "service": service,
        "spec_digest": digest,
    },
    "transaction_id": transaction_id,
}
with open(path, encoding="utf-8") as handle:
    if json.load(handle) != expected:
        raise SystemExit(1)
' \
    "$path" \
    "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
    "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST"
}

vp_worker_admission_current_promotion_matches() {
  local kind="$1"
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local expected_name
  local expected_service
  local source
  case "$kind" in
    PROMOTE_WORKERS|PROMOTE_ROLLBACK_WORKERS)
      expected_name=worker-manifests
      expected_service=worker-admission
      vp_worker_admission_worker_promotion_precondition_matches \
        "$kind" target
      return
      ;;
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      expected_name=control.conf
      expected_service=worker-redis-marker-control
      source="$(vp_worker_redis_marker_control_root)/control.conf"
      ;;
    PROMOTE_CONTROL|PROMOTE_ROLLBACK_CONTROL)
      expected_name=control-current.conf
      expected_service=worker-admission-control
      source="$root/control-current.conf"
      ;;
    *) return 1 ;;
  esac
  [[ "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" == "$kind" \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_NAME" == "$expected_name" \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_SERVICE" == "$expected_service" \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" \
      =~ ^[0-9a-f]{64}$ ]] || return 1
  local digest
  digest="$(vp_worker_admission_promotion_digest "$source")" || return 1
  [[ "$digest" == "$VP_WORKER_ADMISSION_REPLAY_OPERATION_DIGEST" ]] \
    || return 1
  case "$kind" in
    PROMOTE_MARKER|PROMOTE_ROLLBACK_MARKER)
      vp_worker_admission_marker_promotion_receipt_matches "$kind"
      ;;
    *)
      return 0
      ;;
  esac
}

vp_worker_admission_complete_pending_promotion() {
  local kind="$1"
  local operation_id="$2"
  if ! vp_worker_admission_current_promotion_matches "$kind"; then
    vp_worker_admission_hydrate_recovery_context || return 1
    vp_worker_admission_replay_pending_promotion_effect "$kind" || return 1
    vp_worker_admission_load_replay_plan || return 1
    [[ "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" == "$kind" \
      && "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID" == "$operation_id" ]] \
      || return 1
    vp_worker_admission_current_promotion_matches "$kind" || return 1
  fi
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" complete-intent \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$operation_id" >/dev/null
}

vp_worker_admission_promote_phase() {
  local kind="$1"
  local identity
  identity="$(vp_worker_admission_promotion_identity "$kind")" || return 1
  vp_worker_admission_capture_promotion_precondition \
    "$kind" "$identity" || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" intent \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$kind" "$identity" >/dev/null || return 1
  vp_worker_admission_apply_promotion_effect "$kind" || return 1
  vp_worker_admission_load_replay_plan || return 1
  vp_worker_admission_complete_pending_promotion \
    "$kind" "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID"
}

vp_worker_admission_retire_transaction() {
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  vp_worker_admission_process_retirement_journals "$root" || return 1
  vp_worker_control_process_retirements \
    "$root" "$VP_WORKER_CONTROL_GENERATION" || return 1
  vp_worker_redis_marker_cleanup_transaction_baseline
}

vp_worker_admission_finish_transaction() {
  local outcome="$1"
  case "$outcome" in
    succeeded|rolled_back) ;;
    *) return 1 ;;
  esac
  vp_worker_admission_transition_to RETIRING || return 1
  vp_worker_admission_retire_transaction || return 1
  vp_worker_admission_transition_to DONE "$outcome" || return 1
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_PHASE" == DONE ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" archive \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" >/dev/null || return 1
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
}

vp_worker_admission_resume_preparing_transaction() {
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  vp_worker_admission_abort_transaction interrupted_preparing
}

vp_worker_admission_resume_durable_rollback() {
  vp_worker_admission_hydrate_recovery_context || return 1
  vp_restore_worker_admission_transaction \
    "$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS" \
    "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" \
    "$VP_WORKER_ADMISSION_RECOVERY_FAILED_CANDIDATE_RECORDS"
}

vp_worker_admission_resume_forward_failure() {
  vp_worker_admission_hydrate_recovery_context || return 1
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_EARLY_FORWARD" == true ]]; then
    vp_restore_app_snapshots \
      "$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS" \
      "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" false || return 1
    vp_worker_admission_abort_transaction \
      preparing_failed || return 1
    vp_worker_redis_marker_discard_managed_state
    return
  fi
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_PARTIAL_FORWARD" == true ]]; then
    vp_restore_app_snapshots \
      "$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS" \
      "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" false || return 1
    vp_worker_redis_marker_restore_managed_state || return 1
    vp_worker_redis_marker_remove_generation_jobs \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" || return 1
    vp_worker_admission_abort_transaction \
      preparing_failed || return 1
    vp_worker_redis_marker_discard_managed_state
    return
  fi
  vp_worker_admission_abort_vision_jobs || return 1
  if [[ "$VP_WORKER_ADMISSION_RECOVERY_FAILED_FORWARD_CAPTURED" != true ]]; then
    vp_worker_admission_capture_failed_forward \
      "$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES" || return 1
  fi
  vp_worker_admission_transition_to ROLLBACK_PREPARING || return 1
  vp_worker_admission_resume_durable_rollback
}

vp_worker_admission_resume_candidate_restore() {
  vp_worker_admission_hydrate_recovery_context || return 1
  case "$VP_WORKER_ADMISSION_RECOVERY_PHASE" in
    CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING)
      vp_restore_failed_forward_candidate \
        "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES" || return 1
      ;;
    CANDIDATE_RESTORED)
      ;;
    *) return 1 ;;
  esac
  vp_verify_failed_forward_candidate || return 1
  vp_worker_admission_transition_to ROLLBACK_PREPARING || return 1
  vp_worker_admission_resume_durable_rollback
}

vp_reconcile_worker_admission_transaction() {
  vp_worker_admission_lock_assert || return 1
  local iteration
  for ((iteration = 0; iteration < 64; iteration++)); do
    vp_worker_admission_load_replay_plan || return 1
    if [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" != true ]]; then
      VP_WORKER_ADMISSION_TRANSACTION_REPLAYED=true
      return 0
    fi
    if [[ "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" != - ]]; then
      case "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" in
        PROMOTE_WORKERS|PROMOTE_MARKER|PROMOTE_CONTROL|PROMOTE_ROLLBACK_WORKERS|PROMOTE_ROLLBACK_MARKER|PROMOTE_ROLLBACK_CONTROL)
          vp_worker_admission_complete_pending_promotion \
            "$VP_WORKER_ADMISSION_REPLAY_OPERATION_KIND" \
            "$VP_WORKER_ADMISSION_REPLAY_OPERATION_ID" || return 1
          continue
          ;;
        *) return 1 ;;
      esac
    fi
    case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
      PREPARING)
        vp_worker_admission_resume_preparing_transaction || return 1
        ;;
      FORWARD_APPLYING)
        vp_worker_admission_resume_forward_failure || return 1
        ;;
      ROLLBACK_PREPARING|ROLLBACK_APPLYING)
        vp_worker_admission_resume_durable_rollback || return 1
        ;;
      CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING|CANDIDATE_RESTORED)
        vp_worker_admission_resume_candidate_restore || return 1
        ;;
      ABORTING)
        vp_worker_admission_abort_transaction \
          preparing_failed || return 1
        ;;
      FORWARD_VERIFIED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase PROMOTE_WORKERS || return 1
        ;;
      WORKERS_PROMOTED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase PROMOTE_MARKER || return 1
        ;;
      MARKER_PROMOTED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase PROMOTE_CONTROL || return 1
        ;;
      ROLLBACK_VERIFIED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase \
          PROMOTE_ROLLBACK_WORKERS || return 1
        ;;
      ROLLBACK_WORKERS_PROMOTED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase \
          PROMOTE_ROLLBACK_MARKER || return 1
        ;;
      ROLLBACK_MARKER_PROMOTED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_promote_phase \
          PROMOTE_ROLLBACK_CONTROL || return 1
        ;;
      CONTROL_PROMOTED|ROLLBACK_CONTROL_PROMOTED)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_transition_to RETIRING || return 1
        ;;
      RETIRING)
        vp_worker_admission_hydrate_recovery_context || return 1
        vp_worker_admission_retire_transaction || return 1
        vp_worker_admission_load_durable_state || return 1
        local outcome="$VP_WORKER_ADMISSION_DURABLE_RETIRING_OUTCOME"
        [[ "$outcome" =~ ^(succeeded|rolled_back)$ ]] || return 1
        vp_worker_admission_transition_to DONE "$outcome" || return 1
        ;;
      DONE)
        python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" archive \
          "$VP_WORKER_ADMISSION_LOCK_ROOT" \
          "$VP_WORKER_ADMISSION_LOCK_FD" \
          "$VP_WORKER_ADMISSION_REPLAY_REVISION" >/dev/null || return 1
        ;;
      *) return 1 ;;
    esac
  done
  return 1
}

vp_worker_admission_require_stage1_entry_state() {
  local entry_mode="${1:-post-reconcile}"
  case "$entry_mode" in
    pre-reconcile|post-reconcile) ;;
    *) return 1 ;;
  esac
  vp_worker_admission_lock_assert || return 1
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local expected_root
  expected_root="$(vp_worker_admission_root)" || return 1
  [[ -d "$expected_root" && ! -L "$expected_root" \
    && "$root" -ef "$expected_root" ]] || return 1

  local replay_plan
  replay_plan="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      replay-plan "$root" 2>/dev/null
  )" || return 1
  local classification
  classification="$(
    printf '%s\n' "$replay_plan" \
      | python3 -I -c '
import json
import re
import sys


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


try:
    raw = sys.stdin.buffer.read()
    plan = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=reject_duplicate_keys,
    )
    canonical = (
        json.dumps(plan, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    common_fields = {
        "active",
        "allow_new_candidate",
        "allow_stale_cleanup",
        "namespace",
        "next_action",
        "pending_operation",
        "phase",
        "retirements",
        "revision",
        "transaction_id",
    }
    quarantine_fields = common_fields | {
        "journal_sha256",
        "reason_code",
    }
    if raw != canonical or not isinstance(plan, dict):
        raise ValueError
    if set(plan) == quarantine_fields:
        if (
            plan["active"] is not True
            or plan["allow_new_candidate"] is not False
            or plan["allow_stale_cleanup"] is not False
            or plan["namespace"] is not None
            or plan["next_action"] != "QUARANTINE_LEGACY_SCHEMA_1"
            or plan["pending_operation"] is not None
            or not isinstance(plan["phase"], str)
            or plan["reason_code"]
            != "legacy_schema_1_authority_context_unavailable"
            or not isinstance(plan["journal_sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", plan["journal_sha256"])
            is None
            or plan["retirements"] != []
            or type(plan["revision"]) is not int
            or plan["revision"] < 0
            or not isinstance(plan["transaction_id"], str)
            or re.fullmatch(
                r"tx-[0-9a-f]{32}", plan["transaction_id"]
            )
            is None
        ):
            raise ValueError
        print("legacy-quarantine|" + plan["phase"] + "|-")
    elif set(plan) == common_fields and plan["active"] is False:
        if plan != {
            "active": False,
            "allow_new_candidate": True,
            "allow_stale_cleanup": True,
            "namespace": None,
            "next_action": "BEGIN",
            "pending_operation": None,
            "phase": None,
            "retirements": [],
            "revision": None,
            "transaction_id": None,
        }:
            raise ValueError
        print("absent|-|-")
    elif set(plan) == common_fields and plan["active"] is True:
        if (
            plan["allow_new_candidate"] is not False
            or plan["allow_stale_cleanup"] is not False
            or not isinstance(plan["namespace"], str)
            or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", plan["namespace"])
            is None
            or not isinstance(plan["phase"], str)
            or not isinstance(plan["next_action"], str)
            or type(plan["revision"]) is not int
            or plan["revision"] < 0
            or not isinstance(plan["transaction_id"], str)
            or re.fullmatch(
                r"tx-[0-9a-f]{32}", plan["transaction_id"]
            )
            is None
            or not isinstance(plan["retirements"], list)
        ):
            raise ValueError
        if plan["phase"] == "PREPARING" and (
            plan["next_action"] != "RESUME_PREPARING"
            or plan["pending_operation"] is not None
            or plan["retirements"] != []
        ):
            raise ValueError
        print(
            "active|"
            + plan["phase"]
            + "|"
            + plan["namespace"]
        )
    else:
        raise ValueError
except Exception:
    raise SystemExit(1)
'
  )" || return 1

  local active_state
  local active_phase
  local active_namespace
  local extra
  IFS='|' read -r \
    active_state active_phase active_namespace extra \
    <<<"$classification"
  [[ -z "$extra" ]] || return 1
  case "$active_state|$active_phase" in
    absent\|-) return 0 ;;
    legacy-quarantine\|*)
      echo \
        "worker admission transaction quarantined: legacy_schema_1_authority_context_unavailable ($active_phase)" \
      >&2
      return 1
      ;;
    active\|*)
      if [[ "$entry_mode" == pre-reconcile ]]; then
        return 0
      fi
      if [[ "$active_phase" == PREPARING ]]; then
        return 0
      fi
      echo \
        "worker admission transaction requires stage-2 reconciliation: $active_phase" \
        >&2
      return 1
      ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_reset_forward_context() {
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  VP_WORKER_ADMISSION_TRANSACTION_ID=""
  VP_WORKER_ADMISSION_PREPARED=false
  VP_WORKER_ADMISSION_COMMITTED=false
  VP_WORKER_ADMISSION_COMMIT=""
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=""
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  VP_WORKER_FFMPEG_GO_GENERATION=""
  VP_WORKER_FFMPEG_GENERATION=""
  VP_WORKER_VISION_GENERATION=""
  VP_WORKER_YOUTUBE_PUBLISHER_GENERATION=""
  VP_WORKER_FFMPEG_GO_DATABASE_SECRET=""
  VP_WORKER_FFMPEG_GO_ADMISSION_SECRET=""
  VP_WORKER_FFMPEG_DATABASE_SECRET=""
  VP_WORKER_FFMPEG_ADMISSION_SECRET=""
  VP_WORKER_VISION_DATABASE_SECRET=""
  VP_WORKER_VISION_ADMISSION_SECRET=""
  VP_WORKER_YOUTUBE_PUBLISHER_DATABASE_SECRET=""
  VP_WORKER_YOUTUBE_PUBLISHER_ADMISSION_SECRET=""
  VP_WORKER_CONTROL_PREPARED=false
  VP_WORKER_CONTROL_GENERATION=""
  VP_WORKER_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET=""
  VP_STAGING_JANITOR_DATABASE_SECRET=""
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET=""
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET=""
  VP_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_MINIO_SECRET_SECRET=""
  VP_WORKER_ADMISSION_CONTROL_IMAGE=""
  VP_WORKER_CONTROL_PRIOR_GENERATION=""
  VP_WORKER_CONTROL_PRIOR_IMAGE=""
  VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET=""
  VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET=""
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_CONFIG_SHA256=""
  VP_WORKER_REDIS_MARKER_CANDIDATE_CRON_SHA256=""
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
  VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
  VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=""
  VP_WORKER_ADMISSION_JANITOR_GENERATION=""
  VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST=""
  VP_VISION_CUTOVER_JOB_SERVICE_ID=""
  VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS=""
}

vp_worker_admission_prepare_transaction() {
  local backend_image="$1"
  local ffmpeg_go_image="$2"
  local control_image="$3"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
    return 0
  fi
  vp_worker_admission_lock_assert || return 1
  vp_worker_admission_reset_forward_context
  local root="$VP_WORKER_ADMISSION_LOCK_ROOT"
  local expected_root
  expected_root="$(vp_worker_admission_root)" || return 1
  [[ -d "$expected_root" && ! -L "$expected_root" \
    && "$root" -ef "$expected_root" ]] || return 1

  local replay_plan
  replay_plan="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      replay-plan "$root" 2>/dev/null
  )" || return 1
  local replay_fields
  replay_fields="$(
    printf '%s\n' "$replay_plan" \
      | python3 -I -c '
import json
import sys

try:
    plan = json.load(sys.stdin)
    if set(plan) != {
        "active",
        "allow_new_candidate",
        "allow_stale_cleanup",
        "namespace",
        "next_action",
        "pending_operation",
        "phase",
        "retirements",
        "revision",
        "transaction_id",
    }:
        raise ValueError
    if plan["active"]:
        if (
            plan["allow_new_candidate"]
            or plan["allow_stale_cleanup"]
            or not isinstance(plan["phase"], str)
            or not isinstance(plan["namespace"], str)
        ):
            raise ValueError
        print(
            "active|"
            + plan["phase"]
            + "|"
            + plan["namespace"]
        )
    else:
        if (
            not plan["allow_new_candidate"]
            or not plan["allow_stale_cleanup"]
        ):
            raise ValueError
        print("absent|-|-")
except Exception:
    raise SystemExit(1)
'
  )" || return 1
  local active_state
  local active_phase
  local active_namespace
  local extra
  IFS='|' read -r \
    active_state active_phase active_namespace extra \
    <<<"$replay_fields"
  [[ -z "$extra" ]] || return 1
  case "$active_state|$active_phase" in
    absent\|-) ;;
    active\|PREPARING) ;;
    active\|ABORTING|active\|DONE) ;;
    active\|*)
      echo \
        "worker admission transaction requires stage-2 reconciliation: $active_phase" \
        >&2
      return 1
      ;;
    *)
      return 1
      ;;
  esac

  local commit
  commit="$(
    docker image inspect "$control_image" \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
  )" || return 1
  if [[ ! "$commit" =~ ^[0-9a-f]{40}$ \
    || "$control_image" != *":deploy-${commit:0:12}" \
    || "$ffmpeg_go_image" != *":deploy-${commit:0:12}" ]]; then
    echo "worker image commit identity is invalid" >&2
    return 1
  fi
  VP_WORKER_ADMISSION_CONTROL_IMAGE="$control_image"
  if [[ "$active_state" == active \
    && "$active_phase" =~ ^(ABORTING|DONE)$ ]]; then
    [[ "$active_namespace" == "$commit" ]] || return 1
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
    vp_worker_admission_abort_transaction \
      preparing_failed || return 1
    active_state=absent
    active_phase=-
    active_namespace=-
  fi

  local transaction_arguments=(
    "$root"
    "$VP_WORKER_ADMISSION_LOCK_FD"
    "$commit"
    "$backend_image"
    "$ffmpeg_go_image"
  )
  [[ -n "$VP_WORKER_DATABASE_CREDENTIAL_RECORDS" ]] || return 1
  if [[ "$active_state" == active ]]; then
    [[ "$active_namespace" == "$commit" ]] || return 1
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      verify-preparing \
      "${transaction_arguments[@]}" \
      <<<"$VP_WORKER_DATABASE_CREDENTIAL_RECORDS" \
      >/dev/null 2>&1 || return 1
  else
    local baseline_kind=legacy_no_control
    if [[ -e "$root/control-current.conf" ]]; then
      [[ -f "$root/control-current.conf" \
        && ! -L "$root/control-current.conf" ]] || return 1
      baseline_kind=managed
    fi
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      begin \
      "${transaction_arguments[@]}" \
      "$commit" \
      "$baseline_kind" \
      <<<"$VP_WORKER_DATABASE_CREDENTIAL_RECORDS" \
      >/dev/null 2>&1 || return 1
  fi
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_ACTIVE" == true \
    && "$VP_WORKER_ADMISSION_REPLAY_PHASE" == PREPARING \
    && "$VP_WORKER_ADMISSION_REPLAY_TRANSACTION_ID" \
      =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    init-app-progress \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" >/dev/null || return 1
  vp_require_worker_redis_runtime_state || return 1
  vp_worker_admission_record_runtime_redis_state || return 1
  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=true
  VP_WORKER_ADMISSION_COMMIT="$commit"
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$commit"
}

vp_worker_admission_record_runtime_redis_state() {
  local records="$({
    printf '%s|%s|%s\n' \
      control "$VP_WORKER_REDIS_CONTROL_SECRET" \
      "$VP_WORKER_REDIS_CONTROL_SECRET_ID"
    printf '%s|%s|%s\n' \
      ffmpeg_go "$VP_WORKER_REDIS_FFMPEG_GO_SECRET" \
      "$VP_WORKER_REDIS_FFMPEG_GO_SECRET_ID"
    printf '%s|%s|%s\n' \
      ffmpeg "$VP_WORKER_REDIS_FFMPEG_SECRET" \
      "$VP_WORKER_REDIS_FFMPEG_SECRET_ID"
    printf '%s|%s|%s\n' \
      vision "$VP_WORKER_REDIS_VISION_SECRET" \
      "$VP_WORKER_REDIS_VISION_SECRET_ID"
    printf '%s|%s|%s\n' \
      youtube_publisher "$VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET" \
      "$VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET_ID"
    printf '%s|%s|%s\n' \
      watcher "$VP_WORKER_REDIS_WATCHER_SECRET" \
      "$VP_WORKER_REDIS_WATCHER_SECRET_ID"
    printf '%s|%s|%s\n' \
      readiness "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
      "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID"
    printf '%s|%s|%s\n' \
      janitor "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" \
      "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID"
    printf '%s|%s|%s\n' \
      repair "$VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET" \
      "$VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID"
  })" || return 1
  local role
  local name
  local secret_id
  local extra
  while IFS='|' read -r role name secret_id extra; do
    [[ -z "$extra" ]] || return 1
    vp_worker_admission_load_replay_plan || return 1
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      record-runtime-secret \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
      "$role" "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
      "$name" "$secret_id" >/dev/null || return 1
  done <<<"$records"
}

vp_prepare_worker_admission() {
  local control_image="$1"
  local ffmpeg_go_image="$2"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker admission preparation skipped"
    return 0
  fi
  VP_WORKER_ADMISSION_PREPARED=false
  VP_WORKER_ADMISSION_COMMITTED=false
  VP_WORKER_CONTROL_PREPARED=false
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE=""
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  vp_require_worker_redis_runtime_state || return 1
  local commit
  commit="$(
    docker image inspect "$control_image" \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
  )" || return 1
  if [[ ! "$commit" =~ ^[0-9a-f]{40}$ \
    || "$control_image" != *":deploy-${commit:0:12}" \
    || "$ffmpeg_go_image" != *":deploy-${commit:0:12}" ]]; then
    echo "worker image commit identity is invalid" >&2
    return 1
  fi
  local root
  root="$(vp_worker_admission_root)" || return 1
  root="$(
    vp_python_worker_prepare_controlled_directory "$root"
  )" || return 1
  VP_WORKER_ADMISSION_COMMIT="$commit"
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$commit"
  VP_WORKER_ADMISSION_CONTROL_IMAGE="$control_image"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true \
    && "$VP_WORKER_ADMISSION_LOCK_HELD" == true ]] || return 1
  vp_worker_control_capture_prior "$root" || return 1
  vp_worker_admission_prepare_control_roles \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_record_control_selection \
    forward "$root/control-candidates/$VP_WORKER_CONTROL_GENERATION.conf" \
    || return 1
  vp_worker_admission_prepare_service \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_record_prepared_worker_plan \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go_image" || return 1
  vp_worker_admission_prepare_service \
    "$VP_PYTHON_WORKER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_record_prepared_worker_plan \
    "$VP_PYTHON_WORKER_SERVICE" "$control_image" || return 1
  vp_worker_admission_prepare_service \
    "$VP_VISION_WORKER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_record_prepared_worker_plan \
    "$VP_VISION_WORKER_SERVICE" "$control_image" || return 1
  vp_worker_admission_prepare_service \
    "$VP_PUBLISHER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_record_prepared_worker_plan \
    "$VP_PUBLISHER_SERVICE" "$control_image" || return 1
  VP_WORKER_ADMISSION_PREPARED=true
}

vp_worker_admission_service_is_candidate() {
  local service="$1"
  case " $VP_WORKER_ADMISSION_CANDIDATE_SERVICES " in
    *" $service "*) return 0 ;;
    *) return 1 ;;
  esac
}

vp_worker_admission_select_candidate() {
  local service="$1"
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true \
    && "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
      =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  vp_worker_admission_service_is_candidate "$service" || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
  vp_worker_admission_require_v2_manifest \
    "$candidate" "$service" || return 1
  if [[ "$VP_WORKER_MANIFEST_IMAGE" \
      != *":deploy-${VP_WORKER_MANIFEST_COMMIT:0:12}" ]]; then
    return 1
  fi
  VP_WORKER_ADMISSION_COMMIT="$VP_WORKER_MANIFEST_COMMIT"
  vp_worker_admission_set_candidate \
    "$service" \
    "$VP_WORKER_MANIFEST_GENERATION" \
    "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
    "$VP_WORKER_MANIFEST_ADMISSION_SECRET"
}

vp_worker_admission_candidate_records() {
  if [[ -z "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" ]]; then
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
    =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  local service
  for service in $VP_WORKER_ADMISSION_CANDIDATE_SERVICES; do
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
    vp_worker_admission_require_v2_manifest \
      "$candidate" "$service" || return 1
    printf '%s|%s|%s|%s|%s|%s\n' \
      "$service" \
      "$VP_WORKER_MANIFEST_GENERATION" \
      "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
      "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
      "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
      "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
  done
}

vp_worker_admission_snapshot_image() {
  local snapshots="$1"
  local service="$2"
  printf '%s\n' "$snapshots" | awk -F'|' -v service="$service" '
    NF && NF != 4 { invalid=1 }
    $1 == service {
      count++
      image=$3
    }
    END {
      if (invalid || count != 1 || image == "") {
        exit 1
      }
      print image
    }
  '
}

vp_worker_admission_image_commit() {
  local image="$1"
  local commit
  commit="$(
    docker image inspect "$image" \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
  )" || return 1
  if [[ ! "$commit" =~ ^[0-9a-f]{40}$ \
    || "$image" != *":deploy-${commit:0:12}" ]]; then
    return 1
  fi
  printf '%s\n' "$commit"
}

vp_worker_control_select_prior() {
  [[ -n "$VP_WORKER_CONTROL_PRIOR_GENERATION" \
    && -n "$VP_WORKER_CONTROL_PRIOR_IMAGE" ]] || return 1
  VP_WORKER_CONTROL_GENERATION="$VP_WORKER_CONTROL_PRIOR_GENERATION"
  VP_WORKER_OPERATOR_DATABASE_SECRET="$VP_WORKER_CONTROL_PRIOR_OPERATOR_DATABASE_SECRET"
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET="$VP_WORKER_CONTROL_PRIOR_ORCHESTRATOR_DATABASE_SECRET"
  VP_STAGING_JANITOR_DATABASE_SECRET="$VP_WORKER_CONTROL_PRIOR_STAGING_DATABASE_SECRET"
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET="$VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_ACCESS_SECRET"
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET="$VP_WORKER_CONTROL_PRIOR_STAGING_MINIO_SECRET_SECRET"
  VP_WORKER_MINIO_ACCESS_SECRET="$VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_ACCESS_SECRET"
  VP_WORKER_MINIO_SECRET_SECRET="$VP_WORKER_CONTROL_PRIOR_WORKER_MINIO_SECRET_SECRET"
}

vp_prepare_worker_admission_rollback() {
  local snapshots="$1"
  local attempted_services="$2"
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true \
    && -n "$VP_WORKER_ADMISSION_CONTROL_IMAGE" ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    ROLLBACK_PREPARING)
      vp_worker_admission_allocate_rollback_attempt || return 1
      ;;
    ROLLBACK_APPLYING)
      vp_worker_admission_load_durable_state || return 1
      [[ "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_ATTEMPT" \
          =~ ^[1-9][0-9]*$ \
        && "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_NAMESPACE" != - \
        && "$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_MARKER_GENERATION" != - ]] \
        || return 1
      VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_NAMESPACE"
      VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION="$VP_WORKER_ADMISSION_DURABLE_ROLLBACK_MARKER_GENERATION"
      ;;
    *) return 1 ;;
  esac
  local namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  local rollback_control_image="$VP_WORKER_CONTROL_PRIOR_IMAGE"
  local prior_python_image=""
  if vp_app_service_was_attempted \
    "$VP_PYTHON_WORKER_SERVICE" "$attempted_services"; then
    prior_python_image="$(
      vp_worker_admission_snapshot_image \
        "$snapshots" "$VP_PYTHON_WORKER_SERVICE"
    )" || return 1
  fi
  if [[ -n "$prior_python_image" ]]; then
    if [[ -n "$rollback_control_image" \
      && "$rollback_control_image" != "$prior_python_image" ]]; then
      echo "worker rollback control image does not match prior state" >&2
      return 1
    fi
    rollback_control_image="$prior_python_image"
  fi
  [[ -n "$rollback_control_image" \
    && -n "$VP_WORKER_CONTROL_PRIOR_GENERATION" ]] || return 1
  vp_worker_admission_image_commit "$rollback_control_image" \
    >/dev/null || return 1
  vp_worker_control_select_prior || return 1
  VP_WORKER_ADMISSION_CONTROL_IMAGE="$rollback_control_image"
  vp_worker_admission_record_control_selection \
    rollback "$root/control-current.conf" || return 1
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$namespace"
  VP_WORKER_ADMISSION_CANDIDATE_SERVICES=""
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    vp_app_service_was_attempted "$service" "$attempted_services" || continue
    local image
    if ! image="$(
      vp_worker_admission_snapshot_image "$snapshots" "$service"
    )"; then
      continue
    fi
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local current="$root/current/$kind.conf"
    local commit=""
    if vp_worker_admission_read_manifest "$current" "$service"; then
      if [[ "$VP_WORKER_MANIFEST_IMAGE" == "$image" ]]; then
        commit="$VP_WORKER_MANIFEST_COMMIT"
      fi
    elif [[ -e "$current" ]]; then
      echo "worker admission current manifest is invalid" >&2
      return 1
    fi
    if [[ -z "$commit" ]]; then
      commit="$(vp_worker_admission_image_commit "$image")" || return 1
    fi
    vp_worker_admission_prepare_service \
      "$service" "$image" "$rollback_control_image" \
      "$commit" "$root" "$namespace" || return 1
    vp_worker_admission_track_candidate "$service" || return 1
    vp_worker_admission_record_prepared_worker_plan \
      "$service" "$image" || return 1
  done
  vp_prepare_worker_redis_marker_rollback_candidate
}

vp_activate_worker_admission() {
  local service="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local generation
  generation="$(printf '%s\n' "$contract" | cut -d'|' -f6)"
  local root
  root="$(vp_worker_admission_root)" || return 1
  local operator_file="$root/control/$VP_WORKER_CONTROL_GENERATION/worker-registration-operator-database-url"
  vp_worker_admission_operator \
    "$operator_file" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
    activate --service-name "$service" --generation "$generation"
}

vp_worker_admission_candidate_image() {
  local service="$1"
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true \
    && "$VP_WORKER_ADMISSION_COMMIT" =~ ^[0-9a-f]{40}$ ]] || return 1
  if [[ ! "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
    =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]]; then
    case "$service" in
      vp-ffmpeg-worker-go-swarm)
        printf 'vp-ffmpeg-worker-go:deploy-%s\n' \
          "${VP_WORKER_ADMISSION_COMMIT:0:12}"
        ;;
      "$VP_PYTHON_WORKER_SERVICE"|"$VP_VISION_WORKER_SERVICE"|"$VP_PUBLISHER_SERVICE")
        printf 'vp-ffmpeg-worker-python:deploy-%s\n' \
          "${VP_WORKER_ADMISSION_COMMIT:0:12}"
        ;;
      *)
        return 1
        ;;
    esac
    return
  fi
  local root
  root="$(vp_worker_admission_root)" || return 1
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
  vp_worker_admission_read_manifest "$candidate" "$service" || return 1
  [[ "$VP_WORKER_MANIFEST_IMAGE" \
    == *":deploy-${VP_WORKER_MANIFEST_COMMIT:0:12}" ]] || return 1
  printf '%s\n' "$VP_WORKER_MANIFEST_IMAGE"
}

vp_require_worker_service_descriptor() {
  local service="$1"
  local expected_image="${2:-}"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  if [[ -z "$expected_image" ]]; then
    expected_image="$(
      vp_worker_admission_candidate_image "$service"
    )" || return 1
  fi
  vp_worker_service_registration_env \
    "$service" "$expected_image" >/dev/null || return 1
  vp_worker_service_secret_specs "$service" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1
  local network_id="$VP_PIPELINE_NETWORK_ID"

  local expected_constraints=()
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      expected_constraints=(
        "$VP_RUNTIME_CONSTRAINT"
        "$VP_RUNTIME_NODE_CONSTRAINT"
      )
      ;;
    "$VP_PYTHON_WORKER_SERVICE"|"$VP_VISION_WORKER_SERVICE")
      expected_constraints=(
        "$VP_GPU_CONSTRAINT"
        "$VP_GPU_MANAGER_CONSTRAINT"
      )
      ;;
    "$VP_PUBLISHER_SERVICE")
      expected_constraints=(
        "$VP_PUBLISHER_CONSTRAINT"
        "$VP_PUBLISHER_MANAGER_CONSTRAINT"
      )
      ;;
    *)
      return 1
      ;;
  esac

  local validation_args=("$service" "$expected_image" "$network_id")
  local value
  for value in "${expected_constraints[@]}"; do
    validation_args+=("constraint:$value")
  done
  while IFS= read -r value; do
    [[ -n "$value" ]] || continue
    validation_args+=("env:$value")
  done < <(vp_worker_service_registration_env "$service" "$expected_image")
  while IFS= read -r value; do
    [[ -n "$value" ]] || continue
    validation_args+=("secret:$value")
  done < <(vp_worker_service_secret_specs "$service")

  local spec_json
  spec_json="$(
    docker service inspect "$service" --format '{{json .Spec}}'
  )" || return 1
  python3 -c '
import json
import re
import sys

try:
    spec = json.load(sys.stdin)
    service, image, network = sys.argv[1:4]
    expected_constraints = {
        value.removeprefix("constraint:")
        for value in sys.argv[4:]
        if value.startswith("constraint:")
    }
    expected_env = {
        value.removeprefix("env:")
        for value in sys.argv[4:]
        if value.startswith("env:")
    }
    expected_secret_specs = {
        value.removeprefix("secret:")
        for value in sys.argv[4:]
        if value.startswith("secret:")
    }
    expected_secrets = set()
    for secret_spec in expected_secret_specs:
        fields = dict(
            field.split("=", 1)
            for field in secret_spec.split(",")
        )
        if set(fields) != {"source", "target", "uid", "gid", "mode"}:
            raise ValueError
        expected_secrets.add(
            (
                fields["source"],
                fields["target"],
                fields["uid"],
                fields["gid"],
                int(fields["mode"], 8),
            )
        )

    task = spec["TaskTemplate"]
    container = task["ContainerSpec"]
    actual_env = container.get("Env", [])
    env_keys = [value.split("=", 1)[0] for value in actual_env]
    registration_keys = {
        value.split("=", 1)[0] for value in expected_env
    }
    forbidden_env = {
        "DATABASE_URL",
        "REDIS_URL",
        "WORKER_ADMISSION_TOKEN",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE",
        "WORKER_DEPLOY_READ_DATABASE_URL_FILE",
        "WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE",
        "WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE",
    }
    actual_secrets = {
        (
            entry["SecretName"],
            entry["File"]["Name"],
            entry["File"]["UID"],
            entry["File"]["GID"],
            entry["File"]["Mode"],
        )
        for entry in container.get("Secrets", [])
    }
    replicas = spec.get("Mode", {}).get("Replicated", {}).get(
        "Replicas"
    )
    valid = (
        service
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}", service)
        and container.get("Image") == image
        and replicas == 1
        and len(env_keys) == len(set(env_keys))
        and expected_env.issubset(set(actual_env))
        and all(env_keys.count(key) == 1 for key in registration_keys)
        and forbidden_env.isdisjoint(env_keys)
        and actual_secrets == expected_secrets
        and set(task.get("Placement", {}).get("Constraints", []))
        == expected_constraints
        and [entry["Target"] for entry in task.get("Networks", [])]
        == [network]
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
sys.exit(0 if valid else 1)
' "${validation_args[@]}" <<<"$spec_json"
}

vp_registered_worker_service_identity() {
  local reference="$1"
  local expected_service="$2"
  local expected_generation="$3"
  [[ "$reference" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
    && "$expected_service" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
    && "$expected_generation" =~ ^[1-9][0-9]*$ ]] || return 1
  local identity
  identity="$(
    docker service inspect "$reference" --format \
      '{{.ID}}|{{.Spec.Name}}|{{index .Spec.Labels "vp.service"}}|{{index .Spec.Labels "vp.generation"}}|{{index .Spec.Labels "vp.managed-by"}}'
  )" || return 1
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local service_id
  local service_name
  local owner_service
  local generation
  local managed_by
  local extra
  IFS='|' read -r \
    service_id service_name owner_service generation managed_by extra \
    <<<"$identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$service_name" == "$expected_service" \
    && "$owner_service" == "$expected_service" \
    && "$generation" == "$expected_generation" \
    && "$managed_by" == videoprocess-deploy ]] || return 1
  printf '%s\n' "$service_id"
}

vp_registered_worker_service_current_id() {
  local service="$1"
  local identity
  identity="$(
    docker service inspect "$service" --format '{{.ID}}|{{.Spec.Name}}'
  )" || return 1
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local service_id
  local service_name
  local extra
  IFS='|' read -r service_id service_name extra <<<"$identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$service_name" == "$service" ]] || return 1
  printf '%s\n' "$service_id"
}

vp_worker_admission_require_worker_mutation() {
  local action="$1"
  local service="$2"
  local expected_current_id="$3"
  local target_generation="$4"
  case "$action" in
    create) [[ "$expected_current_id" == absent ]] || return 1 ;;
    update|rm)
      [[ "$expected_current_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
      ;;
    *) return 1 ;;
  esac
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local contract_generation
  contract_generation="$(cut -d'|' -f6 <<<"$contract")"
  [[ "$target_generation" == "$contract_generation" \
    && "$target_generation" =~ ^[1-9][0-9]*$ ]]
}

vp_worker_admission_complete_worker_mutation() {
  local action="$1"
  local service="$2"
  local target_generation="$3"
  local service_id="$4"
  case "$action" in
    create|update)
      [[ "$service_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
      ;;
    rm)
      [[ "$service_id" == absent ]] || return 1
      ;;
    *) return 1 ;;
  esac
  vp_worker_admission_kind "$service" >/dev/null \
    && [[ "$target_generation" =~ ^[1-9][0-9]*$ ]]
}

vp_mutate_registered_worker_service() {
  local action="$1"
  local service="$2"
  local expected_current_id="$3"
  local target_generation="$4"
  shift 4
  [[ "$#" -ge 3 \
    && "${1:-}" == service \
    && "${2:-}" == "$action" ]] || return 1
  vp_worker_admission_require_worker_mutation \
    "$action" "$service" "$expected_current_id" \
    "$target_generation" || return 1
  vp_require_worker_redis_marker_status || return 1

  local service_id=""
  case "$action" in
    create)
      if docker service inspect "$service" >/dev/null 2>&1; then
        return 1
      fi
      service_id="$(docker "$@")" || return 1
      [[ "$service_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
      local inspected_created_id
      inspected_created_id="$(
        vp_registered_worker_service_identity \
          "$service_id" "$service" "$target_generation"
      )" || return 1
      [[ "$inspected_created_id" == "$service_id" ]] || return 1
      ;;
    update)
      local inspected_before_id
      inspected_before_id="$(
        vp_registered_worker_service_current_id "$service"
      )" || return 1
      [[ "$inspected_before_id" == "$expected_current_id" ]] || return 1
      local mutation_args=("$@")
      local target_index=$((${#mutation_args[@]} - 1))
      [[ "$target_index" -ge 2 \
        && "${mutation_args[$target_index]}" == "$service" ]] || return 1
      mutation_args[$target_index]="$expected_current_id"
      docker "${mutation_args[@]}" >&2 || return 1
      service_id="$(
        vp_registered_worker_service_identity \
          "$expected_current_id" "$service" "$target_generation"
      )" || return 1
      [[ "$service_id" == "$expected_current_id" ]] || return 1
      ;;
    rm)
      local inspected_remove_id
      inspected_remove_id="$(
        vp_registered_worker_service_identity \
          "$expected_current_id" "$service" "$target_generation"
      )" || return 1
      [[ "$inspected_remove_id" == "$expected_current_id" ]] || return 1
      docker service rm "$expected_current_id" >/dev/null || return 1
      service_id=absent
      ;;
    *) return 1 ;;
  esac
  vp_worker_admission_complete_worker_mutation \
    "$action" "$service" "$target_generation" "$service_id"
}

vp_require_worker_deployment_ready() {
  local service="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  vp_require_pipeline_network_identity || return 1
  local contract
  contract="$(vp_worker_service_contract "$service")" || return 1
  local generation
  generation="$(printf '%s\n' "$contract" | cut -d'|' -f6)"
  vp_require_worker_service_descriptor "$service" || return 1
  local read_file
  read_file="$(
    vp_worker_admission_database_credential_file \
      deploy_read \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  local attempts="${VP_WORKER_DEPLOY_READINESS_ATTEMPTS:-30}"
  local interval="${VP_WORKER_DEPLOY_READINESS_INTERVAL_SECONDS:-2}"
  [[ "$attempts" =~ ^[1-9][0-9]*$ && "$attempts" -le 120 \
    && "$interval" =~ ^[1-9][0-9]*$ && "$interval" -le 30 ]] \
    || return 1
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if vp_run_python_worker_container \
      "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      "$read_file" \
      worker-deploy-read-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
      -- \
      python -m app.services.worker_deployment_cli readiness \
        --service-name "$service" \
        --generation "$generation" >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$attempt" -lt "$attempts" ]]; then
      sleep "$interval" || return 1
    fi
  done
  return 1
}

vp_worker_admission_prepare_query_output() {
  vp_worker_admission_lock_assert || return 1
  [[ "$VP_WORKER_ADMISSION_QUERY_READ_FD" -eq 14 \
    && "$VP_WORKER_ADMISSION_QUERY_WRITE_FD" -eq 15 \
    && "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" == false \
    && "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" == false \
    && -z "$VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE" \
    && -z "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
    && ! -e /dev/fd/14 && ! -e /dev/fd/15 ]] || return 1
  local directory
  directory="$(
    vp_python_worker_prepare_controlled_directory \
      "$VP_WORKER_ADMISSION_LOCK_ROOT/query-output"
  )" || return 1
  local path
  path="$(mktemp "$directory/.query.XXXXXX")" || return 1
  if ! chmod 0600 "$path" \
    || [[ ! -f "$path" || -L "$path" \
      || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    rm -f "$path"
    return 1
  fi
  exec 14<"$path" || {
    rm -f "$path"
    return 1
  }
  if ! exec 15>"$path"; then
    exec 14>&-
    rm -f "$path"
    return 1
  fi
  local identity
  identity="$(
    python3 -I - "$path" 14 15 <<'PY'
import os
import stat
import sys

try:
    path = os.path.abspath(sys.argv[1])
    read_descriptor = int(sys.argv[2])
    write_descriptor = int(sys.argv[3])
    path_metadata = os.lstat(path)
    read_metadata = os.fstat(read_descriptor)
    write_metadata = os.fstat(write_descriptor)
    identities = {
        (path_metadata.st_dev, path_metadata.st_ino),
        (read_metadata.st_dev, read_metadata.st_ino),
        (write_metadata.st_dev, write_metadata.st_ino),
    }
    for metadata in (path_metadata, read_metadata, write_metadata):
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.getuid()
            or metadata.st_gid != os.getgid()
            or metadata.st_nlink != 1
        ):
            raise ValueError
    if len(identities) != 1:
        raise ValueError
    os.unlink(path)
    if os.fstat(read_descriptor).st_nlink != 0:
        raise ValueError
    device, inode = identities.pop()
    print(f"{device}:{inode}")
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
PY
  )" || {
    exec 15>&-
    exec 14>&-
    return 1
  }
  [[ "$identity" =~ ^[0-9]+:[1-9][0-9]*$ \
    && ! -e "$path" ]] || {
    exec 15>&-
    exec 14>&-
    return 1
  }
  VP_WORKER_ADMISSION_QUERY_READ_OPEN=true
  VP_WORKER_ADMISSION_QUERY_WRITE_OPEN=true
  VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE="$path"
  VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY="$identity"
}

vp_worker_admission_verify_query_output_fd() {
  local descriptor="$1"
  local identity="$2"
  [[ "$descriptor" =~ ^(14|15)$ \
    && "$identity" =~ ^[0-9]+:[1-9][0-9]*$ ]] || return 1
  python3 -I -c '
import os
import stat
import sys

try:
    descriptor = int(sys.argv[1])
    expected_device, expected_inode = (
        int(value) for value in sys.argv[2].split(":", 1)
    )
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != os.getuid()
        or metadata.st_gid != os.getgid()
        or metadata.st_nlink != 0
        or (metadata.st_dev, metadata.st_ino)
        != (expected_device, expected_inode)
    ):
        raise ValueError
except (OSError, TypeError, ValueError):
    raise SystemExit(1)
' "$descriptor" "$identity"
}

vp_worker_admission_seal_query_output() {
  [[ "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" == true \
    && "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" == true ]] || return 1
  local status=0
  vp_worker_admission_verify_query_output_fd \
    "$VP_WORKER_ADMISSION_QUERY_WRITE_FD" \
    "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" || status=1
  exec 15>&- || status=1
  VP_WORKER_ADMISSION_QUERY_WRITE_OPEN=false
  return "$status"
}

vp_worker_admission_discard_query_output() {
  local status=0
  if [[ "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" == true ]]; then
    vp_worker_admission_verify_query_output_fd \
      "$VP_WORKER_ADMISSION_QUERY_WRITE_FD" \
      "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" || status=1
    exec 15>&- || status=1
  fi
  if [[ "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" == true ]]; then
    vp_worker_admission_verify_query_output_fd \
      "$VP_WORKER_ADMISSION_QUERY_READ_FD" \
      "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" || status=1
    exec 14>&- || status=1
  fi
  VP_WORKER_ADMISSION_QUERY_READ_OPEN=false
  VP_WORKER_ADMISSION_QUERY_WRITE_OPEN=false
  VP_WORKER_ADMISSION_QUERY_OUTPUT_FILE=""
  VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY=""
  return "$status"
}

vp_worker_admission_parse_query_output() {
  local kind="$1"
  local expected_service="$2"
  local expected_generation="$3"
  [[ "$VP_WORKER_ADMISSION_QUERY_READ_OPEN" == true \
    && "$VP_WORKER_ADMISSION_QUERY_WRITE_OPEN" == false \
    && "$VP_WORKER_ADMISSION_QUERY_READ_FD" -eq 14 \
    && "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
      =~ ^[0-9]+:[1-9][0-9]*$ ]] || return 1
  python3 -I - \
    "$kind" "$VP_WORKER_ADMISSION_QUERY_READ_FD" \
    "$VP_WORKER_ADMISSION_QUERY_OUTPUT_IDENTITY" \
    "$expected_service" "$expected_generation" <<'PY'
import json
import os
import stat
import sys
import uuid

try:
    (
        kind,
        raw_descriptor,
        raw_identity,
        expected_service,
        raw_generation,
    ) = sys.argv[1:]
    descriptor = int(raw_descriptor)
    expected_device, expected_inode = (
        int(value) for value in raw_identity.split(":", 1)
    )
    expected_generation = int(raw_generation)
    opened = os.fstat(descriptor)
    if (
        not stat.S_ISREG(opened.st_mode)
        or stat.S_IMODE(opened.st_mode) != 0o600
        or opened.st_uid != os.getuid()
        or opened.st_gid != os.getgid()
        or opened.st_nlink != 0
        or (opened.st_dev, opened.st_ino)
        != (expected_device, expected_inode)
    ):
        raise ValueError
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    remaining = 1024 * 1024 + 1
    while remaining:
        chunk = os.read(descriptor, min(65536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    raw = b"".join(chunks)
    if not raw or len(raw) > 1024 * 1024:
        raise ValueError
    payload = json.loads(raw.decode("utf-8"))
    canonical = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    if raw != canonical or not isinstance(payload, dict):
        raise ValueError
    if kind == "generation-state":
        if (
            set(payload)
            != {
                "code",
                "generation",
                "grant_state",
                "service_name",
                "status",
            }
            or payload["status"] != "ok"
            or payload["code"] != "worker_deployment_generation_state"
            or payload["service_name"] != expected_service
            or isinstance(payload["generation"], bool)
            or payload["generation"] != expected_generation
            or payload["grant_state"]
            not in {"absent", "pending", "active", "revoked"}
        ):
            raise ValueError
        print(payload["grant_state"])
    elif kind == "retirement-candidates":
        if (
            set(payload)
            != {
                "code",
                "generation",
                "registration_ids",
                "service_name",
                "status",
            }
            or payload["status"] != "ok"
            or payload["code"]
            != "worker_deployment_retirement_candidates"
            or payload["service_name"] != expected_service
            or isinstance(payload["generation"], bool)
            or payload["generation"] != expected_generation
            or not isinstance(payload["registration_ids"], list)
        ):
            raise ValueError
        registration_ids = []
        seen = set()
        for value in payload["registration_ids"]:
            parsed = uuid.UUID(value)
            if str(parsed) != value or value in seen:
                raise ValueError
            seen.add(value)
            registration_ids.append(value)
        for value in registration_ids:
            print(value)
    else:
        raise ValueError
except (KeyError, OSError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
    sys.exit(1)
PY
}

vp_worker_admission_generation_state() {
  local service="$1"
  local generation="$2"
  local control_image="${3:-$VP_WORKER_ADMISSION_CONTROL_IMAGE}"
  VP_WORKER_ADMISSION_GENERATION_STATE=""
  vp_worker_admission_kind "$service" >/dev/null || return 1
  [[ "$generation" =~ ^[1-9][0-9]*$ \
    && "$control_image" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  vp_require_pipeline_network_identity || return 1
  local read_file
  read_file="$(
    vp_worker_admission_database_credential_file \
      deploy_read \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  vp_worker_admission_prepare_query_output || return 1
  local query_status=0
  if vp_run_python_worker_container \
      "$control_image" \
      "$read_file" \
      worker-deploy-read-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
      --query-output \
      -- \
      python -m app.services.worker_deployment_cli \
        generation-state \
        --service-name "$service" \
        --generation "$generation"; then
    vp_worker_admission_seal_query_output || query_status=1
    if [[ "$query_status" -eq 0 ]]; then
      VP_WORKER_ADMISSION_GENERATION_STATE="$(
        vp_worker_admission_parse_query_output \
          generation-state "$service" "$generation" \
          2>/dev/null
      )" || query_status=1
    fi
  else
    query_status=$?
  fi
  vp_worker_admission_discard_query_output || {
    [[ "$query_status" -ne 0 ]] || query_status=1
  }
  [[ "$query_status" -eq 0 ]] || return "$query_status"
  [[ "$VP_WORKER_ADMISSION_GENERATION_STATE" \
    =~ ^(absent|pending|active|revoked)$ ]] || return 1
}

vp_worker_admission_retirement_ids() {
  local service="$1"
  local generation="$2"
  local control_image="${3:-$VP_WORKER_ADMISSION_CONTROL_IMAGE}"
  VP_WORKER_ADMISSION_RETIREMENT_IDS=""
  vp_worker_admission_kind "$service" >/dev/null || return 1
  [[ "$generation" =~ ^[1-9][0-9]*$ \
    && "$control_image" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  vp_require_pipeline_network_identity || return 1
  local read_file
  read_file="$(
    vp_worker_admission_database_credential_file \
      deploy_read \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  vp_worker_admission_prepare_query_output || return 1
  local query_status=0
  if vp_run_python_worker_container \
      "$control_image" \
      "$read_file" \
      worker-deploy-read-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
      --query-output \
      -- \
      python -m app.services.worker_deployment_cli \
        retirement-candidates \
        --service-name "$service" \
        --generation "$generation"; then
    vp_worker_admission_seal_query_output || query_status=1
    if [[ "$query_status" -eq 0 ]]; then
      VP_WORKER_ADMISSION_RETIREMENT_IDS="$(
        vp_worker_admission_parse_query_output \
          retirement-candidates "$service" "$generation" \
          2>/dev/null
      )" || query_status=1
    fi
  else
    query_status=$?
  fi
  vp_worker_admission_discard_query_output || {
    [[ "$query_status" -ne 0 ]] || query_status=1
  }
  return "$query_status"
}

vp_worker_admission_revoke_generation_authority() {
  local service="$1"
  local generation="$2"
  local root="$3"
  local control_image="${4:-$VP_WORKER_ADMISSION_CONTROL_IMAGE}"
  local control_generation="${5:-$VP_WORKER_CONTROL_GENERATION}"
  local operator_reference="${6:-control/$control_generation/worker-registration-operator-database-url}"
  [[ "$root" = /* \
    && "$control_image" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$control_generation" =~ ^c-[0-9a-f]{20}$ \
    && "$operator_reference" \
      == "control/$control_generation/worker-registration-operator-database-url" ]] \
    || return 1
  vp_require_pipeline_network_identity || return 1
  local runtime_state
  runtime_state="$(
    vp_python_worker_prepare_controlled_directory "$root/runtime"
  )" || return 1
  local operator_file="$root/$operator_reference"
  local grant_state
  vp_worker_admission_generation_state \
    "$service" "$generation" "$control_image" || return 1
  grant_state="$VP_WORKER_ADMISSION_GENERATION_STATE"
  local registration_ids
  vp_worker_admission_retirement_ids \
    "$service" "$generation" "$control_image" || return 1
  registration_ids="$VP_WORKER_ADMISSION_RETIREMENT_IDS"
  local registration_id
  while IFS= read -r registration_id; do
    [[ -n "$registration_id" ]] || continue
    vp_worker_admission_operator \
      "$operator_file" "$control_image" \
      revoke-registration \
      --service-name "$service" \
      --registration-id "$registration_id" \
      --reason replaced || return 1
  done <<<"$registration_ids"
  if [[ "$grant_state" != absent ]]; then
    vp_worker_admission_operator \
      "$operator_file" "$control_image" \
      revoke-grant \
      --service-name "$service" \
      --generation "$generation" \
      --reason replaced || return 1
  fi

  local owner_file
  owner_file="$(
    vp_worker_admission_database_credential_file \
      runtime_role_owner \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker runtime-role owner database URL file"
  )" || return 1
  vp_run_python_worker_container \
    "$control_image" \
    "$owner_file" \
    worker-runtime-owner-database-url \
    /runtime-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$runtime_state,dst=/runtime-state" \
    --env WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-runtime-owner-database-url \
    -- \
    python -m app.services.worker_runtime_role_cli \
      revoke --service-name "$service" \
      --generation "$generation" \
      --state-dir /runtime-state >/dev/null || return 1
}

vp_worker_admission_retire_generation() {
  local service="$1"
  local generation="$2"
  local database_secret="$3"
  local database_secret_id="$4"
  local admission_secret="$5"
  local admission_secret_id="$6"
  local root="$7"
  vp_worker_admission_revoke_generation_authority \
    "$service" "$generation" "$root" || return 1
  vp_remove_managed_secret_if_absent_exact \
    "$database_secret_id" "$database_secret" \
    "$service" "$generation" database || return 1
  vp_remove_managed_secret_if_absent_exact \
    "$admission_secret_id" "$admission_secret" \
    "$service" "$generation" admission || return 1
}

vp_worker_admission_retire_records() {
  local records="$1"
  local root="$2"
  [[ "$root" = /* ]] || return 1
  local seen=""
  local service
  local generation
  local database_secret
  local database_secret_id
  local admission_secret
  local admission_secret_id
  local extra
  while IFS='|' read -r \
    service generation database_secret database_secret_id \
    admission_secret admission_secret_id extra; do
    [[ -n "$service" || -n "$generation" || -n "$database_secret" \
      || -n "$database_secret_id" || -n "$admission_secret" \
      || -n "$admission_secret_id" || -n "$extra" ]] || continue
    if [[ -n "$extra" \
      || ! "$generation" =~ ^[1-9][0-9]*$ \
      || ! "$database_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
      || ! "$database_secret_id" =~ ^[a-z0-9]{20,64}$ \
      || ! "$admission_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
      || ! "$admission_secret_id" =~ ^[a-z0-9]{20,64}$ \
      || "$database_secret_id" == "$admission_secret_id" ]] \
      || ! vp_worker_admission_kind "$service" >/dev/null; then
      return 1
    fi
    case " $seen " in
      *" $service:$generation "*) return 1 ;;
    esac
    seen="${seen:+$seen }$service:$generation"
    vp_worker_admission_retire_generation \
      "$service" "$generation" \
      "$database_secret" "$database_secret_id" \
      "$admission_secret" "$admission_secret_id" \
      "$root" || return 1
  done <<<"$records"
}

vp_worker_admission_discard_namespace() {
  local root="$1"
  local namespace="$2"
  [[ "$root" = /* \
    && "$namespace" =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local directory="$root/candidates/$namespace"
  [[ -e "$directory" ]] || return 0
  [[ -d "$directory" && ! -L "$directory" ]] || return 1
  rm -rf "$directory"
}

vp_worker_admission_write_retirement_journal() {
  local path="$1"
  local records="$2"
  local directory
  directory="$(dirname "$path")" || return 1
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  if [[ -e "$path" ]]; then
    [[ -f "$path" && ! -L "$path" \
      && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
      || return 1
    return 0
  fi
  [[ -n "$records" ]] || return 0
  local temporary
  temporary="$(mktemp "$directory/.retirement.XXXXXX")" || return 1
  if ! chmod 0600 "$temporary" \
    || ! printf '%s\n' "$records" >"$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
}

vp_worker_admission_replace_retirement_journal() {
  local path="$1"
  local records="$2"
  [[ "$path" = /* && -n "$records" ]] || return 1
  python3 - "$path" 3<<<"$records" <<'PY'
import os
import secrets
import stat
import sys

DIRECTORY_MODE = 0o700
FILE_MODE = 0o600
path = os.path.abspath(sys.argv[1])
parent = os.path.dirname(path)
name = os.path.basename(path)
chunks = []
remaining = 1024 * 1024 + 1
while remaining:
    chunk = os.read(3, min(65536, remaining))
    if not chunk:
        break
    chunks.append(chunk)
    remaining -= len(chunk)
payload = b"".join(chunks)
if not payload or len(payload) > 1024 * 1024 or not payload.endswith(b"\n"):
    raise SystemExit(1)
parent_before = os.lstat(parent)
if (
    not stat.S_ISDIR(parent_before.st_mode)
    or stat.S_IMODE(parent_before.st_mode) != DIRECTORY_MODE
    or parent_before.st_uid != os.getuid()
    or parent_before.st_gid != os.getgid()
):
    raise SystemExit(1)
directory_fd = os.open(
    parent,
    os.O_RDONLY
    | os.O_CLOEXEC
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0),
)
temporary_name = ""
try:
    parent_opened = os.fstat(directory_fd)
    if (
        (parent_before.st_dev, parent_before.st_ino)
        != (parent_opened.st_dev, parent_opened.st_ino)
    ):
        raise OSError
    destination = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(destination.st_mode)
        or stat.S_IMODE(destination.st_mode) != FILE_MODE
        or destination.st_uid != os.getuid()
        or destination.st_gid != os.getgid()
        or destination.st_nlink != 1
    ):
        raise OSError
    destination_fd = os.open(
        name,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=directory_fd,
    )
    try:
        opened = os.fstat(destination_fd)
        if (
            (destination.st_dev, destination.st_ino)
            != (opened.st_dev, opened.st_ino)
        ):
            raise OSError
    finally:
        os.close(destination_fd)
    for _attempt in range(32):
        temporary_name = f".retirement.{secrets.token_hex(16)}"
        try:
            temporary_fd = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                FILE_MODE,
                dir_fd=directory_fd,
            )
            break
        except FileExistsError:
            temporary_name = ""
    else:
        raise OSError
    try:
        os.fchmod(temporary_fd, FILE_MODE)
        view = memoryview(payload)
        while view:
            written = os.write(temporary_fd, view)
            if written < 1:
                raise OSError
            view = view[written:]
        os.fsync(temporary_fd)
    finally:
        os.close(temporary_fd)
    current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if (
        (destination.st_dev, destination.st_ino)
        != (current.st_dev, current.st_ino)
    ):
        raise OSError
    os.replace(
        temporary_name,
        name,
        src_dir_fd=directory_fd,
        dst_dir_fd=directory_fd,
    )
    temporary_name = ""
    os.fsync(directory_fd)
except OSError:
    raise SystemExit(1)
finally:
    if temporary_name:
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
        except OSError:
            pass
    os.close(directory_fd)
PY
}

vp_worker_admission_hydrate_retirement_journal() {
  local path="$1"
  VP_WORKER_ADMISSION_HYDRATED_RETIREMENT_RECORDS=""
  [[ -f "$path" && ! -L "$path" \
    && -s "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
    || return 1
  if grep -q '^$' "$path" \
    || ! python3 -c \
      'import pathlib,sys; raise SystemExit(not pathlib.Path(sys.argv[1]).read_bytes().endswith(b"\n"))' \
      "$path"; then
    return 1
  fi
  local records
  records="$(<"$path")"
  [[ -n "$records" ]] || return 1

  local schema=""
  local seen_generations=""
  local seen_names=""
  local seen_ids=""
  local line_count=0
  local service
  local generation
  local database_secret
  local database_secret_id
  local admission_secret
  local admission_secret_id
  local extra
  while IFS='|' read -r \
    service generation database_secret database_secret_id \
    admission_secret admission_secret_id extra; do
    line_count=$((line_count + 1))
    local line_schema=""
    if [[ -z "$admission_secret" && -z "$admission_secret_id" \
      && -z "$extra" ]]; then
      line_schema=1
      admission_secret="$database_secret_id"
      database_secret_id=""
    elif [[ -n "$admission_secret" && -n "$admission_secret_id" \
      && -z "$extra" ]]; then
      line_schema=2
    else
      return 1
    fi
    [[ -z "$schema" || "$schema" == "$line_schema" ]] || return 1
    schema="$line_schema"
    [[ "$generation" =~ ^[1-9][0-9]*$ \
      && "$database_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
      && "$admission_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
      && "$database_secret" != "$admission_secret" ]] || return 1
    vp_worker_admission_kind "$service" >/dev/null || return 1
    case " $seen_generations " in
      *" $service:$generation "*) return 1 ;;
    esac
    case " $seen_names " in
      *" $database_secret "*|*" $admission_secret "*) return 1 ;;
    esac
    seen_generations="${seen_generations:+$seen_generations }$service:$generation"
    seen_names="${seen_names:+$seen_names }$database_secret $admission_secret"
    if [[ "$schema" == 2 ]]; then
      [[ "$database_secret_id" =~ ^[a-z0-9]{20,64}$ \
        && "$admission_secret_id" =~ ^[a-z0-9]{20,64}$ \
        && "$database_secret_id" != "$admission_secret_id" ]] || return 1
      case " $seen_ids " in
        *" $database_secret_id "*|*" $admission_secret_id "*) return 1 ;;
      esac
      seen_ids="${seen_ids:+$seen_ids }$database_secret_id $admission_secret_id"
    fi
  done <<<"$records"
  [[ "$line_count" -gt 0 && -n "$schema" ]] || return 1
  if [[ "$schema" == 2 ]]; then
    VP_WORKER_ADMISSION_HYDRATED_RETIREMENT_RECORDS="$records"
    return 0
  fi

  local hydrated=""
  seen_ids=""
  while IFS='|' read -r \
    service generation database_secret admission_secret extra; do
    [[ -z "$extra" ]] || return 1
    database_secret_id="$(
      vp_managed_secret_id \
        "$database_secret" "$database_secret" \
        "$service" "$generation" database
    )" || return 1
    admission_secret_id="$(
      vp_managed_secret_id \
        "$admission_secret" "$admission_secret" \
        "$service" "$generation" admission
    )" || return 1
    [[ "$database_secret_id" != "$admission_secret_id" ]] || return 1
    case " $seen_ids " in
      *" $database_secret_id "*|*" $admission_secret_id "*) return 1 ;;
    esac
    seen_ids="${seen_ids:+$seen_ids }$database_secret_id $admission_secret_id"
    local record="$service|$generation|$database_secret|$database_secret_id|$admission_secret|$admission_secret_id"
    hydrated="${hydrated:+$hydrated$'\n'}$record"
  done <<<"$records"
  vp_worker_admission_replace_retirement_journal \
    "$path" "$hydrated" || return 1
  VP_WORKER_ADMISSION_HYDRATED_RETIREMENT_RECORDS="$hydrated"
}

vp_worker_admission_process_retirement_journals() {
  local root="$1"
  [[ "$root" = /* ]] || return 1
  local directory="$root/retirements"
  [[ -e "$directory" ]] || return 0
  if [[ ! -d "$directory" || -L "$directory" \
    || "$(vp_worker_redis_marker_file_mode "$directory")" != 700 ]]; then
    return 1
  fi
  local journal
  for journal in "$directory"/*.records; do
    [[ -e "$journal" ]] || continue
    local basename="${journal##*/}"
    local namespace="${basename%.records}"
    if [[ "$basename" != "$namespace.records" \
      || ! "$namespace" =~ ^[a-z0-9][a-z0-9-]{0,127}$ \
      || ! -f "$journal" || -L "$journal" \
      || "$(vp_worker_redis_marker_file_mode "$journal")" != 600 ]]; then
      return 1
    fi
    local records
    vp_worker_admission_hydrate_retirement_journal \
      "$journal" || return 1
    records="$VP_WORKER_ADMISSION_HYDRATED_RETIREMENT_RECORDS"
    local service
    local generation
    local database_secret
    local database_secret_id
    local admission_secret
    local admission_secret_id
    local extra
    while IFS='|' read -r \
      service generation database_secret database_secret_id \
      admission_secret admission_secret_id extra; do
      [[ -n "$service$generation$database_secret$database_secret_id$admission_secret$admission_secret_id$extra" ]] \
        || continue
      local kind
      kind="$(vp_worker_admission_kind "$service")" || return 1
      local current="$root/current/$kind.conf"
      if vp_worker_admission_read_manifest "$current" "$service"; then
        [[ "$VP_WORKER_MANIFEST_GENERATION" != "$generation" ]] \
          || return 1
      elif [[ -e "$current" ]]; then
        return 1
      fi
    done <<<"$records"
    vp_worker_admission_retire_records \
      "$records" "$root" || return 1
    rm -f "$journal" || return 1
  done
}

vp_commit_worker_admission() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  [[ -z "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" \
    || "$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE" \
      =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  local retirement_dir="$root/retirements"
  local retirement_journal="$retirement_dir/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE.records"
  local retirement_records=""
  local service
  for service in $VP_WORKER_ADMISSION_CANDIDATE_SERVICES; do
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
    vp_worker_admission_require_v2_manifest \
      "$candidate" "$service" || return 1
    local candidate_generation="$VP_WORKER_MANIFEST_GENERATION"

    local current="$root/current/$kind.conf"
    local prior_generation=""
    local prior_database_secret=""
    local prior_database_secret_id=""
    local prior_admission_secret=""
    local prior_admission_secret_id=""
    if [[ -e "$current" ]]; then
      vp_worker_admission_require_v2_manifest \
        "$current" "$service" || {
        echo "worker admission current manifest is invalid" >&2
        return 1
      }
      prior_generation="$VP_WORKER_MANIFEST_GENERATION"
      prior_database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
      prior_database_secret_id="$VP_WORKER_MANIFEST_DATABASE_SECRET_ID"
      prior_admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
      prior_admission_secret_id="$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
    fi

    if [[ -n "$prior_generation" \
      && "$prior_generation" != "$candidate_generation" ]]; then
      retirement_records="${retirement_records:+$retirement_records$'\n'}$service|$prior_generation|$prior_database_secret|$prior_database_secret_id|$prior_admission_secret|$prior_admission_secret_id"
    fi
  done
  vp_worker_admission_write_retirement_journal \
    "$retirement_journal" "$retirement_records" || return 1

  for service in $VP_WORKER_ADMISSION_CANDIDATE_SERVICES; do
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
    vp_worker_admission_require_v2_manifest \
      "$candidate" "$service" || return 1
    local candidate_commit="$VP_WORKER_MANIFEST_COMMIT"
    local candidate_image="$VP_WORKER_MANIFEST_IMAGE"
    local candidate_generation="$VP_WORKER_MANIFEST_GENERATION"
    local candidate_database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
    local candidate_database_secret_id="$VP_WORKER_MANIFEST_DATABASE_SECRET_ID"
    local candidate_admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
    local candidate_admission_secret_id="$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
    local current="$root/current/$kind.conf"
    vp_worker_admission_write_manifest \
      "$current" "$service" "$candidate_commit" \
      "$candidate_image" "$candidate_generation" \
      "$candidate_database_secret" "$candidate_admission_secret" \
      "$candidate_database_secret_id" "$candidate_admission_secret_id" \
      || return 1
  done
  vp_worker_admission_process_retirement_journals "$root" || return 1
  local candidate_dir="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  if [[ -e "$candidate_dir" ]]; then
    [[ -d "$candidate_dir" && ! -L "$candidate_dir" ]] || return 1
    rm -rf "$candidate_dir" || return 1
  fi
  VP_WORKER_ADMISSION_COMMITTED=true
}

vp_worker_control_generation_unused() {
  local generation="$1"
  local allow_missing_services="${2:-false}"
  case "$allow_missing_services" in
    true|false) ;;
    *) return 1 ;;
  esac
  local managed_secrets
  managed_secrets="$(vp_worker_control_secret_names "$generation")" \
    || return 1
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE" \
    vp-staging-object-janitor; do
    local mounted_secrets
    if ! mounted_secrets="$(
      docker service inspect "$service" \
        --format '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}' \
        2>/dev/null
    )"; then
      if [[ "$allow_missing_services" != true ]]; then
        echo "worker control dependency inspection failed" >&2
        return 1
      fi
      local service_names
      service_names="$(
        docker service ls --format '{{.Name}}' 2>/dev/null
      )" || {
        echo "worker control dependency listing failed" >&2
        return 1
      }
      if grep -Fxq "$service" <<<"$service_names"; then
        echo "worker control dependency inspection failed" >&2
        return 1
      fi
      continue
    fi
    local managed_secret
    while IFS= read -r managed_secret; do
      [[ -n "$managed_secret" ]] || continue
      if grep -Fxq "$managed_secret" <<<"$mounted_secrets"; then
        echo "worker control generation is still mounted" >&2
        return 1
      fi
    done <<<"$managed_secrets"
  done
}

vp_worker_control_revoke_authority() {
  local image="$1"
  local generation="$2"
  local root="$3"
  vp_require_pipeline_network_identity || return 1
  [[ "$generation" =~ ^c-[0-9a-f]{20}$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${generation#c-}" == "${image##*:deploy-}"* ]] || return 1
  local control_state
  control_state="$(
    vp_python_worker_prepare_controlled_directory "$root/control"
  )" || return 1
  local owner_file
  owner_file="$(
    vp_worker_admission_database_credential_file \
      control_role_owner \
      "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker control-role owner database URL file"
  )" || return 1
  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-control-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$control_state,dst=/control-state" \
    --env WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-control-owner-database-url \
    -- \
    python -m app.services.worker_control_role_cli \
      revoke --generation "$generation" \
      --state-dir /control-state >/dev/null || return 1
}

vp_worker_control_retire_generation() {
  local image="$1"
  local generation="$2"
  local root="$3"
  local allow_missing_services="${4:-false}"
  [[ -n "$image" && -n "$generation" ]] || return 0
  [[ "$generation" =~ ^c-[0-9a-f]{20}$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${generation#c-}" == "${image##*:deploy-}"* ]] || return 1
  vp_worker_control_generation_unused \
    "$generation" "$allow_missing_services" || return 1
  vp_worker_control_find_v2_manifest \
    "$root" "$generation" >/dev/null || return 1
  [[ "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$image" ]] || return 1
  local secret_refs
  secret_refs="$(vp_worker_control_manifest_secret_refs)" || return 1
  vp_worker_control_revoke_authority \
    "$image" "$generation" "$root" || return 1
  local secret_name
  local secret_id
  local secret_purpose
  while IFS='|' read -r secret_name secret_id secret_purpose; do
    [[ -n "$secret_name" && -n "$secret_id" && -n "$secret_purpose" ]] \
      || return 1
    vp_remove_managed_secret_if_absent_exact \
      "$secret_id" "$secret_name" \
      vp-worker-control "$generation" "$secret_purpose" || return 1
  done <<<"$secret_refs"
  rm -f "$root/control-candidates/$generation.conf"
}

vp_worker_control_cleanup_candidate() {
  local root="$1"
  [[ "$VP_WORKER_CONTROL_PREPARED" == true ]] || return 0
  if [[ "$VP_WORKER_CONTROL_GENERATION" \
    == "$VP_WORKER_CONTROL_PRIOR_GENERATION" ]]; then
    return 0
  fi
  vp_worker_control_retire_generation \
    "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
    "$VP_WORKER_CONTROL_GENERATION" "$root" true || return 1
  VP_WORKER_CONTROL_PREPARED=false
}

vp_worker_control_cleanup_stale_candidates() {
  local root="$1"
  [[ "$root" = /* ]] || return 1
  local directory="$root/control-candidates"
  [[ -e "$directory" ]] || return 0
  if [[ ! -d "$directory" || -L "$directory" \
    || "$(vp_worker_redis_marker_file_mode "$directory")" != 700 ]]; then
    return 1
  fi
  local candidate
  for candidate in "$directory"/*.conf; do
    [[ -e "$candidate" ]] || continue
    local basename="${candidate##*/}"
    local candidate_generation="${basename%.conf}"
    [[ "$basename" == "$candidate_generation.conf" \
      && "$candidate_generation" =~ ^c-[0-9a-f]{20}$ ]] || return 1
    vp_worker_control_read_manifest "$candidate" || return 1
    local generation="$VP_WORKER_CONTROL_MANIFEST_GENERATION"
    local image="$VP_WORKER_CONTROL_MANIFEST_IMAGE"
    [[ "$generation" == "$candidate_generation" ]] || return 1
    if [[ "$generation" == "$VP_WORKER_CONTROL_PRIOR_GENERATION" ]]; then
      [[ "$image" == "$VP_WORKER_CONTROL_PRIOR_IMAGE" ]] || return 1
    else
      vp_worker_control_retire_generation \
        "$image" "$generation" "$root" true || return 1
    fi
    rm -f "$candidate" || return 1
  done
}

vp_require_staging_object_janitor_control() {
  local root="$1"
  local image="$2"
  local config="$root/staging-object-janitor.conf"
  if [[ ! -f "$config" || -L "$config" \
    || "$(vp_worker_redis_marker_file_mode "$config")" != 600 ]]; then
    return 1
  fi
  local expected_config
  expected_config="$(
    printf '%s\n' \
      "VERSION=2" \
      "GENERATION=$VP_WORKER_CONTROL_GENERATION" \
      "IMAGE=$image" \
      "NETWORK=$VP_PIPELINE_NETWORK" \
      "NETWORK_ID=$VP_PIPELINE_NETWORK_ID" \
      "DATABASE_SECRET=$VP_STAGING_JANITOR_DATABASE_SECRET" \
      "MINIO_ACCESS_SECRET=$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET" \
      "MINIO_SECRET_SECRET=$VP_STAGING_JANITOR_MINIO_SECRET_SECRET" \
      "EVIDENCE_VOLUME=vp-staging-janitor-evidence" \
      "MANAGER_NODE=$VP_MANAGER_NODE"
  )"
  [[ "$(<"$config")" == "$expected_config" ]] || return 1
  local spec_json
  spec_json="$(
    docker service inspect vp-staging-object-janitor \
      --format '{{json .Spec}}'
  )" || return 1
  python3 -c '
import json
import sys

try:
    spec = json.load(sys.stdin)
    task = spec["TaskTemplate"]
    container = task["ContainerSpec"]
    labels = spec["Labels"]
    secret_entries = container.get("Secrets", [])
    expected_secrets = {
        (
            sys.argv[4],
            "vp-staging-janitor-database-url",
            "10001",
            "10001",
            0o400,
        ),
        (
            sys.argv[5],
            "vp-staging-janitor-minio-access-key",
            "10001",
            "10001",
            0o400,
        ),
        (
            sys.argv[6],
            "vp-staging-janitor-minio-secret-key",
            "10001",
            "10001",
            0o400,
        ),
    }
    actual_secrets = {
        (
            item["SecretName"],
            item["File"]["Name"],
            item["File"]["UID"],
            item["File"]["GID"],
            item["File"]["Mode"],
        )
        for item in secret_entries
    }
    expected_env = {
        "DEPLOY_MODE=production",
        "VP_STAGING_JANITOR_RUNNER_ID=ccttww-lap",
        "VP_STAGING_JANITOR_DATABASE_URL_FILE=/run/secrets/"
        "vp-staging-janitor-database-url",
        "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE=/run/secrets/"
        "vp-staging-janitor-minio-access-key",
        "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE=/run/secrets/"
        "vp-staging-janitor-minio-secret-key",
        "VP_STAGING_JANITOR_STATUS_FILE=/run/videoprocess/"
        "staging-janitor/status.json",
        "STORAGE_BACKEND=minio",
        "MINIO_ENDPOINT=10.0.0.150:9000",
        "MINIO_BUCKET=videoprocess",
    }
    valid = (
        labels
        == {
            "vp.videoprocess.job": "staging-object-janitor",
            "vp.videoprocess.generation": sys.argv[1],
        }
        and container.get("Image") == sys.argv[2]
        and container.get("User") == "10001:10001"
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "MaxConcurrent"
        )
        == 1
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "TotalCompletions"
        )
        == 1
        and task.get("RestartPolicy", {}).get("Condition") == "none"
        and task.get("Placement", {}).get("Constraints")
        == ["node.hostname==" + sys.argv[7]]
        and [item["Target"] for item in task.get("Networks", [])]
        == [sys.argv[3]]
        and len(secret_entries) == len(expected_secrets)
        and actual_secrets == expected_secrets
        and len(container.get("Env", [])) == len(expected_env)
        and set(container.get("Env", [])) == expected_env
        and container.get("Args")
        == ["python", "-m", "app.channel_agent.staging_object_janitor_cli"]
        and container.get("Configs", []) == []
        and container.get("Mounts")
        == [
            {
                "Type": "volume",
                "Source": "vp-staging-janitor-evidence",
                "Target": "/run/videoprocess/staging-janitor",
            }
        ]
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
sys.exit(0 if valid else 1)
' \
    "$VP_WORKER_CONTROL_GENERATION" \
    "$image" \
    "$VP_PIPELINE_NETWORK_ID" \
    "$VP_STAGING_JANITOR_DATABASE_SECRET" \
    "$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET" \
    "$VP_STAGING_JANITOR_MINIO_SECRET_SECRET" \
    "$VP_MANAGER_NODE" \
    <<<"$spec_json"
}

vp_commit_worker_control_generation() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  [[ "$VP_WORKER_CONTROL_PREPARED" == true \
    && "$VP_WORKER_ADMISSION_COMMITTED" == true \
    && "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" == false ]] \
    || return 1
  vp_require_pipeline_network_identity || return 1
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    vp_require_worker_service_descriptor "$service" || return 1
  done
  local root
  root="$(vp_worker_admission_root)" || return 1
  vp_require_staging_object_janitor_control \
    "$root" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  if [[ -n "$VP_WORKER_CONTROL_PRIOR_GENERATION" \
    && "$VP_WORKER_CONTROL_PRIOR_GENERATION" \
      != "$VP_WORKER_CONTROL_GENERATION" ]]; then
    vp_worker_control_schedule_retirement \
      "$root" \
      "$VP_WORKER_CONTROL_PRIOR_IMAGE" \
      "$VP_WORKER_CONTROL_PRIOR_GENERATION" || return 1
  fi
  vp_worker_control_write_manifest \
    "$root/control-current.conf" \
    "$VP_WORKER_CONTROL_GENERATION" \
    "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  vp_worker_control_process_retirements \
    "$root" "$VP_WORKER_CONTROL_GENERATION" || return 1
  rm -f "$root/control-candidates/$VP_WORKER_CONTROL_GENERATION.conf"
  VP_WORKER_CONTROL_PREPARED=false
}

vp_finalize_worker_control_rollback() {
  [[ "$VP_WORKER_ADMISSION_ROLLBACK_CONVERGED" == true \
    && "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" == false ]] \
    || return 1
  if [[ -z "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" ]]; then
    return 0
  fi
  local root
  root="$(vp_worker_admission_root)" || return 1
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    vp_require_worker_service_descriptor "$service" || return 1
  done
  vp_require_staging_object_janitor_control \
    "$root" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  if [[ "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
    != "$VP_WORKER_CONTROL_GENERATION" ]]; then
    vp_worker_control_schedule_retirement \
      "$root" \
      "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" \
      "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" || return 1
  fi
  vp_worker_control_write_manifest \
    "$root/control-current.conf" \
    "$VP_WORKER_CONTROL_GENERATION" \
    "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  vp_worker_control_process_retirements \
    "$root" "$VP_WORKER_CONTROL_GENERATION" || return 1
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=""
  VP_WORKER_CONTROL_PREPARED=false
}

vp_install_staging_object_janitor() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "staging object janitor install skipped"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true \
    && "$VP_MANAGER_NODE" == ccttww-lap \
    && -r "$VP_STAGING_JANITOR_SOURCE" ]] || return 1
  vp_require_pipeline_network_identity || return 1
  bash -n "$VP_STAGING_JANITOR_SOURCE" || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  if [[ -e "$root" ]]; then
    [[ -d "$root" && ! -L "$root" ]] || return 1
  else
    mkdir -p "$root" || return 1
  fi
  chmod 0700 "$root" || return 1
  local target="$ROOT/bin/vp-staging-object-janitor-run.sh"
  local config="$root/staging-object-janitor.conf"
  local log_file="$ROOT/logs/vp-staging-object-janitor.log"
  local cron_begin="# BEGIN VIDEOPROCESS STAGING JANITOR"
  local cron_end="# END VIDEOPROCESS STAGING JANITOR"
  local cron_command="*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE=$config $target >> $log_file 2>&1"
  local transaction
  transaction="$(mktemp -d "$root/.staging-janitor-install.XXXXXX")" \
    || return 1
  local current_cron="$transaction/current-cron"
  local next_cron="$transaction/next-cron"
  local verify_cron="$transaction/verify-cron"
  local cron_error="$transaction/cron-error"
  local prior_cron_absent=false
  local prior_target=false
  local prior_config=false
  local prior_job_retired=false
  local cron_changed=false
  local status=1

  if LC_ALL=C crontab -l >"$current_cron" 2>"$cron_error"; then
    :
  elif vp_worker_redis_marker_is_no_crontab_error "$cron_error"; then
    : >"$current_cron"
    prior_cron_absent=true
  else
    rm -rf "$transaction"
    return 1
  fi
  if ! awk -v begin="$cron_begin" -v end="$cron_end" \
    -v target="$target" '
      BEGIN { inside=0; invalid=0 }
      $0 == begin {
        if (inside) { invalid=1; exit }
        inside=1
        next
      }
      $0 == end {
        if (!inside) { invalid=1; exit }
        inside=0
        next
      }
      inside { next }
      $1 !~ /^#/ && index($0, target) { next }
      { print }
      END {
        if (inside || invalid) { exit 1 }
      }
    ' "$current_cron" >"$next_cron" \
    || ! printf '%s\n%s\n%s\n' \
      "$cron_begin" "$cron_command" "$cron_end" >>"$next_cron"; then
    rm -rf "$transaction"
    return 1
  fi
  mkdir -p "$ROOT/bin" "$ROOT/logs" || {
    rm -rf "$transaction"
    return 1
  }
  if [[ -e "$target" ]]; then
    cp -p "$target" "$transaction/prior-target" || {
      rm -rf "$transaction"
      return 1
    }
    prior_target=true
  fi
  if [[ -e "$config" ]]; then
    cp -p "$config" "$transaction/prior-config" || {
      rm -rf "$transaction"
      return 1
    }
    prior_config=true
  fi

  if docker service inspect vp-staging-object-janitor \
    >/dev/null 2>&1; then
    if [[ "$prior_target" != true || "$prior_config" != true \
      || "$(vp_worker_redis_marker_file_mode "$target")" != 700 \
      || "$(vp_worker_redis_marker_file_mode "$config")" != 600 ]] \
      || ! VP_STAGING_JANITOR_CONFIG_FILE="$config" \
        "$target" retire >/dev/null; then
      rm -rf "$transaction"
      return 1
    fi
    prior_job_retired=true
  fi

  if install -m 0700 \
      "$VP_STAGING_JANITOR_SOURCE" "$transaction/launcher" \
    && printf '%s\n' \
      "VERSION=2" \
      "GENERATION=$VP_WORKER_CONTROL_GENERATION" \
      "IMAGE=$image" \
      "NETWORK=$VP_PIPELINE_NETWORK" \
      "NETWORK_ID=$VP_PIPELINE_NETWORK_ID" \
      "DATABASE_SECRET=$VP_STAGING_JANITOR_DATABASE_SECRET" \
      "MINIO_ACCESS_SECRET=$VP_STAGING_JANITOR_MINIO_ACCESS_SECRET" \
      "MINIO_SECRET_SECRET=$VP_STAGING_JANITOR_MINIO_SECRET_SECRET" \
      "EVIDENCE_VOLUME=vp-staging-janitor-evidence" \
      "MANAGER_NODE=$VP_MANAGER_NODE" >"$transaction/config" \
    && chmod 0600 "$transaction/config" \
    && mv -f "$transaction/launcher" "$target" \
    && mv -f "$transaction/config" "$config"; then
    cron_changed=true
    if LC_ALL=C crontab "$next_cron" \
      && LC_ALL=C crontab -l >"$verify_cron" 2>"$cron_error" \
      && cmp -s "$next_cron" "$verify_cron" \
      && cmp -s "$VP_STAGING_JANITOR_SOURCE" "$target" \
      && [[ "$(vp_worker_redis_marker_file_mode "$target")" == 700 \
        && "$(vp_worker_redis_marker_file_mode "$config")" == 600 ]]; then
      status=0
    fi
  fi

  if [[ "$status" -ne 0 ]]; then
    if [[ "$prior_target" == true ]]; then
      cp -p "$transaction/prior-target" "$target" || true
    else
      rm -f "$target" || true
    fi
    if [[ "$prior_config" == true ]]; then
      cp -p "$transaction/prior-config" "$config" || true
    else
      rm -f "$config" || true
    fi
    if [[ "$cron_changed" == true ]]; then
      if [[ "$prior_cron_absent" == true ]]; then
        LC_ALL=C crontab -r >/dev/null 2>&1 || true
      else
        LC_ALL=C crontab "$current_cron" >/dev/null 2>&1 || true
      fi
    fi
    if [[ "$prior_job_retired" == true \
      && "$prior_target" == true \
      && "$prior_config" == true ]]; then
      VP_STAGING_JANITOR_CONFIG_FILE="$config" \
        "$target" >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "$transaction"
  return "$status"
}

vp_run_staging_object_janitor_once() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  local root
  root="$(vp_worker_admission_root)" || return 1
  VP_STAGING_JANITOR_CONFIG_FILE="$root/staging-object-janitor.conf" \
    "$ROOT/bin/vp-staging-object-janitor-run.sh" >/dev/null || return 1
  local attempt
  local task_state
  for ((attempt = 1; attempt <= 60; attempt++)); do
    task_state="$(
      docker service ps vp-staging-object-janitor \
        --no-trunc \
        --format '{{.DesiredState}}|{{.CurrentState}}'
    )" || return 1
    case "$task_state" in
      Shutdown\|Complete*)
        return 0
        ;;
      Shutdown\|Failed*|Shutdown\|Rejected*|Shutdown\|Shutdown*)
        return 1
        ;;
      Running\|*)
        sleep 2 || return 1
        ;;
      *)
        return 1
        ;;
    esac
  done
  return 1
}

vp_worker_admission_janitor_service_json() {
  [[ "$VP_WORKER_CONTROL_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  vp_require_staging_object_janitor_control \
    "$root" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  local identity
  identity="$(
    vp_app_service_durable_identity \
      vp-staging-object-janitor "$VP_WORKER_ADMISSION_CONTROL_IMAGE"
  )" || return 1
  local service_id
  local spec_digest
  local extra
  IFS='|' read -r service_id spec_digest extra <<<"$identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  python3 -I -c '
import json
import sys

service_id, generation, spec_digest = sys.argv[1:]
print(json.dumps({
    "name": "vp-staging-object-janitor",
    "docker_service_id": service_id,
    "generation": generation,
    "spec_digest": spec_digest,
}, sort_keys=True, separators=(",", ":")))
' "$service_id" "$VP_WORKER_CONTROL_GENERATION" "$spec_digest"
}

vp_worker_admission_record_janitor_service() {
  local payload
  payload="$(vp_worker_admission_janitor_service_json)" || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-janitor-service \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    <<<"$payload" >/dev/null || return 1
  local fields
  fields="$(printf '%s\n' "$payload" | python3 -I -c '
import json
import sys

value = json.load(sys.stdin)
print("|".join((
    value["docker_service_id"],
    value["generation"],
    value["spec_digest"],
)))
')" || return 1
  local extra
  IFS='|' read -r \
    VP_WORKER_ADMISSION_JANITOR_SERVICE_ID \
    VP_WORKER_ADMISSION_JANITOR_GENERATION \
    VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST extra <<<"$fields"
  [[ -z "$extra" ]]
}

vp_worker_admission_clear_janitor_service() {
  if [[ -z "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" ]]; then
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" \
      =~ ^[a-z0-9]{12,64}$ \
    && "$VP_WORKER_ADMISSION_JANITOR_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST" \
      =~ ^[0-9a-f]{64}$ ]] || return 1
  if docker service inspect "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" \
      >/dev/null 2>&1 \
    || docker service inspect vp-staging-object-janitor \
      >/dev/null 2>&1; then
    return 1
  fi
  local payload
  payload="$(python3 -I -c '
import json
import sys

service_id, generation, spec_digest = sys.argv[1:]
print(json.dumps({
    "name": "vp-staging-object-janitor",
    "docker_service_id": service_id,
    "generation": generation,
    "spec_digest": spec_digest,
}, sort_keys=True, separators=(",", ":")))
' \
    "$VP_WORKER_ADMISSION_JANITOR_SERVICE_ID" \
    "$VP_WORKER_ADMISSION_JANITOR_GENERATION" \
    "$VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST")" || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    clear-janitor-service \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    <<<"$payload" >/dev/null || return 1
  VP_WORKER_ADMISSION_JANITOR_SERVICE_ID=""
  VP_WORKER_ADMISSION_JANITOR_GENERATION=""
  VP_WORKER_ADMISSION_JANITOR_SPEC_DIGEST=""
}

vp_run_worker_registration_migration() {
  local backend_image="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker registration migration skipped because service updates are disabled"
    return 0
  fi
  vp_require_pipeline_network_identity || return 1
  local migrator_file
  migrator_file="$(
    vp_worker_admission_database_credential_file \
      deploy_migrator \
      "${VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE:-}" \
      "worker deploy-migrator database URL file"
  )" || return 1
  if ! docker run --rm \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$migrator_file,dst=/run/secrets/worker-deploy-migrator-database-url,readonly" \
    --env WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE=/run/secrets/worker-deploy-migrator-database-url \
    "$backend_image" \
    python -m app.services.worker_deployment_cli migrate >/dev/null; then
    echo "worker registration migration failed" >&2
    return 1
  fi
  log "worker registration migration applied"
}

vp_require_channelops_migration_head() {
  local backend_image="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "ChannelOps migration head gate skipped because service updates are disabled"
    return 0
  fi
  vp_require_pipeline_network_identity || return 1
  local read_file
  read_file="$(
    vp_worker_admission_database_credential_file \
      deploy_read \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  if ! docker run --rm \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$read_file,dst=/run/secrets/worker-deploy-read-database-url,readonly" \
    --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
    "$backend_image" \
    python -m app.services.worker_deployment_cli verify-head >/dev/null; then
    echo "ChannelOps migration head gate failed; expected exactly 034_worker_registrations" >&2
    return 1
  fi
  log "ChannelOps migration head verified: 034_worker_registrations"
}

vp_runtime_redis_secret_id() {
  local expected_name="$1"
  local expected_id="$2"
  [[ "$expected_name" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$expected_id" =~ ^[a-z0-9]{20,64}$ ]] || return 1
  local identity
  identity="$(
    docker secret inspect "$expected_id" \
      --format '{{.ID}}|{{.Spec.Name}}'
  )" || return 1
  [[ "$identity" == "$expected_id|$expected_name" ]] || return 1
  printf '%s\n' "$expected_id"
}

vp_vision_cutover_job_identity() {
  local reference="$1"
  local expected_name="$2"
  local expected_mode="$3"
  local expected_generation="$4"
  local identity
  identity="$(
    docker service inspect "$reference" --format \
      '{{.ID}}|{{.Spec.Name}}|{{index .Spec.Labels "vp.service"}}|{{index .Spec.Labels "vp.generation"}}|{{index .Spec.Labels "vp.purpose"}}'
  )" || return 1
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local service_id
  local name
  local owner
  local generation
  local purpose
  local extra
  IFS='|' read -r \
    service_id name owner generation purpose extra <<<"$identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$name" == "$expected_name" \
    && "$owner" == vision-cutover \
    && "$generation" == "$expected_generation" \
    && "$purpose" == "$expected_mode" ]] || return 1
  printf '%s\n' "$service_id"
}

vp_require_vision_cutover_job_descriptor() {
  local service_id="$1"
  local expected_name="$2"
  local mode="$3"
  local image="$4"
  local redis_secret_id="$5"
  local database_secret_id="${6:--}"
  local spec_json
  spec_json="$(
    docker service inspect "$service_id" --format '{{json .Spec}}'
  )" || return 1
  python3 -I -c '
import json
import sys

try:
    spec = json.load(sys.stdin)
    (
        expected_name,
        mode,
        generation,
        image,
        network_id,
        manager_node,
        redis_secret_id,
        database_secret_id,
    ) = sys.argv[1:]
    expected_labels = {
        "vp.service": "vision-cutover",
        "vp.generation": generation,
        "vp.purpose": mode,
    }
    task = spec["TaskTemplate"]
    container = task["ContainerSpec"]
    secrets = {
        (
            item["SecretID"],
            item["File"]["Name"],
            item["File"]["UID"],
            item["File"]["GID"],
            item["File"]["Mode"],
        )
        for item in container.get("Secrets", [])
    }
    expected_secrets = {
        (
            redis_secret_id,
            "vision-cutover-redis-url",
            "10001",
            "10001",
            0o400,
        )
    }
    expected_env = {
        "VISION_CUTOVER_REDIS_URL_FILE=/run/secrets/"
        "vision-cutover-redis-url"
    }
    if database_secret_id != "-":
        expected_secrets.add(
            (
                database_secret_id,
                "vision-cutover-database-url",
                "10001",
                "10001",
                0o400,
            )
        )
        expected_env.add(
            "VISION_CUTOVER_DATABASE_URL_FILE=/run/secrets/"
            "vision-cutover-database-url"
        )
    expected_args = [
        "python",
        "-m",
        "app.services.vision_consumer_cutover",
    ]
    if mode in {"safety", "final-safety"}:
        expected_args.append("--safety")
    elif mode == "check":
        expected_args.append("--check-only")
    elif mode != "reconcile":
        raise ValueError
    valid = (
        spec.get("Name") == expected_name
        and spec.get("Labels") == expected_labels
        and container.get("Image") == image
        and container.get("Args") == expected_args
        and set(container.get("Env", [])) == expected_env
        and len(container.get("Env", [])) == len(expected_env)
        and secrets == expected_secrets
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "TotalCompletions"
        ) == 1
        and spec.get("Mode", {}).get("ReplicatedJob", {}).get(
            "MaxConcurrent"
        ) == 1
        and task.get("RestartPolicy", {}).get("Condition") == "none"
        and task.get("Placement", {}).get("Constraints")
        == ["node.hostname==" + manager_node]
        and [item["Target"] for item in task.get("Networks", [])]
        == [network_id]
    )
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    valid = False
raise SystemExit(0 if valid else 1)
' \
    "$expected_name" "$mode" "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    "$image" "$VP_PIPELINE_NETWORK_ID" "$VP_MANAGER_NODE" \
    "$redis_secret_id" "$database_secret_id" <<<"$spec_json"
}

vp_vision_cutover_job_absent() {
  local expected_id="$1"
  local expected_name="$2"
  [[ "$expected_id" == - || "$expected_id" =~ ^[a-z0-9]{12,64}$ ]] \
    || return 1
  [[ "$expected_name" \
    =~ ^vp-vision-cutover-(safety|final-safety|check|reconcile)-[0-9a-f]{12}$ ]] \
    || return 1
  local inventory
  inventory="$(docker service ls --format '{{.ID}}|{{.Name}}')" || return 1
  local service_id
  local service_name
  local extra
  while IFS='|' read -r service_id service_name extra; do
    [[ -n "$service_id$service_name$extra" ]] || continue
    [[ -z "$extra" \
      && "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$service_name" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ ]] \
      || return 1
    if [[ "$service_id" == "$expected_id" \
      || "$service_name" == "$expected_name" ]]; then
      return 1
    fi
  done <<<"$inventory"
}

vp_wait_vision_cutover_job() {
  local service_id="$1"
  local task_record
  local attempt
  for ((attempt = 0; attempt < 120; attempt++)); do
    task_record="$(
      docker service ps "$service_id" --no-trunc \
        --format '{{.ID}}|{{.DesiredState}}|{{.CurrentState}}'
    )" || return 1
    [[ -n "$task_record" && "$task_record" != *$'\n'* ]] || return 1
    local task_id
    local desired_state
    local current_state
    local extra
    IFS='|' read -r \
      task_id desired_state current_state extra <<<"$task_record"
    [[ -z "$extra" && "$task_id" =~ ^[a-z0-9]{12,64}$ ]] \
      || return 1
    case "$desired_state|$current_state" in
      Complete\|Complete*|Shutdown\|Complete*)
        local exit_code
        exit_code="$(
          docker inspect "$task_id" \
            --format '{{.Status.ContainerStatus.ExitCode}}'
        )" || return 1
        [[ "$exit_code" =~ ^(0|[1-9][0-9]{0,2})$ \
          && "$exit_code" -le 255 ]] || return 1
        docker service logs "$service_id" >/dev/null 2>&1 || return 1
        return "$exit_code"
        ;;
      Complete\|Failed*|Complete\|Rejected*|Complete\|Shutdown*|\
      Shutdown\|Failed*|Shutdown\|Rejected*|Shutdown\|Shutdown*)
        docker service logs "$service_id" >/dev/null 2>&1 || true
        return 1
        ;;
      *\|Running*|*\|Pending*|*\|Starting*|*\|Preparing*|*\|Assigned*)
        sleep 1 || return 1
        ;;
      *) return 1 ;;
    esac
  done
  return 1
}

vp_remove_vision_cutover_job() {
  local service_id="$1"
  local expected_name="$2"
  local mode="$3"
  local image="$4"
  local redis_secret_id="$5"
  local database_secret_id="$6"
  if ! docker service inspect "$service_id" >/dev/null 2>&1; then
    vp_vision_cutover_job_absent "$service_id" "$expected_name"
    return
  fi
  vp_require_vision_cutover_job_descriptor \
    "$service_id" "$expected_name" "$mode" "$image" \
    "$redis_secret_id" "$database_secret_id" || return 1
  docker service rm "$service_id" >/dev/null || return 1
  local attempt
  for ((attempt = 0; attempt < 30; attempt++)); do
    if ! docker service inspect "$service_id" >/dev/null 2>&1; then
      vp_vision_cutover_job_absent "$service_id" "$expected_name"
      return
    fi
    sleep 1 || return 1
  done
  return 1
}

vp_worker_admission_prepare_vision_job() {
  local mode="$1"
  local name="$2"
  local image="$3"
  local redis_role="$4"
  local database_secret_name="$5"
  local database_secret_id="$6"
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" prepare-vision-job \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$mode" "$name" "$image" "$redis_role" \
    "$database_secret_name" "$database_secret_id" >/dev/null
}

vp_worker_admission_load_vision_job() {
  local mode="$1"
  local fields
  fields="$(
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" lookup-vision-job \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" "$mode" \
      | python3 -I -c '
import json
import sys

try:
    job = json.load(sys.stdin)
    if job is None:
        print("-|-|-|absent|-|-|-|-|-")
        raise SystemExit(0)
    if not isinstance(job, dict):
        raise ValueError
    redis = job["redis_secret"]
    database = job["database_secret"]
    print("|".join([
        job["name"], job["image"],
        job["docker_service_id"] or "-", job["state"],
        "-" if job["exit_code"] is None else str(job["exit_code"]),
        redis["name"], redis["docker_secret_id"],
        "-" if database is None else database["name"],
        "-" if database is None else database["docker_secret_id"],
    ]))
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
'
  )" || return 1
  local extra
  IFS='|' read -r \
    VP_VISION_JOB_NAME VP_VISION_JOB_IMAGE \
    VP_VISION_JOB_SERVICE_ID VP_VISION_JOB_STATE \
    VP_VISION_JOB_EXIT_CODE VP_VISION_JOB_REDIS_SECRET_NAME \
    VP_VISION_JOB_REDIS_SECRET_ID VP_VISION_JOB_DATABASE_SECRET_NAME \
    VP_VISION_JOB_DATABASE_SECRET_ID extra <<<"$fields"
  [[ -z "$extra" ]]
}

vp_worker_admission_record_vision_job_service() {
  local mode="$1"
  local service_id="$2"
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-vision-job-service \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$mode" "$service_id" >/dev/null
}

vp_worker_admission_record_vision_job_terminal() {
  local mode="$1"
  local exit_code="$2"
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-vision-job-terminal \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$mode" "$exit_code" >/dev/null
}

vp_worker_admission_complete_vision_job_removal() {
  local mode="$1"
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    complete-vision-job-removal \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$mode" >/dev/null
}

vp_worker_admission_abort_vision_job_removal() {
  local mode="$1"
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    abort-vision-job-removal \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$mode" >/dev/null
}

vp_worker_admission_abort_vision_job() {
  local mode="$1"
  local database_purpose=""
  case "$mode" in
    safety) database_purpose=safety-database ;;
    final-safety) database_purpose=final-safety-database ;;
  esac
  vp_worker_admission_load_vision_job "$mode" || return 1
  case "$VP_VISION_JOB_STATE" in
    absent|removed) return 0 ;;
    planned|created|terminal) ;;
    *) return 1 ;;
  esac

  local service_id="$VP_VISION_JOB_SERVICE_ID"
  if [[ "$VP_VISION_JOB_STATE" == planned ]]; then
    if service_id="$(
      vp_vision_cutover_job_identity \
        "$VP_VISION_JOB_NAME" "$VP_VISION_JOB_NAME" "$mode" \
        "$VP_WORKER_ADMISSION_TRANSACTION_ID" 2>/dev/null
    )"; then
      [[ "$service_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
    else
      service_id=-
      vp_vision_cutover_job_absent - "$VP_VISION_JOB_NAME" || return 1
    fi
  elif [[ ! "$service_id" =~ ^[a-z0-9]{12,64}$ ]]; then
    return 1
  fi

  if [[ "$service_id" != - ]]; then
    vp_remove_vision_cutover_job \
      "$service_id" "$VP_VISION_JOB_NAME" "$mode" \
      "$VP_VISION_JOB_IMAGE" "$VP_VISION_JOB_REDIS_SECRET_ID" \
      "$VP_VISION_JOB_DATABASE_SECRET_ID" || return 1
  fi
  if [[ "$VP_VISION_JOB_DATABASE_SECRET_ID" != - ]]; then
    [[ -n "$database_purpose" ]] || return 1
    vp_remove_managed_secret_if_absent_exact \
      "$VP_VISION_JOB_DATABASE_SECRET_ID" \
      "$VP_VISION_JOB_DATABASE_SECRET_NAME" \
      vision-cutover "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
      "$database_purpose" || return 1
  elif [[ -n "$database_purpose" ]]; then
    return 1
  fi
  vp_worker_admission_abort_vision_job_removal "$mode"
}

vp_worker_admission_abort_vision_jobs() {
  local mode
  for mode in safety final-safety check reconcile; do
    vp_worker_admission_abort_vision_job "$mode" || return 1
  done
}

vp_worker_admission_abort_transaction() {
  local reason="$1"
  vp_worker_admission_abort_vision_jobs || return 1
  vp_worker_admission_abort_preparing_transaction "$reason"
}

vp_run_vision_cutover_job() {
  local mode="$1"
  local image="$2"
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
      =~ ^tx-[0-9a-f]{32}$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] \
    || return 1
  vp_require_pipeline_network_identity || return 1
  local redis_secret_name
  local redis_secret_id
  local database_secret_name=""
  local database_secret_id=-
  local database_secret_purpose=""
  local cli_argument=""
  case "$mode" in
    safety|final-safety)
      redis_secret_name="$VP_WORKER_REDIS_WATCHER_SECRET"
      redis_secret_id="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
      cli_argument=--safety
      ;;
    check)
      redis_secret_name="$VP_WORKER_REDIS_WATCHER_SECRET"
      redis_secret_id="$VP_WORKER_REDIS_WATCHER_SECRET_ID"
      cli_argument=--check-only
      ;;
    reconcile)
      redis_secret_name="$VP_WORKER_REDIS_CONTROL_SECRET"
      redis_secret_id="$VP_WORKER_REDIS_CONTROL_SECRET_ID"
      ;;
    *) return 1 ;;
  esac
  redis_secret_id="$(
    vp_runtime_redis_secret_id "$redis_secret_name" "$redis_secret_id"
  )" || return 1
  local transaction_short="${VP_WORKER_ADMISSION_TRANSACTION_ID#tx-}"
  transaction_short="${transaction_short:0:12}"
  local name="vp-vision-cutover-$mode-$transaction_short"
  local redis_role=watcher
  if [[ "$mode" == reconcile ]]; then
    redis_role=control
  fi

  if [[ "$mode" == safety || "$mode" == final-safety ]]; then
    local read_file
    read_file="$(
      vp_worker_admission_database_credential_file \
        deploy_read \
        "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
        "worker deploy-read database URL file"
    )" || return 1
    if [[ "$mode" == final-safety ]]; then
      database_secret_name="vp-vision-cutover-final-read-db-$transaction_short"
      database_secret_purpose=final-safety-database
    else
      database_secret_name="vp-vision-cutover-read-db-$transaction_short"
      database_secret_purpose=safety-database
    fi
    vp_worker_admission_create_secret \
      "$database_secret_name" "$read_file" \
      vision-cutover "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
      "$database_secret_purpose" || return 1
    database_secret_id="$VP_WORKER_CREATED_SECRET_ID"
  fi

  vp_worker_admission_prepare_vision_job \
    "$mode" "$name" "$image" "$redis_role" \
    "${database_secret_name:--}" "$database_secret_id" || return 1
  vp_worker_admission_load_vision_job "$mode" || return 1
  [[ "$VP_VISION_JOB_NAME" == "$name" \
    && "$VP_VISION_JOB_IMAGE" == "$image" \
    && "$VP_VISION_JOB_REDIS_SECRET_NAME" == "$redis_secret_name" \
    && "$VP_VISION_JOB_REDIS_SECRET_ID" == "$redis_secret_id" \
    && "$VP_VISION_JOB_DATABASE_SECRET_NAME" \
      == "${database_secret_name:--}" \
    && "$VP_VISION_JOB_DATABASE_SECRET_ID" == "$database_secret_id" ]] \
    || return 1
  if [[ "$VP_VISION_JOB_STATE" == removed ]]; then
    [[ "$VP_VISION_JOB_EXIT_CODE" =~ ^(0|[1-9][0-9]{0,2})$ ]] \
      || return 1
    return "$VP_VISION_JOB_EXIT_CODE"
  fi

  local service_id="$VP_VISION_JOB_SERVICE_ID"
  if [[ "$VP_VISION_JOB_STATE" == planned ]] && service_id="$(
    vp_vision_cutover_job_identity \
      "$name" "$name" "$mode" \
      "$VP_WORKER_ADMISSION_TRANSACTION_ID" 2>/dev/null
  )"; then
    vp_worker_admission_record_vision_job_service \
      "$mode" "$service_id" || return 1
  elif [[ "$VP_VISION_JOB_STATE" == planned ]]; then
    local create_args=(
      service create --detach=true --name "$name"
      --mode replicated-job --replicas 1 --max-concurrent 1
      --restart-condition none
      --constraint "node.hostname==$VP_MANAGER_NODE"
      --network "$VP_PIPELINE_NETWORK_ID"
      --label vp.service=vision-cutover
      --label "vp.generation=$VP_WORKER_ADMISSION_TRANSACTION_ID"
      --label "vp.purpose=$mode"
      --secret "source=$redis_secret_name,target=vision-cutover-redis-url,uid=10001,gid=10001,mode=0400"
      --env VISION_CUTOVER_REDIS_URL_FILE=/run/secrets/vision-cutover-redis-url
    )
    if [[ "$mode" == safety || "$mode" == final-safety ]]; then
      create_args+=(
        --secret "source=$database_secret_name,target=vision-cutover-database-url,uid=10001,gid=10001,mode=0400"
        --env VISION_CUTOVER_DATABASE_URL_FILE=/run/secrets/vision-cutover-database-url
      )
    fi
    create_args+=(
      "$image" python -m app.services.vision_consumer_cutover
    )
    if [[ -n "$cli_argument" ]]; then
      create_args+=("$cli_argument")
    fi
    service_id="$(docker "${create_args[@]}")" || return 1
    [[ "$service_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
    vp_worker_admission_record_vision_job_service \
      "$mode" "$service_id" || return 1
  elif [[ "$VP_VISION_JOB_STATE" != created \
    && "$VP_VISION_JOB_STATE" != terminal ]]; then
    return 1
  fi
  VP_VISION_CUTOVER_JOB_SERVICE_ID="$service_id"
  local inspected_id
  inspected_id="$(
    vp_vision_cutover_job_identity \
      "$service_id" "$name" "$mode" \
      "$VP_WORKER_ADMISSION_TRANSACTION_ID"
  )" || return 1
  [[ "$inspected_id" == "$service_id" ]] || return 1
  vp_require_vision_cutover_job_descriptor \
    "$service_id" "$name" "$mode" "$image" \
    "$redis_secret_id" "$database_secret_id" || return 1

  local job_status="$VP_VISION_JOB_EXIT_CODE"
  if [[ "$VP_VISION_JOB_STATE" == terminal ]]; then
    [[ "$job_status" =~ ^(0|[1-9][0-9]{0,2})$ ]] || return 1
  elif vp_wait_vision_cutover_job "$service_id"; then
    job_status=0
  else
    job_status=$?
  fi
  if [[ "$VP_VISION_JOB_STATE" != terminal ]]; then
    vp_worker_admission_record_vision_job_terminal \
      "$mode" "$job_status" || return 1
  fi
  local cleanup_status=0
  vp_remove_vision_cutover_job \
    "$service_id" "$name" "$mode" "$image" \
    "$redis_secret_id" "$database_secret_id" || cleanup_status=1
  if [[ "$mode" == safety || "$mode" == final-safety ]]; then
    vp_remove_managed_secret_if_absent_exact \
      "$database_secret_id" "$database_secret_name" \
      vision-cutover "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
      "$database_secret_purpose" || cleanup_status=1
  fi
  [[ "$cleanup_status" -eq 0 ]] || return 1
  vp_worker_admission_complete_vision_job_removal "$mode" || return 1
  return "$job_status"
}

vp_require_vision_cutover_safe() {
  local python_worker="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "vision cutover gate skipped because service updates are disabled"
    return 0
  fi
  if ! vp_run_vision_cutover_job safety "$python_worker"; then
    echo "vision cutover gate failed; require CLOSED schedule and idle vision work" >&2
    return 1
  fi
  log "vision cutover gate verified: CLOSED and idle"
}

vp_vision_cutover_required() {
  local python_worker="$1"
  VP_VISION_CUTOVER_REQUIRED=false
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  local legacy_names
  legacy_names="$(docker container ls -a \
    --filter 'name=^/vp_vision_worker_1$' \
    --format '{{.Names}}')" || return 1
  case "$legacy_names" in
    vp_vision_worker_1)
      VP_VISION_CUTOVER_REQUIRED=true
      return 0
      ;;
    '') ;;
    *) echo "unexpected legacy vision container list result" >&2; return 1 ;;
  esac
  if ! docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    VP_VISION_CUTOVER_REQUIRED=true
    return 0
  fi
  local check_status=0
  if vp_run_vision_cutover_job check "$python_worker"; then
    check_status=0
  else
    check_status=$?
  fi
  case "$check_status" in
    0) VP_VISION_CUTOVER_REQUIRED=false ;;
    10) VP_VISION_CUTOVER_REQUIRED=true ;;
    *) return 1 ;;
  esac
}

vp_service_values() {
  local service="$1"
  local template="$2"
  docker service inspect "$service" --format "$template"
}

vp_require_service_node() {
  local service="$1"
  local expected_node="$2"
  local running_tasks
  running_tasks="$(docker service ps "$service" \
    --filter desired-state=running \
    --format '{{.Node}}|{{.CurrentState}}')" || return 1
  if ! awk -F'|' -v expected="$expected_node" '
    NF {
      total++
      if ($1 == expected && $2 ~ /^Running([[:space:]]|$)/) {
        matched++
      }
    }
    END {
      exit total == 1 && matched == 1 ? 0 : 1
    }
  ' <<<"$running_tasks"; then
    echo "service $service is not running exactly once on $expected_node" >&2
    return 1
  fi
}

vp_gpu_constraint_update_args() {
  local existing_constraints="$1"
  local gpu_count=0
  local manager_count=0
  local constraint
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      "$VP_GPU_CONSTRAINT")
        gpu_count=$((gpu_count + 1))
        ;;
      "$VP_GPU_MANAGER_CONSTRAINT")
        manager_count=$((manager_count + 1))
        ;;
      *)
        printf '%s\n%s\n' --constraint-rm "$constraint"
        ;;
    esac
  done <<<"$existing_constraints"

  if [[ "$gpu_count" -gt 1 || "$manager_count" -gt 1 ]]; then
    echo "GPU worker has duplicate approved placement constraints" >&2
    return 1
  fi
  if [[ "$gpu_count" -eq 0 ]]; then
    printf '%s\n%s\n' --constraint-add "$VP_GPU_CONSTRAINT"
  fi
  if [[ "$manager_count" -eq 0 ]]; then
    printf '%s\n%s\n' --constraint-add "$VP_GPU_MANAGER_CONSTRAINT"
  fi
}

vp_require_managed_worker_storage_ready() {
  local service="$1"
  local require_artifact_api="${2:-false}"
  local containers
  local container_count=0
  local attempt
  for ((attempt = 1; attempt <= 10; attempt++)); do
    if ! containers="$(
      docker container ls \
        --filter "label=com.docker.swarm.service.name=$service" \
        --filter status=running \
        --format '{{.ID}}' \
        2>/dev/null
    )"; then
      echo "managed worker container discovery failed: $service" >&2
      return 1
    fi
    if ! container_count="$(
      printf '%s\n' "$containers" \
        | awk 'NF { count++ } END { print count+0 }' 2>/dev/null
    )"; then
      echo "managed worker container count failed: $service" >&2
      return 1
    fi
    if [[ "$container_count" -eq 1 ]]; then
      break
    fi
    if [[ "$attempt" -lt 10 ]]; then
      if ! sleep 1; then
        echo "managed worker readiness wait failed: $service" >&2
        return 1
      fi
    fi
  done
  if [[ "$container_count" -ne 1 ]]; then
    echo "managed worker storage readiness requires exactly one local running task: $service" >&2
    return 1
  fi
  local args=(python -m app.channel_agent.worker_storage_readiness_cli)
  if [[ "$require_artifact_api" == true ]]; then
    args+=(--require-artifact-api)
  elif [[ "$require_artifact_api" != false ]]; then
    echo "invalid managed worker artifact API readiness mode" >&2
    return 1
  fi
  if ! docker exec "$containers" "${args[@]}" >/dev/null 2>&1; then
    echo "managed worker storage readiness failed: $service" >&2
    return 1
  fi
  log "managed worker storage readiness passed: $service"
}

vp_require_github_actions_success() {
  local repository="$1"
  local workflow="$2"
  local commit="$3"

  if [[ "${BUILD_IMAGES:-1}" -eq 0 && "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  if [[ ! "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "invalid GitHub Actions repository" >&2
    return 1
  fi
  if [[ ! "$workflow" =~ ^[A-Za-z0-9_.-]+\.ya?ml$ ]]; then
    echo "invalid GitHub Actions workflow file" >&2
    return 1
  fi
  if [[ ! "$commit" =~ ^[0-9a-f]{40}$ ]]; then
    echo "invalid Git commit for GitHub Actions gate" >&2
    return 1
  fi
  if ! command -v gh >/dev/null 2>&1; then
    echo "GitHub CLI is required for applying VideoProcess deployments" >&2
    return 1
  fi

  local filter
  filter='if (.workflow_runs | length) == 0 then
    ["missing", "", "", "", ""] | @tsv
  else
    (.workflow_runs | max_by([.run_number // 0, .run_attempt // 0])) as $run |
    ["found", $run.status, ($run.conclusion // ""), $run.head_sha, ($run.id | tostring)] | @tsv
  end'
  local record
  if ! record="$(
    gh api --method GET \
      "repos/$repository/actions/workflows/$workflow/runs" \
      -f "head_sha=$commit" \
      -f event=push \
      -f per_page=20 \
      --jq "$filter"
  )"; then
    echo "GitHub Actions lookup failed for $repository@$commit" >&2
    return 1
  fi
  if [[ -z "$record" || "$record" == *$'\n'* ]]; then
    echo "GitHub Actions lookup returned an invalid result for $repository@$commit" >&2
    return 1
  fi

  local marker
  local run_status
  local conclusion
  local head_sha
  local run_id
  marker="$(awk -F '\t' '{print $1}' <<<"$record")"
  run_status="$(awk -F '\t' '{print $2}' <<<"$record")"
  conclusion="$(awk -F '\t' '{print $3}' <<<"$record")"
  head_sha="$(awk -F '\t' '{print $4}' <<<"$record")"
  run_id="$(awk -F '\t' '{print $5}' <<<"$record")"

  if [[ "$marker" != found ]]; then
    echo "no push CI run exists for $repository@$commit" >&2
    return 1
  fi
  if [[ "$head_sha" != "$commit" ]]; then
    echo "GitHub Actions head SHA mismatch for $repository@$commit" >&2
    return 1
  fi
  if [[ "$run_status" != completed || "$conclusion" != success ]]; then
    echo "GitHub Actions run is not successful for $repository@$commit: status=$run_status conclusion=${conclusion:-none}" >&2
    return 1
  fi
  if [[ ! "$run_id" =~ ^[0-9]+$ ]]; then
    echo "GitHub Actions run ID is invalid for $repository@$commit" >&2
    return 1
  fi

  log "GitHub Actions gate passed $repository@$commit workflow=$workflow run=$run_id"
}

vp_update_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"
  local expected_service_id="${4:-}"
  if [[ -n "$expected_service_id" \
    && ! "$expected_service_id" =~ ^[a-z0-9]{12,64}$ ]]; then
    return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
  fi
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $service $image"
    return 0
  fi
  local current_service_id
  current_service_id="$(
    vp_registered_worker_service_current_id "$service"
  )" || return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
  if [[ -n "$expected_service_id" \
    && "$current_service_id" != "$expected_service_id" ]]; then
    echo "service identity changed before update: $service" >&2
    return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
  fi

  local constraint
  local has_runtime=false
  local has_runtime_node=false
  local constraint_args=()
  local existing_constraints
  if ! existing_constraints="$(
    vp_service_values "$current_service_id" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )"; then
    echo "service constraint inspection failed: $service" >&2
    return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
  fi
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    case "$constraint" in
      "$VP_RUNTIME_CONSTRAINT")
        has_runtime=true
        ;;
      "$VP_RUNTIME_NODE_CONSTRAINT")
        has_runtime_node=true
        ;;
      *)
        constraint_args+=(--constraint-rm "$constraint")
        ;;
    esac
  done <<<"$existing_constraints"
  if [[ "$has_runtime" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_CONSTRAINT")
  fi
  if [[ "$has_runtime_node" != true ]]; then
    constraint_args+=(--constraint-add "$VP_RUNTIME_NODE_CONSTRAINT")
  fi

  local service_args=()
  local worker_generation=""
  local worker_current_id=""
  if [[ "$service" == "vp-api-swarm" ]]; then
    service_args+=(--no-healthcheck)
    local api_env_key
    for api_env_key in \
      DATABASE_URL \
      VP_GO_ORCHESTRATOR_ENABLED \
      VP_GO_ORCHESTRATOR_JOB_WRITES \
      VP_PYTHON_SCHEDULE_URL; do
      if vp_service_values "$current_service_id" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
        | awk -F= -v key="$api_env_key" \
          '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
        service_args+=(--env-rm "$api_env_key")
      fi
    done
    service_args+=(
      --env-add
      "DATABASE_URL=$VP_API_DATABASE_URL_GO"
      --env-add
      "VP_GO_ORCHESTRATOR_ENABLED=true"
      --env-add
      "VP_GO_ORCHESTRATOR_JOB_WRITES=true"
      --env-add
      "VP_PYTHON_SCHEDULE_URL=http://vp-autoflow-api-swarm:8080"
    )
  fi
  if [[ "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
    if [[ "$VP_WORKER_ADMISSION_PREPARED" != true ]] \
      || ! vp_require_pipeline_network_identity \
      || ! vp_worker_service_registration_env \
        "$service" "$image" >/dev/null \
      || ! vp_worker_service_secret_specs "$service" >/dev/null; then
      echo "Go worker admission state is not prepared" >&2
      return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
    fi
    local worker_contract
    worker_contract="$(vp_worker_service_contract "$service")" \
      || return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
    worker_generation="$(cut -d'|' -f6 <<<"$worker_contract")"
    worker_current_id="$current_service_id"
    service_args+=(--replicas 1)
    service_args+=(
      --label-add "vp.service=$service"
      --label-add "vp.generation=$worker_generation"
      --label-add vp.managed-by=videoprocess-deploy
    )
    local existing_worker_env
    existing_worker_env="$(
      vp_service_values "$current_service_id" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}'
    )" || return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
    local worker_env
    local worker_env_key
    while IFS= read -r worker_env; do
      worker_env_key="${worker_env%%=*}"
      if awk -F= -v key="$worker_env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' \
        <<<"$existing_worker_env"; then
        service_args+=(--env-rm "$worker_env_key")
      fi
      service_args+=(--env-add "$worker_env")
    done < <(vp_worker_service_registration_env "$service" "$image")
    for worker_env_key in \
      DATABASE_URL REDIS_URL WORKER_ADMISSION_TOKEN \
      MINIO_ACCESS_KEY MINIO_SECRET_KEY; do
      if awk -F= -v key="$worker_env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' \
        <<<"$existing_worker_env"; then
        service_args+=(--env-rm "$worker_env_key")
      fi
    done
    local existing_worker_secret
    while IFS= read -r existing_worker_secret; do
      [[ -n "$existing_worker_secret" ]] || continue
      service_args+=(--secret-rm "$existing_worker_secret")
    done < <(
      vp_service_values "$current_service_id" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}'
    )
    local worker_secret_spec
    while IFS= read -r worker_secret_spec; do
      service_args+=(--secret-add "$worker_secret_spec")
    done < <(vp_worker_service_secret_specs "$service")
    local worker_network_target
    local worker_has_pipeline_network=false
    local existing_worker_networks
    existing_worker_networks="$(
      vp_service_values "$current_service_id" \
        '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}'
    )" || return "$VP_SERVICE_UPDATE_NOT_ATTEMPTED"
    while IFS= read -r worker_network_target; do
      [[ -n "$worker_network_target" ]] || continue
      if [[ "$worker_network_target" == "$VP_PIPELINE_NETWORK_ID" ]]; then
        worker_has_pipeline_network=true
      else
        service_args+=(--network-rm "$worker_network_target")
      fi
    done <<<"$existing_worker_networks"
    if [[ "$worker_has_pipeline_network" != true ]]; then
      service_args+=(--network-add "$VP_PIPELINE_NETWORK_ID")
    fi
  fi
  if [[ "$service" == "vp-channel-agent-runner-swarm" ]]; then
    local channelops_env_key
    for channelops_env_key in \
      CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS \
      CHANNELOPS_RUNNER_ID; do
      if vp_service_values "$current_service_id" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
        | awk -F= -v key="$channelops_env_key" \
          '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
        service_args+=(--env-rm "$channelops_env_key")
      fi
    done
    service_args+=(
      --env-add
      "CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120"
      --env-add
      "CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1"
      --health-cmd
      "wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1"
      --health-interval
      "10s"
      --health-timeout
      "3s"
      --health-retries
      "6"
      --health-start-period
      "10s"
    )
  fi
  if [[ "$service" == "$VP_PDS_SERVICE" ]]; then
    if vp_service_values "$current_service_id" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "PDS_HTTP_ADDR" { found=1 } END { exit found ? 0 : 1 }'; then
      service_args+=(--env-rm PDS_HTTP_ADDR)
    fi
    service_args+=(
      --env-add
      "PDS_HTTP_ADDR=$VP_PDS_HTTP_ADDR"
      --health-cmd
      ""
      --health-interval
      "10s"
      --health-timeout
      "3s"
      --health-retries
      "6"
      --health-start-period
      "10s"
    )
  fi

  local update_args=(
    service update --detach=false --no-resolve-image --update-order "$order"
  )
  if [[ "${#constraint_args[@]}" -gt 0 ]]; then
    update_args+=("${constraint_args[@]}")
  fi
  if [[ "${#service_args[@]}" -gt 0 ]]; then
    update_args+=("${service_args[@]}")
  fi
  local update_target="$current_service_id"
  if [[ "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
    update_target="$service"
  fi
  update_args+=(--image "$image" "$update_target")
  if [[ "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
    vp_mutate_registered_worker_service \
      update "$service" "$worker_current_id" "$worker_generation" \
      "${update_args[@]}" || return 1
  else
    docker "${update_args[@]}" >&2 || return 1
  fi
}

vp_build_manager_image() {
  local context_dir="$1"
  local dockerfile="$2"
  local image="$3"
  local build_commit="${4:-}"
  if [[ "${BUILD_IMAGES:-1}" -eq 0 ]]; then
    log "build skipped 10.0.0.150:$context_dir $image"
    return 0
  fi
  log "build 10.0.0.150:$context_dir $image"
  local build_args=()
  if [[ -n "$build_commit" ]]; then
    [[ "$build_commit" =~ ^[0-9a-f]{40}$ ]] || return 1
    build_args+=(--build-arg "VP_BUILD_COMMIT=$build_commit")
  fi
  if (( ${#build_args[@]} > 0 )); then
    docker build "${build_args[@]}" \
      -f "$context_dir/$dockerfile" -t "$image" "$context_dir" >&2
  else
    docker build \
      -f "$context_dir/$dockerfile" -t "$image" "$context_dir" >&2
  fi
}

vp_build_runtime_worker_image() {
  local context_dir="$1"
  local dockerfile="$2"
  local image="$3"
  local build_commit="$4"
  if [[ "${BUILD_IMAGES:-1}" -eq 0 ]]; then
    log "build skipped $VP_RUNTIME_HOST:$context_dir $image"
    return 0
  fi
  if [[ "$VP_RUNTIME_HOST" != 10.0.0.127 \
    || "$context_dir" != /Users/wenjieliu/VideoProcess-app \
    || "$dockerfile" != backend/Dockerfile.ffmpeg-worker-go \
    || ! "$build_commit" =~ ^[0-9a-f]{40}$ \
    || "$image" != "vp-ffmpeg-worker-go:deploy-${build_commit:0:12}" ]]; then
    return 1
  fi
  log "build $VP_RUNTIME_HOST:$context_dir $image"
  remote_sh "$VP_RUNTIME_HOST" /bin/sh -s -- \
    "$context_dir" "$dockerfile" "$image" "$build_commit" <<'REMOTE'
set -eu
context_dir="$1"
dockerfile="$2"
image="$3"
build_commit="$4"
case "$context_dir|$dockerfile|$image|$build_commit" in
  *10.0.0.126*|*colima-126*|*colima-swarmbridged*|*CASPERs-Mac-mini*)
    exit 1
    ;;
esac
[ "$context_dir" = /Users/wenjieliu/VideoProcess-app ]
[ "$dockerfile" = backend/Dockerfile.ffmpeg-worker-go ]
printf '%s\n' "$build_commit" | grep -Eq '^[0-9a-f]{40}$'
[ "$image" = "vp-ffmpeg-worker-go:deploy-$(printf '%s' "$build_commit" | cut -c1-12)" ]
exec docker build \
  --build-arg "VP_BUILD_COMMIT=$build_commit" \
  -f "$context_dir/$dockerfile" \
  -t "$image" \
  "$context_dir"
REMOTE
}

build_vp_app_images() {
  local commit="$1"
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_APP_CI_REPOSITORY" "$VP_APP_CI_WORKFLOW" "$commit" || return 1
  local short
  short="$(printf '%s' "$commit" | cut -c1-12)"
  local api="vp-api:deploy-$short"
  local frontend="vp-frontend:deploy-$short"
  local backend="vp-backend-api:deploy-$short"
  local channelops_runner="vp-channelops-runner-go:deploy-$short"
  local ffmpeg_go="vp-ffmpeg-worker-go:deploy-$short"
  local python_worker="vp-ffmpeg-worker-python:deploy-$short"

  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.api-go "$api" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app/frontend \
    Dockerfile "$frontend" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app/backend \
    Dockerfile.api "$backend" || return 1
  build_image_on_host "$VP_RUNTIME_HOST" /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.channelops-runner-go "$channelops_runner" || return 1
  vp_build_runtime_worker_image \
    /Users/wenjieliu/VideoProcess-app \
    backend/Dockerfile.ffmpeg-worker-go \
    "$ffmpeg_go" "$commit" || return 1
  vp_build_manager_image "$REPO_ROOT/videoprocess/backend" \
    Dockerfile.api "$backend" || return 1
  vp_build_manager_image "$REPO_ROOT/videoprocess/backend" \
    Dockerfile.worker "$python_worker" "$commit" || return 1

  printf '%s %s %s %s %s %s\n' \
    "$api" "$frontend" "$backend" "$channelops_runner" "$ffmpeg_go" "$python_worker"
}

build_feature_aggregator_images() {
  local commit="$1"
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_APP_CI_REPOSITORY" "$VP_APP_CI_WORKFLOW" "$commit" || return 1
  local tag
  tag="$(image_tag vp-feature-aggregator "$commit")"
  build_image_on_host "$VP_RUNTIME_HOST" \
    /Users/wenjieliu/.deploy-build/vp-feature-aggregator \
    deploy/Dockerfile "$tag" || return 1
  printf '%s\n' "$tag"
}

build_pds_images() {
  local commit="$1"
  vp_validate_topology || return 1
  vp_require_github_actions_success \
    "$VP_PDS_CI_REPOSITORY" "$VP_PDS_CI_WORKFLOW" "$commit" || return 1
  local tag
  tag="$(image_tag vp-pds "$commit")"
  build_image_on_host "$VP_RUNTIME_HOST" \
    /Users/wenjieliu/.deploy-build/policy-decision-service \
    deploy/Dockerfile "$tag" || return 1
  printf '%s\n' "$tag"
}

vp_resolve_gpu_mode() {
  local image="$1"
  case "${VP_GPU_RUNTIME_READY:-false}" in
    true|TRUE|1|yes|YES|on|ON)
      log "preflight NVIDIA runtime with $image"
      if ! docker run --rm --gpus all "$image" nvidia-smi >/dev/null 2>&1; then
        echo "GPU mode requested but the NVIDIA container runtime preflight failed" >&2
        return 1
      fi
      echo "GPU host preflight passed, but Swarm task GPU allocation is not configured" >&2
      return 1
      ;;
    false|FALSE|0|no|NO|off|OFF|'')
      printf 'false\n'
      ;;
    *)
      echo "invalid VP_GPU_RUNTIME_READY value" >&2
      return 1
      ;;
  esac
}

vp_python_worker_env() {
  local use_gpu="$1"
  local image="$2"
  printf '%s\n' \
    "STORAGE_BACKEND=minio" \
    "STORAGE_LOCAL_ROOT=/data/storage" \
    "MINIO_ENDPOINT=10.0.0.150:9000" \
    "MINIO_BUCKET=videoprocess" \
    "WORKER_CONCURRENCY=${VP_PYTHON_WORKER_CONCURRENCY:-1}" \
    "VIDEO_USE_GPU=$use_gpu" \
    "VIDEO_GPU_FALLBACK_TO_CPU=true" \
    "NVIDIA_VISIBLE_DEVICES=all" \
    "NVIDIA_DRIVER_CAPABILITIES=compute,video,utility"
  vp_worker_service_registration_env "$VP_PYTHON_WORKER_SERVICE" "$image"
}

vp_vision_worker_env() {
  local image="$1"
  printf '%s\n' \
    "STORAGE_BACKEND=minio" \
    "STORAGE_LOCAL_ROOT=/data/storage" \
    "MINIO_ENDPOINT=10.0.0.150:9000" \
    "MINIO_BUCKET=videoprocess" \
    "WORKER_CONCURRENCY=${VP_VISION_WORKER_CONCURRENCY:-1}" \
    "VP_ARTIFACT_DOWNLOAD_BASE_URL=http://vp-api-swarm:8080/api/v1" \
    "VISION_EMBEDDING_URL=${VP_VISION_EMBEDDING_URL:-}" \
    "VIDEO_USE_GPU=false" \
    "VIDEO_GPU_FALLBACK_TO_CPU=true" \
    "VIDEO_WHISPER_DEVICE=cpu"
  vp_worker_service_registration_env "$VP_VISION_WORKER_SERVICE" "$image"
}

vp_publisher_env() {
  local image="$1"
  printf '%s\n' \
    "STORAGE_BACKEND=minio" \
    "STORAGE_LOCAL_ROOT=/data/storage" \
    "MINIO_ENDPOINT=10.0.0.150:9000" \
    "MINIO_BUCKET=videoprocess" \
    "WORKER_CONCURRENCY=1" \
    "YOUTUBE_MANAGER_URL=http://10.0.0.150:18999" \
    "YOUTUBE_PUBLISH_ENABLED=true" \
    "PUBLIC_PUBLISH_ENABLED=false"
  vp_worker_service_registration_env "$VP_PUBLISHER_SERVICE" "$image"
}

vp_publisher_env_is_sensitive() {
  local key="$1"
  case "$key" in
    YOUTUBE_MANAGER_URL|YOUTUBE_PUBLISH_ENABLED)
      return 1
      ;;
    YOUTUBE_*|GOOGLE_*|*OAUTH*|*oauth*|*CLIENT_SECRET*|*client_secret*|*ACCESS_TOKEN*|*access_token*|*REFRESH_TOKEN*|*refresh_token*|*CREDENTIALS_JSON|*credentials_json*|*CREDENTIALS_FILE|*credentials_file*|*CREDENTIAL_FILE|*credential_file*)
      return 0
      ;;
  esac
  return 1
}

vp_publisher_service_state() {
  local service_names
  service_names="$(docker service ls \
    --filter "name=$VP_PUBLISHER_SERVICE" \
    --format '{{.Name}}')" || return 1
  case "$service_names" in
    "$VP_PUBLISHER_SERVICE")
      printf 'exists\n'
      ;;
    '')
      printf 'absent\n'
      ;;
    *)
      echo "unexpected publisher service list result" >&2
      return 1
      ;;
  esac
}

vp_deploy_python_worker() {
  local image="$1"
  local expected_service_id="${2:-}"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PYTHON_WORKER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  if [[ -n "$expected_service_id" ]]; then
    local initial_service_id
    initial_service_id="$(
      vp_registered_worker_service_current_id "$VP_PYTHON_WORKER_SERVICE"
    )" || return 1
    [[ "$initial_service_id" == "$expected_service_id" ]] || return 1
  fi
  vp_worker_service_registration_env \
    "$VP_PYTHON_WORKER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_PYTHON_WORKER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

  local worker_contract
  local worker_generation
  worker_contract="$(vp_worker_service_contract "$VP_PYTHON_WORKER_SERVICE")" \
    || return 1
  worker_generation="$(cut -d'|' -f6 <<<"$worker_contract")"

  local gpu_mode
  gpu_mode="$(vp_resolve_gpu_mode "$image")" || return 1
  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null || return 1

  local env_key
  local env_value
  local env_args=()
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    if docker service inspect "$VP_PYTHON_WORKER_SERVICE" \
      --format '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      2>/dev/null \
      | awk -F= -v key="$env_key" '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
      env_args+=(--env-rm "$env_key")
    fi
    env_args+=(--env-add "$env_value")
  done < <(vp_python_worker_env "$gpu_mode" "$image")

  if docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    local current_service_id
    current_service_id="$(
      vp_registered_worker_service_current_id "$VP_PYTHON_WORKER_SERVICE"
    )" || return 1
    if [[ -n "$expected_service_id" \
      && "$current_service_id" != "$expected_service_id" ]]; then
      return 1
    fi
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first
      --replicas 1 --image "$image"
      --label-add "vp.service=$VP_PYTHON_WORKER_SERVICE"
      --label-add "vp.generation=$worker_generation"
      --label-add vp.managed-by=videoprocess-deploy
    )
    local obsolete_env_key
    for obsolete_env_key in \
      DATABASE_URL REDIS_URL WORKER_ADMISSION_TOKEN \
      MINIO_ACCESS_KEY MINIO_SECRET_KEY; do
      if vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
        | awk -F= -v key="$obsolete_env_key" \
          '$1 == key { found=1 } END { exit found ? 0 : 1 }'; then
        update_args+=(--env-rm "$obsolete_env_key")
      fi
    done
    local existing_constraints
    existing_constraints="$(
      vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
    )" || return 1
    local constraint_args
    constraint_args="$(vp_gpu_constraint_update_args "$existing_constraints")" || return 1
    local constraint
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      update_args+=("$constraint")
    done <<<"$constraint_args"

    local network_target
    local has_pipeline_network=false
    local existing_networks
    existing_networks="$(
      vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}'
    )" || return 1
    while IFS= read -r network_target; do
      [[ -n "$network_target" ]] || continue
      if [[ "$network_target" == "$VP_PIPELINE_NETWORK_ID" ]]; then
        has_pipeline_network=true
      else
        update_args+=(--network-rm "$network_target")
      fi
    done <<<"$existing_networks"
    if [[ "$has_pipeline_network" != true ]]; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK_ID")
    fi
    if vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{println .Target}}{{end}}' \
      | grep -Fxq /app/youtube_credentials; then
      update_args+=(--mount-rm /app/youtube_credentials)
    fi
    if vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
      | awk -F= '$1 == "YOUTUBE_CREDENTIALS_DIR" { found=1 } END { exit found ? 0 : 1 }'; then
      update_args+=(--env-rm YOUTUBE_CREDENTIALS_DIR)
    fi
    local existing_secret
    while IFS= read -r existing_secret; do
      [[ -n "$existing_secret" ]] || continue
      update_args+=(--secret-rm "$existing_secret")
    done < <(
      vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}'
    )
    local secret_spec
    while IFS= read -r secret_spec; do
      update_args+=(--secret-add "$secret_spec")
    done < <(
      vp_worker_service_secret_specs "$VP_PYTHON_WORKER_SERVICE"
    )
    vp_mutate_registered_worker_service \
      update "$VP_PYTHON_WORKER_SERVICE" "$current_service_id" \
      "$worker_generation" "${update_args[@]}" "${env_args[@]}" \
      "$VP_PYTHON_WORKER_SERVICE" || return 1
  else
    [[ -z "$expected_service_id" ]] || return 1
    local create_args=(
      service create --detach=false --name "$VP_PYTHON_WORKER_SERVICE"
      --replicas 1
      --label "vp.service=$VP_PYTHON_WORKER_SERVICE"
      --label "vp.generation=$worker_generation"
      --label vp.managed-by=videoprocess-deploy
      --constraint "$VP_GPU_CONSTRAINT"
      --constraint "$VP_GPU_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK_ID"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-gpu-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_python_worker_env "$gpu_mode" "$image")
    local secret_spec
    while IFS= read -r secret_spec; do
      create_args+=(--secret "$secret_spec")
    done < <(
      vp_worker_service_secret_specs "$VP_PYTHON_WORKER_SERVICE"
    )
    vp_mutate_registered_worker_service \
      create "$VP_PYTHON_WORKER_SERVICE" absent "$worker_generation" \
      "${create_args[@]}" "${create_env[@]}" "$image" || return 1
  fi
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_deploy_vision_worker() {
  local image="$1"
  local expected_service_id="${2:-}"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_VISION_WORKER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  if [[ -n "$expected_service_id" ]]; then
    local initial_service_id
    initial_service_id="$(
      vp_registered_worker_service_current_id "$VP_VISION_WORKER_SERVICE"
    )" || return 1
    [[ "$initial_service_id" == "$expected_service_id" ]] || return 1
  fi
  vp_worker_service_registration_env \
    "$VP_VISION_WORKER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_VISION_WORKER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

  local worker_contract
  local worker_generation
  worker_contract="$(vp_worker_service_contract "$VP_VISION_WORKER_SERVICE")" \
    || return 1
  worker_generation="$(cut -d'|' -f6 <<<"$worker_contract")"

  docker node update --label-add vp.gpu=true "$VP_MANAGER_NODE" >/dev/null || return 1

  local vision_exists=false
  local existing_env=""
  if docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    vision_exists=true
    existing_env="$(vp_service_values "$VP_VISION_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}')" || return 1
  fi

  local env_key
  local env_value
  local env_args=()
  local desired_env_keys=""
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    desired_env_keys="${desired_env_keys}${desired_env_keys:+$'\n'}$env_key"
    if [[ "$vision_exists" == true ]] \
      && awk -F= -v key="$env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' <<<"$existing_env"; then
      env_args+=(--env-rm "$env_key")
    fi
    if [[ "$vision_exists" == true ]]; then
      env_args+=(--env-add "$env_value")
    fi
  done < <(vp_vision_worker_env "$image")

  if [[ "$vision_exists" == true ]]; then
    local current_service_id
    current_service_id="$(
      vp_registered_worker_service_current_id "$VP_VISION_WORKER_SERVICE"
    )" || return 1
    if [[ -n "$expected_service_id" \
      && "$current_service_id" != "$expected_service_id" ]]; then
      return 1
    fi
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
      --label-add "vp.service=$VP_VISION_WORKER_SERVICE"
      --label-add "vp.generation=$worker_generation"
      --label-add vp.managed-by=videoprocess-deploy
    )
    local constraint
    local has_gpu=false
    local has_manager=false
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      if [[ "$constraint" == "$VP_GPU_CONSTRAINT" ]]; then
        has_gpu=true
      elif [[ "$constraint" == "$VP_GPU_MANAGER_CONSTRAINT" ]]; then
        has_manager=true
      else
        update_args+=(--constraint-rm "$constraint")
      fi
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
    )
    if [[ "$has_gpu" != true ]]; then
      update_args+=(--constraint-add "$VP_GPU_CONSTRAINT")
    fi
    if [[ "$has_manager" != true ]]; then
      update_args+=(--constraint-add "$VP_GPU_MANAGER_CONSTRAINT")
    fi

    local network_target
    local has_pipeline_network=false
    local existing_networks
    existing_networks="$(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}'
    )" || return 1
    while IFS= read -r network_target; do
      [[ -n "$network_target" ]] || continue
      if [[ "$network_target" == "$VP_PIPELINE_NETWORK_ID" ]]; then
        has_pipeline_network=true
      else
        update_args+=(--network-rm "$network_target")
      fi
    done <<<"$existing_networks"
    if [[ "$has_pipeline_network" != true ]]; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK_ID")
    fi

    local existing_mounts
    existing_mounts="$(vp_service_values "$VP_VISION_WORKER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{printf "%s|%s|%s|%t\n" .Type .Source .Target .ReadOnly}}{{end}}')" || return 1
    local mount_type
    local mount_source
    local mount_target
    local mount_readonly
    local desired_scratch_count=0
    local remove_scratch_target=false
    while IFS='|' read -r mount_type mount_source mount_target mount_readonly; do
      [[ -n "$mount_type$mount_source$mount_target$mount_readonly" ]] || continue
      if [[ -z "$mount_target" ]]; then
        echo "vision worker mount has no target" >&2
        return 1
      fi
      if [[ "$mount_type" == volume \
        && "$mount_source" == "vp-vision-worker-scratch" \
        && "$mount_target" == /data/storage \
        && "$mount_readonly" == false ]]; then
        desired_scratch_count=$((desired_scratch_count + 1))
        if [[ "$desired_scratch_count" -gt 1 ]]; then
          remove_scratch_target=true
        fi
      elif [[ "$mount_target" == /data/storage ]]; then
        remove_scratch_target=true
      else
        update_args+=(--mount-rm "$mount_target")
      fi
    done <<<"$existing_mounts"
    if [[ "$remove_scratch_target" == true ]]; then
      vp_mutate_registered_worker_service \
        update "$VP_VISION_WORKER_SERVICE" "$current_service_id" \
        "$worker_generation" service update --detach=false \
        --no-resolve-image --update-order stop-first --replicas 0 \
        --label-add "vp.service=$VP_VISION_WORKER_SERVICE" \
        --label-add "vp.generation=$worker_generation" \
        --label-add vp.managed-by=videoprocess-deploy \
        --mount-rm /data/storage "$VP_VISION_WORKER_SERVICE" || return 1
      desired_scratch_count=0
    fi
    if [[ "$desired_scratch_count" -ne 1 ]]; then
      update_args+=(--mount-add type=volume,src=vp-vision-worker-scratch,dst=/data/storage)
    fi

    local existing_secret
    while IFS= read -r existing_secret; do
      [[ -n "$existing_secret" ]] || continue
      update_args+=(--secret-rm "$existing_secret")
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}'
    )
    local existing_config
    while IFS= read -r existing_config; do
      [[ -n "$existing_config" ]] || continue
      update_args+=(--config-rm "$existing_config")
    done < <(
      vp_service_values "$VP_VISION_WORKER_SERVICE" \
        '{{range .Spec.TaskTemplate.ContainerSpec.Configs}}{{println .ConfigName}}{{end}}'
    )
    local secret_spec
    while IFS= read -r secret_spec; do
      update_args+=(--secret-add "$secret_spec")
    done < <(vp_worker_service_secret_specs "$VP_VISION_WORKER_SERVICE")
    while IFS= read -r env_value; do
      env_key="${env_value%%=*}"
      if ! grep -Fxq "$env_key" <<<"$desired_env_keys"; then
        update_args+=(--env-rm "$env_key")
      fi
    done <<<"$existing_env"

    vp_mutate_registered_worker_service \
      update "$VP_VISION_WORKER_SERVICE" "$current_service_id" \
      "$worker_generation" "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_VISION_WORKER_SERVICE" || return 1
  else
    [[ -z "$expected_service_id" ]] || return 1
    local create_args=(
      service create --detach=false --name "$VP_VISION_WORKER_SERVICE"
      --replicas 1
      --label "vp.service=$VP_VISION_WORKER_SERVICE"
      --label "vp.generation=$worker_generation"
      --label vp.managed-by=videoprocess-deploy
      --constraint "$VP_GPU_CONSTRAINT"
      --constraint "$VP_GPU_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK_ID"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-vision-worker-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_vision_worker_env "$image")
    local secret_spec
    while IFS= read -r secret_spec; do
      create_args+=(--secret "$secret_spec")
    done < <(vp_worker_service_secret_specs "$VP_VISION_WORKER_SERVICE")
    vp_mutate_registered_worker_service \
      create "$VP_VISION_WORKER_SERVICE" absent "$worker_generation" \
      "${create_args[@]}" "${create_env[@]}" "$image" || return 1
  fi
  swarm_service_running "$VP_VISION_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_VISION_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_VISION_WORKER_SERVICE" true
}

vp_retire_legacy_vision_worker() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "legacy vision worker retirement skipped"
    return 0
  fi

  local container="vp_vision_worker_1"
  local inspected
  if ! inspected="$(
    docker container inspect \
      --format '{{.Id}}|{{.Name}}|{{.State.Running}}|{{index .Config.Labels "com.docker.compose.project"}}|{{index .Config.Labels "com.docker.compose.service"}}' \
      "$container" 2>/dev/null
  )"; then
    local matching_names
    matching_names="$(docker container ls -a \
      --filter "name=^/$container$" \
      --format '{{.Names}}')" || return 1
    if [[ -z "$matching_names" ]]; then
      return 0
    fi
    echo "legacy vision worker identity could not be inspected" >&2
    return 1
  fi
  local container_id
  local container_name
  local container_running
  local compose_project
  local compose_service
  local extra
  IFS='|' read -r \
    container_id container_name container_running compose_project compose_service extra \
    <<<"$inspected"
  if [[ ! "$container_id" =~ ^[0-9a-f]{64}$ ]] \
    || [[ "$container_name" != "/$container" ]] \
    || [[ "$container_running" != true ]] \
    || [[ "$compose_project" != videoprocess ]] \
    || [[ "$compose_service" != vision-worker ]] \
    || [[ -n "$extra" ]]; then
    echo "refusing to remove unexpected legacy vision container identity" >&2
    return 1
  fi
  docker rm -f "$container_id" >&2
}

vp_reconcile_vision_consumers() {
  local python_worker="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "vision consumer reconciliation skipped"
    return 0
  fi
  if ! vp_run_vision_cutover_job reconcile "$python_worker"; then
    echo "vision consumer reconciliation failed" >&2
    return 1
  fi
  log "vision consumer reconciliation verified"
}

vp_deploy_publisher() {
  local image="$1"
  local expected_service_id="${2:-}"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PUBLISHER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  if [[ -n "$expected_service_id" ]]; then
    local initial_service_id
    initial_service_id="$(
      vp_registered_worker_service_current_id "$VP_PUBLISHER_SERVICE"
    )" || return 1
    [[ "$initial_service_id" == "$expected_service_id" ]] || return 1
  fi
  vp_worker_service_registration_env \
    "$VP_PUBLISHER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_PUBLISHER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

  local worker_contract
  local worker_generation
  worker_contract="$(vp_worker_service_contract "$VP_PUBLISHER_SERVICE")" \
    || return 1
  worker_generation="$(cut -d'|' -f6 <<<"$worker_contract")"

  http_health vp-youtube-manager "http://10.0.0.150:18999/api/auth/status" || return 1

  local env_key
  local env_value
  local env_args=()
  local publisher_exists=false
  local publisher_state
  publisher_state="$(vp_publisher_service_state)" || return 1
  case "$publisher_state" in
    exists)
      publisher_exists=true
      ;;
    absent)
      ;;
    *)
      return 1
      ;;
  esac

  local existing_env=""
  if [[ "$publisher_exists" == true ]]; then
    existing_env="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}')" || return 1
  fi
  while IFS= read -r env_value; do
    env_key="${env_value%%=*}"
    if [[ "$publisher_exists" == true ]] \
      && awk -F= -v key="$env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' <<<"$existing_env"; then
      env_args+=(--env-rm "$env_key")
    fi
    env_args+=(--env-add "$env_value")
  done < <(vp_publisher_env "$image")

  if [[ "$publisher_exists" == true ]]; then
    local current_service_id
    current_service_id="$(
      vp_registered_worker_service_current_id "$VP_PUBLISHER_SERVICE"
    )" || return 1
    if [[ -n "$expected_service_id" \
      && "$current_service_id" != "$expected_service_id" ]]; then
      return 1
    fi
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
      --label-add "vp.service=$VP_PUBLISHER_SERVICE"
      --label-add "vp.generation=$worker_generation"
      --label-add vp.managed-by=videoprocess-deploy
    )
    local constraint
    local has_publisher=false
    local has_manager=false
    local existing_constraints
    existing_constraints="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}')" || return 1
    while IFS= read -r constraint; do
      [[ -n "$constraint" ]] || continue
      case "$constraint" in
        "$VP_PUBLISHER_CONSTRAINT")
          has_publisher=true
          ;;
        "$VP_PUBLISHER_MANAGER_CONSTRAINT")
          has_manager=true
          ;;
        *)
          update_args+=(--constraint-rm "$constraint")
          ;;
      esac
    done <<<"$existing_constraints"
    if [[ "$has_publisher" != true ]]; then
      update_args+=(--constraint-add "$VP_PUBLISHER_CONSTRAINT")
    fi
    if [[ "$has_manager" != true ]]; then
      update_args+=(--constraint-add "$VP_PUBLISHER_MANAGER_CONSTRAINT")
    fi

    local network_target
    local has_pipeline_network=false
    local existing_networks
    existing_networks="$(
      vp_service_values "$VP_PUBLISHER_SERVICE" \
        '{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}'
    )" || return 1
    while IFS= read -r network_target; do
      [[ -n "$network_target" ]] || continue
      if [[ "$network_target" == "$VP_PIPELINE_NETWORK_ID" ]]; then
        has_pipeline_network=true
      else
        update_args+=(--network-rm "$network_target")
      fi
    done <<<"$existing_networks"
    if [[ "$has_pipeline_network" != true ]]; then
      update_args+=(--network-add "$VP_PIPELINE_NETWORK_ID")
    fi

    local existing_mounts
    existing_mounts="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{printf "%s|%s|%s|%t\\n" .Type .Source .Target .ReadOnly}}{{end}}')" || return 1
    local mount_type
    local mount_source
    local mount_target
    local mount_readonly
    local desired_scratch_count=0
    local rebuild_scratch=false
    local remove_scratch_target=false
    while IFS='|' read -r mount_type mount_source mount_target mount_readonly; do
      [[ -n "$mount_type$mount_source$mount_target$mount_readonly" ]] || continue
      if [[ -z "$mount_target" ]]; then
        echo "publisher mount has no target" >&2
        return 1
      fi
      if [[ "$mount_type" == volume \
        && "$mount_source" == "vp-youtube-publisher-scratch" \
        && "$mount_target" == /data/storage \
        && "$mount_readonly" == false ]]; then
        desired_scratch_count=$((desired_scratch_count + 1))
        if [[ "$desired_scratch_count" -gt 1 ]]; then
          remove_scratch_target=true
          rebuild_scratch=true
        fi
      else
        if [[ "$mount_target" == /data/storage ]]; then
          remove_scratch_target=true
          rebuild_scratch=true
        else
          update_args+=(--mount-rm "$mount_target")
        fi
      fi
    done <<<"$existing_mounts"
    if [[ "$desired_scratch_count" -ne 1 || "$rebuild_scratch" == true ]]; then
      update_args+=(--mount-add type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage)
    fi

    local existing_secrets
    existing_secrets="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{println .SecretName}}{{end}}')" || return 1
    local secret_name
    while IFS= read -r secret_name; do
      [[ -n "$secret_name" ]] || continue
      update_args+=(--secret-rm "$secret_name")
    done <<<"$existing_secrets"

    local existing_configs
    existing_configs="$(vp_service_values "$VP_PUBLISHER_SERVICE" \
      '{{range .Spec.TaskTemplate.ContainerSpec.Configs}}{{println .ConfigName}}{{end}}')" || return 1
    local config_name
    while IFS= read -r config_name; do
      [[ -n "$config_name" ]] || continue
      update_args+=(--config-rm "$config_name")
    done <<<"$existing_configs"

    local obsolete_env_key
    for obsolete_env_key in \
      DATABASE_URL REDIS_URL WORKER_ADMISSION_TOKEN \
      MINIO_ACCESS_KEY MINIO_SECRET_KEY; do
      if awk -F= -v key="$obsolete_env_key" \
        '$1 == key { found=1 } END { exit found ? 0 : 1 }' \
        <<<"$existing_env"; then
        update_args+=(--env-rm "$obsolete_env_key")
      fi
    done
    while IFS= read -r env_value; do
      env_key="${env_value%%=*}"
      if vp_publisher_env_is_sensitive "$env_key"; then
        update_args+=(--env-rm "$env_key")
      fi
    done <<<"$existing_env"
    local secret_spec
    while IFS= read -r secret_spec; do
      update_args+=(--secret-add "$secret_spec")
    done < <(vp_worker_service_secret_specs "$VP_PUBLISHER_SERVICE")
    docker node update --label-add vp.publisher=true "$VP_MANAGER_NODE" >/dev/null || return 1
    if [[ "$remove_scratch_target" == true ]]; then
      vp_mutate_registered_worker_service \
        update "$VP_PUBLISHER_SERVICE" "$current_service_id" \
        "$worker_generation" service update --detach=false \
        --no-resolve-image --update-order stop-first --replicas 0 \
        --label-add "vp.service=$VP_PUBLISHER_SERVICE" \
        --label-add "vp.generation=$worker_generation" \
        --label-add vp.managed-by=videoprocess-deploy \
        --mount-rm /data/storage "$VP_PUBLISHER_SERVICE" || return 1
    fi
    vp_mutate_registered_worker_service \
      update "$VP_PUBLISHER_SERVICE" "$current_service_id" \
      "$worker_generation" "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_PUBLISHER_SERVICE" || return 1
  else
    [[ -z "$expected_service_id" ]] || return 1
    local create_args=(
      service create --detach=false --name "$VP_PUBLISHER_SERVICE"
      --replicas 1
      --label "vp.service=$VP_PUBLISHER_SERVICE"
      --label "vp.generation=$worker_generation"
      --label vp.managed-by=videoprocess-deploy
      --constraint "$VP_PUBLISHER_CONSTRAINT"
      --constraint "$VP_PUBLISHER_MANAGER_CONSTRAINT"
      --network "$VP_PIPELINE_NETWORK_ID"
      --restart-condition any --restart-delay 5s
      --mount type=volume,src=vp-youtube-publisher-scratch,dst=/data/storage
    )
    local create_env=()
    while IFS= read -r env_value; do
      create_env+=(--env "$env_value")
    done < <(vp_publisher_env "$image")
    local secret_spec
    while IFS= read -r secret_spec; do
      create_args+=(--secret "$secret_spec")
    done < <(vp_worker_service_secret_specs "$VP_PUBLISHER_SERVICE")
    docker node update --label-add vp.publisher=true "$VP_MANAGER_NODE" >/dev/null || return 1
    vp_mutate_registered_worker_service \
      create "$VP_PUBLISHER_SERVICE" absent "$worker_generation" \
      "${create_args[@]}" "${create_env[@]}" "$image" || return 1
  fi
  swarm_service_running "$VP_PUBLISHER_SERVICE" || return 1
  vp_require_service_node "$VP_PUBLISHER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false
}

vp_capture_app_snapshots() {
  local service
  local service_id
  local image
  local digest
  local publisher_state
  for service in $VP_APP_SERVICES; do
    if [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_state="$(vp_publisher_service_state)" || return 1
      if [[ "$publisher_state" == absent ]]; then
        continue
      fi
    fi
    if ! service_id="$(
      vp_registered_worker_service_current_id "$service"
    )"; then
      if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" \
        || "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
        continue
      fi
      echo "missing required VideoProcess service: $service" >&2
      return 1
    fi
    image="$(
      vp_service_values \
        "$service_id" '{{.Spec.TaskTemplate.ContainerSpec.Image}}'
    )" || return 1
    if [[ -z "$image" ]]; then
      echo "missing current image for VideoProcess service: $service" >&2
      return 1
    fi
    digest="$(
      vp_app_service_spec_digest "$service" "$service_id" "$image"
    )" || return 1
    printf '%s|%s|%s|%s\n' "$service" "$service_id" "$image" "$digest"
  done
}

vp_app_service_spec_digest() {
  local service="$1"
  local service_id="$2"
  local expected_image="$3"
  [[ "$service_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
  local spec_json
  spec_json="$(
    docker service inspect "$service_id" --format '{{json .Spec}}' 2>/dev/null
  )" || return 1
  local digest
  digest="$(
    printf '%s\n' "$spec_json" | python3 -I -c '
import hashlib
import json
import sys

service, expected_image = sys.argv[1:]
try:
    spec = json.load(sys.stdin)
    if (
        not isinstance(spec, dict)
        or spec.get("Name") != service
        or spec.get("TaskTemplate", {})
        .get("ContainerSpec", {})
        .get("Image")
        != expected_image
    ):
        raise ValueError
    canonical = json.dumps(
        spec,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    print(hashlib.sha256(canonical).hexdigest())
except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
' "$service" "$expected_image"
  )" || return 1
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

vp_app_service_durable_identity() {
  local service="$1"
  local expected_image="$2"
  local identity
  identity="$(
    docker service inspect "$service" \
      --format '{{.ID}}|{{.Spec.Name}}' 2>/dev/null
  )" || return 1
  [[ -n "$identity" && "$identity" != *$'\n'* ]] || return 1
  local service_id
  local service_name
  local extra
  IFS='|' read -r service_id service_name extra <<<"$identity"
  [[ -z "$extra" \
    && "$service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$service_name" == "$service" ]] || return 1
  local digest
  digest="$(
    vp_app_service_spec_digest "$service" "$service_id" "$expected_image"
  )" || return 1
  printf '%s|%s\n' "$service_id" "$digest"
}

vp_worker_admission_control_selection_json() {
  local manifest="$1"
  [[ "$manifest" = /* && -e "$manifest" && ! -L "$manifest" ]] \
    || return 1
  vp_worker_control_require_v2_manifest "$manifest" || return 1
  local manifest_sha256
  manifest_sha256="$(shasum -a 256 "$manifest" | awk '{print $1}')" \
    || return 1
  [[ "$manifest_sha256" =~ ^[0-9a-f]{64}$ ]] || return 1
  local references
  references="$(vp_worker_control_manifest_secret_refs)" || return 1
  printf '%s\n' "$references" | python3 -I -c '
import json
import re
import sys

generation, image, digest = sys.argv[1:]
references = []
try:
    for raw in sys.stdin:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        name, docker_id, purpose = raw.split("|")
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", name)
            is None
            or re.fullmatch(r"[a-z0-9]{20,64}", docker_id) is None
            or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", purpose)
            is None
        ):
            raise ValueError
        references.append({
            "name": name,
            "docker_secret_id": docker_id,
            "service": "vp-worker-control",
            "generation": generation,
            "purpose": purpose,
        })
    if len(references) != 7:
        raise ValueError
    payload = {
        "generation": generation,
        "image": image,
        "manifest_sha256": digest,
        "secrets": references,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
except (TypeError, ValueError):
    raise SystemExit(1)
' \
    "$VP_WORKER_CONTROL_MANIFEST_GENERATION" \
    "$VP_WORKER_CONTROL_MANIFEST_IMAGE" \
    "$manifest_sha256"
}

vp_worker_admission_baseline_control_json() {
  local root="$1"
  local current="$root/control-current.conf"
  if [[ ! -e "$current" ]]; then
    printf 'null\n'
    return 0
  fi
  vp_worker_admission_control_selection_json "$current"
}

vp_worker_admission_record_control_selection() {
  local direction="$1"
  local manifest="$2"
  local payload
  payload="$(vp_worker_admission_control_selection_json "$manifest")" \
    || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-control-selection \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$direction" <<<"$payload" >/dev/null
}

vp_worker_redis_marker_render_cron() {
  local prior_cron="$1"
  local output="$2"
  local control_root="$3"
  local sync_root="${ROOT:-}"
  local source="$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE"
  [[ -n "$sync_root" && "$sync_root" = /* ]] || return 1
  local target="$sync_root/bin/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  local log_dir="$sync_root/logs"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local readiness_cron="* * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target readiness >> $log_dir/worker-redis-marker-readiness.log 2>&1"
  local janitor_cron="*/5 * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target janitor >> $log_dir/worker-redis-marker-janitor.log 2>&1"
  awk -v begin="$cron_begin" -v end="$cron_end" \
    -v target="$target" -v source="$source" '
    BEGIN { managed=0; invalid=0 }
    $0 == begin {
      if (managed) {
        invalid=1
        exit
      }
      managed=1
      next
    }
    $0 == end {
      if (!managed) {
        invalid=1
        exit
      }
      managed=0
      next
    }
    managed { next }
    $1 !~ /^#/ && ($6 == target || $6 == source) { next }
    $1 !~ /^#/ && NF >= 9 && ($9 == target || $9 == source) { next }
    { print }
    END { if (managed || invalid) exit 1 }
  ' "$prior_cron" >"$output" \
    && printf '%s\n%s\n%s\n%s\n' \
      "$cron_begin" "$readiness_cron" "$janitor_cron" "$cron_end" \
      >>"$output"
}

vp_worker_admission_marker_selection_json() {
  local selection_mode="${1:-active}"
  [[ "$selection_mode" == active || "$selection_mode" == expected ]] \
    || return 1
  local generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
  local image="$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE"
  local runtime_generation="$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION"
  [[ "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$runtime_generation" =~ ^[0-9a-f]{40}$ ]] || return 1
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local config="$control_root/control.conf"
  if [[ "$selection_mode" == active \
    && (! -f "$config" || -L "$config" \
      || "$(vp_worker_redis_marker_file_mode "$config")" != 600) ]]; then
    return 1
  fi
  local config_sha256
  config_sha256="$(python3 -I - \
    "$config" "$selection_mode" "$generation" "$image" \
    "$VP_PIPELINE_NETWORK" "$VP_PIPELINE_NETWORK_ID" \
    "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" <<'PY'
import hashlib
import pathlib
import sys

(
    raw_path,
    selection_mode,
    generation,
    image,
    network,
    network_id,
    readiness_redis,
    janitor_redis,
) = sys.argv[1:]
path = pathlib.Path(raw_path)
expected = (
    f"GENERATION={generation}\n"
    f"IMAGE={image}\n"
    f"NETWORK={network}\n"
    f"NETWORK_ID={network_id}\n"
    f"READINESS_DATABASE_SECRET=vp-wrm-readiness-db-{generation}\n"
    f"READINESS_REDIS_SECRET={readiness_redis}\n"
    f"JANITOR_DATABASE_SECRET=vp-wrm-janitor-db-{generation}\n"
    f"JANITOR_REDIS_SECRET={janitor_redis}\n"
).encode("ascii")
if selection_mode not in {"active", "expected"}:
    raise SystemExit(1)
payload = path.read_bytes() if selection_mode == "active" else expected
if selection_mode == "active" and payload != expected:
    raise SystemExit(1)
print(hashlib.sha256(expected).hexdigest())
PY
  )" || return 1

  local transaction
  transaction="$(mktemp -d \
    "${TMPDIR:-/tmp}/vp-marker-selection-cron.XXXXXX")" \
    || return 1
  local cron="$transaction/current"
  local selected_cron="$cron"
  local status=1
  local launcher="$ROOT/bin/worker-redis-marker-control.sh"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  local log_dir="$ROOT/logs"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local readiness_cron="* * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $launcher readiness >> $log_dir/worker-redis-marker-readiness.log 2>&1"
  local janitor_cron="*/5 * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $launcher janitor >> $log_dir/worker-redis-marker-janitor.log 2>&1"
  if vp_worker_redis_marker_read_cron \
      "$cron" "$transaction/read-error"; then
    if [[ "$selection_mode" == expected ]]; then
      selected_cron="$transaction/expected"
      if ! vp_worker_redis_marker_render_cron \
        "$cron" "$selected_cron" "$control_root"; then
        selected_cron=""
      fi
    elif ! awk -v begin="$cron_begin" -v end="$cron_end" \
      -v readiness="$readiness_cron" -v janitor="$janitor_cron" '
      BEGIN { inside=0; begins=0; ends=0; lines=0; invalid=0 }
      $0 == begin {
        if (inside || begins) { invalid=1; exit }
        inside=1
        begins++
        next
      }
      $0 == end {
        if (!inside || ends || lines != 2) { invalid=1; exit }
        inside=0
        ends++
        next
      }
      inside {
        lines++
        if ((lines == 1 && $0 != readiness) \
          || (lines == 2 && $0 != janitor) || lines > 2) {
          invalid=1
          exit
        }
      }
      END { exit invalid || inside || begins != 1 || ends != 1 }
    ' "$cron"; then
      selected_cron=""
    fi
    if [[ -n "$selected_cron" ]]; then
    local cron_sha256
    cron_sha256="$(shasum -a 256 "$selected_cron" | awk '{print $1}')" \
      || status=1
    local references="$({
      printf '%s|%s|%s|%s|%s\n' \
        "$(vp_worker_redis_marker_database_secret_name readiness "$generation")" \
        "$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID" \
        worker-redis-marker-control "$generation" readiness-database
      printf '%s|%s|%s|%s|%s\n' \
        "$(vp_worker_redis_marker_database_secret_name janitor "$generation")" \
        "$VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID" \
        worker-redis-marker-control "$generation" janitor-database
      printf '%s|%s|%s|%s|%s\n' \
        "$(vp_worker_redis_marker_database_secret_name repair "$generation")" \
        "$VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID" \
        worker-redis-marker-control "$generation" repair-database
      printf '%s|%s|%s|%s|%s\n' \
        "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
        "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID" \
        vp-worker-redis-runtime "$runtime_generation" readiness-redis
      printf '%s|%s|%s|%s|%s\n' \
        "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" \
        "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID" \
        vp-worker-redis-runtime "$runtime_generation" janitor-redis
    })" || status=1
    if [[ "$config_sha256" =~ ^[0-9a-f]{64}$ \
      && "$cron_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      printf '%s\n' "$references" | python3 -I -c '
import json
import re
import sys

generation, image, config_sha256, cron_sha256 = sys.argv[1:]
expected_purposes = {
    "readiness-database",
    "janitor-database",
    "repair-database",
    "readiness-redis",
    "janitor-redis",
}
references = []
names = set()
docker_ids = set()
purposes = set()
try:
    for raw in sys.stdin:
        name, docker_id, service, reference_generation, purpose = (
            raw.rstrip("\n").split("|")
        )
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", name) is None
            or re.fullmatch(r"[a-z0-9]{20,64}", docker_id) is None
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", service) is None
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", reference_generation) is None
            or purpose not in expected_purposes
            or name in names
            or docker_id in docker_ids
            or purpose in purposes
        ):
            raise ValueError
        names.add(name)
        docker_ids.add(docker_id)
        purposes.add(purpose)
        references.append({
            "name": name,
            "docker_secret_id": docker_id,
            "service": service,
            "generation": reference_generation,
            "purpose": purpose,
        })
    if purposes != expected_purposes:
        raise ValueError
    print(json.dumps({
        "generation": generation,
        "image": image,
        "config_sha256": config_sha256,
        "cron_sha256": cron_sha256,
        "secrets": references,
    }, sort_keys=True, separators=(",", ":")))
except (TypeError, ValueError):
    raise SystemExit(1)
' "$generation" "$image" "$config_sha256" "$cron_sha256" \
        && status=0
    fi
    fi
  fi
  rm -rf "$transaction"
  return "$status"
}

vp_worker_admission_record_marker_selection() {
  local direction="$1"
  local selection_mode="${2:-active}"
  local payload
  payload="$(
    vp_worker_admission_marker_selection_json "$selection_mode"
  )" || return 1
  vp_worker_admission_load_replay_plan || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    record-marker-selection \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    "$direction" <<<"$payload" >/dev/null || return 1
  local fields
  fields="$(printf '%s\n' "$payload" | python3 -I -c '
import json
import sys

value = json.load(sys.stdin)
references = {item["purpose"]: item for item in value["secrets"]}
print("|".join((
    value["generation"], value["image"],
    value["config_sha256"], value["cron_sha256"],
    references["readiness-database"]["docker_secret_id"],
    references["janitor-database"]["docker_secret_id"],
    references["repair-database"]["docker_secret_id"],
)))
')" || return 1
  local selection_generation=""
  local selection_image=""
  local selection_config_sha256=""
  local selection_cron_sha256=""
  local selection_readiness_database_id=""
  local selection_janitor_database_id=""
  local selection_repair_database_id=""
  local extra
  IFS='|' read -r \
    selection_generation selection_image \
    selection_config_sha256 selection_cron_sha256 \
    selection_readiness_database_id selection_janitor_database_id \
    selection_repair_database_id extra <<<"$fields"
  [[ -z "$extra" ]] || return 1
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$selection_generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$selection_image"
  VP_WORKER_REDIS_MARKER_CANDIDATE_CONFIG_SHA256="$selection_config_sha256"
  VP_WORKER_REDIS_MARKER_CANDIDATE_CRON_SHA256="$selection_cron_sha256"
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID="$selection_readiness_database_id"
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID="$selection_janitor_database_id"
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID="$selection_repair_database_id"
  if [[ "$direction" == forward ]]; then
    IFS='|' read -r \
      VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION \
      VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE \
      VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256 \
      VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256 \
      VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID \
      VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID \
      VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID \
      extra <<<"$fields"
    [[ -z "$extra" ]] || return 1
  fi
}

vp_worker_admission_baseline_payload() {
  local snapshots="$1"
  local root
  root="$(vp_worker_admission_root)" || return 1
  local control_json
  control_json="$(vp_worker_admission_baseline_control_json "$root")" \
    || return 1
  local kind=legacy_no_control
  [[ "$control_json" == null ]] || kind=managed
  local records=""
  local service
  for service in $VP_APP_SERVICES; do
    local snapshot
    snapshot="$(
      printf '%s\n' "$snapshots" | awk -F'|' -v expected="$service" '
        NF && NF != 4 { invalid=1 }
        $1 == expected { count++; snapshot=$2 "|" $3 "|" $4 }
        END {
          if (invalid || count > 1) exit 1
          if (count == 1 && snapshot != "") print snapshot
        }
      '
    )" || return 1
    if [[ -z "$snapshot" ]]; then
      records+="${records:+$'\n'}$service|false|-|-|-"
      continue
    fi
    local service_id
    local image
    local digest
    local extra
    IFS='|' read -r service_id image digest extra <<<"$snapshot"
    [[ -z "$extra" \
      && "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
      && "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
    records+="${records:+$'\n'}$service|true|$service_id|$image|$digest"
  done
  printf '%s\n' "$records" | python3 -I -c '
import json
import sys

kind, raw_control = sys.argv[1:]
try:
    control = json.loads(raw_control)
    services = []
    for raw in sys.stdin:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        name, existed, docker_id, image, digest = raw.split("|")
        present = existed == "true"
        if existed not in {"true", "false"}:
            raise ValueError
        services.append({
            "name": name,
            "existed": present,
            "docker_service_id": docker_id if present else None,
            "image": image if present else None,
            "spec_digest": digest if present else None,
        })
    payload = {"kind": kind, "control": control, "services": services}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
except (TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
' "$kind" "$control_json"
}

vp_worker_admission_capture_baseline() {
  local snapshots="$1"
  local payload
  payload="$(vp_worker_admission_baseline_payload "$snapshots")" || return 1
  vp_worker_admission_load_replay_plan || return 1
  [[ "$VP_WORKER_ADMISSION_REPLAY_PHASE" == PREPARING ]] || return 1
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" capture-baseline \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    <<<"$payload" >/dev/null
}

vp_worker_admission_failed_forward_control_json() {
  if [[ "$VP_WORKER_CONTROL_PREPARED" != true ]]; then
    printf 'null\n'
    return 0
  fi
  [[ "$VP_WORKER_CONTROL_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  local config="$root/staging-object-janitor.conf"
  if [[ ! -f "$config" || -L "$config" \
    || "$(vp_worker_redis_marker_file_mode "$config")" != 600 ]]; then
    return 1
  fi
  local cron
  cron="$(mktemp "${TMPDIR:-/tmp}/vp-failed-control-cron.XXXXXX")" \
    || return 1
  local status=1
  local target="$ROOT/bin/vp-staging-object-janitor-run.sh"
  local log_file="$ROOT/logs/vp-staging-object-janitor.log"
  local cron_begin="# BEGIN VIDEOPROCESS STAGING JANITOR"
  local cron_end="# END VIDEOPROCESS STAGING JANITOR"
  local cron_command="*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE=$config $target >> $log_file 2>&1"
  if LC_ALL=C crontab -l >"$cron" \
    && awk -v begin="$cron_begin" -v end="$cron_end" \
      -v command="$cron_command" '
      BEGIN { inside=0; begins=0; ends=0; commands=0; invalid=0 }
      $0 == begin {
        if (inside || begins) { invalid=1; exit }
        inside=1
        begins++
        next
      }
      $0 == end {
        if (!inside || ends) { invalid=1; exit }
        inside=0
        ends++
        next
      }
      inside {
        if ($0 != command || commands) { invalid=1; exit }
        commands++
      }
      END {
        exit invalid || inside || begins != 1 || ends != 1 || commands != 1
      }
    ' "$cron"; then
    local config_sha256
    local cron_sha256
    config_sha256="$(shasum -a 256 "$config" | awk '{print $1}')" \
      || status=1
    cron_sha256="$(shasum -a 256 "$cron" | awk '{print $1}')" \
      || status=1
    if [[ "$config_sha256" =~ ^[0-9a-f]{64}$ \
      && "$cron_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      python3 -I -c '
import json
import sys

generation, image, config_sha256, cron_sha256 = sys.argv[1:]
print(json.dumps({
    "generation": generation,
    "image": image,
    "config_sha256": config_sha256,
    "cron_sha256": cron_sha256,
}, sort_keys=True, separators=(",", ":")))
' \
        "$VP_WORKER_CONTROL_GENERATION" \
        "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
        "$config_sha256" "$cron_sha256" \
        && status=0
    fi
  fi
  rm -f "$cron"
  return "$status"
}

vp_worker_admission_failed_forward_payload() {
  local attempted_services="$1"
  local control_json
  control_json="$(vp_worker_admission_failed_forward_control_json)" \
    || return 1
  local records=""
  local service
  for service in $attempted_services; do
    local image
    if ! image="$(
      vp_service_values "$service" \
        '{{.Spec.TaskTemplate.ContainerSpec.Image}}' 2>/dev/null
    )"; then
      return 1
    fi
    [[ -n "$image" ]] || return 1
    local identity
    identity="$(vp_app_service_durable_identity "$service" "$image")" \
      || return 1
    local service_id
    local digest
    local extra
    IFS='|' read -r service_id digest extra <<<"$identity"
    [[ -z "$extra" ]] || return 1
    records+="${records:+$'\n'}$service|$service_id|$image|$digest"
  done
  printf '%s\n' "$records" | python3 -I -c '
import json
import sys

try:
    control = json.loads(sys.argv[1])
    services = []
    for raw in sys.stdin:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        name, docker_id, image, digest = raw.split("|")
        services.append({
            "name": name,
            "existed": True,
            "docker_service_id": docker_id,
            "image": image,
            "spec_digest": digest,
        })
    payload = {"control": control, "services": services}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
except (TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
' "$control_json"
}

vp_worker_admission_capture_failed_forward() {
  local attempted_services="$1"
  local payload
  payload="$(
    vp_worker_admission_failed_forward_payload "$attempted_services"
  )" || return 1
  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    PREPARING|FORWARD_APPLYING) ;;
    *) return 1 ;;
  esac
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    capture-failed-forward \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$VP_WORKER_ADMISSION_REPLAY_REVISION" \
    <<<"$payload" >/dev/null
}

vp_record_app_service_attempt() {
  local service="$1"
  case " $VP_APP_ATTEMPTED_SERVICES " in
    *" $service "*)
      return 0
      ;;
  esac
  if [[ "${UPDATE_SERVICES:-1}" -ne 0 ]]; then
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      record-app-attempt \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$service" >/dev/null || return 1
  fi
  VP_APP_ATTEMPTED_SERVICES="${VP_APP_ATTEMPTED_SERVICES:+$VP_APP_ATTEMPTED_SERVICES }$service"
}

vp_remove_app_service_attempt() {
  local service="$1"
  if [[ "${UPDATE_SERVICES:-1}" -ne 0 ]]; then
    python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
      remove-app-attempt \
      "$VP_WORKER_ADMISSION_LOCK_ROOT" \
      "$VP_WORKER_ADMISSION_LOCK_FD" \
      "$service" >/dev/null || return 1
  fi
  local attempted_service
  local remaining_services=""
  for attempted_service in $VP_APP_ATTEMPTED_SERVICES; do
    [[ "$attempted_service" == "$service" ]] && continue
    remaining_services="${remaining_services:+$remaining_services }$attempted_service"
  done
  VP_APP_ATTEMPTED_SERVICES="$remaining_services"
}

vp_worker_admission_advance_migration_state() {
  local expected_state="$1"
  local target_state="$2"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  python3 "$VP_WORKER_ADMISSION_TRANSACTION_HELPER" \
    advance-migration-state \
    "$VP_WORKER_ADMISSION_LOCK_ROOT" \
    "$VP_WORKER_ADMISSION_LOCK_FD" \
    "$expected_state" "$target_state" >/dev/null
}

vp_update_app_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"
  local update_status=0

  vp_record_app_service_attempt "$service" || return 1
  if vp_update_runtime_service "$service" "$image" "$order"; then
    return 0
  else
    update_status=$?
  fi
  if [[ "$update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
    vp_remove_app_service_attempt "$service" || return 1
  fi
  return 1
}

vp_app_service_was_attempted() {
  local service="$1"
  local attempted_services="$2"
  case " $attempted_services " in
    *" $service "*)
      return 0
      ;;
  esac
  return 1
}

vp_restore_gpu_service() {
  local image="$1"
  local expected_service_id="${2:-}"
  local service_id
  service_id="$(
    vp_registered_worker_service_current_id "$VP_PYTHON_WORKER_SERVICE"
  )" || return 1
  if [[ -n "$expected_service_id" \
    && "$service_id" != "$expected_service_id" ]]; then
    return 1
  fi
  local existing_constraints
  existing_constraints="$(
    vp_service_values "$service_id" \
      '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'
  )" || return 1
  local constraint_output
  constraint_output="$(vp_gpu_constraint_update_args "$existing_constraints")" || return 1
  local constraint
  local constraint_args=()
  while IFS= read -r constraint; do
    [[ -n "$constraint" ]] || continue
    constraint_args+=("$constraint")
  done <<<"$constraint_output"

  local update_args=(
    service update --detach=false --no-resolve-image --update-order stop-first
  )
  if [[ "${#constraint_args[@]}" -gt 0 ]]; then
    update_args+=("${constraint_args[@]}")
  fi
  update_args+=(--image "$image" "$service_id")
  docker "${update_args[@]}" >&2 || return 1
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_worker_admission_process_candidate_service_records() {
  local attempted_services="$1"
  local service
  for service in \
    vp-ffmpeg-worker-go-swarm \
    "$VP_PYTHON_WORKER_SERVICE" \
    "$VP_VISION_WORKER_SERVICE" \
    "$VP_PUBLISHER_SERVICE"; do
    vp_app_service_was_attempted "$service" "$attempted_services" || continue
    local contract
    contract="$(vp_worker_service_contract "$service")" || return 1
    local generation
    generation="$(cut -d'|' -f6 <<<"$contract")"
    [[ "$generation" =~ ^[1-9][0-9]*$ ]] || return 1
    local service_id
    if ! service_id="$(
      vp_registered_worker_service_current_id "$service"
    )"; then
      if docker service inspect "$service" >/dev/null 2>&1; then
        return 1
      fi
      continue
    fi
    printf '%s|%s|%s\n' "$service" "$generation" "$service_id"
  done
}

vp_remove_new_registered_worker() {
  local service="$1"
  local candidate_records="${2-$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS}"
  local process_candidate_records="${3:-}"
  local generation=""
  local service_id=""
  local match_count=0
  if [[ -n "$candidate_records" ]]; then
    local record_service
    local record_generation
    local record_service_id
    local extra
    while IFS='|' read -r \
      record_service record_generation record_service_id extra; do
      [[ -n "$record_service$record_generation$record_service_id$extra" ]] \
        || continue
      [[ -z "$extra" \
        && "$record_service" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
        && "$record_generation" =~ ^[1-9][0-9]*$ \
        && "$record_service_id" =~ ^[a-z0-9]{12,64}$ ]] || {
        echo "invalid durable worker removal authority record" >&2
        return 1
      }
      if [[ "$record_service" == "$service" ]]; then
        match_count=$((match_count + 1))
        generation="$record_generation"
        service_id="$record_service_id"
      fi
    done <<<"$candidate_records"
    if [[ "$match_count" -gt 1 ]]; then
      echo "duplicate durable worker removal authority: $service" >&2
      return 1
    fi
  fi
  local process_generation=""
  local process_service_id=""
  local process_match_count=0
  if [[ -n "$process_candidate_records" ]]; then
    local process_service
    local record_process_generation
    local record_process_service_id
    local extra
    while IFS='|' read -r \
      process_service record_process_generation \
      record_process_service_id extra; do
      [[ -n "$process_service$record_process_generation$record_process_service_id$extra" ]] \
        || continue
      [[ -z "$extra" \
        && "$process_service" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
        && "$record_process_generation" =~ ^[1-9][0-9]*$ \
        && "$record_process_service_id" =~ ^[a-z0-9]{12,64}$ ]] || {
        echo "invalid process-local worker removal authority record" >&2
        return 1
      }
      if [[ "$process_service" == "$service" ]]; then
        process_match_count=$((process_match_count + 1))
        process_generation="$record_process_generation"
        process_service_id="$record_process_service_id"
      fi
    done <<<"$process_candidate_records"
    if [[ "$process_match_count" -gt 1 ]]; then
      echo "duplicate process-local worker removal authority: $service" >&2
      return 1
    fi
  fi
  if [[ "$match_count" -eq 1 ]]; then
    if [[ "$process_match_count" -eq 1 \
      && ( "$process_generation" != "$generation" \
        || "$process_service_id" != "$service_id" ) ]]; then
      echo "conflicting worker removal authority: $service" >&2
      return 1
    fi
  elif [[ "$process_match_count" -eq 1 ]]; then
    generation="$process_generation"
    service_id="$process_service_id"
  else
    echo "missing worker removal authority: $service" >&2
    return 1
  fi
  local current_service_id
  current_service_id="$(vp_registered_worker_service_current_id "$service")" \
    || return 1
  [[ "$current_service_id" == "$service_id" ]] || return 1
  vp_require_worker_redis_marker_status || return 1
  local inspected_service_id
  inspected_service_id="$(
    vp_registered_worker_service_identity \
      "$service_id" "$service" "$generation"
  )" || return 1
  [[ "$inspected_service_id" == "$service_id" ]] || return 1
  docker service rm "$service_id" >/dev/null || return 1
  vp_worker_admission_complete_worker_mutation \
    rm "$service" "$generation" absent || return 1
  ! docker service inspect "$service_id" >/dev/null 2>&1 \
    && ! docker service inspect "$service" >/dev/null 2>&1
}

vp_validate_app_snapshot_identities() {
  local snapshots="$1"
  local seen_services=" "
  local service
  local service_id
  local image
  local digest
  local extra
  while IFS='|' read -r service service_id image digest extra; do
    if [[ -z "$service" && -z "$service_id" && -z "$image" \
      && -z "$digest" && -z "$extra" ]]; then
      continue
    fi
    [[ -z "$extra" \
      && "$service" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$ \
      && "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
      && "$digest" =~ ^[0-9a-f]{64}$ ]] || return 1
    case " $VP_APP_SERVICES " in
      *" $service "*) ;;
      *) return 1 ;;
    esac
    case "$seen_services" in
      *" $service "*) return 1 ;;
    esac
    seen_services="$seen_services$service "
    local current_service_id
    current_service_id="$(
      vp_registered_worker_service_current_id "$service"
    )" || return 1
    if [[ "$current_service_id" != "$service_id" ]]; then
      echo "service identity changed before rollback: $service" >&2
      return 1
    fi
  done < <(printf '%s\n' "$snapshots")
}

vp_restore_app_snapshots() {
  local snapshots="$1"
  local attempted_services="${2-$VP_APP_SERVICES}"
  local worker_admission_rollback="${3:-false}"
  local process_candidate_records="${4:-}"
  case "$worker_admission_rollback" in
    true|false) ;;
    *) return 1 ;;
  esac
  vp_validate_app_snapshot_identities "$snapshots" || return 1
  local service
  local service_id
  local image
  local digest
  local extra
  local gpu_was_present=false
  local vision_was_present=false
  local publisher_was_present=false
  local status=0

  while IFS='|' read -r service service_id image digest extra; do
    [[ -n "$service" ]] || continue
    vp_app_service_was_attempted "$service" "$attempted_services" || continue
    log "restore $service -> $image with dedicated VP placement"
    local registered_worker=false
    case "$service" in
      vp-ffmpeg-worker-go-swarm|"$VP_PYTHON_WORKER_SERVICE"|"$VP_VISION_WORKER_SERVICE"|"$VP_PUBLISHER_SERVICE")
        registered_worker=true
        ;;
    esac
    if [[ "$registered_worker" == true ]]; then
      if [[ "$worker_admission_rollback" == true ]]; then
        vp_worker_admission_select_candidate "$service" || return 1
      fi
      vp_require_worker_redis_marker_status || return 1
      if [[ "$worker_admission_rollback" == true ]]; then
        vp_activate_worker_admission "$service" || return 1
        vp_require_worker_redis_marker_status || return 1
      fi
    fi

    local restored=true
    if [[ "$service" == "vp-ffmpeg-worker-go-swarm" ]]; then
      if ! vp_update_runtime_service \
        "$service" "$image" stop-first "$service_id"; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" ]]; then
      gpu_was_present=true
      if [[ "$worker_admission_rollback" == true ]]; then
        if ! vp_deploy_python_worker "$image" "$service_id"; then
          status=1
          restored=false
        fi
      elif ! vp_restore_gpu_service "$image" "$service_id"; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
      vision_was_present=true
      if ! vp_deploy_vision_worker "$image" "$service_id"; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_was_present=true
      if ! vp_deploy_publisher "$image" "$service_id"; then
        status=1
        restored=false
      fi
    elif [[ "$VP_BACKEND_MIGRATION_APPLIED" == true \
      && ( "$service" == "vp-autoflow-api-swarm" \
        || "$service" == "vp-event-outbox-relay-swarm" ) ]]; then
      log "preserve $service at the migration-compatible attempted image"
    elif ! vp_update_runtime_service \
      "$service" "$image" stop-first "$service_id"; then
      status=1
      restored=false
    fi
    if [[ "$registered_worker" == true \
      && "$worker_admission_rollback" == true \
      && "$restored" == true ]]; then
      vp_worker_admission_advance_live_worker_stage \
        "$service" "$image" prepared applied || return 1
      if ! vp_require_worker_deployment_ready "$service"; then
        status=1
      else
        vp_worker_admission_advance_live_worker_stage \
          "$service" "$image" applied verified || return 1
      fi
    fi
  done < <(printf '%s\n' "$snapshots")

  if vp_app_service_was_attempted "$VP_PYTHON_WORKER_SERVICE" "$attempted_services" \
    && [[ "$gpu_was_present" != true ]] \
    && docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_PYTHON_WORKER_SERVICE"
    if ! vp_remove_new_registered_worker \
        "$VP_PYTHON_WORKER_SERVICE" \
        "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" \
        "$process_candidate_records"; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_VISION_WORKER_SERVICE" "$attempted_services" \
    && [[ "$vision_was_present" != true ]] \
    && docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_VISION_WORKER_SERVICE"
    if ! vp_remove_new_registered_worker \
        "$VP_VISION_WORKER_SERVICE" \
        "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" \
        "$process_candidate_records"; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_PUBLISHER_SERVICE" "$attempted_services" \
    && [[ "$publisher_was_present" != true ]]; then
    local publisher_state
    publisher_state="$(vp_publisher_service_state)" || return 1
    if [[ "$publisher_state" == exists ]]; then
      log "remove newly created $VP_PUBLISHER_SERVICE"
      if ! vp_remove_new_registered_worker \
          "$VP_PUBLISHER_SERVICE" \
          "$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_SERVICE_RECORDS" \
          "$process_candidate_records"; then
        status=1
      fi
    fi
  fi
  return "$status"
}

vp_worker_admission_stale_rollback_records() {
  local root
  root="$(vp_worker_admission_root)" || return 1
  local directory
  while IFS= read -r directory; do
    [[ -n "$directory" ]] || continue
    if [[ ! -d "$directory" || -L "$directory" ]]; then
      return 1
    fi
    if [[ "${directory##*/}" \
      == "${VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE:-}" ]]; then
      continue
    fi
    local service
    for service in \
      vp-ffmpeg-worker-go-swarm \
      "$VP_PYTHON_WORKER_SERVICE" \
      "$VP_VISION_WORKER_SERVICE" \
      "$VP_PUBLISHER_SERVICE"; do
      local kind
      kind="$(vp_worker_admission_kind "$service")" || return 1
      local manifest="$directory/$kind.conf"
      [[ -e "$manifest" ]] || continue
      vp_worker_admission_require_v2_manifest \
        "$manifest" "$service" || return 1
      printf '%s|%s|%s|%s|%s|%s\n' \
        "$service" \
        "$VP_WORKER_MANIFEST_GENERATION" \
        "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
        "$VP_WORKER_MANIFEST_DATABASE_SECRET_ID" \
        "$VP_WORKER_MANIFEST_ADMISSION_SECRET" \
        "$VP_WORKER_MANIFEST_ADMISSION_SECRET_ID"
    done
  done < <(
    find "$root/candidates" -mindepth 1 -maxdepth 1 \
      -type d -name 'rollback-*' -print 2>/dev/null | LC_ALL=C sort
  )
}

vp_worker_admission_stale_rollback_namespaces() {
  local root
  root="$(vp_worker_admission_root)" || return 1
  local directory
  while IFS= read -r directory; do
    [[ -n "$directory" ]] || continue
    [[ -d "$directory" && ! -L "$directory" ]] || return 1
    local namespace="${directory##*/}"
    [[ "$namespace" =~ ^rollback-[1-9][0-9]*$ ]] || return 1
    if [[ "$namespace" \
      == "${VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE:-}" ]]; then
      continue
    fi
    printf '%s\n' "$namespace"
  done < <(
    find "$root/candidates" -mindepth 1 -maxdepth 1 \
      -type d -name 'rollback-*' -print 2>/dev/null | LC_ALL=C sort
  )
}

vp_restore_legacy_worker_service() {
  local service="$1"
  local service_id="$2"
  local image="$3"
  local spec_digest="$4"
  local generation="${VP_WORKER_LEGACY_RESTORE_GENERATION:-}"
  [[ "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
  vp_mutate_registered_worker_service \
    update "$service" "$service_id" "$generation" \
    service update --detach=false --no-resolve-image \
      --update-order stop-first --image "$image" \
      --label-add "vp.service=$service" \
      --label-add "vp.generation=$generation" \
      --label-add vp.managed-by=videoprocess-deploy \
      "$service"
}

vp_remove_legacy_candidate_worker() {
  local service="$1"
  local service_id="$2"
  local generation="$3"
  vp_mutate_registered_worker_service \
    rm "$service" "$service_id" "$generation" \
    service rm "$service_id"
}

vp_require_legacy_worker_service_ready() {
  local service="$1"
  local expected_service_id="$2"
  local current_id
  current_id="$(vp_registered_worker_service_current_id "$service")" \
    || return 1
  [[ "$current_id" == "$expected_service_id" ]] || return 1
  swarm_service_running "$expected_service_id"
}

vp_require_legacy_worker_service_absent() {
  local service="$1"
  ! docker service inspect "$service" >/dev/null 2>&1
}

vp_retire_legacy_forward_candidate() {
  local records="$1"
  local root
  root="$(vp_worker_admission_root)" || return 1
  vp_worker_admission_retire_records "$records" "$root"
}

vp_restore_legacy_worker_admission_baseline() {
  local baseline_records="$1"
  local candidate_service_records="$2"
  local candidate_secret_records="$3"
  local control_current="$4"
  [[ "$control_current" = /* && ! -e "$control_current" ]] || return 1

  local service
  local existed
  local baseline_id
  local image
  local spec_digest
  local extra
  while IFS='|' read -r \
    service existed baseline_id image spec_digest extra; do
    [[ -n "$service$existed$baseline_id$image$spec_digest$extra" ]] \
      || continue
    [[ -z "$extra" ]] || return 1
    case "$existed" in
      true)
        [[ "$baseline_id" =~ ^[a-z0-9]{12,64}$ \
          && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
          && "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
        local candidate_generation
        candidate_generation="$(
          awk -F'|' -v expected="$service" '
            $1 == expected { count++; generation=$2 }
            END { if (count != 1) exit 1; print generation }
          ' <<<"$candidate_service_records"
        )" || return 1
        VP_WORKER_LEGACY_RESTORE_GENERATION="$candidate_generation"
        vp_restore_legacy_worker_service \
          "$service" "$baseline_id" "$image" "$spec_digest" || return 1
        ;;
      false)
        [[ "$baseline_id" == - && "$image" == - \
          && "$spec_digest" == - ]] || return 1
        ;;
      *) return 1 ;;
    esac
  done <<<"$baseline_records"

  while IFS='|' read -r \
    service existed baseline_id image spec_digest extra; do
    [[ -n "$service$existed$baseline_id$image$spec_digest$extra" ]] \
      || continue
    [[ -z "$extra" ]] || return 1
    if [[ "$existed" == true ]]; then
      vp_require_legacy_worker_service_ready \
        "$service" "$baseline_id" || return 1
    fi
  done <<<"$baseline_records"

  local candidate_id
  local candidate_generation
  while IFS='|' read -r \
    service candidate_generation candidate_id extra; do
    [[ -n "$service$candidate_generation$candidate_id$extra" ]] \
      || continue
    [[ -z "$extra" \
      && "$candidate_generation" =~ ^[1-9][0-9]*$ \
      && "$candidate_id" =~ ^[a-z0-9]{12,64}$ ]] || return 1
    existed="$(
      awk -F'|' -v expected="$service" '
        $1 == expected { count++; print $2 }
        END { if (count != 1) exit 1 }
      ' <<<"$baseline_records"
    )" || return 1
    if [[ "$existed" == false ]]; then
      vp_remove_legacy_candidate_worker \
        "$service" "$candidate_id" "$candidate_generation" || return 1
      vp_require_legacy_worker_service_absent "$service" || return 1
    fi
  done <<<"$candidate_service_records"

  [[ ! -e "$control_current" ]] || return 1
  vp_retire_legacy_forward_candidate \
    "$candidate_secret_records" || return 1
  [[ ! -e "$control_current" ]]
}

vp_failed_forward_control_identity_matches() {
  local identity
  identity="$(vp_worker_admission_failed_forward_control_json)" || return 1
  printf '%s\n' "$identity" | python3 -I -c '
import json
import sys

generation, image, config_sha256, cron_sha256 = sys.argv[1:]
try:
    identity = json.load(sys.stdin)
    expected = {
        "generation": generation,
        "image": image,
        "config_sha256": config_sha256,
        "cron_sha256": cron_sha256,
    }
    if identity != expected:
        raise ValueError
except (TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
' \
    "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
    "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" \
    "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256" \
    "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256"
}

vp_reinstall_failed_forward_control() {
  [[ "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CONFIG_SHA256" \
      =~ ^[0-9a-f]{64}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_CONTROL_CRON_SHA256" \
      =~ ^[0-9a-f]{64}$ \
    && "$VP_WORKER_CONTROL_GENERATION" \
      == "$VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION" \
    && "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      == "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" ]] || return 1
  vp_install_staging_object_janitor \
    "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" || return 1
  vp_failed_forward_control_identity_matches
}

vp_failed_forward_marker_identity_matches() {
  local identity
  identity="$(vp_worker_admission_marker_selection_json)" || return 1
  printf '%s\n' "$identity" | python3 -I -c '
import json
import sys

(
    generation,
    image,
    config_sha256,
    cron_sha256,
    readiness_database_id,
    janitor_database_id,
    repair_database_id,
    runtime_generation,
    readiness_redis_name,
    readiness_redis_id,
    janitor_redis_name,
    janitor_redis_id,
) = sys.argv[1:]
try:
    identity = json.load(sys.stdin)
    if set(identity) != {
        "generation", "image", "config_sha256", "cron_sha256", "secrets"
    }:
        raise ValueError
    if (
        identity["generation"] != generation
        or identity["image"] != image
        or identity["config_sha256"] != config_sha256
        or identity["cron_sha256"] != cron_sha256
        or not isinstance(identity["secrets"], list)
    ):
        raise ValueError
    actual = {
        reference["purpose"]: (
            reference["name"],
            reference["docker_secret_id"],
            reference["service"],
            reference["generation"],
        )
        for reference in identity["secrets"]
    }
    expected = {
        "readiness-database": (
            f"vp-wrm-readiness-db-{generation}",
            readiness_database_id,
            "worker-redis-marker-control",
            generation,
        ),
        "janitor-database": (
            f"vp-wrm-janitor-db-{generation}",
            janitor_database_id,
            "worker-redis-marker-control",
            generation,
        ),
        "repair-database": (
            f"vp-wrm-repair-db-{generation}",
            repair_database_id,
            "worker-redis-marker-control",
            generation,
        ),
        "readiness-redis": (
            readiness_redis_name,
            readiness_redis_id,
            "vp-worker-redis-runtime",
            runtime_generation,
        ),
        "janitor-redis": (
            janitor_redis_name,
            janitor_redis_id,
            "vp-worker-redis-runtime",
            runtime_generation,
        ),
    }
    if len(identity["secrets"]) != len(expected) or actual != expected:
        raise ValueError
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
' \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID" \
    "$VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION" \
    "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID"
}

vp_restore_failed_forward_marker_controls() {
  [[ "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_MARKER_CONFIG_SHA256" \
      =~ ^[0-9a-f]{64}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_MARKER_CRON_SHA256" \
      =~ ^[0-9a-f]{64}$ ]] || return 1
  local active_generation="$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"
  local active_image="$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE"
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  control_root="$(
    vp_python_worker_prepare_controlled_directory "$control_root"
  )" || return 1
  vp_worker_redis_marker_require_secret_manifest \
    "$control_root" "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
    || return 1
  [[ "$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID" \
      == "$VP_WORKER_ROLLBACK_FAILED_MARKER_READINESS_DATABASE_SECRET_ID" \
    && "$VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID" \
      == "$VP_WORKER_ROLLBACK_FAILED_MARKER_JANITOR_DATABASE_SECRET_ID" \
    && "$VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID" \
      == "$VP_WORKER_ROLLBACK_FAILED_MARKER_REPAIR_DATABASE_SECRET_ID" ]] \
    || return 1
  if [[ -n "$active_generation" && -n "$active_image" ]]; then
    vp_worker_redis_marker_remove_generation_jobs \
      "$active_image" "$active_generation" || return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE"
  if ! vp_install_worker_redis_marker_control \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
      "$control_root" \
    || ! vp_run_worker_redis_marker_readiness "$control_root" \
    || ! vp_failed_forward_marker_identity_matches; then
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=true
}

vp_restore_failed_forward_worker_service() {
  local service="$1"
  local generation="$2"
  local expected_service_id="$3"
  local expected_spec_digest="$4"
  [[ "$generation" =~ ^[1-9][0-9]*$ \
    && "$expected_service_id" =~ ^[a-z0-9]{12,64}$ \
    && "$expected_spec_digest" =~ ^[0-9a-f]{64}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE" \
      =~ ^[a-z0-9][a-z0-9-]{0,127}$ ]] || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  local kind
  kind="$(vp_worker_admission_kind "$service")" || return 1
  local candidate="$root/candidates/$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE/$kind.conf"
  vp_worker_admission_require_v2_manifest \
    "$candidate" "$service" || return 1
  [[ "$VP_WORKER_MANIFEST_SERVICE" == "$service" \
    && "$VP_WORKER_MANIFEST_GENERATION" == "$generation" \
    && "$VP_WORKER_MANIFEST_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ ]] || return 1
  local current_service_id
  current_service_id="$(
    vp_registered_worker_service_current_id "$service"
  )" || return 1
  [[ "$current_service_id" == "$expected_service_id" ]] || return 1
  vp_worker_admission_set_candidate \
    "$service" "$generation" \
    "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
    "$VP_WORKER_MANIFEST_ADMISSION_SECRET" || return 1
  vp_activate_worker_admission "$service" || return 1
  case "$service" in
    vp-ffmpeg-worker-go-swarm)
      vp_update_runtime_service \
        "$service" "$VP_WORKER_MANIFEST_IMAGE" stop-first || return 1
      ;;
    "$VP_PYTHON_WORKER_SERVICE")
      vp_deploy_python_worker "$VP_WORKER_MANIFEST_IMAGE" || return 1
      ;;
    "$VP_VISION_WORKER_SERVICE")
      vp_deploy_vision_worker "$VP_WORKER_MANIFEST_IMAGE" || return 1
      ;;
    "$VP_PUBLISHER_SERVICE")
      vp_deploy_publisher "$VP_WORKER_MANIFEST_IMAGE" || return 1
      ;;
    *) return 1 ;;
  esac
  current_service_id="$(
    vp_registered_worker_service_current_id "$service"
  )" || return 1
  [[ "$current_service_id" == "$expected_service_id" ]] || return 1
  local durable_identity
  durable_identity="$(
    vp_app_service_durable_identity "$service" "$VP_WORKER_MANIFEST_IMAGE"
  )" || return 1
  [[ "$durable_identity" \
    == "$expected_service_id|$expected_spec_digest" ]] || return 1
  vp_require_worker_deployment_ready "$service"
}

vp_verify_failed_forward_candidate() {
  vp_failed_forward_control_identity_matches || return 1
  vp_failed_forward_marker_identity_matches || return 1
  vp_run_staging_object_janitor_once || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  vp_require_staging_object_janitor_control \
    "$root" "$VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE" || return 1
  vp_require_worker_redis_marker_status || return 1
  local service
  local generation
  local expected_service_id
  local expected_spec_digest
  local extra
  while IFS='|' read -r \
    service generation expected_service_id expected_spec_digest extra; do
    [[ -n "$service$generation$expected_service_id$expected_spec_digest$extra" ]] \
      || continue
    [[ -z "$extra" \
      && "$generation" =~ ^[1-9][0-9]*$ \
      && "$expected_service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$expected_spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
    local current_service_id
    current_service_id="$(
      vp_registered_worker_service_current_id "$service"
    )" || return 1
    [[ "$current_service_id" == "$expected_service_id" ]] || return 1
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local candidate="$root/candidates/$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE/$kind.conf"
    vp_worker_admission_require_v2_manifest \
      "$candidate" "$service" || return 1
    [[ "$VP_WORKER_MANIFEST_GENERATION" == "$generation" ]] || return 1
    local durable_identity
    durable_identity="$(
      vp_app_service_durable_identity "$service" "$VP_WORKER_MANIFEST_IMAGE"
    )" || return 1
    [[ "$durable_identity" \
      == "$expected_service_id|$expected_spec_digest" ]] || return 1
    vp_require_worker_deployment_ready "$service" || return 1
    vp_require_worker_service_descriptor "$service" || return 1
  done <<<"$VP_WORKER_ADMISSION_RECOVERY_CANDIDATE_IDENTITIES"
}

vp_restore_failed_forward_candidate() {
  local candidate_records="$1"
  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING) ;;
    *) return 1 ;;
  esac
  vp_reinstall_failed_forward_control || return 1
  if [[ "$VP_WORKER_ADMISSION_REPLAY_PHASE" \
    == CANDIDATE_RESTORE_REQUIRED ]]; then
    vp_worker_admission_transition_to CANDIDATE_RESTORING || return 1
  fi
  vp_restore_failed_forward_marker_controls || return 1
  local service
  local generation
  local service_id
  local spec_digest
  local extra
  while IFS='|' read -r \
    service generation service_id spec_digest extra; do
    [[ -n "$service$generation$service_id$spec_digest$extra" ]] \
      || continue
    [[ -z "$extra" \
      && "$generation" =~ ^[1-9][0-9]*$ \
      && "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$spec_digest" =~ ^[0-9a-f]{64}$ ]] || return 1
    vp_restore_failed_forward_worker_service \
      "$service" "$generation" "$service_id" "$spec_digest" \
      || return 1
  done <<<"$candidate_records"
  vp_verify_failed_forward_candidate || return 1
  vp_worker_admission_transition_to CANDIDATE_RESTORED
}

vp_worker_admission_require_candidate_restore() {
  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    ROLLBACK_APPLYING)
      vp_worker_admission_transition_to CANDIDATE_RESTORE_REQUIRED
      ;;
    CANDIDATE_RESTORE_REQUIRED|CANDIDATE_RESTORING|CANDIDATE_RESTORED)
      return 0
      ;;
    *) return 1 ;;
  esac
}

vp_restore_worker_admission_transaction() {
  local snapshots="$1"
  local attempted_services="$2"
  local failed_candidate_records="$3"
  local preserve_incomplete="${4:-false}"
  local process_candidate_records="${5:-}"
  [[ "$preserve_incomplete" == true || "$preserve_incomplete" == false ]] \
    || return 1
  local root
  root="$(vp_worker_admission_root)" || return 1
  if [[ -z "$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE" ]]; then
    VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
  fi
  local failed_candidate_namespace="$VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE"
  VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=false
  local failed_control_generation="$VP_WORKER_CONTROL_GENERATION"
  local failed_control_image="$VP_WORKER_ADMISSION_CONTROL_IMAGE"

  if [[ "$VP_WORKER_ADMISSION_PREPARED" != true ]]; then
    vp_restore_app_snapshots \
      "$snapshots" "$attempted_services" false \
      "$process_candidate_records" \
      || return 1
    if [[ "$VP_WORKER_ADMISSION_TRANSACTION_PREPARING" == true ]]; then
      if [[ -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
        || -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" ]]; then
        [[ -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" \
          && -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
          && -n "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" ]] \
          || return 1
        vp_worker_redis_marker_restore_managed_state || return 1
        vp_worker_redis_marker_remove_generation_jobs \
          "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
          "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" || return 1
      elif [[ -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" ]]; then
        vp_worker_redis_marker_restore_managed_state || return 1
      fi
      vp_worker_admission_abort_transaction \
        preparing_failed || return 1
      vp_worker_redis_marker_discard_managed_state || return 1
      VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
      VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
      return 0
    fi
    if [[ "$preserve_incomplete" == true ]]; then
      VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
      return
    fi
    vp_worker_admission_retire_records \
      "$failed_candidate_records" "$root" || return 1
    if [[ -n "$failed_candidate_namespace" ]]; then
      vp_worker_admission_discard_namespace \
        "$root" "$failed_candidate_namespace" || return 1
    fi
    vp_worker_control_cleanup_candidate "$root" || return 1
    VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
    VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
    return
  fi

  vp_worker_admission_load_replay_plan || return 1
  case "$VP_WORKER_ADMISSION_REPLAY_PHASE" in
    PREPARING|FORWARD_APPLYING)
      vp_worker_admission_transition_to ROLLBACK_PREPARING || return 1
      ;;
    ROLLBACK_PREPARING|ROLLBACK_APPLYING)
      ;;
    *) return 1 ;;
  esac

  local stale_rollback_records
  stale_rollback_records="$(
    vp_worker_admission_stale_rollback_records
  )" || return 1
  local stale_rollback_namespaces
  stale_rollback_namespaces="$(
    vp_worker_admission_stale_rollback_namespaces
  )" || return 1
  vp_prepare_worker_admission_rollback \
    "$snapshots" "$attempted_services" || return 1
  vp_worker_admission_load_replay_plan || return 1
  if [[ "$VP_WORKER_ADMISSION_REPLAY_PHASE" == ROLLBACK_PREPARING ]]; then
    vp_worker_admission_transition_to ROLLBACK_APPLYING || return 1
  elif [[ "$VP_WORKER_ADMISSION_REPLAY_PHASE" != ROLLBACK_APPLYING ]]; then
    return 1
  fi
  if ! vp_install_staging_object_janitor \
      "$VP_WORKER_ADMISSION_CONTROL_IMAGE"; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  if ! vp_worker_admission_clear_janitor_service; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  if ! vp_run_staging_object_janitor_once; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  if ! vp_worker_admission_record_janitor_service; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  if ! vp_restore_app_snapshots \
      "$snapshots" "$attempted_services" true \
      "$process_candidate_records"; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  if ! vp_worker_admission_transition_to ROLLBACK_VERIFIED; then
    vp_worker_admission_require_candidate_restore || return 1
    return 1
  fi
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$failed_control_generation"
  VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$failed_control_image"
  VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_WORKERS || return 1
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_MARKER || return 1
  vp_worker_admission_promote_phase \
    PROMOTE_ROLLBACK_CONTROL || return 1
  local retirement_records=""
  if [[ "$preserve_incomplete" != true ]]; then
    retirement_records="$failed_candidate_records"
  fi
  if [[ -n "$stale_rollback_records" ]]; then
    retirement_records="${retirement_records:+$retirement_records$'\n'}$stale_rollback_records"
  fi
  vp_worker_admission_retire_records \
    "$retirement_records" "$root" || return 1
  if [[ "$preserve_incomplete" != true \
    && -n "$failed_candidate_namespace" ]]; then
    vp_worker_admission_discard_namespace \
      "$root" "$failed_candidate_namespace" || return 1
  fi
  local stale_namespace
  while IFS= read -r stale_namespace; do
    [[ -n "$stale_namespace" ]] || continue
    vp_worker_admission_discard_namespace \
      "$root" "$stale_namespace" || return 1
  done <<<"$stale_rollback_namespaces"
  if [[ "$preserve_incomplete" != true ]]; then
    VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
  fi
  vp_worker_admission_finish_transaction rolled_back
}

vp_install_soak_watch() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "ChannelOps soak watcher install skipped"
    return 0
  fi

  local sync_root="${ROOT:-}"
  if [[ -z "$sync_root" ]]; then
    echo "ROOT must be set by deploy-github-sync.sh" >&2
    return 1
  fi

  local source="${VP_SOAK_WATCH_SOURCE:-$REPO_ROOT/videoprocess/deploy/swarm/channelops-soak-watch.sh}"
  if [[ ! -r "$source" ]]; then
    echo "ChannelOps soak watcher source is not readable: $source" >&2
    return 1
  fi
  if ! bash -n "$source"; then
    echo "ChannelOps soak watcher source has invalid syntax: $source" >&2
    return 1
  fi

  (
    local target="$sync_root/bin/channelops-soak-watch.sh"
    local log_file="$sync_root/logs/channelops-soak-watch.log"
    local cron_begin="# BEGIN VIDEOPROCESS SOAK WATCH"
    local cron_end="# END VIDEOPROCESS SOAK WATCH"
    local cron_command="*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=$sync_root $target >> $log_file 2>&1"
    local temp_dir=""
    local watch_txn_dir=""
    local current_cron=""
    local next_cron=""
    local verify_cron=""
    local prior_cron_absent=false
    local watcher_had_prior=false
    local watcher_replaced=false
    local cron_may_have_changed=false
    local transaction_status=1
    local failure_reason=""
    local vp_soak_read_absent=false

    vp_soak_watch_is_no_crontab_error() {
      awk 'NR == 1 && /^no crontab for .+$/ { matched=1; next }
        { matched=0; exit }
        END { exit matched ? 0 : 1 }' "$1"
    }

    vp_soak_watch_read_cron() {
      local output="$1"
      local error_output="$2"
      vp_soak_read_absent=false
      if LC_ALL=C crontab -l >"$output" 2>"$error_output"; then
        return 0
      fi
      if vp_soak_watch_is_no_crontab_error "$error_output"; then
        : >"$output"
        vp_soak_read_absent=true
        return 0
      fi
      cat "$error_output" >&2
      return 1
    }

    vp_soak_watch_cleanup() {
      local cleanup_status=0
      if [[ -n "$watch_txn_dir" ]]; then
        if rm -rf "$watch_txn_dir"; then
          watch_txn_dir=""
        else
          cleanup_status=1
        fi
      fi
      if [[ -n "$temp_dir" ]]; then
        if rm -rf "$temp_dir"; then
          temp_dir=""
        else
          cleanup_status=1
        fi
      fi
      return "$cleanup_status"
    }

    vp_soak_watch_restore() {
      local restore_status=0
      local rollback_read="$temp_dir/rollback-read"
      local rollback_error="$temp_dir/rollback-error"

      if [[ "$cron_may_have_changed" == true ]]; then
        if [[ "$prior_cron_absent" == true ]]; then
          if ! LC_ALL=C crontab -r 2>"$rollback_error" \
            && ! vp_soak_watch_is_no_crontab_error "$rollback_error"; then
            cat "$rollback_error" >&2
            restore_status=1
          elif ! vp_soak_watch_read_cron "$rollback_read" "$rollback_error" \
            || [[ "$vp_soak_read_absent" != true ]]; then
            echo "ChannelOps soak watcher no-crontab rollback verification failed" >&2
            restore_status=1
          fi
        else
          if ! LC_ALL=C crontab "$current_cron"; then
            echo "ChannelOps soak watcher crontab rollback install failed" >&2
            restore_status=1
          elif ! vp_soak_watch_read_cron "$rollback_read" "$rollback_error" \
            || [[ "$vp_soak_read_absent" == true ]] \
            || ! cmp -s "$current_cron" "$rollback_read"; then
            echo "ChannelOps soak watcher crontab rollback verification failed" >&2
            restore_status=1
          fi
        fi
      fi

      if [[ "$watcher_replaced" == true ]]; then
        if [[ "$watcher_had_prior" == true ]]; then
          if ! cp -p "$watch_txn_dir/prior-watcher" "$watch_txn_dir/restore-watcher" \
            || ! mv -f "$watch_txn_dir/restore-watcher" "$target" \
            || ! cmp -s "$watch_txn_dir/prior-watcher" "$target"; then
            echo "ChannelOps soak watcher target rollback failed" >&2
            restore_status=1
          fi
        elif ! rm -f "$target" || [[ -e "$target" ]]; then
          echo "ChannelOps soak watcher target absence rollback failed" >&2
          restore_status=1
        fi
      fi
      return "$restore_status"
    }

    vp_soak_watch_interrupted() {
      local signal_name="$1"
      local signal_status="$2"
      trap - HUP INT TERM
      echo "ChannelOps soak watcher install interrupted by $signal_name" >&2
      if ! vp_soak_watch_restore; then
        echo "ChannelOps soak watcher rollback failed after $signal_name" >&2
      fi
      if ! vp_soak_watch_cleanup; then
        echo "ChannelOps soak watcher cleanup failed after $signal_name" >&2
      fi
      exit "$signal_status"
    }

    trap 'vp_soak_watch_interrupted HUP 129' HUP
    trap 'vp_soak_watch_interrupted INT 130' INT
    trap 'vp_soak_watch_interrupted TERM 143' TERM

    while :; do
      temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/vp-soak-watch-cron.XXXXXX")" || {
        failure_reason="could not create cron transaction directory"
        break
      }
      current_cron="$temp_dir/current"
      next_cron="$temp_dir/next"
      verify_cron="$temp_dir/verify"

      if ! vp_soak_watch_read_cron "$current_cron" "$temp_dir/read-error"; then
        failure_reason="crontab read failed"
        break
      fi
      prior_cron_absent="$vp_soak_read_absent"

      if ! awk -v begin="$cron_begin" -v end="$cron_end" \
        -v target="$target" -v source="$source" \
        -v root_assignment="DEPLOY_GITHUB_SYNC_ROOT=$sync_root" '
        BEGIN { in_managed = 0; expected_end = ""; invalid = 0 }
        $0 == begin {
          if (in_managed) {
            invalid = 1
            exit
          }
          in_managed = 1
          expected_end = end
          next
        }
        $0 == end {
          if (!in_managed || $0 != expected_end) {
            invalid = 1
            exit
          }
          in_managed = 0
          expected_end = ""
          next
        }
        in_managed { next }
        $1 !~ /^#/ && NF >= 6 && ($6 == target || $6 == source) { next }
        $1 !~ /^#/ && NF >= 7 && $6 == root_assignment && ($7 == target || $7 == source) { next }
        { print }
        END {
          if (in_managed) {
            invalid = 1
          }
          if (invalid) {
            exit 1
          }
        }
      ' "$current_cron" >"$next_cron"; then
        failure_reason="managed cron block is malformed"
        break
      fi
      if ! printf '%s\n%s\n%s\n' \
        "$cron_begin" "$cron_command" "$cron_end" >>"$next_cron"; then
        failure_reason="managed cron render failed"
        break
      fi

      if ! mkdir -p "$sync_root/bin" "$sync_root/logs" "$sync_root/state"; then
        failure_reason="watcher directories could not be created"
        break
      fi
      watch_txn_dir="$(mktemp -d "$sync_root/bin/.channelops-soak-watch.txn.XXXXXX")" || {
        failure_reason="watcher transaction directory could not be created"
        break
      }
      if ! install -m 0755 "$source" "$watch_txn_dir/staged-watcher" \
        || [[ ! -x "$watch_txn_dir/staged-watcher" ]] \
        || ! cmp -s "$source" "$watch_txn_dir/staged-watcher"; then
        failure_reason="staged watcher verification failed"
        break
      fi
      if [[ -e "$target" ]]; then
        watcher_had_prior=true
        if ! cp -p "$target" "$watch_txn_dir/prior-watcher"; then
          failure_reason="prior watcher backup failed"
          break
        fi
      fi

      watcher_replaced=true
      if ! mv -f "$watch_txn_dir/staged-watcher" "$target"; then
        failure_reason="atomic watcher install failed"
        break
      fi
      cron_may_have_changed=true
      if ! LC_ALL=C crontab "$next_cron"; then
        failure_reason="crontab install failed"
        break
      fi
      if ! vp_soak_watch_read_cron "$verify_cron" "$temp_dir/verify-error" \
        || [[ "$vp_soak_read_absent" == true ]] \
        || ! cmp -s "$next_cron" "$verify_cron"; then
        failure_reason="crontab verification failed"
        break
      fi
      if [[ ! -x "$target" ]] || ! cmp -s "$source" "$target"; then
        failure_reason="installed watcher verification failed"
        break
      fi

      transaction_status=0
      break
    done

    if [[ "$transaction_status" -ne 0 ]]; then
      if [[ -n "$failure_reason" ]]; then
        echo "ChannelOps soak watcher $failure_reason" >&2
      fi
      if ! vp_soak_watch_restore; then
        echo "ChannelOps soak watcher rollback failed" >&2
      fi
    fi
    trap - HUP INT TERM
    if ! vp_soak_watch_cleanup; then
      if [[ "$transaction_status" -eq 0 ]]; then
        echo "ChannelOps soak watcher cleanup failed after verified install; continuing" >&2
      else
        echo "ChannelOps soak watcher cleanup failed" >&2
      fi
    fi
    exit "$transaction_status"
  )
}

vp_worker_redis_marker_file_mode() {
  local path="$1"
  local mode
  if mode="$(stat -f '%Lp' "$path" 2>/dev/null)"; then
    printf '%s\n' "$mode"
    return 0
  fi
  stat -c '%a' "$path" 2>/dev/null
}

vp_worker_redis_marker_reject_126() {
  local value
  value="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    *10.0.0.126*|*caspers-mac-mini*|*colima-swarmbridged*|*colima-126*|*hostname==126*)
      return 1
      ;;
  esac
}

vp_require_worker_redis_runtime_state() {
  local state_file="${VP_WORKER_REDIS_RUNTIME_STATE_FILE:-}"
  local expected_generation="${VP_WORKER_REDIS_RUNTIME_GENERATION:-}"
  if [[ ! "$state_file" = /* || ! -f "$state_file" || -L "$state_file" \
    || "$(vp_worker_redis_marker_file_mode "$state_file")" != 400 \
    || ! "$expected_generation" =~ ^[0-9a-f]{40}$ ]]; then
    echo "worker Redis runtime state is absent or invalid" >&2
    return 1
  fi

  local generation=""
  local acl_identity=""
  local aof_enabled=""
  local aof_status=""
  local maxmemory_policy=""
  local network=""
  local control_secret=""
  local ffmpeg_go_secret=""
  local ffmpeg_secret=""
  local vision_secret=""
  local youtube_publisher_secret=""
  local watcher_secret=""
  local readiness_secret=""
  local janitor_secret=""
  local repair_secret=""
  local key
  local value
  while IFS='=' read -r key value; do
    if [[ -z "$key" || -z "$value" || "$value" == *$'\r'* ]]; then
      echo "worker Redis runtime state is invalid" >&2
      return 1
    fi
    case "$key" in
      GENERATION)
        [[ -z "$generation" ]] || return 1
        generation="$value"
        ;;
      ACL_IDENTITY)
        [[ -z "$acl_identity" ]] || return 1
        acl_identity="$value"
        ;;
      AOF_ENABLED)
        [[ -z "$aof_enabled" ]] || return 1
        aof_enabled="$value"
        ;;
      AOF_STATUS)
        [[ -z "$aof_status" ]] || return 1
        aof_status="$value"
        ;;
      MAXMEMORY_POLICY)
        [[ -z "$maxmemory_policy" ]] || return 1
        maxmemory_policy="$value"
        ;;
      NETWORK)
        [[ -z "$network" ]] || return 1
        network="$value"
        ;;
      CONTROL_REDIS_SECRET)
        [[ -z "$control_secret" ]] || return 1
        control_secret="$value"
        ;;
      FFMPEG_GO_REDIS_SECRET)
        [[ -z "$ffmpeg_go_secret" ]] || return 1
        ffmpeg_go_secret="$value"
        ;;
      FFMPEG_REDIS_SECRET)
        [[ -z "$ffmpeg_secret" ]] || return 1
        ffmpeg_secret="$value"
        ;;
      VISION_REDIS_SECRET)
        [[ -z "$vision_secret" ]] || return 1
        vision_secret="$value"
        ;;
      YOUTUBE_PUBLISHER_REDIS_SECRET)
        [[ -z "$youtube_publisher_secret" ]] || return 1
        youtube_publisher_secret="$value"
        ;;
      WATCHER_REDIS_SECRET)
        [[ -z "$watcher_secret" ]] || return 1
        watcher_secret="$value"
        ;;
      READINESS_REDIS_SECRET)
        [[ -z "$readiness_secret" ]] || return 1
        readiness_secret="$value"
        ;;
      JANITOR_REDIS_SECRET)
        [[ -z "$janitor_secret" ]] || return 1
        janitor_secret="$value"
        ;;
      REPAIR_REDIS_SECRET)
        [[ -z "$repair_secret" ]] || return 1
        repair_secret="$value"
        ;;
      *)
        echo "worker Redis runtime state contains an unknown field" >&2
        return 1
        ;;
    esac
  done <"$state_file"

  if [[ "$generation" != "$expected_generation" \
    || "$acl_identity" != "$VP_WORKER_REDIS_RUNTIME_ACL_IDENTITY" \
    || "$aof_enabled" != yes \
    || "$aof_status" != ok \
    || "$maxmemory_policy" != noeviction \
    || "$network" != "$VP_PIPELINE_NETWORK" ]]; then
    echo "worker Redis runtime identity or persistence state is unready" >&2
    return 1
  fi
  if ! vp_worker_redis_marker_reject_126 \
    "$generation $network $control_secret $ffmpeg_go_secret $ffmpeg_secret $vision_secret $youtube_publisher_secret $watcher_secret $readiness_secret $janitor_secret $repair_secret"; then
    echo "worker Redis runtime state contains forbidden topology" >&2
    return 1
  fi

  local secret
  local secret_id
  local secret_identity
  local inspected_name
  local identity_extra
  local seen_secrets="|"
  local seen_secret_ids="|"
  for secret in \
    "$control_secret" \
    "$ffmpeg_go_secret" \
    "$ffmpeg_secret" \
    "$vision_secret" \
    "$youtube_publisher_secret" \
    "$watcher_secret" \
    "$readiness_secret" \
    "$janitor_secret" \
    "$repair_secret"; do
    if [[ ! "$secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
      echo "worker Redis runtime secret is absent" >&2
      return 1
    fi
    secret_identity="$(
      docker secret inspect "$secret" --format '{{.ID}}|{{.Spec.Name}}'
    )" || {
      echo "worker Redis runtime secret is absent" >&2
      return 1
    }
    IFS='|' read -r \
      secret_id inspected_name identity_extra <<<"$secret_identity"
    if [[ -n "$identity_extra" \
      || ! "$secret_id" =~ ^[a-z0-9]{20,64}$ \
      || "$inspected_name" != "$secret" \
      || "$seen_secret_ids" == *"|$secret_id|"* ]]; then
      echo "worker Redis runtime secret identity is invalid" >&2
      return 1
    fi
    if [[ "$seen_secrets" == *"|$secret|"* ]]; then
      echo "worker Redis runtime secrets are not independent" >&2
      return 1
    fi
    seen_secrets="$seen_secrets$secret|"
    seen_secret_ids="$seen_secret_ids$secret_id|"
    case "$secret" in
      "$control_secret") VP_WORKER_REDIS_CONTROL_SECRET_ID="$secret_id" ;;
      "$ffmpeg_go_secret") VP_WORKER_REDIS_FFMPEG_GO_SECRET_ID="$secret_id" ;;
      "$ffmpeg_secret") VP_WORKER_REDIS_FFMPEG_SECRET_ID="$secret_id" ;;
      "$vision_secret") VP_WORKER_REDIS_VISION_SECRET_ID="$secret_id" ;;
      "$youtube_publisher_secret")
        VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET_ID="$secret_id"
        ;;
      "$watcher_secret") VP_WORKER_REDIS_WATCHER_SECRET_ID="$secret_id" ;;
      "$readiness_secret")
        VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET_ID="$secret_id"
        ;;
      "$janitor_secret")
        VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET_ID="$secret_id"
        ;;
      "$repair_secret")
        VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET_ID="$secret_id"
        ;;
      *) return 1 ;;
    esac
  done

  VP_WORKER_REDIS_CONTROL_SECRET="$control_secret"
  VP_WORKER_REDIS_FFMPEG_GO_SECRET="$ffmpeg_go_secret"
  VP_WORKER_REDIS_FFMPEG_SECRET="$ffmpeg_secret"
  VP_WORKER_REDIS_VISION_SECRET="$vision_secret"
  VP_WORKER_REDIS_YOUTUBE_PUBLISHER_SECRET="$youtube_publisher_secret"
  VP_WORKER_REDIS_WATCHER_SECRET="$watcher_secret"
  VP_WORKER_REDIS_MARKER_RUNTIME_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET="$readiness_secret"
  VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET="$janitor_secret"
  VP_WORKER_REDIS_MARKER_REPAIR_REDIS_SECRET="$repair_secret"
}

vp_worker_redis_marker_control_root() {
  local sync_root="${ROOT:-}"
  if [[ -z "$sync_root" || ! "$sync_root" = /* ]]; then
    return 1
  fi
  printf '%s\n' "$sync_root/state/worker-redis-marker-control"
}

vp_worker_redis_marker_new_generation() {
  local image="$1"
  local digest
  digest="$(printf '%s' "$image" | shasum -a 256 | cut -c1-12)" || return 1
  local epoch
  epoch="$(date +%s)" || return 1
  printf 'm-%s-%s-%04d\n' "$digest" "$epoch" "$((RANDOM % 10000))"
}

vp_worker_redis_marker_owner_file() {
  local path="${VP_WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE:-}"
  if [[ -z "$path" ]]; then
    path="${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}"
  fi
  if [[ -n "${VP_WORKER_DATABASE_CREDENTIAL_RECORDS:-}" ]]; then
    path="$(
      vp_verify_worker_database_credential_record \
        control_role_owner \
        "$path" \
        "${VP_WORKER_CONTROL_ROLE_OWNER_EXPECTED_PRINCIPAL:-}" \
        2>/dev/null
    )" || {
      echo "worker marker owner database URL file is absent or invalid" >&2
      return 1
    }
  fi
  if [[ ! "$path" = /* || ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 400 ]]; then
    echo "worker marker owner database URL file is absent or invalid" >&2
    return 1
  fi
  printf '%s\n' "$path"
}

vp_worker_redis_marker_database_secret_name() {
  local purpose="$1"
  local generation="$2"
  printf 'vp-wrm-%s-db-%s\n' "$purpose" "$generation"
}

vp_worker_redis_marker_provision_roles() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local owner_file
  owner_file="$(vp_worker_redis_marker_owner_file)" || return 1
  local role_state
  role_state="$(
    vp_python_worker_prepare_controlled_directory "$control_root/roles"
  )" || return 1

  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-marker-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$role_state,dst=/control-state" \
    --env WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url \
    -- \
    python -m app.services.worker_marker_control_role_cli \
      provision \
      --generation "$generation" \
      --state-dir /control-state >/dev/null
}

vp_worker_redis_marker_revoke_roles() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local owner_file
  owner_file="$(vp_worker_redis_marker_owner_file)" || return 1
  local role_state
  role_state="$(
    vp_python_worker_prepare_controlled_directory "$control_root/roles"
  )" || return 1

  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-marker-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$role_state,dst=/control-state" \
    --env WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url \
    -- \
    python -m app.services.worker_marker_control_role_cli \
      revoke \
      --generation "$generation" \
      --state-dir /control-state >/dev/null
}

vp_worker_redis_marker_write_secret_manifest() {
  local control_root="$1"
  local generation="$2"
  local readiness_id="$3"
  local janitor_id="$4"
  local repair_id="$5"
  [[ "$control_root" = /* \
    && "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    && "$readiness_id" =~ ^[a-z0-9]{20,64}$ \
    && "$janitor_id" =~ ^[a-z0-9]{20,64}$ \
    && "$repair_id" =~ ^[a-z0-9]{20,64}$ \
    && "$readiness_id" != "$janitor_id" \
    && "$readiness_id" != "$repair_id" \
    && "$janitor_id" != "$repair_id" ]] || return 1
  local directory="$control_root/secret-manifests"
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  local path="$directory/$generation.conf"
  local temporary
  temporary="$(mktemp "$directory/.marker-secrets.XXXXXX")" || return 1
  if ! chmod 0600 "$temporary" \
    || ! printf '%s\n' \
      "VERSION=2" \
      "GENERATION=$generation" \
      "READINESS_SECRET=$(vp_worker_redis_marker_database_secret_name readiness "$generation")" \
      "READINESS_SECRET_ID=$readiness_id" \
      "JANITOR_SECRET=$(vp_worker_redis_marker_database_secret_name janitor "$generation")" \
      "JANITOR_SECRET_ID=$janitor_id" \
      "REPAIR_SECRET=$(vp_worker_redis_marker_database_secret_name repair "$generation")" \
      "REPAIR_SECRET_ID=$repair_id" \
      >"$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
  [[ -f "$path" && ! -L "$path" \
    && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]]
}

vp_worker_redis_marker_write_manual_secret_evidence() {
  local control_root="$1"
  local generation="$2"
  local purpose="$3"
  local secret_name="$4"
  [[ "$control_root" = /* \
    && "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    && "$purpose" =~ ^(readiness|janitor|repair)$ \
    && "$secret_name" \
      == "$(vp_worker_redis_marker_database_secret_name \
        "$purpose" "$generation")" ]] || return 1
  local directory="$control_root/legacy-unretirable-secrets"
  mkdir -p "$directory" || return 1
  chmod 0700 "$directory" || return 1
  local path="$directory/$generation-$purpose.conf"
  if [[ -e "$path" ]]; then
    [[ -f "$path" && ! -L "$path" \
      && "$(vp_worker_redis_marker_file_mode "$path")" == 600 ]] \
      || return 1
    return 0
  fi
  local temporary
  temporary="$(mktemp "$directory/.legacy-secret.XXXXXX")" || return 1
  if ! chmod 0600 "$temporary" \
    || ! printf '%s\n' \
      "VERSION=1" \
      "STATUS=legacy_unretirable" \
      "SERVICE=worker-redis-marker-control" \
      "GENERATION=$generation" \
      "PURPOSE=$purpose-database" \
      "SECRET_NAME=$secret_name" \
      >"$temporary" \
    || ! mv -f "$temporary" "$path"; then
    rm -f "$temporary"
    return 1
  fi
}

vp_worker_redis_marker_require_secret_manifest() {
  local control_root="$1"
  local generation="$2"
  [[ "$control_root" = /* \
    && "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]] || return 1
  local path="$control_root/secret-manifests/$generation.conf"
  if [[ ! -e "$path" ]]; then
    local hydrated_ids=()
    local purpose
    for purpose in readiness janitor repair; do
      local secret_name
      secret_name="$(
        vp_worker_redis_marker_database_secret_name \
          "$purpose" "$generation"
      )" || return 1
      local secret_id
      secret_id="$(
        vp_managed_secret_id \
          "$secret_name" "$secret_name" \
          worker-redis-marker-control "$generation" "$purpose-database"
      )" || {
        vp_worker_redis_marker_write_manual_secret_evidence \
          "$control_root" "$generation" "$purpose" "$secret_name" \
          || return 1
        return 1
      }
      hydrated_ids+=("$secret_id")
    done
    vp_worker_redis_marker_write_secret_manifest \
      "$control_root" "$generation" "${hydrated_ids[@]}" || return 1
  fi
  if [[ ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    return 1
  fi
  local version=""
  local manifest_generation=""
  local readiness_name=""
  local readiness_id=""
  local janitor_name=""
  local janitor_id=""
  local repair_name=""
  local repair_id=""
  local key
  local value
  while IFS='=' read -r key value; do
    [[ -n "$key" && -n "$value" && "$value" != *$'\r'* ]] \
      || return 1
    case "$key" in
      VERSION) [[ -z "$version" ]] || return 1; version="$value" ;;
      GENERATION)
        [[ -z "$manifest_generation" ]] || return 1
        manifest_generation="$value"
        ;;
      READINESS_SECRET)
        [[ -z "$readiness_name" ]] || return 1
        readiness_name="$value"
        ;;
      READINESS_SECRET_ID)
        [[ -z "$readiness_id" ]] || return 1
        readiness_id="$value"
        ;;
      JANITOR_SECRET)
        [[ -z "$janitor_name" ]] || return 1
        janitor_name="$value"
        ;;
      JANITOR_SECRET_ID)
        [[ -z "$janitor_id" ]] || return 1
        janitor_id="$value"
        ;;
      REPAIR_SECRET)
        [[ -z "$repair_name" ]] || return 1
        repair_name="$value"
        ;;
      REPAIR_SECRET_ID)
        [[ -z "$repair_id" ]] || return 1
        repair_id="$value"
        ;;
      *) return 1 ;;
    esac
  done <"$path"
  [[ "$version" == 2 \
    && "$manifest_generation" == "$generation" \
    && "$readiness_name" \
      == "$(vp_worker_redis_marker_database_secret_name readiness "$generation")" \
    && "$janitor_name" \
      == "$(vp_worker_redis_marker_database_secret_name janitor "$generation")" \
    && "$repair_name" \
      == "$(vp_worker_redis_marker_database_secret_name repair "$generation")" \
    && "$readiness_id" =~ ^[a-z0-9]{20,64}$ \
    && "$janitor_id" =~ ^[a-z0-9]{20,64}$ \
    && "$repair_id" =~ ^[a-z0-9]{20,64}$ \
    && "$readiness_id" != "$janitor_id" \
    && "$readiness_id" != "$repair_id" \
    && "$janitor_id" != "$repair_id" ]] || return 1
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID="$readiness_id"
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID="$janitor_id"
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID="$repair_id"
}

vp_worker_redis_marker_create_database_secrets() {
  local generation="$1"
  local control_root="$2"
  local purpose
  local secret_name
  local credential_file
  VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS=""
  VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID=""
  VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID=""
  for purpose in readiness janitor repair; do
    secret_name="$(
      vp_worker_redis_marker_database_secret_name "$purpose" "$generation"
    )" || return 1
    credential_file="$control_root/roles/$generation/worker-marker-$purpose-database-url"
    if ! vp_worker_admission_create_secret \
      "$secret_name" "$credential_file" \
      worker-redis-marker-control "$generation" \
      "$purpose-database"; then
      echo "worker marker database secret creation failed" >&2
      return 1
    fi
    VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS="${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:+$VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS$'\n'}$secret_name|$VP_WORKER_CREATED_SECRET_ID|$purpose-database"
    case "$purpose" in
      readiness)
        VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID="$VP_WORKER_CREATED_SECRET_ID"
        ;;
      janitor)
        VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID="$VP_WORKER_CREATED_SECRET_ID"
        ;;
      repair)
        VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID="$VP_WORKER_CREATED_SECRET_ID"
        ;;
    esac
  done
  vp_worker_redis_marker_write_secret_manifest \
    "$control_root" "$generation" \
    "$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID" \
    "$VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID" \
    "$VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID"
}

vp_worker_redis_marker_expected_job_identity() {
  local image="$1"
  local generation="$2"
  local mode="$3"
  local readiness_redis_secret="${4:-$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET}"
  local janitor_redis_secret="${5:-$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET}"
  local database_secret
  local redis_secret
  local module
  local command
  case "$mode" in
    readiness)
      database_secret="$(
        vp_worker_redis_marker_database_secret_name readiness "$generation"
      )" || return 1
      redis_secret="$readiness_redis_secret"
      module="app.channel_agent.worker_redis_marker_readiness_cli"
      command="check"
      ;;
    janitor)
      database_secret="$(
        vp_worker_redis_marker_database_secret_name janitor "$generation"
      )" || return 1
      redis_secret="$janitor_redis_secret"
      module="app.channel_agent.worker_redis_marker_janitor_cli"
      command="run"
      ;;
    *)
      return 1
      ;;
  esac
  vp_require_pipeline_network_identity || return 1
  printf '%s\n' \
    "2|$mode|$generation|$image|replicated-job|1|1|none|node.hostname==$VP_MANAGER_NODE|$VP_PIPELINE_NETWORK_ID|$database_secret:worker-marker-database-url:10001:10001:256,$redis_secret:worker-marker-redis-url:10001:10001:256|WORKER_REDIS_MARKER_DATABASE_URL_FILE=/run/secrets/worker-marker-database-url,WORKER_REDIS_MARKER_REDIS_URL_FILE=/run/secrets/worker-marker-redis-url|python,-m,$module,$command"
}

vp_worker_redis_marker_job_identity() {
  local name="$1"
  local identity
  identity="$(
    docker service inspect "$name" --format \
      '{{len .Spec.Labels}}|{{index .Spec.Labels "vp.worker-redis-marker.mode"}}|{{index .Spec.Labels "vp.worker-redis-marker.generation"}}|{{.Spec.TaskTemplate.ContainerSpec.Image}}|{{if .Spec.Mode.ReplicatedJob}}replicated-job{{else}}other{{end}}|{{.Spec.Mode.ReplicatedJob.TotalCompletions}}|{{.Spec.Mode.ReplicatedJob.MaxConcurrent}}|{{.Spec.TaskTemplate.RestartPolicy.Condition}}|{{range .Spec.TaskTemplate.Placement.Constraints}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.Networks}}{{printf "%s," .Target}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Secrets}}{{printf "%s:%s:%s:%s:%d," .SecretName .File.Name .File.UID .File.GID .File.Mode}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{printf "%s," .}}{{end}}|{{range .Spec.TaskTemplate.ContainerSpec.Args}}{{printf "%s," .}}{{end}}'
  )" || return 1
  identity="${identity//,|/|}"
  printf '%s\n' "${identity%,}"
}

vp_worker_redis_marker_remove_generation_jobs() {
  local image="$1"
  local generation="$2"
  local readiness_redis_secret="${3:-$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET}"
  local janitor_redis_secret="${4:-$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET}"
  [[ -n "$image" && -n "$generation" ]] || return 0
  local name
  local mode
  local expected_identity
  local actual_identity
  local states
  local service_binding
  local service_id
  local service_name
  local extra
  for mode in readiness janitor; do
    case "$mode" in
      readiness)
        name=vp-worker-redis-marker-readiness-job
        ;;
      janitor)
        name=vp-worker-redis-marker-janitor-job
        ;;
    esac
    if ! service_binding="$(
      docker service inspect "$name" \
        --format '{{.ID}}|{{.Spec.Name}}' 2>/dev/null
    )"; then
      continue
    fi
    IFS='|' read -r service_id service_name extra <<<"$service_binding"
    [[ -z "$extra" \
      && "$service_id" =~ ^[a-z0-9]{12,64}$ \
      && "$service_name" == "$name" ]] || return 1
    expected_identity="$(
      vp_worker_redis_marker_expected_job_identity \
        "$image" "$generation" "$mode" \
        "$readiness_redis_secret" "$janitor_redis_secret"
    )" || return 1
    actual_identity="$(
      vp_worker_redis_marker_job_identity "$service_id"
    )" || return 1
    if [[ "$actual_identity" != "$expected_identity" ]]; then
      local actual_label_count
      local actual_mode
      local actual_generation
      local actual_image
      IFS='|' read -r \
        actual_label_count actual_mode actual_generation actual_image \
        _ <<<"$actual_identity"
      if [[ "$actual_label_count" != 2 \
        || "$actual_mode" != "$mode" \
        || ! "$actual_generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
        || "$actual_generation" == "$generation" \
        || ! "$actual_image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
        || "$actual_identity" \
          != "$(vp_worker_redis_marker_expected_job_identity \
            "$actual_image" "$actual_generation" "$mode")" ]]; then
        echo "worker marker fixed-name job identity does not match generation" >&2
        return 1
      fi
      continue
    fi
    states="$(
      docker service ps "$service_id" --no-trunc \
        --format '{{.CurrentState}}'
    )" || return 1
    if ! printf '%s\n' "$states" | awk '
      NF {
        count++
        if ($1 !~ /^(Complete|Failed|Rejected|Shutdown|Orphaned|Remove)$/) {
          invalid=1
        }
      }
      END { exit count == 1 && !invalid ? 0 : 1 }
    '; then
      echo "worker marker generation still has a running job" >&2
      return 1
    fi
    docker service rm "$service_id" >/dev/null || return 1
    local attempt
    for ((attempt = 0; attempt < 30; attempt++)); do
      if ! docker service inspect "$service_id" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if docker service inspect "$service_id" >/dev/null 2>&1; then
      echo "worker marker fixed-name job removal did not converge" >&2
      return 1
    fi
  done
}

vp_worker_redis_marker_retire_generation() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local readiness_redis_secret="${4:-$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET}"
  local janitor_redis_secret="${5:-$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET}"
  [[ -n "$image" && -n "$generation" ]] || return 0
  vp_worker_redis_marker_require_secret_manifest \
    "$control_root" "$generation" || return 1
  vp_worker_redis_marker_remove_generation_jobs \
    "$image" "$generation" \
    "$readiness_redis_secret" "$janitor_redis_secret" || return 1
  vp_worker_redis_marker_revoke_roles \
    "$image" "$generation" "$control_root" || return 1
  local purpose
  local secret_name
  for purpose in readiness janitor repair; do
    secret_name="$(
      vp_worker_redis_marker_database_secret_name "$purpose" "$generation"
    )" || return 1
    local secret_id
    case "$purpose" in
      readiness)
        secret_id="$VP_WORKER_REDIS_MARKER_READINESS_DATABASE_SECRET_ID"
        ;;
      janitor)
        secret_id="$VP_WORKER_REDIS_MARKER_JANITOR_DATABASE_SECRET_ID"
        ;;
      repair)
        secret_id="$VP_WORKER_REDIS_MARKER_REPAIR_DATABASE_SECRET_ID"
        ;;
    esac
    vp_remove_managed_secret_if_absent_exact \
      "$secret_id" "$secret_name" \
      worker-redis-marker-control "$generation" "$purpose-database" \
      || return 1
  done
}

vp_worker_redis_marker_read_prior_config() {
  local path="$1"
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
  VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=""
  VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=""
  [[ -e "$path" ]] || return 0
  if [[ ! -f "$path" || -L "$path" \
    || "$(vp_worker_redis_marker_file_mode "$path")" != 600 ]]; then
    echo "worker marker active configuration is invalid" >&2
    return 1
  fi
  local key
  local value
  local generation=""
  local image=""
  local network=""
  local network_id=""
  local readiness_database_secret=""
  local readiness_redis_secret=""
  local janitor_database_secret=""
  local janitor_redis_secret=""
  while IFS='=' read -r key value; do
    [[ -n "$key" && -n "$value" && "$value" != *$'\r'* ]] || return 1
    case "$key" in
      GENERATION)
        [[ -z "$generation" ]] || return 1
        generation="$value"
        ;;
      IMAGE)
        [[ -z "$image" ]] || return 1
        image="$value"
        ;;
      NETWORK)
        [[ -z "$network" ]] || return 1
        network="$value"
        ;;
      NETWORK_ID)
        [[ -z "$network_id" ]] || return 1
        network_id="$value"
        ;;
      READINESS_DATABASE_SECRET)
        [[ -z "$readiness_database_secret" ]] || return 1
        readiness_database_secret="$value"
        ;;
      READINESS_REDIS_SECRET)
        [[ -z "$readiness_redis_secret" ]] || return 1
        readiness_redis_secret="$value"
        ;;
      JANITOR_DATABASE_SECRET)
        [[ -z "$janitor_database_secret" ]] || return 1
        janitor_database_secret="$value"
        ;;
      JANITOR_REDIS_SECRET)
        [[ -z "$janitor_redis_secret" ]] || return 1
        janitor_redis_secret="$value"
        ;;
      *)
        return 1
        ;;
    esac
  done <"$path"
  if [[ ! "$generation" =~ ^[a-z0-9][a-z0-9-]{0,62}$ \
    || ! "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    || "$network" != "$VP_PIPELINE_NETWORK" \
    || "$network_id" != "$VP_PIPELINE_NETWORK_ID" \
    || "$readiness_database_secret" \
      != "$(vp_worker_redis_marker_database_secret_name readiness "$generation")" \
    || "$janitor_database_secret" \
      != "$(vp_worker_redis_marker_database_secret_name janitor "$generation")" \
    || ! "$readiness_redis_secret" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    || ! "$janitor_redis_secret" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    || "$readiness_redis_secret" == "$janitor_redis_secret" ]]; then
    return 1
  fi
  if ! vp_worker_redis_marker_reject_126 \
    "$generation $image $network $network_id $readiness_database_secret $readiness_redis_secret $janitor_database_secret $janitor_redis_secret"; then
    return 1
  fi
  VP_WORKER_REDIS_MARKER_PRIOR_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_PRIOR_IMAGE="$image"
  VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET="$readiness_redis_secret"
  VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET="$janitor_redis_secret"
}

vp_worker_redis_marker_render_config() {
  local path="$1"
  local image="$2"
  local generation="$3"
  local readiness_database_secret
  local janitor_database_secret
  readiness_database_secret="$(
    vp_worker_redis_marker_database_secret_name readiness "$generation"
  )" || return 1
  janitor_database_secret="$(
    vp_worker_redis_marker_database_secret_name janitor "$generation"
  )" || return 1

  {
    printf 'GENERATION=%s\n' "$generation"
    printf 'IMAGE=%s\n' "$image"
    printf 'NETWORK=%s\n' "$VP_PIPELINE_NETWORK"
    printf 'NETWORK_ID=%s\n' "$VP_PIPELINE_NETWORK_ID"
    printf 'READINESS_DATABASE_SECRET=%s\n' "$readiness_database_secret"
    printf 'READINESS_REDIS_SECRET=%s\n' \
      "$VP_WORKER_REDIS_MARKER_READINESS_REDIS_SECRET"
    printf 'JANITOR_DATABASE_SECRET=%s\n' "$janitor_database_secret"
    printf 'JANITOR_REDIS_SECRET=%s\n' \
      "$VP_WORKER_REDIS_MARKER_JANITOR_REDIS_SECRET"
  } >"$path"
  chmod 0600 "$path"
}

vp_worker_redis_marker_is_no_crontab_error() {
  awk 'NR == 1 && /^no crontab for .+$/ { matched=1; next }
    { matched=0; exit }
    END { exit matched ? 0 : 1 }' "$1"
}

vp_worker_redis_marker_read_cron() {
  local output="$1"
  local error_output="$2"
  VP_WORKER_REDIS_MARKER_CRON_ABSENT=false
  if LC_ALL=C crontab -l >"$output" 2>"$error_output"; then
    return 0
  fi
  if vp_worker_redis_marker_is_no_crontab_error "$error_output"; then
    : >"$output"
    VP_WORKER_REDIS_MARKER_CRON_ABSENT=true
    return 0
  fi
  cat "$error_output" >&2
  return 1
}

vp_worker_redis_marker_unmanaged_cron() {
  local prior_cron="$1"
  local output="$2"
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local source="$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  awk -v begin="$cron_begin" -v end="$cron_end" \
    -v target="$target" -v source="$source" '
    BEGIN { managed=0; invalid=0 }
    $0 == begin {
      if (managed) {
        invalid=1
        exit
      }
      managed=1
      next
    }
    $0 == end {
      if (!managed) {
        invalid=1
        exit
      }
      managed=0
      next
    }
    managed { next }
    $1 !~ /^#/ && ($6 == target || $6 == source) { next }
    $1 !~ /^#/ && NF >= 9 && ($9 == target || $9 == source) { next }
    { print }
    END { if (managed || invalid) exit 1 }
  ' "$prior_cron" >"$output"
}

vp_worker_redis_marker_capture_managed_state() {
  local control_root="$1"
  local requested_state="${2:-}"
  local state=""
  if [[ -n "$requested_state" ]]; then
    local state_prefix="$control_root/transactions/"
    [[ "$requested_state" == "$state_prefix"* ]] || return 1
    local relative_state="${requested_state#"$state_prefix"}"
    [[ "$relative_state" \
      =~ ^tx-[0-9a-f]{32}/baseline-managed-state$ ]] || return 1
    state="$requested_state"
    if [[ -f "$state/captured" && ! -L "$state/captured" ]]; then
      [[ -d "$state" && ! -L "$state" \
        && "$(vp_worker_redis_marker_file_mode "$state")" == 700 \
        && "$(vp_worker_redis_marker_file_mode "$state/captured")" == 600 \
        && -f "$state/crontab" && ! -L "$state/crontab" ]] || return 1
      VP_WORKER_REDIS_MARKER_CRON_ABSENT=false
      [[ ! -f "$state/crontab.absent" ]] \
        || VP_WORKER_REDIS_MARKER_CRON_ABSENT=true
      VP_WORKER_REDIS_MARKER_MANAGED_STATE="$state"
      return 0
    fi
    rm -rf "$state" || return 1
    mkdir -p "$(dirname "$state")" || return 1
    chmod 0700 "$(dirname "$state")" || return 1
    mkdir "$state" || return 1
  else
    state="$(mktemp -d "$control_root/.managed-state.XXXXXX")" \
      || return 1
  fi
  chmod 0700 "$state" || {
    rm -rf "$state"
    return 1
  }
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  if [[ -e "$target" ]]; then
    cp -p "$target" "$state/launcher" || {
      rm -rf "$state"
      return 1
    }
  fi
  if [[ -e "$config" ]]; then
    cp -p "$config" "$state/control.conf" || {
      rm -rf "$state"
      return 1
    }
  fi
  if ! vp_worker_redis_marker_read_cron \
    "$state/crontab" "$state/crontab-error"; then
    rm -rf "$state"
    return 1
  fi
  if [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]]; then
    : >"$state/crontab.absent"
  fi
  if ! printf 'VERSION=1\n' >"$state/.captured.tmp" \
    || ! chmod 0600 "$state/.captured.tmp" \
    || ! mv -f "$state/.captured.tmp" "$state/captured"; then
    rm -rf "$state"
    return 1
  fi
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$state"
}

vp_worker_redis_marker_deactivate_managed_cron() {
  local state="${1:-$VP_WORKER_REDIS_MARKER_MANAGED_STATE}"
  local current="$state/current-crontab"
  local unmanaged="$state/unmanaged-crontab"
  if ! vp_worker_redis_marker_read_cron \
    "$current" "$state/current-crontab-error"; then
    return 1
  fi
  if [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]]; then
    return 0
  fi
  vp_worker_redis_marker_unmanaged_cron "$current" "$unmanaged" || return 1
  LC_ALL=C crontab "$unmanaged"
}

vp_worker_redis_marker_restore_managed_state() {
  local state="${1:-$VP_WORKER_REDIS_MARKER_MANAGED_STATE}"
  [[ -n "$state" && -d "$state" ]] || return 1
  vp_worker_redis_marker_deactivate_managed_cron "$state" || return 1
  local target="${ROOT}/bin/worker-redis-marker-control.sh"
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local config="$control_root/control.conf"
  mkdir -p "$(dirname "$target")" "$control_root" || return 1
  if [[ -f "$state/launcher" ]]; then
    cp -p "$state/launcher" "$target" || return 1
  else
    rm -f "$target" || return 1
  fi
  if [[ -f "$state/control.conf" ]]; then
    cp -p "$state/control.conf" "$config" || return 1
  else
    rm -f "$config" || return 1
  fi
  if [[ -f "$state/crontab.absent" ]]; then
    LC_ALL=C crontab -r >/dev/null 2>&1 || true
  else
    LC_ALL=C crontab "$state/crontab" || return 1
  fi

  if [[ -f "$state/launcher" ]]; then
    cmp -s "$state/launcher" "$target" || return 1
  else
    [[ ! -e "$target" ]] || return 1
  fi
  if [[ -f "$state/control.conf" ]]; then
    cmp -s "$state/control.conf" "$config" || return 1
  else
    [[ ! -e "$config" ]] || return 1
  fi
  local verify="$state/verify-crontab"
  if ! vp_worker_redis_marker_read_cron \
    "$verify" "$state/verify-crontab-error"; then
    return 1
  fi
  if [[ -f "$state/crontab.absent" ]]; then
    [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == true ]] || return 1
  else
    [[ "$VP_WORKER_REDIS_MARKER_CRON_ABSENT" == false ]] \
      && cmp -s "$state/crontab" "$verify"
  fi
}

vp_worker_redis_marker_discard_managed_state() {
  if [[ -n "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" ]]; then
    rm -rf "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" || return 1
  fi
  VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
}

vp_worker_redis_marker_cleanup_transaction_baseline() {
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local state="$control_root/transactions/$VP_WORKER_ADMISSION_TRANSACTION_ID/baseline-managed-state"
  if [[ ! -e "$state" ]]; then
    [[ ! -L "$state" ]] || return 1
    if [[ "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" == "$state" ]]; then
      VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
    fi
    return 0
  fi
  [[ -d "$state" && ! -L "$state" \
    && "$(vp_worker_redis_marker_file_mode "$state")" == 700 \
    && -f "$state/captured" && ! -L "$state/captured" \
    && "$(vp_worker_redis_marker_file_mode "$state/captured")" == 600 \
    && "$(command cat "$state/captured")" == VERSION=1 \
    && -f "$state/crontab" && ! -L "$state/crontab" ]] || return 1
  rm -rf "$state" || return 1
  if [[ "$VP_WORKER_REDIS_MARKER_MANAGED_STATE" == "$state" ]]; then
    VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
  fi
}

vp_install_worker_redis_marker_control() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local sync_root="${ROOT:-}"
  local source="$VP_WORKER_REDIS_MARKER_CONTROL_SOURCE"
  if [[ -z "$sync_root" || ! "$sync_root" = /* \
    || ! -r "$source" || ! -x "$source" ]] \
    || ! bash -n "$source"; then
    echo "worker Redis marker launcher source is invalid" >&2
    return 1
  fi

  local bin_dir="$sync_root/bin"
  local log_dir="$sync_root/logs"
  local target="$bin_dir/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  local cron_begin="# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local cron_end="# END VIDEOPROCESS WORKER REDIS MARKER CONTROL"
  local readiness_cron="* * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target readiness >> $log_dir/worker-redis-marker-readiness.log 2>&1"
  local janitor_cron="*/5 * * * * VP_WORKER_REDIS_MARKER_CONFIG_FILE=$config VP_WORKER_REDIS_MARKER_STATE_DIR=$state_dir VP_WORKER_REDIS_MARKER_LOCK_DIR=$lock_dir $target janitor >> $log_dir/worker-redis-marker-janitor.log 2>&1"
  local transaction
  transaction="$(mktemp -d "${TMPDIR:-/tmp}/vp-worker-marker-control.XXXXXX")" \
    || return 1
  local status=1
  local prior_cron="$transaction/prior-cron"
  local next_cron="$transaction/next-cron"
  local verify_cron="$transaction/verify-cron"
  local read_error="$transaction/read-error"
  local prior_target=false
  local prior_config=false
  local prior_cron_absent=false
  local cron_read=false
  local cron_may_have_changed=false

  if [[ -e "$target" ]]; then
    prior_target=true
    if ! cp -p "$target" "$transaction/prior-launcher"; then
      rm -rf "$transaction"
      echo "worker Redis marker launcher backup failed" >&2
      return 1
    fi
  fi
  if [[ -e "$config" ]]; then
    prior_config=true
    if ! cp -p "$config" "$transaction/prior-config"; then
      rm -rf "$transaction"
      echo "worker Redis marker config backup failed" >&2
      return 1
    fi
  fi

  if vp_worker_redis_marker_read_cron "$prior_cron" "$read_error"; then
    cron_read=true
    prior_cron_absent="$VP_WORKER_REDIS_MARKER_CRON_ABSENT"
  fi
  if [[ "$cron_read" == true ]] \
    && vp_worker_redis_marker_render_cron \
      "$prior_cron" "$next_cron" "$control_root" \
    && mkdir -p "$bin_dir" "$log_dir" "$control_root" "$state_dir" "$lock_dir" \
    && chmod 0700 "$control_root" "$state_dir" "$lock_dir" \
    && install -m 0755 "$source" "$transaction/launcher" \
    && vp_worker_redis_marker_render_config \
      "$transaction/control.conf" "$image" "$generation"; then
    if mv -f "$transaction/launcher" "$target" \
      && mv -f "$transaction/control.conf" "$config"; then
      cron_may_have_changed=true
    fi
    if [[ "$cron_may_have_changed" == true ]] \
      && LC_ALL=C crontab "$next_cron" \
      && vp_worker_redis_marker_read_cron \
        "$verify_cron" "$transaction/verify-error" \
      && cmp -s "$next_cron" "$verify_cron" \
      && cmp -s "$source" "$target" \
      && [[ "$(vp_worker_redis_marker_file_mode "$target")" == 755 ]] \
      && [[ "$(vp_worker_redis_marker_file_mode "$config")" == 600 ]]; then
      status=0
    fi
  fi

  if [[ "$status" -ne 0 ]]; then
    if [[ "$prior_target" == true ]]; then
      cp -p "$transaction/prior-launcher" "$target" || true
    else
      rm -f "$target" || true
    fi
    if [[ "$prior_config" == true ]]; then
      cp -p "$transaction/prior-config" "$config" || true
    else
      rm -f "$config" || true
    fi
    if [[ "$cron_may_have_changed" == true ]]; then
      if [[ "$prior_cron_absent" == true ]]; then
        LC_ALL=C crontab -r >/dev/null 2>&1 || true
      else
        LC_ALL=C crontab "$prior_cron" >/dev/null 2>&1 || true
      fi
    fi
    echo "worker Redis marker launcher install failed" >&2
  fi
  rm -rf "$transaction"
  return "$status"
}

vp_run_worker_redis_marker_readiness() {
  local control_root="$1"
  local launcher="${ROOT}/bin/worker-redis-marker-control.sh"
  local config="$control_root/control.conf"
  local state_dir="$control_root/status"
  local lock_dir="$control_root/locks"
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$config" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$state_dir" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$lock_dir" \
    "$launcher" readiness >/dev/null \
    || return 1
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$config" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$state_dir" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$lock_dir" \
    "$launcher" status >/dev/null
}

vp_require_worker_redis_marker_status() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker Redis marker status gate skipped"
    return 0
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  local launcher="${ROOT}/bin/worker-redis-marker-control.sh"
  VP_WORKER_REDIS_MARKER_CONFIG_FILE="$control_root/control.conf" \
  VP_WORKER_REDIS_MARKER_STATE_DIR="$control_root/status" \
  VP_WORKER_REDIS_MARKER_LOCK_DIR="$control_root/locks" \
    "$launcher" status >/dev/null
}

vp_worker_redis_marker_provision_generation() {
  local image="$1"
  local generation="$2"
  local control_root="$3"
  local journal_authority=false
  if [[ "$generation" =~ ^m-[0-9a-f]{12}-[1-9][0-9]*-[0-9]{4}$ ]]; then
    journal_authority=true
    vp_worker_admission_mark_authority_provisioning \
      marker worker-redis-marker-control "$generation" || return 1
  fi
  vp_worker_redis_marker_provision_roles \
    "$image" "$generation" "$control_root" || return 1
  if [[ "$journal_authority" == true ]]; then
    vp_worker_admission_mark_authority_provisioned \
      marker worker-redis-marker-control "$generation" || return 1
  fi
  if ! vp_worker_redis_marker_create_database_secrets \
    "$generation" "$control_root"; then
    vp_worker_redis_marker_revoke_roles \
      "$image" "$generation" "$control_root" || return 1
    local secret_name
    local secret_id
    local secret_purpose
    while IFS='|' read -r secret_name secret_id secret_purpose; do
      [[ -n "$secret_name$secret_id$secret_purpose" ]] || continue
      vp_remove_managed_secret \
        "$secret_id" "$secret_name" \
        worker-redis-marker-control "$generation" "$secret_purpose" \
        || return 1
    done <<<"${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:-}"
    return 1
  fi
}

vp_prepare_worker_redis_marker_controls() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker Redis marker control preparation skipped"
    return 0
  fi
  vp_validate_topology || return 1
  vp_require_pipeline_network_identity || return 1
  vp_require_worker_redis_runtime_state || return 1
  vp_worker_redis_marker_owner_file >/dev/null || return 1

  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  control_root="$(
    vp_python_worker_prepare_controlled_directory "$control_root"
  )" || return 1
  vp_python_worker_prepare_controlled_directory \
    "$control_root/roles" >/dev/null || return 1
  vp_worker_redis_marker_read_prior_config \
    "$control_root/control.conf" || return 1
  [[ "$VP_WORKER_ADMISSION_TRANSACTION_ID" \
    =~ ^tx-[0-9a-f]{32}$ ]] || return 1
  local durable_baseline_state="$control_root/transactions/$VP_WORKER_ADMISSION_TRANSACTION_ID/baseline-managed-state"
  vp_worker_redis_marker_capture_managed_state \
    "$control_root" "$durable_baseline_state" || return 1

  local generation
  if ! generation="$(vp_worker_redis_marker_new_generation "$image")"; then
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$image"
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false

  [[ "$VP_WORKER_ADMISSION_COMMIT" =~ ^[0-9a-f]{40}$ ]] || return 1
  local control_generation="c-${VP_WORKER_ADMISSION_COMMIT:0:20}"
  local operator_reference="marker/$generation/worker-marker-owner-database-url"
  vp_worker_admission_record_authority_intent \
    marker worker-redis-marker-control "$generation" \
    "$image" "$control_generation" "$operator_reference" || return 1
  if ! vp_worker_redis_marker_provision_generation \
    "$image" "$generation" "$control_root"; then
    VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
    VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
    VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
    vp_worker_redis_marker_discard_managed_state || return 1
    return 1
  fi
  if ! vp_worker_admission_record_marker_selection forward expected; then
    if vp_worker_redis_marker_retire_generation \
        "$image" "$generation" "$control_root" \
      && vp_worker_redis_marker_discard_managed_state; then
      VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
      VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
      VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
    else
      echo "worker Redis marker unrecorded candidate cleanup did not converge" >&2
    fi
    return 1
  fi

  if ! vp_worker_redis_marker_deactivate_managed_cron \
    || ! vp_worker_redis_marker_remove_generation_jobs \
      "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
      "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
      "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" \
    || ! vp_install_worker_redis_marker_control \
      "$image" "$generation" "$control_root" \
    || ! vp_run_worker_redis_marker_readiness "$control_root"; then
    echo "worker Redis marker candidate readiness failed" >&2
    if vp_worker_redis_marker_restore_managed_state \
      && vp_worker_redis_marker_retire_generation \
        "$image" "$generation" "$control_root"; then
      VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
      VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
      VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
      vp_worker_redis_marker_discard_managed_state || return 1
    else
      echo "worker Redis marker candidate cleanup did not converge" >&2
    fi
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=true
}

vp_prepare_worker_redis_marker_rollback_candidate() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    return 0
  fi
  local generation="$VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION"
  local image="$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE"
  [[ "$generation" =~ ^m-rb-[0-9a-f]{12}-[1-9][0-9]*$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}$ \
    && "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
      =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] || return 1
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  control_root="$(
    vp_python_worker_prepare_controlled_directory "$control_root"
  )" || return 1
  vp_python_worker_prepare_controlled_directory \
    "$control_root/roles" >/dev/null || return 1
  local manifest="$control_root/secret-manifests/$generation.conf"
  if [[ -e "$manifest" ]]; then
    vp_worker_redis_marker_require_secret_manifest \
      "$control_root" "$generation" || return 1
  else
    vp_worker_redis_marker_provision_generation \
      "$image" "$generation" "$control_root" || return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$image"
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
  vp_worker_admission_record_marker_selection rollback expected
}

vp_restore_worker_redis_marker_controls() {
  if [[ "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != true ]]; then
    return 0
  fi
  if [[ -z "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    || -z "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" ]]; then
    echo "worker Redis marker rollback has no prior generation" >&2
    return 1
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  control_root="$(
    vp_python_worker_prepare_controlled_directory "$control_root"
  )" || return 1
  vp_python_worker_prepare_controlled_directory \
    "$control_root/roles" >/dev/null || return 1
  local original_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  vp_worker_redis_marker_capture_managed_state "$control_root" || return 1
  local candidate_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$original_state"
  if ! vp_worker_redis_marker_deactivate_managed_cron "$candidate_state" \
    || ! vp_worker_redis_marker_remove_generation_jobs \
      "$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
      "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION"; then
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    return 1
  fi
  local rollback_generation="$VP_WORKER_ADMISSION_ROLLBACK_MARKER_GENERATION"
  if [[ ! "$rollback_generation" \
    =~ ^m-rb-[0-9a-f]{12}-[1-9][0-9]*$ ]]; then
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    return 1
  fi
  if ! vp_worker_redis_marker_require_secret_manifest \
      "$control_root" "$rollback_generation" \
    || ! vp_install_worker_redis_marker_control \
      "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
      "$rollback_generation" \
      "$control_root" \
    || ! vp_run_worker_redis_marker_readiness "$control_root"; then
    echo "worker Redis marker rollback readiness failed" >&2
    if ! vp_worker_redis_marker_restore_managed_state "$candidate_state" \
      || ! vp_worker_redis_marker_retire_generation \
        "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
        "$rollback_generation" \
        "$control_root"; then
      echo "worker Redis marker failed rollback cleanup did not converge" >&2
    fi
    return 1
  fi
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_IMAGE" \
    "$VP_WORKER_ROLLBACK_FAILED_MARKER_GENERATION" \
    "$control_root" || return 1
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" || return 1
  rm -rf "$candidate_state" || return 1
  vp_worker_redis_marker_require_secret_manifest \
    "$control_root" "$rollback_generation" || return 1
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$rollback_generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE"
}

vp_commit_worker_redis_marker_controls() {
  if [[ "$VP_WORKER_REDIS_MARKER_CONTROL_PREPARED" != true ]]; then
    return 0
  fi
  local control_root
  control_root="$(vp_worker_redis_marker_control_root)" || return 1
  control_root="$(
    vp_python_worker_prepare_controlled_directory "$control_root"
  )" || return 1
  vp_python_worker_prepare_controlled_directory \
    "$control_root/roles" >/dev/null || return 1
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" || return 1
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
}

vp_worker_admission_abort_failed_preapply() {
  if [[ "${VP_WORKER_ADMISSION_TRANSACTION_PREPARING:-false}" != true ]]; then
    return 0
  fi
  vp_worker_admission_abort_transaction preparing_failed
}

vp_apply_app_services() {
  local api="$1"
  local frontend="$2"
  local backend="$3"
  local channelops_runner="$4"
  local ffmpeg_go="$5"
  local python_worker="$6"

  VP_APP_ATTEMPTED_SERVICES=""
  VP_BACKEND_MIGRATION_APPLIED=false
  if [[ "${UPDATE_SERVICES:-1}" -ne 0 ]]; then
    vp_worker_admission_transition_to FORWARD_APPLYING || return 1
  fi
  vp_update_app_runtime_service vp-api-swarm "$api" stop-first || return 1
  http_health vp-api "http://$VP_RUNTIME_HOST:18080/health" || return 1
  vp_update_app_runtime_service vp-frontend-swarm "$frontend" stop-first || return 1
  http_health vp-frontend "http://$VP_RUNTIME_HOST:3001/" || return 1
  vp_worker_admission_advance_migration_state pending applying || return 1
  vp_run_worker_registration_migration "$backend" || return 1
  vp_worker_admission_advance_migration_state applying applied || return 1
  VP_BACKEND_MIGRATION_APPLIED=true
  vp_require_channelops_migration_head "$backend" || return 1
  vp_update_app_runtime_service vp-autoflow-api-swarm "$backend" start-first || return 1
  vp_prepare_worker_redis_marker_controls "$python_worker" || return 1
  vp_prepare_worker_admission "$python_worker" "$ffmpeg_go" || return 1
  vp_install_staging_object_janitor "$python_worker" || return 1
  vp_run_staging_object_janitor_once || return 1
  if [[ "${UPDATE_SERVICES:-1}" -ne 0 ]]; then
    vp_worker_admission_record_janitor_service || return 1
  fi

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    vp-ffmpeg-worker-go-swarm || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_update_app_runtime_service \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first || return 1
  vp_worker_admission_advance_live_worker_stage \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go" \
    prepared applied || return 1
  vp_require_worker_deployment_ready \
    vp-ffmpeg-worker-go-swarm || return 1
  vp_worker_admission_advance_live_worker_stage \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go" \
    applied verified || return 1

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_deploy_python_worker "$python_worker" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_PYTHON_WORKER_SERVICE" "$python_worker" \
    prepared applied || return 1
  vp_require_worker_deployment_ready \
    "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_PYTHON_WORKER_SERVICE" "$python_worker" \
    applied verified || return 1

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_VISION_WORKER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_VISION_WORKER_SERVICE" || return 1
  vp_deploy_vision_worker "$python_worker" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_VISION_WORKER_SERVICE" "$python_worker" \
    prepared applied || return 1
  vp_require_worker_deployment_ready \
    "$VP_VISION_WORKER_SERVICE" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_VISION_WORKER_SERVICE" "$python_worker" \
    applied verified || return 1
  if [[ "$VP_VISION_CUTOVER_REQUIRED" == true ]]; then
    if ! vp_run_vision_cutover_job final-safety "$python_worker"; then
      echo "final vision cutover gate failed; legacy worker remains active" >&2
      return 1
    fi
    log "final vision cutover gate verified immediately before retirement"
    vp_retire_legacy_vision_worker || return 1
    vp_reconcile_vision_consumers "$python_worker" || return 1
  fi

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_PUBLISHER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PUBLISHER_SERVICE" || return 1
  vp_deploy_publisher "$python_worker" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_PUBLISHER_SERVICE" "$python_worker" \
    prepared applied || return 1
  vp_require_worker_deployment_ready \
    "$VP_PUBLISHER_SERVICE" || return 1
  vp_worker_admission_advance_live_worker_stage \
    "$VP_PUBLISHER_SERVICE" "$python_worker" \
    applied verified || return 1

  vp_update_app_runtime_service vp-event-outbox-relay-swarm "$backend" start-first || return 1
  vp_update_app_runtime_service \
    vp-channel-agent-runner-swarm "$channelops_runner" stop-first || return 1

  local service
  for service in $VP_APP_SERVICES; do
    swarm_service_running "$service" || return 1
  done
  vp_install_soak_watch || return 1
  if [[ "${UPDATE_SERVICES:-1}" -ne 0 ]]; then
    vp_worker_admission_transition_to FORWARD_VERIFIED || return 1
    vp_worker_admission_promote_phase PROMOTE_WORKERS || return 1
    vp_worker_admission_promote_phase PROMOTE_MARKER || return 1
    vp_worker_admission_promote_phase PROMOTE_CONTROL || return 1
    vp_worker_admission_finish_transaction succeeded
  fi
}

_vp_deploy_vp_app_services_locked() {
  if ! vp_vision_cutover_required "${6:-}"; then
    vp_worker_admission_abort_failed_preapply || return 1
    return 1
  fi
  case "$VP_VISION_CUTOVER_REQUIRED" in
    true)
      if ! vp_require_vision_cutover_safe "${6:-}"; then
        vp_worker_admission_abort_failed_preapply || return 1
        return 1
      fi
      ;;
    false)
      ;;
    *)
      echo "invalid vision cutover state" >&2
      vp_worker_admission_abort_failed_preapply || return 1
      return 1
      ;;
  esac

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_apply_app_services "$@" || return 1
    printf '%s\n' "$VP_APP_SERVICES"
    return 0
  fi

  local snapshots
  if ! snapshots="$(vp_capture_app_snapshots)"; then
    vp_worker_admission_abort_failed_preapply || return 1
    return 1
  fi
  if ! vp_worker_admission_capture_baseline "$snapshots"; then
    echo "VideoProcess baseline journal capture failed" >&2
    vp_worker_admission_abort_failed_preapply || return 1
    return 1
  fi
  if ! vp_apply_app_services "$@"; then
    local process_candidate_records=""
    if ! process_candidate_records="$(
      vp_worker_admission_process_candidate_service_records \
        "$VP_APP_ATTEMPTED_SERVICES"
    )"; then
      echo "VideoProcess process-local candidate identity capture failed" >&2
      return 1
    fi
    if ! vp_worker_admission_capture_failed_forward \
      "$VP_APP_ATTEMPTED_SERVICES"; then
      echo "VideoProcess failed-forward journal capture failed" >&2
      return 1
    fi
    if ! vp_worker_admission_hydrate_recovery_context; then
      echo "VideoProcess failed-forward recovery context could not be hydrated" >&2
      return 1
    fi
    snapshots="$VP_WORKER_ADMISSION_RECOVERY_SNAPSHOTS"
    VP_APP_ATTEMPTED_SERVICES="$VP_WORKER_ADMISSION_RECOVERY_ATTEMPTED_SERVICES"
    local failed_candidate_records=""
    local candidate_capture_ok=true
    if [[ -n "$VP_WORKER_ADMISSION_CANDIDATE_SERVICES" ]] \
      && ! failed_candidate_records="$(
        vp_worker_admission_candidate_records
      )"; then
      candidate_capture_ok=false
      echo "worker admission candidate state could not be captured" >&2
    fi
    log "VideoProcess service apply failed; restoring prior images with fresh admission"
    local preserve_incomplete=false
    if [[ "$candidate_capture_ok" != true ]]; then
      preserve_incomplete=true
    fi
    local restore_ok=true
    if ! vp_restore_worker_admission_transaction \
      "$snapshots" "$VP_APP_ATTEMPTED_SERVICES" \
      "$failed_candidate_records" "$preserve_incomplete" \
      "$process_candidate_records"; then
      restore_ok=false
      echo "VideoProcess image restore did not fully converge" >&2
    fi
    return 1
  fi
  printf '%s\n' "$VP_APP_SERVICES"
}

deploy_vp_app_services() {
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
    local validation_status=0
    if ! vp_validate_deploy_config "${6:-}"; then
      validation_status=1
    elif _vp_deploy_vp_app_services_locked "$@"; then
      validation_status=0
    else
      validation_status=$?
    fi
    VP_WORKER_DATABASE_CREDENTIAL_RECORDS=""
    return "$validation_status"
  fi

  local caller_hup_trap
  local caller_int_trap
  local caller_term_trap
  caller_hup_trap="$(trap -p HUP)"
  caller_int_trap="$(trap -p INT)"
  caller_term_trap="$(trap -p TERM)"
  VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE=true
  VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS=0
  trap 'vp_worker_admission_record_deploy_signal 129' HUP
  trap 'vp_worker_admission_record_deploy_signal 130' INT
  trap 'vp_worker_admission_record_deploy_signal 143' TERM

  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  local deploy_status=0
  local release_status=0
  local lock_acquired=false
  local admission_root=""
  if vp_worker_admission_raise_if_signaled; then
    admission_root="$(vp_worker_admission_root)" || deploy_status=1
  else
    deploy_status=$?
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    admission_root="$(
      vp_python_worker_prepare_controlled_directory "$admission_root"
    )" || deploy_status=1
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if vp_worker_admission_raise_if_signaled; then
      :
    else
      deploy_status=$?
    fi
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if vp_worker_admission_lock_acquire "$admission_root"; then
      lock_acquired=true
    else
      deploy_status=1
    fi
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if vp_worker_admission_raise_if_signaled; then
      :
    else
      deploy_status=$?
    fi
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if ! vp_worker_admission_require_stage1_entry_state pre-reconcile; then
      deploy_status=1
    fi
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if ! vp_reconcile_worker_admission_transaction; then
      deploy_status=1
    fi
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if ! vp_worker_admission_require_stage1_entry_state; then
      deploy_status=1
    fi
  fi
  if vp_worker_admission_raise_if_signaled; then
    :
  else
    deploy_status=$?
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if ! vp_validate_deploy_config "${6:-}"; then
      deploy_status=1
    fi
  fi
  if vp_worker_admission_raise_if_signaled; then
    :
  else
    deploy_status=$?
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if ! vp_worker_admission_prepare_transaction \
      "${3:-}" "${5:-}" "${6:-}"; then
      deploy_status=1
    fi
  fi
  if vp_worker_admission_raise_if_signaled; then
    :
  else
    deploy_status=$?
  fi
  if [[ "$deploy_status" -eq 0 ]]; then
    if _vp_deploy_vp_app_services_locked "$@"; then
      deploy_status=0
    else
      deploy_status=$?
    fi
  fi
  if vp_worker_admission_raise_if_signaled; then
    :
  else
    deploy_status=$?
  fi

  VP_WORKER_ADMISSION_TRANSACTION_PREPARING=false
  if [[ "$lock_acquired" == true ]]; then
    vp_worker_admission_lock_release || release_status=1
  fi
  vp_python_worker_restore_trap "$caller_hup_trap" HUP
  vp_python_worker_restore_trap "$caller_int_trap" INT
  vp_python_worker_restore_trap "$caller_term_trap" TERM
  local deploy_signal_status="$VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS"
  VP_WORKER_ADMISSION_DEPLOY_SIGNAL_ACTIVE=false
  VP_WORKER_ADMISSION_DEPLOY_SIGNAL_STATUS=0
  VP_WORKER_DATABASE_CREDENTIAL_RECORDS=""
  if [[ "$deploy_signal_status" -ne 0 ]]; then
    return "$deploy_signal_status"
  fi
  [[ "$deploy_status" -eq 0 && "$release_status" -eq 0 ]]
}

vp_deploy_single_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"

  vp_validate_topology || return 1
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_update_runtime_service "$service" "$image" "$order" || return 1
    swarm_service_running "$service" || return 1
    printf '%s\n' "$service"
    return 0
  fi

  local baseline_image
  baseline_image="$(vp_service_values "$service" '{{.Spec.TaskTemplate.ContainerSpec.Image}}')" \
    || return 1
  if [[ -z "$baseline_image" ]]; then
    echo "missing current image for VideoProcess service: $service" >&2
    return 1
  fi

  local candidate_update_status=0
  if vp_update_runtime_service "$service" "$image" "$order"; then
    if swarm_service_running "$service"; then
      printf '%s\n' "$service"
      return 0
    fi
  else
    candidate_update_status=$?
    if [[ "$candidate_update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
      return 1
    fi
  fi

  log "restore $service -> $baseline_image with dedicated VP placement"
  if ! vp_update_runtime_service "$service" "$baseline_image" stop-first; then
    echo "VideoProcess image restore did not converge for $service" >&2
  fi
  return 1
}

deploy_feature_aggregator_services() {
  vp_deploy_single_runtime_service \
    vp-feature-aggregator-swarm "$1" start-first
}

vp_pds_container_snapshot() {
  remote_sh "$VP_RUNTIME_HOST" /bin/sh -s -- "$VP_PDS_SERVICE" <<'REMOTE'
set -eu
PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
export PATH

service="${1:-}"
if [ "$service" != "vp-pds-swarm" ]; then
  exit 10
fi

container_ids="$(
  docker container ls \
    --filter "label=com.docker.swarm.service.name=$service" \
    --filter status=running \
    --format '{{.ID}}' \
    2>/dev/null
)" || exit 11
container_count="$(
  printf '%s\n' "$container_ids" | awk 'NF { count++ } END { print count+0 }'
)" || exit 11
if [ "$container_count" -ne 1 ]; then
  printf 'pending|container_set\n'
  exit 0
fi
case "$container_ids" in
  ''|*[!0-9a-f]*)
    exit 12
    ;;
esac
container_id_length="${#container_ids}"
if [ "$container_id_length" -lt 12 ] || [ "$container_id_length" -gt 64 ]; then
  exit 12
fi

container_data="$(
  docker container inspect \
    --format '{{.Config.Image}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{if .Config.Healthcheck}}{{json .Config.Healthcheck.Test}}|{{.Config.Healthcheck.Interval}}|{{.Config.Healthcheck.Timeout}}|{{.Config.Healthcheck.StartPeriod}}|{{.Config.Healthcheck.Retries}}{{else}}none|0s|0s|0s|0{{end}}{{println}}{{range .Config.Env}}{{println .}}{{end}}' \
    "$container_ids" \
    2>/dev/null
)" || {
  printf 'pending|container_set\n'
  exit 0
}
container_snapshot="$(printf '%s\n' "$container_data" | sed -n '1p')"
container_env="$(printf '%s\n' "$container_data" | sed -n '2,$p')"
http_addr="$(
  printf '%s\n' "$container_env" \
    | awk -F= '$1 == "PDS_HTTP_ADDR" { count++; value=substr($0, index($0, "=") + 1) } END { if (count == 1) print value; else exit 1 }'
)" || exit 14
if [ "$http_addr" != ":8080" ]; then
  exit 15
fi
case "$container_snapshot" in
  *'|'*)
    image="${container_snapshot%%|*}"
    health_fields="${container_snapshot#*|}"
    ;;
  *)
    exit 16
    ;;
esac
printf '%s|:8080|%s\n' "$image" "$health_fields"
REMOTE
}

vp_require_pds_ready() {
  local expected_image="$1"

  vp_validate_topology || return 1
  swarm_service_running "$VP_PDS_SERVICE" || return 1
  vp_require_service_node "$VP_PDS_SERVICE" "$VP_RUNTIME_NODE" || return 1

  local attempt
  local snapshot
  local actual_image
  local actual_http_addr
  local health
  local health_test
  local health_interval
  local health_timeout
  local health_start_period
  local health_retries
  local extra
  for ((attempt = 1; attempt <= 18; attempt++)); do
    if ! snapshot="$(vp_pds_container_snapshot 2>/dev/null)"; then
      echo "PDS container inspection failed: $VP_PDS_SERVICE" >&2
      return 1
    fi
    if [[ "$snapshot" == "pending|container_set" ]]; then
      if [[ "$attempt" -lt 18 ]]; then
        sleep 5 || {
          echo "PDS readiness wait failed: $VP_PDS_SERVICE" >&2
          return 1
        }
        continue
      fi
      break
    fi
    if [[ -z "$snapshot" || "$snapshot" == *$'\n'* ]]; then
      echo "PDS container inspection returned invalid data: $VP_PDS_SERVICE" >&2
      return 1
    fi

    IFS='|' read -r \
      actual_image \
      actual_http_addr \
      health \
      health_test \
      health_interval \
      health_timeout \
      health_start_period \
      health_retries \
      extra <<<"$snapshot"
    if [[ -n "$extra" \
      || -z "$actual_image" \
      || -z "$actual_http_addr" \
      || -z "$health" \
      || -z "$health_test" \
      || -z "$health_interval" \
      || -z "$health_timeout" \
      || -z "$health_start_period" \
      || -z "$health_retries" ]]; then
      echo "PDS container inspection returned invalid data: $VP_PDS_SERVICE" >&2
      return 1
    fi
    if [[ "$actual_image" != "$expected_image" \
      || "$actual_http_addr" != "$VP_PDS_HTTP_ADDR" \
      || "$health_test" != "$VP_PDS_HEALTH_TEST" \
      || "$health_interval" != "10s" \
      || "$health_timeout" != "3s" \
      || "$health_start_period" != "10s" \
      || "$health_retries" != "6" ]]; then
      echo "PDS container contract mismatch: $VP_PDS_SERVICE" >&2
      return 1
    fi
    case "$health" in
      healthy)
        log "PDS readiness passed: $VP_PDS_SERVICE"
        return 0
        ;;
      starting)
        if [[ "$attempt" -lt 18 ]]; then
          sleep 5 || {
            echo "PDS readiness wait failed: $VP_PDS_SERVICE" >&2
            return 1
          }
          continue
        fi
        ;;
      *)
        echo "PDS container is not healthy: $VP_PDS_SERVICE" >&2
        return 1
        ;;
    esac
  done

  echo "PDS readiness deadline exceeded: $VP_PDS_SERVICE" >&2
  return 1
}

deploy_pds_services() {
  local image="$1"

  vp_validate_topology || return 1
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_update_runtime_service "$VP_PDS_SERVICE" "$image" start-first || return 1
    swarm_service_running "$VP_PDS_SERVICE" || return 1
    printf '%s\n' "$VP_PDS_SERVICE"
    return 0
  fi

  local baseline_image
  baseline_image="$(
    vp_service_values "$VP_PDS_SERVICE" \
      '{{.Spec.TaskTemplate.ContainerSpec.Image}}'
  )" || return 1
  if [[ -z "$baseline_image" ]]; then
    echo "missing current image for VideoProcess service: $VP_PDS_SERVICE" >&2
    return 1
  fi

  local candidate_update_status=0
  if vp_update_runtime_service "$VP_PDS_SERVICE" "$image" start-first; then
    if vp_require_pds_ready "$image"; then
      printf '%s\n' "$VP_PDS_SERVICE"
      return 0
    fi
  else
    candidate_update_status=$?
    if [[ "$candidate_update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
      return 1
    fi
  fi

  log "restore $VP_PDS_SERVICE -> $baseline_image with dedicated VP placement"
  if ! vp_update_runtime_service "$VP_PDS_SERVICE" "$baseline_image" stop-first; then
    echo "PDS image restore did not converge" >&2
    return 1
  fi
  if ! vp_require_pds_ready "$baseline_image"; then
    echo "PDS image restore did not become ready" >&2
  fi
  return 1
}
