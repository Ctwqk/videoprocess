#!/usr/bin/env bash

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "deploy-sync-extension.sh must be sourced by deploy-github-sync.sh" >&2
  exit 2
fi

: "${REPO_ROOT:?REPO_ROOT must be set by deploy-github-sync.sh}"

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
VP_VISION_CUTOVER_REQUIRED=false
VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION=""
VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE=""
VP_WORKER_REDIS_MARKER_PRIOR_GENERATION=""
VP_WORKER_REDIS_MARKER_PRIOR_IMAGE=""
VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET=""
VP_WORKER_REDIS_MARKER_MANAGED_STATE=""
VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
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
VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION=""
VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE=""

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

vp_validate_deploy_config() {
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
  if [[ -n "$missing" ]]; then
    echo "missing required VideoProcess deploy settings:$missing" >&2
    return 1
  fi
  vp_worker_admission_required_file \
    "${VP_WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE:-}" \
    "worker deploy-migrator database URL file" >/dev/null || return 1
  vp_worker_admission_required_file \
    "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
    "worker deploy-read database URL file" >/dev/null || return 1
  vp_worker_admission_required_file \
    "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
    "worker control-role owner database URL file" >/dev/null || return 1
  vp_worker_admission_required_file \
    "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
    "worker runtime-role owner database URL file" >/dev/null || return 1
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
import os
import re
import secrets
import stat
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


def ensure_run_root(
    parent_path: Path,
    name: str,
    uid: int,
    gid: int,
) -> None:
    if name != "vp-worker-admission":
        fail()
    before = parent_path.lstat()
    if (
        not stat.S_ISDIR(before.st_mode)
        or before.st_uid != uid
        or before.st_gid != gid
        or stat.S_IMODE(before.st_mode) & 0o022
    ):
        fail()
    parent = os.open(parent_path, directory_flags())
    try:
        opened = os.fstat(parent)
        if identity(before) != identity(opened):
            fail()
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent)
        except FileExistsError:
            pass
        child = os.open(name, directory_flags(), dir_fd=parent)
        try:
            child_metadata = os.fstat(child)
            require_directory(child_metadata, uid, gid)
            child_path = parent_path / name
            if identity(child_metadata) != identity(child_path.lstat()):
                fail()
        finally:
            os.close(child)
        os.fsync(parent)
        current_parent = os.fstat(parent)
        current_path = parent_path.lstat()
        if (
            not stat.S_ISDIR(current_parent.st_mode)
            or current_parent.st_uid != uid
            or current_parent.st_gid != gid
            or stat.S_IMODE(current_parent.st_mode) & 0o022
            or identity(current_parent)[:5] != identity(current_path)[:5]
            or identity(opened)[:5] != identity(current_parent)[:5]
        ):
            fail()
        print(parent_path / name)
    finally:
        os.close(parent)


def create_run_dir(root: Path, uid: int, gid: int) -> None:
    root_descriptor, root_metadata = open_exact_directory(root, uid, gid)
    try:
        name = "one-shot-runs"
        try:
            os.mkdir(name, mode=0o700, dir_fd=root_descriptor)
        except FileExistsError:
            pass
        parent = os.open(name, directory_flags(), dir_fd=root_descriptor)
        try:
            parent_metadata = os.fstat(parent)
            require_directory(parent_metadata, uid, gid)
            parent_path = root / name
            if identity(parent_metadata) != identity(parent_path.lstat()):
                fail()
            for _attempt in range(32):
                run_name = f"run.{secrets.token_hex(16)}"
                try:
                    os.mkdir(run_name, mode=0o700, dir_fd=parent)
                except FileExistsError:
                    continue
                run_metadata = os.stat(
                    run_name,
                    dir_fd=parent,
                    follow_symlinks=False,
                )
                require_directory(run_metadata, uid, gid)
                os.fsync(parent)
                if identity(root_metadata)[:4] != identity(root.lstat())[:4]:
                    os.rmdir(run_name, dir_fd=parent)
                    fail()
                print(parent_path / run_name)
                return
            fail()
        finally:
            os.close(parent)
    finally:
        os.close(root_descriptor)


def stage_file(
    source: Path,
    destination: Path,
    uid: int,
    gid: int,
    mode: int,
) -> None:
    source_descriptor, source_metadata = capture_file(
        source,
        uid,
        gid,
        mode,
    )
    destination_parent = exact_path(str(destination.parent))
    parent_descriptor, _parent_metadata = open_exact_directory(
        destination_parent,
        uid,
        gid,
    )
    created = False
    try:
        payload = bytearray()
        while True:
            chunk = os.read(source_descriptor, 65536)
            if not chunk:
                break
            payload.extend(chunk)
            if len(payload) > 1048576:
                fail()
        if identity(source_metadata) != identity(os.fstat(source_descriptor)):
            fail()
        if identity(source_metadata) != identity(source.lstat()):
            fail()
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        output = os.open(
            destination.name,
            flags,
            mode,
            dir_fd=parent_descriptor,
        )
        created = True
        try:
            view = memoryview(payload)
            while view:
                written = os.write(output, view)
                if written < 1:
                    fail()
                view = view[written:]
            os.fchmod(output, mode)
            os.fsync(output)
            require_file(os.fstat(output), uid, gid, mode)
        finally:
            os.close(output)
        os.fsync(parent_descriptor)
        if identity(source_metadata) != identity(source.lstat()):
            fail()
        print(",".join(str(value) for value in identity(source_metadata)))
    except Exception:
        if created:
            try:
                os.unlink(destination.name, dir_fd=parent_descriptor)
            except OSError:
                pass
        raise
    finally:
        os.close(parent_descriptor)
        os.close(source_descriptor)


def prepare_directory(
    path: Path,
    uid: int,
    gid: int,
    sentinel_name: str,
    marker: str,
) -> None:
    if not re.fullmatch(r"\.vp-python-worker-bind-[0-9a-f]{32}", sentinel_name):
        fail()
    if (
        not marker
        or len(marker) > 512
        or "\n" in marker
        or "\r" in marker
        or "|" in marker
    ):
        fail()
    descriptor, metadata = open_exact_directory(path, uid, gid)
    created = False
    try:
        scan_tree(descriptor, metadata.st_dev, uid, gid)
        output = os.open(
            sentinel_name,
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
        created = True
        try:
            encoded = marker.encode("ascii")
            if os.write(output, encoded) != len(encoded):
                fail()
            os.fchmod(output, 0o400)
            os.fsync(output)
            require_file(os.fstat(output), uid, gid, 0o400)
        finally:
            os.close(output)
        os.fsync(descriptor)
        current = path.lstat()
        require_directory(current, uid, gid)
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
        ):
            fail()
        print(f"{metadata.st_dev},{metadata.st_ino}")
    except Exception:
        if created:
            try:
                os.unlink(sentinel_name, dir_fd=descriptor)
            except OSError:
                pass
        raise
    finally:
        os.close(descriptor)


def finalize_directory(
    path: Path,
    uid: int,
    gid: int,
    sentinel_name: str,
    marker: str,
    record: str,
) -> None:
    descriptor, metadata = open_exact_directory(path, uid, gid)
    try:
        if record != f"{metadata.st_dev},{metadata.st_ino}":
            fail()
        scan_tree(descriptor, metadata.st_dev, uid, gid)
        sentinel = os.open(
            sentinel_name,
            file_flags(),
            dir_fd=descriptor,
        )
        try:
            sentinel_metadata = os.fstat(sentinel)
            require_file(sentinel_metadata, uid, gid, 0o400)
            payload = os.read(sentinel, 1024)
            if payload != marker.encode("ascii") or os.read(sentinel, 1):
                fail()
            path_metadata = os.stat(
                sentinel_name,
                dir_fd=descriptor,
                follow_symlinks=False,
            )
            if identity(path_metadata) != identity(sentinel_metadata):
                fail()
        finally:
            os.close(sentinel)
        os.unlink(sentinel_name, dir_fd=descriptor)
        os.fsync(descriptor)
        current = path.lstat()
        require_directory(current, uid, gid)
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
        ):
            fail()
    finally:
        os.close(descriptor)


def cleanup_run_dir(path: Path, uid: int, gid: int) -> None:
    if not re.fullmatch(r"run\.[0-9a-f]{32}", path.name):
        fail()
    parent_path = exact_path(str(path.parent))
    if parent_path.name != "one-shot-runs":
        fail()
    parent, _parent_metadata = open_exact_directory(parent_path, uid, gid)
    try:
        child = os.open(path.name, directory_flags(), dir_fd=parent)
        try:
            child_metadata = os.fstat(child)
            require_directory(child_metadata, uid, gid)
            if identity(child_metadata) != identity(path.lstat()):
                fail()
            for name in os.listdir(child):
                if not re.fullmatch(
                    r"(bootstrap-secret|bind-file-[0-9]+)",
                    name,
                ):
                    fail()
                metadata = os.stat(
                    name,
                    dir_fd=child,
                    follow_symlinks=False,
                )
                mode = stat.S_IMODE(metadata.st_mode)
                if mode not in {0o400, 0o600}:
                    fail()
                require_file(metadata, uid, gid, mode)
                os.unlink(name, dir_fd=child)
            os.fsync(child)
        finally:
            os.close(child)
        current = os.stat(
            path.name,
            dir_fd=parent,
            follow_symlinks=False,
        )
        if (
            current.st_dev != child_metadata.st_dev
            or current.st_ino != child_metadata.st_ino
        ):
            fail()
        os.rmdir(path.name, dir_fd=parent)
        os.fsync(parent)
    finally:
        os.close(parent)


try:
    action = sys.argv[1]
    if action == "create-run-dir" and len(sys.argv) == 5:
        create_run_dir(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
        )
    elif action == "ensure-run-root" and len(sys.argv) == 6:
        ensure_run_root(
            exact_path(sys.argv[2]),
            sys.argv[3],
            numeric(sys.argv[4]),
            numeric(sys.argv[5]),
        )
    elif action == "stage-file" and len(sys.argv) == 7:
        mode = int(sys.argv[6], 8)
        if mode not in {0o400, 0o600}:
            fail()
        stage_file(
            exact_path(sys.argv[2]),
            Path(sys.argv[3]),
            numeric(sys.argv[4]),
            numeric(sys.argv[5]),
            mode,
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
    elif action == "prepare-directory" and len(sys.argv) == 8:
        prepare_directory(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
            sys.argv[5],
            sys.argv[6],
        )
    elif action == "finalize-directory" and len(sys.argv) == 9:
        finalize_directory(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
            sys.argv[5],
            sys.argv[6],
            sys.argv[7],
        )
    elif action == "cleanup-run-dir" and len(sys.argv) == 5:
        cleanup_run_dir(
            exact_path(sys.argv[2]),
            numeric(sys.argv[3]),
            numeric(sys.argv[4]),
        )
    else:
        fail()
except Exception:
    sys.exit(1)
PY
}

vp_run_python_worker_container() {
  local image="$1"
  local secret_source="$2"
  local secret_target="$3"
  local prepare_dirs="$4"
  shift 4

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
  local bind_file_modes=()
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
        local file_mode=""
        case "$bind_target" in
          /control-state|/runtime-state|/requests)
            if [[ ",$prepare_dirs," == *",$bind_target,"* ]]; then
              [[ -z "$readonly_suffix" ]] || return 1
            else
              [[ "$readonly_suffix" == ,readonly ]] || return 1
            fi
            ;;
          /run/control/upsert.json)
            [[ "$readonly_suffix" == ,readonly ]] || return 1
            file_mode=0600
            ;;
          *)
            return 1
            ;;
        esac
        bind_sources+=("$bind_source")
        bind_targets+=("$bind_target")
        bind_readonly+=("$readonly_suffix")
        bind_file_modes+=("$file_mode")
        seen_targets+="$bind_target|"
        shift 2
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

  local guard_names=()
  local guard_markers=()
  local guard_records=()
  local guard_indices=""
  local prepared_guard_indices=""
  local bind_manifest=""
  local run_dir=""
  local admission_root
  local staged_secret=""
  local secret_record=""
  local staged_sources=()
  local staged_records=()
  local needs_run_dir=false
  local index
  [[ -n "$secret_source" ]] && needs_run_dir=true
  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    [[ -n "${bind_file_modes[$index]}" ]] && needs_run_dir=true
  done
  if [[ "$needs_run_dir" == true ]]; then
    admission_root="$(vp_worker_admission_root)" || return 1
    local admission_parent="${admission_root%/*}"
    local admission_name="${admission_root##*/}"
    admission_root="$(
      vp_python_worker_host_guard \
        ensure-run-root "$admission_parent" "$admission_name" \
        "$caller_uid" "$caller_gid"
    )" || return 1
    run_dir="$(
      vp_python_worker_host_guard \
        create-run-dir "$admission_root" "$caller_uid" "$caller_gid"
    )" || return 1
  fi
  if [[ -n "$secret_source" ]]; then
    staged_secret="$run_dir/bootstrap-secret"
    secret_record="$(
      vp_python_worker_host_guard \
        stage-file "$secret_source" "$staged_secret" \
        "$caller_uid" "$caller_gid" 0400
    )" || {
      vp_python_worker_host_guard \
        cleanup-run-dir "$run_dir" "$caller_uid" "$caller_gid" \
        >/dev/null 2>&1 || true
      return 1
    }
  fi

  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    [[ -n "${bind_file_modes[$index]}" ]] || continue
    local staged_record
    staged_record="$(
      vp_python_worker_host_guard \
        stage-file "${bind_sources[$index]}" "$run_dir/bind-file-$index" \
        "$caller_uid" "$caller_gid" "${bind_file_modes[$index]}"
    )" || {
      vp_python_worker_host_guard \
        cleanup-run-dir "$run_dir" "$caller_uid" "$caller_gid" \
        >/dev/null 2>&1 || true
      return 1
    }
    staged_sources[$index]="${bind_sources[$index]}"
    staged_records[$index]="$staged_record"
  done

  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    [[ -z "${bind_file_modes[$index]}" ]] || continue
    local token
    token="$(python3 -c 'import secrets; print(secrets.token_hex(16))')" \
      || {
        if [[ -n "$run_dir" ]]; then
          vp_python_worker_host_guard \
            cleanup-run-dir "$run_dir" "$caller_uid" "$caller_gid" \
            >/dev/null 2>&1 || true
        fi
        return 1
      }
    guard_names[$index]=".vp-python-worker-bind-$token"
    guard_markers[$index]="vp-python-worker-bind-v1:$token:$caller_uid:$caller_gid"
    guard_indices+=" $index"
  done

  for index in $guard_indices; do
    local guard_record
    if ! guard_record="$(
      vp_python_worker_host_guard \
        prepare-directory \
        "${bind_sources[$index]}" \
        "$caller_uid" \
        "$caller_gid" \
        "${guard_names[$index]}" \
        "${guard_markers[$index]}" \
        "${bind_targets[$index]}"
    )"; then
      local cleanup_index
      for cleanup_index in $prepared_guard_indices; do
        vp_python_worker_host_guard \
          finalize-directory \
          "${bind_sources[$cleanup_index]}" \
          "$caller_uid" \
          "$caller_gid" \
          "${guard_names[$cleanup_index]}" \
          "${guard_markers[$cleanup_index]}" \
          "${guard_records[$cleanup_index]}" \
          "${bind_targets[$cleanup_index]}" >/dev/null 2>&1 || true
      done
      if [[ -n "$run_dir" ]]; then
        vp_python_worker_host_guard \
          cleanup-run-dir "$run_dir" "$caller_uid" "$caller_gid" \
          >/dev/null 2>&1 || true
      fi
      return 1
    fi
    guard_records[$index]="$guard_record"
    prepared_guard_indices+=" $index"
    bind_manifest+="${bind_manifest:+$'\n'}${bind_targets[$index]}|${guard_names[$index]}|${guard_markers[$index]}"
  done

  local run_args=(
    run
    --rm
    --user "$caller_uid:$caller_gid"
    --read-only
    --cap-drop ALL
    --security-opt no-new-privileges
    --tmpfs
    "/tmp:rw,nosuid,nodev,noexec,size=16777216,mode=1777"
    --tmpfs
    "/run/secrets:rw,nosuid,nodev,noexec,size=65536,mode=0700,uid=$caller_uid,gid=$caller_gid"
  )
  if (( ${#passthrough_args[@]} > 0 )); then
    run_args+=("${passthrough_args[@]}")
  fi
  if [[ -n "$staged_secret" ]]; then
    run_args+=(
      --mount
      "type=bind,src=$staged_secret,dst=/run/videoprocess-bootstrap-secret,readonly"
    )
  fi
  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    local effective_source="${bind_sources[$index]}"
    if [[ -n "${bind_file_modes[$index]}" ]]; then
      effective_source="$run_dir/bind-file-$index"
    fi
    run_args+=(
      --mount
      "type=bind,src=$effective_source,dst=${bind_targets[$index]}${bind_readonly[$index]}"
    )
  done

  local docker_status=0
  docker "${run_args[@]}" \
    --env "VP_PYTHON_WORKER_SECRET_TARGET=$secret_target" \
    --env "VP_PYTHON_WORKER_CALLER_UID=$caller_uid" \
    --env "VP_PYTHON_WORKER_CALLER_GID=$caller_gid" \
    --env "VP_PYTHON_WORKER_BIND_SENTINELS=$bind_manifest" \
    "$image" \
    /bin/bash -ceu '
      runtime_uid="${VP_PYTHON_WORKER_CALLER_UID:?}"
      runtime_gid="${VP_PYTHON_WORKER_CALLER_GID:?}"
      [[ "$runtime_uid" =~ ^[0-9]+$ && "$runtime_gid" =~ ^[0-9]+$ ]]
      [[ "$(id -u)" == "$runtime_uid" && "$(id -g)" == "$runtime_gid" ]]
      umask 077
      export HOME=/tmp/vp-python-worker-home
      mkdir -p "$HOME"
      chmod 0700 "$HOME"

      secret_target="${VP_PYTHON_WORKER_SECRET_TARGET:-}"
      if [[ -n "$secret_target" ]]; then
        [[ "$secret_target" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]
        [[ -f /run/videoprocess-bootstrap-secret \
          && ! -L /run/videoprocess-bootstrap-secret ]]
        [[ "$(stat -c "%a:%h" /run/videoprocess-bootstrap-secret)" \
          == "400:1" ]]
        [[ -r /run/videoprocess-bootstrap-secret ]]
        install \
          -m 0400 \
          /run/videoprocess-bootstrap-secret \
          "/run/secrets/$secret_target"
        [[ "$(stat -c "%u:%g:%a:%h" "/run/secrets/$secret_target")" \
          == "$runtime_uid:$runtime_gid:400:1" ]]
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
        [[ "$(stat -c "%u:%g:%a:%h" "$path/$sentinel")" \
          == "$runtime_uid:$runtime_gid:400:1" ]]
        [[ "$(<"$path/$sentinel")" == "$marker" ]]
      done <<<"${VP_PYTHON_WORKER_BIND_SENTINELS:-}"

      unset \
        VP_PYTHON_WORKER_SECRET_TARGET \
        VP_PYTHON_WORKER_CALLER_UID \
        VP_PYTHON_WORKER_CALLER_GID \
        VP_PYTHON_WORKER_BIND_SENTINELS
      exec "$@"
    ' vp-python-worker-bootstrap "${command[@]}" \
    || docker_status=$?

  local validation_status=0
  for ((index = 0; index < ${#bind_sources[@]}; index++)); do
    if [[ -z "${bind_file_modes[$index]}" ]]; then
      vp_python_worker_host_guard \
        finalize-directory \
        "${bind_sources[$index]}" \
        "$caller_uid" \
        "$caller_gid" \
        "${guard_names[$index]}" \
        "${guard_markers[$index]}" \
        "${guard_records[$index]}" \
        "${bind_targets[$index]}" >/dev/null \
        || validation_status=1
    elif [[ -n "${staged_sources[$index]:-}" ]]; then
      vp_python_worker_host_guard \
        verify-file \
        "${staged_sources[$index]}" \
        "$caller_uid" \
        "$caller_gid" \
        "${bind_file_modes[$index]}" \
        "${staged_records[$index]}" >/dev/null \
        || validation_status=1
    fi
  done
  if [[ -n "$secret_source" ]]; then
    vp_python_worker_host_guard \
      verify-file "$secret_source" "$caller_uid" "$caller_gid" 0400 \
      "$secret_record" >/dev/null || validation_status=1
  fi
  if [[ -n "$run_dir" ]]; then
    vp_python_worker_host_guard \
      cleanup-run-dir "$run_dir" "$caller_uid" "$caller_gid" >/dev/null \
      || validation_status=1
  fi
  [[ "$docker_status" -eq 0 && "$validation_status" -eq 0 ]]
}

vp_worker_admission_root() {
  if [[ -z "${ROOT:-}" || ! "$ROOT" = /* ]]; then
    return 1
  fi
  printf '%s\n' "$ROOT/state/vp-worker-admission"
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
    "VERSION=1" \
    "SERVICE=$service" \
    "COMMIT=$commit" \
    "IMAGE=$image" \
    "GENERATION=$generation" \
    "DATABASE_SECRET=$database_secret" \
    "ADMISSION_SECRET=$admission_secret" >"$temporary" \
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
      *) return 1 ;;
    esac
  done <"$path"
  [[ "$VP_WORKER_MANIFEST_VERSION" == 1 \
    && "$VP_WORKER_MANIFEST_SERVICE" == "$expected_service" \
    && "$VP_WORKER_MANIFEST_COMMIT" =~ ^[0-9a-f]{40}$ \
    && "$VP_WORKER_MANIFEST_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "$VP_WORKER_MANIFEST_GENERATION" =~ ^[1-9][0-9]*$ \
    && "$VP_WORKER_MANIFEST_DATABASE_SECRET" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
    && "$VP_WORKER_MANIFEST_ADMISSION_SECRET" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]
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
  local expected="$service|$generation|$purpose"
  if docker secret inspect "$secret_name" >/dev/null 2>&1; then
    local actual
    actual="$(
      docker secret inspect "$secret_name" \
        --format '{{index .Spec.Labels "vp.service"}}|{{index .Spec.Labels "vp.generation"}}|{{index .Spec.Labels "vp.purpose"}}'
    )" || return 1
    [[ "$actual" == "$expected" ]] || {
      echo "worker secret identity mismatch" >&2
      return 1
    }
    return 0
  fi
  docker secret create \
    --label "vp.service=$service" \
    --label "vp.generation=$generation" \
    --label "vp.purpose=$purpose" \
    "$secret_name" - <"$credential_file" >/dev/null
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
  local request_mount=()
  if [[ "${1:-}" == upsert ]]; then
    local request_file="${3:-}"
    [[ "$request_file" = /* && -f "$request_file" && ! -L "$request_file" \
      && "$(vp_worker_redis_marker_file_mode "$request_file")" == 600 ]] \
      || return 1
    request_mount=(
      --mount
      "type=bind,src=$request_file,dst=/run/control/upsert.json,readonly"
    )
    set -- upsert --request-file /run/control/upsert.json
  fi
  vp_run_python_worker_container \
    "$image" \
    "$operator_file" \
    worker-operator-database-url \
    - \
    --network "$VP_PIPELINE_NETWORK_ID" \
    "${request_mount[@]}" \
    --env WORKER_REGISTRATION_OPERATOR_DATABASE_URL_FILE=/run/secrets/worker-operator-database-url \
    -- \
    python -m app.services.worker_registration_operator_cli \
    "$@" >/dev/null
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

vp_worker_control_write_manifest() {
  local path="$1"
  local generation="$2"
  local image="$3"
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
      "VERSION=1" \
      "GENERATION=$generation" \
      "IMAGE=$image" \
      "OPERATOR_DATABASE_SECRET=$operator_secret" \
      "ORCHESTRATOR_DATABASE_SECRET=$orchestrator_secret" \
      "STAGING_DATABASE_SECRET=$staging_secret" \
      "STAGING_MINIO_ACCESS_SECRET=$staging_minio_access_secret" \
      "STAGING_MINIO_SECRET_SECRET=$staging_minio_secret_secret" \
      "WORKER_MINIO_ACCESS_SECRET=$worker_minio_access_secret" \
      "WORKER_MINIO_SECRET_SECRET=$worker_minio_secret_secret" \
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
  [[ "$VP_WORKER_CONTROL_MANIFEST_VERSION" == 1 \
    && "$VP_WORKER_CONTROL_MANIFEST_IMAGE" \
      =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${VP_WORKER_CONTROL_MANIFEST_GENERATION#c-}" \
      == "${VP_WORKER_CONTROL_MANIFEST_IMAGE##*:deploy-}"* \
    && "$actual" == "$expected" ]]
}

vp_worker_control_schedule_retirement() {
  local root="$1"
  local image="$2"
  local generation="$3"
  [[ "$root" = /* ]] || return 1
  local directory="$root/control-retirements"
  local journal="$directory/$generation.conf"
  if [[ -e "$journal" ]]; then
    vp_worker_control_read_manifest "$journal" || return 1
    [[ "$VP_WORKER_CONTROL_MANIFEST_GENERATION" == "$generation" \
      && "$VP_WORKER_CONTROL_MANIFEST_IMAGE" == "$image" ]]
    return
  fi
  vp_worker_control_write_manifest \
    "$journal" "$generation" "$image"
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
  vp_worker_control_read_manifest "$current" || {
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
    vp_worker_admission_required_file \
      "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker control-role owner database URL file"
  )" || return 1
  local generation="c-${commit:0:20}"
  local state="$root/control"
  mkdir -p "$state" || return 1
  chmod 0700 "$state" || return 1
  VP_WORKER_CONTROL_GENERATION="$generation"
  VP_WORKER_OPERATOR_DATABASE_SECRET="vp-wc-operator-$generation"
  VP_WORKER_ORCHESTRATOR_DATABASE_SECRET="vp-wc-orchestrator-$generation"
  VP_STAGING_JANITOR_DATABASE_SECRET="vp-wc-staging-$generation"
  VP_STAGING_JANITOR_MINIO_ACCESS_SECRET="vp-wc-minio-access-$generation"
  VP_STAGING_JANITOR_MINIO_SECRET_SECRET="vp-wc-minio-secret-$generation"
  VP_WORKER_MINIO_ACCESS_SECRET="vp-wc-worker-minio-access-$generation"
  VP_WORKER_MINIO_SECRET_SECRET="vp-wc-worker-minio-secret-$generation"
  VP_WORKER_CONTROL_PREPARED=true
  vp_worker_control_write_manifest \
    "$root/control-candidates/$generation.conf" \
    "$generation" "$image" || return 1
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

  vp_worker_admission_create_secret \
    "$VP_WORKER_OPERATOR_DATABASE_SECRET" \
    "$state/$generation/worker-registration-operator-database-url" \
    vp-worker-control "$generation" operator || return 1
  vp_worker_admission_create_secret \
    "$VP_WORKER_ORCHESTRATOR_DATABASE_SECRET" \
    "$state/$generation/worker-orchestrator-database-url" \
    vp-worker-control "$generation" orchestrator || return 1
  vp_worker_admission_create_secret \
    "$VP_STAGING_JANITOR_DATABASE_SECRET" \
    "$state/$generation/vp-staging-janitor-database-url" \
    vp-worker-control "$generation" staging-janitor || return 1
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
  vp_worker_admission_create_secret \
    "$VP_STAGING_JANITOR_MINIO_SECRET_SECRET" \
    "$minio_secret_file" vp-worker-control "$generation" \
    staging-minio-secret || return 1
  vp_worker_admission_create_secret \
    "$VP_WORKER_MINIO_ACCESS_SECRET" \
    "$minio_access_file" vp-worker-control "$generation" \
    worker-minio-access || return 1
  vp_worker_admission_create_secret \
    "$VP_WORKER_MINIO_SECRET_SECRET" \
    "$minio_secret_file" vp-worker-control "$generation" \
    worker-minio-secret || return 1
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
  local candidate_dir="$root/candidates/$namespace"
  local candidate="$candidate_dir/$kind.conf"
  local generation=""
  local database_secret=""
  local admission_secret=""
  if vp_worker_admission_read_manifest "$candidate" "$service" \
    && [[ "$VP_WORKER_MANIFEST_COMMIT" == "$commit" \
      && "$VP_WORKER_MANIFEST_IMAGE" == "$image" ]]; then
    generation="$VP_WORKER_MANIFEST_GENERATION"
    database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
    admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
  else
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
    vp_worker_admission_required_file \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker runtime-role owner database URL file"
  )" || return 1
  local runtime_state="$root/runtime"
  mkdir -p "$runtime_state" || return 1
  chmod 0700 "$runtime_state" || return 1
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

  local credential_dir="$runtime_state/$service/$generation"
  vp_worker_admission_create_secret \
    "$database_secret" "$credential_dir/worker-database-url" \
    "$service" "$generation" database || return 1
  vp_worker_admission_create_secret \
    "$admission_secret" "$credential_dir/worker-admission-token" \
    "$service" "$generation" admission || return 1
  vp_worker_admission_set_candidate \
    "$service" "$generation" "$database_secret" "$admission_secret" \
    || return 1

  local request_root="$root/requests/$service"
  local request_file="$request_root/$generation/upsert.json"
  mkdir -p "$request_root" || return 1
  chmod 0700 "$request_root" || return 1
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
  mkdir -p "$root" || return 1
  chmod 0700 "$root" || return 1
  VP_WORKER_ADMISSION_COMMIT="$commit"
  VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE="$commit"
  VP_WORKER_ADMISSION_CONTROL_IMAGE="$control_image"
  vp_worker_control_capture_prior "$root" || return 1
  vp_worker_control_cleanup_stale_candidates "$root" || return 1
  vp_worker_admission_prepare_control_roles \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_prepare_service \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_prepare_service \
    "$VP_PYTHON_WORKER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_prepare_service \
    "$VP_VISION_WORKER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
  vp_worker_admission_prepare_service \
    "$VP_PUBLISHER_SERVICE" "$control_image" \
    "$control_image" "$commit" "$root" || return 1
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
  vp_worker_admission_read_manifest "$candidate" "$service" || return 1
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
    vp_worker_admission_read_manifest "$candidate" "$service" || return 1
    printf '%s|%s|%s|%s\n' \
      "$service" \
      "$VP_WORKER_MANIFEST_GENERATION" \
      "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
      "$VP_WORKER_MANIFEST_ADMISSION_SECRET"
  done
}

vp_worker_admission_snapshot_image() {
  local snapshots="$1"
  local service="$2"
  printf '%s\n' "$snapshots" | awk -F'|' -v service="$service" '
    NF && NF != 2 { invalid=1 }
    $1 == service {
      count++
      image=$2
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
  local rollback_id
  rollback_id="$(vp_worker_admission_new_generation)" || return 1
  local namespace="rollback-$rollback_id"
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
  done
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
    vp_worker_admission_required_file \
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

vp_worker_admission_generation_state() {
  local service="$1"
  local generation="$2"
  vp_require_pipeline_network_identity || return 1
  local read_file
  read_file="$(
    vp_worker_admission_required_file \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  local payload
  payload="$(
    vp_run_python_worker_container \
      "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      "$read_file" \
      worker-deploy-read-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
      -- \
      python -m app.services.worker_deployment_cli \
        generation-state \
        --service-name "$service" \
        --generation "$generation"
  )" || return 1
  python3 -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
    if set(payload) != {
        "code",
        "generation",
        "grant_state",
        "service_name",
        "status",
    }:
        raise ValueError
    if (
        payload["status"] != "ok"
        or payload["code"] != "worker_deployment_generation_state"
        or payload["service_name"] != sys.argv[1]
        or payload["generation"] != int(sys.argv[2])
        or payload["grant_state"]
        not in {"absent", "pending", "active", "revoked"}
    ):
        raise ValueError
    print(payload["grant_state"])
except (TypeError, ValueError, json.JSONDecodeError):
    sys.exit(1)
' "$service" "$generation" <<<"$payload"
}

vp_worker_admission_retirement_ids() {
  local service="$1"
  local generation="$2"
  vp_require_pipeline_network_identity || return 1
  local read_file
  read_file="$(
    vp_worker_admission_required_file \
      "${VP_WORKER_DEPLOY_READ_DATABASE_URL_FILE:-}" \
      "worker deploy-read database URL file"
  )" || return 1
  local payload
  payload="$(
    vp_run_python_worker_container \
      "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      "$read_file" \
      worker-deploy-read-database-url \
      - \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env WORKER_DEPLOY_READ_DATABASE_URL_FILE=/run/secrets/worker-deploy-read-database-url \
      -- \
      python -m app.services.worker_deployment_cli \
        retirement-candidates \
        --service-name "$service" \
        --generation "$generation"
  )" || return 1
  RETIREMENT_PAYLOAD="$payload" python3 - <<'PY'
import json
import os
import sys
import uuid

try:
    payload = json.loads(os.environ["RETIREMENT_PAYLOAD"])
    if set(payload) != {
        "code",
        "generation",
        "registration_ids",
        "service_name",
        "status",
    }:
        raise ValueError
    if (
        payload["status"] != "ok"
        or payload["code"]
        != "worker_deployment_retirement_candidates"
        or not isinstance(payload["registration_ids"], list)
    ):
        raise ValueError
    for value in payload["registration_ids"]:
        parsed = uuid.UUID(value)
        if str(parsed) != value:
            raise ValueError
        print(value)
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    sys.exit(1)
PY
}

vp_worker_admission_retire_generation() {
  local service="$1"
  local generation="$2"
  local database_secret="$3"
  local admission_secret="$4"
  local root="$5"
  vp_require_pipeline_network_identity || return 1
  local operator_file="$root/control/$VP_WORKER_CONTROL_GENERATION/worker-registration-operator-database-url"
  local grant_state
  grant_state="$(
    vp_worker_admission_generation_state "$service" "$generation"
  )" || return 1
  local registration_ids
  registration_ids="$(
    vp_worker_admission_retirement_ids "$service" "$generation"
  )" || return 1
  local registration_id
  while IFS= read -r registration_id; do
    [[ -n "$registration_id" ]] || continue
    vp_worker_admission_operator \
      "$operator_file" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      revoke-registration \
      --service-name "$service" \
      --registration-id "$registration_id" \
      --reason replaced || return 1
  done <<<"$registration_ids"
  if [[ "$grant_state" != absent ]]; then
    vp_worker_admission_operator \
      "$operator_file" "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
      revoke-grant \
      --service-name "$service" \
      --generation "$generation" \
      --reason replaced || return 1
  fi

  local owner_file
  owner_file="$(
    vp_worker_admission_required_file \
      "${VP_WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker runtime-role owner database URL file"
  )" || return 1
  mkdir -p "$root/runtime" || return 1
  chmod 0700 "$root/runtime" || return 1
  vp_run_python_worker_container \
    "$VP_WORKER_ADMISSION_CONTROL_IMAGE" \
    "$owner_file" \
    worker-runtime-owner-database-url \
    /runtime-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$root/runtime,dst=/runtime-state" \
    --env WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-runtime-owner-database-url \
    -- \
    python -m app.services.worker_runtime_role_cli \
      revoke --service-name "$service" \
      --generation "$generation" \
      --state-dir /runtime-state >/dev/null || return 1
  if docker secret inspect "$database_secret" >/dev/null 2>&1; then
    docker secret rm "$database_secret" >/dev/null || return 1
  fi
  if docker secret inspect "$admission_secret" >/dev/null 2>&1; then
    docker secret rm "$admission_secret" >/dev/null || return 1
  fi
}

vp_worker_admission_retire_records() {
  local records="$1"
  local root="$2"
  [[ "$root" = /* ]] || return 1
  local seen=""
  local service
  local generation
  local database_secret
  local admission_secret
  local extra
  while IFS='|' read -r \
    service generation database_secret admission_secret extra; do
    [[ -n "$service" || -n "$generation" || -n "$database_secret" \
      || -n "$admission_secret" || -n "$extra" ]] || continue
    if [[ -n "$extra" \
      || ! "$generation" =~ ^[1-9][0-9]*$ \
      || ! "$database_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ \
      || ! "$admission_secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] \
      || ! vp_worker_admission_kind "$service" >/dev/null; then
      return 1
    fi
    case " $seen " in
      *" $service:$generation "*) return 1 ;;
    esac
    seen="${seen:+$seen }$service:$generation"
    vp_worker_admission_retire_generation \
      "$service" "$generation" "$database_secret" "$admission_secret" \
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
    records="$(<"$journal")"
    local service
    local generation
    local database_secret
    local admission_secret
    local extra
    while IFS='|' read -r \
      service generation database_secret admission_secret extra; do
      [[ -n "$service$generation$database_secret$admission_secret$extra" ]] \
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
    vp_worker_admission_read_manifest "$candidate" "$service" || return 1
    local candidate_generation="$VP_WORKER_MANIFEST_GENERATION"

    local current="$root/current/$kind.conf"
    local prior_generation=""
    local prior_database_secret=""
    local prior_admission_secret=""
    if vp_worker_admission_read_manifest "$current" "$service"; then
      prior_generation="$VP_WORKER_MANIFEST_GENERATION"
      prior_database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
      prior_admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
    elif [[ -e "$current" ]]; then
      echo "worker admission current manifest is invalid" >&2
      return 1
    fi

    if [[ -n "$prior_generation" \
      && "$prior_generation" != "$candidate_generation" ]]; then
      retirement_records="${retirement_records:+$retirement_records$'\n'}$service|$prior_generation|$prior_database_secret|$prior_admission_secret"
    fi
  done
  vp_worker_admission_write_retirement_journal \
    "$retirement_journal" "$retirement_records" || return 1

  for service in $VP_WORKER_ADMISSION_CANDIDATE_SERVICES; do
    local kind
    kind="$(vp_worker_admission_kind "$service")" || return 1
    local candidate="$root/candidates/$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE/$kind.conf"
    vp_worker_admission_read_manifest "$candidate" "$service" || return 1
    local candidate_commit="$VP_WORKER_MANIFEST_COMMIT"
    local candidate_image="$VP_WORKER_MANIFEST_IMAGE"
    local candidate_generation="$VP_WORKER_MANIFEST_GENERATION"
    local candidate_database_secret="$VP_WORKER_MANIFEST_DATABASE_SECRET"
    local candidate_admission_secret="$VP_WORKER_MANIFEST_ADMISSION_SECRET"
    local current="$root/current/$kind.conf"
    vp_worker_admission_write_manifest \
      "$current" "$service" "$candidate_commit" \
      "$candidate_image" "$candidate_generation" \
      "$candidate_database_secret" "$candidate_admission_secret" \
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

vp_worker_control_retire_generation() {
  local image="$1"
  local generation="$2"
  local root="$3"
  local allow_missing_services="${4:-false}"
  [[ -n "$image" && -n "$generation" ]] || return 0
  vp_require_pipeline_network_identity || return 1
  [[ "$generation" =~ ^c-[0-9a-f]{20}$ \
    && "$image" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$ \
    && "${generation#c-}" == "${image##*:deploy-}"* ]] || return 1
  vp_worker_control_generation_unused \
    "$generation" "$allow_missing_services" || return 1
  local secret_name
  while IFS= read -r secret_name; do
    [[ -n "$secret_name" ]] || continue
    if docker secret inspect "$secret_name" >/dev/null 2>&1; then
      docker secret rm "$secret_name" >/dev/null || return 1
    fi
  done < <(vp_worker_control_secret_names "$generation")
  local owner_file
  owner_file="$(
    vp_worker_admission_required_file \
      "${VP_WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE:-}" \
      "worker control-role owner database URL file"
  )" || return 1
  mkdir -p "$root/control" || return 1
  chmod 0700 "$root/control" || return 1
  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-control-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$root/control,dst=/control-state" \
    --env WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE=/run/secrets/worker-control-owner-database-url \
    -- \
    python -m app.services.worker_control_role_cli \
      revoke --generation "$generation" \
      --state-dir /control-state >/dev/null || return 1
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

vp_run_worker_registration_migration() {
  local backend_image="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "worker registration migration skipped because service updates are disabled"
    return 0
  fi
  vp_require_pipeline_network_identity || return 1
  local migrator_file
  migrator_file="$(
    vp_worker_admission_required_file \
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
    vp_worker_admission_required_file \
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

vp_require_vision_cutover_safe() {
  local python_worker="$1"

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "vision cutover gate skipped because service updates are disabled"
    return 0
  fi
  if [[ -z "${VP_PYTHON_WORKER_DATABASE_URL:-}" ]]; then
    echo "vision cutover gate requires VP_PYTHON_WORKER_DATABASE_URL" >&2
    return 1
  fi

  local check
  check='import asyncio, os; from sqlalchemy import text; from sqlalchemy.ext.asyncio import create_async_engine; import redis.asyncio as redis; exec("async def check():\n    engine = create_async_engine(os.environ[\"DATABASE_URL\"])\n    client = redis.from_url(os.environ[\"REDIS_URL\"], decode_responses=True)\n    try:\n        async with engine.connect() as connection:\n            schedule = (await connection.execute(text(\"SELECT state, guarded_job_id FROM runtime_schedules WHERE service_name = '\''videoprocess'\''\"))).one_or_none()\n            active_nodes = int((await connection.execute(text(\"SELECT count(*) FROM node_executions WHERE status::text IN ('\''QUEUED'\'', '\''RUNNING'\'')\"))).scalar_one())\n        if schedule is None or schedule.state != \"CLOSED\" or schedule.guarded_job_id is not None or active_nodes != 0:\n            raise SystemExit(1)\n        pending = await client.xpending(\"vp:tasks:vision\", \"vision-workers\")\n        groups = await client.xinfo_groups(\"vp:tasks:vision\")\n        group = next((row for row in groups if row.get(\"name\") == \"vision-workers\"), None)\n        if not isinstance(pending, dict) or pending.get(\"pending\") != 0 or group is None or group.get(\"lag\") != 0:\n            raise SystemExit(1)\n    except Exception:\n        raise SystemExit(1)\n    finally:\n        await client.aclose()\n        await engine.dispose()"); asyncio.run(check())'
  if ! DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" \
    REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env DATABASE_URL \
      --env REDIS_URL \
      "$python_worker" \
      python -c "$check" >/dev/null; then
    echo "vision cutover gate failed; require CLOSED schedule and idle vision work" >&2
    return 1
  fi
  log "vision cutover gate verified: CLOSED and idle"
}

vp_vision_cutover_required() {
  local python_worker="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    printf 'false\n'
    return 0
  fi

  local legacy_names
  legacy_names="$(docker container ls -a \
    --filter 'name=^/vp_vision_worker_1$' \
    --format '{{.Names}}')" || return 1
  case "$legacy_names" in
    vp_vision_worker_1)
      printf 'true\n'
      return 0
      ;;
    '')
      ;;
    *)
      echo "unexpected legacy vision container list result" >&2
      return 1
      ;;
  esac

  if ! docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    printf 'true\n'
    return 0
  fi

  if REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env REDIS_URL \
      "$python_worker" \
      python -m app.services.vision_consumer_cutover --check-only >/dev/null; then
    printf 'false\n'
  else
    printf 'true\n'
  fi
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
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $service $image"
    return 0
  fi

  local constraint
  local has_runtime=false
  local has_runtime_node=false
  local constraint_args=()
  local existing_constraints
  if ! existing_constraints="$(
    vp_service_values "$service" \
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
  if [[ "$service" == "vp-api-swarm" ]]; then
    service_args+=(--no-healthcheck)
    local api_env_key
    for api_env_key in \
      DATABASE_URL \
      VP_GO_ORCHESTRATOR_ENABLED \
      VP_GO_ORCHESTRATOR_JOB_WRITES \
      VP_PYTHON_SCHEDULE_URL; do
      if vp_service_values "$service" \
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
    service_args+=(--replicas 1)
    local existing_worker_env
    existing_worker_env="$(
      vp_service_values "$service" \
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
      vp_service_values "$service" \
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
      vp_service_values "$service" \
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
      if vp_service_values "$service" \
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
    if vp_service_values "$service" \
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
  update_args+=(--image "$image" "$service")
  docker "${update_args[@]}" >&2 || return 1
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
  docker build "${build_args[@]}" \
    -f "$context_dir/$dockerfile" -t "$image" "$context_dir" >&2
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
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PYTHON_WORKER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  vp_worker_service_registration_env \
    "$VP_PYTHON_WORKER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_PYTHON_WORKER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

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
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first
      --replicas 1 --image "$image"
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
    docker "${update_args[@]}" "${env_args[@]}" \
      "$VP_PYTHON_WORKER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_PYTHON_WORKER_SERVICE"
      --replicas 1
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
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_deploy_vision_worker() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_VISION_WORKER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  vp_worker_service_registration_env \
    "$VP_VISION_WORKER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_VISION_WORKER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

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
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
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
      docker service update --detach=false --no-resolve-image --update-order stop-first \
        --replicas 0 --mount-rm /data/storage "$VP_VISION_WORKER_SERVICE" >&2 || return 1
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

    docker "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_VISION_WORKER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_VISION_WORKER_SERVICE"
      --replicas 1
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
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
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

  if ! REDIS_URL="redis://10.0.0.150:6380/0" \
    docker run --rm \
      --network "$VP_PIPELINE_NETWORK_ID" \
      --env REDIS_URL \
      "$python_worker" \
      python -m app.services.vision_consumer_cutover >/dev/null; then
    echo "vision consumer reconciliation failed" >&2
    return 1
  fi
  log "vision consumer reconciliation verified"
}

vp_deploy_publisher() {
  local image="$1"
  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    log "service update skipped $VP_PUBLISHER_SERVICE $image"
    return 0
  fi
  [[ "$VP_WORKER_ADMISSION_PREPARED" == true ]] || return 1
  vp_worker_service_registration_env \
    "$VP_PUBLISHER_SERVICE" "$image" >/dev/null || return 1
  vp_worker_service_secret_specs \
    "$VP_PUBLISHER_SERVICE" >/dev/null || return 1
  vp_require_pipeline_network_identity || return 1

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
    local update_args=(
      service update --detach=false --no-resolve-image --update-order stop-first --replicas 1
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
      docker service update --detach=false --no-resolve-image --update-order stop-first \
        --replicas 0 --mount-rm /data/storage "$VP_PUBLISHER_SERVICE" >&2 || return 1
    fi
    docker "${update_args[@]}" "${env_args[@]}" \
      --image "$image" "$VP_PUBLISHER_SERVICE" >&2 || return 1
  else
    local create_args=(
      service create --detach=false --name "$VP_PUBLISHER_SERVICE"
      --replicas 1
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
    docker "${create_args[@]}" "${create_env[@]}" "$image" >&2 || return 1
  fi
  swarm_service_running "$VP_PUBLISHER_SERVICE" || return 1
  vp_require_service_node "$VP_PUBLISHER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PUBLISHER_SERVICE" false
}

vp_capture_app_snapshots() {
  local service
  local image
  local publisher_state
  for service in $VP_APP_SERVICES; do
    if [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_state="$(vp_publisher_service_state)" || return 1
      if [[ "$publisher_state" == absent ]]; then
        continue
      fi
    elif ! docker service inspect "$service" >/dev/null 2>&1; then
      if [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" \
        || "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
        continue
      fi
      echo "missing required VideoProcess service: $service" >&2
      return 1
    fi
    image="$(vp_service_values "$service" '{{.Spec.TaskTemplate.ContainerSpec.Image}}')" || return 1
    if [[ -z "$image" ]]; then
      echo "missing current image for VideoProcess service: $service" >&2
      return 1
    fi
    printf '%s|%s\n' "$service" "$image"
  done
}

vp_record_app_service_attempt() {
  local service="$1"
  case " $VP_APP_ATTEMPTED_SERVICES " in
    *" $service "*)
      return 0
      ;;
  esac
  VP_APP_ATTEMPTED_SERVICES="${VP_APP_ATTEMPTED_SERVICES:+$VP_APP_ATTEMPTED_SERVICES }$service"
}

vp_remove_app_service_attempt() {
  local service="$1"
  local attempted_service
  local remaining_services=""
  for attempted_service in $VP_APP_ATTEMPTED_SERVICES; do
    [[ "$attempted_service" == "$service" ]] && continue
    remaining_services="${remaining_services:+$remaining_services }$attempted_service"
  done
  VP_APP_ATTEMPTED_SERVICES="$remaining_services"
}

vp_update_app_runtime_service() {
  local service="$1"
  local image="$2"
  local order="$3"
  local update_status=0

  vp_record_app_service_attempt "$service"
  if vp_update_runtime_service "$service" "$image" "$order"; then
    return 0
  else
    update_status=$?
  fi
  if [[ "$update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
    vp_remove_app_service_attempt "$service"
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
  local existing_constraints
  existing_constraints="$(
    vp_service_values "$VP_PYTHON_WORKER_SERVICE" \
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
  update_args+=(--image "$image" "$VP_PYTHON_WORKER_SERVICE")
  docker "${update_args[@]}" >&2 || return 1
  swarm_service_running "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_service_node "$VP_PYTHON_WORKER_SERVICE" "$VP_MANAGER_NODE" || return 1
  vp_require_managed_worker_storage_ready "$VP_PYTHON_WORKER_SERVICE" false
}

vp_restore_app_snapshots() {
  local snapshots="$1"
  local attempted_services="${2-$VP_APP_SERVICES}"
  local worker_admission_rollback="${3:-false}"
  case "$worker_admission_rollback" in
    true|false) ;;
    *) return 1 ;;
  esac
  local service
  local image
  local gpu_was_present=false
  local vision_was_present=false
  local publisher_was_present=false
  local status=0

  while IFS='|' read -r service image; do
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
      if ! vp_update_runtime_service "$service" "$image" stop-first; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_PYTHON_WORKER_SERVICE" ]]; then
      gpu_was_present=true
      if [[ "$worker_admission_rollback" == true ]]; then
        if ! vp_deploy_python_worker "$image"; then
          status=1
          restored=false
        fi
      elif ! vp_restore_gpu_service "$image"; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_VISION_WORKER_SERVICE" ]]; then
      vision_was_present=true
      if ! vp_deploy_vision_worker "$image"; then
        status=1
        restored=false
      fi
    elif [[ "$service" == "$VP_PUBLISHER_SERVICE" ]]; then
      publisher_was_present=true
      if ! vp_deploy_publisher "$image"; then
        status=1
        restored=false
      fi
    elif [[ "$VP_BACKEND_MIGRATION_APPLIED" == true \
      && ( "$service" == "vp-autoflow-api-swarm" \
        || "$service" == "vp-event-outbox-relay-swarm" ) ]]; then
      log "preserve $service at the migration-compatible attempted image"
    elif ! vp_update_runtime_service "$service" "$image" stop-first; then
      status=1
      restored=false
    fi
    if [[ "$registered_worker" == true \
      && "$worker_admission_rollback" == true \
      && "$restored" == true ]] \
      && ! vp_require_worker_deployment_ready "$service"; then
      status=1
    fi
  done < <(printf '%s\n' "$snapshots")

  if vp_app_service_was_attempted "$VP_PYTHON_WORKER_SERVICE" "$attempted_services" \
    && [[ "$gpu_was_present" != true ]] \
    && docker service inspect "$VP_PYTHON_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_PYTHON_WORKER_SERVICE"
    vp_require_worker_redis_marker_status || return 1
    if ! docker service rm "$VP_PYTHON_WORKER_SERVICE" >&2; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_VISION_WORKER_SERVICE" "$attempted_services" \
    && [[ "$vision_was_present" != true ]] \
    && docker service inspect "$VP_VISION_WORKER_SERVICE" >/dev/null 2>&1; then
    log "remove newly created $VP_VISION_WORKER_SERVICE"
    vp_require_worker_redis_marker_status || return 1
    if ! docker service rm "$VP_VISION_WORKER_SERVICE" >&2; then
      status=1
    fi
  fi
  if vp_app_service_was_attempted "$VP_PUBLISHER_SERVICE" "$attempted_services" \
    && [[ "$publisher_was_present" != true ]]; then
    local publisher_state
    publisher_state="$(vp_publisher_service_state)" || return 1
    if [[ "$publisher_state" == exists ]]; then
      log "remove newly created $VP_PUBLISHER_SERVICE"
      vp_require_worker_redis_marker_status || return 1
      if ! docker service rm "$VP_PUBLISHER_SERVICE" >&2; then
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
      vp_worker_admission_read_manifest "$manifest" "$service" || return 1
      printf '%s|%s|%s|%s\n' \
        "$service" \
        "$VP_WORKER_MANIFEST_GENERATION" \
        "$VP_WORKER_MANIFEST_DATABASE_SECRET" \
        "$VP_WORKER_MANIFEST_ADMISSION_SECRET"
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
    printf '%s\n' "$namespace"
  done < <(
    find "$root/candidates" -mindepth 1 -maxdepth 1 \
      -type d -name 'rollback-*' -print 2>/dev/null | LC_ALL=C sort
  )
}

vp_restore_worker_admission_transaction() {
  local snapshots="$1"
  local attempted_services="$2"
  local failed_candidate_records="$3"
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
    vp_restore_app_snapshots "$snapshots" "$attempted_services" false \
      || return 1
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
  vp_install_staging_object_janitor \
    "$VP_WORKER_ADMISSION_CONTROL_IMAGE" || return 1
  vp_run_staging_object_janitor_once || return 1
  vp_restore_app_snapshots \
    "$snapshots" "$attempted_services" true || return 1
  vp_commit_worker_admission || return 1
  local retirement_records="$failed_candidate_records"
  if [[ -n "$stale_rollback_records" ]]; then
    retirement_records="${retirement_records:+$retirement_records$'\n'}$stale_rollback_records"
  fi
  vp_worker_admission_retire_records \
    "$retirement_records" "$root" || return 1
  if [[ -n "$failed_candidate_namespace" ]]; then
    vp_worker_admission_discard_namespace \
      "$root" "$failed_candidate_namespace" || return 1
  fi
  local stale_namespace
  while IFS= read -r stale_namespace; do
    [[ -n "$stale_namespace" ]] || continue
    vp_worker_admission_discard_namespace \
      "$root" "$stale_namespace" || return 1
  done <<<"$stale_rollback_namespaces"
  VP_WORKER_ROLLBACK_FAILED_CANDIDATE_NAMESPACE=""
  VP_WORKER_ROLLBACK_FAILED_CONTROL_GENERATION="$failed_control_generation"
  VP_WORKER_ROLLBACK_FAILED_CONTROL_IMAGE="$failed_control_image"
  VP_WORKER_ADMISSION_ROLLBACK_CONVERGED=true
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
  local seen_secrets="|"
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
    if [[ ! "$secret" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] \
      || ! docker secret inspect "$secret" >/dev/null 2>&1; then
      echo "worker Redis runtime secret is absent" >&2
      return 1
    fi
    if [[ "$seen_secrets" == *"|$secret|"* ]]; then
      echo "worker Redis runtime secrets are not independent" >&2
      return 1
    fi
    seen_secrets="$seen_secrets$secret|"
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
  local role_state="$control_root/roles"
  mkdir -p "$role_state" || return 1
  chmod 0700 "$role_state" || return 1

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
  mkdir -p "$control_root/roles" || return 1
  chmod 0700 "$control_root/roles" || return 1

  vp_run_python_worker_container \
    "$image" \
    "$owner_file" \
    worker-marker-owner-database-url \
    /control-state \
    --network "$VP_PIPELINE_NETWORK_ID" \
    --mount "type=bind,src=$control_root/roles,dst=/control-state" \
    --env WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE=/run/secrets/worker-marker-owner-database-url \
    -- \
    python -m app.services.worker_marker_control_role_cli \
      revoke \
      --generation "$generation" \
      --state-dir /control-state >/dev/null
}

vp_worker_redis_marker_create_database_secrets() {
  local generation="$1"
  local control_root="$2"
  local purpose
  local secret_name
  local credential_file
  VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS=""
  for purpose in readiness janitor repair; do
    secret_name="$(
      vp_worker_redis_marker_database_secret_name "$purpose" "$generation"
    )" || return 1
    credential_file="$control_root/roles/$generation/worker-marker-$purpose-database-url"
    if [[ ! -f "$credential_file" || -L "$credential_file" \
      || "$(vp_worker_redis_marker_file_mode "$credential_file")" != 400 ]] \
      || docker secret inspect "$secret_name" >/dev/null 2>&1; then
      echo "worker marker database credential is absent, invalid, or reused" >&2
      return 1
    fi
    if ! docker secret create "$secret_name" - \
      <"$credential_file" >/dev/null; then
      echo "worker marker database secret creation failed" >&2
      return 1
    fi
    VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS="${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:+$VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS }$secret_name"
  done
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
  for mode in readiness janitor; do
    case "$mode" in
      readiness)
        name=vp-worker-redis-marker-readiness-job
        ;;
      janitor)
        name=vp-worker-redis-marker-janitor-job
        ;;
    esac
    if ! docker service inspect "$name" >/dev/null 2>&1; then
      continue
    fi
    expected_identity="$(
      vp_worker_redis_marker_expected_job_identity \
        "$image" "$generation" "$mode" \
        "$readiness_redis_secret" "$janitor_redis_secret"
    )" || return 1
    actual_identity="$(vp_worker_redis_marker_job_identity "$name")" || return 1
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
      docker service ps "$name" --no-trunc \
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
    docker service rm "$name" >/dev/null || return 1
    local attempt
    for ((attempt = 0; attempt < 30; attempt++)); do
      if ! docker service inspect "$name" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if docker service inspect "$name" >/dev/null 2>&1; then
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
    if docker secret inspect "$secret_name" >/dev/null 2>&1; then
      docker secret rm "$secret_name" >/dev/null || return 1
    fi
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
  local state
  state="$(mktemp -d "$control_root/.managed-state.XXXXXX")" || return 1
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
    && awk -v begin="$cron_begin" -v end="$cron_end" \
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
    ' "$prior_cron" >"$next_cron" \
    && printf '%s\n%s\n%s\n%s\n' \
      "$cron_begin" "$readiness_cron" "$janitor_cron" "$cron_end" \
      >>"$next_cron" \
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
  vp_worker_redis_marker_provision_roles \
    "$image" "$generation" "$control_root" || return 1
  if ! vp_worker_redis_marker_create_database_secrets \
    "$generation" "$control_root"; then
    vp_worker_redis_marker_revoke_roles \
      "$image" "$generation" "$control_root" || return 1
    local secret
    for secret in ${VP_WORKER_REDIS_MARKER_CREATED_DATABASE_SECRETS:-}; do
      docker secret rm "$secret" >/dev/null || return 1
    done
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
  mkdir -p "$control_root" || return 1
  chmod 0700 "$control_root" || return 1
  vp_worker_redis_marker_read_prior_config \
    "$control_root/control.conf" || return 1
  vp_worker_redis_marker_capture_managed_state "$control_root" || return 1
  if ! vp_worker_redis_marker_deactivate_managed_cron; then
    if ! vp_worker_redis_marker_restore_managed_state; then
      echo "worker Redis marker managed-state restore did not verify" >&2
    fi
    return 1
  fi
  if ! vp_worker_redis_marker_remove_generation_jobs \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET"; then
    if ! vp_worker_redis_marker_restore_managed_state; then
      echo "worker Redis marker managed-state restore did not verify" >&2
    fi
    return 1
  fi

  local generation
  if ! generation="$(vp_worker_redis_marker_new_generation "$image")"; then
    vp_worker_redis_marker_restore_managed_state || return 1
    return 1
  fi
  VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION="$generation"
  VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE="$image"
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=true
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false

  if ! vp_worker_redis_marker_provision_generation \
    "$image" "$generation" "$control_root" \
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
  local original_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  vp_worker_redis_marker_capture_managed_state "$control_root" || return 1
  local candidate_state="$VP_WORKER_REDIS_MARKER_MANAGED_STATE"
  VP_WORKER_REDIS_MARKER_MANAGED_STATE="$original_state"
  if ! vp_worker_redis_marker_deactivate_managed_cron "$candidate_state" \
    || ! vp_worker_redis_marker_remove_generation_jobs \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
      "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION"; then
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    return 1
  fi
  local rollback_generation
  rollback_generation="$(
    vp_worker_redis_marker_new_generation \
      "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE"
  )" || {
    vp_worker_redis_marker_restore_managed_state "$candidate_state" || true
    return 1
  }
  if ! vp_worker_redis_marker_provision_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$rollback_generation" \
    "$control_root" \
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
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_CANDIDATE_GENERATION" \
    "$control_root" || return 1
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" || return 1
  rm -rf "$candidate_state" || return 1
  vp_worker_redis_marker_discard_managed_state || return 1
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
  vp_worker_redis_marker_retire_generation \
    "$VP_WORKER_REDIS_MARKER_PRIOR_IMAGE" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_GENERATION" \
    "$control_root" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_READINESS_REDIS_SECRET" \
    "$VP_WORKER_REDIS_MARKER_PRIOR_JANITOR_REDIS_SECRET" || return 1
  vp_worker_redis_marker_discard_managed_state || return 1
  VP_WORKER_REDIS_MARKER_CONTROL_PREPARED=false
  VP_WORKER_REDIS_MARKER_CANDIDATE_READY=false
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
  vp_update_app_runtime_service vp-api-swarm "$api" stop-first || return 1
  http_health vp-api "http://$VP_RUNTIME_HOST:18080/health" || return 1
  vp_update_app_runtime_service vp-frontend-swarm "$frontend" stop-first || return 1
  http_health vp-frontend "http://$VP_RUNTIME_HOST:3001/" || return 1
  vp_run_worker_registration_migration "$backend" || return 1
  VP_BACKEND_MIGRATION_APPLIED=true
  vp_require_channelops_migration_head "$backend" || return 1
  vp_update_app_runtime_service vp-autoflow-api-swarm "$backend" start-first || return 1
  vp_prepare_worker_redis_marker_controls "$python_worker" || return 1
  vp_prepare_worker_admission "$python_worker" "$ffmpeg_go" || return 1
  vp_install_staging_object_janitor "$python_worker" || return 1
  vp_run_staging_object_janitor_once || return 1

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    vp-ffmpeg-worker-go-swarm || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_update_app_runtime_service \
    vp-ffmpeg-worker-go-swarm "$ffmpeg_go" stop-first || return 1
  vp_require_worker_deployment_ready \
    vp-ffmpeg-worker-go-swarm || return 1

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_PYTHON_WORKER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PYTHON_WORKER_SERVICE"
  vp_deploy_python_worker "$python_worker" || return 1
  vp_require_worker_deployment_ready \
    "$VP_PYTHON_WORKER_SERVICE" || return 1

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_VISION_WORKER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_VISION_WORKER_SERVICE"
  vp_deploy_vision_worker "$python_worker" || return 1
  vp_require_worker_deployment_ready \
    "$VP_VISION_WORKER_SERVICE" || return 1
  if [[ "$VP_VISION_CUTOVER_REQUIRED" == true ]]; then
    vp_retire_legacy_vision_worker || return 1
    vp_reconcile_vision_consumers "$python_worker" || return 1
  fi

  vp_require_worker_redis_marker_status || return 1
  vp_activate_worker_admission \
    "$VP_PUBLISHER_SERVICE" || return 1
  vp_require_worker_redis_marker_status || return 1
  vp_record_app_service_attempt "$VP_PUBLISHER_SERVICE"
  vp_deploy_publisher "$python_worker" || return 1
  vp_require_worker_deployment_ready \
    "$VP_PUBLISHER_SERVICE" || return 1

  vp_update_app_runtime_service vp-event-outbox-relay-swarm "$backend" start-first || return 1
  vp_update_app_runtime_service \
    vp-channel-agent-runner-swarm "$channelops_runner" stop-first || return 1

  local service
  for service in $VP_APP_SERVICES; do
    swarm_service_running "$service" || return 1
  done
  vp_install_soak_watch || return 1
  vp_commit_worker_admission || return 1
  vp_commit_worker_redis_marker_controls || return 1
  vp_commit_worker_control_generation || return 1
}

deploy_vp_app_services() {
  vp_validate_deploy_config || return 1
  VP_VISION_CUTOVER_REQUIRED="$(vp_vision_cutover_required "${6:-}")" || return 1
  case "$VP_VISION_CUTOVER_REQUIRED" in
    true)
      vp_require_vision_cutover_safe "${6:-}" || return 1
      ;;
    false)
      ;;
    *)
      echo "invalid vision cutover state" >&2
      return 1
      ;;
  esac

  if [[ "${UPDATE_SERVICES:-1}" -eq 0 ]]; then
    vp_apply_app_services "$@" || return 1
    printf '%s\n' "$VP_APP_SERVICES"
    return 0
  fi

  local snapshots
  snapshots="$(vp_capture_app_snapshots)" || return 1
  if ! vp_apply_app_services "$@"; then
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
    if [[ "$candidate_capture_ok" != true ]] \
      || ! vp_restore_worker_admission_transaction \
        "$snapshots" "$VP_APP_ATTEMPTED_SERVICES" \
        "$failed_candidate_records"; then
      echo "VideoProcess image restore did not fully converge" >&2
    elif ! vp_restore_worker_redis_marker_controls; then
      echo "worker Redis marker control restore did not converge" >&2
    elif ! vp_finalize_worker_control_rollback; then
      echo "worker control generation retirement did not converge" >&2
    fi
    return 1
  fi
  printf '%s\n' "$VP_APP_SERVICES"
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
