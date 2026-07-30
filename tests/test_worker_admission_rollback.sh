#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rc=$?; rm -rf "$TEST_ROOT"; exit "$rc"' EXIT

REPO_ROOT="$TEST_ROOT/repos"
ROOT="$TEST_ROOT/sync"
UPDATE_SERVICES=1
mkdir -p "$ROOT"
log() {
  printf 'log|%s\n' "$*" >>"$CALLS"
}
source "$ROOT_DIR/deploy/swarm/deploy-sync-extension.sh"

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
if document["schema"] != 1:
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

  transaction_cli transition \
    "$transaction_root" 18 0 FORWARD_APPLYING >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 1 FORWARD_VERIFIED >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 2 WORKERS_PROMOTED >/dev/null

  retirement_identity="$TEST_ROOT/durable-core/retirement.json"
  retirement_id=retirement-0123456789abcdef0123456789abcdef
  secret_id=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  printf '%s\n' \
    "{\"docker_id\":\"$secret_id\",\"generation\":\"901\",\"kind\":\"secret\",\"name\":\"vp-worker-candidate\",\"purpose\":\"database\",\"service\":\"vp-ffmpeg-worker-go-swarm\",\"spec_digest\":null}" \
    >"$retirement_identity"
  chmod 0600 "$retirement_identity"
  transaction_cli queue-retirement \
    "$transaction_root" 18 3 "$retirement_id" "$retirement_identity" \
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
    "revision": 4,
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
      "$transaction_root" 18 4 PROMOTE_MARKER "$marker_identity"
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
    "$transaction_root" 18 5 "$marker_operation_id" >/dev/null

  control_identity="$TEST_ROOT/durable-core/control-identity.json"
  printf '%s\n' \
    '{"docker_id":null,"generation":"control-901","kind":"manifest","name":"control-current.conf","purpose":"promotion","service":"worker-admission-control","spec_digest":"1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}' \
    >"$control_identity"
  chmod 0600 "$control_identity"
  control_intent="$(
    transaction_cli intent \
      "$transaction_root" 18 6 PROMOTE_CONTROL "$control_identity"
  )"
  control_operation_id="$(
    python3 -c \
      'import json,sys; print(json.load(sys.stdin)["operation"]["operation_id"])' \
      <<<"$control_intent"
  )"
  transaction_cli complete-intent \
    "$transaction_root" 18 7 "$control_operation_id" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 8 RETIRING >/dev/null
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
    "$transaction_root" 18 9 "$retirement_id" >/dev/null
  transaction_cli transition \
    "$transaction_root" 18 10 DONE succeeded >/dev/null
  done_path="$(
    transaction_cli archive "$transaction_root" 18 11
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
  printf '%s|%s\n' \
    vp-ffmpeg-worker-go-swarm \
    "vp-ffmpeg-worker-go:deploy-$old_short" \
    "$VP_PYTHON_WORKER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    "$VP_VISION_WORKER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short" \
    "$VP_PUBLISHER_SERVICE" \
    "vp-ffmpeg-worker-python:deploy-$old_short"
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

vp_worker_admission_prepare_service() {
  local service="$1"
  local image="$2"
  local _control_image="$3"
  local commit="$4"
  local root="$5"
  local namespace="$6"
  local kind
  kind="$(vp_worker_admission_kind "$service")"
  local generation
  generation="$(vp_worker_admission_new_generation)"
  local database_secret="fresh-$kind-db-$generation"
  local admission_secret="fresh-$kind-admission-$generation"
  vp_worker_admission_write_manifest \
    "$root/candidates/$namespace/$kind.conf" \
    "$service" "$commit" "$image" "$generation" \
    "$database_secret" "$admission_secret" \
    "$(test_secret_id "$generation")" \
    "$(test_secret_id "$((generation + 1000))")"
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
  printf 'restore|%s|%s\n' "$VP_PYTHON_WORKER_SERVICE" "$1" >>"$CALLS"
}

vp_deploy_vision_worker() {
  printf 'restore|%s|%s\n' "$VP_VISION_WORKER_SERVICE" "$1" >>"$CALLS"
}

vp_deploy_publisher() {
  printf 'restore|%s|%s\n' "$VP_PUBLISHER_SERVICE" "$1" >>"$CALLS"
}

vp_install_staging_object_janitor() {
  printf 'janitor|install|%s\n' "$1" >>"$CALLS"
}

vp_run_staging_object_janitor_once() {
  printf 'janitor|ready\n' >>"$CALLS"
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

: >"$CALLS"
FAIL_READY_SERVICE=
vp_restore_worker_admission_transaction \
  "$snapshots" "$attempted_services" "$failed_records"
second_namespace="$VP_WORKER_ADMISSION_CANDIDATE_NAMESPACE"
if [[ -z "$first_namespace" || -z "$second_namespace" \
  || "$first_namespace" == "$second_namespace" ]]; then
  echo 'FAIL: rollback attempt reused its prior credential namespace' >&2
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

for generation in 201 202 203 204; do
  grep -Eq "^retire\\|[^|]+\\|$generation\\|" "$CALLS"
done
if grep -Fq '10.0.0.126' "$CALLS"; then
  echo 'FAIL: rollback referenced host 126' >&2
  exit 1
fi

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

echo 'worker admission rollback transaction tests passed'
