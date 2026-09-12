"""Fixed managed-job entry and bounded deployment callback file protocol.

The owning shell must verify both locks and the actual managed-job descriptor
before the journal helper answers a request. File replies are not lock leases.
Runtime imports are lazy so the host journal can reuse the stdlib-only parser.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import sys
import time
from typing import Any, TypeVar, cast
from uuid import UUID


MAX_BYTES = 1024 * 1024
MAX_FRAME_BYTES = 4096
MAX_SEQUENCE = 1024
CALLBACK_SECONDS = 1.6
POLL_SECONDS = 0.01
STREAMS = {
    "vp-ffmpeg-worker-go-swarm": "vp:tasks:ffmpeg_go",
    "vp-ffmpeg-worker-gpu-swarm": "vp:tasks:ffmpeg",
    "vp-youtube-publisher-swarm": "vp:tasks:youtube_publisher",
}
SERVICES = set(STREAMS) | {"vp-vision-worker-swarm"}
OUTCOMES = {"retired", "already_absent", "unknown"}
CREDENTIAL_FIELDS = {
    "control_generation",
    "redis_generation",
    "redis_secret_name",
    "redis_username",
    "database_secret_id",
    "redis_secret_id",
    "database_secret_sha256",
    "redis_secret_sha256",
}
FILE_FIELDS = {"path", "device", "inode", "uid", "gid"}
FRAME_FIELDS = {
    "version",
    "attempt_id",
    "sequence",
    "nonce",
    "action",
    "binding_sha256",
    "stream",
    "command_sha256",
    "outcome",
}


class ProtocolError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("registered_reconcile_protocol_failed")


def require(condition: bool) -> None:
    if not condition:
        raise ProtocolError()


def exact(value: object, fields: set[str]) -> dict:
    require(type(value) is dict and set(value) == fields)
    return cast(dict, value)


def integer(value: object, minimum: int = 0) -> bool:
    return type(value) is int and minimum <= value <= 2**63 - 1


def matches(value: object, pattern: str) -> bool:
    return type(value) is str and re.fullmatch(pattern, value, re.ASCII) is not None


def canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _pairs(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def decode(raw: bytes) -> dict:
    try:
        require(0 < len(raw) <= MAX_BYTES)
        value = json.loads(raw.decode("ascii"), object_pairs_hook=_pairs)
        require(type(value) is dict and canonical(value) == raw)
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ProtocolError() from None


def _metadata(path: Path) -> dict:
    value = path.lstat()
    return dict(
        path=str(path),
        device=value.st_dev,
        inode=value.st_ino,
        uid=value.st_uid,
        gid=value.st_gid,
    )


def _check(metadata: os.stat_result, record: dict, mode: int, directory: bool) -> None:
    require((stat.S_ISDIR if directory else stat.S_ISREG)(metadata.st_mode))
    require(
        (metadata.st_dev, metadata.st_ino, metadata.st_uid, metadata.st_gid)
        == tuple(record[key] for key in ("device", "inode", "uid", "gid"))
    )
    require(stat.S_IMODE(metadata.st_mode) == mode)
    require(directory or metadata.st_nlink == 1)


def open_checked(
    record: dict, flags: int, mode: int, *, directory: bool = False
) -> int:
    try:
        _check(os.lstat(record["path"]), record, mode, directory)
        descriptor = os.open(
            record["path"],
            flags | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_DIRECTORY if directory else 0),
        )
        try:
            _check(os.fstat(descriptor), record, mode, directory)
            _check(os.lstat(record["path"]), record, mode, directory)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    except OSError:
        raise ProtocolError() from None


def prepare_files(root: Path) -> dict:
    """Caller-owned, already controlled private attempt directory; never overwrite."""
    try:
        root = Path(root)
        metadata = root.lstat()
        require(root.is_absolute() and str(root.resolve()) == str(root))
        require(
            stat.S_ISDIR(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) == 0o700
        )
        require((metadata.st_uid, metadata.st_gid) == (os.getuid(), os.getgid()))
        parent = os.open(
            root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            require(os.fstat(parent).st_ino == metadata.st_ino)
            descriptor = os.open(
                "requests",
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=parent,
            )
            try:
                os.fchmod(descriptor, 0o602)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.mkdir("replies", 0o755, dir_fd=parent)
            os.chmod("replies", 0o755, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(parent)
        return {
            "request": _metadata(root / "requests"),
            "replies": _metadata(root / "replies"),
        }
    except OSError:
        raise ProtocolError() from None


def require_writer_identity(
    record: dict, uid: int, gid: int, groups: list[int]
) -> None:
    require(
        uid == 10001
        and gid == 10001
        and record["uid"] != uid
        and record["gid"] not in {gid, *groups}
    )


def make_binding(request: Any, files: dict, *, descriptor_sha256: str) -> dict:
    # Unit1 validates full pin semantics; the host below checks their immutable
    # projection against its independently loaded forward plan, without ORM imports.
    from app.services.registered_consumer_reconcile import EvalCommand, decode_pins

    pins = decode_pins(request.pins.canonical_json)
    require(type(request.attempt_id) is UUID and request.attempt_id.int != 0)
    binding = dict(
        version=1,
        attempt_id=str(request.attempt_id),
        replay_only=request.replay_only,
        transaction_id=pins.transaction_id,
        binding_revision=pins.revision,
        release_commit=pins.release_commit,
        pin_sha256=pins.sha256,
        pin_json=pins.canonical_json,
        targets={
            worker.current.service_name: [
                worker.current.generation,
                worker.current.image_identity,
            ]
            for worker in pins.workers
        },
        commands={
            STREAMS[worker.current.service_name]: digest(
                list(EvalCommand(pins, worker.current.service_name).arguments)
            )
            for worker in pins.workers
            if worker.current.service_name in STREAMS and worker.predecessor is not None
        },
        credentials={key: getattr(request, key) for key in CREDENTIAL_FIELDS},
        files=copy.deepcopy(files),
        descriptor_sha256=descriptor_sha256,
    )
    validate_binding(binding)
    return binding


def validate_binding(binding: dict) -> None:
    exact(
        binding,
        {
            "version",
            "attempt_id",
            "replay_only",
            "transaction_id",
            "binding_revision",
            "release_commit",
            "pin_sha256",
            "pin_json",
            "targets",
            "commands",
            "credentials",
            "files",
            "descriptor_sha256",
        },
    )
    require(type(binding["version"]) is int and binding["version"] == 1)
    require(
        matches(binding["attempt_id"], r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}")
    )
    require(
        UUID(binding["attempt_id"]).int != 0 and type(binding["replay_only"]) is bool
    )
    require(matches(binding["transaction_id"], r"tx-[0-9a-f]{32}"))
    require(
        integer(binding["binding_revision"])
        and matches(binding["release_commit"], r"[0-9a-f]{40}")
    )
    for key in ("pin_sha256", "descriptor_sha256"):
        require(matches(binding[key], r"[0-9a-f]{64}"))
    require(type(binding["pin_json"]) is str)
    raw = binding["pin_json"].encode("ascii")
    pins = decode(raw + b"\n")
    exact(pins, {"version", "transaction_id", "revision", "release_commit", "workers"})
    require(hashlib.sha256(raw).hexdigest() == binding["pin_sha256"])
    require(
        pins["transaction_id"] == binding["transaction_id"]
        and type(pins["revision"]) is int
        and pins["revision"] == binding["binding_revision"]
        and pins["release_commit"] == binding["release_commit"]
        and type(pins["version"]) is int
        and pins["version"] == 1
    )
    require(type(pins["workers"]) is list and len(pins["workers"]) == 4)
    targets = {}
    expected_streams = set()
    for worker in pins["workers"]:
        exact(worker, {"current", "predecessor"})
        current = worker["current"]
        require(type(current) is dict)
        service = current.get("service_name")
        require(type(service) is str and service in SERVICES and service not in targets)
        require(
            integer(current.get("generation"), 1)
            and current.get("release_commit") == binding["release_commit"]
            and matches(
                current.get("image_identity"),
                r"[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}",
            )
            and current["image_identity"].endswith(
                ":deploy-" + binding["release_commit"][:12]
            )
        )
        targets[service] = [current["generation"], current["image_identity"]]
        if service in STREAMS and worker["predecessor"] is not None:
            expected_streams.add(STREAMS[service])
    require(set(targets) == SERVICES and binding["targets"] == targets)
    exact(binding["commands"], expected_streams)
    require(
        all(matches(value, r"[0-9a-f]{64}") for value in binding["commands"].values())
    )
    credentials = exact(binding["credentials"], CREDENTIAL_FIELDS)
    for key in ("control_generation", "redis_generation"):
        require(matches(credentials[key], r"[a-z0-9][a-z0-9-]{0,62}"))
    require(
        matches(credentials["redis_secret_name"], r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
    )
    require(
        matches(credentials["redis_username"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
        and credentials["redis_username"] != "default"
    )
    for key in ("database_secret_id", "redis_secret_id"):
        require(matches(credentials[key], r"[a-z0-9]{25}"))
    for key in ("database_secret_sha256", "redis_secret_sha256"):
        require(matches(credentials[key], r"[0-9a-f]{64}"))
    exact(binding["files"], {"request", "replies"})
    for record in binding["files"].values():
        exact(record, FILE_FIELDS)
        require(
            type(record["path"]) is str
            and record["path"].startswith("/")
            and str(Path(record["path"])) == record["path"]
            and "\x00" not in record["path"]
        )
        require(all(integer(record[key]) for key in FILE_FIELDS - {"path"}))
    require(binding["files"]["request"]["path"] != binding["files"]["replies"]["path"])


def new_record(binding: dict) -> dict:
    validate_binding(binding)
    return dict(
        version=1,
        binding=copy.deepcopy(binding),
        service_id=None,
        sequence=0,
        prefix_length=0,
        prefix_sha256=hashlib.sha256(b"").hexdigest(),
        streams={stream: "unused" for stream in binding["commands"]},
        last_request=None,
    )


def validate_record(record: dict) -> None:
    exact(
        record,
        {
            "version",
            "binding",
            "service_id",
            "sequence",
            "prefix_length",
            "prefix_sha256",
            "streams",
            "last_request",
        },
    )
    validate_binding(record["binding"])
    require(type(record["version"]) is int and record["version"] == 1)
    require(
        record["service_id"] is None
        or matches(record["service_id"], r"[a-z0-9]{12,64}")
    )
    require(
        integer(record["sequence"])
        and record["sequence"] <= MAX_SEQUENCE
        and integer(record["prefix_length"])
        and record["prefix_length"] <= MAX_BYTES
        and matches(record["prefix_sha256"], r"[0-9a-f]{64}")
    )
    exact(record["streams"], set(record["binding"]["commands"]))
    require(
        all(
            value in {"unused", "consumed", *OUTCOMES}
            for value in record["streams"].values()
        )
    )
    if record["sequence"] == 0:
        require(
            record["last_request"] is None
            and record["prefix_length"] == 0
            and record["prefix_sha256"] == hashlib.sha256(b"").hexdigest()
            and all(value == "unused" for value in record["streams"].values())
        )
    else:
        validate_frame(record["last_request"], record["binding"], record["sequence"])
        if record["last_request"]["action"] == "finished":
            require(
                not {"consumed", "unknown"}.intersection(record["streams"].values())
            )
            require(
                (record["last_request"]["outcome"] == "already_absent")
                == all(value == "unused" for value in record["streams"].values())
            )


def validate_frame(frame: dict, binding: dict, sequence: int) -> None:
    exact(frame, FRAME_FIELDS)
    require(
        type(frame["version"]) is int
        and frame["version"] == 1
        and integer(frame["sequence"], 1)
        and frame["sequence"] == sequence
        and sequence <= MAX_SEQUENCE
        and matches(frame["nonce"], r"[0-9a-f]{32}")
        and frame["attempt_id"] == binding["attempt_id"]
        and frame["binding_sha256"] == digest(binding)
    )
    action = frame["action"]
    require(
        type(action) is str
        and action in {"revalidate", "before_eval", "after_eval", "finished"}
    )
    if action == "revalidate":
        require(
            all(frame[key] is None for key in ("stream", "command_sha256", "outcome"))
        )
    elif action == "finished":
        require(
            frame["stream"] is None
            and frame["command_sha256"] is None
            and frame["outcome"] in {"reconciled", "already_absent"}
        )
    else:
        require(
            type(frame["stream"]) is str
            and frame["stream"] in binding["commands"]
            and frame["command_sha256"] == binding["commands"][frame["stream"]]
        )
        require(
            type(frame["outcome"]) is str and frame["outcome"] in OUTCOMES
            if action == "after_eval"
            else frame["outcome"] is None
        )


def read_request(record: dict) -> tuple[dict, bytes] | None:
    validate_record(record)
    descriptor = open_checked(record["binding"]["files"]["request"], os.O_RDONLY, 0o602)
    try:
        before = os.fstat(descriptor)
        require(record["prefix_length"] <= before.st_size <= MAX_BYTES)
        raw = bytearray()
        while len(raw) <= MAX_BYTES:
            chunk = os.read(descriptor, min(65536, MAX_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after = os.fstat(descriptor)
        require(len(raw) <= MAX_BYTES)
        require(
            hashlib.sha256(raw[: record["prefix_length"]]).hexdigest()
            == record["prefix_sha256"]
        )
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            return None
    finally:
        os.close(descriptor)
    tail = bytes(raw[record["prefix_length"] :])
    require(len(tail) <= MAX_FRAME_BYTES)
    if not tail or b"\n" not in tail:
        return None
    require(tail.endswith(b"\n") and tail.count(b"\n") == 1)
    value = decode(tail)
    validate_frame(value, record["binding"], record["sequence"] + 1)
    return value, bytes(raw)


def advance_record(record: dict, request: dict, prefix: bytes) -> dict:
    validate_record(record)
    require(
        record["last_request"] is None or record["last_request"]["action"] != "finished"
    )
    validate_frame(request, record["binding"], record["sequence"] + 1)
    require(
        prefix[record["prefix_length"] :] == canonical(request)
        and hashlib.sha256(prefix[: record["prefix_length"]]).hexdigest()
        == record["prefix_sha256"]
    )
    require(
        all(
            decode(line + b"\n")["nonce"] != request["nonce"]
            for line in prefix[: record["prefix_length"]].splitlines()
        )
    )
    result = copy.deepcopy(record)
    action, stream = request["action"], request["stream"]
    if action == "before_eval":
        require(
            not record["binding"]["replay_only"]
            and record["streams"][stream] == "unused"
            and not any(
                value in {"consumed", "unknown"} for value in record["streams"].values()
            )
        )
        result["streams"][stream] = "consumed"
    elif action == "after_eval":
        require(record["streams"][stream] == "consumed")
        result["streams"][stream] = request["outcome"]
    elif action == "finished":
        require(
            not any(
                value in {"consumed", "unknown"} for value in record["streams"].values()
            )
        )
        require(
            (request["outcome"] == "already_absent")
            == all(value == "unused" for value in record["streams"].values())
        )
    result.update(
        sequence=request["sequence"],
        prefix_length=len(prefix),
        prefix_sha256=hashlib.sha256(prefix).hexdigest(),
        last_request=copy.deepcopy(request),
    )
    validate_record(result)
    return result


def write_reply(files: dict, request: dict) -> None:
    directory = open_checked(files["replies"], os.O_RDONLY, 0o755, directory=True)
    temporary = ".reply-" + secrets.token_hex(16)
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        try:
            payload = canonical({"status": "accepted", "request": request})
            _write_all(descriptor, payload)
            os.fchmod(descriptor, 0o644)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, "reply.json", src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    except OSError:
        raise ProtocolError() from None
    finally:
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass
        os.close(directory)


def read_reply(files: dict, request: dict) -> bool:
    directory = open_checked(files["replies"], os.O_RDONLY, 0o755, directory=True)
    try:
        try:
            descriptor = os.open(
                "reply.json",
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory,
            )
        except FileNotFoundError:
            return False
        try:
            before = os.fstat(descriptor)
            require(
                stat.S_ISREG(before.st_mode)
                and before.st_nlink == 1
                and stat.S_IMODE(before.st_mode) == 0o644
                and (before.st_uid, before.st_gid)
                == (files["replies"]["uid"], files["replies"]["gid"])
            )
            raw = os.read(descriptor, MAX_FRAME_BYTES + 1)
            after = os.fstat(descriptor)
            stable = (
                "st_dev",
                "st_ino",
                "st_uid",
                "st_gid",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
            require(
                len(raw) <= MAX_FRAME_BYTES
                and all(getattr(before, key) == getattr(after, key) for key in stable)
            )
        finally:
            os.close(descriptor)
        reply = exact(decode(raw), {"status", "request"})
        require(reply["status"] == "accepted")
        return canonical(reply["request"]) == canonical(request)
    except OSError:
        raise ProtocolError() from None
    finally:
        os.close(directory)


def _write_all(descriptor: int, payload: bytes) -> None:
    while payload:
        count = os.write(descriptor, payload)
        require(count > 0)
        payload = payload[count:]


def _exchange(files: dict, request: dict, offset: int) -> None:
    deadline = time.monotonic() + 1.3
    descriptor = open_checked(files["request"], os.O_WRONLY | os.O_APPEND, 0o602)
    try:
        require(os.fstat(descriptor).st_size == offset)
        _write_all(descriptor, canonical(request))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    while time.monotonic() < deadline:
        if read_reply(files, request):
            return
        time.sleep(POLL_SECONDS)
    raise ProtocolError()


T = TypeVar("T")


async def _settle(task: asyncio.Task[T]) -> T:
    """Own terminal cleanup after cancellation/kill, including repeated cancels."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass
    return task.result()


class FileAuthority:
    """One invocation only. Any cancellation/error poisons the client permanently.

    Blocking fsync runs in an owned, killed-and-reaped I/O process, not an orphan
    executor thread. Task2 supplies only verified fixed mount paths in production.
    """

    def __init__(self, binding: dict, files: dict | None = None):
        validate_binding(binding)
        self.binding = copy.deepcopy(binding)
        self.files = copy.deepcopy(binding["files"] if files is None else files)
        for key in self.files:
            require(
                {k: v for k, v in self.files[key].items() if k != "path"}
                == {k: v for k, v in binding["files"][key].items() if k != "path"}
            )
        require_writer_identity(
            self.files["request"], os.getuid(), os.getgid(), os.getgroups()
        )
        self.sequence = self.offset = 0
        self.poisoned = self.busy = False
        self.pending_process: asyncio.subprocess.Process | None = None

    async def revalidate(self, request: Any) -> None:
        await self._call(request, "revalidate")

    async def before_eval(self, request: Any, command: Any) -> None:
        await self._call(request, "before_eval", command)

    async def after_eval(self, request: Any, command: Any, outcome: str) -> None:
        await self._call(request, "after_eval", command, outcome)

    async def finished(self, request: Any, outcome: str) -> None:
        await self._call(request, "finished", outcome=outcome)

    async def _call(
        self,
        invocation: Any,
        action: str,
        command: Any = None,
        outcome: str | None = None,
    ) -> None:
        require(not self.poisoned and not self.busy)
        self.busy = True
        created = None
        try:
            require(
                make_binding(
                    invocation,
                    self.binding["files"],
                    descriptor_sha256=self.binding["descriptor_sha256"],
                )
                == self.binding
            )
            if command is not None:
                require(command.pins.sha256 == self.binding["pin_sha256"])
            frame = dict(
                version=1,
                attempt_id=self.binding["attempt_id"],
                sequence=self.sequence + 1,
                nonce=secrets.token_hex(16),
                action=action,
                binding_sha256=digest(self.binding),
                stream=None if command is None else command.stream,
                command_sha256=None
                if command is None
                else digest(list(command.arguments)),
                outcome=outcome,
            )
            validate_frame(frame, self.binding, self.sequence + 1)
            payload = canonical(
                dict(files=self.files, request=frame, offset=self.offset)
            )
            async with asyncio.timeout(CALLBACK_SECONDS):
                created = asyncio.create_task(
                    asyncio.create_subprocess_exec(
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--file-exchange",
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                )
                self.pending_process = await asyncio.shield(created)
                stdout, _ = await self.pending_process.communicate(payload)
                require(self.pending_process.returncode == 0 and stdout == b"ok\n")
            self.sequence += 1
            self.offset += len(canonical(frame))
        except asyncio.CancelledError:
            self.poisoned = True
            raise
        except Exception:
            self.poisoned = True
            raise ProtocolError() from None
        finally:
            try:
                if created is not None:
                    if not created.done():
                        # CPython owns any child before publishing Process. Cancel
                        # startup so its transport closes and reaps that child.
                        created.cancel()
                    try:
                        process = await _settle(created)
                    except asyncio.CancelledError:
                        # This is the creator's terminal cancellation, not the
                        # caller's cancellation (preserved below).
                        pass
                    else:
                        if process.returncode is None:
                            try:
                                process.kill()
                            except ProcessLookupError:
                                pass
                        await _settle(asyncio.create_task(process.wait()))
            except Exception:
                self.poisoned = True
                raise ProtocolError() from None
            finally:
                self.pending_process = None
                self.busy = False
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    self.poisoned = True
                    raise asyncio.CancelledError


def capture_snapshot(registrations: list, grants: list, *, now: Any) -> dict:
    """Complete active-set capture, before activation or after native readiness."""
    from dataclasses import asdict, fields
    from datetime import timedelta, timezone
    from app.services.registered_consumer_reconcile import (
        IdentityPin,
        _CONTRACTS,
        _identity_matches,
        _json_scalar,
    )

    try:
        require(now.tzinfo is not None)
        require(all(row.service_name in SERVICES for row in [*registrations, *grants]))
        workers: list[dict | None] = []
        for service in _CONTRACTS:
            rows = [row for row in registrations if row.service_name == service]
            selected = [grant for grant in grants if grant.service_name == service]
            if not rows and not selected:
                workers.append(None)
                continue
            require(len(rows) == len(selected) == 1)
            row, grant = rows[0], selected[0]
            require(row.status == grant.state == "active")
            require(
                row.revoked_at is None
                and row.revoke_reason is None
                and row.superseded_by is None
                and grant.revoked_at is None
                and grant.revoke_reason is None
                and grant.activated_at <= now
                and row.registered_at <= now
                and row.lease_expires_at > now + timedelta(seconds=60)
            )
            values = {
                field.name: getattr(row, field.name)
                for field in fields(IdentityPin)
                if field.name
                not in {
                    "registration_id",
                    "generation",
                    "release_commit",
                    "capabilities",
                }
            }
            values.update(
                registration_id=UUID(str(row.id)),
                grant_id=UUID(str(row.grant_id)),
                worker_instance_id=UUID(str(row.worker_instance_id)),
                generation=grant.generation,
                release_commit=grant.release_commit,
                capabilities=tuple(row.capabilities_json),
            )
            pin = IdentityPin(**values)
            _identity_matches(pin, row, grant)
            workers.append(json.loads(json.dumps(asdict(pin), default=_json_scalar)))
        return {
            "observed_at": now.astimezone(timezone.utc).isoformat(),
            "workers": workers,
        }
    except Exception:
        raise ProtocolError() from None


def managed_spec(
    binding: dict,
    *,
    image: str,
    network_id: str,
    manager_node: str,
    pins_secret_id: str,
    manager_node_id: str,
) -> dict:
    validate_binding(binding)
    require(image.endswith(":deploy-" + binding["release_commit"][:12]))
    require(matches(pins_secret_id, r"[a-z0-9]{25}"))
    spec = _job_spec(
        attempt_id=binding["attempt_id"],
        transaction_id=binding["transaction_id"],
        files=binding["files"],
        credentials=binding["credentials"],
        image=image,
        network_id=network_id,
        manager_node=manager_node,
        manager_node_id=manager_node_id,
    )
    container = spec["TaskTemplate"]["ContainerSpec"]
    spec["Name"] = "vp-registered-reconcile-" + binding["attempt_id"]
    container["Args"][-1] = "--run"
    container["Secrets"].append(
        {
            "SecretID": pins_secret_id,
            "SecretName": "vp-registered-pins-" + binding["attempt_id"],
            "File": {
                "Name": "registered-reconcile-pins",
                "UID": "10001",
                "GID": "10001",
                "Mode": 0o400,
            },
        }
    )
    return spec


def capture_spec(*, capture_read: dict, **arguments: Any) -> dict:
    validate_capture_read(capture_read)
    require(capture_read["name"] == "vp-registered-read-" + arguments["transaction_id"])
    spec = _job_spec(**arguments)
    spec["TaskTemplate"]["ContainerSpec"]["Secrets"].append(
        {
            "SecretID": capture_read["id"],
            "SecretName": capture_read["name"],
            "File": {
                "Name": "registered-reconcile-capture-read",
                "UID": "10001",
                "GID": "10001",
                "Mode": 0o400,
            },
        }
    )
    return spec


def validate_capture_read(value: dict) -> None:
    exact(value, {"id", "name", "sha256", "principal"})
    require(matches(value["id"], r"[a-z0-9]{25}"))
    require(matches(value["name"], r"vp-registered-read-tx-[0-9a-f]{32}"))
    require(matches(value["sha256"], r"[0-9a-f]{64}"))
    require(matches(value["principal"], r"[A-Za-z_][A-Za-z0-9_.$@-]{0,127}"))


def _job_spec(
    *,
    attempt_id: str,
    transaction_id: str,
    files: dict,
    credentials: dict,
    image: str,
    network_id: str,
    manager_node: str,
    manager_node_id: str,
) -> dict:
    require(matches(image, r"[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}"))
    require(matches(network_id, r"[a-z0-9]{25}"))
    require(str(UUID(attempt_id)) == attempt_id)
    require(matches(transaction_id, r"tx-[0-9a-f]{32}"))
    require(matches(manager_node, r"[A-Za-z0-9][A-Za-z0-9.-]{0,62}"))
    require(matches(manager_node_id, r"[a-z0-9]{25}"))
    directory = str(Path(files["request"]["path"]).parent)
    require("," not in directory and "\n" not in directory)
    require(
        matches(credentials.get("redis_secret_name"), r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
    )
    secret_refs = [
        (
            credentials["database_secret_id"],
            "vp-wc-operator-" + credentials["control_generation"],
            "database-url",
        ),
        (
            credentials["redis_secret_id"],
            credentials["redis_secret_name"],
            "redis-url",
        ),
    ]
    require(all(matches(sid, r"[a-z0-9]{25}") for sid, _, _ in secret_refs))
    return {
        "Name": "vp-registered-capture-" + attempt_id,
        "Labels": {
            "vp.service": "registered-reconcile",
            "vp.generation": transaction_id,
            "vp.attempt": attempt_id,
            "vp.manager-node-id": manager_node_id,
        },
        "Mode": {"ReplicatedJob": {"TotalCompletions": 1, "MaxConcurrent": 1}},
        "TaskTemplate": {
            "ContainerSpec": {
                "Image": image,
                "User": "10001:10001",
                "Groups": [],
                "Env": [],
                "Command": ["python"],
                "Args": [
                    "-m",
                    "app.services.registered_consumer_reconcile_job",
                    "--capture",
                ],
                "ReadOnly": True,
                "Healthcheck": {"Test": ["NONE"]},
                "Mounts": [
                    {
                        "Type": "bind",
                        "Source": directory + "/input.json",
                        "Target": "/run/registered-input.json",
                        "ReadOnly": True,
                    },
                    {
                        "Type": "bind",
                        "Source": files["request"]["path"],
                        "Target": "/run/registered-requests",
                        "ReadOnly": False,
                    },
                    {
                        "Type": "bind",
                        "Source": files["replies"]["path"],
                        "Target": "/run/registered-replies",
                        "ReadOnly": True,
                    },
                ],
                "Secrets": [
                    {
                        "SecretID": sid,
                        "SecretName": name,
                        "File": {
                            "Name": "registered-reconcile-" + suffix,
                            "UID": "10001",
                            "GID": "10001",
                            "Mode": 0o400,
                        },
                    }
                    for sid, name, suffix in secret_refs
                ],
            },
            "RestartPolicy": {"Condition": "none"},
            "Placement": {
                "Constraints": [
                    "node.hostname==" + manager_node,
                    "node.id==" + manager_node_id,
                ]
            },
            "Networks": [{"Target": network_id}],
        },
    }


def write_input(path: Path, value: dict) -> dict:
    descriptor = None
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
        )
        _write_all(descriptor, canonical(value))
        os.fchmod(descriptor, 0o644)
        os.fsync(descriptor)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return _metadata(path)
    except OSError:
        raise ProtocolError() from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def read_input(path: Path, metadata: dict | None = None) -> dict:
    descriptor = open_checked(metadata or _metadata(path), os.O_RDONLY, 0o644)
    try:
        raw = os.read(descriptor, MAX_BYTES + 1)
        require(os.read(descriptor, 1) == b"")
        value = decode(raw)
        files = value.get("files", value.get("binding", {}).get("files"))
        require(files is not None)
        opened = os.fstat(descriptor)
        require(
            (opened.st_uid, opened.st_gid)
            == (files["request"]["uid"], files["request"]["gid"])
        )
        return value
    finally:
        os.close(descriptor)


async def read_snapshot(connection: Any) -> dict:
    from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration

    registration_fields = (
        "id",
        "grant_id",
        "service_name",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "image_identity",
        "database_principal",
        "worker_instance_id",
        "worker_slot",
        "redis_consumer_id",
        "lease_epoch",
        "registered_at",
        "database_fingerprint",
        "redis_fingerprint",
        "storage_fingerprint",
        "status",
        "lease_expires_at",
        "revoked_at",
        "revoke_reason",
        "superseded_by",
    )
    grant_fields = (
        "id",
        "service_name",
        "generation",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "release_commit",
        "image_identity",
        "database_principal",
        "redis_stream",
        "redis_group",
        "endpoint_bindings_json",
        "state",
        "activated_at",
        "revoked_at",
        "revoke_reason",
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        now = await connection.fetchval("SELECT transaction_timestamp()")
        collections = []
        for table, model, fields, state in (
            ("worker_registrations", WorkerRegistration, registration_fields, "status"),
            ("worker_admission_grants", WorkerAdmissionGrant, grant_fields, "state"),
        ):
            rows = await connection.fetch(
                "SELECT "
                + ",".join(fields)
                + " FROM public."
                + table
                + " WHERE service_name=ANY($1::text[]) AND "
                + state
                + "='active' ORDER BY service_name,id",
                sorted(SERVICES),
            )
            values = []
            for row in rows:
                data = dict(row)
                for key in ("capabilities_json", "endpoint_bindings_json"):
                    if key in data and isinstance(data[key], str):
                        data[key] = json.loads(data[key], object_pairs_hook=_pairs)
                values.append(model(**data))
            collections.append(values)
        return capture_snapshot(*collections, now=now)


def build_capture_pins(
    snapshot: dict,
    baseline: dict,
    *,
    transaction_id: str,
    revision: int,
    release_commit: str,
) -> dict:
    from app.services.registered_consumer_reconcile import EvalCommand, decode_pins

    try:
        for value in (snapshot, baseline):
            exact(value, {"observed_at", "workers"})
            require(type(value["workers"]) is list and len(value["workers"]) == 4)
        pins = decode_pins(
            json.dumps(
                dict(
                    version=1,
                    transaction_id=transaction_id,
                    revision=revision,
                    release_commit=release_commit,
                    workers=[
                        dict(current=current, predecessor=old)
                        for current, old in zip(
                            snapshot["workers"], baseline["workers"], strict=True
                        )
                    ],
                )
            )
        )
        return dict(
            pin_json=pins.canonical_json,
            pin_sha256=pins.sha256,
            commands={
                STREAMS[worker.current.service_name]: digest(
                    list(EvalCommand(pins, worker.current.service_name).arguments)
                )
                for worker in pins.workers
                if worker.current.service_name in STREAMS
                and worker.predecessor is not None
            },
        )
    except Exception:
        raise ProtocolError() from None


def validate_managed_spec(actual: dict, expected: dict) -> None:
    """Reject changed security surfaces; permit only inert Docker defaults."""
    actual = copy.deepcopy(actual)
    try:
        require(actual.pop("EndpointSpec", {}) in ({}, {"Mode": "vip"}))
        task = actual["TaskTemplate"]
        require(task.pop("ForceUpdate", 0) == 0)
        for key in ("Resources", "LogDriver"):
            require(task.pop(key, {}) == {})
        container = task["ContainerSpec"]
        for key in ("Groups", "Env"):
            if not container.get(key):
                container[key] = []
        for key in ("Isolation", "Init"):
            require(container.pop(key, None) in (None, False, "default"))
        require(canonical(actual) == canonical(expected))
    except (KeyError, TypeError):
        raise ProtocolError() from None


def task_exit(task: dict, service_id: str, spec: dict) -> int | None:
    try:
        require(
            matches(task["ID"], r"[a-z0-9]{25}") and task["ServiceID"] == service_id
        )
        validate_managed_spec({**spec, "TaskTemplate": task["Spec"]}, spec)
        state = task["Status"]["State"]
        if state not in {"new", "allocated", "pending"}:
            require(task["NodeID"] == spec["Labels"]["vp.manager-node-id"])
        if state in {
            "new",
            "allocated",
            "pending",
            "assigned",
            "accepted",
            "preparing",
            "ready",
            "starting",
            "running",
        }:
            return None
        require(state in {"complete", "failed"})
        code = task["Status"]["ContainerStatus"]["ExitCode"]
        require(
            type(code) is int and 0 <= code <= 255 and (state != "failed" or code != 0)
        )
        return code
    except (KeyError, TypeError):
        raise ProtocolError() from None


def retain_managed_files(files: dict, input_file: dict) -> None:
    """After verified task/service absence, retire only the exact bound names."""
    records = [
        (input_file, 0o644, False),
        (files["request"], 0o602, False),
        (files["replies"], 0o755, True),
    ]
    parent = Path(input_file["path"]).parent
    require({Path(item[0]["path"]).parent for item in records} == {parent})
    metadata = parent.lstat()
    require(
        stat.S_ISDIR(metadata.st_mode)
        and stat.S_IMODE(metadata.st_mode) == 0o700
        and (metadata.st_uid, metadata.st_gid) == (os.getuid(), os.getgid())
        and parent.resolve() == parent
    )
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for record, mode, directory in records:
            original = Path(record["path"])
            retained = parent / ("retained-" + original.name)
            if retained.exists() or retained.is_symlink():
                require(not original.exists() and not original.is_symlink())
                fd = open_checked(
                    {**record, "path": str(retained)},
                    os.O_RDONLY,
                    mode,
                    directory=directory,
                )
                os.close(fd)
            else:
                fd = open_checked(record, os.O_RDONLY, mode, directory=directory)
                os.close(fd)
                os.rename(
                    original.name,
                    retained.name,
                    src_dir_fd=descriptor,
                    dst_dir_fd=descriptor,
                )
                fd = open_checked(
                    {**record, "path": str(retained)},
                    os.O_RDONLY,
                    mode,
                    directory=directory,
                )
                os.close(fd)
            os.fsync(descriptor)
    except OSError:
        raise ProtocolError() from None
    finally:
        os.close(descriptor)


async def run_managed(binding: dict, files: dict) -> dict:
    from dataclasses import asdict
    from app.services.registered_consumer_reconcile import decode_pins
    from app.services.registered_consumer_reconcile_runtime import (
        Invocation,
        reconcile_registered_consumers,
    )

    validate_binding(binding)
    request = Invocation(
        pins=decode_pins(binding["pin_json"]),
        attempt_id=UUID(binding["attempt_id"]),
        replay_only=False,
        **binding["credentials"],
    )
    authority = FileAuthority(binding, files)
    result = await reconcile_registered_consumers(request, authority)
    await authority.finished(request, result.outcome)
    return asdict(result)


def _read_capture_mount() -> str:
    from app.services.worker_role_cli_common import read_secure_file

    path = Path("/run/secrets/registered-reconcile-capture-read")
    before = path.lstat()
    require((before.st_uid, before.st_gid) == (10001, 10001))
    result = read_secure_file(path, required_mode=0o400)
    after = path.lstat()
    fields = (
        "st_dev",
        "st_ino",
        "st_uid",
        "st_gid",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    require(all(getattr(before, key) == getattr(after, key) for key in fields))
    return result


def _capture_database_binding(raw: str, principal: str) -> dict:
    from urllib.parse import urlsplit
    from sqlalchemy.engine import make_url
    from app.services.worker_registration import _database_connection_identity
    from app.services.registered_consumer_reconcile_runtime import _line

    url = _line(raw)
    parsed = make_url(url)
    require(
        parsed.drivername in {"postgresql", "postgresql+asyncpg"}
        and bool(parsed.password)
        and parsed.port is not None
        and not parsed.query
        and not urlsplit(url).fragment
    )
    binding, actual_principal = _database_connection_identity(url)
    require(actual_principal == principal)
    return binding


async def capture_managed(value: dict) -> dict:
    import asyncpg  # type: ignore[import-untyped]
    from urllib.parse import unquote, urlsplit
    from app.services.registered_consumer_reconcile_runtime import (
        _read_mount,
        _line,
        FORBIDDEN_ENV,
    )
    from app.services.worker_control_role_cli import role_names_for_generation
    from app.services.worker_role_cli_common import asyncpg_url

    connection = None
    try:
        async with asyncio.timeout(15):
            require(not any(os.environ.get(name) for name in FORBIDDEN_ENV))
            credentials = exact(
                value["credentials"],
                {
                    "control_generation",
                    "redis_generation",
                    "redis_secret_name",
                    "database_secret_id",
                    "redis_secret_id",
                },
            )
            require(
                matches(credentials["redis_secret_name"], r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
            )
            database, redis = _read_mount("database"), _read_mount("redis")
            principal = role_names_for_generation(
                credentials["control_generation"]
            ).versioned["operator"]
            database_binding = _capture_database_binding(database, principal)
            parsed = urlsplit(_line(redis))
            username = unquote(parsed.username or "")
            require(
                parsed.scheme in {"redis", "rediss"}
                and bool(parsed.password)
                and matches(username, r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
                and username != "default"
            )
            connection = await asyncpg.connect(
                asyncpg_url(_line(database)), timeout=2, command_timeout=2
            )
            identity_query = "SELECT session_user, current_user, pg_catalog.current_database() AS database_name"
            identity = await connection.fetchrow(identity_query)
            require(
                identity["session_user"] == identity["current_user"] == principal
                and identity["database_name"] == database_binding["database"]
            )
            await connection.close(timeout=2)
            connection = None
            reader = value["capture_read"]
            validate_capture_read(reader)
            raw_read = _read_capture_mount()
            require(hashlib.sha256(raw_read.encode()).hexdigest() == reader["sha256"])
            # Existing endpoint vocabulary has no authority to equate different aliases.
            require(
                _capture_database_binding(raw_read, reader["principal"])
                == database_binding
            )
            connection = await asyncpg.connect(
                asyncpg_url(_line(raw_read)), timeout=2, command_timeout=2
            )
            identity = await connection.fetchrow(identity_query)
            require(
                identity["session_user"]
                == identity["current_user"]
                == reader["principal"]
                and identity["database_name"] == database_binding["database"]
            )
            snapshot = await read_snapshot(connection)
            result = dict(
                snapshot=snapshot,
                credentials={
                    **credentials,
                    "database_secret_sha256": hashlib.sha256(
                        database.encode()
                    ).hexdigest(),
                    "redis_secret_sha256": hashlib.sha256(redis.encode()).hexdigest(),
                    "redis_username": username,
                },
                pins=None,
            )
            if value["baseline"] is not None:
                result["pins"] = build_capture_pins(
                    snapshot,
                    value["baseline"],
                    transaction_id=value["transaction_id"],
                    revision=value["revision"],
                    release_commit=value["release_commit"],
                )
            return result
    except Exception:
        raise ProtocolError() from None
    finally:
        if connection is not None:
            await connection.close(timeout=2)


def _mounted_files(files: dict) -> dict:
    return {
        key: {
            **metadata,
            "path": "/run/registered-"
            + ("requests" if key == "request" else "replies"),
        }
        for key, metadata in files.items()
    }


def managed_entry(mode: str) -> None:
    value = read_input(Path("/run/registered-input.json"))
    files = _mounted_files(value.get("files", value.get("binding", {}).get("files")))
    require_writer_identity(files["request"], os.getuid(), os.getgid(), os.getgroups())
    if mode == "--run":
        exact(value, {"binding"})
        asyncio.run(run_managed(value["binding"], files))
    else:
        require(mode == "--capture")
        result = asyncio.run(capture_managed(value))
        descriptor = open_checked(files["request"], os.O_WRONLY | os.O_APPEND, 0o602)
        try:
            require(os.fstat(descriptor).st_size == 0)
            _write_all(descriptor, canonical(result))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


if __name__ == "__main__":
    try:
        if sys.argv[1:] in (["--run"], ["--capture"]):
            managed_entry(sys.argv[1])
            raise SystemExit(0)
        require(sys.argv[1:] == ["--file-exchange"])
        data = exact(
            decode(sys.stdin.buffer.read(MAX_BYTES + 1)), {"files", "request", "offset"}
        )
        _exchange(data["files"], data["request"], data["offset"])
        sys.stdout.write("ok\n")
    except SystemExit:
        raise
    except BaseException:
        raise SystemExit(1) from None
