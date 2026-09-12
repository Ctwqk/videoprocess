"""Inert fixed-file callback protocol; no deployment or Unit2 runtime entrypoint.

The owning shell must verify both locks and the actual managed-job descriptor
before the journal helper answers a request. File replies are not lock leases.
This module is stdlib-only so the host journal can reuse the exact wire parser.
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
        type(action) is str and action in {"revalidate", "before_eval", "after_eval"}
    )
    if action == "revalidate":
        require(
            all(frame[key] is None for key in ("stream", "command_sha256", "outcome"))
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


if __name__ == "__main__":
    # Private subprocess primitive only; this is not a reconcile/deployment CLI.
    try:
        require(sys.argv[1:] == ["--file-exchange"])
        data = exact(
            decode(sys.stdin.buffer.read(MAX_BYTES + 1)), {"files", "request", "offset"}
        )
        _exchange(data["files"], data["request"], data["offset"])
        sys.stdout.write("ok\n")
    except BaseException:
        raise SystemExit(1) from None
