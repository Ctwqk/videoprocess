from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import redis.asyncio as redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from worker.secret_config import WorkerSecretError, read_mode_0400_secret


VISION_STREAM = "vp:tasks:vision"
VISION_GROUP = "vision-workers"
VISION_CUTOVER_REDIS_URL_FILE = "VISION_CUTOVER_REDIS_URL_FILE"
VISION_CUTOVER_DATABASE_URL_FILE = "VISION_CUTOVER_DATABASE_URL_FILE"
MANAGED_VISION_CONSUMER = re.compile(r"^vision-worker@150-vision:[1-9][0-9]*$")
_MANAGED_VISION_CONSUMER_LUA = "^vision%-worker@150%-vision:[1-9][0-9]*$"
_CONFIGURATION_ERROR = "vision cutover configuration invalid"
_OPERATION_ERROR = "vision cutover operation failed"
_SAFETY_ERROR = "vision cutover safety check failed"
_SCHEDULE_SAFETY_QUERY = text(
    "SELECT state, guarded_job_id "
    "FROM runtime_schedules "
    "WHERE service_name = 'videoprocess'"
)
_ACTIVE_EXECUTIONS_QUERY = text(
    "SELECT count(*) "
    "FROM node_executions "
    "WHERE status::text IN ('QUEUED', 'RUNNING')"
)
_ATOMIC_RECONCILE_LUA = """
local records = redis.call("XINFO", "CONSUMERS", KEYS[1], ARGV[1])
local managed_name = nil
local managed_count = 0
local legacy_names = {}

for _, record in ipairs(records) do
  local name = nil
  local pending = nil
  for index = 1, #record, 2 do
    if record[index] == "name" then
      name = record[index + 1]
    elseif record[index] == "pending" then
      pending = tonumber(record[index + 1])
    end
  end
  if not name or not pending then
    return redis.error_reply("VISION_MALFORMED")
  end
  if pending ~= 0 then
    return redis.error_reply("VISION_PENDING")
  end
  if string.match(name, ARGV[2]) then
    managed_name = name
    managed_count = managed_count + 1
  else
    table.insert(legacy_names, name)
  end
end

if managed_count ~= 1 then
  return redis.error_reply("VISION_MANAGED_COUNT")
end

local result = {managed_name}
for _, name in ipairs(legacy_names) do
  local deleted_pending = redis.call(
    "XGROUP", "DELCONSUMER", KEYS[1], ARGV[1], name
  )
  if deleted_pending ~= 0 then
    return redis.error_reply("VISION_PENDING")
  end
  table.insert(result, name)
end
return result
"""


class VisionConsumerCutoverError(RuntimeError):
    """Raised when the vision consumer group cannot be reconciled safely."""


class VisionConsumerCutoverConfigError(RuntimeError):
    """A sanitized vision cutover credential configuration failure."""


def _read_credential_file(
    environment_name: str,
    *,
    label: str,
) -> str:
    path = os.environ.get(environment_name, "").strip()
    if not path:
        raise VisionConsumerCutoverConfigError
    try:
        value = read_mode_0400_secret(path, label=label)
    except (OSError, WorkerSecretError):
        raise VisionConsumerCutoverConfigError from None
    if value != value.strip() or "\n" in value or "\r" in value:
        raise VisionConsumerCutoverConfigError
    return value


def _load_redis_url() -> str:
    if os.environ.get("REDIS_URL", "").strip():
        raise VisionConsumerCutoverConfigError
    redis_url = _read_credential_file(
        VISION_CUTOVER_REDIS_URL_FILE,
        label="vision cutover Redis URL",
    )
    try:
        parsed = urlsplit(redis_url)
        port = parsed.port
        username = parsed.username
        password = parsed.password
    except (TypeError, UnicodeError, ValueError):
        raise VisionConsumerCutoverConfigError from None
    if (
        parsed.scheme not in {"redis", "rediss"}
        or not parsed.hostname
        or port is None
        or not 1 <= port <= 65535
        or not username
        or username == "default"
        or not password
    ):
        raise VisionConsumerCutoverConfigError
    return redis_url


def _load_database_url() -> str:
    if os.environ.get("DATABASE_URL", "").strip():
        raise VisionConsumerCutoverConfigError
    database_url = _read_credential_file(
        VISION_CUTOVER_DATABASE_URL_FILE,
        label="vision cutover database URL",
    )
    try:
        parsed = make_url(database_url)
    except Exception:
        raise VisionConsumerCutoverConfigError from None
    if (
        parsed.drivername != "postgresql+asyncpg"
        or not parsed.username
        or not parsed.host
        or not parsed.database
    ):
        raise VisionConsumerCutoverConfigError
    return database_url


def _validated_consumers(records: object) -> list[tuple[str, int]]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise VisionConsumerCutoverError("vision consumer records are malformed")

    validated: list[tuple[str, int]] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise VisionConsumerCutoverError("vision consumer records are malformed")
        name = record.get("name")
        pending = record.get("pending")
        if (
            not isinstance(name, str)
            or not name
            or type(pending) is not int
            or pending < 0
        ):
            raise VisionConsumerCutoverError("vision consumer records are malformed")
        validated.append((name, pending))
    return validated


async def vision_consumers_converged(client: Any) -> bool:
    consumers = _validated_consumers(
        await client.xinfo_consumers(VISION_STREAM, VISION_GROUP)
    )
    return (
        len(consumers) == 1
        and consumers[0][1] == 0
        and MANAGED_VISION_CONSUMER.fullmatch(consumers[0][0]) is not None
    )


async def vision_cutover_safe(engine: Any, client: Any) -> bool:
    async with engine.connect() as connection:
        schedule = (
            await connection.execute(_SCHEDULE_SAFETY_QUERY)
        ).one_or_none()
        active_executions = (
            await connection.execute(_ACTIVE_EXECUTIONS_QUERY)
        ).scalar_one()

    if (
        schedule is None
        or getattr(schedule, "state", None) != "CLOSED"
        or getattr(schedule, "guarded_job_id", None) is not None
        or type(active_executions) is not int
        or active_executions != 0
    ):
        return False

    pending = await client.xpending(VISION_STREAM, VISION_GROUP)
    groups = await client.xinfo_groups(VISION_STREAM)
    if not isinstance(pending, Mapping) or type(pending.get("pending")) is not int:
        return False
    if not isinstance(groups, Sequence) or isinstance(groups, (str, bytes)):
        return False

    matching_groups = [
        group
        for group in groups
        if isinstance(group, Mapping) and group.get("name") == VISION_GROUP
    ]
    if len(matching_groups) != 1:
        return False
    lag = matching_groups[0].get("lag")
    return pending["pending"] == 0 and type(lag) is int and lag == 0


async def reconcile_vision_consumers(
    client: Any,
    *,
    wait_attempts: int = 60,
    wait_delay_seconds: float = 1.0,
) -> dict[str, object]:
    if wait_attempts < 1 or wait_delay_seconds < 0:
        raise ValueError("invalid vision consumer wait settings")

    consumers: list[tuple[str, int]] = []
    managed: list[str] = []
    for attempt in range(wait_attempts):
        consumers = _validated_consumers(
            await client.xinfo_consumers(VISION_STREAM, VISION_GROUP)
        )
        if any(pending != 0 for _, pending in consumers):
            raise VisionConsumerCutoverError("vision consumer has pending work")
        managed = [name for name, _ in consumers if MANAGED_VISION_CONSUMER.fullmatch(name)]
        if len(managed) > 1:
            raise VisionConsumerCutoverError(
                "vision cutover requires exactly one managed consumer"
            )
        if len(managed) == 1:
            break
        if attempt + 1 < wait_attempts:
            await asyncio.sleep(wait_delay_seconds)
    else:
        raise VisionConsumerCutoverError(
            "vision cutover requires exactly one managed consumer"
        )

    try:
        atomic_result = await client.eval(
            _ATOMIC_RECONCILE_LUA,
            1,
            VISION_STREAM,
            VISION_GROUP,
            _MANAGED_VISION_CONSUMER_LUA,
        )
    except redis.ResponseError as exc:
        if "VISION_PENDING" in str(exc):
            raise VisionConsumerCutoverError("vision consumer has pending work") from None
        if "VISION_MANAGED_COUNT" in str(exc):
            raise VisionConsumerCutoverError(
                "vision cutover requires exactly one managed consumer"
            ) from None
        raise VisionConsumerCutoverError("vision consumer atomic cutover failed") from exc

    if (
        not isinstance(atomic_result, Sequence)
        or isinstance(atomic_result, (str, bytes))
        or not atomic_result
        or any(not isinstance(name, str) or not name for name in atomic_result)
    ):
        raise VisionConsumerCutoverError("vision consumer atomic cutover failed")
    managed_name = atomic_result[0]
    removed = list(atomic_result[1:])
    if MANAGED_VISION_CONSUMER.fullmatch(managed_name) is None:
        raise VisionConsumerCutoverError("vision consumer atomic cutover failed")

    final_consumers = _validated_consumers(
        await client.xinfo_consumers(VISION_STREAM, VISION_GROUP)
    )
    if final_consumers != [(managed_name, 0)]:
        raise VisionConsumerCutoverError("vision consumer cutover did not converge")
    return {
        "managed_consumer": managed_name,
        "removed_consumers": sorted(removed),
    }


async def run(*, check_only: bool = False, safety: bool = False) -> int:
    if check_only and safety:
        print(_CONFIGURATION_ERROR, file=sys.stderr)
        return 2
    try:
        redis_url = _load_redis_url()
        database_url = _load_database_url() if safety else None
        wait_attempts = (
            60
            if safety
            else int(os.environ.get("VISION_CUTOVER_WAIT_ATTEMPTS", "60"))
        )
    except (ValueError, VisionConsumerCutoverConfigError):
        print(_CONFIGURATION_ERROR, file=sys.stderr)
        return 2

    client: Any | None = None
    engine: Any | None = None
    status = 1
    output: dict[str, object] | None = None
    error_message: str | None = None
    try:
        client = redis.from_url(redis_url, decode_responses=True)
        if safety:
            if database_url is None:
                raise VisionConsumerCutoverConfigError
            engine = create_async_engine(database_url, pool_pre_ping=True)
            if await vision_cutover_safe(engine, client):
                status = 0
                output = {"safe": True}
            else:
                error_message = _SAFETY_ERROR
        elif check_only:
            converged = await vision_consumers_converged(client)
            status = 0 if converged else 10
            output = {"converged": converged}
        else:
            output = await reconcile_vision_consumers(
                client,
                wait_attempts=wait_attempts,
            )
            status = 0
    except Exception:
        error_message = _OPERATION_ERROR
    finally:
        cleanup_failed = False
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                cleanup_failed = True
        if engine is not None:
            try:
                await engine.dispose()
            except Exception:
                cleanup_failed = True
        if cleanup_failed:
            status = 1
            output = None
            error_message = _OPERATION_ERROR

    if output is not None:
        print(json.dumps(output, sort_keys=True))
    if error_message is not None:
        print(error_message, file=sys.stderr)
    return status


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="vision-consumer-cutover")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--safety",
        action="store_const",
        const="safety",
        dest="mode",
    )
    modes.add_argument(
        "--check-only",
        action="store_const",
        const="check-only",
        dest="mode",
    )
    modes.add_argument(
        "--reconcile",
        action="store_const",
        const="reconcile",
        dest="mode",
    )
    parser.set_defaults(mode="reconcile")
    args = parser.parse_args(argv)
    return asyncio.run(
        run(
            check_only=args.mode == "check-only",
            safety=args.mode == "safety",
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
