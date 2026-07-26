from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

import redis.asyncio as redis


VISION_STREAM = "vp:tasks:vision"
VISION_GROUP = "vision-workers"
MANAGED_VISION_CONSUMER = re.compile(r"^vision-worker@150-vision:[1-9][0-9]*$")
_MANAGED_VISION_CONSUMER_LUA = "^vision%-worker@150%-vision:[1-9][0-9]*$"
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


async def run(*, check_only: bool = False) -> int:
    redis_url = os.environ.get("REDIS_URL", "")
    if not redis_url:
        return 2
    try:
        wait_attempts = int(os.environ.get("VISION_CUTOVER_WAIT_ATTEMPTS", "60"))
    except ValueError:
        return 2

    client = redis.from_url(redis_url, decode_responses=True)
    try:
        if check_only:
            converged = await vision_consumers_converged(client)
            print(json.dumps({"converged": converged}, sort_keys=True))
            return 0 if converged else 10
        result = await reconcile_vision_consumers(client, wait_attempts=wait_attempts)
    except (ValueError, VisionConsumerCutoverError, redis.RedisError):
        return 1
    finally:
        await client.aclose()
    print(json.dumps(result, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="vision-consumer-cutover")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    return asyncio.run(run(check_only=args.check_only))


if __name__ == "__main__":
    raise SystemExit(main())
