from __future__ import annotations

import argparse
import asyncio

from app.channel_agent.runner import ChannelAgentRunner
from app.config import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ChannelOps queue worker")
    parser.add_argument("mode", choices=["once", "run"], nargs="?", default="once")
    parser.add_argument("--worker-id", default="channel-agent-runner")
    parser.add_argument("--poll-seconds", type=float, default=settings.channel_agent_runner_poll_seconds)
    args = parser.parse_args()

    runner = ChannelAgentRunner(worker_id=args.worker_id)
    if args.mode == "run":
        asyncio.run(runner.run_forever(poll_seconds=args.poll_seconds))
    else:
        asyncio.run(runner.run_once())


if __name__ == "__main__":
    main()
