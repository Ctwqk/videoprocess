from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Mapping


PRODUCTION_DEPLOY_MODES = frozenset({"shared", "production"})


def assert_python_channelops_runner_admission(
    env: Mapping[str, str] | None = None,
) -> None:
    deploy_env = os.environ if env is None else env
    deploy_mode = deploy_env.get("DEPLOY_MODE", "").strip().lower()
    if deploy_mode in PRODUCTION_DEPLOY_MODES:
        raise RuntimeError("Go ChannelOps runner is the production owner")


def main() -> None:
    assert_python_channelops_runner_admission()

    from app.channel_agent.runner import ChannelAgentRunner
    from app.config import settings

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
