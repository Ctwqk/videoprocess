from worker.handlers.base import BaseHandler


class SourceHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        # Source nodes are resolved by the orchestrator engine.
        # This handler should never be called.
        raise RuntimeError("Source nodes should be resolved by the orchestrator, not the worker")
