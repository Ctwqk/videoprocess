from worker.handlers.base import BaseHandler
from worker.handlers.concat_stack import execute_stack_concat


class ConcatHorizontalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        await execute_stack_concat(
            self,
            output_path,
            input_paths["video_left"],
            input_paths["video_right"],
            primary_label="left",
            secondary_label="right",
            stack_axis="horizontal",
            resize_mode=node_config.get("resize_mode", "match_height"),
        )
