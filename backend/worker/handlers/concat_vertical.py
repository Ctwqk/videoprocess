from worker.handlers.base import BaseHandler
from worker.handlers.concat_stack import execute_stack_concat


class ConcatVerticalHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        await execute_stack_concat(
            self,
            output_path,
            input_paths["video_top"],
            input_paths["video_bottom"],
            primary_label="top",
            secondary_label="bottom",
            stack_axis="vertical",
            resize_mode=node_config.get("resize_mode", "match_width"),
        )
