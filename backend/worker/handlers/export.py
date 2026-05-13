import os
import shutil
from pathlib import Path
from worker.handlers.base import BaseHandler


class ExportHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        input_file = input_paths["input"]
        output_dir = node_config.get("output_dir", "/tmp/vp_export")
        filename = node_config.get("filename", "")

        if not filename:
            filename = os.path.basename(input_file)

        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Copy to export destination
        export_path = os.path.join(output_dir, filename)
        shutil.copy2(input_file, export_path)

        # Also copy to output_path for artifact tracking
        shutil.copy2(input_file, output_path)
