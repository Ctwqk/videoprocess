import os
import shutil
from pathlib import Path

from worker.handlers.base import BaseHandler
from worker.handlers.media_quality import MediaQualityService


class ExportHandler(BaseHandler):
    def __init__(self, quality_service: MediaQualityService | None = None):
        super().__init__()
        self.quality_service = quality_service or MediaQualityService(self)

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

        qa_result = await self.quality_service.qa_export(
            source_path=input_file,
            output_path=output_path,
            node_config=node_config,
        )
        if qa_result.repaired_path:
            shutil.copy2(qa_result.repaired_path, export_path)
            shutil.copy2(qa_result.repaired_path, output_path)

        return {"quality_report": qa_result.report}
