from worker.handlers.base import BaseHandler
from worker.handlers.source import SourceHandler
from worker.handlers.trim import TrimHandler
from worker.handlers.concat_horizontal import ConcatHorizontalHandler
from worker.handlers.concat_vertical import ConcatVerticalHandler
from worker.handlers.concat_timeline import ConcatTimelineHandler
from worker.handlers.concat_vertical_timeline import ConcatVerticalTimelineHandler
from worker.handlers.watermark import WatermarkHandler
from worker.handlers.subtitle import SubtitleHandler
from worker.handlers.speech_to_subtitle import SpeechToSubtitleHandler
from worker.handlers.subtitle_translate import SubtitleTranslateHandler
from worker.handlers.subtitle_to_speech import SubtitleToSpeechHandler
from worker.handlers.bgm import BgmHandler
from worker.handlers.replace_audio import ReplaceAudioHandler
from worker.handlers.transcode import TranscodeHandler
from worker.handlers.url_download import UrlDownloadHandler
from worker.handlers.export import ExportHandler
from worker.handlers.platform_publish import XUploadHandler, XiaohongshuUploadHandler
from worker.handlers.youtube_upload import YouTubeUploadHandler
from worker.handlers.material_library_ingest import MaterialLibraryIngestHandler

HANDLER_MAP: dict[str, type[BaseHandler]] = {
    "source": SourceHandler,
    "trim": TrimHandler,
    "concat_horizontal": ConcatHorizontalHandler,
    "concat_vertical": ConcatVerticalHandler,
    "concat_timeline": ConcatTimelineHandler,
    "concat_vertical_timeline": ConcatVerticalTimelineHandler,
    "watermark": WatermarkHandler,
    "subtitle": SubtitleHandler,
    "speech_to_subtitle": SpeechToSubtitleHandler,
    "subtitle_translate": SubtitleTranslateHandler,
    "subtitle_to_speech": SubtitleToSpeechHandler,
    "bgm": BgmHandler,
    "replace_audio": ReplaceAudioHandler,
    "transcode": TranscodeHandler,
    "url_download": UrlDownloadHandler,
    "material_library_ingest": MaterialLibraryIngestHandler,
    "export": ExportHandler,
    "youtube_upload": YouTubeUploadHandler,
    "x_upload": XUploadHandler,
    "xiaohongshu_upload": XiaohongshuUploadHandler,
}
