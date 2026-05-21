from worker.handlers.base import BaseHandler
from worker.handlers.source import SourceHandler
from worker.handlers.subtitle import SubtitleHandler
from worker.handlers.speech_to_subtitle import SpeechToSubtitleHandler
from worker.handlers.smart_trim import SmartTrimHandler
from worker.handlers.subtitle_translate import SubtitleTranslateHandler
from worker.handlers.subtitle_to_speech import SubtitleToSpeechHandler
from worker.handlers.url_download import UrlDownloadHandler
from worker.handlers.platform_publish import XUploadHandler, XiaohongshuUploadHandler
from worker.handlers.youtube_upload import YouTubeUploadHandler
from worker.handlers.material_library_ingest import MaterialLibraryIngestHandler

HANDLER_MAP: dict[str, type[BaseHandler]] = {
    "source": SourceHandler,
    "subtitle": SubtitleHandler,
    "speech_to_subtitle": SpeechToSubtitleHandler,
    "smart_trim": SmartTrimHandler,
    "subtitle_translate": SubtitleTranslateHandler,
    "subtitle_to_speech": SubtitleToSpeechHandler,
    "url_download": UrlDownloadHandler,
    "material_library_ingest": MaterialLibraryIngestHandler,
    "youtube_upload": YouTubeUploadHandler,
    "x_upload": XUploadHandler,
    "xiaohongshu_upload": XiaohongshuUploadHandler,
}
