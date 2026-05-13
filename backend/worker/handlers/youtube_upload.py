import logging
import os
import shutil
import json
from pathlib import Path
from datetime import datetime
from worker.handlers.base import BaseHandler

DAILY_UPLOAD_QUOTA_LIMIT = 10_000
UPLOAD_INSERT_COST = 1_600
logger = logging.getLogger("worker")


class YouTubeUploadHandler(BaseHandler):
    """Upload a video to YouTube using the YouTube Data API v3.

    Requires a valid OAuth2 credentials file at the path specified by
    the YOUTUBE_CREDENTIALS_DIR environment variable (default: ~/.youtube_credentials).
    """

    async def execute(self, node_config, input_paths, output_path):
        input_file = input_paths["input"]
        title = node_config.get("title", "Untitled")
        description = node_config.get("description", "")
        privacy = node_config.get("privacy", "private")
        made_for_kids = str(node_config.get("made_for_kids", "not_set") or "not_set").strip().lower()
        tags_str = node_config.get("tags", "")
        tags = [t.strip() for t in tags_str.split(",") if t.strip()] if tags_str else []

        # Try to upload via YouTube API
        cred_dir = os.environ.get("YOUTUBE_CREDENTIALS_DIR", os.path.expanduser("~/.youtube_credentials"))
        client_secret = None
        for f in Path(cred_dir).glob("client_secret*.json"):
            client_secret = str(f)
            break

        if not client_secret:
            raise RuntimeError(
                f"YouTube credentials not found in {cred_dir}. "
                "Mount a directory containing client_secret*.json and token.json."
            )

        upload_result = await self._upload_youtube(
            input_file, title, description, privacy, made_for_kids, tags, cred_dir, client_secret
        )

        # Copy input to output_path for artifact tracking
        shutil.copy2(input_file, output_path)
        return {
            "youtube": upload_result,
        }

    def _quota_file_path(self, cred_dir: str) -> Path:
        return Path(cred_dir) / "quota_usage.json"

    def _load_quota_usage(self, cred_dir: str) -> dict:
        today = datetime.utcnow().date().isoformat()
        quota_path = self._quota_file_path(cred_dir)
        default = {
            "date": today,
            "daily_limit": DAILY_UPLOAD_QUOTA_LIMIT,
            "estimated_units_used": 0,
            "estimated_upload_requests": 0,
            "last_video_id": None,
            "last_recorded_at": None,
        }
        if not quota_path.exists():
            return default
        try:
            data = json.loads(quota_path.read_text())
        except Exception:
            return default
        if data.get("date") != today:
            return default
        return {**default, **data}

    def _save_quota_usage(self, cred_dir: str, data: dict) -> None:
        quota_path = self._quota_file_path(cred_dir)
        quota_path.parent.mkdir(parents=True, exist_ok=True)
        quota_path.write_text(json.dumps(data, indent=2, sort_keys=True))

    def _record_quota_estimate(
        self,
        cred_dir: str,
        *,
        increment_units: bool = False,
        video_id: str | None = None,
    ) -> None:
        data = self._load_quota_usage(cred_dir)
        if increment_units:
            data["estimated_units_used"] = int(data.get("estimated_units_used", 0)) + UPLOAD_INSERT_COST
            data["estimated_upload_requests"] = int(data.get("estimated_upload_requests", 0)) + 1
        if video_id:
            data["last_video_id"] = video_id
        data["last_recorded_at"] = datetime.utcnow().isoformat() + "Z"
        self._save_quota_usage(cred_dir, data)

    def _enforce_quota_estimate(self, cred_dir: str) -> None:
        data = self._load_quota_usage(cred_dir)
        used = int(data.get("estimated_units_used", 0) or 0)
        limit = int(data.get("daily_limit", DAILY_UPLOAD_QUOTA_LIMIT) or DAILY_UPLOAD_QUOTA_LIMIT)
        projected = used + UPLOAD_INSERT_COST
        if projected > limit:
            raise RuntimeError(
                "Estimated YouTube upload quota would be exceeded "
                f"({used}/{limit} used, next upload costs {UPLOAD_INSERT_COST})."
            )

    async def _upload_youtube(
        self, video_path, title, description, privacy, made_for_kids, tags, cred_dir, client_secret
    ):
        """Upload using google-api-python-client."""
        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaFileUpload
        except ImportError:
            raise RuntimeError(
                "YouTube upload dependencies are missing in the worker image. "
                "Install google-api-python-client/google-auth-oauthlib for the worker."
            )

        token_path = os.path.join(cred_dir, "token.json")
        if not os.path.exists(token_path):
            raise RuntimeError(
                f"Missing OAuth token at {token_path}. "
                "Generate it via the YouTubeManager auth flow before using youtube_upload."
            )

        creds = Credentials.from_authorized_user_file(
            token_path,
            scopes=["https://www.googleapis.com/auth/youtube.upload"],
        )

        if not creds.valid:
            if creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            else:
                raise RuntimeError(
                    f"OAuth token at {token_path} is invalid and cannot be refreshed. "
                    "Re-authorize via YouTubeManager."
                )

        youtube = build("youtube", "v3", credentials=creds)

        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags,
                "categoryId": "22",  # People & Blogs
            },
            "status": {
                "privacyStatus": privacy,
            },
        }
        if made_for_kids == "yes":
            body["status"]["selfDeclaredMadeForKids"] = True
        elif made_for_kids == "no":
            body["status"]["selfDeclaredMadeForKids"] = False

        media = MediaFileUpload(video_path, mimetype="video/*", resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )
        self._enforce_quota_estimate(cred_dir)
        self._record_quota_estimate(cred_dir, increment_units=True)

        response = None
        while response is None:
            _, response = request.next_chunk()

        video_id = response.get("id")
        self._record_quota_estimate(cred_dir, video_id=video_id)

        logger.info("YouTube upload complete: video_id=%s", video_id)
        return {
            "video_id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}" if video_id else None,
            "title": title,
            "privacy": privacy,
            "made_for_kids": (
                True if made_for_kids == "yes"
                else False if made_for_kids == "no"
                else None
            ),
            "tags": tags,
            "quota_cost_estimate": UPLOAD_INSERT_COST,
        }
