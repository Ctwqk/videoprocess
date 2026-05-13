import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List

import aiofiles
import yt_dlp
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Google Auth / YouTube Upload imports (gracefully optional at import time)
# ---------------------------------------------------------------------------
try:
    from google_auth_oauthlib.flow import Flow
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants & shared state
# ---------------------------------------------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
CREDENTIALS_DIR = "/app/credentials"
TOKEN_FILE = os.path.join(CREDENTIALS_DIR, "token.json")
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/app/downloads")

executor = ThreadPoolExecutor(max_workers=4)
tasks: dict = {}  # task_id -> task dict

# ---------------------------------------------------------------------------
# FastAPI app setup
# ---------------------------------------------------------------------------
app = FastAPI(title="YouTube Manager", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class SearchRequest(BaseModel):
    query: str
    max_results: int = 10


class DownloadRequest(BaseModel):
    url: str
    format: str = "best"


class UploadLocalRequest(BaseModel):
    filename: str
    title: str
    description: str = ""
    tags: str = ""
    privacy_status: str = "private"


# ---------------------------------------------------------------------------
# Helper: new task
# ---------------------------------------------------------------------------
def new_task(task_type: str) -> str:
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "id": task_id,
        "type": task_type,
        "status": "pending",
        "progress": 0,
        "result": None,
        "error": None,
    }
    return task_id


# ---------------------------------------------------------------------------
# yt-dlp helpers
# ---------------------------------------------------------------------------
def search_youtube(query: str, max_results: int = 10) -> list:
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "default_search": f"ytsearch{max_results}",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(query, download=False)
        entries = result.get("entries", [])
        videos = []
        for entry in entries:
            thumbnails = entry.get("thumbnails") or []
            thumbnail = thumbnails[-1].get("url") if thumbnails else None
            videos.append(
                {
                    "id": entry.get("id"),
                    "title": entry.get("title"),
                    "url": f"https://www.youtube.com/watch?v={entry.get('id')}",
                    "thumbnail": thumbnail,
                    "duration": entry.get("duration"),
                    "channel": entry.get("channel") or entry.get("uploader"),
                    "view_count": entry.get("view_count"),
                    "upload_date": entry.get("upload_date"),
                }
            )
        return videos


def download_video(url: str, task_id: str, format_str: str = "best") -> None:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    def progress_hook(d):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                tasks[task_id]["progress"] = int(downloaded / total * 100)
            tasks[task_id]["status"] = "downloading"
        elif d["status"] == "finished":
            tasks[task_id]["status"] = "completed"
            tasks[task_id]["progress"] = 100
            tasks[task_id]["result"] = {"filename": os.path.basename(d["filename"])}

    ydl_opts = {
        "format": format_str,
        "outtmpl": os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s"),
        "progress_hooks": [progress_hook],
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# Google OAuth helpers
# ---------------------------------------------------------------------------
def get_auth_flow() -> "Flow":
    if not GOOGLE_AVAILABLE:
        raise RuntimeError("google-auth-oauthlib is not installed")
    client_secrets = os.environ.get(
        "GOOGLE_CLIENT_SECRETS_FILE",
        os.path.join(CREDENTIALS_DIR, "client_secrets.json"),
    )
    redirect_uri = os.environ.get(
        "OAUTH_REDIRECT_URI", "http://localhost:8899/api/auth/callback"
    )
    flow = Flow.from_client_secrets_file(
        client_secrets, scopes=SCOPES, redirect_uri=redirect_uri
    )
    return flow


def get_credentials() -> Optional["Credentials"]:
    if not GOOGLE_AVAILABLE:
        return None
    if not os.path.exists(TOKEN_FILE):
        return None
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        os.makedirs(CREDENTIALS_DIR, exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds if (creds and creds.valid) else None


def upload_to_youtube(
    filepath: str,
    title: str,
    description: str,
    tags: List[str],
    privacy: str,
    task_id: str,
) -> None:
    creds = get_credentials()
    if not creds:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = "Not authenticated. Please authorize first."
        return

    try:
        youtube = build("youtube", "v3", credentials=creds)
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags,
            },
            "status": {
                "privacyStatus": privacy,
            },
        }
        media = MediaFileUpload(
            filepath, resumable=True, chunksize=10 * 1024 * 1024
        )
        request = youtube.videos().insert(
            part="snippet,status", body=body, media_body=media
        )

        tasks[task_id]["status"] = "uploading"
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tasks[task_id]["progress"] = int(status.progress() * 100)

        tasks[task_id]["status"] = "completed"
        tasks[task_id]["progress"] = 100
        tasks[task_id]["result"] = {
            "video_id": response["id"],
            "url": f"https://www.youtube.com/watch?v={response['id']}",
        }
    except Exception as e:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# Routes: General
# ---------------------------------------------------------------------------
@app.get("/api/tasks")
async def list_tasks():
    return {"tasks": list(tasks.values())}


# ---------------------------------------------------------------------------
# Routes: Search
# ---------------------------------------------------------------------------
@app.post("/api/search")
async def search(request: SearchRequest):
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    loop = asyncio.get_event_loop()
    try:
        results = await loop.run_in_executor(
            executor, search_youtube, request.query, request.max_results
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    return {"results": results}


# ---------------------------------------------------------------------------
# Routes: Download
# ---------------------------------------------------------------------------
@app.post("/api/download")
async def start_download(request: DownloadRequest):
    if not request.url.strip():
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    task_id = new_task("download")
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor, download_video, request.url, task_id, request.format
    )
    return {"task_id": task_id, "status": "pending"}


@app.get("/api/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return tasks[task_id]


@app.get("/api/downloads")
async def list_downloads():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    files = []
    for entry in Path(DOWNLOAD_DIR).iterdir():
        if entry.is_file():
            stat = entry.stat()
            files.append(
                {
                    "filename": entry.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                }
            )
    files.sort(key=lambda x: x["modified"], reverse=True)
    return {"files": files}


@app.get("/api/download/{filename}")
async def serve_download(filename: str):
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    # Security: ensure the path is inside DOWNLOAD_DIR
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    return FileResponse(real_path, filename=filename)


@app.delete("/api/download/{filename}")
async def delete_download(filename: str):
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    os.remove(real_path)
    return {"message": f"Deleted {filename}"}


# ---------------------------------------------------------------------------
# Routes: Auth
# ---------------------------------------------------------------------------
@app.get("/api/auth/url")
async def get_auth_url():
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    client_secrets = os.environ.get(
        "GOOGLE_CLIENT_SECRETS_FILE",
        os.path.join(CREDENTIALS_DIR, "client_secrets.json"),
    )
    if not os.path.exists(client_secrets):
        raise HTTPException(
            status_code=503,
            detail="client_secrets.json not found. Please place it in the credentials directory.",
        )
    try:
        flow = get_auth_flow()
        auth_url, _ = flow.authorization_url(
            access_type="offline", include_granted_scopes="true", prompt="consent"
        )
        return {"url": auth_url}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate auth URL: {str(e)}"
        )


@app.get("/api/auth/callback")
async def auth_callback(code: str):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    try:
        flow = get_auth_flow()
        flow.fetch_token(code=code)
        creds = flow.credentials
        os.makedirs(CREDENTIALS_DIR, exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Authorization failed: {str(e)}"
        )
    return {"message": "Authorization successful", "authenticated": True}


@app.get("/api/auth/status")
async def auth_status():
    if not GOOGLE_AVAILABLE:
        return {"authenticated": False, "reason": "Google libraries not available"}
    client_secrets = os.environ.get(
        "GOOGLE_CLIENT_SECRETS_FILE",
        os.path.join(CREDENTIALS_DIR, "client_secrets.json"),
    )
    has_secrets = os.path.exists(client_secrets)
    creds = get_credentials()
    return {
        "authenticated": creds is not None,
        "has_client_secrets": has_secrets,
        "token_exists": os.path.exists(TOKEN_FILE),
    }


@app.post("/api/auth/logout")
async def auth_logout():
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
    return {"message": "Logged out successfully"}


# ---------------------------------------------------------------------------
# Routes: Upload
# ---------------------------------------------------------------------------
@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(""),
    tags: str = Form(""),
    privacy_status: str = Form("private"),
):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    creds = get_credentials()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated. Please authorize with Google first.",
        )

    # Save uploaded file to a temp location inside downloads dir
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    safe_name = os.path.basename(file.filename or "upload")
    temp_path = os.path.join(DOWNLOAD_DIR, f"_upload_{uuid.uuid4()}_{safe_name}")
    try:
        async with aiofiles.open(temp_path, "wb") as f_out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                await f_out.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    task_id = new_task("upload")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        upload_to_youtube,
        temp_path,
        title,
        description,
        tag_list,
        privacy_status,
        task_id,
    )
    return {"task_id": task_id, "status": "pending"}


@app.post("/api/upload/local")
async def upload_local_file(request: UploadLocalRequest):
    if not GOOGLE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Google auth libraries not available"
        )
    creds = get_credentials()
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated. Please authorize with Google first.",
        )

    filepath = os.path.join(DOWNLOAD_DIR, request.filename)
    real_path = os.path.realpath(filepath)
    real_dir = os.path.realpath(DOWNLOAD_DIR)
    if not real_path.startswith(real_dir + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")
    if not os.path.exists(real_path):
        raise HTTPException(
            status_code=404, detail=f"File '{request.filename}' not found in downloads"
        )

    tag_list = [t.strip() for t in request.tags.split(",") if t.strip()]
    task_id = new_task("upload")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        upload_to_youtube,
        real_path,
        request.title,
        request.description,
        tag_list,
        request.privacy_status,
        task_id,
    )
    return {"task_id": task_id, "status": "pending"}
