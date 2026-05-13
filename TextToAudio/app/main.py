import asyncio
import hashlib
import io
import json
import logging
import os
import re
import uuid
import tempfile
from pathlib import Path
from typing import Generator, Optional, Tuple
from threading import Lock

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import soundfile as sf
from TTS.api import TTS
import torch


MODEL_NAME = os.getenv("XTTS_MODEL_NAME", "tts_models/multilingual/multi-dataset/xtts_v2")
USE_GPU = os.getenv("USE_GPU", "true").lower() in {"1", "true", "yes", "on"}
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
DEFAULT_SPEAKER_WAV = os.getenv("DEFAULT_SPEAKER_WAV", "")
SPEAKER_CACHE_DIR = os.getenv("SPEAKER_CACHE_DIR", "/app/voicesource/cache")
PRELOAD_ON_START = os.getenv("XTTS_PRELOAD_ON_START", "false").lower() in {"1", "true", "yes", "on"}

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SPEAKER_CACHE_DIR, exist_ok=True)

app = FastAPI(title="XTTS v2 API", version="1.0.0")
logger = logging.getLogger(__name__)


tts_engine: Optional[TTS] = None
tts_engine_device: Optional[str] = None
_xtts_lock = Lock()
SENTENCE_ENDINGS = {"。", "！", "？", ".", "!", "?", "\n"}


class HealthResp(BaseModel):
    status: str
    model: str
    gpu: bool
    device: str


def _speaker_meta_path(audio_path: str) -> Path:
    return Path(f"{audio_path}.meta.json")


def _load_speaker_meta(audio_path: str) -> dict:
    meta_path = _speaker_meta_path(audio_path)
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_speaker_meta(audio_path: str, meta: dict) -> None:
    meta_path = _speaker_meta_path(audio_path)
    meta_path.write_text(json.dumps(meta, ensure_ascii=True, indent=2), encoding="utf-8")


def resolve_tts_device() -> str:
    configured_device = (os.environ.get("TTS_DEVICE", "auto") or "auto").strip().lower()
    if configured_device in {"cpu", "cuda", "mps"}:
        return configured_device
    if USE_GPU and torch.cuda.is_available():
        return "cuda"
    if USE_GPU and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def get_xtts_engine() -> TTS:
    global tts_engine, tts_engine_device
    requested_device = resolve_tts_device()
    if tts_engine is not None and tts_engine_device == requested_device:
        return tts_engine
    with _xtts_lock:
        if tts_engine is not None and tts_engine_device == requested_device:
            return tts_engine
        engine = TTS(MODEL_NAME)
        if requested_device != "cpu":
            engine = engine.to(requested_device)
        tts_engine = engine
        tts_engine_device = requested_device
        logger.info("Loaded XTTS provider: %s on %s", MODEL_NAME, requested_device)
        return tts_engine


def reset_xtts_engine() -> None:
    global tts_engine, tts_engine_device
    tts_engine = None
    tts_engine_device = None


async def resolve_reference_audio(
    speaker_wav: Optional[UploadFile],
    speaker_id: Optional[str] = None,
) -> Tuple[str, str]:
    if speaker_id:
        speaker_path = resolve_registered_speaker_path(speaker_id)
        speaker_text = str(_load_speaker_meta(speaker_path).get("speaker_text") or "").strip()
        return speaker_path, speaker_text

    if speaker_wav is not None:
        cached_path = await cache_uploaded_speaker_wav(speaker_wav)
        speaker_text = str(_load_speaker_meta(cached_path).get("speaker_text") or "").strip()
        return cached_path, speaker_text

    if not DEFAULT_SPEAKER_WAV or not os.path.isfile(DEFAULT_SPEAKER_WAV):
        raise HTTPException(
            status_code=400,
            detail="speaker_wav is required unless DEFAULT_SPEAKER_WAV is configured on server",
        )
    return DEFAULT_SPEAKER_WAV, str(_load_speaker_meta(DEFAULT_SPEAKER_WAV).get("speaker_text") or "").strip()


def resolve_registered_speaker_path(speaker_id: str) -> str:
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "", speaker_id)
    if not safe_id:
        raise HTTPException(status_code=400, detail="invalid speaker_id")
    matches = sorted(
        path for path in Path(SPEAKER_CACHE_DIR).glob(f"{safe_id}.*")
        if path.suffix.lower() in {".wav", ".mp3", ".m4a", ".flac", ".ogg", ".webm"}
    )
    if not matches:
        raise HTTPException(status_code=404, detail=f"speaker_id not found: {speaker_id}")
    return str(matches[0])


async def cache_uploaded_speaker_wav(speaker_wav: UploadFile) -> str:
    suffix = os.path.splitext(speaker_wav.filename or "ref.wav")[1] or ".wav"
    data = await speaker_wav.read()
    if not data:
        raise HTTPException(status_code=400, detail="speaker_wav is empty")
    speaker_id = hashlib.sha256(data).hexdigest()
    target_path = Path(SPEAKER_CACHE_DIR) / f"{speaker_id}{suffix}"
    if not target_path.exists():
        target_path.write_bytes(data)
    return str(target_path)


def resolve_reference_path_for_ws(speaker_wav_path: Optional[str]) -> str:
    if speaker_wav_path:
        if not os.path.isfile(speaker_wav_path):
            raise HTTPException(status_code=400, detail=f"speaker_wav_path not found: {speaker_wav_path}")
        return speaker_wav_path

    if not DEFAULT_SPEAKER_WAV or not os.path.isfile(DEFAULT_SPEAKER_WAV):
        raise HTTPException(
            status_code=400,
            detail="speaker_wav is required unless DEFAULT_SPEAKER_WAV is configured on server",
        )
    return DEFAULT_SPEAKER_WAV


def synthesize_to_wav(text: str, language: str, ref_path: str, output_path: str) -> str:
    if not text.strip():
        raise HTTPException(status_code=400, detail="text is empty")
    try:
        engine = get_xtts_engine()
        engine.tts_to_file(
            text=text,
            speaker_wav=ref_path,
            language=language,
            file_path=output_path,
        )
        return "xtts"
    except Exception as exc:
        if resolve_tts_device() == "mps":
            logger.warning("XTTS on mps failed; retrying on cpu: %s", exc)
            original_device = (os.environ.get("TTS_DEVICE", "auto") or "auto").strip().lower()
            try:
                os.environ["TTS_DEVICE"] = "cpu"
                reset_xtts_engine()
                engine = get_xtts_engine()
                engine.tts_to_file(
                    text=text,
                    speaker_wav=ref_path,
                    language=language,
                    file_path=output_path,
                )
                return "xtts"
            except Exception as cpu_exc:
                raise HTTPException(status_code=500, detail=f"tts failed after mps->cpu fallback: {cpu_exc}") from cpu_exc
            finally:
                if original_device == "auto":
                    os.environ.pop("TTS_DEVICE", None)
                else:
                    os.environ["TTS_DEVICE"] = original_device
        raise HTTPException(status_code=500, detail=f"tts failed: {exc}") from exc


def get_output_sample_rate() -> int:
    if tts_engine is not None:
        sample_rate = getattr(tts_engine.synthesizer, "output_sample_rate", None)
        if isinstance(sample_rate, int) and sample_rate > 0:
            return sample_rate
    return 24000


def synthesize_to_wav_bytes(text: str, language: str, ref_path: str) -> tuple[bytes, str]:
    if not text.strip():
        raise HTTPException(status_code=400, detail="text is empty")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_audio:
        tmp_path = tmp_audio.name
    try:
        provider = synthesize_to_wav(text=text, language=language, ref_path=ref_path, output_path=tmp_path)
        with open(tmp_path, "rb") as handle:
            return handle.read(), provider
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def iter_file_and_cleanup(path: str, chunk_size: int = 64 * 1024) -> Generator[bytes, None, None]:
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    finally:
        if os.path.exists(path):
            os.remove(path)


def split_complete_sentences(buffer: str) -> Tuple[list[str], str]:
    sentences = []
    start = 0
    for idx, ch in enumerate(buffer):
        if ch in SENTENCE_ENDINGS:
            segment = buffer[start : idx + 1].strip()
            if segment:
                sentences.append(segment)
            start = idx + 1
    return sentences, buffer[start:]


@app.on_event("startup")
def load_model() -> None:
    logger.info(
        "TTS service starting with provider=xtts, use_gpu=%s, requested_device=%s, resolved_device=%s",
        USE_GPU,
        (os.environ.get("TTS_DEVICE", "auto") or "auto").strip().lower(),
        resolve_tts_device(),
    )
    if PRELOAD_ON_START:
        logger.info("Preloading XTTS engine during startup")
        try:
            get_xtts_engine()
        except Exception:
            logger.exception("XTTS preload failed during startup")
            raise


@app.get("/health", response_model=HealthResp)
def health() -> HealthResp:
    device = tts_engine_device or resolve_tts_device()
    return HealthResp(status="ok", model=MODEL_NAME, gpu=device != "cpu", device=device)


@app.post("/v1/tts")
async def tts(
    text: str = Form(...),
    speaker_wav: Optional[UploadFile] = File(None),
    speaker_id: Optional[str] = Form(None),
    language: str = Form("en"),
    output_filename: Optional[str] = Form(None),
):
    ref_path, _ = await resolve_reference_audio(speaker_wav, speaker_id)

    file_name = output_filename or f"tts_{uuid.uuid4().hex}.wav"
    if not file_name.lower().endswith(".wav"):
        file_name += ".wav"
    output_path = os.path.join(OUTPUT_DIR, file_name)

    provider = synthesize_to_wav(text=text, language=language, ref_path=ref_path, output_path=output_path)

    return {
        "message": "ok",
        "file": file_name,
        "download_url": f"/v1/files/{file_name}",
        "tts_provider": provider,
    }


@app.post("/v1/tts/stream")
async def tts_stream(
    text: str = Form(...),
    speaker_wav: Optional[UploadFile] = File(None),
    speaker_id: Optional[str] = Form(None),
    language: str = Form("en"),
):
    ref_path, _ = await resolve_reference_audio(speaker_wav, speaker_id)
    tmp_audio_path: Optional[str] = None
    response: Optional[StreamingResponse] = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_audio:
            tmp_audio_path = tmp_audio.name
        provider = synthesize_to_wav(text=text, language=language, ref_path=ref_path, output_path=tmp_audio_path)
        response = StreamingResponse(
            iter_file_and_cleanup(tmp_audio_path),
            media_type="audio/wav",
            headers={
                "Content-Disposition": 'inline; filename="tts_stream.wav"',
                "X-TTS-Provider": provider,
            },
        )
        return response
    finally:
        if response is None and tmp_audio_path and os.path.exists(tmp_audio_path):
            # If response creation fails before generator starts, clean up here.
            os.remove(tmp_audio_path)


@app.post("/v1/speakers/register")
async def register_speaker(
    speaker_wav: UploadFile = File(...),
    speaker_text: Optional[str] = Form(None),
):
    cached_path = await cache_uploaded_speaker_wav(speaker_wav)
    speaker_id = Path(cached_path).stem
    normalized_text = str(speaker_text or "").strip()
    _save_speaker_meta(cached_path, {"speaker_text": normalized_text})
    return {
        "speaker_id": speaker_id,
        "speaker_wav_path": cached_path,
        "speaker_text": normalized_text,
    }


@app.get("/v1/files/{file_name}")
def get_file(file_name: str):
    path = os.path.join(OUTPUT_DIR, file_name)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(path, media_type="audio/wav", filename=file_name)


@app.websocket("/v1/tts/ws")
async def tts_ws(websocket: WebSocket):
    await websocket.accept()

    language = websocket.query_params.get("language", "en")
    speaker_wav_path = websocket.query_params.get("speaker_wav_path")
    try:
        ref_path = resolve_reference_path_for_ws(speaker_wav_path)
    except HTTPException as exc:
        await websocket.send_json({"type": "error", "detail": exc.detail})
        await websocket.close(code=1008)
        return

    await websocket.send_json({"type": "ready", "language": language})
    buffer = ""

    async def emit_sentence(sentence: str) -> None:
        wav_bytes, provider = await asyncio.to_thread(
            synthesize_to_wav_bytes,
            sentence,
            language,
            ref_path,
        )
        await websocket.send_json({"type": "provider", "provider": provider})
        await websocket.send_bytes(wav_bytes)

    try:
        while True:
            msg = await websocket.receive_text()
            text = msg.strip()

            if text == "__flush__":
                if buffer.strip():
                    await emit_sentence(buffer.strip())
                    buffer = ""
                await websocket.send_json({"type": "flushed"})
                continue

            if text == "__close__":
                if buffer.strip():
                    await emit_sentence(buffer.strip())
                await websocket.send_json({"type": "done"})
                await websocket.close()
                return

            buffer += text
            ready_sentences, remain = split_complete_sentences(buffer)
            buffer = remain
            for sentence in ready_sentences:
                await emit_sentence(sentence)
    except WebSocketDisconnect:
        return
    except Exception as exc:
        await websocket.send_json({"type": "error", "detail": str(exc)})
        await websocket.close(code=1011)
