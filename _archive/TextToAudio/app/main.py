import asyncio
import io
import os
import re
import uuid
import tempfile
from typing import Generator, Optional, Tuple

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import soundfile as sf
from TTS.api import TTS


MODEL_NAME = os.getenv("XTTS_MODEL_NAME", "tts_models/multilingual/multi-dataset/xtts_v2")
USE_GPU = os.getenv("USE_GPU", "true").lower() in {"1", "true", "yes", "on"}
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
DEFAULT_SPEAKER_WAV = os.getenv("DEFAULT_SPEAKER_WAV", "")

os.makedirs(OUTPUT_DIR, exist_ok=True)

app = FastAPI(title="XTTS v2 API", version="1.0.0")


tts_engine: Optional[TTS] = None
SENTENCE_ENDINGS = {"。", "！", "？", ".", "!", "?", "\n"}


class HealthResp(BaseModel):
    status: str
    model: str
    gpu: bool


async def resolve_reference_audio(
    speaker_wav: Optional[UploadFile],
) -> Tuple[str, Optional[str]]:
    if speaker_wav is not None:
        suffix = os.path.splitext(speaker_wav.filename or "ref.wav")[1] or ".wav"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_ref:
            tmp_ref.write(await speaker_wav.read())
            return tmp_ref.name, tmp_ref.name

    if not DEFAULT_SPEAKER_WAV or not os.path.isfile(DEFAULT_SPEAKER_WAV):
        raise HTTPException(
            status_code=400,
            detail="speaker_wav is required unless DEFAULT_SPEAKER_WAV is configured on server",
        )
    return DEFAULT_SPEAKER_WAV, None


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


def synthesize_to_wav(text: str, language: str, ref_path: str, output_path: str) -> None:
    if tts_engine is None:
        raise HTTPException(status_code=503, detail="model not ready")

    if not text.strip():
        raise HTTPException(status_code=400, detail="text is empty")

    try:
        tts_engine.tts_to_file(
            text=text,
            speaker_wav=ref_path,
            language=language,
            file_path=output_path,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"tts failed: {exc}") from exc


def get_output_sample_rate() -> int:
    if tts_engine is None:
        return 24000
    sample_rate = getattr(tts_engine.synthesizer, "output_sample_rate", None)
    if isinstance(sample_rate, int) and sample_rate > 0:
        return sample_rate
    return 24000


def synthesize_to_wav_bytes(text: str, language: str, ref_path: str) -> bytes:
    if tts_engine is None:
        raise HTTPException(status_code=503, detail="model not ready")
    if not text.strip():
        raise HTTPException(status_code=400, detail="text is empty")
    try:
        wav = tts_engine.tts(text=text, speaker_wav=ref_path, language=language)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"tts failed: {exc}") from exc

    with io.BytesIO() as buf:
        sf.write(buf, wav, get_output_sample_rate(), format="WAV")
        return buf.getvalue()


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
    global tts_engine
    try:
        tts_engine = TTS(MODEL_NAME)
        if USE_GPU:
            tts_engine = tts_engine.to("cuda")
    except Exception as exc:
        raise RuntimeError(f"failed to load model {MODEL_NAME}: {exc}") from exc


@app.get("/health", response_model=HealthResp)
def health() -> HealthResp:
    return HealthResp(status="ok", model=MODEL_NAME, gpu=USE_GPU)


@app.post("/v1/tts")
async def tts(
    text: str = Form(...),
    speaker_wav: Optional[UploadFile] = File(None),
    language: str = Form("en"),
    output_filename: Optional[str] = Form(None),
):
    ref_path, tmp_ref_path = await resolve_reference_audio(speaker_wav)

    file_name = output_filename or f"tts_{uuid.uuid4().hex}.wav"
    if not file_name.lower().endswith(".wav"):
        file_name += ".wav"
    output_path = os.path.join(OUTPUT_DIR, file_name)

    try:
        synthesize_to_wav(text=text, language=language, ref_path=ref_path, output_path=output_path)
    finally:
        if tmp_ref_path and os.path.exists(tmp_ref_path):
            os.remove(tmp_ref_path)

    return {
        "message": "ok",
        "file": file_name,
        "download_url": f"/v1/files/{file_name}",
    }


@app.post("/v1/tts/stream")
async def tts_stream(
    text: str = Form(...),
    speaker_wav: Optional[UploadFile] = File(None),
    language: str = Form("en"),
):
    ref_path, tmp_ref_path = await resolve_reference_audio(speaker_wav)
    tmp_audio_path: Optional[str] = None
    response: Optional[StreamingResponse] = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_audio:
            tmp_audio_path = tmp_audio.name
        synthesize_to_wav(text=text, language=language, ref_path=ref_path, output_path=tmp_audio_path)
        response = StreamingResponse(
            iter_file_and_cleanup(tmp_audio_path),
            media_type="audio/wav",
            headers={"Content-Disposition": 'inline; filename="tts_stream.wav"'},
        )
        return response
    finally:
        if tmp_ref_path and os.path.exists(tmp_ref_path):
            os.remove(tmp_ref_path)
        if response is None and tmp_audio_path and os.path.exists(tmp_audio_path):
            # If response creation fails before generator starts, clean up here.
            os.remove(tmp_audio_path)


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
        wav_bytes = await asyncio.to_thread(
            synthesize_to_wav_bytes,
            sentence,
            language,
            ref_path,
        )
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
