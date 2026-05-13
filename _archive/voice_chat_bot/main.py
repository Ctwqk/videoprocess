import asyncio
import io
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
import numpy as np
import soundfile as sf
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from faster_whisper import WhisperModel

from get_model import get_current_model

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("voice-chat-bot")

APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8090"))

LLM_URL = os.getenv("LLM_URL", "http://192.168.20.2:52415/v1/chat/completions")
#LLM_MODEL = os.getenv("LLM_MODEL", "mlx-community/GLM-4.7-Flash-5bit")
LLM_MODEL = get_current_model()
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))
logger.info("resolved LLM model at startup: %s", LLM_MODEL)

TTS_WS_URL = os.getenv("TTS_WS_URL", "ws://127.0.0.1:8000/v1/tts/ws")
TTS_LANGUAGE = os.getenv("TTS_LANGUAGE", "zh-cn")
TTS_SPEAKER_WAV_PATH = os.getenv("TTS_SPEAKER_WAV_PATH", "")

STT_MODEL_NAME = os.getenv("STT_MODEL", "medium")
STT_DEVICE = os.getenv("STT_DEVICE", "cuda")
STT_COMPUTE_TYPE = os.getenv("STT_COMPUTE_TYPE", "float16")
STT_DEFAULT_LANGUAGE = os.getenv("STT_LANGUAGE", "zh")

SYSTEM_PROMPT = os.getenv(
    "SYSTEM_PROMPT",
    "你是一个简洁、友好的中文语音助手。只输出最终回答，不要输出任何思考过程、推理标签或<think>内容。回答尽量简短自然，适合直接语音播报。",
)


app = FastAPI(title="Streaming Voice Chat Bot", version="1.0.0")
stt_model: Optional[WhisperModel] = None
stt_model_lock = asyncio.Lock()
WEB_DIR = Path(__file__).parent / "web"

if WEB_DIR.exists():
    app.mount("/web", StaticFiles(directory=str(WEB_DIR)), name="web")


@dataclass
class SessionState:
    sample_rate: int = 16000
    stt_language: str = STT_DEFAULT_LANGUAGE
    tts_language: str = TTS_LANGUAGE
    speaker_wav_path: str = TTS_SPEAKER_WAV_PATH
    audio_buffer: bytearray = field(default_factory=bytearray)


class ThinkTagFilter:
    def __init__(self) -> None:
        self.in_think = False
        self.buffer = ""

    def feed(self, text: str) -> str:
        self.buffer += text
        out: list[str] = []

        while self.buffer:
            if self.in_think:
                end = self.buffer.find("</think>")
                if end == -1:
                    if len(self.buffer) > 8:
                        self.buffer = self.buffer[-8:]
                    return ""
                self.buffer = self.buffer[end + len("</think>") :]
                self.in_think = False
                continue

            start = self.buffer.find("<think>")
            if start == -1:
                keep = min(len("<think>") - 1, len(self.buffer))
                if len(self.buffer) > keep:
                    out.append(self.buffer[:-keep])
                    self.buffer = self.buffer[-keep:]
                return "".join(out)

            if start > 0:
                out.append(self.buffer[:start])
            self.buffer = self.buffer[start + len("<think>") :]
            self.in_think = True

        return "".join(out)

    def flush(self) -> str:
        if self.in_think:
            return ""
        out = self.buffer.replace("<think>", "").replace("</think>", "")
        self.buffer = ""
        return out


def parse_audio_bytes(raw: bytes, sample_rate: int) -> np.ndarray:
    if len(raw) < 4:
        return np.array([], dtype=np.float32)

    if raw.startswith(b"RIFF") or raw.startswith(b"fLaC"):
        with io.BytesIO(raw) as buf:
            data, sr = sf.read(buf, dtype="float32", always_2d=False)
            if isinstance(data, np.ndarray) and data.ndim > 1:
                data = data.mean(axis=1)
            if sr != sample_rate:
                logger.warning("input sample rate %s != configured %s", sr, sample_rate)
            return np.asarray(data, dtype=np.float32)

    pcm = np.frombuffer(raw, dtype=np.int16)
    if pcm.size == 0:
        return np.array([], dtype=np.float32)
    return (pcm.astype(np.float32) / 32768.0).copy()


def transcribe_audio(audio: np.ndarray, language: str) -> str:
    if stt_model is None:
        raise RuntimeError("STT model is not ready")

    segments, _ = stt_model.transcribe(
        audio,
        language=language,
        beam_size=5,
        vad_filter=True,
    )
    return " ".join(seg.text.strip() for seg in segments if seg.text.strip()).strip()


def load_stt_model_sync() -> WhisperModel:
    logger.info("loading STT model: %s", STT_MODEL_NAME)
    model = WhisperModel(STT_MODEL_NAME, device=STT_DEVICE, compute_type=STT_COMPUTE_TYPE)
    logger.info("STT model loaded")
    return model


async def ensure_stt_model() -> WhisperModel:
    global stt_model
    if stt_model is not None:
        return stt_model

    async with stt_model_lock:
        if stt_model is None:
            stt_model = await asyncio.to_thread(load_stt_model_sync)
    return stt_model


async def stream_llm_to_tts_and_client(
    client_ws: WebSocket,
    user_text: str,
    tts_language: str,
    speaker_wav_path: str,
) -> None:
    query = f"language={tts_language}"
    if speaker_wav_path:
        query += f"&speaker_wav_path={speaker_wav_path}"

    tts_url = f"{TTS_WS_URL}?{query}"

    async with websockets.connect(tts_url, max_size=None) as tts_ws:
        async def forward_tts_audio() -> None:
            try:
                while True:
                    msg = await tts_ws.recv()
                    if isinstance(msg, bytes):
                        await client_ws.send_bytes(msg)
                    else:
                        await client_ws.send_json({"type": "tts_event", "data": msg})
            except Exception:
                return

        tts_forward_task = asyncio.create_task(forward_tts_audio())
        think_filter = ThinkTagFilter()

        payload: dict[str, Any] = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            "max_tokens": LLM_MAX_TOKENS,
            "stream": True,
        }
        logger.info(
            "sending LLM request: url=%s model=%s stream=%s",
            LLM_URL,
            payload["model"],
            payload["stream"],
        )

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("POST", LLM_URL, json=payload) as resp:
                if resp.status_code >= 400:
                    err_body = (await resp.aread()).decode("utf-8", errors="replace")
                    logger.error(
                        "LLM request failed: status=%s url=%s model=%s body=%s",
                        resp.status_code,
                        LLM_URL,
                        payload["model"],
                        err_body[:2000],
                    )
                resp.raise_for_status()

                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        continue

                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta = (choices[0].get("delta") or {}).get("content")
                    if not delta:
                        continue

                    clean_delta = think_filter.feed(delta)
                    if not clean_delta:
                        continue

                    await client_ws.send_json({"type": "llm_delta", "text": clean_delta})
                    await tts_ws.send(clean_delta)

        tail = think_filter.flush()
        if tail:
            await client_ws.send_json({"type": "llm_delta", "text": tail})
            await tts_ws.send(tail)

        await tts_ws.send("__flush__")
        await asyncio.sleep(0.05)
        await tts_ws.send("__close__")

        try:
            await asyncio.wait_for(tts_forward_task, timeout=5.0)
        except asyncio.TimeoutError:
            tts_forward_task.cancel()


@app.get("/health")
def health() -> dict[str, str | bool]:
    return {
        "status": "ok",
        "llm_url": LLM_URL,
        "tts_ws_url": TTS_WS_URL,
        "stt_model": STT_MODEL_NAME,
        "stt_ready": stt_model is not None,
    }


@app.get("/")
def index():
    index_file = WEB_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"message": "web ui not found"}


@app.websocket("/ws/voice")
async def ws_voice(ws: WebSocket) -> None:
    await ws.accept()
    state = SessionState()
    await ws.send_json(
        {
            "type": "ready",
            "protocol": {
                "audio_binary": "pcm_s16le or wav bytes",
                "commands": ["commit", "reset", "close"],
            },
        }
    )

    try:
        while True:
            msg = await ws.receive()

            if msg.get("type") == "websocket.disconnect":
                break

            text = msg.get("text")
            data = msg.get("bytes")

            if data is not None:
                state.audio_buffer.extend(data)
                continue

            if text is None:
                continue

            cmd = text.strip().lower()
            if cmd.startswith("{"):
                try:
                    obj = json.loads(text)
                    if obj.get("type") == "config":
                        if isinstance(obj.get("sample_rate"), int):
                            state.sample_rate = obj["sample_rate"]
                        if isinstance(obj.get("stt_language"), str):
                            state.stt_language = obj["stt_language"]
                        if isinstance(obj.get("tts_language"), str):
                            state.tts_language = obj["tts_language"]
                        if isinstance(obj.get("speaker_wav_path"), str):
                            state.speaker_wav_path = obj["speaker_wav_path"]
                        await ws.send_json({"type": "config_applied"})
                        continue
                except json.JSONDecodeError:
                    pass

            if cmd == "reset":
                state.audio_buffer.clear()
                await ws.send_json({"type": "reset_ok"})
                continue

            if cmd == "close":
                await ws.send_json({"type": "bye"})
                await ws.close()
                return

            if cmd != "commit":
                await ws.send_json({"type": "error", "detail": "unknown command"})
                continue

            raw_audio = bytes(state.audio_buffer)
            state.audio_buffer.clear()
            if not raw_audio:
                await ws.send_json({"type": "error", "detail": "empty audio buffer"})
                continue

            await ws.send_json({"type": "processing"})
            await ensure_stt_model()
            audio = parse_audio_bytes(raw_audio, state.sample_rate)
            if audio.size == 0:
                await ws.send_json({"type": "error", "detail": "audio parse failed"})
                continue

            user_text = await asyncio.to_thread(transcribe_audio, audio, state.stt_language)
            if not user_text:
                await ws.send_json({"type": "stt", "text": ""})
                await ws.send_json({"type": "done"})
                continue

            await ws.send_json({"type": "stt", "text": user_text})
            await stream_llm_to_tts_and_client(
                client_ws=ws,
                user_text=user_text,
                tts_language=state.tts_language,
                speaker_wav_path=state.speaker_wav_path,
            )
            await ws.send_json({"type": "done"})

    except WebSocketDisconnect:
        logger.info("client disconnected")
    except Exception as exc:
        logger.exception("ws error: %s", exc)
        try:
            await ws.send_json({"type": "error", "detail": str(exc)})
            await ws.close(code=1011)
        except Exception:
            return


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host=APP_HOST, port=APP_PORT, reload=False)
