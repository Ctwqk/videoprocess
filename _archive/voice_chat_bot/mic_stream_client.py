import argparse
import asyncio
import io
import json
import queue
import sys
from dataclasses import dataclass
from typing import Optional

import numpy as np
import sounddevice as sd
import soundfile as sf
import websockets


@dataclass
class VadConfig:
    sample_rate: int = 16000
    channels: int = 1
    chunk_ms: int = 20
    start_threshold: float = 0.015
    end_threshold: float = 0.008
    min_speech_ms: int = 300
    silence_ms_to_commit: int = 700
    max_utter_ms: int = 10000


class MicChunker:
    def __init__(self, cfg: VadConfig, input_device: Optional[int]):
        self.cfg = cfg
        self.input_device = input_device
        self.q: queue.Queue[np.ndarray] = queue.Queue()
        self.frames_per_chunk = int(cfg.sample_rate * cfg.chunk_ms / 1000)
        self.stream = sd.InputStream(
            samplerate=cfg.sample_rate,
            channels=cfg.channels,
            dtype="int16",
            device=input_device,
            blocksize=self.frames_per_chunk,
            callback=self._callback,
        )

    def _callback(self, indata, _frames, _time_info, status):
        if status:
            print(f"[mic-status] {status}", file=sys.stderr)
        self.q.put(indata.copy().reshape(-1))

    def start(self):
        self.stream.start()

    def stop(self):
        self.stream.stop()
        self.stream.close()


def play_wav_bytes(wav_bytes: bytes, output_device: Optional[int]) -> None:
    audio, sr = sf.read(io.BytesIO(wav_bytes), dtype="float32", always_2d=False)
    if isinstance(audio, np.ndarray) and audio.ndim > 1:
        audio = audio.mean(axis=1)
    sd.play(audio, sr, device=output_device)
    sd.wait()


async def ws_recv_and_play(ws, output_device: Optional[int], enable_playback: bool) -> None:
    while True:
        msg = await ws.recv()
        if isinstance(msg, bytes):
            if enable_playback:
                # Run playback in worker thread to avoid blocking asyncio loop and websocket keepalive.
                await asyncio.to_thread(play_wav_bytes, msg, output_device)
            continue

        try:
            obj = json.loads(msg)
        except json.JSONDecodeError:
            print(msg)
            continue

        t = obj.get("type")
        if t == "stt":
            print(f"\n[you] {obj.get('text', '')}")
        elif t == "llm_delta":
            print(obj.get("text", ""), end="", flush=True)
        elif t == "done":
            print("\n[done]")
            return
        elif t == "error":
            print(f"\n[error] {obj.get('detail')}")
            return


def drain_queue(q: queue.Queue) -> None:
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            return


async def run_loop(args):
    cfg = VadConfig(
        sample_rate=args.sample_rate,
        channels=1,
        chunk_ms=args.chunk_ms,
        start_threshold=args.start_threshold,
        end_threshold=args.end_threshold,
        min_speech_ms=args.min_speech_ms,
        silence_ms_to_commit=args.silence_ms_to_commit,
        max_utter_ms=args.max_utter_ms,
    )

    mic = MicChunker(cfg, args.input_device)
    mic.start()

    print("[info] started mic monitoring. Ctrl+C to stop.")
    print("[info] speak naturally; auto-commit on silence.")

    frames_min_speech = int(cfg.min_speech_ms / cfg.chunk_ms)
    frames_silence_commit = int(cfg.silence_ms_to_commit / cfg.chunk_ms)
    frames_max_utter = int(cfg.max_utter_ms / cfg.chunk_ms)

    speaking = False
    speech_frames: list[bytes] = []
    speech_count = 0
    silence_count = 0

    try:
        async with websockets.connect(
            args.ws,
            max_size=None,
            ping_interval=20,
            ping_timeout=120,
            close_timeout=20,
        ) as ws:
            ready = await ws.recv()
            print(f"[server] {ready}")

            config_msg = {
                "type": "config",
                "sample_rate": cfg.sample_rate,
                "stt_language": args.stt_language,
                "tts_language": args.tts_language,
            }
            if args.speaker_wav_path:
                config_msg["speaker_wav_path"] = args.speaker_wav_path
            await ws.send(json.dumps(config_msg))

            while True:
                try:
                    chunk = mic.q.get(timeout=0.1)
                except queue.Empty:
                    await asyncio.sleep(0)
                    continue

                rms = float(np.sqrt(np.mean((chunk.astype(np.float32) / 32768.0) ** 2)))

                if not speaking:
                    if rms >= cfg.start_threshold:
                        speaking = True
                        speech_frames = [chunk.tobytes()]
                        speech_count = 1
                        silence_count = 0
                    continue

                speech_frames.append(chunk.tobytes())
                speech_count += 1

                if rms < cfg.end_threshold:
                    silence_count += 1
                else:
                    silence_count = 0

                should_commit = False
                if speech_count >= frames_max_utter:
                    should_commit = True
                elif speech_count >= frames_min_speech and silence_count >= frames_silence_commit:
                    should_commit = True

                if not should_commit:
                    continue

                payload = b"".join(speech_frames)
                speaking = False
                speech_frames = []
                speech_count = 0
                silence_count = 0

                await ws.send(payload)
                await ws.send("commit")
                await ws_recv_and_play(ws, args.output_device, not args.no_playback)

                if args.drop_captured_during_reply:
                    drain_queue(mic.q)

    finally:
        mic.stop()


def list_devices() -> None:
    print(sd.query_devices())


def parse_args():
    p = argparse.ArgumentParser(description="Mic -> voice-chat-bot -> output device")
    p.add_argument("--ws", default="ws://localhost:8090/ws/voice")
    p.add_argument("--list-devices", action="store_true")
    p.add_argument("--input-device", type=int, default=None)
    p.add_argument("--output-device", type=int, default=None)
    p.add_argument("--sample-rate", type=int, default=16000)
    p.add_argument("--chunk-ms", type=int, default=20)
    p.add_argument("--start-threshold", type=float, default=0.015)
    p.add_argument("--end-threshold", type=float, default=0.008)
    p.add_argument("--min-speech-ms", type=int, default=300)
    p.add_argument("--silence-ms-to-commit", type=int, default=700)
    p.add_argument("--max-utter-ms", type=int, default=10000)
    p.add_argument("--stt-language", default="zh")
    p.add_argument("--tts-language", default="zh-cn")
    p.add_argument("--speaker-wav-path", default="")
    p.add_argument("--no-playback", action="store_true", help="Receive TTS but do not play on local output device")
    p.add_argument(
        "--drop-captured-during-reply",
        action="store_true",
        default=True,
        help="Drop buffered capture after each reply to avoid loopback re-trigger (default: on)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.list_devices:
        list_devices()
        raise SystemExit(0)

    try:
        asyncio.run(run_loop(args))
    except KeyboardInterrupt:
        print("\n[info] stopped")
