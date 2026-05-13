import argparse
import asyncio
import json
import os
from pathlib import Path

import websockets


async def recv_loop(ws, out_dir: Path, done_evt: asyncio.Event) -> None:
    chunk_idx = 0
    while True:
        msg = await ws.recv()

        if isinstance(msg, bytes):
            chunk_idx += 1
            out_path = out_dir / f"reply_{chunk_idx:04d}.wav"
            out_path.write_bytes(msg)
            print(f"[audio] saved {out_path}")
            continue

        try:
            obj = json.loads(msg)
        except json.JSONDecodeError:
            print(f"[text] {msg}")
            continue

        t = obj.get("type")
        if t == "stt":
            print(f"[stt] {obj.get('text', '')}")
        elif t == "llm_delta":
            print(obj.get("text", ""), end="", flush=True)
        elif t == "tts_event":
            pass
        elif t == "done":
            print("\n[done]")
            done_evt.set()
            return
        elif t == "error":
            print(f"\n[error] {obj.get('detail')}")
            done_evt.set()
            return


async def main() -> None:
    p = argparse.ArgumentParser(description="Remote streaming demo for /ws/voice")
    p.add_argument("--ws", required=True, help="e.g. ws://<linux-ip>:8090/ws/voice")
    p.add_argument("--input", required=True, help="Input audio file bytes (wav or pcm16)")
    p.add_argument("--out-dir", default="./demo_out")
    p.add_argument("--chunk-bytes", type=int, default=4096)
    p.add_argument("--chunk-delay-ms", type=int, default=30)
    p.add_argument("--sample-rate", type=int, default=16000)
    p.add_argument("--stt-language", default="zh")
    p.add_argument("--tts-language", default="zh-cn")
    p.add_argument("--speaker-wav-path", default="")
    args = p.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"input not found: {in_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    done_evt = asyncio.Event()
    delay = max(args.chunk_delay_ms, 0) / 1000.0

    async with websockets.connect(args.ws, max_size=None) as ws:
        ready = await ws.recv()
        print(f"[server] {ready}")

        cfg = {
            "type": "config",
            "sample_rate": args.sample_rate,
            "stt_language": args.stt_language,
            "tts_language": args.tts_language,
        }
        if args.speaker_wav_path:
            cfg["speaker_wav_path"] = args.speaker_wav_path
        await ws.send(json.dumps(cfg))

        recv_task = asyncio.create_task(recv_loop(ws, out_dir, done_evt))

        data = in_path.read_bytes()
        sent = 0
        while sent < len(data):
            end = min(sent + args.chunk_bytes, len(data))
            await ws.send(data[sent:end])
            sent = end
            if delay:
                await asyncio.sleep(delay)

        print(f"[send] {len(data)} bytes streamed")
        await ws.send("commit")

        await done_evt.wait()

        try:
            await ws.send("close")
        except Exception:
            pass

        if not recv_task.done():
            recv_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
