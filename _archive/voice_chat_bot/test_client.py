import argparse
import asyncio
import json

import websockets


async def main(ws_url: str, wav_path: str, out_path: str) -> None:
    audio_out = bytearray()

    async with websockets.connect(ws_url, max_size=None) as ws:
        ready = await ws.recv()
        print("server:", ready)

        with open(wav_path, "rb") as f:
            data = f.read()

        await ws.send(data)
        await ws.send("commit")

        while True:
            msg = await ws.recv()
            if isinstance(msg, bytes):
                audio_out.extend(msg)
                continue

            try:
                obj = json.loads(msg)
            except json.JSONDecodeError:
                print("text:", msg)
                continue

            t = obj.get("type")
            if t == "stt":
                print("STT:", obj.get("text", ""))
            elif t == "llm_delta":
                print(obj.get("text", ""), end="", flush=True)
            elif t == "done":
                print("\n[DONE]")
                break
            elif t == "error":
                print("\n[ERROR]", obj.get("detail"))
                break

        await ws.send("close")

    with open(out_path, "wb") as f:
        f.write(audio_out)
    print("saved:", out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ws", required=True)
    parser.add_argument("--wav", required=True)
    parser.add_argument("--out", default="reply.wav")
    args = parser.parse_args()
    asyncio.run(main(args.ws, args.wav, args.out))
