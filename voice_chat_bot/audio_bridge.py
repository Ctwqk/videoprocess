import argparse
import time

import numpy as np
import sounddevice as sd


def list_devices() -> None:
    print(sd.query_devices())


def run_bridge(
    input_device: int | None,
    output_device: int | None,
    sample_rate: int,
    channels: int,
    blocksize: int,
    gain: float,
) -> None:
    def callback(indata, outdata, _frames, _time, status):
        if status:
            print(f"[audio-status] {status}")
        out = indata * gain
        np.clip(out, -1.0, 1.0, out=out)
        outdata[:] = out

    print("[info] bridge started. Ctrl+C to stop.")
    with sd.Stream(
        device=(input_device, output_device),
        samplerate=sample_rate,
        channels=channels,
        dtype="float32",
        blocksize=blocksize,
        callback=callback,
    ):
        while True:
            time.sleep(1)


def parse_args():
    p = argparse.ArgumentParser(description="Bridge audio from input device to output device")
    p.add_argument("--list-devices", action="store_true")
    p.add_argument("--input-device", type=int, default=None, required=False)
    p.add_argument("--output-device", type=int, default=None, required=False)
    p.add_argument("--sample-rate", type=int, default=48000)
    p.add_argument("--channels", type=int, default=1)
    p.add_argument("--blocksize", type=int, default=960)
    p.add_argument("--gain", type=float, default=1.0)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.list_devices:
        list_devices()
        raise SystemExit(0)

    if args.input_device is None or args.output_device is None:
        raise SystemExit("--input-device and --output-device are required unless --list-devices")

    try:
        run_bridge(
            input_device=args.input_device,
            output_device=args.output_device,
            sample_rate=args.sample_rate,
            channels=args.channels,
            blocksize=args.blocksize,
            gain=args.gain,
        )
    except KeyboardInterrupt:
        print("\n[info] stopped")
