import argparse
import os
import sys
import tempfile

from faster_whisper import WhisperModel


def srt_ts(t):
    h = int(t // 3600)
    t %= 3600
    m = int(t // 60)
    t %= 60
    s = int(t)
    ms = int(round((t - s) * 1000))
    if ms == 1000:
        s += 1
        ms = 0
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def parse_args():
    parser = argparse.ArgumentParser(description="Faster-Whisper to SRT with stdin/stdout support")
    parser.add_argument("--input", default="/media/output.mp4", help="Input media path, or '-' for stdin")
    parser.add_argument("--output", default="/work/output.srt", help="Output SRT path, or '-' for stdout")
    parser.add_argument("--model", default="medium", help="Whisper model name")
    parser.add_argument("--device", default="cuda", help="Device for inference, e.g. cuda/cpu")
    parser.add_argument("--compute-type", default="float16", help="Compute type, e.g. float16/int8")
    parser.add_argument("--language", default=None, help="Optional language hint, e.g. zh/en")
    parser.add_argument("--beam-size", type=int, default=5, help="Beam size")
    return parser.parse_args()


def resolve_input_path(input_arg):
    if input_arg != "-":
        return input_arg, None

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    try:
        while True:
            chunk = sys.stdin.buffer.read(1024 * 1024)
            if not chunk:
                break
            tmp.write(chunk)
    finally:
        tmp.close()
    return tmp.name, tmp.name


def main():
    args = parse_args()
    input_path, tmp_to_delete = resolve_input_path(args.input)
    writer = None

    try:
        model = WhisperModel(args.model, device=args.device, compute_type=args.compute_type)
        segments, info = model.transcribe(
            input_path,
            language=args.language,
            beam_size=args.beam_size,
        )

        if args.output == "-":
            writer = sys.stdout
        else:
            writer = open(args.output, "w", encoding="utf-8")

        for i, seg in enumerate(segments, 1):
            writer.write(f"{i}\\n{srt_ts(seg.start)} --> {srt_ts(seg.end)}\\n{seg.text.strip()}\\n\\n")
            writer.flush()

        print(f"language: {info.language}", file=sys.stderr)
        if args.output != "-":
            print(f"wrote: {args.output}", file=sys.stderr)
    finally:
        if writer and writer is not sys.stdout:
            writer.close()
        if tmp_to_delete and os.path.exists(tmp_to_delete):
            os.unlink(tmp_to_delete)


if __name__ == "__main__":
    main()
