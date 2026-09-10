"""One-request JSON interface for the vision worker's bounded child process."""

from __future__ import annotations

from contextlib import redirect_stdout
import json
import sys

from worker.visual_embedding_model import MODEL_REVISION, score_images


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict) or set(payload) != {"model_path", "texts", "image_paths"}:
            raise ValueError("expected only model_path, texts, and image_paths")
        # Keep dependency diagnostics off the single-object stdout protocol.
        with redirect_stdout(sys.stderr):
            similarities = score_images(**payload)
        result = json.dumps(
            {"similarities": similarities, "model_revision": MODEL_REVISION}, allow_nan=False,
        )
    except Exception as exc:
        print(f"visual scoring unavailable: {exc}", file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
