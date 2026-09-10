"""Pinned Chinese-CLIP build download and worker-local, CPU-only scoring."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import math
from pathlib import Path
import sys


MODEL_ID = "OFA-Sys/chinese-clip-vit-base-patch16"
MODEL_REVISION = "f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e"
MODEL_SHA256 = "29cc0b2bcf6ff777f2e15742be92b110e4acbdb2068356e862c4637a4b15fe4f"
MODEL_FILES = ("config.json", "preprocessor_config.json", "vocab.txt", "model.safetensors")
DOWNLOAD_FILES = (*MODEL_FILES, "README.md")
MAX_IMAGES = 240
MAX_TEXTS = 2
MAX_TEXT_CHARACTERS = 512
BATCH_SIZE = 4
TORCH_THREADS = 2


def _local_model_directory(model_path: str) -> Path:
    if not isinstance(model_path, str) or not model_path.strip():
        raise ValueError("model_path must name a local model directory")
    directory = Path(model_path)
    if not directory.is_dir():
        raise ValueError(f"local model directory is missing: {model_path}")
    for name in MODEL_FILES:
        if not (directory / name).is_file():
            raise ValueError(f"local model file is missing: {name}")
    return directory


def _validate_inputs(model_path: str, texts: list[str], image_paths: list[str]) -> None:
    if not isinstance(texts, list) or not 1 <= len(texts) <= MAX_TEXTS:
        raise ValueError(f"texts must contain 1 to {MAX_TEXTS} strings")
    for text in texts:
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_TEXT_CHARACTERS:
            raise ValueError(f"each text must contain 1 to {MAX_TEXT_CHARACTERS} characters")
    if not isinstance(image_paths, list) or not 1 <= len(image_paths) <= MAX_IMAGES:
        raise ValueError(f"image_paths must contain 1 to {MAX_IMAGES} local paths")
    for path in image_paths:
        if not isinstance(path, str) or not path.strip():
            raise ValueError("image_paths must contain nonempty local paths")
    _local_model_directory(model_path)
    for path in image_paths:
        if not Path(path).is_file():
            raise ValueError(f"local image file is missing: {path}")


def _validate_similarities(matrix: list[list[float]], images: int, texts: int) -> None:
    if not isinstance(matrix, list) or len(matrix) != images:
        raise ValueError("similarities must have exactly one row per image")
    for row in matrix:
        if not isinstance(row, list) or len(row) != texts:
            raise ValueError("similarities must have exactly one column per text")
        for value in row:
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not -1 <= value <= 1
            ):
                raise ValueError("similarities must be finite numbers in [-1, 1]")


def score_images(model_path: str, texts: list[str], image_paths: list[str]) -> list[list[float]]:
    """Return image-by-text cosine scores without downloading or executing remote code."""
    _validate_inputs(model_path, texts, image_paths)

    import torch
    from PIL import Image
    from transformers import ChineseCLIPModel, ChineseCLIPProcessor

    torch.set_num_threads(TORCH_THREADS)
    processor = ChineseCLIPProcessor.from_pretrained(
        model_path, local_files_only=True, trust_remote_code=False, use_fast=False,
    )
    model = ChineseCLIPModel.from_pretrained(
        model_path, local_files_only=True, trust_remote_code=False, use_safetensors=True,
    ).to("cpu").eval()
    max_tokens = min(
        model.config.text_config.max_position_embeddings, processor.tokenizer.model_max_length,
    )
    similarities: list[list[float]] = []
    with torch.inference_mode():
        for start in range(0, len(image_paths), BATCH_SIZE):
            batch = image_paths[start:start + BATCH_SIZE]
            with ExitStack() as stack:
                images = []
                for path in batch:
                    with Image.open(path) as source:
                        image = source.convert("RGB")
                    stack.callback(image.close)
                    images.append(image)
                inputs = processor(
                    text=texts, images=images, return_tensors="pt", padding=True,
                    truncation=True, max_length=max_tokens,
                ).to("cpu")
                output = model(**inputs)
                # Forward returns unit-normalized embeddings; logits include a learned scale.
                matrix = (output.image_embeds @ output.text_embeds.T).tolist()
                _validate_similarities(matrix, len(batch), len(texts))
                similarities.extend(matrix)
    return similarities


def download_model(directory: str) -> Path:
    """Build-only download; verify the pinned safetensors before recording provenance."""
    from huggingface_hub import snapshot_download

    target = Path(snapshot_download(
        repo_id=MODEL_ID, revision=MODEL_REVISION,
        allow_patterns=list(DOWNLOAD_FILES), local_dir=directory,
    ))
    _local_model_directory(str(target))
    digest = hashlib.sha256()
    with (target / "model.safetensors").open("rb") as weights:
        for chunk in iter(lambda: weights.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != MODEL_SHA256:
        raise ValueError(f"model.safetensors SHA-256 mismatch: {digest.hexdigest()}")
    provenance = {
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "weight_sha256": MODEL_SHA256,
        "model_source": f"https://huggingface.co/{MODEL_ID}/tree/{MODEL_REVISION}",
        "upstream_code": "https://github.com/OFA-Sys/Chinese-CLIP",
        "upstream_code_license": "https://github.com/OFA-Sys/Chinese-CLIP/blob/master/MIT-LICENSE.txt",
        "license_note": (
            "The upstream code is MIT licensed; the pinned model card has no separate license field. "
            "The original model card is retained in README.md. "
            "This does not establish rights to input footage."
        ),
    }
    (target / "MODEL_PROVENANCE.json").write_text(
        json.dumps(provenance, indent=2) + "\n", encoding="utf-8",
    )
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Download the pinned visual model at image build time")
    parser.add_argument("--download", required=True, metavar="DIRECTORY")
    args = parser.parse_args()
    try:
        target = download_model(args.download)
    except Exception as exc:
        print(f"visual model download failed: {exc}", file=sys.stderr)
        return 1
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
