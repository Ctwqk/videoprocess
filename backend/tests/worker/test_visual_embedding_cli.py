from __future__ import annotations

import builtins
from contextlib import contextmanager
import importlib
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


BACKEND = Path(__file__).resolve().parents[2]
MODEL_FILES = ("config.json", "preprocessor_config.json", "vocab.txt", "model.safetensors")


@pytest.fixture
def local_files(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    for name in MODEL_FILES:
        (model / name).write_bytes(b"local test fixture")
    image = tmp_path / "frame.jpg"
    image.write_bytes(b"image decoding is tested separately")
    return str(model), str(image)


@pytest.fixture
def no_heavy_imports(monkeypatch):
    original = builtins.__import__
    attempted = []

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"torch", "transformers", "PIL", "huggingface_hub"}:
            attempted.append(name)
            raise AssertionError(f"Unexpected heavy import: {name}")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    return attempted


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("texts", []),
        ("texts", ["blue"] * 3),
        ("texts", [""]),
        ("texts", [" \t\n"]),
        ("texts", ["x" * 513]),
        ("texts", [True]),
        ("texts", [12]),
        ("texts", "blue"),
        ("texts", None),
        ("image_paths", []),
        ("image_paths", ["frame.jpg"] * 241),
        ("image_paths", [""]),
        ("image_paths", [False]),
        ("image_paths", "frame.jpg"),
        ("image_paths", None),
        ("model_path", ""),
        ("model_path", None),
        ("model_path", 123),
    ],
)
def test_invalid_input_is_rejected_before_heavy_imports(
    field, value, local_files, no_heavy_imports,
):
    model_path, image_path = local_files
    payload = {"model_path": model_path, "texts": ["blue"], "image_paths": [image_path]}
    payload[field] = value
    module = importlib.import_module("worker.visual_embedding_model")

    with pytest.raises(ValueError):
        module.score_images(**payload)

    assert no_heavy_imports == []


@pytest.mark.parametrize("missing", ["directory", *MODEL_FILES, "image"])
def test_missing_local_files_are_rejected_before_heavy_imports(
    missing, local_files, no_heavy_imports,
):
    model_path, image_path = local_files
    if missing == "directory":
        model_path += "-missing"
    elif missing == "image":
        image_path += "-missing"
    else:
        (Path(model_path) / missing).unlink()
    module = importlib.import_module("worker.visual_embedding_model")

    with pytest.raises(ValueError, match="local"):
        module.score_images(model_path, ["blue"], [image_path])

    assert no_heavy_imports == []


@pytest.mark.parametrize(
    "payload",
    [
        {"model_path": "/missing", "texts": ["blue"], "image_paths": ["/missing.jpg"]},
        {"model_path": "/missing", "texts": [], "image_paths": ["/missing.jpg"]},
        {"model_path": "/missing", "texts": ["x" * 513], "image_paths": ["/missing.jpg"]},
        {"model_path": "/missing", "texts": ["blue"] * 3, "image_paths": ["/missing.jpg"]},
        {"model_path": "/missing", "texts": ["blue"], "image_paths": []},
        {"model_path": "/missing", "texts": ["blue"], "image_paths": ["/missing.jpg"] * 241},
        {"texts": ["blue"]},
        {"model_path": "/missing", "texts": ["blue"], "image_paths": [], "extra": True},
        [],
        None,
    ],
)
def test_cli_invalid_request_exits_without_success_matrix(payload):
    result = subprocess.run(
        [sys.executable, "-m", "worker.visual_embedding_cli"],
        cwd=BACKEND, input=json.dumps(payload), text=True, capture_output=True, timeout=10,
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "visual scoring unavailable" in result.stderr


def test_cli_malformed_json_exits_without_success_matrix():
    result = subprocess.run(
        [sys.executable, "-m", "worker.visual_embedding_cli"],
        cwd=BACKEND, input="{", text=True, capture_output=True, timeout=10,
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "visual scoring unavailable" in result.stderr


class Matrix:
    """Tiny tensor boundary double; no Torch, NumPy, Pillow, or model needed."""

    def __init__(self, rows):
        self.rows = rows

    @property
    def T(self):
        return Matrix(list(zip(*self.rows)))

    def __matmul__(self, other):
        return Matrix([
            [sum(a * b for a, b in zip(row, column)) for column in zip(*other.rows)]
            for row in self.rows
        ])

    def tolist(self):
        return self.rows


@pytest.fixture
def fake_runtime(monkeypatch, local_files, tmp_path):
    vectors = [[0.6, 0.8], [-0.8, 0.6], [1.0, 0.0], [0.0, -1.0], [-1.0, 0.0]]
    paths = []
    for index in range(5):
        path = tmp_path / f"{index}.jpg"
        path.write_bytes(b"frame")
        paths.append(str(path))
    state = SimpleNamespace(
        paths=paths, batches=[], open_images=[], max_open=0, inference=False,
        threads=None, device=None, evaluating=False, fail_at=None, fail_decode=None,
        fail_convert=False, matrix=None, tokenizer_limit=10**30, positional_limit=512,
    )

    class Image:
        def __init__(self, path, rgb=False):
            self.path = path
            self.rgb = rgb
            self.closed = False
            state.open_images.append(self)
            state.max_open = max(state.max_open, len(state.open_images))

        def __enter__(self):
            return self

        def __exit__(self, *_):
            self.close()

        def close(self):
            if not self.closed:
                self.closed = True
                state.open_images.remove(self)

        def convert(self, mode):
            assert mode == "RGB"
            if state.fail_convert:
                raise OSError("conversion failed")
            return Image(self.path, rgb=True)

    def open_image(path):
        if path == state.fail_decode:
            raise OSError("decode failed")
        return Image(path)

    class Inputs(dict):
        def to(self, device):
            assert device == "cpu"
            return self

    class Processor:
        @property
        def tokenizer(self):
            return SimpleNamespace(model_max_length=state.tokenizer_limit)

        @classmethod
        def from_pretrained(cls, path, **kwargs):
            assert path == local_files[0]
            assert kwargs["local_files_only"] is True
            assert kwargs["trust_remote_code"] is False
            assert kwargs["use_fast"] is False
            return cls()

        def __call__(self, *, text, images, **kwargs):
            assert all(image.rgb and not image.closed for image in images)
            assert kwargs == {
                "return_tensors": "pt", "padding": True, "truncation": True,
                "max_length": min(state.positional_limit, state.tokenizer_limit),
            }
            state.batches.append(([image.path for image in images], list(text)))
            return Inputs(images=images, text=text)

    class Model:
        @classmethod
        def from_pretrained(cls, path, **kwargs):
            assert path == local_files[0]
            assert kwargs["local_files_only"] is True
            assert kwargs["trust_remote_code"] is False
            assert kwargs["use_safetensors"] is True
            return cls()

        @property
        def config(self):
            return SimpleNamespace(
                text_config=SimpleNamespace(max_position_embeddings=state.positional_limit),
            )

        def to(self, device):
            state.device = device
            return self

        def eval(self):
            state.evaluating = True
            return self

        def __call__(self, *, images, text):
            assert state.inference and state.evaluating and state.device == "cpu"
            if state.fail_at == len(state.batches):
                raise RuntimeError("inference failed")
            image_vectors = [vectors[paths.index(image.path)] for image in images]
            image_embeds = Matrix(image_vectors)
            if state.matrix is not None:
                class InvalidEmbeds:
                    def __matmul__(self, _):
                        return Matrix(state.matrix)
                image_embeds = InvalidEmbeds()
            return SimpleNamespace(
                image_embeds=image_embeds,
                text_embeds=Matrix([[1.0, 0.0], [0.0, 1.0]][:len(text)]),
            )

    @contextmanager
    def inference_mode():
        state.inference = True
        try:
            yield
        finally:
            state.inference = False

    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(
        set_num_threads=lambda count: setattr(state, "threads", count),
        inference_mode=inference_mode,
    ))
    monkeypatch.setitem(sys.modules, "PIL", SimpleNamespace(Image=SimpleNamespace(open=open_image)))
    monkeypatch.setitem(sys.modules, "transformers", SimpleNamespace(
        ChineseCLIPModel=Model, ChineseCLIPProcessor=Processor,
    ))
    monkeypatch.setitem(sys.modules, "huggingface_hub", None)
    return state


def test_score_uses_offline_cpu_cosines_in_bounded_ordered_batches(local_files, fake_runtime):
    module = importlib.import_module("worker.visual_embedding_model")
    texts = ["\u84dd\u8272", "\u7ea2\u8272"]

    result = module.score_images(local_files[0], texts, fake_runtime.paths)

    assert result == [[0.6, 0.8], [-0.8, 0.6], [1.0, 0.0], [0.0, -1.0], [-1.0, 0.0]]
    assert fake_runtime.batches == [
        (fake_runtime.paths[:4], texts), (fake_runtime.paths[4:], texts),
    ]
    assert fake_runtime.threads == 2
    assert fake_runtime.open_images == []
    assert fake_runtime.max_open <= 8


@pytest.mark.parametrize(("positional_limit", "tokenizer_limit"), [(512, 10**30), (512, 77), (128, 512)])
def test_accepts_exact_limits_without_slicing_text_and_bounds_tokens(
    positional_limit, tokenizer_limit, local_files, fake_runtime,
):
    module = importlib.import_module("worker.visual_embedding_model")
    fake_runtime.positional_limit = positional_limit
    fake_runtime.tokenizer_limit = tokenizer_limit
    text = "\u84dd" * 512

    result = module.score_images(local_files[0], [text], [fake_runtime.paths[0]] * 240)

    assert result == [[0.6]] * 240
    assert len(fake_runtime.batches) == 60
    assert all(len(paths) == 4 and texts == [text] for paths, texts in fake_runtime.batches)
    assert fake_runtime.open_images == []


@pytest.mark.parametrize("failure", ["decode", "convert", "inference"])
def test_failed_batch_closes_all_decoded_images(failure, local_files, fake_runtime):
    module = importlib.import_module("worker.visual_embedding_model")
    if failure == "decode":
        fake_runtime.fail_decode = fake_runtime.paths[2]
    elif failure == "convert":
        fake_runtime.fail_convert = True
    else:
        fake_runtime.fail_at = 2

    with pytest.raises((OSError, RuntimeError), match="failed"):
        module.score_images(local_files[0], ["blue"], fake_runtime.paths)

    assert fake_runtime.open_images == []
    assert fake_runtime.inference is False


@pytest.mark.parametrize("matrix", [
    [], [[0.4], [0.5]], [[0.4, 0.5]], [[True]], [["0.4"]],
    [[float("nan")]], [[float("inf")]], [[-1.01]], [[1.01]],
])
def test_score_rejects_invalid_model_matrices(matrix, local_files, fake_runtime):
    module = importlib.import_module("worker.visual_embedding_model")
    fake_runtime.matrix = matrix

    with pytest.raises(ValueError, match="similarities"):
        module.score_images(local_files[0], ["blue"], fake_runtime.paths[:1])

    assert fake_runtime.open_images == []


def test_cli_outputs_single_json_object_with_revision(monkeypatch, capsys, local_files, fake_runtime):
    cli = importlib.import_module("worker.visual_embedding_cli")
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({
        "model_path": local_files[0], "texts": ["blue"], "image_paths": fake_runtime.paths[:1],
    })))

    assert cli.main() == 0

    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "similarities": [[0.6]], "model_revision": "f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e",
    }
    assert output.err == ""


def test_cli_late_failure_never_prints_partial_matrix(monkeypatch, capsys, local_files, fake_runtime):
    cli = importlib.import_module("worker.visual_embedding_cli")
    fake_runtime.fail_at = 2
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({
        "model_path": local_files[0], "texts": ["blue"], "image_paths": fake_runtime.paths,
    })))

    assert cli.main() != 0

    output = capsys.readouterr()
    assert output.out == ""
    assert "inference failed" in output.err
    assert fake_runtime.open_images == []


@pytest.fixture
def fake_download(monkeypatch, tmp_path):
    target = tmp_path / "download"

    def snapshot_download(*, repo_id, revision, allow_patterns, local_dir):
        assert repo_id == "OFA-Sys/chinese-clip-vit-base-patch16"
        assert revision == "f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e"
        assert set(allow_patterns) == {*MODEL_FILES, "README.md"}
        assert Path(local_dir) == target
        target.mkdir(exist_ok=True)
        for name in MODEL_FILES:
            (target / name).write_bytes(b"abc")
        (target / "README.md").write_text("Upstream model attribution", encoding="utf-8")
        return str(target)

    monkeypatch.setitem(sys.modules, "huggingface_hub", SimpleNamespace(snapshot_download=snapshot_download))
    monkeypatch.setitem(sys.modules, "torch", None)
    monkeypatch.setitem(sys.modules, "transformers", None)
    monkeypatch.setitem(sys.modules, "PIL", None)
    return target


def test_download_rejects_weight_hash_mismatch(fake_download):
    module = importlib.import_module("worker.visual_embedding_model")

    with pytest.raises(ValueError, match="SHA-256"):
        module.download_model(str(fake_download))

    assert not (fake_download / "MODEL_PROVENANCE.json").exists()


def test_download_verifies_streaming_hash_and_preserves_provenance(monkeypatch, fake_download):
    module = importlib.import_module("worker.visual_embedding_model")
    monkeypatch.setattr(module, "MODEL_SHA256", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
    original_open = Path.open
    read_sizes = []

    class BoundedReader(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024
            read_sizes.append(size)
            return super().read(size)

    def checked_open(path, mode="r", *args, **kwargs):
        if path.name == "model.safetensors" and mode == "rb":
            return BoundedReader(b"abc")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", checked_open)

    assert module.download_model(str(fake_download)) == fake_download

    assert len(read_sizes) >= 2
    assert (fake_download / "README.md").read_text() == "Upstream model attribution"
    provenance = json.loads((fake_download / "MODEL_PROVENANCE.json").read_text())
    assert provenance["model_id"] == "OFA-Sys/chinese-clip-vit-base-patch16"
    assert provenance["model_revision"] == "f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e"
    assert provenance["weight_sha256"] == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert "https://github.com/OFA-Sys/Chinese-CLIP" in provenance["upstream_code"]
    assert "no separate license" in provenance["license_note"]


def test_download_script_help_works_without_heavy_dependencies():
    result = subprocess.run(
        [sys.executable, "worker/visual_embedding_model.py", "--help"],
        cwd=BACKEND, text=True, capture_output=True, timeout=10,
    )

    assert result.returncode == 0
    assert "--download" in result.stdout
