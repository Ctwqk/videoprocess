from __future__ import annotations
import abc
import asyncio
import json
import logging
import os
import shutil
import sys

logger = logging.getLogger(__name__)


class CancelledError(Exception):
    """Raised when a handler detects its node has been cancelled."""


class BaseHandler(abc.ABC):
    """Base class for node execution handlers."""

    def __init__(self):
        self._cancelled = False
        self._proc: asyncio.subprocess.Process | None = None

    @abc.abstractmethod
    async def execute(
        self,
        node_config: dict,
        input_paths: dict[str, str],   # port_name -> local file path
        output_path: str,               # local file path for output
    ) -> dict | None:
        """Execute the node operation. Raise on failure and optionally return artifact metadata."""
        ...

    def cancel(self) -> None:
        """Signal cancellation. Kills any running subprocess."""
        self._cancelled = True
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
                logger.info("Killed ffmpeg subprocess due to cancellation")
            except ProcessLookupError:
                pass

    def gpu_enabled(self) -> bool:
        value = os.environ.get("VIDEO_USE_GPU", "").strip().lower()
        return value in {"1", "true", "yes", "on"}

    def videotoolbox_enabled(self) -> bool:
        value = os.environ.get("VIDEO_USE_VIDEOTOOLBOX", "").strip().lower()
        return value in {"1", "true", "yes", "on"}

    def gpu_fallback_enabled(self) -> bool:
        value = os.environ.get("VIDEO_GPU_FALLBACK_TO_CPU", "true").strip().lower()
        return value in {"1", "true", "yes", "on"}

    def gpu_busy_util_threshold(self) -> int:
        return int(os.environ.get("VIDEO_GPU_BUSY_UTIL_THRESHOLD", "90"))

    def gpu_busy_mem_threshold(self) -> int:
        return int(os.environ.get("VIDEO_GPU_BUSY_MEM_THRESHOLD", "92"))

    @staticmethod
    def parse_bool_param(value, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def preferred_video_codec(self, codec: str | None = None) -> str:
        selected = codec or "libx264"
        if self.gpu_enabled():
            codec_map = {
                "libx264": "h264_nvenc",
                "libx265": "hevc_nvenc",
            }
            return codec_map.get(selected, selected)
        if self.videotoolbox_enabled():
            codec_map = {
                "libx264": "h264_videotoolbox",
                "libx265": "hevc_videotoolbox",
            }
            return codec_map.get(selected, selected)
        return selected

    def build_video_encode_args(
        self,
        codec: str | None = None,
        *,
        preset: str = "fast",
        crf: int | None = None,
        bitrate: str = "",
    ) -> list[str]:
        selected = self.preferred_video_codec(codec)
        args = ["-c:v", selected]

        if selected in ("libx264", "libx265"):
            if crf is not None:
                args.extend(["-crf", str(int(crf))])
            args.extend(["-preset", preset])
        elif selected in ("h264_nvenc", "hevc_nvenc"):
            if crf is not None:
                args.extend(["-rc:v", "vbr", "-cq:v", str(int(crf))])
            args.extend(["-preset", preset])
        elif selected in ("h264_videotoolbox", "hevc_videotoolbox"):
            # `-q:v` support is inconsistent across macOS ffmpeg builds; bitrate mode
            # is the stable baseline for headless Apple Silicon workers.
            args.extend(["-b:v", bitrate or self._default_videotoolbox_bitrate(selected)])

        if bitrate:
            if selected not in ("h264_videotoolbox", "hevc_videotoolbox"):
                args.extend(["-b:v", bitrate])

        return args

    def _default_videotoolbox_bitrate(self, codec: str) -> str:
        return {
            "h264_videotoolbox": "6M",
            "hevc_videotoolbox": "4M",
        }.get(codec, "6M")

    def _contains_hardware_codec(self, args: list[str]) -> bool:
        return any(
            token in {"h264_nvenc", "hevc_nvenc", "h264_videotoolbox", "hevc_videotoolbox"}
            for token in args
        )

    def _cpu_codec_for(self, codec: str) -> str:
        return {
            "h264_nvenc": "libx264",
            "hevc_nvenc": "libx265",
            "h264_videotoolbox": "libx264",
            "hevc_videotoolbox": "libx265",
        }.get(codec, codec)

    def _rewrite_hardware_args_for_cpu(self, args: list[str]) -> list[str]:
        rewritten: list[str] = []
        removed_cq: str | None = None
        has_crf = False
        i = 0
        while i < len(args):
            token = args[i]
            nxt = args[i + 1] if i + 1 < len(args) else None

            if token == "-c:v" and nxt is not None:
                rewritten.extend([token, self._cpu_codec_for(nxt)])
                i += 2
                continue
            if token == "-crf" and nxt is not None:
                has_crf = True
                rewritten.extend([token, nxt])
                i += 2
                continue
            if token == "-cq:v" and nxt is not None:
                removed_cq = nxt
                i += 2
                continue
            if token == "-rc:v" and nxt is not None:
                i += 2
                continue

            rewritten.append(token)
            i += 1

        if removed_cq is not None and not has_crf:
            insert_at = len(rewritten) - 1 if rewritten and not rewritten[-1].startswith("-") else len(rewritten)
            rewritten[insert_at:insert_at] = ["-crf", removed_cq]

        return rewritten

    async def _gpu_looks_busy(self) -> bool:
        if not self.gpu_enabled() or not self.gpu_fallback_enabled():
            return False
        if shutil.which("nvidia-smi") is None:
            return False

        proc = await asyncio.create_subprocess_exec(
            "nvidia-smi",
            "--query-gpu=utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return False

        for line in stdout.decode("utf-8", errors="replace").splitlines():
            parts = [part.strip() for part in line.split(",")]
            if len(parts) != 3:
                continue
            try:
                util = int(parts[0])
                mem_used = int(parts[1])
                mem_total = int(parts[2])
            except ValueError:
                continue

            mem_pct = (mem_used / mem_total * 100) if mem_total else 0
            if util >= self.gpu_busy_util_threshold() or mem_pct >= self.gpu_busy_mem_threshold():
                logger.warning(
                    "GPU looks busy (util=%s%%, mem=%s/%s MiB); falling back to CPU encoding",
                    util,
                    mem_used,
                    mem_total,
                )
                return True

        return False

    def _is_gpu_capacity_error(self, stderr_text: str) -> bool:
        lowered = stderr_text.lower()
        indicators = (
            "openencodesessionex failed",
            "no nvenc capable devices found",
            "device busy",
            "resource temporarily unavailable",
            "cannot init cuda",
            "cuda_error_out_of_memory",
            "out of memory",
            "nvenc",
            "videotoolbox",
            "videotoolbox encoder",
            "hardware encoder may be busy",
            "error while opening encoder for output stream",
        )
        return any(indicator in lowered for indicator in indicators)

    def resolve_executable(self, name: str) -> str:
        """Resolve a binary from PATH or from the current Python environment's bin directory."""
        env_name = f"{name.upper().replace('-', '_')}_BIN"
        explicit_path = os.environ.get(env_name, "").strip()
        if explicit_path:
            return explicit_path

        resolved = shutil.which(name)
        if resolved:
            return resolved

        python_bin = os.path.dirname(sys.executable)
        candidate = os.path.join(python_bin, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate

        raise FileNotFoundError(f"Could not find executable '{name}'")

    async def run_ffmpeg(self, args: list[str]) -> str:
        """Run an ffmpeg command and return stderr output."""
        if self._cancelled:
            raise CancelledError("Node cancelled before ffmpeg started")

        ffmpeg_args = list(args)
        retried_on_cpu = False
        if self._contains_hardware_codec(ffmpeg_args) and await self._gpu_looks_busy():
            ffmpeg_args = self._rewrite_hardware_args_for_cpu(ffmpeg_args)
            retried_on_cpu = True

        cmd = ["ffmpeg", "-y", "-hide_banner"] + ffmpeg_args
        logger.info(f"Running: {' '.join(cmd)}")
        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await self._proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace")

        if self._cancelled:
            raise CancelledError("Node cancelled during ffmpeg execution")
        if self._proc.returncode != 0:
            if (
                not retried_on_cpu
                and self._contains_hardware_codec(ffmpeg_args)
                and self.gpu_fallback_enabled()
                and self._is_gpu_capacity_error(stderr_text)
            ):
                logger.warning("Hardware-accelerated ffmpeg run failed; retrying on CPU")
                return await self.run_ffmpeg(self._rewrite_hardware_args_for_cpu(args))
            raise RuntimeError(f"ffmpeg failed (exit {self._proc.returncode}):\n{stderr_text[-2000:]}")
        return stderr_text

    async def run_ffprobe(self, path: str) -> dict:
        """Run ffprobe and return JSON output."""
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format", "-show_streams",
            path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            return {}
        try:
            return json.loads(stdout.decode())
        except Exception:
            return {}
