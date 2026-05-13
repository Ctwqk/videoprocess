from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import platform
import re
import tempfile
from pathlib import Path

import httpx

from worker.handlers.base import BaseHandler
from worker.handlers.subtitle_utils import SubtitleCue, parse_srt

logger = logging.getLogger(__name__)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?。！？])\s+")


def _base_url_string(client: httpx.AsyncClient) -> str:
    return str(client.base_url).rstrip("/")


@dataclass
class SpeechBlock:
    index: int
    start_seconds: float
    end_seconds: float
    text: str
    cue_indexes: list[int]


@dataclass
class GeneratedAudioBlock:
    block: SpeechBlock
    audio_path: str
    duration_ms: int
    provider: str


class SubtitleToSpeechHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        subtitle_path = input_paths["subtitle_file"]
        reference_audio_path = input_paths.get("reference_audio")
        reference_text_path = input_paths.get("ref_text")
        language = str(node_config.get("language", "en") or "en")
        tts_base_urls = self._local_tts_base_urls()
        block_merge_gap_seconds = float(node_config.get("block_merge_gap_seconds", 0.6) or 0.6)
        block_min_chars = int(node_config.get("block_min_chars", 70) or 70)
        block_max_chars = int(node_config.get("block_max_chars", 220) or 220)
        block_min_duration_seconds = float(node_config.get("block_min_duration_seconds", 2.5) or 2.5)
        block_max_duration_seconds = float(node_config.get("block_max_duration_seconds", 10.0) or 10.0)
        alignment_max_speedup = float(node_config.get("alignment_max_speedup", 1.35) or 1.35)
        alignment_max_leading_delay_ms = int(
            node_config.get("alignment_max_leading_delay_ms", 800) or 800
        )

        try:
            with open(subtitle_path, "r", encoding="utf-8") as handle:
                cues = parse_srt(handle.read())
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                "subtitle_file input is not a valid UTF-8 subtitle file. "
                "Connect a subtitle-producing node to 'subtitle_file' and put optional audio/video on 'reference_audio'."
            ) from exc
        if not cues:
            raise RuntimeError("Subtitle file contains no cues")

        speech_blocks = self._create_speech_blocks(
            cues,
            max_gap_seconds=block_merge_gap_seconds,
            min_chars=block_min_chars,
            max_chars=block_max_chars,
            min_duration_seconds=block_min_duration_seconds,
            max_duration_seconds=block_max_duration_seconds,
        )
        if not speech_blocks:
            raise RuntimeError("Subtitle file could not be converted into speech blocks")

        temp_audio_files: list[str] = []
        temp_reference_audio: str | None = None
        local_speaker_id: str | None = None
        local_speaker_text: str | None = None
        provider_used: str | None = None
        minimax_fallback_reason: str | None = None
        prefer_minimax = self._can_use_minimax(reference_audio_path)
        should_use_ref_text = self._should_use_reference_text()
        try:
            if reference_audio_path:
                temp_reference_audio = await self._ensure_wav_reference(reference_audio_path)
            if reference_text_path and should_use_ref_text:
                local_speaker_text = self._load_reference_text(reference_text_path)

            current_tts_base_url = tts_base_urls[0]
            client = httpx.AsyncClient(base_url=current_tts_base_url, timeout=240)
            try:
                if temp_reference_audio and not prefer_minimax:
                    local_speaker_id, client, current_tts_base_url = await self._try_register_local_speaker(
                        tts_base_urls=tts_base_urls,
                        current_base_url=current_tts_base_url,
                        client=client,
                        reference_audio_path=temp_reference_audio,
                        speaker_text=local_speaker_text,
                    )
                    if local_speaker_id is None:
                        logger.warning("Local TTS speaker registration failed on all local TTS services; using per-request speaker upload")

                audio_blocks: list[GeneratedAudioBlock] = []
                total_blocks = len(speech_blocks)
                for block_index, block in enumerate(speech_blocks, start=1):
                    temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
                    temp_output.close()
                    temp_audio_files.append(temp_output.name)
                    if prefer_minimax:
                        try:
                            await self._synthesize_cue_with_minimax(
                                text=block.text,
                                language=language,
                                output_path=temp_output.name,
                            )
                            provider_used = self._merge_provider(provider_used, "minimax")
                        except Exception as exc:
                            minimax_fallback_reason = str(exc)
                            logger.warning(
                                "MiniMax TTS failed for subtitle block %s; falling back to local TTS: %s",
                                block.index,
                                exc,
                            )
                            prefer_minimax = False

                    block_provider = "minimax" if prefer_minimax else "local"
                    if not prefer_minimax:
                        local_provider, client, current_tts_base_url, local_speaker_id = await self._synthesize_cue_with_local_fallback(
                            tts_base_urls=tts_base_urls,
                            current_base_url=current_tts_base_url,
                            client=client,
                            text=block.text,
                            language=language,
                            output_path=temp_output.name,
                            reference_audio_path=temp_reference_audio,
                            speaker_id=local_speaker_id,
                            speaker_text=local_speaker_text,
                        )
                        block_provider = local_provider
                        provider_used = self._merge_provider(provider_used, local_provider)
                    block_duration_ms = await self._probe_audio_duration_ms(temp_output.name)
                    audio_blocks.append(
                        GeneratedAudioBlock(
                            block=block,
                            audio_path=temp_output.name,
                            duration_ms=block_duration_ms,
                            provider=block_provider,
                        )
                    )
                    logger.info(
                        "subtitle_to_speech progress: %s/%s blocks finished",
                        block_index,
                        total_blocks,
                    )
            finally:
                await client.aclose()

            aligned_inputs, final_duration, peak_shift_ms = await self._align_audio_blocks(
                audio_blocks=audio_blocks,
                timeline_duration=max(cue.end_seconds for cue in cues),
                max_speedup=alignment_max_speedup,
                max_leading_delay_ms=alignment_max_leading_delay_ms,
                temp_audio_files=temp_audio_files,
            )
            await self._mix_audio_timeline(
                audio_inputs=aligned_inputs,
                duration=final_duration,
                output_path=output_path,
            )
        finally:
            for path in temp_audio_files:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            if temp_reference_audio and temp_reference_audio != reference_audio_path:
                try:
                    os.unlink(temp_reference_audio)
                except OSError:
                    pass

        return {
            "subtitle_segments": len(cues),
            "speech_blocks": len(speech_blocks),
            "tts_language": language,
            "output_duration": final_duration,
            "tts_provider": provider_used or "local",
            "tts_fallback_reason": minimax_fallback_reason,
            "reference_text_used": bool(local_speaker_text),
            "alignment_strategy": "subdub-inspired-block-fit",
            "alignment_peak_shift_ms": peak_shift_ms,
        }

    def _local_tts_base_urls(self) -> list[str]:
        primary = str(os.environ.get("VIDEO_TTS_BASE_URL", "http://127.0.0.1:8010") or "").rstrip("/")
        fallback = str(os.environ.get("VIDEO_TTS_FALLBACK_BASE_URL", "") or "").rstrip("/")
        urls = [url for url in [primary, fallback] if url]
        deduped: list[str] = []
        for url in urls:
            if url not in deduped:
                deduped.append(url)
        return deduped or ["http://127.0.0.1:8010"]

    def _should_use_reference_text(self) -> bool:
        return platform.system().lower() == "darwin"

    def _load_reference_text(self, reference_text_path: str) -> str | None:
        try:
            with open(reference_text_path, "r", encoding="utf-8") as handle:
                raw_text = handle.read()
        except UnicodeDecodeError as exc:
            raise RuntimeError("ref_text input is not a valid UTF-8 subtitle/text file") from exc
        except OSError as exc:
            raise RuntimeError(f"Unable to read ref_text input: {exc}") from exc

        if reference_text_path.lower().endswith((".srt", ".vtt", ".ass", ".ssa")):
            try:
                cues = parse_srt(raw_text)
            except Exception:
                cues = []
            if cues:
                joined = " ".join(self._normalize_tts_text(cue.text) for cue in cues)
                cleaned = re.sub(r"\s+", " ", joined).strip()
                return cleaned or None

        cleaned = re.sub(r"\s+", " ", raw_text).strip()
        return cleaned or None

    def _create_speech_blocks(
        self,
        cues: list[SubtitleCue],
        *,
        max_gap_seconds: float,
        min_chars: int,
        max_chars: int,
        min_duration_seconds: float,
        max_duration_seconds: float,
    ) -> list[SpeechBlock]:
        blocks: list[SpeechBlock] = []
        current_cues: list[SubtitleCue] = []

        for cue in cues:
            if not current_cues:
                current_cues = [cue]
                continue

            candidate = current_cues + [cue]
            candidate_text = self._join_block_text(candidate)
            candidate_duration = candidate[-1].end_seconds - candidate[0].start_seconds
            gap_seconds = max(0.0, cue.start_seconds - current_cues[-1].end_seconds)
            current_text = self._join_block_text(current_cues)
            current_duration = current_cues[-1].end_seconds - current_cues[0].start_seconds
            should_merge = (
                gap_seconds <= max_gap_seconds
                and candidate_duration <= max_duration_seconds
                and len(candidate_text) <= max_chars
                and (
                    len(current_text) < min_chars
                    or current_duration < min_duration_seconds
                    or len(self._normalize_tts_text(cue.text)) < min_chars
                )
            )

            if should_merge:
                current_cues.append(cue)
                continue

            blocks.extend(self._finalize_block_group(current_cues, max_chars=max_chars))
            current_cues = [cue]

        if current_cues:
            blocks.extend(self._finalize_block_group(current_cues, max_chars=max_chars))

        merged_blocks: list[SpeechBlock] = []
        for block in blocks:
            if not merged_blocks:
                merged_blocks.append(block)
                continue
            prev = merged_blocks[-1]
            combined_text = self._join_text_parts(prev.text, block.text)
            combined_duration = block.end_seconds - prev.start_seconds
            gap_seconds = max(0.0, block.start_seconds - prev.end_seconds)
            if (
                len(block.text) < min_chars
                and gap_seconds <= max_gap_seconds
                and combined_duration <= max_duration_seconds
                and len(combined_text) <= max_chars
            ):
                merged_blocks[-1] = SpeechBlock(
                    index=prev.index,
                    start_seconds=prev.start_seconds,
                    end_seconds=block.end_seconds,
                    text=combined_text,
                    cue_indexes=prev.cue_indexes + block.cue_indexes,
                )
            else:
                merged_blocks.append(block)

        return [
            SpeechBlock(
                index=index,
                start_seconds=block.start_seconds,
                end_seconds=block.end_seconds,
                text=block.text,
                cue_indexes=block.cue_indexes,
            )
            for index, block in enumerate(merged_blocks, start=1)
        ]

    def _finalize_block_group(self, cues: list[SubtitleCue], *, max_chars: int) -> list[SpeechBlock]:
        if not cues:
            return []

        text = self._join_block_text(cues)
        sentences = self._split_text_for_block(text, max_chars=max_chars)
        if len(sentences) == 1:
            return [
                SpeechBlock(
                    index=0,
                    start_seconds=cues[0].start_seconds,
                    end_seconds=cues[-1].end_seconds,
                    text=sentences[0],
                    cue_indexes=[cue.index for cue in cues],
                )
            ]

        total_span = max(cues[-1].end_seconds - cues[0].start_seconds, 0.001)
        per_sentence_chars = [max(len(sentence), 1) for sentence in sentences]
        total_chars = sum(per_sentence_chars)
        start = cues[0].start_seconds
        blocks: list[SpeechBlock] = []
        for sentence_index, sentence in enumerate(sentences):
            ratio = per_sentence_chars[sentence_index] / total_chars if total_chars else 1 / len(sentences)
            if sentence_index == len(sentences) - 1:
                end = cues[-1].end_seconds
            else:
                end = start + total_span * ratio
            blocks.append(
                SpeechBlock(
                    index=0,
                    start_seconds=start,
                    end_seconds=end,
                    text=sentence,
                    cue_indexes=[cue.index for cue in cues],
                )
            )
            start = end
        return blocks

    def _split_text_for_block(self, text: str, *, max_chars: int) -> list[str]:
        normalized = self._normalize_tts_text(text)
        if len(normalized) <= max_chars:
            return [normalized]

        parts = [part.strip() for part in SENTENCE_SPLIT_RE.split(normalized) if part.strip()]
        if len(parts) <= 1:
            return self._split_text_by_length(normalized, max_chars=max_chars)

        chunks: list[str] = []
        current = parts[0]
        for part in parts[1:]:
            combined = self._join_text_parts(current, part)
            if len(combined) <= max_chars:
                current = combined
            else:
                chunks.append(current)
                current = part
        chunks.append(current)
        return [chunk for part in chunks for chunk in self._split_text_by_length(part, max_chars=max_chars)]

    def _split_text_by_length(self, text: str, *, max_chars: int) -> list[str]:
        remaining = text.strip()
        chunks: list[str] = []
        while len(remaining) > max_chars:
            split_at = remaining.rfind(" ", 0, max_chars + 1)
            if split_at < max_chars // 2:
                split_at = max_chars
            chunks.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()
        if remaining:
            chunks.append(remaining)
        return chunks

    def _join_block_text(self, cues: list[SubtitleCue]) -> str:
        return self._join_text_parts(*(self._normalize_tts_text(cue.text) for cue in cues))

    @staticmethod
    def _normalize_tts_text(text: str) -> str:
        return " ".join(text.replace("\n", " ").split())

    @staticmethod
    def _join_text_parts(*parts: str) -> str:
        cleaned = [part.strip() for part in parts if part and part.strip()]
        return " ".join(cleaned)

    def _can_use_minimax(self, reference_audio_path: str | None) -> bool:
        if reference_audio_path:
            return False
        return bool(self._minimax_api_key())

    def _minimax_api_key(self) -> str:
        return str(os.environ.get("MINIMAX_API_KEY", "") or "").strip()

    def _minimax_base_url(self) -> str:
        return str(os.environ.get("VIDEO_MINIMAX_TTS_BASE_URL", "https://api.minimaxi.com/v1") or "").rstrip("/")

    def _minimax_model(self) -> str:
        return str(os.environ.get("VIDEO_MINIMAX_TTS_MODEL", "speech-2.8-hd") or "speech-2.8-hd").strip()

    def _minimax_voice_id(self, language: str) -> str:
        configured = str(os.environ.get("VIDEO_MINIMAX_TTS_VOICE_ID", "") or "").strip()
        if configured:
            return configured
        return "female-shaonv"

    async def _synthesize_cue_with_minimax(
        self,
        *,
        text: str,
        language: str,
        output_path: str,
    ) -> None:
        api_key = self._minimax_api_key()
        if not api_key:
            raise RuntimeError("MINIMAX_API_KEY is not configured")

        payload = {
            "model": self._minimax_model(),
            "text": text,
            "voice_setting": {
                "voice_id": self._minimax_voice_id(language),
                "speed": float(os.environ.get("VIDEO_MINIMAX_TTS_SPEED", "1.0") or "1.0"),
                "vol": float(os.environ.get("VIDEO_MINIMAX_TTS_VOLUME", "1.0") or "1.0"),
            },
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        async with httpx.AsyncClient(timeout=240) as client:
            response = await client.post(
                f"{self._minimax_base_url()}/t2a_v2",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            content_type = str(response.headers.get("content-type") or "").lower()
            if "json" in content_type:
                try:
                    payload = response.json()
                except ValueError:
                    payload = {"raw": response.text[:200]}
                base_resp = payload.get("base_resp") if isinstance(payload, dict) else None
                status_msg = ""
                if isinstance(base_resp, dict):
                    status_msg = str(base_resp.get("status_msg") or "").strip()
                audio_payload = None
                if isinstance(payload, dict):
                    data_payload = payload.get("data")
                    if isinstance(data_payload, dict):
                        audio_payload = data_payload.get("audio")
                if isinstance(audio_payload, str) and audio_payload.strip():
                    audio_bytes = self._decode_minimax_audio(audio_payload)
                else:
                    raise RuntimeError(
                        "MiniMax TTS returned JSON instead of audio"
                        + (f": {status_msg}" if status_msg else "")
                    )
            else:
                audio_bytes = response.content
            temp_source = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".mp3" if "mpeg" in content_type or "mp3" in content_type or "json" in content_type else ".wav",
            )
            temp_source.close()
            try:
                with open(temp_source.name, "wb") as handle:
                    handle.write(audio_bytes)
                await self.run_ffmpeg([
                    "-i", temp_source.name,
                    "-vn",
                    "-ac", "1",
                    "-ar", "24000",
                    output_path,
                ])
            finally:
                try:
                    os.unlink(temp_source.name)
                except OSError:
                    pass

    def _decode_minimax_audio(self, audio_payload: str) -> bytes:
        payload = audio_payload.strip()
        if not payload:
            raise RuntimeError("MiniMax TTS returned empty audio payload")
        try:
            return bytes.fromhex(payload)
        except ValueError:
            try:
                import base64
                return base64.b64decode(payload)
            except Exception as exc:  # pragma: no cover - defensive
                raise RuntimeError("MiniMax TTS returned an unknown audio encoding") from exc

    async def _ensure_wav_reference(self, input_path: str) -> str:
        if input_path.lower().endswith(".wav"):
            return input_path
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        tmp_file.close()
        await self.run_ffmpeg([
            "-i", input_path,
            "-vn",
            "-ac", "1",
            "-ar", "24000",
            tmp_file.name,
        ])
        return tmp_file.name

    async def _post_local_tts_stream(
        self,
        *,
        client: httpx.AsyncClient,
        text: str,
        language: str,
        reference_audio_path: str | None,
        speaker_id: str | None,
    ) -> httpx.Response:
        files: dict[str, tuple[str, object, str]] = {}
        data = {"text": text, "language": language}
        if speaker_id:
            data["speaker_id"] = speaker_id
        elif reference_audio_path:
            files["speaker_wav"] = (
                Path(reference_audio_path).name,
                open(reference_audio_path, "rb"),
                "audio/wav",
            )
        try:
            return await client.post("/v1/tts/stream", data=data, files=files or None)
        finally:
            upload = files.get("speaker_wav")
            if upload:
                upload[1].close()

    async def _synthesize_cue_with_local_base_url(
        self,
        *,
        client: httpx.AsyncClient,
        text: str,
        language: str,
        output_path: str,
        reference_audio_path: str | None,
        speaker_id: str | None,
    ) -> tuple[str, str | None]:
        last_exc: Exception | None = None

        if speaker_id:
            try:
                response = await self._post_local_tts_stream(
                    client=client,
                    text=text,
                    language=language,
                    reference_audio_path=None,
                    speaker_id=speaker_id,
                )
                response.raise_for_status()
                with open(output_path, "wb") as handle:
                    handle.write(response.content)
                provider = str(response.headers.get("x-tts-provider") or "local").strip().lower() or "local"
                return provider, speaker_id
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Local TTS request with speaker_id %s failed; falling back to speaker_wav upload: %s",
                    speaker_id,
                    exc,
                )

        if reference_audio_path:
            response = await self._post_local_tts_stream(
                client=client,
                text=text,
                language=language,
                reference_audio_path=reference_audio_path,
                speaker_id=None,
            )
            response.raise_for_status()
            with open(output_path, "wb") as handle:
                handle.write(response.content)
            provider = str(response.headers.get("x-tts-provider") or "local").strip().lower() or "local"
            return provider, None

        if last_exc:
            raise last_exc
        raise RuntimeError("Local TTS requires either a speaker_id or reference audio")

    async def _try_register_local_speaker(
        self,
        *,
        tts_base_urls: list[str],
        current_base_url: str,
        client: httpx.AsyncClient,
        reference_audio_path: str,
        speaker_text: str | None,
    ) -> tuple[str | None, httpx.AsyncClient, str]:
        last_exc: Exception | None = None
        ordered_urls = [current_base_url] + [url for url in tts_base_urls if url != current_base_url]
        for base_url in ordered_urls:
            if _base_url_string(client) != base_url:
                await client.aclose()
                client = httpx.AsyncClient(base_url=base_url, timeout=240)
            try:
                speaker_id = await self._register_local_speaker(
                    client=client,
                    reference_audio_path=reference_audio_path,
                    speaker_text=speaker_text,
                )
                return speaker_id, client, base_url
            except Exception as exc:
                last_exc = exc
                logger.warning("Local TTS speaker registration failed on %s: %s", base_url, exc)
        if last_exc:
            logger.warning("All local TTS speaker registration attempts failed: %s", last_exc)
        return None, client, current_base_url

    async def _synthesize_cue_with_local_fallback(
        self,
        *,
        tts_base_urls: list[str],
        current_base_url: str,
        client: httpx.AsyncClient,
        text: str,
        language: str,
        output_path: str,
        reference_audio_path: str | None,
        speaker_id: str | None,
        speaker_text: str | None,
    ) -> tuple[str, httpx.AsyncClient, str, str | None]:
        ordered_urls = [current_base_url] + [url for url in tts_base_urls if url != current_base_url]
        last_exc: Exception | None = None
        current_speaker_id = speaker_id
        active_client = client

        for base_url in ordered_urls:
            if _base_url_string(active_client) != base_url:
                await active_client.aclose()
                active_client = httpx.AsyncClient(base_url=base_url, timeout=240)
                current_speaker_id = None
                if reference_audio_path:
                    try:
                        current_speaker_id = await self._register_local_speaker(
                            client=active_client,
                            reference_audio_path=reference_audio_path,
                            speaker_text=speaker_text,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Speaker registration failed on fallback local TTS %s; continuing with per-request upload: %s",
                            base_url,
                            exc,
                        )
            try:
                provider, next_speaker_id = await self._synthesize_cue_with_local_base_url(
                    client=active_client,
                    text=text,
                    language=language,
                    output_path=output_path,
                    reference_audio_path=reference_audio_path,
                    speaker_id=current_speaker_id,
                )
                return provider, active_client, base_url, next_speaker_id
            except Exception as exc:
                last_exc = exc
                logger.warning("Local TTS synthesis failed on %s for current block: %s", base_url, exc)
                continue

        raise RuntimeError(f"All local TTS services failed for subtitle block: {last_exc}") from last_exc

    async def _register_local_speaker(
        self,
        *,
        client: httpx.AsyncClient,
        reference_audio_path: str,
        speaker_text: str | None,
    ) -> str:
        with open(reference_audio_path, "rb") as reference_audio:
            response = await client.post(
                "/v1/speakers/register",
                files={
                    "speaker_wav": (
                        Path(reference_audio_path).name,
                        reference_audio,
                        "audio/wav",
                    )
                },
                data={"speaker_text": speaker_text or ""},
            )
        response.raise_for_status()
        payload = response.json()
        speaker_id = str(payload.get("speaker_id") or "").strip()
        if not speaker_id:
            raise RuntimeError("Local TTS speaker registration did not return speaker_id")
        logger.info("Registered local TTS speaker %s", speaker_id)
        return speaker_id

    async def _probe_audio_duration_ms(self, audio_path: str) -> int:
        probe = await self.run_ffprobe(audio_path)
        duration_seconds = float(probe.get("format", {}).get("duration", 0) or 0)
        if duration_seconds <= 0:
            raise RuntimeError(f"Unable to determine audio duration for {audio_path}")
        return max(1, int(round(duration_seconds * 1000)))

    async def _align_audio_blocks(
        self,
        *,
        audio_blocks: list[GeneratedAudioBlock],
        timeline_duration: float,
        max_speedup: float,
        max_leading_delay_ms: int,
        temp_audio_files: list[str],
    ) -> tuple[list[tuple[str, int]], float, int]:
        audio_inputs: list[tuple[str, int]] = []
        current_time_ms = 0
        total_shift_ms = 0
        peak_shift_ms = 0

        for index, block in enumerate(audio_blocks):
            base_start_ms = int(round(block.block.start_seconds * 1000))
            if index < len(audio_blocks) - 1:
                next_start_ms = int(round(audio_blocks[index + 1].block.start_seconds * 1000))
                slot_ms = max(1, next_start_ms - base_start_ms)
            else:
                slot_ms = max(1, int(round((block.block.end_seconds - block.block.start_seconds) * 1000)))

            play_start_ms = max(current_time_ms, base_start_ms + total_shift_ms)
            delay_ms = 0
            selected_path = block.audio_path
            selected_duration_ms = block.duration_ms

            if selected_duration_ms < slot_ms and total_shift_ms <= 0:
                spare_ms = slot_ms - selected_duration_ms
                delay_ms = min(max_leading_delay_ms, int(spare_ms * 0.7))

            speedup_factor = 1.0
            if total_shift_ms > 0 and max_speedup > 1.0:
                speedup_factor = min(
                    max_speedup,
                    (selected_duration_ms + total_shift_ms) / max(selected_duration_ms, 1),
                )
            elif total_shift_ms <= 0 and selected_duration_ms > slot_ms and max_speedup > 1.0:
                speedup_factor = min(
                    max_speedup,
                    selected_duration_ms / max(slot_ms, 1),
                )

            if speedup_factor > 1.01:
                selected_path = await self._speed_up_audio(
                    audio_path=block.audio_path,
                    factor=speedup_factor,
                    temp_audio_files=temp_audio_files,
                )
                selected_duration_ms = await self._probe_audio_duration_ms(selected_path)

            actual_start_ms = play_start_ms + delay_ms
            actual_span_ms = delay_ms + selected_duration_ms
            audio_inputs.append((selected_path, actual_start_ms))
            current_time_ms = actual_start_ms + selected_duration_ms

            if actual_span_ms > slot_ms:
                total_shift_ms += actual_span_ms - slot_ms
            else:
                total_shift_ms = max(0, total_shift_ms - (slot_ms - actual_span_ms))
            peak_shift_ms = max(peak_shift_ms, total_shift_ms)

        final_duration = max(timeline_duration, current_time_ms / 1000)
        return audio_inputs, final_duration, peak_shift_ms

    async def _speed_up_audio(
        self,
        *,
        audio_path: str,
        factor: float,
        temp_audio_files: list[str],
    ) -> str:
        adjusted = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        adjusted.close()
        temp_audio_files.append(adjusted.name)
        filters = self._build_atempo_filters(factor)
        await self.run_ffmpeg([
            "-i", audio_path,
            "-af", filters,
            adjusted.name,
        ])
        return adjusted.name

    @staticmethod
    def _build_atempo_filters(factor: float) -> str:
        segments: list[str] = []
        remaining = factor
        while remaining > 2.0:
            segments.append("atempo=2.0")
            remaining /= 2.0
        while remaining < 0.5:
            segments.append("atempo=0.5")
            remaining /= 0.5
        if abs(remaining - 1.0) > 0.01:
            segments.append(f"atempo={remaining:.3f}")
        return ",".join(segments) or "anull"

    @staticmethod
    def _merge_provider(current: str | None, new_provider: str) -> str:
        if not current:
            return (new_provider or "local").strip().lower() or "local"
        if current == "mixed":
            return current
        normalized = (new_provider or "local").strip().lower() or "local"
        if current != normalized:
            return "mixed"
        return normalized

    async def _mix_audio_timeline(
        self,
        *,
        audio_inputs: list[tuple[str, int]],
        duration: float,
        output_path: str,
    ) -> None:
        args = [
            "-f", "lavfi",
            "-t", f"{duration:.3f}",
            "-i", "anullsrc=r=24000:cl=mono",
        ]
        filter_parts: list[str] = []
        mix_inputs = ["[0:a]"]
        for index, (audio_path, delay_ms) in enumerate(audio_inputs, start=1):
            args.extend(["-i", audio_path])
            delayed_label = f"d{index}"
            filter_parts.append(f"[{index}:a]adelay={delay_ms}:all=1[{delayed_label}]")
            mix_inputs.append(f"[{delayed_label}]")

        filter_parts.append(
            "".join(mix_inputs) + f"amix=inputs={len(mix_inputs)}:duration=longest:dropout_transition=0[aout]"
        )
        args.extend([
            "-filter_complex", ";".join(filter_parts),
            "-map", "[aout]",
            "-t", f"{duration:.3f}",
            output_path,
        ])
        await self.run_ffmpeg(args)
