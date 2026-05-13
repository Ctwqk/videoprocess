from __future__ import annotations

import json
import os
import re

import httpx

from worker.handlers.base import BaseHandler
from worker.handlers.subtitle_utils import SubtitleCue, parse_srt, write_srt


class SubtitleTranslateHandler(BaseHandler):
    DEFAULT_CHUNK_SIZE = 8
    DEFAULT_MAX_TOKENS = 2048
    MINIMAX_MAX_TOKENS = 196000

    async def execute(self, node_config, input_paths, output_path):
        subtitle_path = input_paths["subtitle_file"]
        target_language = str(node_config.get("target_language", "") or "").strip()
        if not target_language:
            raise RuntimeError("target_language is required")

        with open(subtitle_path, "r", encoding="utf-8") as handle:
            cues = parse_srt(handle.read())

        if not cues:
            raise RuntimeError("Subtitle file contains no cues")

        translated_cues = await self._translate_cues(
            cues,
            source_language=str(node_config.get("source_language", "") or "").strip() or None,
            target_language=target_language,
            model=str(node_config.get("model", "") or "").strip() or None,
        )
        write_srt(translated_cues, output_path)
        return {
            "source_language": str(node_config.get("source_language", "") or "").strip() or "auto",
            "target_language": target_language,
            "subtitle_segments": len(translated_cues),
        }

    async def _translate_cues(
        self,
        cues: list[SubtitleCue],
        *,
        source_language: str | None,
        target_language: str,
        model: str | None,
    ) -> list[SubtitleCue]:
        translated_text_by_index: dict[int, str] = {}

        async with httpx.AsyncClient(
            base_url=os.environ.get("VIDEO_LLM_BASE_URL", "http://127.0.0.1:8000/v1").rstrip("/"),
            timeout=180,
        ) as client:
            cursor = 0
            while cursor < len(cues):
                chunk = cues[cursor:cursor + self.DEFAULT_CHUNK_SIZE]
                translated_entries = await self._translate_chunk_with_split(
                    client,
                    chunk,
                    source_language=source_language,
                    target_language=target_language,
                    model=model,
                )
                for entry in translated_entries:
                    translated_text_by_index[int(entry["index"])] = str(entry["text"]).strip()
                cursor += len(chunk)

        translated_cues: list[SubtitleCue] = []
        for cue in cues:
            translated_text = translated_text_by_index.get(cue.index, "").strip()
            if not translated_text:
                raise RuntimeError(f"Missing translated text for subtitle cue {cue.index}")
            translated_cues.append(
                SubtitleCue(
                    index=cue.index,
                    start_seconds=cue.start_seconds,
                    end_seconds=cue.end_seconds,
                    text=translated_text,
                )
            )
        return translated_cues

    async def _translate_chunk_with_split(
        self,
        client: httpx.AsyncClient,
        chunk: list[SubtitleCue],
        *,
        source_language: str | None,
        target_language: str,
        model: str | None,
    ) -> list[dict]:
        try:
            translated_entries = await self._request_chunk_translation(
                client,
                chunk,
                source_language=source_language,
                target_language=target_language,
                model=model,
            )
            self._validate_translated_entries(chunk, translated_entries)
            return translated_entries
        except Exception as exc:
            if len(chunk) <= 1 or not self._should_split_error(exc):
                raise RuntimeError(f"Subtitle translation could not produce valid JSON: {exc}") from exc

            midpoint = max(1, len(chunk) // 2)
            left_entries = await self._translate_chunk_with_split(
                client,
                chunk[:midpoint],
                source_language=source_language,
                target_language=target_language,
                model=model,
            )
            right_entries = await self._translate_chunk_with_split(
                client,
                chunk[midpoint:],
                source_language=source_language,
                target_language=target_language,
                model=model,
            )
            return left_entries + right_entries

    def _should_split_error(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {400, 408, 413, 422, 429, 500, 502, 503, 504}

        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "truncated",
                "max tokens",
                "context",
                "wrong number of entries",
                "mismatched cue indices",
                "json",
                "bad request",
                "too large",
                "length",
            )
        )

    async def _request_chunk_translation(
        self,
        client: httpx.AsyncClient,
        chunk: list[SubtitleCue],
        *,
        source_language: str | None,
        target_language: str,
        model: str | None,
    ) -> list[dict]:
        prompt_payload = {
            "source_language": source_language or "auto",
            "target_language": target_language,
            "entries": [{"index": cue.index, "text": cue.text} for cue in chunk],
        }
        request_body = {
            "source": "videoprocess",
            "profile": "generic_chat",
            "temperature": 0.1,
            "max_tokens": self._requested_max_tokens(model),
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Translate subtitle entries into the target language. "
                        "Return only JSON in the form "
                        '[{"index": 1, "text": "..."}, ...]. '
                        "Keep each index exactly once. Preserve line breaks inside text with \\n. "
                        "Do not include markdown fences or commentary."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(prompt_payload, ensure_ascii=False),
                },
            ],
        }
        if model:
            request_body["model"] = model

        response = await client.post("/chat/completions", json=request_body)
        response.raise_for_status()
        payload = response.json()
        choices = payload.get("choices") or [{}]
        choice = choices[0] or {}
        finish_reason = str(choice.get("finish_reason") or "").strip().lower()
        content = (choice.get("message") or {}).get("content", "")
        translated_entries = self._parse_translated_entries(content)
        if finish_reason == "length":
            raise RuntimeError("Subtitle translation response was truncated")
        return translated_entries

    def _requested_max_tokens(self, model: str | None) -> int:
        normalized = str(model or "").strip().lower()
        if "minimax" in normalized:
            return self.MINIMAX_MAX_TOKENS
        return self.DEFAULT_MAX_TOKENS

    def _validate_translated_entries(self, chunk: list[SubtitleCue], translated_entries: list[dict]) -> None:
        if len(translated_entries) != len(chunk):
            raise RuntimeError("Subtitle translation returned the wrong number of entries")

        chunk_indexes = {cue.index for cue in chunk}
        translated_indexes = {entry["index"] for entry in translated_entries}
        if chunk_indexes != translated_indexes:
            raise RuntimeError("Subtitle translation returned mismatched cue indices")

    def _parse_translated_entries(self, raw_content: str) -> list[dict]:
        content = raw_content.strip()
        fenced_match = re.search(r"```(?:json)?\s*(.*?)```", content, flags=re.DOTALL)
        if fenced_match:
            content = fenced_match.group(1).strip()
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
        if not content.startswith("["):
            array_match = re.search(r"(\[\s*\{.*\}\s*\])", content, flags=re.DOTALL)
            if array_match:
                content = array_match.group(1).strip()

        parsed = json.loads(content)
        if not isinstance(parsed, list):
            raise RuntimeError("Subtitle translation did not return a JSON array")

        entries: list[dict] = []
        for item in parsed:
            if not isinstance(item, dict):
                raise RuntimeError("Subtitle translation returned a non-object entry")
            index = int(item.get("index"))
            text = str(item.get("text", "")).strip()
            if not text:
                raise RuntimeError(f"Subtitle translation returned empty text for cue {index}")
            entries.append({"index": index, "text": text})
        return entries
