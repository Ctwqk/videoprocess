from __future__ import annotations

from dataclasses import dataclass
import re


TIMECODE_RE = re.compile(
    r"(?P<start>\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(?P<end>\d{2}:\d{2}:\d{2},\d{3})"
)


@dataclass
class SubtitleCue:
    index: int
    start_seconds: float
    end_seconds: float
    text: str


def srt_timestamp_to_seconds(value: str) -> float:
    hours, minutes, seconds = value.split(":")
    whole_seconds, milliseconds = seconds.split(",")
    return (
        int(hours) * 3600
        + int(minutes) * 60
        + int(whole_seconds)
        + int(milliseconds) / 1000
    )


def seconds_to_srt_timestamp(value: float) -> str:
    total_milliseconds = max(0, int(round(value * 1000)))
    hours = total_milliseconds // 3_600_000
    remaining = total_milliseconds % 3_600_000
    minutes = remaining // 60_000
    remaining %= 60_000
    seconds = remaining // 1000
    milliseconds = remaining % 1000
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"


def parse_srt(content: str) -> list[SubtitleCue]:
    normalized = content.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    cues: list[SubtitleCue] = []
    for block in re.split(r"\n{2,}", normalized):
        lines = [line for line in block.split("\n") if line.strip()]
        if len(lines) < 2:
            continue

        line_index = 0
        index_line = lines[0].strip()
        if index_line.isdigit():
            cue_index = int(index_line)
            line_index = 1
        else:
            cue_index = len(cues) + 1

        if line_index >= len(lines):
            continue

        match = TIMECODE_RE.fullmatch(lines[line_index].strip())
        if not match:
            continue

        text = "\n".join(lines[line_index + 1 :]).strip()
        cues.append(
            SubtitleCue(
                index=cue_index,
                start_seconds=srt_timestamp_to_seconds(match.group("start")),
                end_seconds=srt_timestamp_to_seconds(match.group("end")),
                text=text,
            )
        )

    return cues


def write_srt(cues: list[SubtitleCue], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        for display_index, cue in enumerate(cues, start=1):
            handle.write(f"{display_index}\n")
            handle.write(
                f"{seconds_to_srt_timestamp(cue.start_seconds)} --> "
                f"{seconds_to_srt_timestamp(cue.end_seconds)}\n"
            )
            handle.write(f"{cue.text.strip()}\n\n")
