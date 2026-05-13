from faster_whisper import WhisperModel

def srt_ts(t):
    h = int(t // 3600); t %= 3600
    m = int(t // 60);   t %= 60
    s = int(t)
    ms = int(round((t - s) * 1000))
    if ms == 1000:
        s += 1; ms = 0
    return f"{h:02}:{m:02}:{s:02},{ms:03}"

model = WhisperModel("medium", device="cuda", compute_type="float16")
segments, info = model.transcribe("/media/output.mp4")

out = "/work/output.srt"
with open(out, "w", encoding="utf-8") as f:
    for i, seg in enumerate(segments, 1):
        f.write(f"{i}\n{srt_ts(seg.start)} --> {srt_ts(seg.end)}\n{seg.text.strip()}\n\n")

print("language:", info.language)
print("wrote:", out)
