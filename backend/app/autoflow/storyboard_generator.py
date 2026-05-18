from __future__ import annotations

from app.schemas.autoflow import (
    AutoFlowStoryboardRequest,
    AutoFlowStoryboardResponse,
    CameraSpec,
    ShotSpec,
    StoryboardPlan,
    VideoGenerationHints,
    VisualStyleSpec,
)
from app.autoflow.platform_profiles import PlatformProfile, PlatformProfileService


class StoryboardGenerator:
    def generate(self, request: AutoFlowStoryboardRequest) -> AutoFlowStoryboardResponse:
        strategy = _storyboard_strategy(request)
        platform_profile = PlatformProfileService().for_platforms(request.target_platforms)
        subject = _subject(request.prompt)
        base_shots = _shot_templates(subject, request.allow_video_generation or strategy == "generate_missing")
        shot_count = max(request.min_shots, min(request.max_shots, len(base_shots)))
        shots, pacing_warnings = _fit_durations(base_shots[:shot_count], request.target_duration, platform_profile)
        storyboard = StoryboardPlan(
            subject=subject,
            title=_title(subject),
            logline=_logline(subject, request.target_duration),
            style=request.style if request.style != "auto" else _style(subject, request.prompt),
            target_platforms=request.target_platforms,
            aspect_ratio=request.aspect_ratio,
            total_duration=float(request.target_duration),
            source_strategy=strategy,
            allow_video_generation=request.allow_video_generation,
            shots=shots,
            title_candidates=_title_candidates(subject),
            description=f"围绕{subject}组织的短视频分镜计划，优先使用确定性素材检索和剪辑节点生成工作流。",
            tags=[subject, "短视频", "AutoFlow"],
            hashtags=[f"#{subject}", "#短视频"],
            warnings=pacing_warnings,
            extra={"platform_profile": platform_profile.to_dict()},
        )
        return AutoFlowStoryboardResponse(storyboard=storyboard)


def _storyboard_strategy(request: AutoFlowStoryboardRequest) -> str:
    if request.source_strategy == "auto":
        if request.input_asset_id:
            return "input_video"
        if request.material_library_ids:
            return "material_library"
        if request.allow_video_generation:
            return "generate_missing"
        return "input_video"
    if request.source_strategy == "auto":
        return "input_video"
    return request.source_strategy  # type: ignore[return-value]


def _subject(prompt: str) -> str:
    lowered = prompt.lower()
    if any(term in prompt for term in ("小猫", "猫咪")) or "kitten" in lowered or "cat" in lowered:
        return "小猫"
    if any(term in prompt for term in ("小狗", "狗狗")) or "puppy" in lowered or "dog" in lowered:
        return "dog" if "dog" in lowered and "小狗" not in prompt else "小狗"
    if "产品" in prompt or "product" in lowered:
        return "产品"
    return "视频主题"


def _shot_templates(subject: str, generation_enabled: bool) -> list[ShotSpec]:
    if subject == "小猫":
        return [
            _shot(
                "shot_01",
                "hook",
                "开场用小猫脸部或眼睛清楚的近景，最好带有看镜头、突然靠近或眨眼的动作，第一秒就让观众知道这是可爱小猫主题。",
                "小猫近景看镜头，可爱，呆萌，脸部特写",
                generation_enabled,
                "A cute kitten looking directly into the camera in a close-up shot, warm natural light, curious expression, realistic short video style.",
                CameraSpec(shot_size="close_up", angle="eye_level", movement="static"),
                VisualStyleSpec(mood="cute and warm", lighting="soft natural light", realism="realistic"),
            ),
            _shot(
                "shot_02",
                "action",
                "小猫在地板、沙发或房间里追逐玩具、扑向玩具或快速跑动，动作要明显，适合承担短视频中段的节奏推进。",
                "小猫追玩具，扑玩具，跳跃，玩耍",
                generation_enabled,
                "A playful kitten chasing a small toy across a wooden floor, low angle camera near the ground, quick energetic movement, warm indoor daylight, realistic video.",
                CameraSpec(shot_size="medium", angle="low_angle", movement="handheld"),
                VisualStyleSpec(mood="playful", lighting="natural indoor light", realism="realistic"),
            ),
            _shot(
                "shot_03",
                "reaction",
                "选择小猫突然回头、歪头、扑空、停住或露出疑惑表情的搞笑反应镜头，用来制造记忆点，但要避免危险或受伤画面。",
                "小猫搞笑反应，扑空，回头，歪头",
                generation_enabled,
                "A cute kitten suddenly stops and tilts its head with a funny confused expression, cozy room, realistic short video, safe and playful.",
                CameraSpec(shot_size="medium", angle="eye_level", movement="static"),
                VisualStyleSpec(mood="funny but safe", realism="realistic"),
            ),
            _shot(
                "shot_04",
                "detail",
                "加入小猫爪子、尾巴、打哈欠或伸懒腰的细节镜头，让视频节奏从动作段落过渡到更治愈的情绪。",
                "小猫爪子，打哈欠，伸懒腰，治愈细节",
                generation_enabled,
                "A soft detailed shot of a kitten paw and sleepy yawn, cozy indoor light, gentle healing mood, realistic video.",
                CameraSpec(shot_size="close_up", angle="eye_level", movement="static"),
                VisualStyleSpec(mood="soft and healing", lighting="warm indoor light", realism="realistic"),
            ),
            _shot(
                "shot_05",
                "ending",
                "结尾回到小猫安静看镜头或窝在柔软位置的画面，作为自然收束，适合叠加一句轻量标题或结束文案。",
                "小猫安静看镜头，睡觉，治愈结尾",
                generation_enabled,
                "A calm kitten resting in a soft cozy place and looking at the camera, warm natural light, peaceful ending shot, realistic video.",
                CameraSpec(shot_size="close_up", angle="eye_level", movement="static"),
                VisualStyleSpec(mood="calm and healing", lighting="warm natural light", realism="realistic"),
            ),
        ]

    if subject in {"小狗", "dog"}:
        return [
            _shot(
                "shot_01",
                "hook",
                "开场使用小狗靠近镜头、摇尾巴或抬头看人的近景，画面要立刻呈现活泼友好的情绪。",
                "小狗近景，看镜头，摇尾巴，可爱",
                generation_enabled,
                "A cute puppy looking into the camera and wagging its tail, bright natural light, realistic short video hook.",
                CameraSpec(shot_size="close_up", angle="eye_level", movement="handheld"),
                VisualStyleSpec(mood="friendly and playful", lighting="bright natural light", realism="realistic"),
            ),
            _shot(
                "shot_02",
                "action",
                "中段选择小狗奔跑、追球或跳起来的明显动作，节奏要轻快，适合和音乐卡点拼接。",
                "小狗奔跑，追球，跳跃，玩耍",
                generation_enabled,
                "A playful puppy running after a ball in a safe open area, energetic movement, realistic video.",
                CameraSpec(shot_size="wide", angle="eye_level", movement="tracking"),
                VisualStyleSpec(mood="energetic", realism="realistic"),
            ),
            _shot(
                "shot_03",
                "ending",
                "结尾用小狗坐下、喘气、看镜头或被轻轻抚摸的镜头收束，形成温暖安全的结束。",
                "小狗坐下，看镜头，温暖结尾",
                generation_enabled,
                "A puppy sitting calmly and looking at the camera after playing, warm friendly mood, realistic video.",
                CameraSpec(shot_size="medium", angle="eye_level", movement="static"),
                VisualStyleSpec(mood="warm and safe", realism="realistic"),
            ),
        ]

    return [
        _shot(
            "shot_01",
            "hook",
            "开场选择最能代表主题的清晰镜头，画面主体要明确，节奏上适合吸引观众继续观看。",
            f"{subject} 清晰主体 开场",
            generation_enabled,
            f"A clear opening shot focused on {subject}, strong subject visibility, realistic short video style.",
            CameraSpec(shot_size="medium", angle="eye_level", movement="static"),
            VisualStyleSpec(mood="clear and engaging", realism="realistic"),
        ),
        _shot(
            "shot_02",
            "action",
            "中段选择有动作、变化或信息推进的镜头，让视频不只是静态展示，适合接在开场之后。",
            f"{subject} 动作 变化 过程",
            generation_enabled,
            f"A dynamic shot showing movement or a meaningful change related to {subject}, realistic video.",
            CameraSpec(shot_size="medium", angle="eye_level", movement="handheld"),
            VisualStyleSpec(mood="dynamic", realism="realistic"),
        ),
        _shot(
            "shot_03",
            "ending",
            "结尾选择能自然收束主题的镜头，可以是结果、安静定格或最有记忆点的画面。",
            f"{subject} 结尾 定格 结果",
            generation_enabled,
            f"A closing shot that resolves the short video about {subject}, clean composition, realistic style.",
            CameraSpec(shot_size="medium", angle="eye_level", movement="static"),
            VisualStyleSpec(mood="resolved", realism="realistic"),
        ),
    ]


def _shot(
    shot_id: str,
    role: str,
    description: str,
    search_query: str,
    generation_enabled: bool,
    generation_prompt: str,
    camera: CameraSpec,
    visual_style: VisualStyleSpec,
) -> ShotSpec:
    return ShotSpec(
        id=shot_id,
        role=role,  # type: ignore[arg-type]
        description=description,
        director_notes=f"优先选择主体清楚、动作完整、无明显水印和安全风险的素材。检索失败时保留缺失状态，不用错误素材冒充。",
        search_query=search_query,
        search_queries=[search_query, generation_prompt],
        negative_queries=["水印", "低清晰度", "危险", "侵权"],
        must_have=[search_query.split("，")[0]],
        must_not_have=["严重模糊", "危险场景", "明显水印"],
        target_duration=4.0,
        min_duration=1.5,
        max_duration=8.0,
        camera=camera,
        visual_style=visual_style,
        generation=VideoGenerationHints(
            enabled=generation_enabled,
            prompt=generation_prompt,
            negative_prompt="watermark, logo, low quality, blurry, distorted subject, unsafe scene",
        ),
    )


def _fit_durations(
    shots: list[ShotSpec],
    target_duration: float,
    profile: PlatformProfile,
) -> tuple[list[ShotSpec], list[str]]:
    if not shots:
        return [], []
    total = float(target_duration or len(shots) * 4)
    count = len(shots)
    min_total = profile.min_shot_seconds * count
    max_total = profile.max_shot_seconds * count
    relaxed = total < min_total or total > max_total

    if count == 1:
        durations = [total]
    else:
        hook = _clamp(total, profile.min_shot_seconds, profile.max_shot_seconds)
        hook = min(hook, profile.hook_seconds)
        hook = _clamp(hook, profile.min_shot_seconds, profile.max_shot_seconds)
        remaining = max(0.0, total - hook)
        weights = _pacing_weights(count - 1, profile.pacing_curve)
        weight_sum = sum(weights) or 1.0
        durations = [hook, *[remaining * weight / weight_sum for weight in weights]]

    if relaxed:
        durations = _redistribute_relaxed(durations, total)
    else:
        durations = [_clamp_duration(duration, profile) for duration in durations]
        durations = _redistribute_to_total(durations, total, profile, protected_indices={0})

    rounded = [round(duration, 3) for duration in durations]
    if rounded:
        rounded[-1] = round(total - sum(rounded[:-1]), 3)
    updated = [
        shot.model_copy(
            update={
                "target_duration": rounded[index],
                "min_duration": profile.min_shot_seconds,
                "max_duration": max(profile.max_shot_seconds, rounded[index]),
            }
        )
        for index, shot in enumerate(shots)
    ]
    return updated, (["platform_pacing_relaxed"] if relaxed else [])


def _pacing_weights(count: int, pacing_curve: str) -> list[float]:
    if count <= 0:
        return []
    if pacing_curve == "front_loaded":
        return [1.0 + (count - index - 1) * 0.25 for index in range(count)]
    if pacing_curve == "long_form":
        return [0.85 + index * 0.15 for index in range(count)]
    return [1.0 for _index in range(count)]


def _redistribute_to_total(
    durations: list[float],
    total: float,
    profile: PlatformProfile,
    *,
    protected_indices: set[int] | None = None,
) -> list[float]:
    adjusted = list(durations)
    protected = protected_indices or set()
    for _iteration in range(len(adjusted) * 4):
        diff = total - sum(adjusted)
        if abs(diff) < 0.0005:
            break
        if diff > 0:
            candidates = [
                index
                for index, duration in enumerate(adjusted)
                if index not in protected and duration < profile.max_shot_seconds - 0.0005
            ]
            if not candidates:
                candidates = [
                    index
                    for index, duration in enumerate(adjusted)
                    if duration < profile.max_shot_seconds - 0.0005
                ]
            if not candidates:
                break
            increment = diff / len(candidates)
            for index in candidates:
                adjusted[index] = min(profile.max_shot_seconds, adjusted[index] + increment)
        else:
            candidates = [
                index
                for index, duration in enumerate(adjusted)
                if index not in protected and duration > profile.min_shot_seconds + 0.0005
            ]
            if not candidates:
                candidates = [
                    index
                    for index, duration in enumerate(adjusted)
                    if duration > profile.min_shot_seconds + 0.0005
                ]
            if not candidates:
                break
            decrement = abs(diff) / len(candidates)
            for index in candidates:
                adjusted[index] = max(profile.min_shot_seconds, adjusted[index] - decrement)
    return adjusted


def _redistribute_relaxed(durations: list[float], total: float) -> list[float]:
    if not durations:
        return []
    current = sum(durations)
    if current <= 0:
        return [total / len(durations) for _duration in durations]
    scale = total / current
    return [duration * scale for duration in durations]


def _clamp_duration(duration: float, profile: PlatformProfile) -> float:
    return _clamp(duration, profile.min_shot_seconds, profile.max_shot_seconds)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _title(subject: str) -> str:
    return f"{subject}短视频分镜"


def _title_candidates(subject: str) -> list[str]:
    return [f"{subject}高光合集", f"今日份{subject}", f"{subject}短视频预览"]


def _logline(subject: str, duration: float) -> str:
    return f"用一组清晰、有节奏的镜头组成约 {int(duration)} 秒的{subject}短视频。"


def _style(subject: str, prompt: str) -> str:
    if subject in {"小猫", "小狗", "dog"}:
        return "cute_fast_montage" if any(term in prompt for term in ("可爱", "快节奏", "搞笑")) else "animal_montage"
    return "storyboard_remix"
