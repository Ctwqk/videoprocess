# VideoProcess Smart Trim 与 Storyboard Planner 实施规格

> 目标：在 `videoprocess` 中新增面向自然语言剪辑的 `smart_trim` 能力，并新增可扩展的 storyboard planner，使系统能够：
>
> 1. 从输入视频中剪辑出语言描述的片段。
> 2. 判断“素材本身就是目标合集”时是否返回整段视频。
> 3. 根据基础 prompt 扩写出分镜。
> 4. 为未来接入视频生成模型预留长分镜描述、镜头参数、生成 prompt、负向 prompt、参考素材等字段。

---

## 0. 总体原则

### 0.1 不要直接扩展现有 `trim` 为复杂节点

当前 `trim` 的职责应保持不变：

```text
input video + start/end/duration -> output video
```

`trim` 是确定性时间裁剪节点。不要把 prompt、视觉模型、ASR、分镜规划、素材检索等逻辑塞入现有 `trim`。

新增：

```text
smart_trim
```

用于：

```text
input video + natural language prompt -> matched clip / montage / full video
```

再新增：

```text
AutoFlow / Storyboard Planner
```

用于：

```text
base prompt -> structured storyboard -> pipeline definition
```

### 0.2 LLM 不直接生成任意 workflow

LLM 只允许输出结构化 intent / storyboard / search queries / generation prompts。

实际 pipeline 由后端 builder 根据模板确定性生成，并必须经过：

```python
validate_pipeline()
```

### 0.3 先做可工作的 MVP，再扩展视觉理解

第一版重点：

```text
source -> smart_trim -> export
```

第二版：

```text
prompt -> storyboard -> smart_trim per shot -> concat_many/montage_assembler -> export
```

第三版：

```text
prompt -> storyboard -> material library search -> concat_many/montage_assembler -> export
```

第四版：

```text
missing shot -> video generation fallback
```

---

## 1. 现有代码上下文

### 1.1 当前 `trim` 节点

现有节点定义：

```text
backend/app/node_registry/builtin/trim.py
```

当前参数：

```text
start_time
end_time
duration
```

当前 worker handler：

```text
backend/worker/handlers/trim.py
```

handler 基本逻辑：

```text
读取 input video
读取 start/end/duration
调用 ffmpeg -ss / -to / -t
输出单个视频 artifact
```

这说明 `trim` 适合保留为简单、可预测、可复用的基础裁剪节点。

### 1.2 新节点注册入口

新增节点定义后需要修改：

```text
backend/app/node_registry/builtin/__init__.py
```

新增 handler 后需要修改：

```text
backend/worker/handlers/__init__.py
```

### 1.3 当前 worker 执行模型

当前 worker handler 约定：

```python
async def execute(self, node_config, input_paths, output_path):
    ...
```

一个节点通常只生成一个 `output_path`，worker 成功后创建一个 artifact。

因此 MVP 不建议做真实多输出节点。对于多个分镜片段，建议：

```text
方案 A：每个分镜生成一个 smart_trim 节点
方案 B：smart_trim 自己输出一个 montage
方案 C：materialize search 在 plan 阶段产出多个 asset_id，再生成多个 source 节点
```

### 1.4 当前素材库能力

当前项目已有素材库雏形：

```text
backend/app/node_registry/builtin/material_library_ingest.py
backend/worker/handlers/material_library_ingest.py
backend/app/services/material_service.py
backend/app/api/materials.py
backend/app/schemas/material.py
```

已有能力包括：

```text
- 视频入库
- ASR
- 滑动窗口切片
- embedding gateway
- Qdrant 检索
- search preview
- materialize search
- refined clip asset 生成
```

但当前能力更偏 ASR / 字幕文本检索。对于“小猫”这种无对白视觉主题，应后续增强视觉索引。

---

## 2. 目标功能

### 2.1 Smart Trim

输入：

```text
视频
自然语言 prompt
可选 negative prompt
可选目标时长
可选返回策略
```

输出：

```text
匹配的视频片段
或多个匹配片段拼成的 montage
或整段原视频
```

典型例子：

```text
用户 prompt: 我要小猫的视频
输入视频 A: 小猫集锦
输出: 整段视频

用户 prompt: 我要小猫的视频
输入视频 B: 家庭 vlog，中间有猫
输出: 小猫出现的片段

用户 prompt: 小猫扑玩具
输入视频 C: 小猫集锦
输出: 只包含扑玩具动作的片段

用户 prompt: 黑色小猫睡觉
输入视频 D: 没有黑色小猫
输出: no_match / 或 fallback generation prompt
```

### 2.2 Storyboard Planner

输入：

```text
基础 prompt
目标时长
目标平台
风格
是否允许生成素材
是否只用输入视频 / 素材库
```

输出：

```text
结构化分镜 JSON
```

关键要求：

```text
允许生成更长、更细的分镜描述
为未来接视频生成模型预留字段
不只生成搜索词，也要生成 video generation prompt
```

---

## 3. 新增节点：`smart_trim`

### 3.1 新增文件

```text
backend/app/node_registry/builtin/smart_trim.py
backend/worker/handlers/smart_trim.py
```

修改：

```text
backend/app/node_registry/builtin/__init__.py
backend/worker/handlers/__init__.py
backend/pyproject.toml
```

### 3.2 Node definition

建议实现：

```python
from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="smart_trim",
    display_name="Smart Trim",
    category="ai_transform",
    description="Find and trim video segments by natural language prompt",
    icon="sparkles-scissors",
    inputs=[
        PortDefinition(
            name="input",
            port_type=PortType.VIDEO,
            description="Input video",
        ),
    ],
    outputs=[
        PortDefinition(
            name="output",
            port_type=PortType.VIDEO,
            description="Matched video clip, montage, or original video",
        ),
    ],
    params=[
        ParamDefinition(
            name="prompt",
            param_type="string",
            required=True,
            description="Natural language description of the desired segment",
        ),
        ParamDefinition(
            name="negative_prompt",
            param_type="string",
            default="",
            required=False,
            description="Things that should not appear",
        ),
        ParamDefinition(
            name="mode",
            param_type="select",
            default="auto",
            required=True,
            options=[
                "auto",
                "best_clip",
                "all_matches_montage",
                "full_if_match",
                "no_full_video",
            ],
            description="Output selection policy",
        ),
        ParamDefinition(
            name="target_duration",
            param_type="number",
            default=0,
            required=True,
            min_value=0,
            max_value=600,
            description="Target output duration in seconds. 0 means unconstrained.",
        ),
        ParamDefinition(
            name="min_clip_duration",
            param_type="number",
            default=1.5,
            required=True,
            min_value=0.3,
            max_value=30,
            description="Minimum selected clip duration",
        ),
        ParamDefinition(
            name="max_clip_duration",
            param_type="number",
            default=8,
            required=True,
            min_value=1,
            max_value=120,
            description="Maximum selected clip duration",
        ),
        ParamDefinition(
            name="max_clips",
            param_type="number",
            default=8,
            required=True,
            min_value=1,
            max_value=30,
            description="Maximum number of clips in montage mode",
        ),
        ParamDefinition(
            name="sample_fps",
            param_type="number",
            default=1,
            required=True,
            min_value=0.1,
            max_value=4,
            description="Frame sampling rate for visual scoring",
        ),
        ParamDefinition(
            name="match_threshold",
            param_type="number",
            default=0.35,
            required=True,
            min_value=0,
            max_value=1,
            description="Minimum score for a window to be considered matched",
        ),
        ParamDefinition(
            name="return_full_threshold",
            param_type="number",
            default=0.65,
            required=True,
            min_value=0,
            max_value=1,
            description="If matched coverage exceeds this ratio, full video may be returned",
        ),
        ParamDefinition(
            name="padding_before",
            param_type="number",
            default=0.5,
            required=True,
            min_value=0,
            max_value=10,
            description="Seconds to expand before each matched segment",
        ),
        ParamDefinition(
            name="padding_after",
            param_type="number",
            default=0.5,
            required=True,
            min_value=0,
            max_value=10,
            description="Seconds to expand after each matched segment",
        ),
        ParamDefinition(
            name="merge_gap",
            param_type="number",
            default=1.0,
            required=True,
            min_value=0,
            max_value=10,
            description="Merge matched windows when the gap is below this value",
        ),
        ParamDefinition(
            name="use_visual",
            param_type="boolean",
            default=True,
            required=False,
            description="Enable visual semantic matching",
        ),
        ParamDefinition(
            name="use_asr",
            param_type="boolean",
            default=True,
            required=False,
            description="Enable speech/subtitle matching",
        ),
        ParamDefinition(
            name="use_vlm_verify",
            param_type="boolean",
            default=False,
            required=False,
            description="Optionally verify top windows with a VLM service",
        ),
        ParamDefinition(
            name="language",
            param_type="string",
            default="zh",
            required=False,
            description="Language hint for ASR and query expansion",
        ),
        ParamDefinition(
            name="output_format",
            param_type="select",
            default="mp4",
            required=True,
            options=["mp4", "mkv", "webm"],
            description="Output format",
        ),
    ],
    worker_type="vision",
)
```

### 3.3 Handler 高层流程

`backend/worker/handlers/smart_trim.py`

```text
1. 读取 input_paths["input"]
2. ffprobe 读取 duration / stream info
3. 根据 sample_fps 抽帧
4. 可选 ASR
5. 对 prompt 做 query expansion
6. 视觉打分
7. 字幕打分
8. 合成窗口分数
9. 选择片段
10. 决策：full video / best clip / montage / no_match
11. FFmpeg 输出
12. 返回 metadata
```

### 3.4 Handler 伪代码

```python
class SmartTrimHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video_path = input_paths["input"]

        config = SmartTrimConfig.from_node_config(node_config)
        media_info = await self.run_ffprobe(video_path)
        duration = parse_duration(media_info)

        frames = await self.extract_frames(
            video_path=video_path,
            duration=duration,
            sample_fps=config.sample_fps,
        )

        visual_scores = []
        if config.use_visual:
            visual_scores = await self.score_frames_with_embedding(
                frames=frames,
                prompt=config.prompt,
                negative_prompt=config.negative_prompt,
            )

        subtitle_segments = []
        subtitle_scores = []
        if config.use_asr:
            subtitle_segments = await self.transcribe_or_empty(
                video_path=video_path,
                language=config.language,
            )
            subtitle_scores = await self.score_subtitles(
                subtitle_segments=subtitle_segments,
                prompt=config.prompt,
                negative_prompt=config.negative_prompt,
            )

        windows = self.build_scored_windows(
            duration=duration,
            visual_scores=visual_scores,
            subtitle_scores=subtitle_scores,
            config=config,
        )

        selected = self.select_segments(
            windows=windows,
            duration=duration,
            config=config,
        )

        if selected.decision == "no_match":
            await self.write_empty_placeholder_or_raise(
                output_path=output_path,
                config=config,
            )
        elif selected.decision == "return_full_video":
            await self.copy_or_reencode_full_video(video_path, output_path, config)
        elif selected.decision == "best_clip":
            await self.cut_single_clip(video_path, output_path, selected.segments[0], config)
        else:
            await self.cut_and_concat(video_path, output_path, selected.segments, config)

        return {
            "smart_trim_prompt": config.prompt,
            "smart_trim_negative_prompt": config.negative_prompt,
            "decision": selected.decision,
            "coverage_ratio": selected.coverage_ratio,
            "matched_windows": [segment.model_dump() for segment in selected.segments],
            "query_expansion": selected.query_expansion,
            "video_duration": duration,
        }
```

### 3.5 输出 decision

支持：

```text
return_full_video
best_clip
all_matches_montage
no_match
```

### 3.6 覆盖率策略

规则：

```python
coverage_ratio = matched_duration / video_duration

can_return_full = mode in {"auto", "full_if_match"}
has_no_target_duration = target_duration <= 0

if (
    can_return_full
    and has_no_target_duration
    and coverage_ratio >= return_full_threshold
):
    decision = "return_full_video"
```

注意：

```text
如果 target_duration > 0，即使 coverage 很高，也应该输出指定时长 montage，而不是整片。
```

### 3.7 小猫例子

输入：

```json
{
  "prompt": "我要小猫的视频",
  "mode": "auto",
  "target_duration": 0,
  "return_full_threshold": 0.65
}
```

如果视频 90% 窗口都包含小猫：

```json
{
  "decision": "return_full_video",
  "coverage_ratio": 0.9,
  "segments": [
    {"start": 0.0, "end": 180.0}
  ]
}
```

如果只有 20% 视频包含小猫：

```json
{
  "decision": "all_matches_montage",
  "coverage_ratio": 0.2,
  "segments": [
    {"start": 15.2, "end": 24.8, "score": 0.82},
    {"start": 71.4, "end": 83.0, "score": 0.77}
  ]
}
```

如果 prompt 更具体：

```text
小猫跳进纸箱
```

即使视频整体都是猫，也应该只返回“跳进纸箱”的片段。

---

## 4. 新增节点：`concat_many`

### 4.1 目的

用于拼接多个 `smart_trim` 或素材库返回的片段。

当前不建议做 `trim_many` 多输出，因为现有 worker 模型更适合一个 node 输出一个 artifact。

### 4.2 新增文件

```text
backend/app/node_registry/builtin/concat_many.py
backend/worker/handlers/concat_many.py
```

修改：

```text
backend/app/node_registry/builtin/__init__.py
backend/worker/handlers/__init__.py
```

### 4.3 Node definition

端口先固定最多 12 个输入：

```text
video_1
video_2
...
video_12
```

参数：

```text
input_count
target_duration
normalize_resolution
width
height
fps
transition
transition_duration
output_format
```

建议：

```python
DEFINITION = NodeTypeDefinition(
    type_name="concat_many",
    display_name="Concat Many",
    category="combine",
    description="Concatenate multiple videos sequentially",
    icon="git-merge",
    inputs=[
        PortDefinition(name=f"video_{i}", port_type=PortType.VIDEO, required=(i <= 2))
        for i in range(1, 13)
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO),
    ],
    params=[
        ParamDefinition(name="input_count", param_type="number", default=2, required=True, min_value=1, max_value=12),
        ParamDefinition(name="target_duration", param_type="number", default=0, required=True, min_value=0, max_value=600),
        ParamDefinition(name="normalize_resolution", param_type="boolean", default=True),
        ParamDefinition(name="width", param_type="number", default=1080, min_value=160, max_value=3840),
        ParamDefinition(name="height", param_type="number", default=1920, min_value=160, max_value=3840),
        ParamDefinition(name="fps", param_type="number", default=30, min_value=1, max_value=120),
        ParamDefinition(name="transition", param_type="select", default="none", options=["none", "fade"]),
        ParamDefinition(name="transition_duration", param_type="number", default=0.3, min_value=0, max_value=3),
        ParamDefinition(name="output_format", param_type="select", default="mp4", options=["mp4", "mkv", "webm"]),
    ],
)
```

### 4.4 Handler MVP

MVP 不做复杂转场，只做：

```text
- 输入统一转码到临时 mp4
- 分辨率统一
- fps 统一
- 音频缺失时补静音
- concat demuxer 拼接
- 如果 target_duration > 0，最后整体 trim 到 target_duration
```

---

## 5. Storyboard Planner

### 5.1 新增文件

```text
backend/app/schemas/autoflow.py
backend/app/autoflow/__init__.py
backend/app/autoflow/intent_parser.py
backend/app/autoflow/storyboard_generator.py
backend/app/autoflow/pipeline_builder.py
backend/app/autoflow/service.py
backend/app/api/autoflow.py
```

修改：

```text
backend/app/main.py
```

### 5.2 核心要求

Storyboard Planner 必须支持长分镜描述。

原因：

```text
当前用于检索时，只需要短 search query。
未来接视频生成模型时，需要更完整的画面描述、运动描述、镜头语言、风格、负向提示、首尾帧约束。
```

因此一个 shot 不能只有：

```json
{
  "query": "小猫玩玩具"
}
```

而应该有：

```json
{
  "description": "一个温暖自然光的室内场景，小猫在木地板上追逐一个小球，镜头贴近地面，能看到小猫扑向玩具时的动作和轻微失衡。",
  "search_query": "小猫追玩具，扑玩具，可爱，室内",
  "generation_prompt": "A cute kitten chasing a small ball on a wooden floor in a cozy room, warm natural light, low-angle camera close to the ground, playful movement, soft handheld shot, realistic video.",
  "negative_prompt": "cartoon, anime, dog, tiger, text, logo, watermark, distorted body, extra limbs, blurry, low quality"
}
```

### 5.3 Schema 建议

```python
from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class VideoGenerationHints(BaseModel):
    enabled: bool = False

    # 给视频生成模型用的完整 prompt，允许长文本。
    prompt: str = ""

    # 给视频生成模型用的负向 prompt。
    negative_prompt: str = ""

    # 用于 image-to-video / video-to-video 的参考图、参考片段、首帧、尾帧。
    reference_asset_ids: list[str] = Field(default_factory=list)
    reference_image_asset_id: str | None = None
    reference_video_asset_id: str | None = None
    first_frame_asset_id: str | None = None
    last_frame_asset_id: str | None = None

    # 生成模型参数。不要在 MVP 中强依赖具体模型。
    model_hint: str = "auto"
    resolution: str = "auto"
    fps: int | None = None
    seed: int | None = None
    guidance_scale: float | None = None
    motion_strength: float | None = None

    # 未来扩展。
    extra: dict[str, Any] = Field(default_factory=dict)


class CameraSpec(BaseModel):
    shot_size: Literal[
        "extreme_close_up",
        "close_up",
        "medium",
        "wide",
        "establishing",
        "auto",
    ] = "auto"
    angle: Literal[
        "eye_level",
        "low_angle",
        "high_angle",
        "top_down",
        "dutch_angle",
        "auto",
    ] = "auto"
    movement: Literal[
        "static",
        "handheld",
        "push_in",
        "pull_out",
        "pan",
        "tilt",
        "tracking",
        "orbit",
        "auto",
    ] = "auto"
    lens: str = ""
    composition: str = ""


class VisualStyleSpec(BaseModel):
    mood: str = ""
    lighting: str = ""
    color_palette: str = ""
    realism: Literal["realistic", "cinematic", "documentary", "anime", "illustration", "auto"] = "auto"
    texture: str = ""
    platform_style: str = ""


class ShotSpec(BaseModel):
    id: str

    role: Literal[
        "hook",
        "setup",
        "action",
        "reaction",
        "detail",
        "transition",
        "ending",
        "b_roll",
    ] = "action"

    # 给人和 planner 看的完整分镜描述，允许长文本。
    description: str = ""

    # 更长的导演意图说明，未来给视频生成模型或人工审核使用。
    director_notes: str = ""

    # 检索用短 query。
    search_query: str

    # 检索用扩展 query，可以中英混合。
    search_queries: list[str] = Field(default_factory=list)

    # 检索或生成时的负向约束。
    negative_queries: list[str] = Field(default_factory=list)

    # 语义约束。
    must_have: list[str] = Field(default_factory=list)
    nice_to_have: list[str] = Field(default_factory=list)
    must_not_have: list[str] = Field(default_factory=list)

    # 时间约束。
    target_duration: float = 4.0
    min_duration: float = 1.5
    max_duration: float = 8.0

    # 镜头和风格。
    camera: CameraSpec = Field(default_factory=CameraSpec)
    visual_style: VisualStyleSpec = Field(default_factory=VisualStyleSpec)

    # 音频、字幕、文案提示。
    narration: str = ""
    on_screen_text: str = ""
    sound_design: str = ""

    # 未来视频生成模型字段。
    generation: VideoGenerationHints = Field(default_factory=VideoGenerationHints)

    # 匹配结果。
    matched_asset_id: str | None = None
    matched_source_asset_id: str | None = None
    matched_start_sec: float | None = None
    matched_end_sec: float | None = None
    match_score: float | None = None
    match_status: Literal["pending", "matched", "missing", "generated", "skipped"] = "pending"

    extra: dict[str, Any] = Field(default_factory=dict)


class StoryboardPlan(BaseModel):
    subject: str
    title: str = ""
    logline: str = ""

    style: str = "auto"
    target_platforms: list[str] = Field(default_factory=list)
    aspect_ratio: Literal["9:16", "16:9", "1:1", "auto"] = "auto"
    total_duration: float = 30

    source_strategy: Literal[
        "input_video",
        "material_library",
        "external_research",
        "generate_missing",
        "hybrid",
    ] = "input_video"

    allow_video_generation: bool = False

    shots: list[ShotSpec]

    title_candidates: list[str] = Field(default_factory=list)
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)

    warnings: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
```

### 5.4 Storyboard 输出示例

用户输入：

```text
我要一个 30 秒小猫视频，竖屏，可爱快节奏。素材来自我上传的视频。如果没有合适片段，先标记缺失，不要生成。
```

输出：

```json
{
  "subject": "小猫",
  "title": "今日份小猫快乐源泉",
  "logline": "用一组可爱、轻快、有反应点的小猫镜头，组成 30 秒治愈短视频。",
  "style": "cute_fast_montage",
  "target_platforms": ["short_video"],
  "aspect_ratio": "9:16",
  "total_duration": 30,
  "source_strategy": "input_video",
  "allow_video_generation": false,
  "shots": [
    {
      "id": "shot_01",
      "role": "hook",
      "description": "开场用一个非常近的小猫脸部特写。小猫最好正看镜头，眼睛清楚，表情有好奇、呆萌或突然靠近镜头的感觉。画面要立刻传达可爱和吸引注意力。",
      "director_notes": "这个镜头承担前 1 秒吸引用户停留的任务。优先选择脸部清楚、动作突出的片段。如果有小猫突然靠近镜头或眨眼，优先级最高。",
      "search_query": "小猫近景看镜头，可爱，呆萌，脸部特写",
      "search_queries": [
        "小猫看镜头 特写 可爱",
        "kitten looking at camera close up cute",
        "cute kitten face close-up"
      ],
      "negative_queries": ["狗", "老虎", "卡通猫", "模糊", "水印"],
      "must_have": ["小猫清晰可见", "脸部或眼睛清楚"],
      "nice_to_have": ["看镜头", "靠近镜头", "眨眼"],
      "must_not_have": ["只有人", "没有猫", "严重模糊"],
      "target_duration": 3,
      "min_duration": 1.5,
      "max_duration": 4,
      "camera": {
        "shot_size": "close_up",
        "angle": "eye_level",
        "movement": "static",
        "lens": "natural phone camera",
        "composition": "kitten face centered, eyes visible"
      },
      "visual_style": {
        "mood": "cute and warm",
        "lighting": "soft natural light",
        "color_palette": "warm neutral",
        "realism": "realistic",
        "platform_style": "short video hook"
      },
      "on_screen_text": "今日份小猫快乐",
      "sound_design": "轻快可爱的开场音效",
      "generation": {
        "enabled": false,
        "prompt": "A cute kitten looking directly into the camera in a close-up shot, warm natural light, curious expression, realistic short video style.",
        "negative_prompt": "dog, tiger, cartoon, anime, watermark, text, blurry, distorted face, extra limbs"
      }
    },
    {
      "id": "shot_02",
      "role": "action",
      "description": "小猫在地板上追逐玩具、扑向玩具或突然跳起来。动作要明显，节奏要快，适合接在开场后制造活力。",
      "director_notes": "优先选择有完整动作起承转合的片段：发现玩具、扑过去、落地或反应。若源视频中没有玩具，可退化为小猫奔跑或跳跃。",
      "search_query": "小猫追玩具，扑玩具，跳跃，玩耍",
      "search_queries": [
        "小猫追玩具 扑玩具 玩耍",
        "kitten chasing toy playful",
        "small cat jumping and playing"
      ],
      "negative_queries": ["成年老虎", "狗", "只有人", "静止不动"],
      "must_have": ["小猫", "明显动作"],
      "nice_to_have": ["玩具", "扑跳", "快速移动"],
      "target_duration": 5,
      "min_duration": 2,
      "max_duration": 6,
      "camera": {
        "shot_size": "medium",
        "angle": "low_angle",
        "movement": "handheld",
        "composition": "kitten body and movement visible"
      },
      "visual_style": {
        "mood": "playful",
        "lighting": "natural indoor light",
        "realism": "realistic"
      },
      "generation": {
        "enabled": false,
        "prompt": "A playful kitten chasing a small toy across a wooden floor, low angle camera near the ground, quick energetic movement, warm indoor daylight, realistic video.",
        "negative_prompt": "dog, tiger, cartoon, watermark, low quality, motion artifacts, distorted paws"
      }
    },
    {
      "id": "shot_03",
      "role": "reaction",
      "description": "小猫出现搞笑反应，例如扑空、摔倒、被声音吸引后突然回头、愣住或做出疑惑表情。这个镜头用于制造短视频的记忆点。",
      "director_notes": "如果找不到明确搞笑反应，可以选择小猫突然停住、回头、抬头或歪头的片段。避免过长铺垫。",
      "search_query": "小猫搞笑反应，扑空，摔倒，回头，歪头",
      "search_queries": [
        "小猫 搞笑反应 歪头 回头",
        "kitten funny reaction",
        "kitten misses toy funny"
      ],
      "negative_queries": ["受伤", "危险", "虐待动物", "严重惊吓"],
      "must_have": ["小猫清楚可见", "反应动作"],
      "must_not_have": ["危险场景", "动物受伤"],
      "target_duration": 5,
      "min_duration": 2,
      "max_duration": 6,
      "camera": {
        "shot_size": "medium",
        "angle": "eye_level",
        "movement": "static"
      },
      "visual_style": {
        "mood": "funny but safe",
        "realism": "realistic"
      },
      "generation": {
        "enabled": false,
        "prompt": "A cute kitten suddenly stops and tilts its head with a funny confused expression, cozy room, realistic short video, safe and playful.",
        "negative_prompt": "injury, animal abuse, danger, dog, tiger, cartoon, watermark, blurry"
      }
    }
  ],
  "title_candidates": [
    "今日份小猫快乐源泉",
    "这些小猫的反应也太可爱了",
    "30 秒小猫治愈合集"
  ],
  "description": "一组可爱小猫的短视频合集，节奏轻快，适合竖屏平台。",
  "tags": ["小猫", "猫咪", "萌宠", "治愈", "搞笑"],
  "hashtags": ["#小猫", "#猫咪", "#萌宠", "#治愈"]
}
```

---

## 6. AutoFlow API

### 6.1 新增 router

```text
backend/app/api/autoflow.py
```

注册到：

```text
backend/app/main.py
```

### 6.2 API 端点

```text
POST /api/v1/autoflow/storyboard
POST /api/v1/autoflow/plan
POST /api/v1/autoflow/execute
GET  /api/v1/autoflow/plans/{plan_id}
GET  /api/v1/autoflow/capabilities
```

### 6.3 Request schema

```python
class AutoFlowStoryboardRequest(BaseModel):
    prompt: str
    input_asset_id: str | None = None
    material_library_ids: list[str] = Field(default_factory=list)

    target_duration: float = 30
    aspect_ratio: Literal["9:16", "16:9", "1:1", "auto"] = "auto"
    target_platforms: list[str] = Field(default_factory=list)

    source_strategy: Literal[
        "input_video",
        "material_library",
        "external_research",
        "generate_missing",
        "hybrid",
    ] = "input_video"

    allow_video_generation: bool = False
    max_shots: int = 8
    min_shots: int = 3

    style: str = "auto"

    # LLM provider config. MVP 可以不用，先 rule-based fallback。
    provider_config_id: str | None = None
    model: str | None = None

    constraints: dict[str, Any] = Field(default_factory=dict)
```

### 6.4 Response schema

```python
class AutoFlowStoryboardResponse(BaseModel):
    storyboard: StoryboardPlan
    raw_model_output: str | None = None
    warnings: list[str] = Field(default_factory=list)
```

### 6.5 Plan schema

```python
class AutoFlowPlanRequest(BaseModel):
    prompt: str
    input_asset_id: str | None = None
    material_library_ids: list[str] = Field(default_factory=list)
    target_duration: float = 30
    aspect_ratio: Literal["9:16", "16:9", "1:1", "auto"] = "auto"
    source_strategy: Literal["input_video", "material_library", "hybrid"] = "input_video"
    allow_video_generation: bool = False
    execute: bool = False
    constraints: dict[str, Any] = Field(default_factory=dict)


class AutoFlowPlanResponse(BaseModel):
    plan_id: str
    storyboard: StoryboardPlan
    pipeline_definition: dict[str, Any]
    validation: dict[str, Any]
    warnings: list[str] = Field(default_factory=list)
```

---

## 7. Pipeline Builder

### 7.1 输入视频策略

当 request 有 `input_asset_id` 且 `source_strategy=input_video`：

```text
source(input_asset_id)
  -> smart_trim(shot_01)
source(input_asset_id)
  -> smart_trim(shot_02)
source(input_asset_id)
  -> smart_trim(shot_03)
...
smart_trim outputs
  -> concat_many
  -> export
```

注意：

```text
每个 smart_trim 都连接同一个 source 输出。
```

Pipeline 示例：

```text
src_video
  ├── smart_trim_01
  ├── smart_trim_02
  ├── smart_trim_03
  └── smart_trim_04

smart_trim_01.output -> concat_many.video_1
smart_trim_02.output -> concat_many.video_2
smart_trim_03.output -> concat_many.video_3
smart_trim_04.output -> concat_many.video_4

concat_many.output -> export.input
```

### 7.2 素材库策略

当 `source_strategy=material_library`：

```text
plan 阶段：
  对每个 shot 调用 materialize_material_search()
  得到 refined clip asset_id

runtime pipeline：
  source(clip_asset_id_1)
  source(clip_asset_id_2)
  source(clip_asset_id_3)
  -> concat_many
  -> export
```

不要在 runtime pipeline 中依赖 `material_search` planner 节点执行真实检索。

### 7.3 混合策略

当 `source_strategy=hybrid`：

```text
优先 input_video smart_trim
如果 shot match_status=missing
  再尝试 material_library search
如果仍 missing 且 allow_video_generation=true
  创建 generation placeholder
否则标记 missing
```

### 7.4 未来视频生成策略

暂不实现真实生成模型，但 schema 和 plan 应支持：

```text
shot.match_status = "missing"
shot.generation.enabled = true
shot.generation.prompt = ...
shot.generation.negative_prompt = ...
```

未来新增：

```text
video_generate
image_to_video
video_to_video
```

即可接入。

---

## 8. LLM Prompt 规范

### 8.1 System prompt

```text
你是 VideoProcess 的短视频分镜规划器。
你必须只输出 JSON，不要输出 Markdown，不要输出解释。
你输出的 JSON 必须符合 StoryboardPlan schema。

要求：
1. 每个 shot 必须包含长 description，供人工理解和未来视频生成模型使用。
2. 每个 shot 必须包含短 search_query，供素材检索使用。
3. 每个 shot 必须包含 generation.prompt 和 generation.negative_prompt。
4. 如果用户要求只使用输入视频，generation.enabled 必须为 false。
5. 如果用户允许生成缺失镜头，generation.enabled 可以为 true。
6. 不要生成危险、违法、侵权、虐待动物或明显不可执行的镜头。
7. 分镜总时长应接近 target_duration。
8. 不要直接输出 workflow nodes 或 edges。
```

### 8.2 User prompt template

```text
用户需求：
{prompt}

目标时长：
{target_duration} 秒

画幅：
{aspect_ratio}

目标平台：
{target_platforms}

素材策略：
{source_strategy}

是否允许视频生成：
{allow_video_generation}

最多分镜数：
{max_shots}

最少分镜数：
{min_shots}

请输出 StoryboardPlan JSON。
```

### 8.3 Rule-based fallback

LLM 不可用时，对小猫类 prompt 至少能生成基础 storyboard：

```python
if any(term in prompt for term in ["小猫", "猫", "cat", "kitten"]):
    subject = "小猫"
    shots = [
        hook close-up,
        playing action,
        funny reaction,
        sleeping/yawning detail,
        ending close-up,
    ]
```

---

## 9. Smart Trim 视觉与 ASR 打分实现建议

### 9.1 MVP：不强依赖大型 VLM

MVP 推荐：

```text
- 抽帧
- embedding gateway 或本地 CLIP/SigLIP 服务
- ASR 文本相似度
- 简单规则融合
```

不要第一版就让 VLM 看完整视频。

### 9.2 推荐外部服务接口

新增可选配置：

```python
# backend/app/config.py
vision_embedding_url: str = ""
smart_trim_vlm_url: str = ""
smart_trim_default_worker_type: str = "vision"
```

视觉 embedding 服务接口：

```http
POST /embed-image-text
```

请求：

```json
{
  "texts": ["小猫玩玩具", "kitten playing with toy"],
  "images_base64": ["..."]
}
```

响应：

```json
{
  "similarities": [
    [0.72, 0.68],
    [0.31, 0.29]
  ]
}
```

### 9.3 如果没有视觉服务

Fallback：

```text
- 只用 ASR / subtitle
- 如果 ASR 也没有，则按低置信度 no_match
```

但对于“小猫”这类视觉主题，必须在 warning 中说明：

```json
{
  "warning": "visual scoring unavailable; result may be poor for visual-only queries"
}
```

### 9.4 窗口构建

抽帧结果：

```python
FrameScore(
    timestamp=12.0,
    visual_score=0.78,
    subtitle_score=0.0,
    final_score=0.55,
)
```

窗口聚合：

```text
以连续高分 frame 聚合成候选窗口
高分 frame 间隔 <= merge_gap 时合并
每个窗口左右加 padding
窗口时长限制在 min_clip_duration / max_clip_duration 内
```

### 9.5 分数融合

```python
final_score = (
    0.55 * visual_score
    + 0.30 * subtitle_score
    + 0.10 * query_keyword_score
    + 0.05 * duration_fit
)
```

如果 `use_visual=false`：

```python
final_score = (
    0.75 * subtitle_score
    + 0.20 * query_keyword_score
    + 0.05 * duration_fit
)
```

如果 `use_asr=false`：

```python
final_score = (
    0.85 * visual_score
    + 0.10 * query_keyword_score
    + 0.05 * duration_fit
)
```

---

## 10. FFmpeg 输出策略

### 10.1 整段返回

如果返回整片：

```text
优先 copy，必要时转码
```

```bash
ffmpeg -y -i input.mp4 -map 0:v:0 -map 0:a? -c copy output.mp4
```

如果容器或 codec 不兼容，则回退：

```bash
ffmpeg -y -i input.mp4 -map 0:v:0 -map 0:a? -c:v libx264 -crf 23 -preset fast -c:a aac output.mp4
```

### 10.2 单片段

```bash
ffmpeg -y -ss {start} -i input.mp4 -t {duration} \
  -map 0:v:0 -map 0:a? \
  -c:v libx264 -crf 23 -preset fast -c:a aac output.mp4
```

### 10.3 多片段 montage

```text
1. 对每个 segment 生成临时 clip
2. 写 concat list
3. concat demuxer 拼接
4. 清理临时文件
```

---

## 11. 硬件部署建议

### 11.1 Mac mini 1

用途：

```text
FastAPI backend
PostgreSQL
Redis
MinIO / local shared storage
Qdrant
frontend
```

### 11.2 Mac mini 2

用途：

```text
ffmpeg worker
export
transcode
concat
普通 trim
watermark
subtitle
```

启动示例：

```bash
cd backend
WORKER_TYPE=ffmpeg \
VIDEO_USE_VIDEOTOOLBOX=true \
python -m worker.main
```

### 11.3 3070Ti 8GB 笔记本

用途：

```text
vision worker
smart_trim
ASR
visual embedding
VLM top-k verify
NVENC 编码
```

启动示例：

```bash
cd backend
WORKER_TYPE=vision \
VIDEO_USE_GPU=true \
VIDEO_WHISPER_DEVICE=cuda \
VIDEO_WHISPER_COMPUTE_TYPE=float16 \
python -m worker.main
```

### 11.4 LLM 额度使用

```text
DeepSeek / Kimi:
  intent parser
  storyboard JSON
  title / description / tags

MiniMax:
  可选 TTS
  可选多模态复核

GPT Pro:
  开发、调试、prompt 设计、代码审查
  不建议作为无人值守后端 batch runtime 的唯一依赖

本地 3070Ti:
  smart_trim 主力
```

---

## 12. 实施阶段

### Phase 1：Smart Trim MVP

目标：

```text
source -> smart_trim -> export
```

任务：

```text
1. 新增 smart_trim node definition
2. 新增 SmartTrimHandler
3. 注册 node 和 handler
4. 实现 ffprobe
5. 实现抽帧
6. 实现 ASR fallback
7. 实现视觉服务接口 fallback
8. 实现窗口选择
9. 实现 full video / best clip / montage 输出
10. 写测试
```

验收：

```text
- /api/v1/node-types 能看到 smart_trim
- handler map 中有 smart_trim
- 无音频视频可运行
- 有音频视频可运行
- mode=best_clip 输出单段
- mode=all_matches_montage 输出多段拼接
- mode=full_if_match 在 coverage 高时输出整片
- target_duration > 0 时不返回整片
- handler 返回 matched_windows metadata
```

### Phase 2：Concat Many MVP

目标：

```text
多个 source/smart_trim 输出 -> concat_many -> export
```

任务：

```text
1. 新增 concat_many node definition
2. 新增 ConcatManyHandler
3. 注册 node 和 handler
4. 支持 1-12 个输入
5. 支持不同分辨率输入
6. 支持无音频输入
7. 支持 target_duration
```

验收：

```text
- 2 个输入可拼接
- 5 个输入可拼接
- 无音频输入不失败
- 不同分辨率输入能统一输出
- 输出 mp4 可播放
```

### Phase 3：Storyboard Planner

目标：

```text
prompt -> storyboard JSON
```

任务：

```text
1. 新增 autoflow schemas
2. 新增 storyboard_generator
3. 接 LLM provider，可暂时 mock
4. 实现 rule-based fallback
5. 确保每个 shot 有长 description
6. 确保每个 shot 有 generation prompt
7. Pydantic 校验
```

验收：

```text
- 输入“小猫视频”可生成 3-8 个 shot
- 每个 shot.description 长度 >= 30 个中文字符或 >= 50 个英文字符
- 每个 shot 有 search_query
- 每个 shot 有 generation.prompt
- allow_video_generation=false 时 generation.enabled=false
- LLM 输出非法 JSON 时 fallback 不报错
```

### Phase 4：AutoFlow Plan Builder

目标：

```text
prompt + input_asset_id -> pipeline_definition
```

任务：

```text
1. 新增 AutoFlowService
2. 新增 PipelineBuilder
3. input_video 策略生成 source + smart_trim per shot + concat_many + export
4. material_library 策略先 materialize，再生成 source + concat_many + export
5. validate_pipeline
6. 返回 plan
```

验收：

```text
- POST /api/v1/autoflow/plan 返回有效 pipeline_definition
- validate_pipeline().valid == true
- input_video 策略可生成多个 smart_trim 节点
- material_library 策略可生成多个 source 节点
```

### Phase 5：素材库视觉增强

目标：

```text
material_search 能找到无字幕视觉主题，例如小猫、狗、车、海边。
```

任务：

```text
1. material_library_ingest 增加抽帧
2. 增加 visual caption / tags
3. 增加视觉 embedding
4. Qdrant 增加 visual vector 或另建 collection
5. material_search 结合 subtitle + visual score
```

验收：

```text
- 无对白小猫视频入库后，搜索“小猫”能返回相关片段
- 搜索“狗”不会返回小猫片段，或分数明显更低
- 搜索“小猫玩玩具”优先返回动作相关片段
```

### Phase 6：Video Generation Placeholder

目标：

```text
分镜缺失时，不立刻失败，而是返回可生成的 missing shot。
```

任务：

```text
1. StoryboardPlan 中保留 generation fields
2. AutoFlowPlanResponse 标记 missing shots
3. 如果 allow_video_generation=true，则生成 video_generate placeholder node
4. 如果当前没有 video_generate 节点，则只在 plan warnings 中提示
```

验收：

```text
- 找不到片段时 shot.match_status=missing
- allow_video_generation=true 时 generation.enabled=true
- plan 中包含 generation prompt
- 不会静默用错误素材冒充匹配成功
```

---

## 13. 需要新增的测试

### 13.1 Node registry 测试

```text
backend/tests/test_node_registry_smart_trim.py
backend/tests/test_node_registry_concat_many.py
```

检查：

```text
smart_trim 已注册
concat_many 已注册
端口类型正确
必填参数正确
worker_type 正确
```

### 13.2 Handler 单元测试

```text
backend/tests/worker/test_smart_trim_handler.py
backend/tests/worker/test_concat_many_handler.py
```

检查：

```text
可处理短视频
可处理无音频视频
可生成 output_path
metadata 包含 decision / matched_windows
```

### 13.3 AutoFlow 测试

```text
backend/tests/autoflow/test_storyboard_generator.py
backend/tests/autoflow/test_pipeline_builder.py
backend/tests/autoflow/test_autoflow_plan_api.py
```

检查：

```text
小猫 prompt 可生成 storyboard
每个 shot 有长 description 和 generation.prompt
builder 输出 pipeline 可 validate
```

### 13.4 集成样例

准备 fixtures：

```text
tests/fixtures/videos/cat_compilation.mp4
tests/fixtures/videos/mixed_vlog_with_cat.mp4
tests/fixtures/videos/no_cat.mp4
```

如果仓库不放真实视频，可用 ffmpeg 生成 synthetic fixture，或跳过视觉真实断言，只测试结构和 handler fallback。

---

## 14. 前端最小改动

### 14.1 NodePalette

如果 NodeTypeRegistry 正常返回，前端应该自动显示新节点。确认 `category="ai_transform"` 是否需要 UI 映射。

### 14.2 AutoFlow 页面

后续新增：

```text
frontend/src/pages/AutoFlowPage.tsx
frontend/src/components/autoflow/StoryboardPreview.tsx
frontend/src/components/autoflow/ShotCard.tsx
frontend/src/components/autoflow/PlanPreview.tsx
```

Shot card 显示：

```text
- role
- description
- search_query
- duration
- match_status
- matched clip time range
- generation prompt
- warning
```

---

## 15. Codex 执行顺序

建议让 Codex 按下面顺序实施，不要一次做完所有复杂能力。

### Task 1

```text
Implement smart_trim node definition and handler skeleton.
Do not implement visual model yet.
Use ASR/text fallback and deterministic mock visual scoring when no visual service is configured.
Register node and add tests.
```

### Task 2

```text
Implement smart_trim segment selection and ffmpeg output.
Support full video, best clip, and montage decisions.
Return metadata.
Add handler tests.
```

### Task 3

```text
Implement concat_many node and handler.
Support 1-12 inputs.
Normalize video/audio and concatenate.
Add tests.
```

### Task 4

```text
Add AutoFlow storyboard schemas.
Implement rule-based storyboard generator for cat/dog/generic prompt.
Ensure long shot descriptions and generation prompts.
Add tests.
```

### Task 5

```text
Implement AutoFlow plan API for input_video strategy.
Generate source -> smart_trim per shot -> concat_many -> export pipeline.
Validate pipeline.
Add API tests.
```

### Task 6

```text
Implement material_library strategy.
Use materialize_material_search during plan phase.
Generate source nodes from refined clip asset_ids.
Validate pipeline.
Add tests.
```

### Task 7

```text
Add optional visual embedding service integration for smart_trim.
Keep fallback behavior if service is unavailable.
Add config fields and tests with mocked HTTP service.
```

---

## 16. 明确不要做的事

MVP 不要做：

```text
- 不要把原有 trim 改成 prompt 节点
- 不要让 LLM 直接生成任意 nodes/edges
- 不要引入多输出 artifact list port
- 不要第一版强依赖大 VLM
- 不要第一版直接接真实视频生成模型
- 不要在找不到素材时硬剪错误片段
- 不要默认公开发布外部平台素材
```

---

## 17. 最小成功标准

当实现完成后，以下流程必须可用。

### 17.1 单视频小猫片段

```text
上传 mixed_vlog_with_cat.mp4
创建 pipeline:
source -> smart_trim(prompt="小猫") -> export
执行 job
得到只包含小猫片段的视频
```

### 17.2 小猫集锦整片返回

```text
上传 cat_compilation.mp4
创建 pipeline:
source -> smart_trim(prompt="我要小猫的视频", mode="auto", target_duration=0) -> export
执行 job
得到接近原视频长度的视频
metadata.decision == "return_full_video"
```

### 17.3 小猫 30 秒分镜视频

```text
POST /api/v1/autoflow/plan
{
  "prompt": "我要一个 30 秒小猫视频，竖屏，可爱快节奏",
  "input_asset_id": "...",
  "target_duration": 30,
  "aspect_ratio": "9:16",
  "source_strategy": "input_video"
}

返回:
source -> smart_trim_01 -> ...
source -> smart_trim_02 -> ...
...
concat_many -> export

validate_pipeline().valid == true
```

### 17.4 缺失镜头不伪造

```text
输入 no_cat.mp4
prompt="我要小猫视频"
smart_trim metadata.decision == "no_match"
AutoFlow shot.match_status == "missing"
如果 allow_video_generation=false，不生成假片段
```

---

## 18. 推荐目录结构汇总

```text
backend/app/api/autoflow.py

backend/app/autoflow/
  __init__.py
  schemas.py
  intent_parser.py
  storyboard_generator.py
  pipeline_builder.py
  service.py

backend/app/node_registry/builtin/
  smart_trim.py
  concat_many.py

backend/worker/handlers/
  smart_trim.py
  concat_many.py

backend/tests/autoflow/
  test_storyboard_generator.py
  test_pipeline_builder.py
  test_autoflow_plan_api.py

backend/tests/worker/
  test_smart_trim_handler.py
  test_concat_many_handler.py
```

---

## 19. 后续扩展方向

### 19.1 `montage_assembler`

比 `concat_many` 更智能：

```text
- 按节奏重排
- hook clip 放开头
- 根据 BGM beat 切换
- 自动插入过渡
- 自动竖屏构图
```

### 19.2 `video_generate`

未来新增：

```text
backend/app/node_registry/builtin/video_generate.py
backend/worker/handlers/video_generate.py
```

输入：

```text
generation.prompt
generation.negative_prompt
reference images
duration
aspect_ratio
seed
model_hint
```

输出：

```text
generated video
```

### 19.3 `smart_subject_crop`

未来用于竖屏裁切：

```text
input video -> detect subject -> crop to 9:16
```

### 19.4 视觉索引升级

素材库入库时新增：

```text
frame captions
object tags
visual embeddings
motion score
quality score
face/animal/person/car detector tags
```

---

## 20. 给 Codex 的最终指令摘要

```text
Implement a new smart_trim node instead of modifying trim.
Implement a concat_many node for multi-shot assembly.
Add AutoFlow storyboard schemas with long shot descriptions and generation prompts.
Build input_video AutoFlow plan:
  source -> smart_trim per shot -> concat_many -> export
Validate generated pipeline with validate_pipeline().
Keep LLM output limited to structured storyboard JSON.
Do not directly generate arbitrary workflow graphs with LLM.
Prepare generation fields but do not implement actual video generation yet.
```
