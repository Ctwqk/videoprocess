# AutoFlow Templates

Templates are the approved workflow shapes AutoFlow can build. A prompt may
influence intent, slots, candidates, and metadata, but graph structure must come
from `TemplateLibrary` and `PipelineBuilder`.

## `animal_compilation_short`

Use for short animal or pet compilations.

- Intent types: `animal_compilation`
- Common prompts: cat, kitten, dog, puppy, pet compilations
- Default slots: `target_duration=30`, `aspect_ratio=9:16`
- Required capabilities: `source`, `trim`, `concat_timeline`, `transcode`,
  `export`
- Typical pipeline: source clips -> trim clips -> concat timeline -> transcode
  -> export
- Safety: owned/library candidates are allowed for preview. External candidates
  require a research/review source policy.

Example request:

```json
{
  "prompt": "我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要直接公开发布。",
  "target_platforms": ["youtube_shorts"]
}
```

## `hot_topic_explainer_short`

Use for short explainers based on research material.

- Intent types: `hot_topic_explainer`
- Common prompts: hot topic, explainer, what happened, discussion prompts
- Default slots: `target_duration=45`, `aspect_ratio=9:16`
- Required capabilities: `youtube_search`, `url_download`,
  `subtitle_to_speech`, `subtitle`, `concat_timeline`, `transcode`, `export`
- Typical pipeline in Phase 1 builder: download/source candidates -> trim ->
  concat/transcode/export
- Safety: external research candidates should set `source_policy=research_only`
  or `remix_with_review`; plans should require human review before publication.

Example request:

```json
{
  "prompt": "请做一个 45 秒热点解释短视频，讨论 AI 视频生成，竖屏，带讲解和字幕，只做草稿预览。",
  "source_policy": "research_only",
  "publish_mode": "preview_only",
  "target_platforms": ["youtube_shorts"]
}
```

## `material_library_remix`

Use for low-risk remixes from owned material libraries.

- Intent types: `material_library_remix`, `generic_video`
- Common prompts: material library, remix, travel material, owned clips
- Default slots: `target_duration=20`, `source_policy=owned_only`
- Required capabilities: `material_search`, `source`, `trim`,
  `concat_timeline`, `transcode`, `export`
- Typical pipeline: material search intent -> owned sources -> trim -> concat
  timeline -> transcode/export
- Safety: safest default template because candidates should be owned or
  library-backed.

Example request:

```json
{
  "prompt": "用素材库里的旅行素材做一个 20 秒海边日落治愈混剪，竖屏，先导出预览。",
  "material_library_ids": ["travel-library"]
}
```

## Adding A Template

1. Add the template to `backend/app/autoflow/template_library.py`.
2. Add or update parser rules so the intent maps to the template.
3. Extend `PipelineBuilder` only if the existing deterministic graph builder
   cannot represent the template safely.
4. Add tests for template selection and pipeline validation.
5. Add or update docs and, when useful, an e2e example.

Do not add a template that depends on arbitrary LLM-generated node graphs.
