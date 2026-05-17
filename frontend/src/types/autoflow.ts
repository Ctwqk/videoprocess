export type AutoFlowAspectRatio = '9:16' | '16:9' | '1:1' | 'auto';

export type AutoFlowSourcePolicy =
  | 'owned_only'
  | 'licensed_only'
  | 'public_domain_or_cc'
  | 'research_only'
  | 'remix_with_review';

export type AutoFlowPublishMode =
  | 'preview_only'
  | 'private_upload'
  | 'unlisted_upload'
  | 'public_after_review';

export type AutoFlowSourceStrategy =
  | 'auto'
  | 'input_video'
  | 'material_library'
  | 'external_research'
  | 'generate_missing'
  | 'hybrid';

export interface AutoFlowRequest {
  prompt: string;
  input_asset_id?: string | null;
  target_platforms: string[];
  source_platforms: string[];
  duration_sec: number | null;
  aspect_ratio: AutoFlowAspectRatio;
  source_policy: AutoFlowSourcePolicy;
  publish_mode: AutoFlowPublishMode;
  material_library_ids: string[];
  source_strategy: AutoFlowSourceStrategy;
  allow_video_generation: boolean;
  min_shots: number;
  max_shots: number;
  provider_config_id?: string | null;
  model?: string | null;
  constraints: Record<string, unknown>;
  user_constraints: Record<string, unknown>;
}

export interface AutoFlowIntent {
  intent_type: string;
  subject: string;
  style: string;
  duration_sec: number;
  aspect_ratio: string;
  target_platforms: string[];
  source_policy: string;
  publish_mode: string;
  keywords: string[];
  negative_keywords: string[];
  needs_voiceover: boolean;
  needs_subtitles: boolean;
  needs_bgm: boolean;
}

export interface AutoFlowClipCandidate {
  id: string;
  title: string;
  source_type: string;
  url: string | null;
  asset_id: string | null;
  start_sec: number | null;
  end_sec: number | null;
  score: number;
  score_breakdown: Record<string, number>;
  rights_status: string;
  metadata: Record<string, unknown>;
}

export interface AutoFlowMetadata {
  title_candidates: string[];
  selected_title: string | null;
  description: string;
  tags: string[];
  hashtags: string[];
  thumbnail_text_candidates: string[];
  platform_payloads: Record<string, Record<string, unknown>>;
}

export interface VideoGenerationHints {
  enabled: boolean;
  prompt: string;
  negative_prompt: string;
  reference_asset_ids: string[];
  reference_image_asset_id?: string | null;
  reference_video_asset_id?: string | null;
  first_frame_asset_id?: string | null;
  last_frame_asset_id?: string | null;
  model_hint: string;
  resolution: string;
  fps?: number | null;
  seed?: number | null;
  guidance_scale?: number | null;
  motion_strength?: number | null;
  extra: Record<string, unknown>;
}

export interface CameraSpec {
  shot_size: string;
  angle: string;
  movement: string;
  lens: string;
  composition: string;
}

export interface VisualStyleSpec {
  mood: string;
  lighting: string;
  color_palette: string;
  realism: string;
  texture: string;
  platform_style: string;
}

export interface ShotSpec {
  id: string;
  role: string;
  description: string;
  director_notes: string;
  search_query: string;
  search_queries: string[];
  negative_queries: string[];
  must_have: string[];
  nice_to_have: string[];
  must_not_have: string[];
  target_duration: number;
  min_duration: number;
  max_duration: number;
  camera: CameraSpec;
  visual_style: VisualStyleSpec;
  narration: string;
  on_screen_text: string;
  sound_design: string;
  generation: VideoGenerationHints;
  matched_asset_id?: string | null;
  matched_source_asset_id?: string | null;
  matched_start_sec?: number | null;
  matched_end_sec?: number | null;
  match_score?: number | null;
  match_status: 'pending' | 'matched' | 'missing' | 'generated' | 'skipped';
  extra: Record<string, unknown>;
}

export interface StoryboardPlan {
  subject: string;
  title: string;
  logline: string;
  style: string;
  target_platforms: string[];
  aspect_ratio: AutoFlowAspectRatio;
  total_duration: number;
  source_strategy: Exclude<AutoFlowSourceStrategy, 'auto'>;
  allow_video_generation: boolean;
  shots: ShotSpec[];
  title_candidates: string[];
  description: string;
  tags: string[];
  hashtags: string[];
  warnings: string[];
  extra: Record<string, unknown>;
}

export interface AutoFlowStoryboardRequest {
  prompt: string;
  input_asset_id?: string | null;
  material_library_ids: string[];
  target_duration: number;
  aspect_ratio: AutoFlowAspectRatio;
  target_platforms: string[];
  source_strategy: AutoFlowSourceStrategy;
  allow_video_generation: boolean;
  max_shots: number;
  min_shots: number;
  style: string;
  provider_config_id?: string | null;
  model?: string | null;
  constraints: Record<string, unknown>;
}

export interface AutoFlowStoryboardResponse {
  storyboard: StoryboardPlan;
  raw_model_output?: string | null;
  warnings: string[];
}

export type AutoFlowPlanStatus =
  | 'blocked'
  | 'needs_review'
  | 'approved'
  | 'public_approved'
  | 'rejected'
  | 'executed'
  | (string & {});

export interface AutoFlowCandidateEditDraft {
  selected: boolean;
  locked: boolean;
  replacement: string;
}

export interface AutoFlowMetadataPatch {
  selected_title?: string | null;
  description?: string;
  tags?: string[];
  hashtags?: string[];
}

export interface AutoFlowMetadataEditDraft {
  selected_title: string;
  description: string;
  tags: string[];
  hashtags: string[];
  publish_mode: AutoFlowPublishMode;
}

export interface AutoFlowPlanPatch {
  selected_candidate_ids?: string[];
  locked_candidate_ids?: string[];
  replacement_candidates?: AutoFlowClipCandidate[];
  metadata?: AutoFlowMetadataPatch;
  publish_mode?: AutoFlowPublishMode;
  publish_settings?: Record<string, unknown>;
  target_platforms?: string[];
  user_constraints?: Record<string, unknown>;
  rebuild_definition?: boolean;
  validate?: boolean;
  evaluate_rights?: boolean;
}

export interface AutoFlowPublicApprovalRequest {
  review_notes?: string | null;
  public_approved?: boolean;
}

export interface AutoFlowApprovalRequest {
  review_notes?: string | null;
}

export interface AutoFlowRejectRequest {
  review_notes?: string | null;
  rejected_reason?: string | null;
}

export interface AutoFlowPlan {
  plan_id: string;
  status?: AutoFlowPlanStatus;
  request: AutoFlowRequest;
  intent: AutoFlowIntent;
  template_id: string;
  pipeline_definition: Record<string, unknown>;
  storyboard?: StoryboardPlan | null;
  candidates: AutoFlowClipCandidate[];
  metadata: AutoFlowMetadata;
  validation: Record<string, unknown>;
  rights: Record<string, unknown>;
  warnings: string[];
  needs_review: boolean;
  review_approved_at?: string | null;
  public_approved_at?: string | null;
  review_notes?: string | null;
  rejected_reason?: string | null;
}

export type AutoFlowRunStatus =
  | 'PENDING'
  | 'PLANNING'
  | 'QUEUED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELLED'
  | 'PARTIALLY_FAILED'
  | (string & {});

export interface AutoFlowArtifactRef {
  artifact_id?: string | null;
  id?: string | null;
  filename?: string | null;
  mime_type?: string | null;
  download_url?: string | null;
  preview_url?: string | null;
}

export interface AutoFlowRun {
  run_id: string;
  plan_id: string | null;
  pipeline_id: string | null;
  job_id: string | null;
  status: AutoFlowRunStatus;
  submitted_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  error_message?: string | null;
  output_artifact_id?: string | null;
  output_artifact?: AutoFlowArtifactRef | null;
  output_artifacts?: AutoFlowArtifactRef[];
  preview_url?: string | null;
  download_url?: string | null;
  metadata?: Record<string, unknown>;
}

export interface ExecuteOptions {
  save_as_template?: boolean;
  execute?: boolean;
  plan?: AutoFlowPlan;
}

export interface WorkflowTemplate {
  id: string;
  name: string;
  description?: string | null;
  tags?: string[];
  supported_platforms?: string[];
  default_duration_sec?: number | null;
  default_aspect_ratio?: string | null;
  source_policy?: string | null;
  publish_modes?: string[];
  [key: string]: unknown;
}

export interface CapabilityNodeParam {
  name: string;
  param_type?: string;
  default?: unknown;
  required?: boolean;
  description?: string;
  options?: string[] | null;
}

export interface CapabilityNodePort {
  name: string;
  port_type?: string;
  required?: boolean;
  description?: string;
}

export interface CapabilityNode {
  type_name: string;
  display_name?: string;
  category?: string;
  description?: string;
  icon?: string;
  inputs?: CapabilityNodePort[];
  outputs?: CapabilityNodePort[];
  params?: CapabilityNodeParam[];
  worker_type?: string;
  autoflow_tags?: string[];
}

export interface MaterialLibraryOption {
  id: string;
  name: string;
  description?: string | null;
}

export interface CapabilityManifest {
  nodes?: CapabilityNode[];
  node_types?: CapabilityNode[];
  templates?: WorkflowTemplate[];
  platforms?: string[];
  target_platforms?: string[];
  source_platforms?: string[];
  source_policies?: AutoFlowSourcePolicy[];
  publish_modes?: AutoFlowPublishMode[];
  aspect_ratios?: AutoFlowAspectRatio[];
  material_libraries?: MaterialLibraryOption[];
  [key: string]: unknown;
}

export interface AutoFlowMetric {
  metric_id: string;
  run_id: string;
  template_id: string;
  intent_type: string;
  platform: string;
  platform_content_id: string;
  views: number;
  likes: number;
  comments: number;
  shares: number;
  like_rate: number;
  comment_rate: number;
  share_rate: number;
  avg_retention: number;
  virality_score: number;
}

export interface AutoFlowTemplateMetricSummary {
  template_id: string;
  metric_count: number;
  total_views: number;
  avg_views: number;
  avg_like_rate: number;
  avg_comment_rate: number;
  avg_share_rate: number;
  avg_retention: number;
  avg_virality_score: number;
  intent_type?: string;
}

export interface AutoFlowTrendSignalInput {
  source: string;
  keyword: string;
  score?: number;
  trend_growth?: number;
  cross_platform_mentions?: number;
  material_availability?: number;
  competition?: number;
  rights_risk?: number;
  metadata?: Record<string, unknown>;
}

export interface AutoFlowTrendSuggestion {
  keyword: string;
  opportunity_score: number;
  recommended_template: string;
  estimated_material_count: number;
  rights_risk: number;
  reason: string;
}

export interface AutoFlowIdea {
  idea_id: string;
  prompt: string;
  template_id: string;
  opportunity_score: number;
  estimated_material_count: number;
  risk: string;
  target_platforms: string[];
  source_policy: string;
}
