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

export interface AutoFlowRequest {
  prompt: string;
  target_platforms: string[];
  duration_sec: number | null;
  aspect_ratio: AutoFlowAspectRatio;
  source_policy: AutoFlowSourcePolicy;
  publish_mode: AutoFlowPublishMode;
  material_library_ids: string[];
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

export interface AutoFlowPlan {
  plan_id: string;
  request: AutoFlowRequest;
  intent: AutoFlowIntent;
  template_id: string;
  pipeline_definition: Record<string, unknown>;
  candidates: AutoFlowClipCandidate[];
  metadata: AutoFlowMetadata;
  validation: Record<string, unknown>;
  rights: Record<string, unknown>;
  warnings: string[];
  needs_review: boolean;
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
  review_approved?: boolean;
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
  source_policies?: AutoFlowSourcePolicy[];
  publish_modes?: AutoFlowPublishMode[];
  aspect_ratios?: AutoFlowAspectRatio[];
  material_libraries?: MaterialLibraryOption[];
  [key: string]: unknown;
}
