export interface PipelineNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: {
    label: string;
    config: Record<string, unknown>;
    asset_id?: string;
  };
}

export interface PipelineEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string;
  targetHandle: string;
}

export interface PipelineDefinition {
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  viewport: { x: number; y: number; zoom: number };
}

export interface Pipeline {
  id: string;
  name: string;
  description: string;
  definition: PipelineDefinition;
  is_template: boolean;
  template_tags: string[];
  created_at: string;
  updated_at: string;
  version: number;
}

export type JobStatus = 'PENDING' | 'VALIDATING' | 'PLANNING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | 'PARTIALLY_FAILED';
export type NodeStatus = 'PENDING' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'SKIPPED' | 'CANCELLED';

export interface Job {
  id: string;
  pipeline_id: string;
  status: JobStatus;
  submitted_at: string;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

export interface NodeExecution {
  id: string;
  node_id: string;
  node_type: string;
  node_label: string;
  status: NodeStatus;
  progress: number;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  output_artifact_id: string | null;
  output_artifact_filename?: string | null;
  output_artifact_media_info?: Record<string, unknown> | null;
}

export interface Asset {
  id: string;
  filename: string;
  original_name: string;
  mime_type: string | null;
  file_size: number | null;
  media_info: Record<string, unknown> | null;
  uploaded_at: string;
}

export interface MaterialLibrary {
  id: string;
  name: string;
  description: string;
  is_disabled: boolean;
  created_at: string;
  updated_at: string;
}

export type ArtifactKind = 'intermediate' | 'final';

export interface Artifact {
  id: string;
  job_id: string;
  node_execution_id: string;
  kind: ArtifactKind;
  filename: string;
  mime_type: string | null;
  file_size: number | null;
  created_at: string;
}

export interface PortDefinition {
  name: string;
  port_type: 'video' | 'audio' | 'image' | 'subtitle' | 'any_media' | 'search_results' | 'url_value' | 'asset_value';
  required: boolean;
  description: string;
}

export interface ParamDefinition {
  name: string;
  param_type: 'string' | 'number' | 'boolean' | 'select' | 'file';
  default: unknown;
  required: boolean;
  description: string;
  options: string[] | null;
  min_value: number | null;
  max_value: number | null;
}

export interface NodeTypeInfo {
  type_name: string;
  display_name: string;
  category: string;
  description: string;
  icon: string;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  params: ParamDefinition[];
  worker_type: string;
}
