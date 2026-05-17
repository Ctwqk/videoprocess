import apiClient from './client';
import type {
  AutoFlowApprovalRequest,
  AutoFlowPlanPatch,
  AutoFlowPlan,
  AutoFlowPublicApprovalRequest,
  AutoFlowRejectRequest,
  AutoFlowRequest,
  AutoFlowRun,
  AutoFlowIdea,
  AutoFlowMetric,
  AutoFlowTemplateMetricSummary,
  AutoFlowTrendSignalInput,
  AutoFlowTrendSuggestion,
  CapabilityManifest,
  ExecuteOptions,
  WorkflowTemplate,
} from '../types/autoflow';

function unwrapItems<T>(payload: T[] | { items?: T[] }): T[] {
  return Array.isArray(payload) ? payload : payload.items ?? [];
}

export async function createAutoFlowPlan(payload: AutoFlowRequest): Promise<AutoFlowPlan> {
  const res = await apiClient.post<AutoFlowPlan>('/autoflow/plan', payload);
  return res.data;
}

export async function patchAutoFlowPlan(planId: string, payload: AutoFlowPlanPatch): Promise<AutoFlowPlan> {
  const res = await apiClient.patch<AutoFlowPlan>(`/autoflow/plans/${planId}`, payload);
  return res.data;
}

export async function approveAutoFlowPlan(
  planId: string,
  payload: AutoFlowApprovalRequest = {},
): Promise<AutoFlowPlan> {
  const res = await apiClient.post<AutoFlowPlan>(`/autoflow/plans/${planId}/approve`, payload);
  return res.data;
}

export async function approveAutoFlowPlanPublic(
  planId: string,
  payload: AutoFlowPublicApprovalRequest = {},
): Promise<AutoFlowPlan> {
  const res = await apiClient.post<AutoFlowPlan>(`/autoflow/plans/${planId}/approve-public`, payload);
  return res.data;
}

export async function rejectAutoFlowPlan(
  planId: string,
  payload: AutoFlowRejectRequest = {},
): Promise<AutoFlowPlan> {
  const res = await apiClient.post<AutoFlowPlan>(`/autoflow/plans/${planId}/reject`, payload);
  return res.data;
}

export async function executeAutoFlowPlan(
  planId: string,
  options: ExecuteOptions = {},
): Promise<AutoFlowRun> {
  const res = await apiClient.post<AutoFlowRun>('/autoflow/execute', {
    plan_id: planId,
    execute: true,
    ...options,
  });
  return res.data;
}

export async function getAutoFlowRun(runId: string): Promise<AutoFlowRun> {
  const res = await apiClient.get<AutoFlowRun>(`/autoflow/runs/${runId}`);
  return res.data;
}

export async function listAutoFlowTemplates(): Promise<WorkflowTemplate[]> {
  const res = await apiClient.get<WorkflowTemplate[] | { items?: WorkflowTemplate[] }>('/autoflow/templates');
  return unwrapItems(res.data);
}

export async function getAutoFlowCapabilities(): Promise<CapabilityManifest> {
  const res = await apiClient.get<CapabilityManifest>('/autoflow/capabilities');
  return res.data;
}

export async function collectAutoFlowMetrics(
  runId: string,
  payload: Record<string, unknown>,
): Promise<AutoFlowMetric> {
  const res = await apiClient.post<AutoFlowMetric>(`/autoflow/runs/${runId}/collect-metrics`, payload);
  return res.data;
}

export async function listAutoFlowRunMetrics(runId: string): Promise<AutoFlowMetric[]> {
  const res = await apiClient.get<AutoFlowMetric[]>(`/autoflow/runs/${runId}/metrics`);
  return res.data;
}

export async function listAutoFlowTemplateMetrics(): Promise<AutoFlowTemplateMetricSummary[]> {
  const res = await apiClient.get<AutoFlowTemplateMetricSummary[]>('/autoflow/metrics/templates');
  return res.data;
}

export async function createAutoFlowTrendSignal(payload: AutoFlowTrendSignalInput): Promise<Record<string, unknown>> {
  const res = await apiClient.post<Record<string, unknown>>('/autoflow/trend-signals', payload);
  return res.data;
}

export async function listAutoFlowTrendSuggestions(params: {
  source_policy?: string;
  material_library_ids?: string[];
  limit?: number;
} = {}): Promise<AutoFlowTrendSuggestion[]> {
  const res = await apiClient.get<AutoFlowTrendSuggestion[]>('/autoflow/trend-suggestions', {
    params: {
      source_policy: params.source_policy,
      material_library_ids: params.material_library_ids?.join(','),
      limit: params.limit,
    },
  });
  return res.data;
}

export async function createAutoFlowIdeas(payload: {
  target_platforms: string[];
  material_library_ids: string[];
  source_policy: string;
  count: number;
}): Promise<AutoFlowIdea[]> {
  const res = await apiClient.post<AutoFlowIdea[]>('/autoflow/ideas', payload);
  return res.data;
}
