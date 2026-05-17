import apiClient from './client';
import type {
  AutoFlowPlan,
  AutoFlowRequest,
  AutoFlowRun,
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

export async function approveAutoFlowPlan(planId: string): Promise<AutoFlowPlan> {
  const res = await apiClient.post<AutoFlowPlan>(`/autoflow/plans/${planId}/approve`);
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
