import { useEffect, useState } from 'react';

import {
  approveAutoFlowPlan,
  createAutoFlowPlan,
  executeAutoFlowPlan,
  getAutoFlowCapabilities,
  listAutoFlowTemplates,
} from '../api/autoflow';
import AutoFlowCandidateClips from '../components/autoflow/AutoFlowCandidateClips';
import AutoFlowMetricsPanel from '../components/autoflow/AutoFlowMetricsPanel';
import AutoFlowPlanPanel from '../components/autoflow/AutoFlowPlanPanel';
import AutoFlowPromptBox from '../components/autoflow/AutoFlowPromptBox';
import AutoFlowReviewGate from '../components/autoflow/AutoFlowReviewGate';
import AutoFlowRunStatus from '../components/autoflow/AutoFlowRunStatus';
import AutoFlowWorkflowPreview from '../components/autoflow/AutoFlowWorkflowPreview';
import type { AutoFlowPlan, AutoFlowRequest, AutoFlowRun, CapabilityManifest, WorkflowTemplate } from '../types/autoflow';

const defaultRequest: AutoFlowRequest = {
  prompt: '我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要公开发布。',
  target_platforms: ['youtube_shorts'],
  source_platforms: ['youtube', 'bilibili', 'x', 'xiaohongshu'],
  duration_sec: 30,
  aspect_ratio: '9:16',
  source_policy: 'owned_only',
  publish_mode: 'preview_only',
  material_library_ids: [],
  user_constraints: {},
};

export default function AutoFlowPage() {
  const [request, setRequest] = useState<AutoFlowRequest>(defaultRequest);
  const [templates, setTemplates] = useState<WorkflowTemplate[]>([]);
  const [capabilities, setCapabilities] = useState<CapabilityManifest | null>(null);
  const [plan, setPlan] = useState<AutoFlowPlan | null>(null);
  const [run, setRun] = useState<AutoFlowRun | null>(null);
  const [approved, setApproved] = useState(false);
  const [loadingReferenceData, setLoadingReferenceData] = useState(true);
  const [planning, setPlanning] = useState(false);
  const [approving, setApproving] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function loadReferenceData() {
      setLoadingReferenceData(true);
      try {
        const [nextTemplates, nextCapabilities] = await Promise.all([
          listAutoFlowTemplates(),
          getAutoFlowCapabilities(),
        ]);
        if (!cancelled) {
          setTemplates(nextTemplates);
          setCapabilities(nextCapabilities);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load AutoFlow reference data');
        }
      } finally {
        if (!cancelled) setLoadingReferenceData(false);
      }
    }
    void loadReferenceData();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleCreatePlan = async () => {
    setPlanning(true);
    setError(null);
    setRun(null);
    setApproved(false);
    try {
      const nextPlan = await createAutoFlowPlan(request);
      setPlan(nextPlan);
      setApproved(!nextPlan.needs_review);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate plan');
    } finally {
      setPlanning(false);
    }
  };

  const handleApprove = async () => {
    if (!plan) return;
    setApproving(true);
    setError(null);
    try {
      const nextPlan = await approveAutoFlowPlan(plan.plan_id);
      setPlan(nextPlan);
      setApproved(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to approve plan');
    } finally {
      setApproving(false);
    }
  };

  const handleExecute = async () => {
    if (!plan) return;
    setExecuting(true);
    setError(null);
    try {
      const nextRun = await executeAutoFlowPlan(plan.plan_id, { review_approved: approved });
      setRun(nextRun);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to execute plan');
    } finally {
      setExecuting(false);
    }
  };

  return (
    <div
      style={{
        height: '100%',
        overflowY: 'auto',
        backgroundColor: '#020617',
        color: '#e2e8f0',
        padding: 18,
        boxSizing: 'border-box',
      }}
    >
      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(320px, 420px) minmax(0, 1fr)', gap: 16, alignItems: 'start' }}>
        <div style={{ display: 'grid', gap: 12 }}>
          <AutoFlowPromptBox
            value={request}
            templates={templates}
            capabilities={capabilities}
            planning={planning}
            loadingReferenceData={loadingReferenceData}
            onChange={setRequest}
            onSubmit={() => void handleCreatePlan()}
          />
          <AutoFlowReviewGate
            plan={plan}
            approved={approved}
            approving={approving}
            executing={executing}
            onApprove={() => void handleApprove()}
            onExecute={() => void handleExecute()}
          />
          <AutoFlowRunStatus run={run} />
          <AutoFlowMetricsPanel
            request={request}
            run={run}
            onUseIdea={prompt => setRequest(current => ({ ...current, prompt }))}
          />
        </div>

        <div style={{ display: 'grid', gap: 12, minWidth: 0 }}>
          {error ? (
            <div
              style={{
                border: '1px solid #7f1d1d',
                backgroundColor: '#450a0a',
                color: '#fecaca',
                borderRadius: 8,
                padding: 12,
                fontSize: 13,
              }}
            >
              {error}
            </div>
          ) : null}
          <AutoFlowPlanPanel plan={plan} templates={templates} />
          <AutoFlowWorkflowPreview pipelineDefinition={plan?.pipeline_definition ?? null} />
          <AutoFlowCandidateClips candidates={plan?.candidates ?? []} />
        </div>
      </div>
    </div>
  );
}
