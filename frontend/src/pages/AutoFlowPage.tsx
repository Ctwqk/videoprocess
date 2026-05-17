import { useEffect, useState } from 'react';

import {
  approveAutoFlowPlan,
  approveAutoFlowPlanPublic,
  createAutoFlowPlan,
  executeAutoFlowPlan,
  getAutoFlowCapabilities,
  listAutoFlowTemplates,
  patchAutoFlowPlan,
  rejectAutoFlowPlan,
} from '../api/autoflow';
import AutoFlowCandidateClips from '../components/autoflow/AutoFlowCandidateClips';
import AutoFlowMetricsPanel from '../components/autoflow/AutoFlowMetricsPanel';
import AutoFlowMetadataEditor from '../components/autoflow/AutoFlowMetadataEditor';
import AutoFlowPlanPanel from '../components/autoflow/AutoFlowPlanPanel';
import AutoFlowPromptBox from '../components/autoflow/AutoFlowPromptBox';
import AutoFlowReviewGate from '../components/autoflow/AutoFlowReviewGate';
import AutoFlowRunStatus from '../components/autoflow/AutoFlowRunStatus';
import AutoFlowWorkflowPreview from '../components/autoflow/AutoFlowWorkflowPreview';
import type {
  AutoFlowCandidateEditDraft,
  AutoFlowClipCandidate,
  AutoFlowMetadataEditDraft,
  AutoFlowMetadataPatch,
  AutoFlowPlan,
  AutoFlowPlanPatch,
  AutoFlowPublishMode,
  AutoFlowRequest,
  AutoFlowRun,
  CapabilityManifest,
  ExecuteOptions,
  WorkflowTemplate,
} from '../types/autoflow';

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

const publishModes: AutoFlowPublishMode[] = [
  'preview_only',
  'private_upload',
  'unlisted_upload',
  'public_after_review',
];

const defaultMetadataDraft: AutoFlowMetadataEditDraft = {
  selected_title: '',
  description: '',
  tags: [],
  hashtags: [],
  publish_mode: 'preview_only',
};

function isPublishMode(value: string): value is AutoFlowPublishMode {
  return publishModes.includes(value as AutoFlowPublishMode);
}

function metadataBoolean(candidate: AutoFlowClipCandidate, keys: string[], fallback: boolean) {
  for (const key of keys) {
    const value = candidate.metadata[key];
    if (typeof value === 'boolean') return value;
  }
  return fallback;
}

function metadataString(candidate: AutoFlowClipCandidate, keys: string[]) {
  for (const key of keys) {
    const value = candidate.metadata[key];
    if (typeof value === 'string') return value;
  }
  return '';
}

function candidateDraftFromCandidate(candidate: AutoFlowClipCandidate): AutoFlowCandidateEditDraft {
  return {
    selected: metadataBoolean(candidate, ['selected', 'autoflow_selected', 'included'], true),
    locked: metadataBoolean(candidate, ['locked', 'autoflow_locked'], false),
    replacement: metadataString(candidate, ['replacement', 'replacement_url', 'replacement_asset_id']),
  };
}

function candidateDraftsFromPlan(plan: AutoFlowPlan): Record<string, AutoFlowCandidateEditDraft> {
  return Object.fromEntries(
    plan.candidates.map(candidate => [candidate.id, candidateDraftFromCandidate(candidate)]),
  );
}

function planPublishMode(plan: AutoFlowPlan): AutoFlowPublishMode {
  const mode = plan.request.publish_mode || plan.intent.publish_mode;
  return isPublishMode(mode) ? mode : 'preview_only';
}

function metadataDraftFromPlan(plan: AutoFlowPlan): AutoFlowMetadataEditDraft {
  return {
    selected_title: plan.metadata.selected_title ?? plan.metadata.title_candidates[0] ?? '',
    description: plan.metadata.description,
    tags: [...plan.metadata.tags],
    hashtags: [...plan.metadata.hashtags],
    publish_mode: planPublishMode(plan),
  };
}

function arraysEqual(first: string[], second: string[]) {
  return first.length === second.length && first.every((value, index) => value === second[index]);
}

function replacementCandidateFor(
  candidate: AutoFlowClipCandidate,
  replacement: string,
): AutoFlowClipCandidate {
  const isExternalUrl = /^https?:\/\//i.test(replacement);
  return {
    ...candidate,
    title: `Replacement for ${candidate.title}`,
    source_type: isExternalUrl ? 'external_url' : 'asset',
    url: isExternalUrl ? replacement : null,
    asset_id: isExternalUrl ? null : replacement,
    rights_status: isExternalUrl ? 'review_required' : 'allowed',
    metadata: {
      ...candidate.metadata,
      replacement,
      replacement_for: candidate.id,
    },
  };
}

function buildCandidatePatch(
  plan: AutoFlowPlan,
  candidateEdits: Record<string, AutoFlowCandidateEditDraft>,
): Pick<AutoFlowPlanPatch, 'selected_candidate_ids' | 'locked_candidate_ids' | 'replacement_candidates'> | null {
  let hasCandidateChanges = false;
  const selectedCandidateIds: string[] = [];
  const lockedCandidateIds: string[] = [];
  const replacementCandidates: AutoFlowClipCandidate[] = [];

  for (const candidate of plan.candidates) {
    const base = candidateDraftFromCandidate(candidate);
    const edit = candidateEdits[candidate.id] ?? base;
    const replacement = edit.replacement.trim();
    if (edit.selected) selectedCandidateIds.push(candidate.id);
    if (edit.selected && edit.locked) lockedCandidateIds.push(candidate.id);
    if (replacement) replacementCandidates.push(replacementCandidateFor(candidate, replacement));

    if (
      base.selected !== edit.selected
      || base.locked !== edit.locked
      || base.replacement.trim() !== replacement
    ) {
      hasCandidateChanges = true;
    }
  }

  if (!hasCandidateChanges) return null;
  return {
    selected_candidate_ids: selectedCandidateIds,
    locked_candidate_ids: lockedCandidateIds,
    ...(replacementCandidates.length > 0 ? { replacement_candidates: replacementCandidates } : {}),
  };
}

function buildMetadataPatch(plan: AutoFlowPlan, draft: AutoFlowMetadataEditDraft): AutoFlowMetadataPatch | null {
  const base = metadataDraftFromPlan(plan);
  const patch: AutoFlowMetadataPatch = {};
  const selectedTitle = draft.selected_title.trim();

  if (base.selected_title !== selectedTitle) patch.selected_title = selectedTitle || null;
  if (base.description !== draft.description) patch.description = draft.description;
  if (!arraysEqual(base.tags, draft.tags)) patch.tags = draft.tags;
  if (!arraysEqual(base.hashtags, draft.hashtags)) patch.hashtags = draft.hashtags;

  return Object.keys(patch).length > 0 ? patch : null;
}

function buildPlanPatch(
  plan: AutoFlowPlan,
  candidateEdits: Record<string, AutoFlowCandidateEditDraft>,
  metadataDraft: AutoFlowMetadataEditDraft,
): AutoFlowPlanPatch | null {
  const patch: AutoFlowPlanPatch = {};
  const candidatePatch = buildCandidatePatch(plan, candidateEdits);
  const metadataPatch = buildMetadataPatch(plan, metadataDraft);
  const basePublishMode = planPublishMode(plan);

  if (candidatePatch) Object.assign(patch, candidatePatch);
  if (metadataPatch) patch.metadata = metadataPatch;
  if (basePublishMode !== metadataDraft.publish_mode) {
    patch.publish_mode = metadataDraft.publish_mode;
  }
  if (Object.keys(patch).length > 0) {
    patch.rebuild_definition = true;
    patch.validate = true;
    patch.evaluate_rights = true;
  }

  return Object.keys(patch).length > 0 ? patch : null;
}

function isReviewApproved(plan: AutoFlowPlan) {
  return !plan.needs_review || Boolean(plan.review_approved_at);
}

function requiresPublicApproval(plan: AutoFlowPlan) {
  return planPublishMode(plan) === 'public_after_review';
}

function executeBlockReason(plan: AutoFlowPlan) {
  const status = (plan.status ?? '').toLowerCase();
  const rightsStatus = String(plan.rights.status ?? plan.rights.decision ?? '').toLowerCase();
  const rejected = status === 'rejected' || Boolean(plan.rejected_reason);
  const blocked = ['blocked', 'denied', 'rejected'].includes(status)
    || ['blocked', 'denied', 'rejected'].includes(rightsStatus)
    || plan.rights.execute_allowed === false
    || plan.rights.allowed === false;

  if (rejected) return 'Rejected plans cannot be executed.';
  if (blocked) return 'AutoFlow plan is blocked by rights policy.';
  if (!isReviewApproved(plan)) return 'Human review is required before execution.';
  if (plan.rights.publish_allowed === false || plan.rights.can_publish === false) {
    return 'Publishing is blocked by rights policy.';
  }
  if (requiresPublicApproval(plan) && !plan.public_approved_at) {
    return 'Public publishing requires public approval before execution.';
  }
  return null;
}

export default function AutoFlowPage() {
  const [request, setRequest] = useState<AutoFlowRequest>(defaultRequest);
  const [templates, setTemplates] = useState<WorkflowTemplate[]>([]);
  const [capabilities, setCapabilities] = useState<CapabilityManifest | null>(null);
  const [plan, setPlan] = useState<AutoFlowPlan | null>(null);
  const [run, setRun] = useState<AutoFlowRun | null>(null);
  const [candidateEdits, setCandidateEdits] = useState<Record<string, AutoFlowCandidateEditDraft>>({});
  const [metadataDraft, setMetadataDraft] = useState<AutoFlowMetadataEditDraft>(defaultMetadataDraft);
  const [loadingReferenceData, setLoadingReferenceData] = useState(true);
  const [planning, setPlanning] = useState(false);
  const [approving, setApproving] = useState(false);
  const [publicApproving, setPublicApproving] = useState(false);
  const [rejecting, setRejecting] = useState(false);
  const [savingPlanPatch, setSavingPlanPatch] = useState(false);
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

  const hydratePlanDrafts = (nextPlan: AutoFlowPlan) => {
    setPlan(nextPlan);
    setCandidateEdits(candidateDraftsFromPlan(nextPlan));
    setMetadataDraft(metadataDraftFromPlan(nextPlan));
  };

  const hasUnsavedPlanEdits = plan ? Boolean(buildPlanPatch(plan, candidateEdits, metadataDraft)) : false;

  const savePendingPlanEdits = async (basePlan: AutoFlowPlan) => {
    const patch = buildPlanPatch(basePlan, candidateEdits, metadataDraft);
    if (!patch) return basePlan;

    setSavingPlanPatch(true);
    try {
      const nextPlan = await patchAutoFlowPlan(basePlan.plan_id, patch);
      hydratePlanDrafts(nextPlan);
      return nextPlan;
    } finally {
      setSavingPlanPatch(false);
    }
  };

  const handleCreatePlan = async () => {
    setPlanning(true);
    setError(null);
    setRun(null);
    try {
      const nextPlan = await createAutoFlowPlan(request);
      hydratePlanDrafts(nextPlan);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate plan');
    } finally {
      setPlanning(false);
    }
  };

  const handleCandidateEditChange = (candidateId: string, edit: Partial<AutoFlowCandidateEditDraft>) => {
    setCandidateEdits(current => ({
      ...current,
      [candidateId]: {
        ...(current[candidateId] ?? { selected: true, locked: false, replacement: '' }),
        ...edit,
      },
    }));
  };

  const handleSavePlanEdits = async () => {
    if (!plan) return;
    setError(null);
    try {
      await savePendingPlanEdits(plan);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save plan edits');
    }
  };

  const handleApprove = async () => {
    if (!plan) return;
    setApproving(true);
    setError(null);
    try {
      const latestPlan = await savePendingPlanEdits(plan);
      const nextPlan = await approveAutoFlowPlan(latestPlan.plan_id);
      hydratePlanDrafts(nextPlan);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to approve plan');
    } finally {
      setApproving(false);
    }
  };

  const handleApprovePublic = async (reviewNotes: string) => {
    if (!plan) return;
    setPublicApproving(true);
    setError(null);
    try {
      const latestPlan = await savePendingPlanEdits(plan);
      const nextPlan = await approveAutoFlowPlanPublic(latestPlan.plan_id, {
        public_approved: true,
        review_notes: reviewNotes.trim() || null,
      });
      hydratePlanDrafts(nextPlan);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to approve public publishing');
    } finally {
      setPublicApproving(false);
    }
  };

  const handleReject = async (reason: string) => {
    if (!plan) return;
    setRejecting(true);
    setError(null);
    try {
      const nextPlan = await rejectAutoFlowPlan(plan.plan_id, {
        rejected_reason: reason.trim() || null,
        review_notes: reason.trim() || null,
      });
      hydratePlanDrafts(nextPlan);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reject plan');
    } finally {
      setRejecting(false);
    }
  };

  const handleExecute = async () => {
    if (!plan) return;
    setExecuting(true);
    setError(null);
    try {
      const latestPlan = await savePendingPlanEdits(plan);
      const blockReason = executeBlockReason(latestPlan);
      if (blockReason) {
        setError(blockReason);
        return;
      }

      const executeOptions: ExecuteOptions = {};
      const nextRun = await executeAutoFlowPlan(latestPlan.plan_id, executeOptions);
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
            approving={approving}
            publicApproving={publicApproving}
            rejecting={rejecting}
            saving={savingPlanPatch}
            executing={executing}
            hasUnsavedEdits={hasUnsavedPlanEdits}
            publishMode={metadataDraft.publish_mode}
            onApprove={() => void handleApprove()}
            onApprovePublic={reviewNotes => void handleApprovePublic(reviewNotes)}
            onReject={reason => void handleReject(reason)}
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
          <AutoFlowMetadataEditor
            plan={plan}
            draft={metadataDraft}
            dirty={hasUnsavedPlanEdits}
            saving={savingPlanPatch}
            onChange={setMetadataDraft}
            onSave={() => void handleSavePlanEdits()}
          />
          <AutoFlowWorkflowPreview pipelineDefinition={plan?.pipeline_definition ?? null} />
          <AutoFlowCandidateClips
            candidates={plan?.candidates ?? []}
            candidateEdits={candidateEdits}
            onCandidateEditChange={handleCandidateEditChange}
          />
        </div>
      </div>
    </div>
  );
}
