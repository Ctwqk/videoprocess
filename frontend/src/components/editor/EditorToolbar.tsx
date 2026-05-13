import { useState } from 'react';
import { isAxiosError } from 'axios';
import useEditorStore from '../../store/editorStore';
import apiClient from '../../api/client';
import { useNavigate } from 'react-router-dom';
import type { PipelineDefinition } from '../../api/types';
import useNodeTypes from '../../hooks/useNodeTypes';
import BatchExecuteModal, { parseBatchItems } from '../batch/BatchExecuteModal';
import { buildBatchItems, buildPlannerBatchItems, hasPlannerNodes } from '../../utils/plannerBatch';
import { buildPipelineDefinition } from '../../utils/pipelineDefinition';

export default function EditorToolbar() {
  const { nodes, edges, pipelineId, pipelineName, isDirty, setPipeline, setPipelineName, clear } = useEditorStore();
  const { nodeTypes } = useNodeTypes();
  const [saving, setSaving] = useState(false);
  const [validating, setValidating] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [submittingBatch, setSubmittingBatch] = useState(false);
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);
  const [batchPipelineId, setBatchPipelineId] = useState<string | null>(null);
  const [batchInputText, setBatchInputText] = useState('');
  const [batchInputError, setBatchInputError] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const navigate = useNavigate();

  const getDefinition = (): PipelineDefinition => {
    return buildPipelineDefinition(nodes, edges, {
      applyDefaults: true,
      nodeTypes,
    });
  };

  const persistPipeline = async (
    definition: PipelineDefinition,
    options?: { pipelineId?: string; isTemplate?: boolean },
  ): Promise<string> => {
    const targetPipelineId = options?.pipelineId ?? pipelineId;
    const payload = {
      name: pipelineName,
      definition,
      ...(options?.isTemplate ? { is_template: true } : {}),
    };

    if (targetPipelineId) {
      const res = await apiClient.put(`/pipelines/${targetPipelineId}`, payload);
      setPipeline(res.data.id, res.data.name, nodes, edges);
      return res.data.id as string;
    }

    const res = await apiClient.post('/pipelines', payload);
    setPipeline(res.data.id, res.data.name, nodes, edges);
    navigate(`/editor/${res.data.id}`, { replace: true });
    return res.data.id as string;
  };

  const ensureSaved = async (): Promise<string | null> => {
    setSaving(true);
    setMessage(null);
    try {
      const savedPipelineId = await persistPipeline(getDefinition());
      setMessage({ type: 'success', text: 'Saved' });
      return savedPipelineId;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Save failed';
      setMessage({ type: 'error', text: msg });
      return null;
    } finally {
      setSaving(false);
    }
  };

  const handleSave = async () => {
    await ensureSaved();
  };

  const handleValidate = async () => {
    setValidating(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      const res = await apiClient.post('/pipelines/validate', definition);
      if (res.data.valid) {
        setMessage({ type: 'success', text: 'Pipeline is valid!' });
      } else {
        const errors = res.data.errors.map((e: { message: string }) => e.message).join('; ');
        setMessage({ type: 'error', text: errors });
      }
    } catch {
      setMessage({ type: 'error', text: 'Validation request failed' });
    } finally {
      setValidating(false);
    }
  };

  const handleSaveAsTemplate = async () => {
    setSavingTemplate(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      let targetPipelineId = pipelineId;

      if (isDirty || !targetPipelineId) {
        targetPipelineId = await ensureSaved();
        if (!targetPipelineId) {
          return;
        }
      }

      await persistPipeline(definition, { pipelineId: targetPipelineId, isTemplate: true });
      setMessage({ type: 'success', text: 'Saved as template!' });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Save as template failed';
      setMessage({ type: 'error', text: msg });
    } finally {
      setSavingTemplate(false);
    }
  };

  const handleRun = async () => {
    setSubmitting(true);
    setMessage(null);
    try {
      const definition = getDefinition();
      const targetPipelineId = isDirty || !pipelineId
        ? await ensureSaved()
        : pipelineId;
      if (!targetPipelineId) {
        return;
      }

      const payload = hasPlannerNodes(definition)
        ? (() => {
            const items = buildPlannerBatchItems(definition);
            if (items.length !== 1) {
              throw new Error(`Planner flow resolved to ${items.length} records. Use Batch Run instead of Run.`);
            }
            return { pipeline_id: targetPipelineId, inputs: items[0] };
          })()
        : { pipeline_id: targetPipelineId };

      const res = await apiClient.post('/jobs', payload);
      setMessage({ type: 'success', text: 'Job submitted!' });
      setTimeout(() => navigate(`/jobs/${res.data.id}`), 1000);
    } catch (err: unknown) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      const message = err instanceof Error ? err.message : null;
      setMessage({ type: 'error', text: detail || message || 'Submit failed' });
    } finally {
      setSubmitting(false);
    }
  };

  const handleClearWorkspace = () => {
    if (nodes.length === 0 && edges.length === 0 && !pipelineId && pipelineName === 'Untitled Pipeline') {
      return;
    }

    const confirmed = window.confirm(
      'Clear the current workspace? This will reset the editor canvas and unsaved draft changes, but it will not delete any saved pipeline from the server.',
    );
    if (!confirmed) {
      return;
    }

    clear();
    setMessage({ type: 'success', text: 'Workspace cleared' });
    navigate('/editor', { replace: true });
  };

  const openBatchDialog = async () => {
    const targetPipelineId = isDirty || !pipelineId
      ? await ensureSaved()
      : pipelineId;
    if (!targetPipelineId) {
      return;
    }

    setBatchInputError(null);
    try {
      setBatchInputText(JSON.stringify(buildBatchItems(getDefinition()), null, 2));
      setBatchPipelineId(targetPipelineId);
      setBatchOpen(true);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Failed to prepare batch input';
      setMessage({ type: 'error', text: message });
    }
  };

  const handleBatchSubmit = async () => {
    setSubmittingBatch(true);
    setMessage(null);

    try {
      if (!batchPipelineId) {
        setMessage({ type: 'error', text: 'Save the pipeline first' });
        return;
      }

      const inputs = parseBatchItems(batchInputText);

      const res = await apiClient.post('/jobs/batch', {
        pipeline_id: batchPipelineId,
        inputs,
      });

      const count = Array.isArray(res.data) ? res.data.length : inputs.length;
      setBatchOpen(false);
      setBatchPipelineId(null);
      setBatchInputError(null);
      setMessage({ type: 'success', text: `Submitted ${count} batch jobs` });
      navigate('/jobs');
    } catch (err: unknown) {
      if (err instanceof Error && !isAxiosError(err)) {
        setBatchInputError(err.message);
        return;
      }
      const detail = isAxiosError(err) ? err.response?.data?.detail : undefined;
      setMessage({ type: 'error', text: detail || 'Batch submit failed' });
    } finally {
      setSubmittingBatch(false);
    }
  };

  return (
    <>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        padding: '8px 16px',
        backgroundColor: '#0f172a',
        borderBottom: '1px solid #1e293b',
      }}>
        <input
          value={pipelineName}
          onChange={e => setPipelineName(e.target.value)}
          style={{
            backgroundColor: 'transparent',
            border: 'none',
            borderBottom: '1px solid #334155',
            color: '#e2e8f0',
            fontSize: 16,
            fontWeight: 600,
            padding: '4px 0',
            outline: 'none',
            width: 200,
          }}
        />
        {isDirty && <span style={{ color: '#f59e0b', fontSize: 12 }}>unsaved</span>}

        <div style={{ flex: 1 }} />

        {message && (
          <span style={{
            fontSize: 12,
            color: message.type === 'success' ? '#22c55e' : '#ef4444',
            maxWidth: 400,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {message.text}
          </span>
        )}

        <button onClick={handleValidate} disabled={validating}
          style={btnStyle('#334155')}>
          {validating ? '...' : 'Validate'}
        </button>
        <button onClick={handleSave} disabled={saving}
          style={btnStyle('#334155')}>
          {saving ? '...' : 'Save'}
        </button>
        <button onClick={handleSaveAsTemplate} disabled={savingTemplate}
          style={btnStyle('#7c3aed')}>
          {savingTemplate ? '...' : 'Save as Template'}
        </button>
        <button
          onClick={handleClearWorkspace}
          disabled={saving || validating || submitting || submittingBatch || savingTemplate}
          style={btnStyle('#7f1d1d')}
          title="Clear the current editor workspace without deleting saved pipelines"
        >
          Clear Workspace
        </button>
        <button
          onClick={openBatchDialog}
          disabled={submittingBatch || saving}
          style={btnStyle('#0f766e')}
          title="Submit multiple jobs with parameter dictionaries"
        >
          {submittingBatch ? '...' : 'Batch Run'}
        </button>
        <button onClick={handleRun} disabled={submitting}
          style={btnStyle('#2563eb')}>
          {submitting ? '...' : '▶ Run'}
        </button>
      </div>

      {batchOpen && (
        <BatchExecuteModal
          title="Batch Run"
          description={hasPlannerNodes(getDefinition())
            ? 'Planner nodes generated these batch items from selected search results. You can inspect or edit the JSON before submission.'
            : 'Submit a JSON array of parameter dictionaries to the pipeline batch API.'}
          value={batchInputText}
          submitting={submittingBatch}
          error={batchInputError}
          onChange={setBatchInputText}
          onClose={() => {
            if (submittingBatch) return;
            setBatchOpen(false);
            setBatchPipelineId(null);
            setBatchInputError(null);
          }}
          onSubmit={() => void handleBatchSubmit()}
        />
      )}
    </>
  );
}

function btnStyle(bg: string): React.CSSProperties {
  return {
    padding: '6px 16px',
    backgroundColor: bg,
    color: '#e2e8f0',
    border: 'none',
    borderRadius: 6,
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 500,
  };
}
