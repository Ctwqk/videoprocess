import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { isAxiosError } from 'axios';
import apiClient from '../api/client';
import type { Pipeline } from '../api/types';
import BatchExecuteModal, { parseBatchItems } from '../components/batch/BatchExecuteModal';
import { buildBatchItems, hasPlannerNodes } from '../utils/plannerBatch';

export default function TemplatesPage() {
  const [templates, setTemplates] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningBatch, setRunningBatch] = useState(false);
  const [batchTemplate, setBatchTemplate] = useState<Pipeline | null>(null);
  const [batchInputText, setBatchInputText] = useState('');
  const [batchInputError, setBatchInputError] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const navigate = useNavigate();

  const fetchTemplates = () => {
    apiClient.get('/templates').then(res => {
      setTemplates(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => {
    fetchTemplates();
  }, []);

  const handleUseTemplate = async (templateId: string) => {
    try {
      const res = await apiClient.post(`/pipelines/${templateId}/duplicate`);
      navigate(`/editor/${res.data.id}`);
    } catch {
      alert('Failed to create from template');
    }
  };

  const openBatchRun = (template: Pipeline) => {
    setMessage(null);
    setBatchInputError(null);
    try {
      setBatchTemplate(template);
      setBatchInputText(JSON.stringify(buildBatchItems(template.definition), null, 2));
    } catch (error) {
      const text = error instanceof Error ? error.message : 'Failed to build batch input';
      setMessage({ type: 'error', text });
      setBatchTemplate(null);
    }
  };

  const closeBatchRun = () => {
    if (runningBatch) return;
    setBatchTemplate(null);
    setBatchInputText('');
    setBatchInputError(null);
  };

  const handleBatchRun = async () => {
    if (!batchTemplate) return;

    let items: Array<Record<string, unknown>>;
    try {
      items = parseBatchItems(batchInputText);
    } catch (error) {
      const text = error instanceof Error ? error.message : 'Invalid JSON';
      setBatchInputError(text);
      return;
    }

    setRunningBatch(true);
    setMessage(null);
    setBatchInputError(null);
    try {
      const res = await apiClient.post(`/templates/${batchTemplate.id}/execute/batch`, { items });
      const count = Array.isArray(res.data) ? res.data.length : items.length;
      setMessage({ type: 'success', text: `Submitted ${count} jobs` });
      closeBatchRun();
      navigate('/jobs');
    } catch (error) {
      const detail = isAxiosError(error)
        ? error.response?.data?.detail
        : null;
      setMessage({ type: 'error', text: detail || 'Batch run failed' });
    } finally {
      setRunningBatch(false);
    }
  };

  const handleDeleteTemplate = async (templateId: string, templateName: string) => {
    if (!window.confirm(`Delete template "${templateName}"?`)) {
      return;
    }

    try {
      await apiClient.delete(`/pipelines/${templateId}`);
      setTemplates(current => current.filter(tpl => tpl.id !== templateId));
    } catch (error) {
      const detail = isAxiosError(error)
        ? error.response?.data?.detail
        : null;
      alert(detail || 'Failed to delete template');
    }
  };

  return (
    <div
      style={{
        padding: 24,
        color: '#e2e8f0',
        overflowY: 'auto',
        height: '100%',
        backgroundColor: '#020617',
      }}
    >
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Templates</h1>
      {message && (
        <div style={{
          marginBottom: 16,
          padding: '10px 12px',
          borderRadius: 8,
          backgroundColor: message.type === 'success' ? '#052e16' : '#450a0a',
          color: message.type === 'success' ? '#86efac' : '#fca5a5',
          border: `1px solid ${message.type === 'success' ? '#166534' : '#7f1d1d'}`,
          fontSize: 13,
        }}>
          {message.text}
        </div>
      )}

      {loading ? (
        <div style={{ color: '#94a3b8' }}>Loading...</div>
      ) : templates.length === 0 ? (
        <div style={{ color: '#94a3b8' }}>
          No templates yet. Save a pipeline as a template from the editor.
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 16 }}>
          {templates.map(tpl => (
            <div
              key={tpl.id}
              style={{
                backgroundColor: '#1e293b',
                borderRadius: 8,
                padding: 16,
                border: '1px solid #334155',
              }}
            >
              <div style={{ fontSize: 15, fontWeight: 600, marginBottom: 8 }}>{tpl.name}</div>
              <div style={{ fontSize: 12, color: '#94a3b8', marginBottom: 12 }}>
                {tpl.description || 'No description'}
              </div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 8 }}>
                {tpl.definition.nodes?.length || 0} nodes · v{tpl.version}
              </div>
              {tpl.template_tags?.length > 0 && (
                <div style={{ marginBottom: 12, display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                  {tpl.template_tags.map(tag => (
                    <span key={tag} style={{
                      fontSize: 11, padding: '2px 6px', backgroundColor: '#334155',
                      borderRadius: 4, color: '#94a3b8',
                    }}>
                      {tag}
                    </span>
                  ))}
                </div>
              )}
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                <button
                  onClick={() => handleUseTemplate(tpl.id)}
                  style={{
                    padding: '6px 16px',
                    backgroundColor: '#2563eb',
                    color: '#fff',
                    border: 'none',
                    borderRadius: 6,
                    cursor: 'pointer',
                    fontSize: 13,
                    fontWeight: 500,
                  }}
                >
                  Use Template
                </button>
                <button
                  onClick={() => openBatchRun(tpl)}
                  style={{
                    padding: '6px 16px',
                    backgroundColor: '#0f766e',
                    color: '#ccfbf1',
                    border: '1px solid #115e59',
                    borderRadius: 6,
                    cursor: 'pointer',
                    fontSize: 13,
                    fontWeight: 500,
                  }}
                >
                  Batch Run
                </button>
                <button
                  onClick={() => void handleDeleteTemplate(tpl.id, tpl.name)}
                  style={{
                    padding: '6px 16px',
                    backgroundColor: '#7f1d1d',
                    color: '#fecaca',
                    border: '1px solid #991b1b',
                    borderRadius: 6,
                    cursor: 'pointer',
                    fontSize: 13,
                    fontWeight: 500,
                  }}
                >
                  Delete
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {batchTemplate && (
        <BatchExecuteModal
          title={batchTemplate.name}
          description={hasPlannerNodes(batchTemplate.definition)
            ? 'Planner nodes generated these batch items from the template’s saved search selections.'
            : 'Submit a JSON array of parameter dictionaries to the template batch API.'}
          value={batchInputText}
          submitting={runningBatch}
          error={batchInputError}
          onChange={setBatchInputText}
          onClose={closeBatchRun}
          onSubmit={() => void handleBatchRun()}
        />
      )}
    </div>
  );
}
