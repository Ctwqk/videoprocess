import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { isAxiosError } from 'axios';
import apiClient from '../api/client';
import type { Pipeline } from '../api/types';
import BatchExecuteModal, { parseBatchItems } from '../components/batch/BatchExecuteModal';
import { buildBatchItems, hasPlannerNodes } from '../utils/plannerBatch';
import { Icons, Tag } from '../components/common/ui';

function TemplatePreview({ accent }: { accent: string }) {
  const safe = accent.replace(/[^a-z0-9]/gi, '');
  return (
    <svg viewBox="0 0 220 88" style={{ width: '100%', height: '100%', display: 'block' }}>
      <defs>
        <linearGradient id={`g-${safe}`} x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor={accent} stopOpacity="0.18" />
          <stop offset="1" stopColor={accent} stopOpacity="0" />
        </linearGradient>
      </defs>
      <rect width="220" height="88" fill={`url(#g-${safe})`} />
      {[[20, 30], [70, 18], [70, 50], [120, 30], [170, 30]].map(([x, y], i) => (
        <g key={i}>
          <rect x={x} y={y} width="28" height="20" rx="3" fill="var(--bg-3)" stroke="var(--border-2)" />
          <circle cx={x + 3} cy={y + 10} r="1.5" fill={accent} />
        </g>
      ))}
      {([[48, 40, 70, 28], [48, 40, 70, 60], [98, 28, 120, 40], [98, 60, 120, 40], [148, 40, 170, 40]] as const).map(
        ([x1, y1, x2, y2], i) => (
          <path key={i} d={`M${x1},${y1} C${(x1 + x2) / 2},${y1} ${(x1 + x2) / 2},${y2} ${x2},${y2}`}
            fill="none" stroke={accent} strokeWidth="1" strokeOpacity="0.55" />
        )
      )}
    </svg>
  );
}

const ACCENTS = ['var(--acc)', '#60a5fa', '#c084fc', '#fbbf24', '#f87171', '#22c55e'];

export default function TemplatesPage() {
  const [templates, setTemplates] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningBatch, setRunningBatch] = useState(false);
  const [batchTemplate, setBatchTemplate] = useState<Pipeline | null>(null);
  const [batchInputText, setBatchInputText] = useState('');
  const [batchInputError, setBatchInputError] = useState<string | null>(null);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [tagFilter, setTagFilter] = useState<string>('all');
  const [search, setSearch] = useState('');
  const navigate = useNavigate();

  const fetchTemplates = () => {
    apiClient.get('/templates').then(res => {
      setTemplates(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => { fetchTemplates(); }, []);

  const handleUseTemplate = async (templateId: string) => {
    try {
      const res = await apiClient.post(`/pipelines/${templateId}/duplicate`);
      navigate(`/editor/${res.data.id}`);
    } catch {
      window.alert('Failed to create from template');
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
      setBatchInputError(error instanceof Error ? error.message : 'Invalid JSON');
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
      const detail = isAxiosError(error) ? error.response?.data?.detail : null;
      setMessage({ type: 'error', text: detail || 'Batch run failed' });
    } finally {
      setRunningBatch(false);
    }
  };

  const handleDeleteTemplate = async (templateId: string, templateName: string) => {
    if (!window.confirm(`Delete template "${templateName}"?`)) return;
    try {
      await apiClient.delete(`/pipelines/${templateId}`);
      setTemplates(current => current.filter(tpl => tpl.id !== templateId));
    } catch (error) {
      const detail = isAxiosError(error) ? error.response?.data?.detail : null;
      window.alert(detail || 'Failed to delete template');
    }
  };

  const allTags = useMemo(() => {
    const set = new Set<string>();
    templates.forEach(t => (t.template_tags ?? []).forEach(tag => set.add(tag)));
    return Array.from(set);
  }, [templates]);

  const filtered = templates.filter(t => {
    if (tagFilter !== 'all' && !(t.template_tags ?? []).includes(tagFilter)) return false;
    if (search && !t.name.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="vp-page">
      <div style={{ padding: '20px 24px 12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
          <h2 style={{ margin: 0, fontSize: 20, letterSpacing: '-0.02em', fontWeight: 600 }}>Templates</h2>
          <span className="mono dim" style={{ fontSize: 12 }}>·  {templates.length} workflows</span>
          <div style={{ flex: 1 }} />
          <button type="button" className="vp-btn vp-btn-sm" onClick={() => navigate('/editor')}>
            <Icons.plus size={13} />New template
          </button>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
          <button
            type="button"
            className="vp-btn vp-btn-sm"
            onClick={() => setTagFilter('all')}
            style={{
              background: tagFilter === 'all' ? 'var(--acc-soft)' : 'var(--bg-2)',
              borderColor: tagFilter === 'all' ? 'var(--acc)' : 'var(--border-2)',
              color: tagFilter === 'all' ? 'var(--acc)' : 'var(--fg-2)',
            }}
          >
            all
          </button>
          {allTags.map(t => (
            <button
              key={t}
              type="button"
              className="vp-btn vp-btn-sm"
              onClick={() => setTagFilter(t)}
              style={{
                background: tagFilter === t ? 'var(--acc-soft)' : 'var(--bg-2)',
                borderColor: tagFilter === t ? 'var(--acc)' : 'var(--border-2)',
                color: tagFilter === t ? 'var(--acc)' : 'var(--fg-2)',
              }}
            >
              {t}
            </button>
          ))}
          <div style={{ flex: 1 }} />
          <div style={{ position: 'relative' }}>
            <Icons.search size={13} style={{
              position: 'absolute', left: 9, top: '50%', transform: 'translateY(-50%)', color: 'var(--fg-4)',
            }} />
            <input
              className="vp-input"
              placeholder="Search templates…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              style={{ width: 240, paddingLeft: 28 }}
            />
          </div>
        </div>

        {message && (
          <div style={{
            marginBottom: 16, padding: '10px 12px', borderRadius: 8,
            background: message.type === 'success' ? 'var(--status-ok-soft)' : 'var(--status-fail-soft)',
            color: message.type === 'success' ? 'var(--status-ok)' : 'var(--status-fail)',
            border: `1px solid ${message.type === 'success' ? 'var(--status-ok)' : 'var(--status-fail)'}`,
            fontSize: 13,
          }}>
            {message.text}
          </div>
        )}
      </div>

      {loading ? (
        <div className="muted" style={{ padding: 24 }}>Loading…</div>
      ) : filtered.length === 0 ? (
        <div className="vp-empty">
          <div className="ico"><Icons.layers size={22} /></div>
          <div style={{ fontSize: 14, color: 'var(--fg-2)', marginBottom: 4 }}>No templates here yet.</div>
          <div className="muted" style={{ fontSize: 12.5 }}>
            Save a pipeline as a template from the editor.
          </div>
        </div>
      ) : (
        <div style={{
          padding: '0 24px 24px',
          display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 14,
        }}>
          {filtered.map((tpl, idx) => {
            const accent = ACCENTS[idx % ACCENTS.length];
            return (
              <div key={tpl.id} className="vp-card" style={{
                padding: 18, display: 'flex', flexDirection: 'column', gap: 12,
              }}>
                <div style={{
                  height: 88, borderRadius: 6,
                  background: 'var(--bg-2)', border: '1px solid var(--border-2)',
                  overflow: 'hidden',
                }}>
                  <TemplatePreview accent={accent} />
                </div>
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                    <h3 style={{ margin: 0, fontSize: 14, fontWeight: 600, letterSpacing: '-0.005em' }}>
                      {tpl.name}
                    </h3>
                    <div style={{ flex: 1 }} />
                  </div>
                  <p className="muted" style={{
                    fontSize: 12.5, margin: 0, lineHeight: 1.45, textWrap: 'pretty' as const,
                  }}>
                    {tpl.description || 'No description'}
                  </p>
                </div>
                {(tpl.template_tags ?? []).length > 0 && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                    {tpl.template_tags.map(tag => <Tag key={tag}>{tag}</Tag>)}
                  </div>
                )}
                <div style={{
                  display: 'flex', alignItems: 'center', gap: 14,
                  fontSize: 11, fontFamily: 'var(--font-mono)', color: 'var(--fg-4)',
                  paddingTop: 10, borderTop: '1px solid var(--border-1)',
                }}>
                  <span><span className="dim">nodes</span> · {tpl.definition.nodes?.length ?? 0}</span>
                  <span><span className="dim">version</span> · v{tpl.version}</span>
                  <span style={{ marginLeft: 'auto' }} className="dim">
                    {new Date(tpl.updated_at).toLocaleDateString()}
                  </span>
                </div>
                <div style={{ display: 'flex', gap: 6, marginTop: 'auto' }}>
                  <button
                    type="button"
                    onClick={() => void handleUseTemplate(tpl.id)}
                    className="vp-btn vp-btn-sm vp-btn-primary"
                    style={{ flex: 1, justifyContent: 'center' }}
                  >
                    <Icons.flow size={12} />Use template
                  </button>
                  <button
                    type="button"
                    onClick={() => openBatchRun(tpl)}
                    className="vp-btn vp-btn-sm"
                  >
                    <Icons.layers size={12} />Batch
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleDeleteTemplate(tpl.id, tpl.name)}
                    className="vp-btn vp-btn-sm vp-btn-ghost"
                    style={{ color: 'var(--status-fail)' }}
                    title="Delete"
                  >
                    <Icons.trash size={13} />
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {batchTemplate && (
        <BatchExecuteModal
          title={batchTemplate.name}
          description={hasPlannerNodes(batchTemplate.definition)
            ? 'Planner nodes generated these batch items from the template\u2019s saved search selections.'
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
