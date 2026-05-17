import { useEffect, useState, type CSSProperties } from 'react';

import {
  createAutoFlowIdeas,
  createAutoFlowTrendSignal,
  listAutoFlowRunMetrics,
  listAutoFlowTemplateMetrics,
  listAutoFlowTrendSuggestions,
} from '../../api/autoflow';
import type {
  AutoFlowIdea,
  AutoFlowMetric,
  AutoFlowRequest,
  AutoFlowRun,
  AutoFlowTemplateMetricSummary,
  AutoFlowTrendSuggestion,
} from '../../types/autoflow';

function percent(value: number | undefined) {
  return `${Math.round((value ?? 0) * 100)}%`;
}

export default function AutoFlowMetricsPanel({
  request,
  run,
  onUseIdea,
}: {
  request: AutoFlowRequest;
  run: AutoFlowRun | null;
  onUseIdea: (prompt: string) => void;
}) {
  const [suggestions, setSuggestions] = useState<AutoFlowTrendSuggestion[]>([]);
  const [ideas, setIdeas] = useState<AutoFlowIdea[]>([]);
  const [templateMetrics, setTemplateMetrics] = useState<AutoFlowTemplateMetricSummary[]>([]);
  const [runMetrics, setRunMetrics] = useState<AutoFlowMetric[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextSuggestions, nextIdeas, nextTemplateMetrics, nextRunMetrics] = await Promise.all([
        listAutoFlowTrendSuggestions({
          source_policy: request.source_policy,
          material_library_ids: request.material_library_ids,
          limit: 4,
        }),
        createAutoFlowIdeas({
          target_platforms: request.target_platforms,
          material_library_ids: request.material_library_ids,
          source_policy: request.source_policy,
          count: 4,
        }),
        listAutoFlowTemplateMetrics(),
        run?.run_id ? listAutoFlowRunMetrics(run.run_id) : Promise.resolve([]),
      ]);
      setSuggestions(nextSuggestions);
      setIdeas(nextIdeas);
      setTemplateMetrics(nextTemplateMetrics);
      setRunMetrics(nextRunMetrics);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load AutoFlow growth data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [request.source_policy, request.material_library_ids.join(','), request.target_platforms.join(','), run?.run_id]);

  const seedTrend = async () => {
    setLoading(true);
    setError(null);
    try {
      await createAutoFlowTrendSignal({
        source: 'manual',
        keyword: 'cat fails',
        score: 0.9,
        trend_growth: 0.8,
        cross_platform_mentions: 0.7,
        material_availability: 0.9,
        competition: 0.2,
        rights_risk: 0.1,
      });
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save trend signal');
      setLoading(false);
    }
  };

  return (
    <section
      style={{
        backgroundColor: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 14,
        display: 'grid',
        gap: 12,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center' }}>
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Growth Loop</h2>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <button type="button" onClick={() => void seedTrend()} disabled={loading} style={buttonStyle('#0f766e')}>
            Add Trend
          </button>
          <button type="button" onClick={() => void refresh()} disabled={loading} style={buttonStyle('#2563eb')}>
            {loading ? 'Loading...' : 'Refresh'}
          </button>
        </div>
      </div>

      {error ? (
        <div style={{ border: '1px solid #7f1d1d', backgroundColor: '#450a0a', color: '#fecaca', borderRadius: 6, padding: 8, fontSize: 12 }}>
          {error}
        </div>
      ) : null}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 220px), 1fr))', gap: 12 }}>
        <div style={panelStyle}>
          <div style={panelTitleStyle}>Trend Suggestions</div>
          {suggestions.length ? suggestions.map(item => (
            <div key={`${item.keyword}-${item.recommended_template}`} style={rowStyle}>
              <div style={{ color: '#e2e8f0', fontWeight: 700 }}>{item.keyword}</div>
              <div style={mutedStyle}>{item.recommended_template} · {percent(item.opportunity_score)}</div>
            </div>
          )) : <div style={mutedStyle}>No trend signals yet.</div>}
        </div>

        <div style={panelStyle}>
          <div style={panelTitleStyle}>Ideas</div>
          {ideas.length ? ideas.map(item => (
            <button
              key={item.idea_id}
              type="button"
              onClick={() => onUseIdea(item.prompt)}
              style={{ ...rowStyle, textAlign: 'left', width: '100%', cursor: 'pointer', backgroundColor: '#020617' }}
            >
              <div style={{ color: '#e2e8f0', fontWeight: 700 }}>{item.prompt}</div>
              <div style={mutedStyle}>{item.template_id} · {percent(item.opportunity_score)} · {item.risk}</div>
            </button>
          )) : <div style={mutedStyle}>No ideas generated.</div>}
        </div>

        <div style={panelStyle}>
          <div style={panelTitleStyle}>Template Metrics</div>
          {templateMetrics.length ? templateMetrics.map(item => (
            <div key={item.template_id} style={rowStyle}>
              <div style={{ color: '#e2e8f0', fontWeight: 700 }}>{item.template_id}</div>
              <div style={mutedStyle}>{item.metric_count} runs · {Math.round(item.avg_views)} avg views · {percent(item.avg_virality_score)}</div>
            </div>
          )) : <div style={mutedStyle}>No metrics imported.</div>}
        </div>

        <div style={panelStyle}>
          <div style={panelTitleStyle}>Run Metrics</div>
          {runMetrics.length ? runMetrics.map(item => (
            <div key={item.metric_id} style={rowStyle}>
              <div style={{ color: '#e2e8f0', fontWeight: 700 }}>{item.platform}</div>
              <div style={mutedStyle}>{item.views} views · {percent(item.like_rate)} like · {percent(item.avg_retention)} retention</div>
            </div>
          )) : <div style={mutedStyle}>{run ? 'No run metrics imported.' : 'Execute a run to attach metrics.'}</div>}
        </div>
      </div>
    </section>
  );
}

const panelStyle = {
  border: '1px solid #1e293b',
  borderRadius: 8,
  padding: 10,
  backgroundColor: '#020617',
  display: 'grid',
  gap: 8,
} satisfies CSSProperties;

const panelTitleStyle = {
  color: '#94a3b8',
  fontSize: 11,
  fontWeight: 700,
  textTransform: 'uppercase',
} satisfies CSSProperties;

const rowStyle = {
  border: '1px solid #1e293b',
  borderRadius: 6,
  padding: 8,
  display: 'grid',
  gap: 4,
} satisfies CSSProperties;

const mutedStyle = {
  color: '#94a3b8',
  fontSize: 12,
} satisfies CSSProperties;

function buttonStyle(color: string): CSSProperties {
  return {
    border: 'none',
    borderRadius: 6,
    padding: '7px 10px',
    backgroundColor: color,
    color: '#fff',
    fontSize: 12,
    fontWeight: 700,
    cursor: 'pointer',
  };
}
