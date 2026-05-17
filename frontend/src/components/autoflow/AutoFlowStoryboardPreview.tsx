import type { StoryboardPlan } from '../../types/autoflow';

function formatSeconds(value: number | null | undefined) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '-';
  return `${Math.round(value * 10) / 10}s`;
}

function statusColor(status: string) {
  if (status === 'matched') return '#86efac';
  if (status === 'missing') return '#fca5a5';
  if (status === 'generated') return '#93c5fd';
  return '#fde68a';
}

export default function AutoFlowStoryboardPreview({
  storyboard,
}: {
  storyboard: StoryboardPlan | null | undefined;
}) {
  return (
    <section
      style={{
        backgroundColor: '#0f172a',
        border: '1px solid #1e293b',
        borderRadius: 8,
        padding: 14,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 12 }}>
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Storyboard</h2>
        {storyboard ? (
          <div style={{ fontSize: 12, color: '#94a3b8' }}>
            {storyboard.shots.length} shots · {formatSeconds(storyboard.total_duration)}
          </div>
        ) : null}
      </div>

      {!storyboard ? (
        <div style={{ color: '#94a3b8', fontSize: 13 }}>Storyboard details will appear when the plan uses storyboard mode.</div>
      ) : (
        <div style={{ display: 'grid', gap: 10 }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 10 }}>
            <div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Subject</div>
              <div style={{ fontSize: 13, color: '#e2e8f0' }}>{storyboard.subject}</div>
            </div>
            <div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Strategy</div>
              <div style={{ fontSize: 13, color: '#e2e8f0' }}>{storyboard.source_strategy}</div>
            </div>
            <div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Aspect</div>
              <div style={{ fontSize: 13, color: '#e2e8f0' }}>{storyboard.aspect_ratio}</div>
            </div>
          </div>

          {storyboard.shots.map(shot => (
            <article
              key={shot.id}
              style={{
                border: '1px solid #1e293b',
                borderRadius: 8,
                padding: 12,
                backgroundColor: '#020617',
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'start' }}>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: 12, color: '#60a5fa', fontWeight: 700 }}>{shot.id} · {shot.role}</div>
                  <div style={{ marginTop: 6, color: '#e2e8f0', fontSize: 13, lineHeight: 1.45 }}>
                    {shot.description}
                  </div>
                </div>
                <div style={{ color: statusColor(shot.match_status), fontSize: 12, fontWeight: 700, whiteSpace: 'nowrap' }}>
                  {shot.match_status}
                </div>
              </div>

              <div style={{ marginTop: 10, display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 8 }}>
                <div>
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Query</div>
                  <div style={{ fontSize: 12, color: '#cbd5e1' }}>{shot.search_query}</div>
                </div>
                <div>
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Duration</div>
                  <div style={{ fontSize: 12, color: '#cbd5e1' }}>{formatSeconds(shot.target_duration)}</div>
                </div>
                <div>
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Matched Range</div>
                  <div style={{ fontSize: 12, color: '#cbd5e1' }}>
                    {shot.matched_start_sec != null && shot.matched_end_sec != null
                      ? `${formatSeconds(shot.matched_start_sec)} - ${formatSeconds(shot.matched_end_sec)}`
                      : '-'}
                  </div>
                </div>
              </div>

              {shot.generation.prompt ? (
                <div style={{ marginTop: 10 }}>
                  <div style={{ fontSize: 11, color: '#64748b', marginBottom: 3 }}>Generation Prompt</div>
                  <div style={{ fontSize: 12, color: '#94a3b8', lineHeight: 1.45 }}>
                    {shot.generation.prompt}
                  </div>
                </div>
              ) : null}
            </article>
          ))}

          {storyboard.warnings.length > 0 ? (
            <div style={{ display: 'grid', gap: 6 }}>
              {storyboard.warnings.map(warning => (
                <div key={warning} style={{ color: '#fde68a', fontSize: 12 }}>
                  {warning}
                </div>
              ))}
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}
