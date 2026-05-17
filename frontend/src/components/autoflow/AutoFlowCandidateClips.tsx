import type { AutoFlowCandidateEditDraft, AutoFlowClipCandidate } from '../../types/autoflow';

const DEFAULT_CANDIDATE_EDIT: AutoFlowCandidateEditDraft = {
  selected: true,
  locked: false,
  replacement: '',
};

function metadataString(candidate: AutoFlowClipCandidate, key: string) {
  const value = candidate.metadata[key];
  return typeof value === 'string' ? value : null;
}

function metadataNumber(candidate: AutoFlowClipCandidate, key: string) {
  const value = candidate.metadata[key];
  return typeof value === 'number' ? value : null;
}

function formatDuration(candidate: AutoFlowClipCandidate) {
  if (typeof candidate.start_sec === 'number' && typeof candidate.end_sec === 'number') {
    return `${Math.max(0, candidate.end_sec - candidate.start_sec).toFixed(1)}s`;
  }
  const duration = metadataNumber(candidate, 'duration_sec') ?? metadataNumber(candidate, 'duration');
  return duration ? `${duration.toFixed(1)}s` : '-';
}

function formatScore(score: number) {
  if (score <= 1) return `${Math.round(score * 100)}%`;
  return `${Math.round(score)}%`;
}

function rightsColor(status: string) {
  const normalized = status.toLowerCase();
  if (['approved', 'allowed', 'clear', 'owned', 'licensed'].includes(normalized)) return '#86efac';
  if (['blocked', 'denied', 'rejected'].includes(normalized)) return '#fca5a5';
  if (['review', 'needs_review', 'unknown'].includes(normalized)) return '#fde68a';
  return '#cbd5e1';
}

export default function AutoFlowCandidateClips({
  candidates,
  candidateEdits,
  onCandidateEditChange,
}: {
  candidates: AutoFlowClipCandidate[];
  candidateEdits: Record<string, AutoFlowCandidateEditDraft>;
  onCandidateEditChange: (candidateId: string, edit: Partial<AutoFlowCandidateEditDraft>) => void;
}) {
  const selectedCount = candidates.filter(candidate => {
    const edit = candidateEdits[candidate.id] ?? DEFAULT_CANDIDATE_EDIT;
    return edit.selected;
  }).length;

  if (candidates.length === 0) {
    return (
      <section
        style={{
          backgroundColor: '#0f172a',
          border: '1px solid #1e293b',
          borderRadius: 8,
          padding: 14,
        }}
      >
        <h2 style={{ margin: '0 0 8px', fontSize: 14, color: '#f8fafc' }}>Candidate Clips</h2>
        <div style={{ color: '#94a3b8', fontSize: 13 }}>No candidates returned yet.</div>
      </section>
    );
  }

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
        <h2 style={{ margin: 0, fontSize: 14, color: '#f8fafc' }}>Candidate Clips</h2>
        <div style={{ fontSize: 12, color: '#94a3b8' }}>
          {selectedCount} / {candidates.length} selected
        </div>
      </div>

      <div style={{ display: 'grid', gap: 10 }}>
        {candidates.map(candidate => {
          const edit = candidateEdits[candidate.id] ?? DEFAULT_CANDIDATE_EDIT;
          const selected = edit.selected;
          const locked = edit.locked;
          const thumbnailUrl = metadataString(candidate, 'thumbnail_url') ?? metadataString(candidate, 'thumbnail');
          const replacementValue = edit.replacement;

          return (
            <div
              key={candidate.id}
              style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 132px) minmax(0, 1fr)',
                gap: 12,
                padding: 10,
                borderRadius: 8,
                border: `1px solid ${selected ? '#334155' : '#1e293b'}`,
                backgroundColor: selected ? '#111827' : '#020617',
                opacity: selected ? 1 : 0.65,
              }}
            >
              <div
                style={{
                  borderRadius: 6,
                  backgroundColor: '#020617',
                  border: '1px solid #1e293b',
                  minHeight: 74,
                  overflow: 'hidden',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                {thumbnailUrl ? (
                  <img
                    src={thumbnailUrl}
                    alt=""
                    style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
                  />
                ) : (
                  <div style={{ fontSize: 11, color: '#64748b' }}>{candidate.source_type}</div>
                )}
              </div>

              <div style={{ minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'start', justifyContent: 'space-between', gap: 10 }}>
                  <div style={{ minWidth: 0 }}>
                    <div style={{ color: '#e2e8f0', fontSize: 13, fontWeight: 700, wordBreak: 'break-word' }}>
                      {candidate.title || candidate.id}
                    </div>
                    <div style={{ marginTop: 4, display: 'flex', flexWrap: 'wrap', gap: 8, fontSize: 11, color: '#94a3b8' }}>
                      <span>{candidate.source_type}</span>
                      <span>{formatDuration(candidate)}</span>
                      <span>{formatScore(candidate.score)}</span>
                      <span style={{ color: rightsColor(candidate.rights_status), fontWeight: 700 }}>
                        {candidate.rights_status}
                      </span>
                    </div>
                  </div>

                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#cbd5e1' }}>
                    <input
                      type="checkbox"
                      checked={selected}
                      disabled={locked}
                      onChange={() => onCandidateEditChange(candidate.id, { selected: !selected })}
                    />
                    Use
                  </label>
                </div>

                {Object.keys(candidate.score_breakdown).length > 0 ? (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 8 }}>
                    {Object.entries(candidate.score_breakdown).map(([key, score]) => (
                      <span
                        key={key}
                        style={{
                          fontSize: 10,
                          color: '#bfdbfe',
                          backgroundColor: '#172554',
                          border: '1px solid #1d4ed8',
                          borderRadius: 4,
                          padding: '2px 5px',
                        }}
                      >
                        {key}: {formatScore(score)}
                      </span>
                    ))}
                  </div>
                ) : null}

                <div style={{ display: 'grid', gridTemplateColumns: 'auto minmax(0, 1fr)', gap: 8, marginTop: 10, alignItems: 'center' }}>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#cbd5e1' }}>
                    <input
                      type="checkbox"
                      checked={locked}
                      onChange={() => onCandidateEditChange(candidate.id, { locked: !locked })}
                    />
                    Lock
                  </label>
                  <input
                    value={replacementValue}
                    onChange={event => onCandidateEditChange(candidate.id, { replacement: event.target.value })}
                    placeholder="Replacement URL or asset ID"
                    style={{
                      borderRadius: 6,
                      border: '1px solid #334155',
                      backgroundColor: '#020617',
                      color: '#e2e8f0',
                      padding: '6px 8px',
                      fontSize: 12,
                    }}
                  />
                </div>

                {candidate.url ? (
                  <a
                    href={candidate.url}
                    target="_blank"
                    rel="noreferrer"
                    style={{ display: 'inline-block', marginTop: 8, color: '#60a5fa', fontSize: 11, wordBreak: 'break-all' }}
                  >
                    {candidate.url}
                  </a>
                ) : candidate.asset_id ? (
                  <div style={{ marginTop: 8, color: '#94a3b8', fontSize: 11 }}>Asset: {candidate.asset_id}</div>
                ) : null}
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
