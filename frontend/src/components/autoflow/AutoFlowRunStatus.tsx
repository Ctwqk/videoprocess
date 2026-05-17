import { Link } from 'react-router-dom';
import type { AutoFlowRun } from '../../types/autoflow';

function statusColor(status: string) {
  const normalized = status.toLowerCase();
  if (['succeeded', 'published_private', 'published_unlisted'].includes(normalized)) return '#86efac';
  if (['failed', 'cancelled'].includes(normalized)) return '#fca5a5';
  if (['running', 'planning', 'pending', 'queued'].includes(normalized)) return '#93c5fd';
  return '#cbd5e1';
}

export default function AutoFlowRunStatus({
  run,
}: {
  run: AutoFlowRun | null;
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
      <h2 style={{ margin: '0 0 10px', fontSize: 14, color: '#f8fafc' }}>Run Status</h2>
      {!run ? (
        <div style={{ color: '#94a3b8', fontSize: 13 }}>Execute a plan to create a run.</div>
      ) : (
        <div style={{ display: 'grid', gap: 10, fontSize: 13 }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 10 }}>
            <div>
              <div style={{ color: '#64748b', fontSize: 11 }}>Run</div>
              <div style={{ color: '#e2e8f0', wordBreak: 'break-all' }}>{run.run_id}</div>
            </div>
            <div>
              <div style={{ color: '#64748b', fontSize: 11 }}>Status</div>
              <div style={{ color: statusColor(run.status), fontWeight: 700 }}>{run.status}</div>
            </div>
            <div>
              <div style={{ color: '#64748b', fontSize: 11 }}>Pipeline</div>
              <div style={{ color: '#e2e8f0', wordBreak: 'break-all' }}>{run.pipeline_id ?? '-'}</div>
            </div>
            <div>
              <div style={{ color: '#64748b', fontSize: 11 }}>Job</div>
              {run.job_id ? (
                <Link to={`/jobs/${run.job_id}`} style={{ color: '#60a5fa', textDecoration: 'none' }}>
                  {run.job_id}
                </Link>
              ) : (
                <div style={{ color: '#e2e8f0' }}>-</div>
              )}
            </div>
          </div>

          {run.error_message ? (
            <div
              style={{
                padding: '8px 10px',
                borderRadius: 6,
                border: '1px solid #7f1d1d',
                backgroundColor: '#450a0a',
                color: '#fecaca',
                fontSize: 12,
              }}
            >
              {run.error_message}
            </div>
          ) : null}

          {(run.preview_url || run.download_url) ? (
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              {run.preview_url ? (
                <a href={run.preview_url} target="_blank" rel="noreferrer" style={{ color: '#60a5fa' }}>
                  Preview
                </a>
              ) : null}
              {run.download_url ? (
                <a href={run.download_url} target="_blank" rel="noreferrer" style={{ color: '#60a5fa' }}>
                  Download
                </a>
              ) : null}
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}
