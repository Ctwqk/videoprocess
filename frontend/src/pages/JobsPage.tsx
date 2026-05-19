import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';
import type { Job } from '../api/types';
import { Badge, Icons, toneForJobStatus } from '../components/common/ui';

type FilterKey = 'all' | 'running' | 'pending' | 'succeeded' | 'failed';

const FILTER_LABELS: Array<[FilterKey, string]> = [
  ['all', 'All'],
  ['running', 'Running'],
  ['pending', 'Queued'],
  ['succeeded', 'Succeeded'],
  ['failed', 'Failed'],
];

function durationFor(job: Job): string {
  if (job.completed_at && job.started_at) {
    const ms = new Date(job.completed_at).getTime() - new Date(job.started_at).getTime();
    return `${(ms / 1000).toFixed(1)}s`;
  }
  if (job.status === 'RUNNING') return 'running…';
  return '—';
}

function timeAgo(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.round(h / 24);
  return `${d}d ago`;
}

function Stat({ label, value, tone, sub }: { label: string; value: number; tone: string; sub: string }) {
  const toneColor: Record<string, string> = {
    ok: 'var(--status-ok)',
    run: 'var(--status-run)',
    fail: 'var(--status-fail)',
    queue: 'var(--status-queue)',
  };
  return (
    <div className="vp-card" style={{ padding: '14px 18px' }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: 'var(--fg-4)',
        textTransform: 'uppercase', letterSpacing: '.08em', fontFamily: 'var(--font-mono)',
      }}>
        <span style={{ width: 5, height: 5, borderRadius: 99, background: toneColor[tone] }} />
        {label}
      </div>
      <div style={{
        fontSize: 28, fontWeight: 600, letterSpacing: '-0.02em', marginTop: 6,
        fontVariantNumeric: 'tabular-nums',
      }}>
        {value}
      </div>
      <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>{sub}</div>
    </div>
  );
}

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<FilterKey>('all');

  const fetchJobs = () => {
    apiClient.get('/jobs').then(res => {
      setJobs(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleDelete = async (jobId: string) => {
    if (!window.confirm(`Delete job ${jobId.slice(0, 8)}…?`)) return;
    try {
      await apiClient.delete(`/jobs/${jobId}`);
      setJobs(current => current.filter(j => j.id !== jobId));
    } catch {
      window.alert('Failed to delete job');
    }
  };

  const stats = useMemo(() => ({
    running: jobs.filter(j => j.status === 'RUNNING').length,
    queued: jobs.filter(j => j.status === 'PENDING').length,
    ok: jobs.filter(j => j.status === 'SUCCEEDED').length,
    fail: jobs.filter(j => ['FAILED', 'PARTIALLY_FAILED', 'CANCELLED'].includes(j.status)).length,
  }), [jobs]);

  const filtered = jobs.filter(j => {
    if (filter === 'all') return true;
    if (filter === 'failed') return ['FAILED', 'PARTIALLY_FAILED', 'CANCELLED'].includes(j.status);
    return j.status.toLowerCase() === filter;
  });

  return (
    <div className="vp-page">
      <div style={{ padding: '20px 24px 0' }}>
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 18,
        }}>
          <Stat label="Running"           value={stats.running} tone="run"   sub="auto-refresh · 5s" />
          <Stat label="Queued"            value={stats.queued}  tone="queue" sub="awaiting GPU" />
          <Stat label="Succeeded"         value={stats.ok}      tone="ok"    sub="lifetime" />
          <Stat label="Failed / partial"  value={stats.fail}    tone="fail"  sub="needs review" />
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
          <div style={{
            display: 'flex', gap: 4, padding: 3,
            background: 'var(--bg-1)', border: '1px solid var(--border-1)', borderRadius: 7,
          }}>
            {FILTER_LABELS.map(([k, l]) => {
              const count = k === 'all' ? jobs.length :
                            k === 'failed' ? stats.fail :
                            k === 'running' ? stats.running :
                            k === 'pending' ? stats.queued :
                            stats.ok;
              return (
                <button
                  key={k}
                  type="button"
                  className="vp-btn vp-btn-sm"
                  onClick={() => setFilter(k)}
                  style={{
                    background: filter === k ? 'var(--bg-3)' : 'transparent',
                    border: '1px solid ' + (filter === k ? 'var(--border-2)' : 'transparent'),
                    color: filter === k ? 'var(--fg-1)' : 'var(--fg-3)',
                  }}
                >
                  {l} <span className="mono dim" style={{ marginLeft: 6, fontSize: 10.5 }}>{count}</span>
                </button>
              );
            })}
          </div>
          <div style={{ flex: 1 }} />
          <button type="button" className="vp-btn vp-btn-sm">
            <Icons.history size={12} />Auto‑refresh · 5s
          </button>
        </div>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', padding: '0 24px 24px' }}>
        {loading ? (
          <div className="muted" style={{ padding: 24 }}>Loading…</div>
        ) : filtered.length === 0 ? (
          <div className="vp-empty">
            <div className="ico"><Icons.play size={22} /></div>
            <div style={{ fontSize: 14, color: 'var(--fg-2)', marginBottom: 4 }}>No jobs yet.</div>
            <div className="muted" style={{ fontSize: 12.5 }}>
              Run an AutoFlow plan or click <strong>Run</strong> on a pipeline in the editor.
            </div>
          </div>
        ) : (
          <table className="vp-table">
            <thead>
              <tr>
                <th style={{ width: 130 }}>Job ID</th>
                <th style={{ width: 120 }}>Status</th>
                <th>Submitted</th>
                <th style={{ width: 110 }}>Duration</th>
                <th style={{ width: 80 }} className="actions">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(job => {
                const s = toneForJobStatus(job.status);
                const isTerminal = !['PENDING', 'PLANNING', 'VALIDATING', 'RUNNING'].includes(job.status);
                return (
                  <tr key={job.id}>
                    <td className="id">
                      <Link className="vp-row-link" to={`/jobs/${job.id}`}>
                        {job.id.slice(0, 8)}…
                      </Link>
                    </td>
                    <td><Badge status={s.tone}>{s.label}</Badge></td>
                    <td className="muted mono" style={{ fontSize: 12 }}>
                      {timeAgo(job.submitted_at)}
                      <span className="dim" style={{ marginLeft: 8 }}>
                        {new Date(job.submitted_at).toLocaleString()}
                      </span>
                    </td>
                    <td className="muted mono" style={{ fontSize: 12 }}>{durationFor(job)}</td>
                    <td className="actions">
                      <Link to={`/jobs/${job.id}`} className="vp-btn vp-btn-sm vp-btn-ghost">View</Link>
                      {isTerminal && (
                        <button
                          type="button"
                          onClick={() => void handleDelete(job.id)}
                          className="vp-btn vp-btn-sm vp-btn-ghost"
                          style={{ color: 'var(--status-fail)' }}
                        >
                          <Icons.trash size={12} />
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
