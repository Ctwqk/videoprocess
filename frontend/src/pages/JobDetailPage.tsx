import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import apiClient from '../api/client';
import type { Job, NodeExecution } from '../api/types';
import { Badge, Icons, Tag, toneForJobStatus, toneForNodeStatus } from '../components/common/ui';

interface JobDetail extends Job {
  pipeline_snapshot: Record<string, unknown>;
  execution_plan: Record<string, unknown> | null;
  node_executions: NodeExecution[];
}

function getYouTubeOutput(mediaInfo: Record<string, unknown> | null | undefined) {
  if (!mediaInfo || typeof mediaInfo !== 'object') return null;
  const youtube = (mediaInfo as Record<string, unknown>).youtube;
  if (!youtube || typeof youtube !== 'object') return null;
  const record = youtube as Record<string, unknown>;
  const url = typeof record.url === 'string' ? record.url : null;
  const videoId = typeof record.video_id === 'string' ? record.video_id : null;
  const title = typeof record.title === 'string' ? record.title : null;
  const privacy = typeof record.privacy === 'string' ? record.privacy : null;
  if (!url && !videoId) return null;
  return { url, videoId, title, privacy };
}

function fmtDuration(ms: number) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return `${m}:${String(sec).padStart(2, '0')}`;
}

export default function JobDetailPage() {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const [job, setJob] = useState<JobDetail | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchJob = useCallback(() => {
    if (!jobId) return;
    apiClient.get(`/jobs/${jobId}`).then(res => {
      setJob(res.data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [jobId]);

  useEffect(() => {
    fetchJob();
    const interval = setInterval(fetchJob, 3000);
    return () => clearInterval(interval);
  }, [fetchJob]);

  const handleCancel = async () => {
    if (!jobId) return;
    await apiClient.post(`/jobs/${jobId}/cancel`);
    fetchJob();
  };

  const handleDownload = (artifactId: string) => {
    window.open(`/api/v1/artifacts/${artifactId}/download`, '_blank');
  };

  const handleRerun = async () => {
    if (!jobId) return;
    try {
      const res = await apiClient.post(`/jobs/${jobId}/rerun`);
      navigate(`/jobs/${res.data.id}`);
    } catch {
      window.alert('Re-run failed');
    }
  };

  const handleOpenPipeline = () => {
    if (job?.pipeline_id) navigate(`/editor/${job.pipeline_id}`);
  };

  const terminalNodeIds = useMemo(() => {
    const snapshot = job?.pipeline_snapshot as { edges?: { source: string }[] } | null;
    const edgeSources = new Set((snapshot?.edges ?? []).map(e => e.source));
    return new Set(
      (job?.node_executions ?? [])
        .filter(ne => !edgeSources.has(ne.node_id))
        .map(ne => ne.node_id)
    );
  }, [job]);

  const progress = useMemo(() => {
    if (!job?.node_executions?.length) return 0;
    const total = job.node_executions.length;
    const score = job.node_executions.reduce((acc, ne) => {
      if (ne.status === 'SUCCEEDED') return acc + 1;
      if (ne.status === 'RUNNING') return acc + (ne.progress ?? 0) / 100;
      return acc;
    }, 0);
    return Math.round((score / total) * 100);
  }, [job]);

  if (loading) return <div className="muted" style={{ padding: 24 }}>Loading…</div>;
  if (!job) return <div style={{ padding: 24, color: 'var(--status-fail)' }}>Job not found</div>;

  const isTerminal = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'PARTIALLY_FAILED'].includes(job.status);
  const s = toneForJobStatus(job.status);
  const completedCount = job.node_executions.filter(n => n.status === 'SUCCEEDED').length;
  const elapsed = job.started_at
    ? (new Date(job.completed_at ?? new Date().toISOString()).getTime() - new Date(job.started_at).getTime())
    : 0;

  return (
    <div className="vp-page">
      <div style={{ padding: '20px 24px 0' }}>
        <div className="job-detail-toolbar">
          <Link to="/jobs" className="vp-btn vp-btn-sm vp-btn-ghost">
            <Icons.chevron size={12} style={{ transform: 'rotate(180deg)' }} /> Jobs
          </Link>
          <h2 style={{ margin: 0, fontSize: 20, letterSpacing: '-0.02em', fontWeight: 600 }}>
            Job {job.id.slice(0, 8)}
          </h2>
          <Tag>{job.id}</Tag>
          <Badge status={s.tone}>{s.label}</Badge>
          <div className="job-detail-toolbar-spacer" />
          <button type="button" onClick={handleOpenPipeline} className="vp-btn vp-btn-sm">
            <Icons.flow size={12} />Edit pipeline
          </button>
          {isTerminal && (
            <button type="button" onClick={() => void handleRerun()} className="vp-btn vp-btn-sm">
              <Icons.history size={12} />Re-run
            </button>
          )}
          {!isTerminal && (
            <button type="button" onClick={() => void handleCancel()} className="vp-btn vp-btn-sm vp-btn-danger">
              <Icons.x size={12} />Cancel
            </button>
          )}
        </div>

        <div className="vp-card" style={{ padding: '14px 18px', marginBottom: 16 }}>
          <div style={{ flex: 1 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11.5, marginBottom: 6 }}>
              <span className="muted mono" style={{ textTransform: 'uppercase', letterSpacing: '.06em' }}>
                Overall progress
              </span>
              <span className="mono num">
                {progress}%  ·  {completedCount}/{job.node_executions.length} nodes
                {elapsed > 0 && `  ·  elapsed ${fmtDuration(elapsed)}`}
              </span>
            </div>
            <div className={`vp-meter ${s.tone === 'ok' ? 'ok' : s.tone === 'fail' ? 'fail' : 'warn'}`} style={{ height: 8 }}>
              <div style={{ width: `${progress}%` }} />
            </div>
          </div>
          <div className="job-progress-strip">
            {job.node_executions.map(n => {
              const tone = toneForNodeStatus(n.status);
              const colorMap: Record<string, string> = {
                ok: 'var(--status-ok)',
                run: 'var(--status-run)',
                fail: 'var(--status-fail)',
                queue: 'var(--border-2)',
                idle: 'var(--border-2)',
              };
              const bgMap: Record<string, string> = {
                ok: 'var(--status-ok-soft)',
                run: 'var(--status-run-soft)',
                fail: 'var(--status-fail-soft)',
                queue: 'var(--bg-2)',
                idle: 'var(--bg-2)',
              };
              return (
                <div
                  key={n.id}
                  title={n.node_label}
                  className="job-progress-node"
                  style={{
                    background: bgMap[tone],
                    border: `1px solid ${colorMap[tone]}`,
                    color: tone === 'queue' || tone === 'idle' ? 'var(--fg-4)' : colorMap[tone],
                  }}
                >
                  {n.node_label.slice(0, 10)}
                </div>
              );
            })}
          </div>
        </div>

        {job.error_message && (
          <div style={{
            marginBottom: 16, padding: '10px 12px', borderRadius: 8,
            background: 'var(--status-fail-soft)', color: 'var(--status-fail)',
            border: '1px solid var(--status-fail)', fontSize: 13,
          }}>
            <strong>Error:</strong> {job.error_message}
          </div>
        )}
      </div>

      <div className="job-detail-layout">
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
            <h3 style={{ margin: 0, fontSize: 13, fontWeight: 600, color: 'var(--fg-2)' }}>Node executions</h3>
            <Tag>{job.node_executions.length}</Tag>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {job.node_executions.map(ne => {
              const tone = toneForNodeStatus(ne.status);
              const youtubeOutput = getYouTubeOutput(ne.output_artifact_media_info);
              const colorMap: Record<string, string> = {
                ok: 'var(--status-ok)',
                run: 'var(--status-run)',
                fail: 'var(--status-fail)',
                queue: 'var(--fg-5)',
                idle: 'var(--fg-5)',
              };
              const isFinal = ne.status === 'SUCCEEDED' && ne.output_artifact_id && terminalNodeIds.has(ne.node_id);
              return (
                <div
                  key={ne.id}
                  className="vp-card"
                  style={{
                    padding: '12px 14px',
                    background: ne.status === 'RUNNING'
                      ? 'linear-gradient(180deg, var(--status-run-soft), var(--bg-1))'
                      : 'var(--bg-1)',
                    borderColor: ne.status === 'RUNNING' ? 'rgba(245,185,66,0.3)' : 'var(--border-1)',
                  }}
                >
                  <div className="job-execution-row">
                    <div style={{
                      width: 26, height: 26, borderRadius: 999,
                      display: 'grid', placeItems: 'center',
                      fontFamily: 'var(--font-mono)', fontSize: 10.5,
                      border: `1px solid ${colorMap[tone]}`,
                      background: ne.status === 'SUCCEEDED' ? 'var(--status-ok-soft)' :
                                  ne.status === 'RUNNING' ? 'var(--status-run-soft)' :
                                  ne.status === 'FAILED' ? 'var(--status-fail-soft)' : 'var(--bg-2)',
                      color: colorMap[tone],
                    }}>
                      {ne.status === 'SUCCEEDED' ? <Icons.check size={13} /> :
                       ne.status === 'RUNNING' ? <Icons.play size={11} /> :
                       ne.status === 'FAILED' ? <Icons.x size={13} /> : '·'}
                    </div>
                    <div style={{ minWidth: 0 }}>
                      <div className="job-execution-title">
                        <span style={{ fontSize: 13.5, fontWeight: 500 }}>{ne.node_label}</span>
                        <Tag>{ne.node_type}</Tag>
                        {isFinal && <Badge status="ok">FINAL</Badge>}
                      </div>
                      <div className="mono dim" style={{ fontSize: 11.5, marginTop: 2 }}>
                        {ne.output_artifact_filename || ne.node_id}
                      </div>
                      {ne.status === 'RUNNING' && (
                        <div className="vp-meter warn" style={{ marginTop: 8 }}>
                          <div style={{ width: `${ne.progress}%` }} />
                        </div>
                      )}
                      {ne.error_message && (
                        <div style={{ fontSize: 11.5, color: 'var(--status-fail)', marginTop: 6 }}>
                          {ne.error_message.slice(0, 240)}
                        </div>
                      )}
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 4 }}>
                      <Badge status={tone}>{ne.status}</Badge>
                      {isFinal && (
                        <button
                          type="button"
                          onClick={() => handleDownload(ne.output_artifact_id!)}
                          className="vp-btn vp-btn-sm"
                        >
                          <Icons.download size={12} />Download
                        </button>
                      )}
                    </div>
                  </div>

                  {youtubeOutput && ne.status === 'SUCCEEDED' && (
                    <div style={{
                      marginTop: 10, padding: 10, borderRadius: 6,
                      background: 'var(--status-queue-soft)', border: '1px solid var(--status-queue)',
                    }}>
                      <div className="mono" style={{
                        fontSize: 10, color: 'var(--status-queue)', fontWeight: 700,
                        textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 6,
                      }}>
                        YOUTUBE {youtubeOutput.privacy && `· ${youtubeOutput.privacy}`}
                      </div>
                      {youtubeOutput.title && (
                        <div style={{ fontSize: 12, color: 'var(--fg-1)', marginBottom: 4 }}>
                          {youtubeOutput.title}
                        </div>
                      )}
                      {youtubeOutput.url ? (
                        <a href={youtubeOutput.url} target="_blank" rel="noreferrer"
                           style={{ fontSize: 11, color: 'var(--status-queue)', wordBreak: 'break-all' }}>
                          {youtubeOutput.url}
                        </a>
                      ) : (
                        <div className="mono" style={{ fontSize: 11, color: 'var(--fg-3)' }}>
                          Video ID: {youtubeOutput.videoId}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        <aside>
          <div className="vp-card">
            <div className="vp-section-head"><h3>Timing</h3></div>
            <div style={{ padding: '0 20px 18px', display: 'flex', flexDirection: 'column', gap: 8 }}>
              <Row k="submitted" v={new Date(job.submitted_at).toLocaleString()} />
              {job.started_at && <Row k="started" v={new Date(job.started_at).toLocaleString()} />}
              {job.completed_at && <Row k="completed" v={new Date(job.completed_at).toLocaleString()} />}
              {elapsed > 0 && <Row k="elapsed" v={fmtDuration(elapsed)} />}
            </div>
          </div>

          <div className="vp-card" style={{ marginTop: 14 }}>
            <div className="vp-section-head"><h3>Pipeline</h3></div>
            <div style={{ padding: '0 20px 16px', fontSize: 12.5 }}>
              <Row k="pipeline" v={<Link to={`/editor/${job.pipeline_id}`} className="vp-row-link">{job.pipeline_id.slice(0, 8)}…</Link>} />
              <Row k="nodes" v={String(job.node_executions.length)} />
              <Row k="status" v={<Badge status={s.tone}>{s.label}</Badge>} />
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}

function Row({ k, v }: { k: string; v: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center' }}>
      <span className="dim mono" style={{
        fontSize: 10.5, textTransform: 'uppercase', letterSpacing: '.05em',
      }}>
        {k}
      </span>
      <span className="mono" style={{ fontSize: 12 }}>{v}</span>
    </div>
  );
}
