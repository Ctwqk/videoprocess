import { useState, useEffect } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import apiClient from '../api/client';
import type { Job, NodeExecution } from '../api/types';
import { JOB_STATUS_COLORS } from '../utils/jobStatus';

const NODE_STATUS_COLORS: Record<string, string> = {
  PENDING: '#6b7280',
  QUEUED: '#6b7280',
  RUNNING: '#3b82f6',
  SUCCEEDED: '#22c55e',
  FAILED: '#ef4444',
  SKIPPED: '#94a3b8',
  CANCELLED: '#f59e0b',
};

interface JobDetail extends Job {
  pipeline_snapshot: Record<string, unknown>;
  execution_plan: Record<string, unknown> | null;
  node_executions: NodeExecution[];
}

function getYouTubeOutput(mediaInfo: Record<string, unknown> | null | undefined) {
  if (!mediaInfo || typeof mediaInfo !== 'object') return null;
  const youtube = mediaInfo.youtube;
  if (!youtube || typeof youtube !== 'object') return null;

  const record = youtube as Record<string, unknown>;
  const url = typeof record.url === 'string' ? record.url : null;
  const videoId = typeof record.video_id === 'string' ? record.video_id : null;
  const title = typeof record.title === 'string' ? record.title : null;
  const privacy = typeof record.privacy === 'string' ? record.privacy : null;

  if (!url && !videoId) return null;
  return { url, videoId, title, privacy };
}

export default function JobDetailPage() {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const [job, setJob] = useState<JobDetail | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchJob = () => {
    if (!jobId) return;
    apiClient.get(`/jobs/${jobId}`).then(res => {
      setJob(res.data);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => {
    fetchJob();
    const interval = setInterval(fetchJob, 3000);
    return () => clearInterval(interval);
  }, [jobId]);

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
      alert('Re-run failed');
    }
  };

  const handleOpenPipeline = () => {
    if (job?.pipeline_id) navigate(`/editor/${job.pipeline_id}`);
  };

  if (loading) return <div style={{ padding: 24, color: '#94a3b8' }}>Loading...</div>;
  if (!job) return <div style={{ padding: 24, color: '#ef4444' }}>Job not found</div>;

  const isTerminal = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'PARTIALLY_FAILED'].includes(job.status);

  const snapshot = job.pipeline_snapshot as { edges?: { source: string }[] } | null;
  const edgeSources = new Set((snapshot?.edges || []).map(e => e.source));
  const terminalNodeIds = new Set(
    job.node_executions
      .filter(ne => !edgeSources.has(ne.node_id))
      .map(ne => ne.node_id)
  );

  return (
    <div style={{ padding: 24, color: '#e2e8f0', overflowY: 'auto', height: '100%' }}>
      <div style={{ marginBottom: 16 }}>
        <Link to="/jobs" style={{ color: '#3b82f6', textDecoration: 'none', fontSize: 13 }}>
          ← Back to Jobs
        </Link>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 700, margin: 0 }}>Job {job.id.slice(0, 8)}...</h1>
        <span style={{
          color: JOB_STATUS_COLORS[job.status] || '#6b7280',
          fontWeight: 700,
          fontSize: 14,
          padding: '2px 10px',
          borderRadius: 4,
          backgroundColor: '#1e293b',
        }}>
          {job.status}
        </span>
        {!isTerminal && (
          <button onClick={handleCancel}
            style={{
              padding: '4px 12px', backgroundColor: '#dc2626', color: '#fff',
              border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 12,
            }}>
            Cancel
          </button>
        )}
        {isTerminal && (
          <button onClick={handleRerun}
            style={{
              padding: '4px 12px', backgroundColor: '#2563eb', color: '#fff',
              border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 12,
            }}>
            Re-run
          </button>
        )}
        <button onClick={handleOpenPipeline}
          style={{
            padding: '4px 12px', backgroundColor: '#334155', color: '#e2e8f0',
            border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 12,
          }}>
          Edit Pipeline
        </button>
      </div>

      <div style={{ fontSize: 13, color: '#94a3b8', marginBottom: 24 }}>
        <div>Submitted: {new Date(job.submitted_at).toLocaleString()}</div>
        {job.started_at && <div>Started: {new Date(job.started_at).toLocaleString()}</div>}
        {job.completed_at && <div>Completed: {new Date(job.completed_at).toLocaleString()}</div>}
        {job.error_message && (
          <div style={{ color: '#ef4444', marginTop: 8 }}>Error: {job.error_message}</div>
        )}
      </div>

      <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 12 }}>Node Executions</h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12 }}>
        {job.node_executions.map(ne => {
          const youtubeOutput = getYouTubeOutput(ne.output_artifact_media_info);
          return (
            <div
              key={ne.id}
              style={{
                backgroundColor: '#1e293b',
                border: `1px solid ${NODE_STATUS_COLORS[ne.status] || '#334155'}`,
                borderRadius: 8,
                padding: 12,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>{ne.node_label}</span>
                <span style={{
                  color: NODE_STATUS_COLORS[ne.status],
                  fontSize: 11,
                  fontWeight: 600,
                }}>
                  {ne.status}
                </span>
              </div>
              <div style={{ fontSize: 11, color: '#64748b', marginBottom: 4 }}>{ne.node_type}</div>

              {ne.output_artifact_filename ? (
                <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 6, wordBreak: 'break-all' }}>
                  {ne.output_artifact_filename}
                </div>
              ) : null}

              {ne.status === 'RUNNING' && (
                <div style={{
                  height: 4, backgroundColor: '#334155', borderRadius: 2, marginTop: 8, overflow: 'hidden',
                }}>
                  <div style={{
                    height: '100%',
                    width: `${ne.progress}%`,
                    backgroundColor: '#3b82f6',
                    borderRadius: 2,
                    transition: 'width 0.3s',
                  }} />
                </div>
              )}

              {ne.error_message && (
                <div style={{ fontSize: 11, color: '#ef4444', marginTop: 6, wordBreak: 'break-all' }}>
                  {ne.error_message.slice(0, 150)}
                </div>
              )}

              {youtubeOutput && ne.status === 'SUCCEEDED' ? (
                <div style={{
                  marginTop: 8,
                  padding: 10,
                  borderRadius: 6,
                  backgroundColor: '#172554',
                  border: '1px solid #1d4ed8',
                }}>
                  <div style={{ fontSize: 10, color: '#93c5fd', fontWeight: 700, marginBottom: 6 }}>
                    YOUTUBE
                  </div>
                  {youtubeOutput.title ? (
                    <div style={{ fontSize: 12, color: '#dbeafe', marginBottom: 4 }}>
                      {youtubeOutput.title}
                    </div>
                  ) : null}
                  {youtubeOutput.privacy ? (
                    <div style={{ fontSize: 11, color: '#93c5fd', marginBottom: 6 }}>
                      Privacy: {youtubeOutput.privacy}
                    </div>
                  ) : null}
                  {youtubeOutput.url ? (
                    <a
                      href={youtubeOutput.url}
                      target="_blank"
                      rel="noreferrer"
                      style={{ fontSize: 11, color: '#bfdbfe', wordBreak: 'break-all' }}
                    >
                      {youtubeOutput.url}
                    </a>
                  ) : (
                    <div style={{ fontSize: 11, color: '#bfdbfe' }}>
                      Video ID: {youtubeOutput.videoId}
                    </div>
                  )}
                </div>
              ) : null}

              {ne.status === 'SUCCEEDED' && ne.output_artifact_id && terminalNodeIds.has(ne.node_id) && (
                <div style={{ marginTop: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ fontSize: 10, color: '#22c55e', fontWeight: 700 }}>FINAL</span>
                  <button
                    onClick={() => handleDownload(ne.output_artifact_id!)}
                    style={{
                      padding: '3px 8px', fontSize: 11,
                      backgroundColor: '#166534',
                      color: '#e2e8f0', border: 'none',
                      borderRadius: 4, cursor: 'pointer',
                    }}>
                    Download
                  </button>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
