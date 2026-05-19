import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  fetchChannelOverview,
  fetchChannels,
  haltChannel,
  resumeChannel,
  setDryRun,
  type AccountHealth,
  type ChannelOverview,
  type ChannelProfile,
  type ChannelPublication,
  type ChannelQueueItem,
  type ChannelTask,
  type LaneHealth,
} from '../api/channelAgent';
import { Badge, Icons, Tag, type StatusTone } from '../components/common/ui';

type TabKey = 'queue' | 'tasks' | 'pubs';

const STATE_TONE: Record<string, StatusTone> = {
  active: 'ok', running: 'run', completed: 'ok', succeeded: 'ok',
  queued: 'queue', claimed: 'queue', pending: 'queue',
  failed: 'fail', dead_lettered: 'fail',
  held: 'run', paused: 'run', uploaded_private: 'run',
  token_invalid: 'fail',
  scheduled: 'ok', published: 'ok',
  disabled: 'idle',
};

function tone(value: string | null | undefined): StatusTone {
  if (!value) return 'idle';
  return STATE_TONE[value.toLowerCase()] ?? 'idle';
}

function liveLoopStatus(measuredAt: string | null | undefined): { tone: StatusTone; label: string } {
  if (!measuredAt) return { tone: 'fail', label: 'NO MEASURED LOOP' };
  const measuredMs = Date.parse(measuredAt);
  if (!Number.isFinite(measuredMs)) return { tone: 'fail', label: 'MEASURED UNKNOWN' };
  const ageHours = (Date.now() - measuredMs) / 3_600_000;
  if (ageHours > 48) return { tone: 'fail', label: `MEASURED ${Math.floor(ageHours)}H AGO` };
  if (ageHours > 24) return { tone: 'run', label: `MEASURED ${Math.floor(ageHours)}H AGO` };
  if (ageHours >= 1) return { tone: 'ok', label: `MEASURED ${Math.floor(ageHours)}H AGO` };
  return { tone: 'ok', label: 'MEASURED <1H AGO' };
}

function Kpi({ label, value, toneName, sub }: {
  label: string; value: number; toneName: StatusTone; sub: string;
}) {
  const c: Record<StatusTone, string> = {
    ok: 'var(--status-ok)',
    run: 'var(--status-run)',
    fail: 'var(--status-fail)',
    queue: 'var(--status-queue)',
    idle: 'var(--fg-5)',
  };
  return (
    <div className="vp-card" style={{ padding: '14px 18px' }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: 'var(--fg-4)',
        textTransform: 'uppercase', letterSpacing: '.08em', fontFamily: 'var(--font-mono)',
      }}>
        <span style={{ width: 5, height: 5, borderRadius: 99, background: c[toneName] }} />
        {label}
      </div>
      <div style={{
        fontSize: 28, fontWeight: 600, letterSpacing: '-0.02em', marginTop: 6,
        fontVariantNumeric: 'tabular-nums',
      }}>{value}</div>
      <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>{sub}</div>
    </div>
  );
}

function StatPanel({ title, count, children, action }: {
  title: string; count?: ReactNode; children: ReactNode; action?: ReactNode;
}) {
  return (
    <div className="vp-card">
      <div className="vp-section-head">
        <h3>{title}</h3>
        {count !== undefined && <span className="vp-count">{count}</span>}
        <div className="vp-spacer" />
        {action}
      </div>
      {children}
    </div>
  );
}

export default function ChannelOpsStatusPage() {
  const [channels, setChannels] = useState<ChannelProfile[]>([]);
  const [selectedChannelId, setSelectedChannelId] = useState('');
  const [overview, setOverview] = useState<ChannelOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>('queue');

  const selectedChannel = useMemo(
    () => channels.find(channel => channel.id === selectedChannelId) ?? null,
    [channels, selectedChannelId],
  );

  const loadChannels = useCallback(async () => {
    const data = await fetchChannels();
    setChannels(data);
    setSelectedChannelId(current => current || data[0]?.id || '');
    return data;
  }, []);

  const loadOverview = useCallback(async (channelId: string) => {
    if (!channelId) { setOverview(null); return; }
    const data = await fetchChannelOverview(channelId);
    setOverview(data);
  }, []);

  const refresh = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const loadedChannels = await loadChannels();
      const channelId = selectedChannelId || loadedChannels[0]?.id || '';
      await loadOverview(channelId);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load ChannelOps status');
    } finally {
      setLoading(false);
    }
  }, [loadChannels, loadOverview, selectedChannelId]);

  useEffect(() => {
    setLoading(true);
    void loadChannels()
      .catch(err => setError(err instanceof Error ? err.message : 'Failed to load channels'))
      .finally(() => setLoading(false));
  }, [loadChannels]);

  useEffect(() => {
    if (!selectedChannelId) return;
    setLoading(true);
    setError(null);
    void loadOverview(selectedChannelId)
      .catch(err => setError(err instanceof Error ? err.message : 'Failed to load overview'))
      .finally(() => setLoading(false));
  }, [loadOverview, selectedChannelId]);

  useEffect(() => {
    if (!selectedChannelId) return;
    const interval = window.setInterval(() => {
      void loadOverview(selectedChannelId).catch(() => undefined);
    }, 10000);
    return () => window.clearInterval(interval);
  }, [loadOverview, selectedChannelId]);

  const reloadAfterControl = async (operation: () => Promise<ChannelProfile>) => {
    setBusy('control');
    setError(null);
    try {
      const updated = await operation();
      setChannels(current => current.map(c => c.id === updated.id ? updated : c));
      await loadOverview(updated.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Channel control failed');
    } finally {
      setBusy(null);
    }
  };

  const handleDryRunToggle = () => {
    if (!selectedChannel) return;
    void reloadAfterControl(() => setDryRun(selectedChannel.id, !selectedChannel.dry_run));
  };

  const handleHaltToggle = () => {
    if (!selectedChannel) return;
    if (selectedChannel.halted_at) {
      void reloadAfterControl(() => resumeChannel(selectedChannel.id));
      return;
    }
    void reloadAfterControl(() => haltChannel(selectedChannel.id, 'operator_halt'));
  };

  const funnelEntries = useMemo(() => {
    const funnel = overview?.funnel ?? {};
    const entries = Object.entries(funnel);
    const max = Math.max(1, ...entries.map(([, value]) => value));
    return entries.map(([label, value], i) => ({
      label, value,
      pct: (value / max) * 100,
      drop: i > 0 ? ((entries[i - 1][1] - value) / entries[i - 1][1]) * 100 : 0,
    }));
  }, [overview]);

  const loopStatus = useMemo(
    () => liveLoopStatus(overview?.health.last_successful_measured_at),
    [overview],
  );

  if (channels.length === 0 && !loading) {
    return (
      <div className="vp-empty" style={{ flex: 1 }}>
        <div className="ico"><Icons.branch size={22} /></div>
        <div style={{ fontSize: 14, color: 'var(--fg-2)', marginBottom: 4 }}>No ChannelOps profiles yet.</div>
        <div className="muted" style={{ fontSize: 12.5 }}>Create a profile to begin automated publishing.</div>
      </div>
    );
  }

  return (
    <div className="vp-page">
      <div style={{ padding: '20px 24px 0' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600, letterSpacing: '-0.02em' }}>ChannelOps</h2>
          {selectedChannel && (
            <span className="muted mono" style={{ fontSize: 12 }}>
              · {selectedChannel.name} · config v{selectedChannel.config_version}
            </span>
          )}
          <div style={{ flex: 1 }} />
          <select
            className="vp-input"
            value={selectedChannelId}
            onChange={e => setSelectedChannelId(e.target.value)}
            disabled={channels.length === 0}
            style={{ width: 220 }}
          >
            {channels.length === 0
              ? <option value="">No channels</option>
              : channels.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
          <button type="button" onClick={() => void refresh()} disabled={loading}
                  className="vp-btn vp-btn-sm">
            <Icons.history size={12} />Refresh
          </button>
          {selectedChannel && (
            <>
              <button
                type="button"
                onClick={handleDryRunToggle}
                disabled={busy === 'control'}
                className="vp-btn vp-btn-sm"
              >
                {selectedChannel.dry_run ? <Icons.play size={12} /> : <Icons.pause size={12} />}
                {selectedChannel.dry_run ? 'Enable live' : 'Dry-run'}
              </button>
              <button
                type="button"
                onClick={handleHaltToggle}
                disabled={busy === 'control'}
                className={`vp-btn vp-btn-sm ${selectedChannel.halted_at ? 'vp-btn-primary' : 'vp-btn-danger'}`}
              >
                {selectedChannel.halted_at ? <Icons.play size={12} /> : <Icons.x size={12} />}
                {selectedChannel.halted_at ? 'Resume' : 'Halt'}
              </button>
            </>
          )}
        </div>

        {selectedChannel && (
          <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
            <Badge status={selectedChannel.dry_run ? 'run' : 'ok'}>
              {selectedChannel.dry_run ? 'DRY-RUN' : 'LIVE'}
            </Badge>
            <Badge status={selectedChannel.halted_at ? 'fail' : 'ok'}>
              {selectedChannel.halted_at ? 'HALTED' : 'RUNNING'}
            </Badge>
            <Badge status={loopStatus.tone}>{loopStatus.label}</Badge>
            {selectedChannel.halt_reason && (
              <span className="muted mono" style={{ fontSize: 12 }}>
                reason: {selectedChannel.halt_reason}
              </span>
            )}
          </div>
        )}

        {error && (
          <div style={{
            marginBottom: 14, padding: '10px 12px', borderRadius: 8,
            background: 'var(--status-fail-soft)', color: 'var(--status-fail)',
            border: '1px solid var(--status-fail)', fontSize: 13,
          }}>
            {error}
          </div>
        )}

        {overview && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 18 }}>
            <Kpi label="Active tasks"    value={overview.health.active_tasks}  toneName="queue" sub="claimed + running" />
            <Kpi label="Queued items"    value={overview.health.queued_items}  toneName="idle"  sub="awaiting claim" />
            <Kpi label="Recent failures" value={overview.health.recent_failures}
                 toneName={overview.health.recent_failures > 0 ? 'fail' : 'ok'} sub="last 24h" />
            <Kpi label="Warnings"        value={overview.health.warnings.length}
                 toneName={overview.health.warnings.length > 0 ? 'run' : 'ok'} sub="needs attention" />
          </div>
        )}
      </div>

      {!overview ? (
        <div className="muted" style={{ padding: 24 }}>{loading ? 'Loading…' : 'No data'}</div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: 18, padding: '0 24px 24px' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <StatPanel title="7-day funnel" count={selectedChannel?.name}>
              <div style={{ padding: '4px 20px 20px' }}>
                {funnelEntries.length === 0 ? (
                  <div className="muted" style={{ fontSize: 12 }}>No funnel data</div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                    {funnelEntries.map((f, i) => (
                      <div key={f.label} style={{
                        display: 'grid', gridTemplateColumns: '150px 1fr 80px',
                        gap: 14, alignItems: 'center',
                      }}>
                        <span className="mono dim" style={{
                          fontSize: 11, textTransform: 'uppercase', letterSpacing: '.05em',
                        }}>{f.label}</span>
                        <div style={{
                          position: 'relative', height: 26,
                          background: 'var(--bg-2)', borderRadius: 4, overflow: 'hidden',
                        }}>
                          <div style={{
                            position: 'absolute', left: 0, top: 0, bottom: 0,
                            width: `${Math.max(4, f.pct)}%`,
                            background: 'linear-gradient(90deg, var(--acc) 0%, var(--acc-2) 100%)',
                            opacity: 0.85 - i * 0.08,
                          }} />
                          <span style={{
                            position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)',
                            fontSize: 11.5, fontFamily: 'var(--font-mono)',
                            color: 'var(--acc-fg)', fontWeight: 600, mixBlendMode: 'screen',
                          }}>{f.pct.toFixed(0)}%</span>
                        </div>
                        <div style={{ textAlign: 'right' }}>
                          <span className="mono" style={{ fontVariantNumeric: 'tabular-nums' }}>{f.value}</span>
                          {f.drop > 0 && (
                            <span className="muted mono" style={{ fontSize: 10.5, marginLeft: 6 }}>
                              −{f.drop.toFixed(0)}%
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </StatPanel>

            <div className="vp-card">
              <div className="vp-section-head">
                <h3>Work</h3>
                <div className="vp-spacer" />
                <div style={{
                  display: 'flex', gap: 2, padding: 3,
                  background: 'var(--bg-2)', border: '1px solid var(--border-1)', borderRadius: 7,
                }}>
                  {(['queue', 'tasks', 'pubs'] as TabKey[]).map(k => {
                    const labels: Record<TabKey, string> = { queue: 'Queue', tasks: 'Tasks', pubs: 'Publications' };
                    const counts: Record<TabKey, number> = {
                      queue: overview.queue.length,
                      tasks: overview.tasks.length,
                      pubs: overview.publications.length,
                    };
                    return (
                      <button
                        key={k}
                        type="button"
                        onClick={() => setTab(k)}
                        className="vp-btn vp-btn-sm"
                        style={{
                          background: tab === k ? 'var(--bg-3)' : 'transparent',
                          border: '1px solid ' + (tab === k ? 'var(--border-2)' : 'transparent'),
                          color: tab === k ? 'var(--fg-1)' : 'var(--fg-3)',
                        }}
                      >
                        {labels[k]}
                        <span className="mono dim" style={{ marginLeft: 6, fontSize: 10.5 }}>{counts[k]}</span>
                      </button>
                    );
                  })}
                </div>
              </div>

              {tab === 'queue' && <QueueTable items={overview.queue} />}
              {tab === 'tasks' && <TasksTable items={overview.tasks} />}
              {tab === 'pubs' && <PublicationsTable items={overview.publications} />}
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {overview.health.warnings.length > 0 && (
              <StatPanel title="Warnings" count={overview.health.warnings.length}>
                <div style={{ padding: '0 20px 16px', display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {overview.health.warnings.map(w => (
                    <div key={w} style={{
                      display: 'flex', gap: 10, padding: '10px 12px', borderRadius: 6,
                      background: 'var(--status-run-soft)',
                      border: '1px solid var(--status-run)',
                      fontSize: 12.5,
                    }}>
                      <Icons.spark size={14} style={{ color: 'var(--status-run)', marginTop: 1 }} />
                      <div style={{ flex: 1 }}>{w}</div>
                    </div>
                  ))}
                </div>
              </StatPanel>
            )}

            <StatPanel title="Lanes" count={overview.lanes.length}>
              <div style={{ padding: '0 20px 18px', display: 'flex', flexDirection: 'column', gap: 8 }}>
                {overview.lanes.length === 0
                  ? <div className="muted" style={{ fontSize: 12 }}>No lanes</div>
                  : overview.lanes.map(l => <LaneRow key={l.lane_id} lane={l} />)}
              </div>
            </StatPanel>

            <StatPanel title="Accounts" count={overview.accounts.length}>
              <div style={{ padding: '0 20px 18px', display: 'flex', flexDirection: 'column', gap: 8 }}>
                {overview.accounts.length === 0
                  ? <div className="muted" style={{ fontSize: 12 }}>No accounts</div>
                  : overview.accounts.map(a => <AccountRow key={a.id} account={a} />)}
              </div>
            </StatPanel>
          </div>
        </div>
      )}
    </div>
  );
}

function QueueTable({ items }: { items: ChannelQueueItem[] }) {
  if (items.length === 0) return <div className="muted" style={{ padding: 20 }}>Queue is empty</div>;
  return (
    <table className="vp-table">
      <thead><tr>
        <th style={{ width: 110 }}>ID</th>
        <th style={{ width: 110 }}>Kind</th>
        <th>Payload</th>
        <th style={{ width: 70 }}>Prio</th>
        <th style={{ width: 110 }}>Status</th>
        <th style={{ width: 70 }}>Attempts</th>
      </tr></thead>
      <tbody>
        {items.map(q => (
          <tr key={q.id}>
            <td className="id">{q.id.slice(0, 8)}</td>
            <td><Tag>{q.kind}</Tag></td>
            <td className="muted" style={{ fontSize: 12, fontFamily: 'var(--font-mono)' }}>
              {q.idempotency_key.slice(0, 60)}
            </td>
            <td className="mono">{q.priority}</td>
            <td><Badge status={tone(q.status)}>{q.status}</Badge></td>
            <td className="mono dim">{q.attempt_count}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TasksTable({ items }: { items: ChannelTask[] }) {
  if (items.length === 0) return <div className="muted" style={{ padding: 20 }}>No tasks</div>;
  return (
    <table className="vp-table">
      <thead><tr>
        <th style={{ width: 110 }}>ID</th>
        <th>Prompt</th>
        <th style={{ width: 130 }}>State</th>
        <th style={{ width: 130 }}>Guard</th>
      </tr></thead>
      <tbody>
        {items.map(t => (
          <tr key={t.id}>
            <td className="id">{t.id.slice(0, 8)}</td>
            <td>
              <div style={{ fontSize: 13 }}>{t.title_seed || t.prompt.slice(0, 80)}</div>
              {t.failure_reason && (
                <div style={{ fontSize: 11, color: 'var(--status-fail)', marginTop: 2 }}>
                  {t.failure_reason}
                </div>
              )}
            </td>
            <td><Badge status={tone(t.state)}>{t.state}</Badge></td>
            <td className="mono dim" style={{ fontSize: 11.5 }}>
              {t.blocked_by_guard || '—'}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function PublicationsTable({ items }: { items: ChannelPublication[] }) {
  if (items.length === 0) return <div className="muted" style={{ padding: 20 }}>No publications</div>;
  return (
    <table className="vp-table">
      <thead><tr>
        <th style={{ width: 90 }}>ID</th>
        <th style={{ width: 110 }}>Platform</th>
        <th>Title</th>
        <th style={{ width: 130 }}>Privacy</th>
        <th style={{ width: 130 }}>Status</th>
      </tr></thead>
      <tbody>
        {items.map(p => (
          <tr key={p.id}>
            <td className="id">{p.id.slice(0, 8)}</td>
            <td><Tag>{p.platform}</Tag></td>
            <td>
              <span className="vp-row-link">{p.title}</span>
              {p.warnings_json.length > 0 && (
                <div style={{ display: 'flex', gap: 4, marginTop: 4, flexWrap: 'wrap' }}>
                  {p.warnings_json.map(w => (
                    <span key={w} className="vp-tag" style={{ color: 'var(--status-run)' }}>{w}</span>
                  ))}
                </div>
              )}
            </td>
            <td className="muted mono" style={{ fontSize: 12 }}>
              {p.current_privacy} <span style={{ color: 'var(--fg-5)' }}>→</span> {p.desired_privacy}
            </td>
            <td><Badge status={tone(p.publish_status)}>{p.publish_status}</Badge></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function LaneRow({ lane }: { lane: LaneHealth }) {
  const state = lane.paused_until ? 'paused' : lane.enabled ? 'active' : 'disabled';
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 10, padding: '9px 0',
      borderBottom: '1px solid var(--border-1)',
    }}>
      <div>
        <div style={{ fontWeight: 500, fontSize: 13 }}>{lane.name}</div>
        <div className="muted mono" style={{ fontSize: 11 }}>{lane.lane_id.slice(0, 8)}</div>
      </div>
      <div style={{ flex: 1 }} />
      <Badge status={tone(state)}>{state}</Badge>
    </div>
  );
}

function AccountRow({ account }: { account: AccountHealth }) {
  const state = account.paused_until ? 'paused' :
                account.enabled ? account.last_token_check_status || 'active' : 'disabled';
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 10, padding: '9px 0',
      borderBottom: '1px solid var(--border-1)',
    }}>
      <div>
        <div style={{ fontWeight: 500, fontSize: 13 }}>{account.account_label}</div>
        <div className="muted mono" style={{ fontSize: 11 }}>{account.platform}</div>
      </div>
      <div style={{ flex: 1 }} />
      <Badge status={tone(state)}>{state}</Badge>
    </div>
  );
}
