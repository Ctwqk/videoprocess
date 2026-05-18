import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  PauseCircle,
  PlayCircle,
  RefreshCw,
  ShieldAlert,
} from 'lucide-react';

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
import './ChannelOpsStatusPage.css';

type DetailItem =
  | { type: 'queue'; item: ChannelQueueItem }
  | { type: 'task'; item: ChannelTask }
  | { type: 'publication'; item: ChannelPublication };

const statusTone: Record<string, string> = {
  pending: 'muted',
  queued: 'muted',
  claimed: 'info',
  running: 'info',
  completed: 'good',
  succeeded: 'good',
  failed: 'bad',
  dead_lettered: 'bad',
  held: 'warn',
  uploaded_private: 'warn',
  scheduled: 'good',
  published: 'good',
};

function toneFor(value: string | null | undefined) {
  if (!value) return 'muted';
  return statusTone[value.toLowerCase()] ?? 'muted';
}

function shortId(id: string | null | undefined) {
  return id ? `${id.slice(0, 8)}` : '-';
}

function formatDate(value: string | null | undefined) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function HealthCard({
  label,
  value,
  tone,
}: {
  label: string;
  value: string | number;
  tone?: 'good' | 'warn' | 'bad' | 'info';
}) {
  return (
    <div className={`channel-ops-card ${tone ? `channel-ops-card--${tone}` : ''}`}>
      <div className="channel-ops-card__label">{label}</div>
      <div className="channel-ops-card__value">{value}</div>
    </div>
  );
}

function StatusPill({ value }: { value: string | null | undefined }) {
  return <span className={`channel-ops-pill channel-ops-pill--${toneFor(value)}`}>{value || '-'}</span>;
}

function MiniStatus({
  items,
  empty,
  render,
}: {
  items: Array<LaneHealth | AccountHealth>;
  empty: string;
  render: (item: LaneHealth | AccountHealth) => ReactNode;
}) {
  if (items.length === 0) {
    return <div className="channel-ops-empty channel-ops-empty--inline">{empty}</div>;
  }

  return <div className="channel-ops-mini-list">{items.map(render)}</div>;
}

export default function ChannelOpsStatusPage() {
  const [channels, setChannels] = useState<ChannelProfile[]>([]);
  const [selectedChannelId, setSelectedChannelId] = useState('');
  const [overview, setOverview] = useState<ChannelOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [detail, setDetail] = useState<DetailItem | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

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
    if (!channelId) {
      setOverview(null);
      return;
    }
    const data = await fetchChannelOverview(channelId);
    setOverview(data);
    setLastUpdated(new Date().toISOString());
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
    setError(null);
    void loadChannels()
      .catch(err => setError(err instanceof Error ? err.message : 'Failed to load ChannelOps channels'))
      .finally(() => setLoading(false));
  }, [loadChannels]);

  useEffect(() => {
    if (!selectedChannelId) return;
    setLoading(true);
    setError(null);
    setDetail(null);
    void loadOverview(selectedChannelId)
      .catch(err => setError(err instanceof Error ? err.message : 'Failed to load ChannelOps status'))
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
      setChannels(current => current.map(channel => (channel.id === updated.id ? updated : channel)));
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
    return entries.map(([label, value]) => ({
      label,
      value,
      width: `${Math.max(4, Math.round((value / max) * 100))}%`,
    }));
  }, [overview]);

  return (
    <div className="channel-ops-page">
      <header className="channel-ops-header">
        <div>
          <h1>ChannelOps</h1>
          <div className="channel-ops-subtitle">
            {selectedChannel ? `${selectedChannel.name} · config v${selectedChannel.config_version}` : 'No channel selected'}
          </div>
        </div>

        <div className="channel-ops-toolbar">
          <select
            className="channel-ops-select"
            value={selectedChannelId}
            onChange={event => setSelectedChannelId(event.target.value)}
            disabled={channels.length === 0}
          >
            {channels.length === 0 ? (
              <option value="">No channels</option>
            ) : (
              channels.map(channel => (
                <option key={channel.id} value={channel.id}>
                  {channel.name}
                </option>
              ))
            )}
          </select>
          <button
            type="button"
            className="channel-ops-button"
            onClick={() => void refresh()}
            disabled={loading}
            title="Refresh status"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
        </div>
      </header>

      {error ? (
        <div className="channel-ops-alert">
          <ShieldAlert size={16} />
          <span>{error}</span>
        </div>
      ) : null}

      {loading && !overview ? (
        <div className="channel-ops-empty">Loading...</div>
      ) : channels.length === 0 ? (
        <div className="channel-ops-empty">No ChannelOps profiles yet.</div>
      ) : selectedChannel && overview ? (
        <>
          <section className="channel-ops-control-row">
            <div className="channel-ops-mode">
              <span className={`channel-ops-pill ${selectedChannel.dry_run ? 'channel-ops-pill--warn' : 'channel-ops-pill--good'}`}>
                {selectedChannel.dry_run ? 'dry-run' : 'live'}
              </span>
              <span className={`channel-ops-pill ${selectedChannel.halted_at ? 'channel-ops-pill--bad' : 'channel-ops-pill--good'}`}>
                {selectedChannel.halted_at ? 'halted' : 'running'}
              </span>
              {selectedChannel.halt_reason ? <span className="channel-ops-muted">{selectedChannel.halt_reason}</span> : null}
            </div>
            <div className="channel-ops-actions">
              <button
                type="button"
                className="channel-ops-button channel-ops-button--secondary"
                onClick={handleDryRunToggle}
                disabled={busy === 'control'}
                title={selectedChannel.dry_run ? 'Disable dry-run' : 'Enable dry-run'}
              >
                {selectedChannel.dry_run ? <PlayCircle size={16} /> : <PauseCircle size={16} />}
                {selectedChannel.dry_run ? 'Enable Live' : 'Dry-run'}
              </button>
              <button
                type="button"
                className={`channel-ops-button ${selectedChannel.halted_at ? 'channel-ops-button--good' : 'channel-ops-button--danger'}`}
                onClick={handleHaltToggle}
                disabled={busy === 'control'}
                title={selectedChannel.halted_at ? 'Resume channel' : 'Halt channel'}
              >
                {selectedChannel.halted_at ? <PlayCircle size={16} /> : <PauseCircle size={16} />}
                {selectedChannel.halted_at ? 'Resume' : 'Halt'}
              </button>
            </div>
          </section>

          <section className="channel-ops-grid channel-ops-grid--cards">
            <HealthCard label="Active tasks" value={overview.health.active_tasks} tone="info" />
            <HealthCard label="Queued items" value={overview.health.queued_items} />
            <HealthCard
              label="Recent failures"
              value={overview.health.recent_failures}
              tone={overview.health.recent_failures > 0 ? 'bad' : 'good'}
            />
            <HealthCard
              label="Warnings"
              value={overview.health.warnings.length}
              tone={overview.health.warnings.length > 0 ? 'warn' : 'good'}
            />
          </section>

          {overview.health.warnings.length > 0 ? (
            <section className="channel-ops-warning-list">
              {overview.health.warnings.map(warning => (
                <div key={warning} className="channel-ops-warning">
                  <AlertTriangle size={15} />
                  <span>{warning}</span>
                </div>
              ))}
            </section>
          ) : null}

          <section className="channel-ops-grid channel-ops-grid--main">
            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>7-day funnel</h2>
              </div>
              {funnelEntries.length === 0 ? (
                <div className="channel-ops-empty channel-ops-empty--inline">No funnel data</div>
              ) : (
                <div className="channel-ops-funnel">
                  {funnelEntries.map(entry => (
                    <div key={entry.label} className="channel-ops-funnel__row">
                      <div className="channel-ops-funnel__label">{entry.label}</div>
                      <div className="channel-ops-funnel__track">
                        <div className="channel-ops-funnel__bar" style={{ width: entry.width }} />
                      </div>
                      <div className="channel-ops-funnel__value">{entry.value}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>Lanes</h2>
              </div>
              <MiniStatus
                items={overview.lanes}
                empty="No lanes"
                render={item => {
                  const lane = item as LaneHealth;
                  return (
                    <div key={lane.lane_id} className="channel-ops-mini-row">
                      <div>
                        <div className="channel-ops-mini-row__title">{lane.name}</div>
                        <div className="channel-ops-muted">{shortId(lane.lane_id)}</div>
                      </div>
                      <StatusPill value={lane.paused_until ? 'paused' : lane.enabled ? 'active' : 'disabled'} />
                    </div>
                  );
                }}
              />
            </div>

            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>Accounts</h2>
              </div>
              <MiniStatus
                items={overview.accounts}
                empty="No accounts"
                render={item => {
                  const account = item as AccountHealth;
                  return (
                    <div key={account.id} className="channel-ops-mini-row">
                      <div>
                        <div className="channel-ops-mini-row__title">{account.account_label}</div>
                        <div className="channel-ops-muted">{account.platform}</div>
                      </div>
                      <StatusPill
                        value={
                          account.paused_until
                            ? 'paused'
                            : account.enabled
                              ? account.last_token_check_status || 'active'
                              : 'disabled'
                        }
                      />
                    </div>
                  );
                }}
              />
            </div>
          </section>

          <section className="channel-ops-table-grid">
            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>Queue</h2>
              </div>
              <div className="channel-ops-table-wrap">
                <table className="channel-ops-table">
                  <thead>
                    <tr>
                      <th>Kind</th>
                      <th>Status</th>
                      <th>Priority</th>
                      <th>Attempts</th>
                      <th>Key</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.queue.length === 0 ? (
                      <tr>
                        <td colSpan={5}>No queue items</td>
                      </tr>
                    ) : (
                      overview.queue.map(item => (
                        <tr key={item.id} onClick={() => setDetail({ type: 'queue', item })}>
                          <td>{item.kind}</td>
                          <td><StatusPill value={item.status} /></td>
                          <td>{item.priority}</td>
                          <td>{item.attempt_count}</td>
                          <td className="channel-ops-mono">{item.idempotency_key}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>Tasks</h2>
              </div>
              <div className="channel-ops-table-wrap">
                <table className="channel-ops-table">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>State</th>
                      <th>Prompt</th>
                      <th>Guard</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.tasks.length === 0 ? (
                      <tr>
                        <td colSpan={4}>No tasks</td>
                      </tr>
                    ) : (
                      overview.tasks.map(task => (
                        <tr key={task.id} onClick={() => setDetail({ type: 'task', item: task })}>
                          <td className="channel-ops-mono">{shortId(task.id)}</td>
                          <td><StatusPill value={task.state} /></td>
                          <td>{task.prompt || task.title_seed || '-'}</td>
                          <td>{task.blocked_by_guard || task.failure_reason || '-'}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="channel-ops-panel">
              <div className="channel-ops-panel__header">
                <h2>Publications</h2>
              </div>
              <div className="channel-ops-table-wrap">
                <table className="channel-ops-table">
                  <thead>
                    <tr>
                      <th>Title</th>
                      <th>Status</th>
                      <th>Privacy</th>
                      <th>Platform ID</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.publications.length === 0 ? (
                      <tr>
                        <td colSpan={4}>No publications</td>
                      </tr>
                    ) : (
                      overview.publications.map(publication => (
                        <tr
                          key={publication.id}
                          onClick={() => setDetail({ type: 'publication', item: publication })}
                        >
                          <td>{publication.title}</td>
                          <td><StatusPill value={publication.publish_status} /></td>
                          <td>{publication.current_privacy || publication.desired_privacy}</td>
                          <td className="channel-ops-mono">{publication.platform_content_id || '-'}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <aside className="channel-ops-detail">
            <div className="channel-ops-panel__header">
              <h2>Selected detail</h2>
              {detail ? (
                <button type="button" className="channel-ops-text-button" onClick={() => setDetail(null)}>
                  Clear
                </button>
              ) : null}
            </div>
            {detail ? (
              <pre>{formatJson(detail.item)}</pre>
            ) : (
              <div className="channel-ops-empty channel-ops-empty--inline">
                Select a queue item, task, or publication.
              </div>
            )}
          </aside>

          <footer className="channel-ops-footer">
            <CheckCircle2 size={15} />
            <span>Last refreshed {formatDate(lastUpdated)}</span>
          </footer>
        </>
      ) : null}
    </div>
  );
}
