import apiClient from './client';

export type ChannelProfile = {
  id: string;
  name: string;
  language: string;
  enabled: boolean;
  dry_run: boolean;
  halted_at: string | null;
  halt_reason: string | null;
  config_version: number;
};

export type ChannelHealth = {
  channel_id: string;
  dry_run: boolean;
  halted: boolean;
  active_tasks: number;
  queued_items: number;
  recent_failures: number;
  warnings: string[];
};

export type ChannelQueueItem = {
  id: string;
  kind: string;
  idempotency_key: string;
  priority: number;
  status: string;
  payload_json: Record<string, unknown>;
  attempt_count: number;
  last_error: string | null;
};

export type ChannelTask = {
  id: string;
  state: string;
  prompt: string;
  title_seed: string;
  target_account_id: string;
  blocked_by_guard: string | null;
  failure_reason: string | null;
};

export type ChannelPublication = {
  id: string;
  production_task_id: string;
  platform: string;
  platform_content_id: string;
  title: string;
  desired_privacy: string;
  current_privacy: string;
  publish_status: string;
  warnings_json: string[];
};

export type LaneHealth = {
  lane_id: string;
  name: string;
  enabled: boolean;
  paused_until: string | null;
};

export type AccountHealth = {
  id: string;
  account_label: string;
  platform: string;
  enabled: boolean;
  paused_until: string | null;
  last_token_check_status: string | null;
};

export type FunnelSummary = Record<string, number>;

export type ChannelOverview = {
  health: ChannelHealth;
  queue: ChannelQueueItem[];
  tasks: ChannelTask[];
  publications: ChannelPublication[];
  lanes: LaneHealth[];
  accounts: AccountHealth[];
  funnel: FunnelSummary;
};

export async function fetchChannels() {
  const res = await apiClient.get<ChannelProfile[]>('/channel-agent/channels');
  return res.data;
}

export async function fetchChannelOverview(channelId: string): Promise<ChannelOverview> {
  const [health, queue, tasks, publications, lanes, accounts, funnel] = await Promise.all([
    apiClient.get<ChannelHealth>(`/channel-agent/channels/${channelId}/health`),
    apiClient.get<ChannelQueueItem[]>(`/channel-agent/channels/${channelId}/queue`),
    apiClient.get<ChannelTask[]>(`/channel-agent/channels/${channelId}/tasks`),
    apiClient.get<ChannelPublication[]>(`/channel-agent/channels/${channelId}/publications`),
    apiClient.get<LaneHealth[]>(`/channel-agent/channels/${channelId}/lanes/health`),
    apiClient.get<AccountHealth[]>(`/channel-agent/channels/${channelId}/accounts/health`),
    apiClient.get<FunnelSummary>(`/channel-agent/channels/${channelId}/metrics/funnel?days=7`),
  ]);

  return {
    health: health.data,
    queue: queue.data,
    tasks: tasks.data,
    publications: publications.data,
    lanes: lanes.data,
    accounts: accounts.data,
    funnel: funnel.data,
  };
}

export async function setDryRun(channelId: string, dryRun: boolean) {
  const res = await apiClient.patch<ChannelProfile>(`/channel-agent/channels/${channelId}/dry-run`, {
    dry_run: dryRun,
  });
  return res.data;
}

export async function haltChannel(channelId: string, reason: string) {
  const res = await apiClient.post<ChannelProfile>(`/channel-agent/channels/${channelId}/halt`, {
    reason,
  });
  return res.data;
}

export async function resumeChannel(channelId: string) {
  const res = await apiClient.post<ChannelProfile>(`/channel-agent/channels/${channelId}/resume`);
  return res.data;
}
