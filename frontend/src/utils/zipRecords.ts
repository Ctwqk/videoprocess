export function getZipChannelCount(config: Record<string, unknown> | undefined): number {
  const raw = config?.channel_count;
  const value = typeof raw === 'number' ? raw : Number(raw || 2);
  if (!Number.isFinite(value)) {
    return 2;
  }
  return Math.max(1, Math.trunc(value));
}
