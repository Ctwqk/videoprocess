import { useEffect, useMemo, useRef, useState } from 'react';
import apiClient from '../api/client';
import type { Asset } from '../api/types';
import { formatFileSize } from '../utils/fileSize';
import { Icons } from '../components/common/ui';

type View = 'grid' | 'list';
type KindFilter = 'all' | 'video' | 'audio' | 'image' | 'subtitle' | 'other';

const KIND_TONE: Record<string, string> = {
  video: '#60a5fa',
  audio: '#c084fc',
  image: '#fbbf24',
  subtitle: 'var(--acc)',
  other: 'var(--fg-4)',
};

function kindOf(asset: Asset): KindFilter {
  const m = asset.mime_type ?? '';
  if (m.startsWith('video/')) return 'video';
  if (m.startsWith('audio/')) return 'audio';
  if (m.startsWith('image/')) return 'image';
  if (m.includes('subrip') || asset.original_name.endsWith('.srt') || asset.original_name.endsWith('.vtt')) return 'subtitle';
  return 'other';
}

function durationOf(asset: Asset): string {
  const info = asset.media_info as Record<string, unknown> | null;
  if (!info) return '—';
  const d = typeof info.duration === 'number' ? info.duration : null;
  if (!d) return '—';
  const m = Math.floor(d / 60);
  const s = Math.floor(d % 60);
  return `${m}:${String(s).padStart(2, '0')}`;
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

const KIND_ICON: Record<KindFilter, (p: { size?: number; style?: React.CSSProperties }) => React.ReactElement> = {
  video: Icons.film,
  audio: Icons.music,
  image: Icons.type,
  subtitle: Icons.caption,
  other: Icons.folder,
  all: Icons.folder,
};

export default function AssetsPage() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [view, setView] = useState<View>('grid');
  const [kind, setKind] = useState<KindFilter>('all');
  const [search, setSearch] = useState('');
  const fileInputRef = useRef<HTMLInputElement>(null);

  const fetchAssets = () => {
    apiClient.get('/assets').then(res => {
      setAssets(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => { fetchAssets(); }, []);

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      await apiClient.post('/assets/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 300000,
      });
      fetchAssets();
    } catch {
      window.alert('Upload failed');
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const handleDelete = async (id: string) => {
    if (!window.confirm('Delete this asset?')) return;
    await apiClient.delete(`/assets/${id}`);
    fetchAssets();
  };

  const handleDownload = (id: string) => {
    window.open(`/api/v1/assets/${id}/download`, '_blank');
  };

  const filtered = useMemo(() => {
    return assets.filter(a => {
      const k = kindOf(a);
      if (kind !== 'all' && k !== kind) return false;
      if (search && !a.original_name.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    });
  }, [assets, kind, search]);

  const totalSize = useMemo(
    () => assets.reduce((s, a) => s + (a.file_size ?? 0), 0),
    [assets],
  );

  const counts: Record<KindFilter, number> = {
    all: assets.length,
    video: assets.filter(a => kindOf(a) === 'video').length,
    audio: assets.filter(a => kindOf(a) === 'audio').length,
    image: assets.filter(a => kindOf(a) === 'image').length,
    subtitle: assets.filter(a => kindOf(a) === 'subtitle').length,
    other: assets.filter(a => kindOf(a) === 'other').length,
  };

  return (
    <div className="vp-page">
      <div style={{ padding: '20px 24px 12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
          <h2 style={{ margin: 0, fontSize: 20, letterSpacing: '-0.02em', fontWeight: 600 }}>Assets</h2>
          <span className="mono dim" style={{ fontSize: 12 }}>
            ·  {assets.length} items  ·  {formatFileSize(totalSize)} used
          </span>
          <div style={{ flex: 1 }} />
          <div style={{
            display: 'flex', gap: 0, padding: 3,
            background: 'var(--bg-1)', border: '1px solid var(--border-1)', borderRadius: 7,
          }}>
            <button
              type="button"
              className="vp-btn vp-btn-sm"
              onClick={() => setView('grid')}
              style={{
                background: view === 'grid' ? 'var(--bg-3)' : 'transparent',
                border: '1px solid ' + (view === 'grid' ? 'var(--border-2)' : 'transparent'),
              }}
            >
              <Icons.layers size={12} />Grid
            </button>
            <button
              type="button"
              className="vp-btn vp-btn-sm"
              onClick={() => setView('list')}
              style={{
                background: view === 'list' ? 'var(--bg-3)' : 'transparent',
                border: '1px solid ' + (view === 'list' ? 'var(--border-2)' : 'transparent'),
              }}
            >
              <Icons.list size={12} />List
            </button>
          </div>
          <label className="vp-btn vp-btn-primary" style={{ cursor: uploading ? 'wait' : 'pointer' }}>
            <Icons.upload size={13} />{uploading ? 'Uploading…' : 'Upload'}
            <input
              ref={fileInputRef}
              type="file"
              onChange={handleUpload}
              style={{ display: 'none' }}
              accept="video/*,audio/*,image/*,.srt,.vtt,.ass,.ssa"
            />
          </label>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            display: 'flex', gap: 4, padding: 3,
            background: 'var(--bg-1)', border: '1px solid var(--border-1)', borderRadius: 7,
          }}>
            {(['all', 'video', 'audio', 'image', 'subtitle'] as KindFilter[]).map(k => (
              <button
                key={k}
                type="button"
                className="vp-btn vp-btn-sm"
                onClick={() => setKind(k)}
                style={{
                  background: kind === k ? 'var(--bg-3)' : 'transparent',
                  border: '1px solid ' + (kind === k ? 'var(--border-2)' : 'transparent'),
                  color: kind === k ? 'var(--fg-1)' : 'var(--fg-3)',
                }}
              >
                {k === 'all' ? 'All' : k.charAt(0).toUpperCase() + k.slice(1)}
                <span className="mono dim" style={{ marginLeft: 6, fontSize: 10.5 }}>{counts[k]}</span>
              </button>
            ))}
          </div>
          <div style={{ flex: 1 }} />
          <div style={{ position: 'relative' }}>
            <Icons.search size={13} style={{
              position: 'absolute', left: 9, top: '50%', transform: 'translateY(-50%)',
              color: 'var(--fg-4)',
            }} />
            <input
              className="vp-input"
              placeholder="Search assets…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              style={{ width: 240, paddingLeft: 28 }}
            />
          </div>
        </div>
      </div>

      {loading ? (
        <div className="muted" style={{ padding: 24 }}>Loading…</div>
      ) : filtered.length === 0 ? (
        <div className="vp-empty">
          <div className="ico"><Icons.folder size={22} /></div>
          <div style={{ fontSize: 14, color: 'var(--fg-2)', marginBottom: 4 }}>No assets here yet.</div>
          <div className="muted" style={{ fontSize: 12.5 }}>Drop a file or click Upload above.</div>
        </div>
      ) : view === 'grid' ? (
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 14,
          padding: '12px 24px 24px',
        }}>
          {filtered.map(a => {
            const k = kindOf(a);
            const I = KIND_ICON[k];
            const dur = durationOf(a);
            return (
              <div key={a.id} className="vp-card" style={{ overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
                <div style={{
                  aspectRatio: '16/10',
                  background: 'repeating-linear-gradient(135deg, #1c1c20 0 8px, #16161a 8px 16px)',
                  borderBottom: '1px solid var(--border-1)',
                  position: 'relative',
                  display: 'grid', placeItems: 'center',
                }}>
                  <I size={28} style={{ color: KIND_TONE[k], opacity: 0.5 }} />
                  <span style={{
                    position: 'absolute', top: 8, left: 8,
                    fontSize: 10, fontFamily: 'var(--font-mono)',
                    background: 'rgba(0,0,0,0.55)', border: '1px solid rgba(255,255,255,0.06)',
                    padding: '2px 6px', borderRadius: 3,
                    color: KIND_TONE[k], textTransform: 'uppercase',
                  }}>
                    {k}
                  </span>
                  {dur !== '—' && (
                    <span style={{
                      position: 'absolute', bottom: 8, right: 8,
                      fontSize: 10.5, fontFamily: 'var(--font-mono)',
                      background: 'rgba(0,0,0,0.55)',
                      padding: '2px 6px', borderRadius: 3, color: 'var(--fg-1)',
                    }}>
                      {dur}
                    </span>
                  )}
                </div>
                <div style={{ padding: '10px 12px', display: 'flex', flexDirection: 'column', gap: 4 }}>
                  <div style={{ fontSize: 13, fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {a.original_name}
                  </div>
                  <div className="mono dim" style={{ fontSize: 11, display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span>{formatFileSize(a.file_size)}</span>
                    <span>·</span>
                    <span>{timeAgo(a.uploaded_at)}</span>
                    <div style={{ flex: 1 }} />
                    <button
                      type="button"
                      onClick={() => handleDownload(a.id)}
                      className="vp-btn vp-btn-sm vp-btn-ghost"
                      style={{ padding: 4 }}
                      title="Download"
                    >
                      <Icons.download size={13} />
                    </button>
                    <button
                      type="button"
                      onClick={() => void handleDelete(a.id)}
                      className="vp-btn vp-btn-sm vp-btn-ghost"
                      style={{ padding: 4, color: 'var(--status-fail)' }}
                      title="Delete"
                    >
                      <Icons.trash size={13} />
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div style={{ padding: '0 24px 24px' }}>
          <table className="vp-table">
            <thead>
              <tr>
                <th>Name</th>
                <th style={{ width: 90 }}>Kind</th>
                <th style={{ width: 100 }}>Size</th>
                <th style={{ width: 100 }}>Duration</th>
                <th style={{ width: 160 }}>Uploaded</th>
                <th style={{ width: 110 }} className="actions">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(a => {
                const k = kindOf(a);
                const I = KIND_ICON[k];
                return (
                  <tr key={a.id}>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <I size={14} style={{ color: KIND_TONE[k] }} />
                        <span className="vp-row-link">{a.original_name}</span>
                      </div>
                    </td>
                    <td className="mono dim" style={{ fontSize: 11.5, textTransform: 'uppercase' }}>{k}</td>
                    <td className="mono dim" style={{ fontSize: 12 }}>{formatFileSize(a.file_size)}</td>
                    <td className="mono dim" style={{ fontSize: 12 }}>{durationOf(a)}</td>
                    <td className="mono dim" style={{ fontSize: 12 }}>
                      {new Date(a.uploaded_at).toLocaleString()}
                    </td>
                    <td className="actions">
                      <button
                        type="button"
                        onClick={() => handleDownload(a.id)}
                        className="vp-btn vp-btn-sm vp-btn-ghost"
                      >
                        <Icons.download size={13} />Download
                      </button>
                      <button
                        type="button"
                        onClick={() => void handleDelete(a.id)}
                        className="vp-btn vp-btn-sm vp-btn-ghost"
                        style={{ color: 'var(--status-fail)' }}
                      >
                        <Icons.trash size={13} />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
