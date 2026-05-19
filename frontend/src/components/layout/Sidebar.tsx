import { NavLink, useLocation } from 'react-router-dom';
import { useYouTubeAuth } from '../../hooks/useYouTubeAuth';
import { usePlatformAuth } from '../../hooks/usePlatformAuth';
import { Icons } from '../common/ui';

const LINKS = [
  { to: '/autoflow',    label: 'AutoFlow',    icon: Icons.spark,  kbd: '1' },
  { to: '/editor',      label: 'Editor',      icon: Icons.flow,   kbd: '2' },
  { to: '/templates',   label: 'Templates',   icon: Icons.layers, kbd: '3' },
] as const;

const LIBRARY_LINKS = [
  { to: '/jobs',        label: 'Jobs',        icon: Icons.play,   kbd: '4' },
  { to: '/assets',      label: 'Assets',      icon: Icons.folder, kbd: '5' },
  { to: '/channel-ops', label: 'ChannelOps',  icon: Icons.branch, kbd: '6' },
] as const;

type Auth = {
  authStatus: { authenticated?: boolean } | null;
  authLoading: boolean;
  authError: string | null;
  authInitialized: boolean;
};

function chipState(a: Auth): 'ok' | 'warn' | 'err' | 'check' {
  if (a.authError && !a.authStatus) return 'err';
  if (!a.authInitialized && !a.authStatus) return 'check';
  return a.authStatus?.authenticated ? 'ok' : 'warn';
}

const STATE_COLOR = {
  ok: 'var(--status-ok)',
  warn: 'var(--status-run)',
  err: 'var(--status-fail)',
  check: 'var(--fg-5)',
} as const;

const STATE_META = {
  ok: 'connected',
  warn: 'login',
  err: 'offline',
  check: '...',
} as const;

export default function Sidebar({
  collapsed,
  onToggle,
}: {
  collapsed: boolean;
  onToggle: () => void;
}) {
  const location = useLocation();
  const youtube = useYouTubeAuth();
  const x = usePlatformAuth('x');
  const xhs = usePlatformAuth('xiaohongshu');
  const bili = usePlatformAuth('bilibili');

  const platforms = [
    { key: 'yt',   label: 'YouTube',     auth: { authStatus: youtube.authStatus, authLoading: youtube.authLoading, authError: youtube.authError, authInitialized: youtube.authInitialized },
      onClick: () => void youtube.openYouTubeAuth() },
    { key: 'xhs',  label: 'Xiaohongshu', auth: { authStatus: xhs.authStatus, authLoading: xhs.authLoading, authError: xhs.authError, authInitialized: xhs.authInitialized },
      onClick: () => void xhs.openPlatformAuth() },
    { key: 'bili', label: 'Bilibili',    auth: { authStatus: bili.authStatus, authLoading: bili.authLoading, authError: bili.authError, authInitialized: bili.authInitialized },
      onClick: () => void bili.openPlatformAuth() },
    { key: 'x',    label: 'X',           auth: { authStatus: x.authStatus, authLoading: x.authLoading, authError: x.authError, authInitialized: x.authInitialized },
      onClick: () => void x.openPlatformAuth() },
  ];

  const isEditorActive = location.pathname.startsWith('/editor');
  const isJobsActive = location.pathname.startsWith('/jobs');

  const navLinkClass = ({ isActive }: { isActive: boolean }) =>
    `vp-nav-item${isActive ? ' active' : ''}`;

  return (
    <aside className={`vp-sidebar${collapsed ? ' collapsed' : ''}`}>
      <div className="vp-brand">
        <div className="vp-brand-mark">V</div>
        {!collapsed && (
          <>
            <div className="vp-brand-name">VideoProcess</div>
            <div className="vp-brand-tag">v0.4</div>
          </>
        )}
      </div>

      <div className="vp-nav">
        {!collapsed && <div className="vp-nav-section">Workflow</div>}
        {LINKS.map(link => {
          const Icon = link.icon;
          const forceActive = link.to === '/editor' && isEditorActive;
          return (
            <NavLink
              key={link.to}
              to={link.to}
              title={collapsed ? link.label : undefined}
              className={forceActive ? 'vp-nav-item active' : navLinkClass}
            >
              <Icon className="vp-nav-icon" size={16} />
              <span>{link.label}</span>
              <span className="vp-nav-kbd">{link.kbd}</span>
            </NavLink>
          );
        })}

        {!collapsed && <div className="vp-nav-section">Library</div>}
        {LIBRARY_LINKS.map(link => {
          const Icon = link.icon;
          const forceActive = link.to === '/jobs' && isJobsActive;
          return (
            <NavLink
              key={link.to}
              to={link.to}
              title={collapsed ? link.label : undefined}
              className={forceActive ? 'vp-nav-item active' : navLinkClass}
            >
              <Icon className="vp-nav-icon" size={16} />
              <span>{link.label}</span>
              <span className="vp-nav-kbd">{link.kbd}</span>
            </NavLink>
          );
        })}

        {!collapsed && <div className="vp-nav-section">Platforms</div>}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {platforms.map(p => {
            const state = chipState(p.auth);
            return (
              <button
                key={p.key}
                type="button"
                onClick={p.onClick}
                disabled={p.auth.authLoading}
                title={collapsed ? p.label : undefined}
                className="vp-platform-chip"
              >
                <span className="vp-dot" style={{ background: STATE_COLOR[state] }} />
                <span className="vp-label">{p.label}</span>
                <span className="vp-meta">{p.auth.authLoading ? '...' : STATE_META[state]}</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="vp-sidebar-footer">
        <div className="vp-platform-chip" style={{ pointerEvents: 'none' }}>
          <span className="vp-dot" style={{ background: 'var(--acc)' }} />
          {!collapsed && (
            <>
              <span className="vp-label">runner-01</span>
              <span className="vp-meta">2/4 GPU</span>
            </>
          )}
        </div>
        <button
          type="button"
          onClick={onToggle}
          className="vp-nav-item"
          style={{ width: '100%', background: 'transparent', border: 'none' }}
        >
          <Icons.panelLeft className="vp-nav-icon" size={16} />
          {!collapsed && <span style={{ flex: 1, textAlign: 'left' }}>Collapse</span>}
          {!collapsed && <span className="vp-nav-kbd">⌘\</span>}
        </button>
      </div>
    </aside>
  );
}
