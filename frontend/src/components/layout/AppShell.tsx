import { useEffect, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import Sidebar from './Sidebar';
import { Icons, Kbd } from '../common/ui';

type RouteMeta = { title: string; crumbs: string[] };

const ROUTE_META: Array<{ match: (path: string) => boolean; meta: (path: string) => RouteMeta }> = [
  { match: p => p.startsWith('/autoflow'),    meta: () => ({ title: 'AutoFlow',         crumbs: ['workflow', 'autoflow'] }) },
  { match: p => p.startsWith('/editor'),      meta: p => ({ title: 'Pipeline editor',   crumbs: ['workflow', p.split('/')[2] || 'untitled', 'editor'] }) },
  { match: p => p.startsWith('/templates'),   meta: () => ({ title: 'Templates',        crumbs: ['workflow', 'templates'] }) },
  { match: p => p.startsWith('/jobs/'),       meta: p => ({ title: 'Job detail',        crumbs: ['library', 'jobs', p.split('/')[2]?.slice(0, 8) ?? ''] }) },
  { match: p => p.startsWith('/jobs'),        meta: () => ({ title: 'Jobs',             crumbs: ['library', 'jobs'] }) },
  { match: p => p.startsWith('/assets'),      meta: () => ({ title: 'Assets',           crumbs: ['library', 'assets'] }) },
  { match: p => p.startsWith('/channel-ops'), meta: () => ({ title: 'ChannelOps',       crumbs: ['ops', 'channels'] }) },
];

function metaFor(path: string): RouteMeta {
  const found = ROUTE_META.find(r => r.match(path));
  return found ? found.meta(path) : { title: 'VideoProcess', crumbs: [] };
}

export default function AppShell() {
  const location = useLocation();
  const navigate = useNavigate();

  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.localStorage.getItem('vp_sidebar_collapsed') === 'true';
  });
  const [compactViewport, setCompactViewport] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.matchMedia('(max-width: 720px)').matches;
  });

  useEffect(() => {
    window.localStorage.setItem('vp_sidebar_collapsed', String(sidebarCollapsed));
  }, [sidebarCollapsed]);

  useEffect(() => {
    const query = window.matchMedia('(max-width: 720px)');
    const updateCompactViewport = () => setCompactViewport(query.matches);
    updateCompactViewport();
    query.addEventListener('change', updateCompactViewport);
    return () => query.removeEventListener('change', updateCompactViewport);
  }, []);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement | null)?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      const map: Record<string, string> = {
        '1': '/autoflow',
        '2': '/editor',
        '3': '/templates',
        '4': '/jobs',
        '5': '/assets',
        '6': '/channel-ops',
      };
      if (map[e.key]) {
        navigate(map[e.key]);
        e.preventDefault();
        return;
      }
      if ((e.metaKey || e.ctrlKey) && e.key === '\\') {
        setSidebarCollapsed(c => !c);
        e.preventDefault();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [navigate]);

  const renderedSidebarCollapsed = sidebarCollapsed || compactViewport;
  const meta = metaFor(location.pathname);

  return (
    <div className="vp-shell">
      <Sidebar
        collapsed={renderedSidebarCollapsed}
        onToggle={() => setSidebarCollapsed(prev => !prev)}
      />
      <main className="vp-main">
        <header className="vp-topbar">
          <div className="vp-topbar-title">
            <h1>{meta.title}</h1>
            {meta.crumbs.map((c, i) => (
              <span key={i} className="vp-topbar-crumb">{c}</span>
            ))}
          </div>
          <div className="vp-topbar-spacer" />
          <div className="vp-topbar-actions">
            <button type="button" className="vp-btn vp-btn-sm vp-btn-ghost">
              <Icons.search size={13} /> Search <Kbd>⌘K</Kbd>
            </button>
            <button type="button" className="vp-btn vp-btn-icon vp-btn-ghost" aria-label="Notifications">
              <Icons.bell size={14} />
            </button>
            <span style={{ width: 1, height: 22, background: 'var(--border-1)', margin: '0 4px' }} />
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div style={{
                width: 24, height: 24, borderRadius: '50%',
                background: 'linear-gradient(135deg, var(--acc), var(--acc-2))',
                display: 'grid', placeItems: 'center',
                fontSize: 11, fontWeight: 700, color: 'var(--acc-fg)',
              }}>
                T
              </div>
              <span style={{ fontSize: 13 }}>tom</span>
            </div>
          </div>
        </header>
        <Outlet />
      </main>
    </div>
  );
}
