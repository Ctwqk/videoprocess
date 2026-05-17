import { useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';

export default function AppShell() {
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

  const renderedSidebarCollapsed = sidebarCollapsed || compactViewport;

  return (
    <div style={{ display: 'flex', height: '100vh', minWidth: 0 }}>
      <Sidebar
        collapsed={renderedSidebarCollapsed}
        onToggle={() => setSidebarCollapsed(prev => !prev)}
      />
      <main
        style={{
          flex: 1,
          minWidth: 0,
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
          backgroundColor: '#020617',
        }}
      >
        <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
          <Outlet />
        </div>
      </main>
    </div>
  );
}
