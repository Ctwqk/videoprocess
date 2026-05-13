import { NavLink } from 'react-router-dom';
import { useYouTubeAuth } from '../../hooks/useYouTubeAuth';
import { usePlatformAuth } from '../../hooks/usePlatformAuth';

const links = [
  { to: '/editor', label: 'Editor', icon: '⬡' },
  { to: '/jobs', label: 'Jobs', icon: '▶' },
  { to: '/assets', label: 'Assets', icon: '📁' },
  { to: '/templates', label: 'Templates', icon: '📋' },
];

export default function Sidebar({
  collapsed,
  onToggle,
}: {
  collapsed: boolean;
  onToggle: () => void;
}) {
  const { authStatus, authLoading, authError, authInitialized, openYouTubeAuth } = useYouTubeAuth();
  const xAuth = usePlatformAuth('x');
  const xiaohongshuAuth = usePlatformAuth('xiaohongshu');
  const bilibiliAuth = usePlatformAuth('bilibili');
  const navWidth = collapsed ? 72 : 200;
  const checking = !authInitialized && !authStatus && !authError;
  const primaryButtonStyle = {
    width: '100%',
    border: 'none',
    borderRadius: 8,
    padding: '10px 12px',
    color: '#fff',
    fontSize: 13,
  } as const;

  const xChecking = !xAuth.authInitialized && !xAuth.authStatus && !xAuth.authError;
  const xiaohongshuChecking = !xiaohongshuAuth.authInitialized && !xiaohongshuAuth.authStatus && !xiaohongshuAuth.authError;
  const bilibiliChecking = !bilibiliAuth.authInitialized && !bilibiliAuth.authStatus && !bilibiliAuth.authError;

  const platformItems = [
    {
      key: 'x',
      shortLabel: 'X',
      label: 'X',
      status: xAuth.authStatus,
      loading: xAuth.authLoading,
      error: xAuth.authError,
      checking: xChecking,
      openAuth: xAuth.openPlatformAuth,
    },
    {
      key: 'xiaohongshu',
      shortLabel: 'XHS',
      label: 'Xiaohongshu',
      status: xiaohongshuAuth.authStatus,
      loading: xiaohongshuAuth.authLoading,
      error: xiaohongshuAuth.authError,
      checking: xiaohongshuChecking,
      openAuth: xiaohongshuAuth.openPlatformAuth,
    },
    {
      key: 'bilibili',
      shortLabel: 'BILI',
      label: 'Bilibili',
      status: bilibiliAuth.authStatus,
      loading: bilibiliAuth.authLoading,
      error: bilibiliAuth.authError,
      checking: bilibiliChecking,
      openAuth: bilibiliAuth.openPlatformAuth,
    },
  ];

  return (
    <nav style={{
      width: navWidth,
      backgroundColor: '#1a1a2e',
      color: '#eee',
      display: 'flex',
      flexDirection: 'column',
      padding: '16px 0',
      transition: 'width 0.2s ease',
    }}>
      <div style={{ padding: collapsed ? '0 12px 18px' : '0 16px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: collapsed ? 'center' : 'space-between', gap: 8 }}>
          {!collapsed ? (
            <div style={{ fontSize: 18, fontWeight: 'bold' }}>
              VideoProcess
            </div>
          ) : null}
          <button
            type="button"
            onClick={onToggle}
            style={{
              width: 36,
              height: 36,
              border: '1px solid rgba(255,255,255,0.12)',
              borderRadius: 10,
              backgroundColor: 'rgba(255,255,255,0.04)',
              color: '#e2e8f0',
              cursor: 'pointer',
              fontSize: 16,
            }}
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {collapsed ? '»' : '«'}
          </button>
        </div>
      </div>
      {links.map(link => (
        <NavLink
          key={link.to}
          to={link.to}
          style={({ isActive }) => ({
            display: 'flex',
            alignItems: 'center',
            justifyContent: collapsed ? 'center' : 'flex-start',
            gap: 8,
            padding: collapsed ? '12px 0' : '10px 16px',
            color: isActive ? '#fff' : '#aaa',
            backgroundColor: isActive ? '#16213e' : 'transparent',
            textDecoration: 'none',
            fontSize: 14,
          })}
          title={collapsed ? link.label : undefined}
        >
          <span>{link.icon}</span>
          {!collapsed ? <span>{link.label}</span> : null}
        </NavLink>
      ))}
      <div style={{ marginTop: 'auto', padding: collapsed ? '12px' : '16px', borderTop: '1px solid rgba(255,255,255,0.08)' }}>
        {collapsed ? (
          <div style={{ display: 'grid', gap: 8 }}>
            <div
              title={authStatus?.authenticated ? 'YouTube connected' : 'YouTube login required'}
              style={{
                width: 12,
                height: 12,
                borderRadius: 999,
                backgroundColor: authStatus?.authenticated ? '#86efac' : '#fbbf24',
                margin: '0 auto',
              }}
            />
            <button
              type="button"
              onClick={() => void openYouTubeAuth()}
              disabled={authLoading}
              title={authStatus?.authenticated ? 'Re-login YouTube' : 'Login YouTube'}
              style={{
                width: '100%',
                border: 'none',
                borderRadius: 10,
                padding: '10px 0',
                backgroundColor: '#2563eb',
                color: '#fff',
                cursor: authLoading ? 'default' : 'pointer',
                fontSize: 16,
                opacity: authLoading ? 0.7 : 1,
              }}
            >
              YT
            </button>
            {platformItems.map(item => (
              <button
                key={item.key}
                type="button"
                onClick={() => void item.openAuth()}
                disabled={item.loading}
                title={item.status?.authenticated ? `Re-login ${item.label}` : `Login ${item.label}`}
                style={{
                  width: '100%',
                  border: 'none',
                  borderRadius: 10,
                  padding: '10px 0',
                  backgroundColor: '#0f766e',
                  color: '#fff',
                  cursor: item.loading ? 'default' : 'pointer',
                  fontSize: 11,
                  fontWeight: 700,
                  opacity: item.loading ? 0.7 : 1,
                }}
              >
                {item.shortLabel}
              </button>
            ))}
          </div>
        ) : (
          <>
            <div style={{ fontSize: 12, color: '#9aa4c7', marginBottom: 8 }}>
              YouTube
            </div>
            <div style={{ fontSize: 12, color: authStatus?.authenticated ? '#86efac' : '#fbbf24', marginBottom: 10 }}>
              {checking ? 'Checking...' : authStatus?.authenticated ? 'Connected' : 'Login required'}
            </div>
            <button
              type="button"
              onClick={() => void openYouTubeAuth()}
              disabled={authLoading}
              style={{
                backgroundColor: '#2563eb',
                cursor: authLoading ? 'default' : 'pointer',
                opacity: authLoading ? 0.7 : 1,
                ...primaryButtonStyle,
              }}
            >
              {authStatus?.authenticated ? 'Re-login YouTube' : 'Login YouTube'}
            </button>
            {authError ? (
              <div style={{ marginTop: 8, fontSize: 11, color: '#fca5a5' }}>
                {authError}
              </div>
            ) : null}

            <div style={{ display: 'grid', gap: 14, marginTop: 16 }}>
              {platformItems.map(item => {
                const unavailable = Boolean(item.error && !item.status);
                const statusText = item.checking
                  ? 'Checking...'
                  : unavailable
                    ? 'Unavailable'
                    : item.status?.authenticated
                      ? 'Connected'
                      : 'Login required';
                const statusColor = unavailable
                  ? '#fca5a5'
                  : item.status?.authenticated
                    ? '#86efac'
                    : '#fbbf24';
                return (
                  <div
                    key={item.key}
                    style={{
                      paddingTop: 2,
                    }}
                  >
                    <div style={{ fontSize: 12, color: '#e2e8f0', marginBottom: 6 }}>
                      {item.label}
                    </div>
                    <div style={{ fontSize: 12, color: statusColor, marginBottom: 10 }}>
                      {statusText}
                    </div>
                    <div style={{ display: 'grid', gap: 8 }}>
                      <button
                        type="button"
                        onClick={() => void item.openAuth()}
                        disabled={item.loading}
                        style={{
                          backgroundColor: '#0f766e',
                          cursor: item.loading ? 'default' : 'pointer',
                          opacity: item.loading ? 0.7 : 1,
                          ...primaryButtonStyle,
                        }}
                      >
                        {item.status?.authenticated ? `Re-login ${item.label}` : `Login ${item.label}`}
                      </button>
                    </div>
                    {item.error ? (
                      <div style={{ marginTop: 8, fontSize: 11, color: '#fca5a5' }}>
                        {item.error}
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </>
        )}
        {collapsed && authError ? (
          <div style={{ marginTop: 8, fontSize: 10, color: '#fca5a5', textAlign: 'center' }}>
            !
          </div>
        ) : null}
        {collapsed && platformItems.some(item => item.error) ? (
          <div style={{ marginTop: 8, fontSize: 10, color: '#fca5a5', textAlign: 'center' }}>
            !
          </div>
        ) : null}
      </div>
    </nav>
  );
}
