import { useEffect, useState } from 'react';

export type PlatformKey = 'x' | 'xiaohongshu' | 'bilibili';

export type PlatformAuthStatus = {
  platform: PlatformKey;
  authenticated: boolean;
  browser_running: boolean;
  headed: boolean;
  cookie_present: boolean;
  reason?: string | null;
  detail?: string | null;
  last_checked_at: number;
};

const AUTH_STATUS_RETRY_DELAYS_MS = [0, 500, 1500];
const AUTH_MESSAGE_TYPE = 'vp-platform-auth';

function sleep(ms: number) {
  return new Promise(resolve => window.setTimeout(resolve, ms));
}

export function usePlatformAuth(platform: PlatformKey) {
  const storageKey = `vp_platform_auth_status_${platform}`;
  const authBase = `/platforms/api/platforms/${platform}`;

  const [authStatus, setAuthStatus] = useState<PlatformAuthStatus | null>(() => {
    if (typeof window === 'undefined') return null;
    try {
      const raw = window.localStorage.getItem(storageKey);
      return raw ? JSON.parse(raw) as PlatformAuthStatus : null;
    } catch {
      return null;
    }
  });
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authInitialized, setAuthInitialized] = useState(false);

  async function refreshAuthStatus() {
    let lastError: Error | null = null;

    for (const delayMs of AUTH_STATUS_RETRY_DELAYS_MS) {
      try {
        if (delayMs > 0) {
          await sleep(delayMs);
        }

        const response = await fetch(`${authBase}/auth/status`, { cache: 'no-store' });
        if (!response.ok) {
          throw new Error(`Platform auth service returned ${response.status}`);
        }
        const contentType = response.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
          throw new Error('Platform auth service returned a non-JSON response');
        }

        const data = await response.json() as PlatformAuthStatus;
        setAuthStatus(data);
        window.localStorage.setItem(storageKey, JSON.stringify(data));
        setAuthError(null);
        setAuthInitialized(true);
        return data;
      } catch (error) {
        lastError = error instanceof Error
          ? error
          : new Error('Platform auth status unavailable');
      }
    }

    setAuthError(lastError?.message || 'Platform auth status unavailable');
    setAuthInitialized(true);
    return null;
  }

  async function openPlatformAuth() {
    try {
      setAuthLoading(true);
      setAuthError(null);
      const authUrl = `${window.location.origin}${authBase}/auth/start`;
      const authWindow = window.open(
        authUrl,
        `vp-platform-auth-${platform}`,
        'popup=yes,width=720,height=820,resizable=yes,scrollbars=yes',
      );
      if (!authWindow) {
        window.location.assign(authUrl);
        return;
      }
      authWindow.focus();
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : 'failed to open platform login');
    } finally {
      window.setTimeout(() => setAuthLoading(false), 300);
    }
  }

  async function logoutPlatformAuth() {
    try {
      setAuthLoading(true);
      const response = await fetch(`${authBase}/auth/logout`, { method: 'POST' });
      if (!response.ok) {
        throw new Error(`status ${response.status}`);
      }
      await refreshAuthStatus();
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : 'logout failed');
    } finally {
      setAuthLoading(false);
    }
  }

  useEffect(() => {
    void refreshAuthStatus();

    function handleFocus() {
      void refreshAuthStatus();
    }

    function handleMessage(event: MessageEvent) {
      if (event.origin !== window.location.origin) return;
      if (!event.data || typeof event.data !== 'object') return;
      const payload = event.data as { type?: string; platform?: PlatformKey };
      if (payload.type !== AUTH_MESSAGE_TYPE) return;
      if (payload.platform !== platform) return;
      void refreshAuthStatus();
    }

    window.addEventListener('focus', handleFocus);
    window.addEventListener('message', handleMessage);
    return () => {
      window.removeEventListener('focus', handleFocus);
      window.removeEventListener('message', handleMessage);
    };
  }, [platform]);

  return {
    authStatus,
    authLoading,
    authError,
    authInitialized,
    refreshAuthStatus,
    openPlatformAuth,
    logoutPlatformAuth,
  };
}
