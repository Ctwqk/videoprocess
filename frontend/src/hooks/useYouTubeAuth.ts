import { useEffect, useState } from 'react';

export type YouTubeAuthStatus = {
  authenticated: boolean;
  has_client_secrets: boolean;
  token_exists: boolean;
  quota_estimate?: {
    date: string;
    daily_limit: number;
    estimated_units_used: number;
    estimated_units_remaining: number;
    estimated_upload_requests: number;
    upload_cost_per_request: number;
    source: string;
    search_uses_official_quota: boolean;
    last_video_id?: string | null;
    last_recorded_at?: string | null;
    note?: string;
  };
};

const YOUTUBE_BASE = '/youtube/api';
const AUTH_STATUS_RETRY_DELAYS_MS = [0, 500, 1500];
const AUTH_STATUS_STORAGE_KEY = 'vp_youtube_auth_status';
const AUTH_MESSAGE_TYPE = 'vp-youtube-auth';

function sleep(ms: number) {
  return new Promise(resolve => window.setTimeout(resolve, ms));
}

export function useYouTubeAuth() {
  const [authStatus, setAuthStatus] = useState<YouTubeAuthStatus | null>(() => {
    if (typeof window === 'undefined') return null;
    try {
      const raw = window.localStorage.getItem(AUTH_STATUS_STORAGE_KEY);
      return raw ? JSON.parse(raw) as YouTubeAuthStatus : null;
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

        const response = await fetch(`${YOUTUBE_BASE}/auth/status`, { cache: 'no-store' });
        if (!response.ok) {
          throw new Error(`YouTube auth service returned ${response.status}`);
        }
        const contentType = response.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
          throw new Error('YouTube auth service returned a non-JSON response');
        }
        const data = await response.json() as YouTubeAuthStatus;
        setAuthStatus(data);
        window.localStorage.setItem(AUTH_STATUS_STORAGE_KEY, JSON.stringify(data));
        setAuthError(null);
        setAuthInitialized(true);
        return data;
      } catch (error) {
        lastError = error instanceof Error
          ? error
          : new Error('YouTube auth status unavailable');
      }
    }

    setAuthError(lastError?.message || 'YouTube auth status unavailable');
    setAuthInitialized(true);
    return null;
  }

  async function openYouTubeAuth() {
    try {
      setAuthLoading(true);
      setAuthError(null);
      const returnTo = encodeURIComponent(window.location.href);
      const authUrl = `${window.location.origin}/youtube/api/auth/start?return_to=${returnTo}&mode=popup`;
      const authWindow = window.open(
        authUrl,
        'vp-youtube-auth',
        'popup=yes,width=720,height=820,resizable=yes,scrollbars=yes',
      );
      if (!authWindow) {
        window.location.assign(authUrl);
        return;
      }
      authWindow.focus();
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : 'failed to open auth');
    } finally {
      window.setTimeout(() => setAuthLoading(false), 300);
    }
  }

  async function logoutYouTubeAuth() {
    try {
      setAuthLoading(true);
      const response = await fetch(`${YOUTUBE_BASE}/auth/logout`, { method: 'POST' });
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
      if ((event.data as { type?: string }).type !== AUTH_MESSAGE_TYPE) return;
      void refreshAuthStatus();
    }

    window.addEventListener('focus', handleFocus);
    window.addEventListener('message', handleMessage);
    return () => {
      window.removeEventListener('focus', handleFocus);
      window.removeEventListener('message', handleMessage);
    };
  }, []);

  return {
    authStatus,
    authLoading,
    authError,
    authInitialized,
    refreshAuthStatus,
    openYouTubeAuth,
    logoutYouTubeAuth,
  };
}
