/* eslint-disable react-refresh/only-export-components */
import type { CSSProperties, ReactNode, SVGProps } from 'react';

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

const base = (size: number): SVGProps<SVGSVGElement> => ({
  width: size,
  height: size,
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
});

export const Icons = {
  spark: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M12 3v4M12 17v4M3 12h4M17 12h4M5.6 5.6l2.8 2.8M15.6 15.6l2.8 2.8M5.6 18.4l2.8-2.8M15.6 8.4l2.8-2.8"/></svg>
  ),
  flow: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><rect x="3" y="3" width="6" height="6" rx="1.4"/><rect x="15" y="15" width="6" height="6" rx="1.4"/><path d="M9 6h3a3 3 0 0 1 3 3v9"/></svg>
  ),
  play: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} fill="currentColor" stroke="none" {...p}><path d="M7 5v14l12-7z"/></svg>
  ),
  pause: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} fill="currentColor" stroke="none" {...p}><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>
  ),
  folder: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M3 6.5A1.5 1.5 0 0 1 4.5 5h4l2 2h9A1.5 1.5 0 0 1 21 8.5v9A1.5 1.5 0 0 1 19.5 19h-15A1.5 1.5 0 0 1 3 17.5v-11Z"/></svg>
  ),
  layers: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="m12 3 9 5-9 5-9-5 9-5Z"/><path d="m3 13 9 5 9-5"/><path d="m3 17 9 5 9-5"/></svg>
  ),
  search: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
  ),
  check: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="m5 12 4 4L19 7"/></svg>
  ),
  x: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M6 6l12 12M6 18 18 6"/></svg>
  ),
  plus: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M12 5v14M5 12h14"/></svg>
  ),
  upload: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M12 16V4M7 9l5-5 5 5"/><path d="M5 20h14"/></svg>
  ),
  download: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M12 4v12M7 11l5 5 5-5"/><path d="M5 20h14"/></svg>
  ),
  trash: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M3 6h18M8 6V4a1 1 0 0 1 1-1h6a1 1 0 0 1 1 1v2"/><path d="M19 6v14a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V6"/><path d="M10 11v6M14 11v6"/></svg>
  ),
  more: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} fill="currentColor" stroke="none" {...p}><circle cx="5" cy="12" r="1.2"/><circle cx="12" cy="12" r="1.2"/><circle cx="19" cy="12" r="1.2"/></svg>
  ),
  chevron: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="m9 6 6 6-6 6"/></svg>
  ),
  pin: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M12 17v5"/><path d="M9 11V4h6v7l3 3v2H6v-2l3-3Z"/></svg>
  ),
  film: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M7 3v18M17 3v18M3 7h4M3 12h4M3 17h4M17 7h4M17 12h4M17 17h4"/></svg>
  ),
  music: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M9 18V5l11-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="17" cy="16" r="3"/></svg>
  ),
  type: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M4 7V5h16v2"/><path d="M9 20h6M12 5v15"/></svg>
  ),
  scissors: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="m20 4-8.5 8.5M14 14l6 6M8.1 8.1l5.9 5.9"/></svg>
  ),
  wand: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="m21 3-7 7M14 10l-9 9M15 3l-2 2 4 4 2-2-4-4Z"/></svg>
  ),
  caption: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M7 11h3M14 11h3M7 15h6"/></svg>
  ),
  history: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 4v4h4M12 8v4l3 2"/></svg>
  ),
  bell: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M6 8a6 6 0 1 1 12 0c0 7 3 8 3 8H3s3-1 3-8Z"/><path d="M10 21h4"/></svg>
  ),
  branch: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="6" cy="18" r="2.5"/><path d="M6 9v6M18 8.5v.5a5 5 0 0 1-5 5H6"/></svg>
  ),
  panelLeft: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16"/></svg>
  ),
  copy: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><rect x="8" y="8" width="13" height="13" rx="2"/><path d="M3 16V5a2 2 0 0 1 2-2h11"/></svg>
  ),
  rocket: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M4.5 16.5C3 18 3 21 3 21s3 0 4.5-1.5"/><path d="M9 12a8 8 0 0 1 8-8h3v3a8 8 0 0 1-8 8l-2 1-2-2 1-2Z"/></svg>
  ),
  list: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></svg>
  ),
  share: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><circle cx="6" cy="12" r="3"/><circle cx="18" cy="6" r="3"/><circle cx="18" cy="18" r="3"/><path d="m8.5 10.5 7-3M8.5 13.5l7 3"/></svg>
  ),
  globe: ({ size = 16, ...p }: IconProps) => (
    <svg {...base(size)} {...p}><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/></svg>
  ),
};

/* ============================================================
   Badge / Status
   ============================================================ */

export type StatusTone = 'ok' | 'run' | 'fail' | 'queue' | 'idle';

export function Badge({ status = 'idle', children }: { status?: StatusTone; children: ReactNode }) {
  return (
    <span className={`vp-badge vp-badge-${status}`}>
      <span className="vp-dot" />
      {children}
    </span>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return <span className="vp-kbd">{children}</span>;
}

export function Tag({ children, style }: { children: ReactNode; style?: CSSProperties }) {
  return <span className="vp-tag" style={style}>{children}</span>;
}

/* Job/Node status mapping shared across pages */
export function toneForJobStatus(status: string | undefined): { tone: StatusTone; label: string } {
  if (!status) return { tone: 'idle', label: '—' };
  if (status === 'SUCCEEDED') return { tone: 'ok', label: 'OK' };
  if (status === 'RUNNING' || status === 'PLANNING' || status === 'VALIDATING') return { tone: 'run', label: status };
  if (status === 'FAILED' || status === 'CANCELLED') return { tone: 'fail', label: status };
  if (status === 'PENDING' || status === 'QUEUED') return { tone: 'queue', label: 'PENDING' };
  if (status === 'PARTIALLY_FAILED') return { tone: 'fail', label: 'PARTIAL' };
  return { tone: 'idle', label: status };
}

export function toneForNodeStatus(status: string | undefined): StatusTone {
  if (status === 'SUCCEEDED') return 'ok';
  if (status === 'RUNNING') return 'run';
  if (status === 'FAILED') return 'fail';
  if (status === 'PENDING' || status === 'QUEUED') return 'queue';
  return 'idle';
}
