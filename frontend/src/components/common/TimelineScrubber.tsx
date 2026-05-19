import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Icons } from '../common/ui';

export type TimelineShot = {
  i: number;
  title: string;
  start: number; // seconds
  dur: number;   // seconds
  color?: string;
  desc?: string;
  source?: string;
};

function fmtTime(s: number) {
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  const cs = Math.floor((s % 1) * 100);
  return `${m}:${String(sec).padStart(2, '0')}.${String(cs).padStart(2, '0')}`;
}

const DEFAULT_COLOR = '#7dd3fc';

export function TimelineScrubber({
  shots,
  totalDuration,
  initialTime = 0,
}: {
  shots: TimelineShot[];
  totalDuration: number;
  initialTime?: number;
}) {
  const [time, setTime] = useState(initialTime);
  const [playing, setPlaying] = useState(false);
  const [hover, setHover] = useState<{ t: number; x: number } | null>(null);
  const trackRef = useRef<HTMLDivElement | null>(null);
  const draggingRef = useRef(false);

  useEffect(() => {
    if (!playing) return;
    let raf = 0;
    let last = performance.now();
    const step = (now: number) => {
      const dt = (now - last) / 1000;
      last = now;
      setTime(t => {
        const next = t + dt;
        if (next >= totalDuration) { setPlaying(false); return totalDuration; }
        return next;
      });
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [playing, totalDuration]);

  const xToTime = useCallback((clientX: number) => {
    const r = trackRef.current?.getBoundingClientRect();
    if (!r) return 0;
    const x = Math.max(0, Math.min(r.width, clientX - r.left));
    return (x / r.width) * totalDuration;
  }, [totalDuration]);

  const onDown = (e: React.PointerEvent) => {
    draggingRef.current = true;
    setPlaying(false);
    setTime(xToTime(e.clientX));
    e.preventDefault();
    const move = (ev: PointerEvent) => {
      if (!draggingRef.current) return;
      setTime(xToTime(ev.clientX));
    };
    const up = () => {
      draggingRef.current = false;
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  const onHover = (e: React.PointerEvent) => {
    const t = xToTime(e.clientX);
    setHover({ t, x: e.clientX - (trackRef.current?.getBoundingClientRect().left || 0) });
  };

  const activeShot = shots.find(s => time >= s.start && time < s.start + s.dur) ?? shots[shots.length - 1];
  const localT = activeShot ? Math.max(0, time - activeShot.start) : 0;
  const localProgress = activeShot ? Math.min(1, localT / activeShot.dur) : 0;

  // Deterministic pseudo-waveform
  const wave = useMemo(() => {
    const N = Math.max(80, Math.ceil(totalDuration * 7));
    return Array.from({ length: N }, (_, i) => {
      const v = Math.abs(Math.sin(i * 0.31) + Math.sin(i * 0.13) * 0.7 + Math.cos(i * 0.07) * 0.4);
      return 0.35 + v * 0.35;
    });
  }, [totalDuration]);

  const tickEvery = totalDuration <= 30 ? 5 : totalDuration <= 90 ? 15 : 30;

  return (
    <div className="vp-card">
      <div className="vp-section-head">
        <h3>Timeline</h3>
        <span className="vp-count">{shots.length} clips · {totalDuration}s</span>
        <div className="vp-spacer" />
        <button className="vp-btn vp-btn-sm vp-btn-ghost" type="button"><Icons.scissors size={12} />Split at playhead</button>
        <button className="vp-btn vp-btn-sm vp-btn-ghost" type="button"><Icons.wand size={12} />Auto‑pace</button>
        <button className="vp-btn vp-btn-sm vp-btn-ghost" type="button"><Icons.history size={12} />Markers</button>
      </div>

      <div style={{ padding: '0 20px 18px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '152px 1fr', gap: 14, marginBottom: 14 }}>
          {/* Preview */}
          <div style={{
            aspectRatio: '9/16', width: 152, position: 'relative',
            borderRadius: 8, overflow: 'hidden',
            background: 'linear-gradient(135deg,#1f1f25,#131316)',
            border: '1px solid var(--border-2)',
          }}>
            <div style={{ position: 'absolute', inset: 10, border: '1px dashed #2a2a30', borderRadius: 5 }} />
            <div style={{
              position: 'absolute', top: 8, left: 8, display: 'flex', alignItems: 'center', gap: 6,
              fontSize: 10, fontFamily: 'var(--font-mono)',
              background: 'rgba(0,0,0,0.55)', padding: '2px 6px', borderRadius: 3,
            }}>
              <span style={{ width: 5, height: 5, borderRadius: 99, background: activeShot?.color ?? DEFAULT_COLOR }} />
              <span style={{ color: 'var(--fg-2)', textTransform: 'uppercase' }}>
                shot {String(activeShot?.i ?? 0).padStart(2, '0')}
              </span>
            </div>
            <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', color: 'var(--fg-3)' }}>
              <Icons.play size={28} />
            </div>
            <div style={{ position: 'absolute', bottom: 8, left: 8, right: 8 }}>
              <div style={{
                fontFamily: 'var(--font-mono)', fontSize: 9.5, color: 'var(--fg-3)',
                background: 'rgba(0,0,0,0.5)', padding: '1px 5px', borderRadius: 3,
                display: 'inline-block',
              }}>
                {activeShot?.title ?? '—'}
              </div>
              <div style={{ height: 3, marginTop: 6, background: 'rgba(0,0,0,0.5)', borderRadius: 2 }}>
                <div style={{
                  height: '100%',
                  width: `${localProgress * 100}%`,
                  background: activeShot?.color ?? DEFAULT_COLOR,
                  borderRadius: 2,
                }} />
              </div>
            </div>
          </div>

          {/* Controls + track */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <button
                type="button"
                className="vp-btn vp-btn-icon"
                onClick={() => {
                  const prev = [...shots].reverse().find(s => s.start < time - 0.05);
                  setTime(prev ? prev.start : 0);
                }}
              >
                <Icons.chevron size={14} style={{ transform: 'rotate(180deg)' }} />
              </button>
              <button
                type="button"
                className="vp-btn vp-btn-primary vp-btn-icon"
                onClick={() => setPlaying(p => !p)}
              >
                {playing ? <Icons.pause size={14} /> : <Icons.play size={14} />}
              </button>
              <button
                type="button"
                className="vp-btn vp-btn-icon"
                onClick={() => {
                  const next = shots.find(s => s.start > time + 0.05);
                  setTime(next ? next.start : totalDuration);
                }}
              >
                <Icons.chevron size={14} />
              </button>
              <span style={{ width: 1, height: 22, background: 'var(--border-2)' }} />
              <div className="mono num" style={{ fontSize: 13 }}>
                <span style={{ color: 'var(--acc)' }}>{fmtTime(time)}</span>
                <span style={{ color: 'var(--fg-5)', margin: '0 6px' }}>/</span>
                <span style={{ color: 'var(--fg-3)' }}>{fmtTime(totalDuration)}</span>
              </div>
              {activeShot && (
                <div className="muted mono" style={{ fontSize: 11, marginLeft: 10 }}>
                  shot {String(activeShot.i).padStart(2, '0')} · t={localT.toFixed(2)}s · {Math.round(localProgress * 100)}%
                </div>
              )}
            </div>

            {/* Ruler */}
            <div style={{ position: 'relative', height: 22, marginTop: 4 }}>
              <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%' }} preserveAspectRatio="none">
                {Array.from({ length: totalDuration + 1 }, (_, i) => {
                  const major = i % tickEvery === 0;
                  const x = `${(i / totalDuration) * 100}%`;
                  return (
                    <g key={i}>
                      <line x1={x} y1={major ? 4 : 12} x2={x} y2={20}
                            stroke={major ? '#3f3f46' : '#27272a'} strokeWidth={1} />
                      {major && (
                        <text x={x} y={2} dy="0.4em"
                              fontSize="9" fill="var(--fg-4)" fontFamily="var(--font-mono)"
                              textAnchor={i === 0 ? 'start' : i === totalDuration ? 'end' : 'middle'}>
                          {i}s
                        </text>
                      )}
                    </g>
                  );
                })}
              </svg>
            </div>

            {/* Track */}
            <div
              ref={trackRef}
              onPointerDown={onDown}
              onPointerMove={onHover}
              onPointerLeave={() => setHover(null)}
              style={{
                position: 'relative', height: 70,
                background: 'var(--bg-2)', borderRadius: 6,
                border: '1px solid var(--border-1)',
                overflow: 'hidden',
                touchAction: 'none',
                userSelect: 'none',
                cursor: 'ew-resize',
              }}
            >
              {shots.map(s => {
                const left = (s.start / totalDuration) * 100;
                const width = (s.dur / totalDuration) * 100;
                const isActive = activeShot?.i === s.i;
                const color = s.color ?? DEFAULT_COLOR;
                const waveStart = Math.floor((s.start / totalDuration) * wave.length);
                const waveEnd = Math.floor(((s.start + s.dur) / totalDuration) * wave.length);
                return (
                  <div key={s.i} style={{
                    position: 'absolute', top: 0, bottom: 0,
                    left: `${left}%`, width: `${width}%`,
                    background: `linear-gradient(180deg, ${color}22, ${color}11)`,
                    borderLeft: `2px solid ${color}`,
                    borderRight: '1px solid var(--border-1)',
                    display: 'flex', flexDirection: 'column',
                    padding: '5px 7px',
                    opacity: isActive ? 1 : 0.85,
                  }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                      <span className="mono" style={{ fontSize: 9, color, fontWeight: 600 }}>
                        #{String(s.i).padStart(2, '0')}
                      </span>
                      <span style={{
                        fontSize: 10, color: 'var(--fg-2)',
                        whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                      }}>
                        {s.title}
                      </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 1, flex: 1, marginTop: 4 }}>
                      {wave.slice(waveStart, waveEnd).map((v, i) => (
                        <div key={i} style={{
                          width: 2, height: `${v * 100}%`,
                          background: color, opacity: 0.6, borderRadius: 1,
                        }} />
                      ))}
                    </div>
                    <div className="mono" style={{ fontSize: 9, color: 'var(--fg-4)', marginTop: 2 }}>
                      {s.dur}s
                    </div>
                  </div>
                );
              })}

              {hover && (
                <div style={{
                  position: 'absolute', top: 0, bottom: 0,
                  left: hover.x, width: 1,
                  background: 'rgba(255,255,255,0.18)', pointerEvents: 'none',
                }}>
                  <div style={{
                    position: 'absolute', top: -22, left: '50%', transform: 'translateX(-50%)',
                    fontFamily: 'var(--font-mono)', fontSize: 10,
                    background: 'var(--bg-3)', border: '1px solid var(--border-2)',
                    color: 'var(--fg-2)', padding: '1px 6px', borderRadius: 3, whiteSpace: 'nowrap',
                  }}>
                    {fmtTime(hover.t)}
                  </div>
                </div>
              )}

              <div style={{
                position: 'absolute', top: -4, bottom: -4,
                left: `${(time / totalDuration) * 100}%`,
                width: 2, background: 'var(--acc)',
                boxShadow: '0 0 0 1px rgba(125,211,252,0.18)',
                pointerEvents: 'none',
                zIndex: 5,
              }}>
                <div style={{
                  position: 'absolute', top: -6, left: '50%', transform: 'translateX(-50%)',
                  width: 0, height: 0,
                  borderLeft: '6px solid transparent', borderRight: '6px solid transparent',
                  borderTop: '6px solid var(--acc)',
                }} />
                <div style={{
                  position: 'absolute', bottom: -6, left: '50%', transform: 'translateX(-50%)',
                  width: 0, height: 0,
                  borderLeft: '6px solid transparent', borderRight: '6px solid transparent',
                  borderBottom: '6px solid var(--acc)',
                }} />
              </div>
            </div>

            <div style={{
              display: 'flex', gap: 14, alignItems: 'center', fontSize: 10.5,
              fontFamily: 'var(--font-mono)', color: 'var(--fg-4)',
              textTransform: 'uppercase', letterSpacing: '.06em',
            }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}><Icons.film size={11} /> V1 · video</span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}><Icons.music size={11} /> A1 · audio</span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}><Icons.caption size={11} /> S1 · subs</span>
              {activeShot?.desc && <span style={{ marginLeft: 'auto' }}>{activeShot.desc}</span>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
