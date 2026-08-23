import React from 'react'
import { fmtTimeIST } from '../lib/format'

/** Grouped bar time-series for completed/failed per minute. */
export function ThroughputChart({ buckets, onMinuteClick }) {
  const [hover, setHover] = React.useState(null)
  const data = buckets || []
  const max = Math.max(1, ...data.map(d => Math.max(d.completed, d.failed)))
  const W = 640, H = 160, PAD = 26
  const bw = (W - PAD * 2) / Math.max(data.length, 1)
  const y = v => H - PAD - (v / max) * (H - PAD * 2)
  return (
    <figure className="panel chart-panel" role="img" aria-label={`Jobs per minute over the last ${data.length} minutes`}>
      <div className="panel-head"><div><h2>Throughput</h2><p>Terminal transitions per minute</p></div>
        <div className="legend">
          <span><i className="lg-ok" /> completed</span>
          <span><i className="lg-bad" /> failed</span>
        </div>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        <line x1={PAD} y1={H - PAD} x2={W - PAD / 2} y2={H - PAD} stroke="rgba(255,255,255,.14)" />
        {[max, Math.floor(max / 2), 0].map((v, i) => (
          <g key={i}>
            <line x1={PAD} x2={W - PAD / 2} y1={y(v)} y2={y(v)} stroke="rgba(255,255,255,.07)" />
            <text x={4} y={y(v) + 3} fontSize="9" fill="#9aa6bf">{v}</text>
          </g>
        ))}
        {data.map((d, i) => {
          const x = PAD + i * bw
          const w = Math.max(bw * 0.28, 3)
          const isHover = hover === d.bucket
          return (
            <g key={d.bucket}
               onMouseEnter={() => setHover(d.bucket)}
               onMouseLeave={() => setHover(h => (h === d.bucket ? null : h))}
               onClick={() => onMinuteClick?.(d)}
               style={{ cursor: onMinuteClick ? 'pointer' : 'default' }}>
              {isHover && <>
                <rect x={x} y={PAD / 2} width={bw * 2.4} height={H - PAD} fill="rgba(124,111,240,.12)" rx="6" />
                <g>
                  <rect x={Math.min(x, W - 150)} y={PAD / 2 + 4} width="140" height="40" rx="8"
                        fill="rgba(10,16,30,.92)" stroke="rgba(255,255,255,.18)" />
                  <text x={Math.min(x, W - 150) + 10} y={PAD / 2 + 22} fontSize="10" fill="#e8edf7">
                    {fmtTimeIST(d.bucket)} IST
                  </text>
                  <text x={Math.min(x, W - 150) + 10} y={PAD / 2 + 36} fontSize="10">
                    <tspan fill="#3ddc97">{d.completed} done</tspan>
                    <tspan fill="#9aa6bf"> · </tspan>
                    <tspan fill="#fb7185">{d.failed} failed</tspan>
                  </text>
                </g>
              </>}
              <rect x={x + w * 0.55} y={y(d.completed)} width={w} height={H - PAD - y(d.completed)} rx="1.5"
                    fill="var(--ok)" opacity={hover && !isHover ? 0.45 : 1}>
                <title>{`${d.bucket}: ${d.completed} completed`}</title>
              </rect>
              <rect x={x + w * 1.65} y={y(d.failed)} width={w} height={H - PAD - y(d.failed)} rx="1.5"
                    fill="#fb7185" opacity={hover && !isHover ? 0.45 : 1}>
                <title>{`${d.bucket}: ${d.failed} failed`}</title>
              </rect>
              {i % Math.ceil(data.length / 8) === 0 &&
                <text x={x + bw / 2} y={H - 8} fontSize="9" textAnchor="middle" fill="#9aa6bf">{fmtTimeIST(d.bucket)}</text>}
            </g>
          )
        })}
      </svg>
    </figure>
  )
}

const SEG_COLORS = { COMPLETED: '#3ddc97', RUNNING: '#7c6ff0', QUEUED: '#5b8def', RETRY_WAIT: '#f59e0b', FAILED: '#fb7185', SCHEDULED: '#94a3b8', CLAIMED: '#c084fc', WAITING: '#64748b' }

/** Status donut with center total. */
export function StatusDonut({ counts, total, onSegmentClick }) {
  const entries = Object.entries(counts || {}).filter(([, v]) => v > 0)
  const sum = entries.reduce((a, [, v]) => a + v, 0) || 1
  const R = 52, C = 2 * Math.PI * R
  let offset = 0
  return (
    <figure className="panel chart-panel" role="img" aria-label="Job status distribution donut chart">
      <div className="panel-head"><div><h2>Status mix</h2><p>Share of all jobs in this queue</p></div></div>
      <svg viewBox="0 0 140 140" width="150" height="150" style={{ display:'block', margin:'0 auto' }}>
        <circle cx="70" cy="70" r={R} fill="none" stroke="rgba(255,255,255,.07)" strokeWidth="16" />
        {entries.map(([k, v]) => {
          const frac = v / sum
          const dash = `${frac * C} ${C}`
          const el = (
            <circle key={k} cx="70" cy="70" r={R} fill="none"
              stroke={SEG_COLORS[k] || '#cbd5e1'} strokeWidth="16"
              strokeDasharray={`${dash} ${C}`} strokeDashoffset={-offset * C}
              transform="rotate(-90 70 70)"
              style={{ cursor: 'pointer' }}
              onClick={() => onSegmentClick?.(k)}>
              <title>{`${cap(k)}: ${v} — click to filter jobs`}</title>
            </circle>
          )
          offset += frac
          return el
        })}
        <text x="70" y="66" textAnchor="middle" fontSize="20" fontWeight="700" fill="#e8edf7">{total}</text>
        <text x="70" y="82" textAnchor="middle" fontSize="9" fill="#98a2b3">TOTAL JOBS</text>
      </svg>
      <div className="donut-legend">
        {entries.map(([k, v]) =>
          <span key={k}><i style={{ background: SEG_COLORS[k] || '#cbd5e1' }} />{cap(k)} · {v}</span>)}
      </div>
    </figure>
  )
}
function cap(s){return String(s||'').replaceAll('_',' ').toLowerCase().replace(/\b\w/g,x=>x.toUpperCase())}

/** Lifecycle pipeline with live counts under each stage. */
export function LifecycleStrip({ s = {}, onStageClick }) {
  const stages = [
    ['Queued', s.queued], ['Claimed', s.claimed], ['Running', s.running],
    ['Retry wait', s.retry_wait], ['Done', s.completed], ['Failed', s.failed],
  ]
  const stage_filter = { Queued:'QUEUED', Claimed:'CLAIMED', Running:'RUNNING', 'Retry wait':'RETRY_WAIT', Done:'COMPLETED', Failed:'FAILED' }
  return (
    <div className="lifecycle-strip" aria-label="Job lifecycle overview">
      {stages.map(([label, n], i) => (
        <React.Fragment key={label}>
          {i > 0 && <span className="lc-arrow" aria-hidden="true">→</span>}
          <span className="lc-stage" role="button" tabIndex={0}
                style={{ cursor:'pointer' }}
                onClick={() => onStageClick?.(stage_filter[label] || '')}
                onKeyDown={e => (e.key === 'Enter' || e.key === ' ') && onStageClick?.(stage_filter[label] || '')}
                title={`Show ${label.toLowerCase()} jobs`}>
            <b>{Number(n || 0).toLocaleString()}</b>
            <small>{label}</small>
          </span>
        </React.Fragment>
      ))}
    </div>
  )
}
