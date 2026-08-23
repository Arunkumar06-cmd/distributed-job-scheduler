import React from 'react'

/** Grouped bar time-series for completed/failed per minute. */
export function ThroughputChart({ buckets }) {
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
        <line x1={PAD} y1={H - PAD} x2={W - PAD / 2} y2={H - PAD} stroke="#e5e9f0" />
        {[max, Math.floor(max / 2), 0].map((v, i) => (
          <g key={i}>
            <line x1={PAD} x2={W - PAD / 2} y1={y(v)} y2={y(v)} stroke="#eef1f6" />
            <text x={4} y={y(v) + 3} fontSize="9" fill="#98a2b3">{v}</text>
          </g>
        ))}
        {data.map((d, i) => {
          const x = PAD + i * bw
          const w = Math.max(bw * 0.28, 3)
          return (
            <g key={d.bucket}>
              <title>{`${d.bucket} · ${d.completed} done · ${d.failed} failed`}</title>
              <rect x={x + w * 0.55} y={y(d.completed)} width={w} height={H - PAD - y(d.completed)} rx="1.5" fill="#31c48d">
                <title>{`${d.bucket}: ${d.completed} completed`}</title>
              </rect>
              <rect x={x + w * 1.65} y={y(d.failed)} width={w} height={H - PAD - y(d.failed)} rx="1.5" fill="#ef4444">
                <title>{`${d.bucket}: ${d.failed} failed`}</title>
              </rect>
              {i % Math.ceil(data.length / 8) === 0 &&
                <text x={x + bw / 2} y={H - 8} fontSize="9" textAnchor="middle" fill="#98a2b3">{d.bucket}</text>}
            </g>
          )
        })}
      </svg>
    </figure>
  )
}

const SEG_COLORS = { COMPLETED: '#31c48d', RUNNING: '#7c6ff0', QUEUED: '#5b8def', RETRY_WAIT: '#f59e0b', FAILED: '#ef4444', SCHEDULED: '#94a3b8', CLAIMED: '#c084fc', WAITING: '#64748b' }

/** Status donut with center total. */
export function StatusDonut({ counts, total }) {
  const entries = Object.entries(counts || {}).filter(([, v]) => v > 0)
  const sum = entries.reduce((a, [, v]) => a + v, 0) || 1
  const R = 52, C = 2 * Math.PI * R
  let offset = 0
  return (
    <figure className="panel chart-panel" role="img" aria-label="Job status distribution donut chart">
      <div className="panel-head"><div><h2>Status mix</h2><p>Share of all jobs in this queue</p></div></div>
      <svg viewBox="0 0 140 140" width="150" height="150" style={{ display:'block', margin:'0 auto' }}>
        <circle cx="70" cy="70" r={R} fill="none" stroke="#eef1f6" strokeWidth="16" />
        {entries.map(([k, v]) => {
          const frac = v / sum
          const dash = `${frac * C} ${C}`
          const el = (
            <circle key={k} cx="70" cy="70" r={R} fill="none"
              stroke={SEG_COLORS[k] || '#cbd5e1'} strokeWidth="16"
              strokeDasharray={`${dash} ${C}`} strokeDashoffset={-offset * C}
              transform="rotate(-90 70 70)">
              <title>{`${k}: ${v}`}</title>
            </circle>
          )
          offset += frac
          return el
        })}
        <text x="70" y="66" textAnchor="middle" fontSize="20" fontWeight="700" fill="#182033">{total}</text>
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
export function LifecycleStrip({ s = {} }) {
  const stages = [
    ['Queued', s.queued], ['Claimed', s.claimed], ['Running', s.running],
    ['Retry wait', s.retry_wait], ['Done', s.completed], ['Failed', s.failed],
  ]
  return (
    <div className="lifecycle-strip" aria-label="Job lifecycle overview">
      {stages.map(([label, n], i) => (
        <React.Fragment key={label}>
          {i > 0 && <span className="lc-arrow" aria-hidden="true">→</span>}
          <span className="lc-stage" title={`Jobs currently ${label.toLowerCase()}`}>
            <b>{Number(n || 0).toLocaleString()}</b>
            <small>{label}</small>
          </span>
        </React.Fragment>
      ))}
    </div>
  )
}
