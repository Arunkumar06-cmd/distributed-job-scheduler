import React from 'react'
export function Metric({ x, y, suffix = "", d, c }) {
  const n = Number(y)
  const shown = Number.isFinite(n)
    ? (suffix === "%" ? n.toFixed(0) : n.toLocaleString()) + suffix
    : String(y ?? "—")
  return <article className={c}><span>{x}</span><b>{shown}</b>{d && <small>{d}</small>}</article>
}
export function QueueCharts({ stats, maxConcurrency }) {
  const states = [['Queued',stats.queued||0,'queued'],['Running',stats.running||0,'running'],['Retrying',stats.retry_wait||0,'retrying'],['Completed',stats.completed||0,'completed'],['Failed',stats.failed||0,'failed']]
  const max = Math.max(1, ...states.map(([,v]) => v))
  const used = Math.min(100, Math.round(((stats.running||0) / Math.max(1, maxConcurrency)) * 100))
  return <section className="charts" aria-label="Queue status charts"><article className="panel chart-panel"><div className="panel-head"><div><h2>Queue state distribution</h2><p>Current persisted job counts.</p></div></div><div className="bar-chart">{states.map(([label,value,tone]) => <div className="bar-row" key={label}><span>{label}</span><div className="bar-track"><i className={tone} style={{width:`${Math.max(value?4:0,Math.round(value/max*100))}%`}}/></div><b>{value}</b></div>)}</div></article><article className="panel capacity-panel"><div><h2>Worker capacity</h2><p>{stats.running||0} of {maxConcurrency} concurrent slots in use.</p></div><div className="capacity-ring" style={{'--fill':`${used*3.6}deg`}}><strong>{used}%</strong><span>in use</span></div></article></section>
}
