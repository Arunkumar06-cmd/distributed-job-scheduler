import React from 'react'
import { short, dt } from '../lib/format'

const TONE = { COMPLETED:'completed', FAILED:'failed', RETRY_WAIT:'retrying', CANCELLED:'failed' }
const VERB = { COMPLETED:'finished', FAILED:'failed', RETRY_WAIT:'scheduled for retry', CANCELLED:'cancelled' }

export function ActivityFeed({ events }) {
  const list = events || []
  return <aside className="panel details" aria-label="Live activity">
    <div className="panel-head"><div><small>Live</small><h2>Activity</h2></div>
      <em className="live-tag">{list.length ? 'streaming' : 'idle'}</em>
    </div>
    <section className="activity">
      {!list.length && <p className="muted">No terminal activity yet. Events appear here the moment a job finishes.</p>}
      {list.map(e =>
        <p key={e.id + String(e.at)} className="activity-row">
          <time title={new Date(e.at).toLocaleString()}>{dt(e.at)}</time>{' '}
          <b className={TONE[e.status] || ''}>{e.status === 'COMPLETED' ? '✓' : '✕'} {VERB[e.status] || e.status}</b>{' '}
          <span className="mono">{short(e.id)}</span> <span className="muted">({e.type})</span>
        </p>)}
    </section>
  </aside>
}
