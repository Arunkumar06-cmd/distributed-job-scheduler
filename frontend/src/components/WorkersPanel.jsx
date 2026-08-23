import React from 'react'
import { cap } from '../lib/format'

export function WorkersPanel({ workers }) {
  if (!workers.length) return null
  return <aside className="panel details">
    <div className="panel-head"><div><small>Fleet</small><h2>Workers</h2></div><em>{workers.filter(w=>w.status==='ONLINE').length} online</em></div>
    <section>
      {workers.slice(0,8).map(w =>
        <p key={w.id}>
          <span className="muted">{w.worker_name}</span>
          <b><em className={String(w.status||'OFFLINE').toLowerCase()}>{cap(w.status || 'OFFLINE')}</em>{' '}
          {w.running_jobs ?? 0} running</b>
        </p>)}
    </section>
  </aside>
}
