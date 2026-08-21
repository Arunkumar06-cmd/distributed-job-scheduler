import React from 'react'

export function QueueCard({q, stats, isActive, onToggle}){
  return <div className="card" style={{borderColor: isActive?'var(--accent)':'var(--border)'}}>
    <div style={{display:'flex', justifyContent:'space-between'}}>
      <div style={{fontWeight:700}}>{q.name}</div>
      <span className={`badge badge-${q.is_paused?'FAILED':'COMPLETED'}`}>{q.is_paused?'PAUSED':'ACTIVE'}</span>
    </div>
    <div style={{color:'var(--muted)', fontSize:12, marginTop:4}}>concurrency: {q.max_concurrency} • priority: {q.default_priority} {q.rate_limit?`• rate: ${q.rate_limit}/min`:''}</div>
    {stats && <div style={{display:'flex', gap:10, marginTop:12, flexWrap:'wrap'}}>
      <span style={{fontSize:12}}>Q:{stats.queued}</span>
      <span style={{fontSize:12, color:'var(--warn)'}}>R:{stats.running}</span>
      <span style={{fontSize:12, color:'var(--ok)'}}>C:{stats.completed}</span>
      <span style={{fontSize:12, color:'var(--danger)'}}>F:{stats.failed}</span>
      <span style={{fontSize:12}}>W:{stats.retry_wait}</span>
      <span style={{fontSize:12}}>DLQ:{stats.dlq}</span>
    </div>}
    <div style={{marginTop:12, display:'flex', gap:8}}>
      <button className={`btn btn-sm ${q.is_paused?'btn-primary':''}`} onClick={onToggle}>{q.is_paused?'Resume':'Pause'}</button>
    </div>
  </div>
}

export function WorkerTable({workers}){
  return <div className="card">
    <table><thead><tr><th>Name</th><th>Status</th><th>Heartbeat</th><th>Running</th><th>Version</th></tr></thead>
    <tbody>{workers.map(w=><tr key={w.id}><td>{w.worker_name}</td><td><span className={`badge badge-${w.status||'OFFLINE'}`}>{w.status||'-'}</span></td><td style={{fontSize:11}}>{w.last_heartbeat_at? new Date(w.last_heartbeat_at).toLocaleString():'-'}</td><td>{w.running_jobs??0}</td><td>{w.version}</td></tr>)}</tbody></table>
    {!workers.length && <div style={{color:'var(--muted)', textAlign:'center', padding:20}}>No workers. Run <code>cargo run -p worker</code></div>}
  </div>
}

export function JobRow({j, onOpen, onRetry}){
  return <tr>
    <td style={{fontFamily:'monospace', fontSize:11}}>{j.id.slice(0,8)}…</td>
    <td><span className={`badge badge-${j.status}`}>{j.status}</span></td>
    <td>{j.priority}</td>
    <td>{j.attempt}/{j.max_attempts}</td>
    <td style={{fontSize:11, color:'var(--muted)'}}>{new Date(j.created_at).toLocaleString()}</td>
    <td><button className="btn btn-sm" onClick={()=>onOpen(j)}>Details</button>
        {(j.status==='FAILED'||j.status==='RETRY_WAIT'||j.status==='UNKNOWN_EXTERNAL_RESULT') && <button className="btn btn-sm" style={{marginLeft:6}} onClick={()=>onRetry(j.id)}>Retry</button>}</td>
  </tr>
}
