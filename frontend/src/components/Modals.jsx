import React, { useState } from 'react'
import { slug } from '../lib/format'
import { api } from '../lib/api'

export function Modal({ title, close, children }) {
  const titleId = React.useId()
  const ref = React.useRef(null)
  React.useEffect(() => { ref.current?.focus() }, [])
  return <div className="backdrop" onMouseDown={close}>
    <div
      className="modal" role="dialog" aria-modal="true" aria-labelledby={titleId}
      tabIndex={-1} ref={ref}
      onKeyDown={e => { if (e.key === 'Escape') close() }}
      onMouseDown={e => e.stopPropagation()}
    >
      <div className="modal-head"><h2 id={titleId}>{title}</h2><button aria-label="Close dialog" onClick={close}>×</button></div>
      {children}
    </div>
  </div>
}

export function EntityForm({ type, auth, org, project, close, done }) {
  const [name, setName] = useState(''), [s, setS] = useState(''), [n, setN] = useState(5), [err, setErr] = useState(''), [busy, setBusy] = useState(false)
  const label = { organization:'Create organization', project:'Create project', queue:'Create queue' }[type]
  const submit = async e => {
    e.preventDefault(); setBusy(true)
    try {
      if (type==='organization') await api('/organizations',{method:'POST',body:JSON.stringify({name,slug:s||slug(name)})},auth.token)
      if (type==='project') await api('/projects',{method:'POST',body:JSON.stringify({org_id:org,name,slug:s||slug(name)})},auth.token)
      if (type==='queue') await api('/queues',{method:'POST',body:JSON.stringify({project_id:project,name,max_concurrency:Number(n)})},auth.token)
      done(label)
    } catch (e) { setErr(e.message) } finally { setBusy(false) }
  }
  return <Modal title={label} close={close}>
    <form onSubmit={submit}>
      <label>Name<input autoFocus required value={name} onChange={e=>setName(e.target.value)} placeholder={type==='queue'?'email-delivery':'Acme Inc.'}/></label>
      {type!=='queue' && <label>URL slug<input value={s} onChange={e=>setS(e.target.value)} placeholder="acme-inc"/><small className="muted">Leave blank to generate from the name.</small></label>}
      {type==='queue' && <label>Maximum concurrency<input type="number" min="1" max="1000" value={n} onChange={e=>setN(e.target.value)}/></label>}
      {err && <p className="error">{err}</p>}
      <div className="modal-actions">
        <button type="button" onClick={close}>Cancel</button>
        <button className="primary" disabled={busy}>{busy?'Creating…':label}</button>
      </div>
    </form>
  </Modal>
}

export function JobForm({ q, auth, close, done }) {
  const [p, setP] = useState('{\n  "type": "echo",\n  "message": "Hello from Jobflow"\n}')
  const [priority, setPriority] = useState(q.default_priority || 0), [err, setErr] = useState(''), [busy, setBusy] = useState(false)
  const submit = async e => {
    e.preventDefault()
    let value
    try { value = JSON.parse(p) } catch { setErr('Payload must be valid JSON.'); return }
    if (!value || typeof value !== 'object' || Array.isArray(value)) { setErr('Payload must be a JSON object.'); return }
    setBusy(true)
    try { await api('/jobs',{method:'POST',body:JSON.stringify({queue_id:q.id,payload:value,priority:Number(priority)})},auth.token); done() }
    catch (e) { setErr(e.message) } finally { setBusy(false) }
  }
  return <Modal title="Create job" close={close}>
    <p className="description">Submit work to <b>{q.name}</b>. A worker will claim it when capacity is available.</p>
    <form onSubmit={submit}>
      <label>Priority<input type="number" min="0" max="100" value={priority} onChange={e=>setPriority(e.target.value)}/></label>
      <label>Payload<textarea rows="8" value={p} onChange={e=>setP(e.target.value)}/></label>
      {err && <p className="error">{err}</p>}
      <div className="modal-actions">
        <button type="button" onClick={close}>Cancel</button>
        <button className="primary" disabled={busy}>{busy?'Submitting…':'Submit job'}</button>
      </div>
    </form>
  </Modal>
}
