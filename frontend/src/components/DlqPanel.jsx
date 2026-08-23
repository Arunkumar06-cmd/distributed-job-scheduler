import React, { useCallback, useEffect, useState } from 'react'
import { Modal } from './Modals'
import { api } from '../lib/api'
import { short, dt } from '../lib/format'

export function DlqPanel({ q, auth, note, onChanged }) {
  const [entries, setEntries] = useState([])
  const [busy, setBusy] = useState(null)
  const [summary, setSummary] = useState(null)
  const load = useCallback(async () => {
    try {
      const r = await api(`/dlq?queue_id=${q.id}&page_size=50`, {}, auth.token)
      setEntries(r.data || [])
    } catch (e) { note({ e:1, t: e.message }) }
  }, [q.id, auth.token])
  useEffect(() => { load() }, [load])

  const replay = async (id) => {
    setBusy(id)
    try {
      await api(`/dlq/${id}/replay`, { method:'POST' }, auth.token)
      note({ t:'Entry replayed as a fresh job.' })
      await load(); onChanged?.()
    } catch (e) { note({ e:1, t: e.message }) }
    finally { setBusy(null) }
  }

  return <section className="panel jobs" aria-label="Dead letter queue">
    <div className="panel-head"><div><h2>Dead letter queue</h2><p>{entries.length ? `${entries.length} entries awaiting recovery.` : 'Nothing dead here.'}</p></div><button onClick={load}>Refresh</button></div>
    <div className="table-wrap" role="region" aria-label="Dead letter list" tabIndex={0}>
      <table>
        <thead><tr><th scope="col">Job</th><th scope="col">Reason</th><th scope="col">Attempts</th><th scope="col">Failed at</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
        <tbody>
          {!entries.length && <tr><td colSpan="5" className="empty-cell"><b>Clean slate</b><span>Failed jobs land here after exhausting retries.</span></td></tr>}
          {entries.map(e =>
            <tr key={e.id}>
              <td><button className="job" onClick={() => navigator.clipboard?.writeText(e.job_id)} title="Copy job id" aria-label={`Copy job id for entry ${e.id}`}><b>{short(e.job_id)}</b></button></td>
              <td><em className="failed">{cap(e.reason)}</em></td>
              <td>{e.attempt}</td>
              <td className="muted">{dt(e.moved_at)}</td>
              <td>
                <button className="text" onClick={() => {
                  api(`/dlq/${e.id}/summary`, {}, auth.token)
                    .then(r => setSummary(r))
                    .catch(() => setSummary({ summary:'No AI summary has been generated for this entry yet.', root_cause:null, remediation:null, model:null }))
                }} aria-label={`AI summary for ${short(e.job_id)}`}>✨ AI</button>
                {!e.replayed_to_job_id && <button className="text" disabled={busy===e.id} onClick={() => replay(e.id)} aria-label={`Replay dead-lettered job ${short(e.job_id)}`}>{busy===e.id?'Replaying…':'Replay'}</button>}
                  {e.replayed_to_job_id && <span className="muted">Replayed</span>}</td>
            </tr>)}
        </tbody>
      </table>
    </div>
    {summary && (
      <Modal title="AI failure analysis" close={() => setSummary(null)}>
        <div className="ai-summary">
          <p><span className="kicker">Summary</span>{summary.summary}</p>
          {summary.root_cause && <p><span className="kicker">Root cause</span>{summary.root_cause}</p>}
          {summary.remediation && <p><span className="kicker">Remediation</span>{summary.remediation}</p>}
          <p className="muted" style={{fontSize:11}}>model: {summary.model || '—'}</p>
        </div>
      </Modal>
    )}
  </section>
}
