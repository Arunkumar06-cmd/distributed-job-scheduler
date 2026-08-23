import React from 'react'
import { cap, short, dt } from '../lib/format'

export function JobDetails({ job, logs, retry }) {
  if (!job) return <aside className="panel details blank"><div>◎</div><h2>Job details</h2><p>Select a job to view attempts, timestamps, logs and retry history.</p></aside>
  return <aside className="panel details">
    <div className="panel-head"><div><small>Job details</small><h2>{short(job.id)}</h2></div><em className={String(job.status).toLowerCase()}>{cap(job.status)}</em></div>
    <section>
      <h3>Execution</h3>
      <p><span>Attempts</span><b>{job.attempt||0} of {job.max_attempts||1}</b></p>
      <p><span>Worker</span><b>{short(job.lease_owner)}</b></p>
      <p><span>Created</span><b>{dt(job.created_at)}</b></p>
      {job.error_message && <p><span>Last error</span><b>{job.error_message}</b></p>}
    </section>
    <section>
      <h3>Recent logs</h3>
      <div className="logs">{logs.slice(-4).map(x =>
        <p key={x.id}><time>{new Date(x.created_at).toLocaleTimeString()}</time> <b>{x.level}</b> {x.message}</p>)}
        {!logs.length && <p className="muted">No logs recorded for this job.</p>}</div>
    </section>
    {['FAILED','RETRY_WAIT'].includes(String(job.status).toUpperCase()) && <button onClick={retry}>Retry job</button>}
  </aside>
}
