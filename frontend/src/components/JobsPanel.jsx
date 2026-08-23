import React, { useState } from 'react'
import { cap, short, dt } from '../lib/format'

const S = ['QUEUED','RUNNING','RETRY_WAIT','COMPLETED','FAILED','SCHEDULED','WAITING']

export function JobsPanel({ jobs, loading, total, totalPages, page, setPage, pageSize, onInspect, onRetry, search, setSearch, filter, setFilter, updated }) {
  return <section className="panel jobs">
    <div className="panel-head">
      <div><h2>Jobs</h2><p>{updated ? `Updated ${updated.toLocaleTimeString([], {hour:'numeric',minute:'2-digit',second:'2-digit'})}` : 'Live updates via event stream.'}</p></div>
      <input className="job-search" value={search} onChange={e => setSearch(e.target.value)} placeholder="Search job ID or key" aria-label="Search jobs"/>
      <select aria-label="Filter by status" value={filter} onChange={e => setFilter(e.target.value)}>
        <option value="">All states</option>{S.map(x => <option key={x} value={x}>{cap(x)}</option>)}
      </select>
    </div>
    <div className="table-wrap" role="region" aria-label="Job list" tabIndex={0}>
      <table>
        <thead><tr><th scope="col">Job</th><th scope="col">Status</th><th scope="col">Attempts</th><th scope="col">Created</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead>
        <tbody>
          {loading && !jobs.length && Array.from({length:4}).map((_,i)=>(
            <tr key={`sk${i}`}><td colSpan="5" style={{padding:'6px 10px'}}><div className="skel"/></td></tr>))}
          {jobs.map(x =>
            <tr key={x.id}>
              <td><button className="job" onClick={() => onInspect(x)}><b>{short(x.id)}</b><span>{x.idempotency_key || 'No idempotency key'}</span></button></td>
              <td><em className={String(x.status).toLowerCase()} title={{QUEUED:"Waiting for a worker to claim it",CLAIMED:"A worker has reserved this job",RUNNING:"Executing right now",RETRY_WAIT:"Failed — backing off before the next attempt",COMPLETED:"Finished successfully",FAILED:"Out of attempts",SCHEDULED:"Will start at its scheduled time",WAITING:"Blocked until upstream workflow jobs finish"}[x.status] || cap(x.status)}>{cap(x.status)}</em></td>
              <td>{x.attempt || 0} <span className="muted">/ {x.max_attempts || 1}</span></td>
              <td className="muted">{dt(x.created_at)}</td>
              <td>
                <button className="text" onClick={() => onInspect(x)}>Inspect</button>
                {['FAILED','RETRY_WAIT'].includes(String(x.status).toUpperCase()) &&
                  <button className="text" onClick={() => onRetry(x)}>Retry</button>}
              </td>
            </tr>)}
          {!loading && !jobs.length && <tr><td colSpan="5" className="empty-cell"><b>No matching jobs</b><span>Adjust filters or create a job.</span></td></tr>}
        </tbody>
      </table>
    </div>
    <div className="pagination">
      <button disabled={page <= 1} onClick={() => setPage(page - 1)}>← Prev</button>
      <span>Page {page} of {Math.max(totalPages, 1)} · {total} job{total === 1 ? '' : 's'}</span>
      <button disabled={page >= totalPages} onClick={() => setPage(page + 1)}>Next →</button>
    </div>
  </section>
}
export const PAGE_SIZE = 25
export { S as JOB_STATES }
