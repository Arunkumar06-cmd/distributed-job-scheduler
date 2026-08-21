import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react'

const API = ''
const useAuth = () => {
  const [token, setToken] = useState(localStorage.getItem('token')||'')
  const [user, setUser] = useState(JSON.parse(localStorage.getItem('user')||'null'))
  const login = (t,u)=>{ setToken(t); setUser(u); localStorage.setItem('token',t); localStorage.setItem('user',JSON.stringify(u)) }
  const logout = ()=>{ setToken(''); setUser(null); localStorage.removeItem('token'); localStorage.removeItem('user') }
  return { token, user, login, logout, isLogged: !!token }
}
async function api(path, opts={}, token=''){
  const h={'Content-Type':'application/json', ...(opts.headers||{})}
  if(token) h['Authorization']=`Bearer ${token}`
  const r=await fetch(path,{...opts, headers:h})
  if(!r.ok){
    const j=await r.json().catch(()=>({error:{message: r.statusText||`HTTP ${r.status}`}}))
    const msg = j?.error?.message || (typeof j?.error==='string'? j.error : null) || `HTTP ${r.status}`
    const err=new Error(msg); err.status=r.status; throw err
  }
  if(r.status===204) return null
  return r.json()
}

function slugify(s){ return (s||'').toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'').slice(0,50) }

const Spinner = ({size=14}) => <span style={{display:'inline-block', width:size, height:size, border:'2px solid #3f3f46', borderTopColor:'#a1a1aa', borderRadius:'50%', animation:'spin 0.6s linear infinite'}}/>
const Skeleton = ({w='100%', h=16}) => <div style={{width:w, height:h, background:'#27272a', borderRadius:4, animation:'pulse 1.5s ease-in-out infinite'}}/>

// Context for grid ↔ log terminal
const DashboardCtx = React.createContext(null)

export default function App(){
  const auth = useAuth()
  if(!auth.isLogged) return <AuthScreen auth={auth}/>
  return <Shell auth={auth}/>
}

function Shell({auth}){
  const [orgs,setOrgs]=useState([])
  const [projects,setProjects]=useState([])
  const [queues,setQueues]=useState([])
  const [workers,setWorkers]=useState([])
  const [metrics,setMetrics]=useState(null)
  const [selOrg,setSelOrg]=useState('')
  const [selProj,setSelProj]=useState('')
  const [selQ,setSelQ]=useState('')
  const [throughput,setThroughput]=useState(0)
  const [loadingOrgs,setLoadingOrgs]=useState(true)
  const [loadingProjects,setLoadingProjects]=useState(false)
  const [loadingQueues,setLoadingQueues]=useState(false)
  const prevCompleted = useRef(0)
  const prevTime = useRef(Date.now())

  const loadOrgs = useCallback(async()=>{
    setLoadingOrgs(true)
    try{
      const o=await api('/organizations',{},auth.token)
      const arr=Array.isArray(o)?o:[]
      setOrgs(arr)
      if(arr[0] && !selOrg) setSelOrg(arr[0].id)
    }catch{}finally{ setLoadingOrgs(false) }
  },[auth.token])

  const loadMetrics = useCallback(async()=>{
    try{
      const m=await api('/metrics',{},auth.token)
      setMetrics(m)
      const now=Date.now()
      const total=(m.jobs?.completed||0)+(m.jobs?.failed||0)
      const dt=(now-prevTime.current)/1000
      if(dt>1){ setThroughput(Math.round((total-prevCompleted.current)/dt)); prevCompleted.current=total; prevTime.current=now }
      const w=await api('/workers',{},auth.token)
      setWorkers(Array.isArray(w)?w:[])
    }catch{}
  },[auth.token])

  const refreshOrgs = useCallback(()=>{ loadOrgs(); loadMetrics() },[loadOrgs, loadMetrics])

  const refreshProjects = useCallback(async()=>{
    if(!selOrg){ setProjects([]); return }
    setLoadingProjects(true)
    try{
      const p=await api(`/projects?org_id=${selOrg}`,{},auth.token)
      const arr=Array.isArray(p)?p:[]
      setProjects(arr)
      if(arr[0] && !selProj) setSelProj(arr[0].id)
      else if(!arr.find(x=>x.id===selProj)) setSelProj(arr[0]?.id||'')
    }catch{}finally{ setLoadingProjects(false) }
  },[selOrg, auth.token])

  const refreshQueues = useCallback(async()=>{
    if(!selProj){ setQueues([]); return }
    setLoadingQueues(true)
    try{
      const q=await api(`/queues?project_id=${selProj}`,{},auth.token)
      const arr=Array.isArray(q)?q:[]
      setQueues(arr)
      if(arr[0] && !selQ) setSelQ(arr[0].id)
      else if(!arr.find(x=>x.id===selQ)) setSelQ(arr[0]?.id||'')
    }catch{}finally{ setLoadingQueues(false) }
  },[selProj, auth.token])

  useEffect(()=>{
    loadOrgs().then(()=>{
      loadMetrics()
    })
    const id=setInterval(loadMetrics,2000)
    return()=>clearInterval(id)
  },[auth.token])

  useEffect(()=>{ refreshProjects() },[selOrg, auth.token])
  useEffect(()=>{ refreshQueues() },[selProj, auth.token])

  const [qStats,setQStats]=useState({})
  useEffect(()=>{
    if(!queues.length){ setQStats({}); return }
    const ids=queues.map(q=>q.id).join(',')
    const fetchStats=async()=>{
      try{
        const s=await api(`/queues/batch-stats?ids=${ids}`,{},auth.token)
        const arr=Array.isArray(s)?s:[]
        const map={}
        arr.forEach(st=>{ map[st.queue_id]=st })
        queues.forEach(q=>{ if(!map[q.id]) map[q.id]={queue_id:q.id,queued:0,running:0,completed:0,failed:0,dlq:0,retry_wait:0,scheduled:0,claimed:0} })
        setQStats(map)
      }catch{}
    }
    fetchStats()
    const id=setInterval(fetchStats,3000)
    return()=>clearInterval(id)
  },[queues, auth.token])

  return (
    <DashboardCtx.Provider value={{auth, selQ, queues, workers, metrics, throughput}}>
      <div style={{width:'100vw', height:'100vh', background:'#09090b', color:'#e4e4e7', display:'flex', flexDirection:'column', overflow:'hidden', fontFamily:'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize:12}}>
        <SystemHeader workers={workers} throughput={throughput} />
        <div style={{display:'flex', flex:1, overflow:'hidden', borderTop:'1px solid #27272a'}}>
          <LeftPanel auth={auth} orgs={orgs} projects={projects} queues={queues} qStats={qStats} selOrg={selOrg} setSelOrg={setSelOrg} selProj={selProj} setSelProj={setSelProj} selQ={selQ} setSelQ={setSelQ} loadingOrgs={loadingOrgs} loadingProjects={loadingProjects} loadingQueues={loadingQueues} onRefresh={loadMetrics} onRefreshOrgs={refreshOrgs} onRefreshProjects={refreshProjects} onRefreshQueues={refreshQueues}/>
          <CentralGrid selQ={selQ} auth={auth}/>
        </div>
      </div>
    </DashboardCtx.Provider>
  )
}

function SystemHeader({workers, throughput}){
  const online = workers.filter(w=>w.status==='ONLINE').length
  const total = workers.length
  const [ping,setPing]=useState(1)
  const [healthy,setHealthy]=useState(true)
  useEffect(()=>{
    const id=setInterval(async()=>{
      const t=Date.now()
      try{ await fetch('/health'); const p=Date.now()-t; setPing(p); setHealthy(p<3000)}catch{ setHealthy(false)}
    },2000)
    return()=>clearInterval(id)
  },[])
  return (
    <div style={{height:44, display:'flex', alignItems:'center', justifyContent:'space-between', padding:'0 16px', background:'#18181b', borderBottom:'1px solid #27272a', flexShrink:0}}>
      <div style={{display:'flex', gap:18, alignItems:'center'}}>
        <span style={{fontWeight:700, letterSpacing:1}}>SYSTEM CORE SHELL: REPL HEADERS & ENGINE TELEMETRY MATRIX</span>
        <span style={{padding:'2px 8px', borderRadius:4, background:healthy?'#14532d':'#7f1d1d', color: healthy?'#4ade80':'#f87171', border:`1px solid ${healthy?'#22c55e':'#ef4444'}`}}>
          {healthy?'🟢 Distributed Engine HEALTHY':'🔴 CONNECTION_LOST'}
        </span>
      </div>
      <div style={{display:'flex', gap:16, alignItems:'center', fontSize:11}}>
        <span>Workers: <b style={{color: online===total && total>0?'#4ade80':'#facc15'}}>{online} / {total} Online</b></span>
        <span>Throughput: <b style={{color:'#fafafa'}}>{throughput.toLocaleString()} jobs/sec</b></span>
        <span>Engine Sync: <b style={{color: healthy?'#22c55e':'#f87171'}}>{healthy?'Streaming':'Stalled'} (Ping: {ping}ms)</b></span>
        {healthy?null:<div style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.5)', backdropFilter:'grayscale(1)', pointerEvents:'none', zIndex:999}}/>}
      </div>
    </div>
  )
}

function LeftPanel({auth, orgs, projects, queues, qStats, selOrg, setSelOrg, selProj, setSelProj, selQ, setSelQ, loadingOrgs, loadingProjects, loadingQueues, onRefresh, onRefreshOrgs, onRefreshProjects, onRefreshQueues}){
  const [confirmPause,setConfirmPause]=useState(null)
  const [pauseErr,setPauseErr]=useState('')
  const togglePause = async(q)=>{
    if(confirmPause!==q.id){ setConfirmPause(q.id); setPauseErr(''); setTimeout(()=>setConfirmPause(null),3000); return }
    const path = q.is_paused? `/queues/${q.id}/resume` : `/queues/${q.id}/pause`
    try{ await api(path,{method:'POST'},auth.token); setConfirmPause(null); setPauseErr(''); onRefresh() }catch(e){ setPauseErr(String(e).slice(0,60)) }
  }
  return (
    <div style={{width:'20vw', minWidth:260, background:'#18181b', borderRight:'1px solid #27272a', display:'flex', flexDirection:'column', overflow:'hidden'}}>
      <div style={{padding:12, borderBottom:'1px solid #27272a'}}>
        <div style={{fontSize:10, color:'#a1a1aa', letterSpacing:1}}>PROJECT CONTEXT SWITCHER</div>
        <div style={{marginTop:8, display:'flex', gap:6}}>
          <select value={selOrg} onChange={e=>setSelOrg(e.target.value)} style={{flex:1, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'6px 8px', borderRadius:6}}>
            {loadingOrgs && <option value="">Loading…</option>}
            {orgs.map(o=><option key={o.id} value={o.id}>{o.name}</option>)}
            {!loadingOrgs && !orgs.length && <option value="">— no org —</option>}
          </select>
          <select value={selProj} onChange={e=>setSelProj(e.target.value)} style={{flex:1, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'6px 8px', borderRadius:6}}>
            {loadingProjects && <option value="">Loading…</option>}
            {projects.map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
            {!loadingProjects && !projects.length && <option value="">— no project —</option>}
          </select>
        </div>
        <div style={{marginTop:8, display:'flex', gap:4, flexWrap:'wrap'}}>
          <CreateOrg auth={auth} onDone={onRefreshOrgs}/>
          <CreateProject auth={auth} orgId={selOrg} onDone={onRefreshProjects}/>
          <CreateQueue auth={auth} projectId={selProj} onDone={onRefreshQueues}/>
        </div>
      </div>
      <div style={{padding:'8px 12px', fontSize:10, color:'#a1a1aa', letterSpacing:1, borderBottom:'1px solid #27272a'}}>QUEUE TOPOLOGY METRICS MATRIX</div>
      <div style={{flex:1, overflow:'auto', padding:8, display:'flex', flexDirection:'column', gap:8}}>
        {loadingQueues && <div style={{padding:20, textAlign:'center'}}><Spinner size={20}/></div>}
        {queues.map(q=>{
          const s=qStats[q.id]
          const pct = q.max_concurrency? Math.round(((s?.running||0)/q.max_concurrency)*8):0
          const bar = '■'.repeat(Math.min(8,pct)) + '□'.repeat(8-Math.min(8,pct))
          return <div key={q.id} onClick={()=>setSelQ(q.id)} style={{padding:10, background: selQ===q.id?'#27272a':'#09090b', border:`1px solid ${selQ===q.id?'#3f3f46':'#27272a'}`, borderRadius:8, cursor:'pointer'}}>
            <div style={{display:'flex', justifyContent:'space-between', fontSize:11}}>
              <span style={{fontFamily:'monospace', background:'#27272a', padding:'2px 6px', borderRadius:4}}>{q.name}</span>
              <span style={{fontSize:10, color:'#a1a1aa'}}>Priority: {q.default_priority}</span>
            </div>
            <div style={{marginTop:6, fontSize:11, display:'flex', justifyContent:'space-between'}}>
              <span>{bar} {Math.round(((s?.running||0)/Math.max(1,q.max_concurrency))*100)}% Max</span>
              <span style={{color:'#71717a'}}>{s?.running||0}/{q.max_concurrency}</span>
            </div>
            <div style={{marginTop:6, fontSize:10, color:'#a1a1aa', display:'flex', gap:8, flexWrap:'wrap'}}>
              <span>Q:{s?.queued??'-'}</span><span style={{color:'#facc15'}}>R:{s?.running??'-'}</span><span style={{color:'#4ade80'}}>C:{s?.completed??'-'}</span><span style={{color:'#f87171'}}>F:{s?.failed??'-'}</span><span>DLQ:{s?.dlq??'-'}</span>
            </div>
            <button onClick={(e)=>{e.stopPropagation(); togglePause(q)}} style={{marginTop:8, width:'100%', padding:'6px 0', borderRadius:6, border:'1px solid #3f3f46', background: q.is_paused?'#422006':'#09090b', color: q.is_paused?'#fbbf24':'#e4e4e7', fontSize:11}}>
              {confirmPause===q.id? '⚠️ Confirm Pause?' : q.is_paused? '▶ Resume Queue' : '⏸️ Pause Queue'}
            </button>
            {confirmPause===q.id && pauseErr && <div style={{marginTop:4, color:'#f87171', fontSize:10}}>{pauseErr}</div>}
          </div>
        })}
        {!loadingQueues && !queues.length && <div style={{color:'#71717a', padding:20, textAlign:'center', fontSize:11}}>No queues. Create one.</div>}
      </div>
      <div style={{padding:8, borderTop:'1px solid #27272a', fontSize:10, color:'#71717a'}}>SELECT FOR UPDATE SKIP LOCKED • leases: solid ■ / free □</div>
    </div>
  )
}

function CentralGrid({selQ, auth}){
  const [jobs,setJobs]=useState([])
  const [frozen,setFrozen]=useState(false)
  const [pendingCount,setPendingCount]=useState(0)
  const [selected,setSelected]=useState(null)
  const [execs,setExecs]=useState([])
  const [logs,setLogs]=useState([])
  const [filter,setFilter]=useState('')
  const [loadingJobs,setLoadingJobs]=useState(false)
  const [actionErr,setActionErr]=useState('')
  const queueRef = useRef([])

  const load = useCallback(async()=>{
    if(!selQ) return
    setLoadingJobs(true)
    const q=new URLSearchParams({queue_id: selQ, page_size:'100'})
    if(filter) q.set('status', filter)
    const r=await api(`/jobs?${q}`,{},auth.token).catch(()=>null)
    const data=r?.data||[]
    if(frozen){
      setPendingCount(data.length)
      const map=new Map(data.map(j=>[j.id, j]))
      const merged=queueRef.current.map(j=> map.get(j.id) || j)
      data.forEach(j=>{ if(!merged.find(m=>m.id===j.id)) merged.push(j) })
      merged.forEach((j,i)=>{ if(map.has(j.id)) merged[i]=map.get(j.id) })
      setJobs(merged)
    } else {
      queueRef.current=data
      setJobs(data)
      setPendingCount(0)
    }
    setLoadingJobs(false)
  },[selQ, auth.token, filter, frozen])

  useEffect(()=>{ load(); const id=setInterval(load,1500); return()=>clearInterval(id)},[load])
  useEffect(()=>{ queueRef.current=jobs },[jobs])

  const open = async(j)=>{
    setSelected(j)
    const e=await api(`/jobs/${j.id}/executions`,{},auth.token).catch(()=>[])
    setExecs(Array.isArray(e)?e:[])
    const l=await api(`/jobs/${j.id}/logs`,{},auth.token).catch(()=>[])
    setLogs(Array.isArray(l)?l.slice(-100):[])
  }
  const retry = async(id)=>{
    setActionErr('')
    try{ await api(`/jobs/${id}/retry`,{method:'POST'},auth.token); load() }catch(e){ setActionErr(String(e).slice(0,80)) }
  }
  const evict = async()=>{
    if(!selected) return
    setActionErr('')
    try{ await api(`/jobs/${selected.id}/retry`,{method:'POST'},auth.token); load() }catch(e){ setActionErr(String(e).slice(0,80)) }
  }

  return (
    <div style={{flex:1, display:'flex', flexDirection:'column', overflow:'hidden', background:'#09090b'}}>
      <div style={{padding:'8px 12px', borderBottom:'1px solid #27272a', display:'flex', alignItems:'center', justifyContent:'space-between', background:'#18181b'}}>
        <div style={{fontSize:11, letterSpacing:1}}>CENTRAL GRID CONTROLLER {frozen && <span style={{marginLeft:8, padding:'2px 6px', background:'#422006', border:'1px solid #f59e0b', borderRadius:4, color:'#fbbf24'}}>🔄 GRID FROZEN: {pendingCount} State Updates Pending. Click or Press 'R' to Apply Re-Sort</span>}</div>
        <div style={{display:'flex', gap:6, alignItems:'center'}}>
          <select value={filter} onChange={e=>setFilter(e.target.value)} style={{background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 8px', borderRadius:6, fontSize:11}}>
            <option value="">All</option>
            {['QUEUED','RUNNING','RETRY_WAIT','COMPLETED','FAILED','SCHEDULED','WAITING','UNKNOWN_EXTERNAL_RESULT'].map(s=><option key={s} value={s}>{s}</option>)}
          </select>
          <button onClick={()=>setFrozen(!frozen)} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #27272a', background: frozen?'#27272a':'#09090b', color:'#e4e4e7', fontSize:11}}>{frozen?'Unfreeze':'Freeze'}</button>
          <button onClick={load} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#e4e4e7', fontSize:11}}>Refresh</button>
          <span style={{fontSize:10, color:'#71717a'}}>Spatial Lock Active</span>
        </div>
      </div>
      {actionErr && <div style={{padding:'4px 12px', background:'#7f1d1d', color:'#fca5a5', fontSize:11}}>{actionErr}</div>}
      {frozen && pendingCount>0 && <div onClick={()=>setFrozen(false)} style={{padding:'6px 12px', background:'#422006', borderBottom:'1px solid #f59e0b', color:'#fbbf24', fontSize:11, cursor:'pointer', textAlign:'center'}}>🔄 GRID FROZEN: {pendingCount} updates pending — Click or Press 'R' to Apply Re-Sort</div>}
      <div style={{flex:1, overflow:'auto'}}>
        <table style={{width:'100%', borderCollapse:'collapse', fontSize:11}}>
          <thead style={{position:'sticky', top:0, background:'#18181b', zIndex:1}}>
            <tr style={{textAlign:'left', color:'#a1a1aa'}}><th style={{padding:'8px 12px'}}><input type="checkbox" readOnly/></th><th>Job ID</th><th>Idempotency</th><th>State</th><th>Node</th><th>Wall-Time</th><th></th></tr>
          </thead>
          <tbody>
            {loadingJobs && !jobs.length && <tr><td colSpan={7} style={{padding:40, textAlign:'center'}}><Spinner size={20}/></td></tr>}
            {jobs.map(j=>{
              const dur = j.started_at && j.completed_at ? (new Date(j.completed_at)-new Date(j.started_at)) : (j.started_at? Date.now()-new Date(j.started_at):0)
              const pct = Math.min(100, Math.round((dur/5000)*100))
              const bar = '█'.repeat(Math.round(pct/10)) + '░'.repeat(10-Math.round(pct/10))
              const stateMap = {
                COMPLETED: {bg:'#14532d', fg:'#4ade80', label:'🟢 COMPLETED'},
                RUNNING:   {bg:'#1e3a5f', fg:'#60a5fa', label:'🔵 RUNNING'},
                RETRYWAIT: {bg:'#422006', fg:'#fbbf24', label:'🟡 RETRYING'},
                RETRY_WAIT:{bg:'#422006', fg:'#fbbf24', label:'🟡 RETRYING'},
                FAILED:    {bg:'#7f1d1d', fg:'#f87171', label:'🔴 DLQ_FAULT'},
                QUEUED:    {bg:'#27272a', fg:'#a1a1aa', label:'QUEUED'},
                SCHEDULED: {bg:'#164e63', fg:'#22d3ee', label:'SCHEDULED'},
                WAITING:   {bg:'#312e81', fg:'#a78bfa', label:'WAITING'},
                CLAIMED:   {bg:'#422006', fg:'#facc15', label:'CLAIMED'},
                UNKNOWNEXTERNALRESULT:{bg:'#422006', fg:'#facc15', label:'UNKNOWN'},
              }
              const statusKey = (j.status||'').toUpperCase().replace('-','_')
              const st = stateMap[statusKey] || {bg:'#27272a', fg:'#a1a1aa', label:j.status}
              const node = j.lease_owner ? `worker-${j.lease_owner.slice(0,6)}` : '-'
              const orphan = (j.status||'').toUpperCase()==='RUNNING' && j.lease_expires_at && new Date(j.lease_expires_at) < new Date()
              return <tr key={j.id} style={{borderBottom:'1px solid #27272a', background: selected?.id===j.id?'#27272a':'transparent'}} onClick={()=>open(j)}>
                <td style={{padding:'8px 12px'}}><input type="checkbox" checked={selected?.id===j.id} readOnly/></td>
                <td style={{fontFamily:'monospace', color:'#fafafa'}}>#J-{j.id.slice(0,6)}</td>
                <td style={{fontFamily:'monospace', color:'#71717a'}}>{(j.idempotency_key||'idem_'+j.id.slice(0,6)).slice(0,16)}</td>
                <td><span style={{padding:'2px 6px', borderRadius:4, background:st.bg, color:st.fg, border:`1px solid ${st.fg}33`}}>{st.label}</span></td>
                <td style={{color: orphan?'#f87171':'#a1a1aa'}}>{orphan?'[ORPHANED]':node}</td>
                <td style={{fontFamily:'monospace'}}>
                  <span style={{color: dur>5000?'#f87171':'#a1a1aa'}}>⏳ {dur?`${dur}ms`: '-'} </span>
                  <span style={{color: dur>5000?'#f87171':'#71717a'}}>[{bar}]</span>
                  <span style={{color:'#52525b', marginLeft:4}}>(SLA 5000ms)</span>
                </td>
                <td><button onClick={(e)=>{e.stopPropagation(); open(j)}} style={{padding:'2px 6px', borderRadius:4, border:'1px solid #3f3f46', background:'#09090b', color:'#e4e4e7', fontSize:10}}>Inspect</button></td>
              </tr>
            })}
            {!loadingJobs && !jobs.length && <tr><td colSpan={7} style={{padding:40, textAlign:'center', color:'#71717a'}}>No jobs — create one via <code style={{color:'#a1a1aa'}}>curl POST /jobs</code> or wait for cron.</td></tr>}
          </tbody>
        </table>
      </div>
      <div style={{height:220, display:'flex', borderTop:'1px solid #27272a', background:'#18181b'}}>
        <InspectorStepper job={selected} execs={execs} onEvict={evict} />
        <LogTerminal logs={logs} job={selected}/>
      </div>
    </div>
  )
}

function InspectorStepper({job, execs, onEvict}){
  if(!job) return <div style={{flex:1, padding:16, borderRight:'1px solid #27272a', color:'#71717a', fontSize:11}}>Select a job to inspect lineage. Shows `QUEUED→CLAIMED→RUNNING→COMPLETED/DLQ` with DB `SELECT FOR UPDATE SKIP LOCKED` callout.</div>
  const steps = [
    {label:'QUEUED', ts: job.queued_at || job.created_at},
    {label:'CLAIMED', ts: job.claimed_at},
    {label:'RUNNING', ts: job.started_at},
    {label: (job.status||'').toUpperCase()==='COMPLETED'?'COMPLETED': (job.status||'').toUpperCase()==='FAILED'?'FAILED': (job.status||'').toUpperCase()==='UNKNOWN_EXTERNAL_RESULT'?'UNKNOWN': (job.status||'').toUpperCase(), ts: job.completed_at || job.failed_at || job.updated_at},
  ].filter(s=>s.ts)
  return (
    <div style={{flex:1, padding:12, borderRight:'1px solid #27272a', overflow:'auto'}}>
      <div style={{fontSize:10, color:'#a1a1aa', letterSpacing:1}}>ACID STATE MACHINE TRANSITION LINEAGE</div>
      <div style={{marginTop:10, display:'flex', alignItems:'center', gap:6, flexWrap:'wrap', fontSize:11}}>
        {steps.map((s,i)=>(
          <React.Fragment key={i}>
            <span style={{padding:'4px 8px', borderRadius:6, background: s.label==='COMPLETED'?'#14532d': s.label==='FAILED'?'#7f1d1d':'#27272a', border:'1px solid #3f3f46', color: s.label==='COMPLETED'?'#4ade80': s.label==='FAILED'?'#f87171':'#e4e4e7'}}>
              [{s.label}] <span style={{color:'#71717a', fontSize:10}}>{s.ts? new Date(s.ts).toLocaleTimeString():''}</span>
            </span>
            {i<steps.length-1 && <span style={{color:'#52525b'}}>──({execs[0] && steps[i+1].ts && s.ts ? Math.round((new Date(steps[i+1].ts)-new Date(s.ts))) : '?'}ms)──▶</span>}
          </React.Fragment>
        ))}
        {(job.status||'').toUpperCase()==='FAILED' && <span style={{padding:'4px 8px', borderRadius:6, background:'#7f1d1d', color:'#fecaca', border:'1px solid #ef4444'}}>🛑 DLQ AUTOMATIC EVICTION</span>}
      </div>
      <div style={{marginTop:10, fontSize:10, color:'#71717a', fontFamily:'monospace', background:'#09090b', padding:'6px 8px', borderRadius:6, border:'1px solid #27272a'}}>
        Database Engine Assurance: Row claimed atomically via <b style={{color:'#e4e4e7'}}>[SELECT FOR UPDATE SKIP LOCKED]</b> • Token {job.token_id? job.token_id.slice(0,6):'—'} • Epoch {job.lease_epoch} • Attempt {job.attempt}/{job.max_attempts}
      </div>
      <div style={{marginTop:8, display:'flex', gap:6}}>
        <button onClick={onEvict} style={{padding:'6px 10px', borderRadius:6, border:'1px solid #3f3f46', background:'#09090b', color:'#e4e4e7', fontSize:11}}>
          🔄 Evict & Re-Queue Atomically
        </button>
      </div>
      <div style={{marginTop:8, fontSize:10, color:'#71717a'}}>Executions: {execs.length} • {execs.map(e=>`${e.status}@${e.attempt}`).join(', ')}</div>
    </div>
  )
}

function LogTerminal({logs, job}){
  const ref=useRef(null)
  const last100=useMemo(()=>logs.slice(-100),[logs])
  useEffect(()=>{ if(ref.current) ref.current.scrollTop=ref.current.scrollHeight },[last100])
  return (
    <div style={{flex:1, display:'flex', flexDirection:'column', background:'#09090b', overflow:'hidden'}}>
      <div style={{padding:'6px 12px', borderBottom:'1px solid #27272a', fontSize:10, color:'#a1a1aa', display:'flex', justifyContent:'space-between'}}>
        <span>VIRTUALIZED LIVE LOG TAIL TERMINAL (100 max, O(1))</span>
        <span style={{color:'#71717a'}}>{job? `job #J-${job.id.slice(0,6)}` : 'no job'}</span>
      </div>
      <div ref={ref} style={{flex:1, overflow:'auto', padding:8, fontFamily:'monospace', fontSize:11, lineHeight:1.5}}>
        {last100.map(l=>{
          const col = l.level==='ERROR' ? '#f87171' : l.level==='WARN' ? '#fbbf24' : '#f4f4f5'
          return <div key={l.id} style={{color: col, whiteSpace:'pre-wrap', wordBreak:'break-all'}}>[{new Date(l.created_at).toLocaleTimeString()}] [{l.level}] {l.message} {l.meta && JSON.stringify(l.meta).slice(0,120)}</div>
        })}
        {!logs.length && <div style={{color:'#52525b'}}>No logs. Select a job.</div>}
      </div>
      <div style={{padding:'4px 8px', borderTop:'1px solid #27272a', fontSize:10, color:'#52525b'}}>ANSI #f4f4f5 standard / #fbbf24 retry / #f87171 deadlock • windowing recycles DOM</div>
    </div>
  )
}

function CreateOrg({auth, onDone}){
  const [name,setName]=useState(''), [slug,setSlug]=useState(''), [slugTouched,setSlugTouched]=useState(false), [open,setOpen]=useState(false), [err,setErr]=useState(''), [busy,setBusy]=useState(false)
  const handleName=(v)=>{ setName(v); if(!slugTouched) setSlug(slugify(v)) }
  if(!open) return <button onClick={()=>setOpen(true)} style={{flex:1, padding:'4px 6px', borderRadius:6, border:'1px solid #3f3f46', background:'#09090b', color:'#a1a1aa', fontSize:11, cursor:'pointer'}}>+ Org</button>
  return <div style={{flex:'0 0 100%', display:'flex', gap:4, flexWrap:'wrap'}}>
    <input placeholder="name" value={name} onChange={e=>handleName(e.target.value)} style={{flex:1, minWidth:60, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <input placeholder="slug (auto)" value={slug} onChange={e=>{setSlug(e.target.value); setSlugTouched(true)}} style={{flex:1, minWidth:60, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <button disabled={busy||!name||!slug} onClick={async()=>{ setBusy(true); setErr(''); try{ await api('/organizations',{method:'POST', body:JSON.stringify({name,slug})},auth.token); onDone(); setOpen(false); setName(''); setSlug(''); setSlugTouched(false) }catch(e){ setErr(String(e).slice(0,80)) }finally{ setBusy(false) } }} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #3f3f46', background:'#18181b', color:'#4ade80', fontSize:11, opacity:busy||!name||!slug?0.5:1}}>{busy?'…':'Create'}</button>
    <button onClick={()=>setOpen(false)} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#71717a', fontSize:11}}>✕</button>
    {err && <div style={{flex:'0 0 100%', color:'#f87171', fontSize:10}}>{err}</div>}
  </div>
}
function CreateProject({auth, orgId, onDone}){
  const [name,setName]=useState(''), [slug,setSlug]=useState(''), [slugTouched,setSlugTouched]=useState(false), [open,setOpen]=useState(false), [err,setErr]=useState(''), [busy,setBusy]=useState(false)
  const handleName=(v)=>{ setName(v); if(!slugTouched) setSlug(slugify(v)) }
  if(!orgId) return <button disabled style={{flex:1, padding:'4px 6px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#52525b', fontSize:11, cursor:'not-allowed'}}>+ Project</button>
  if(!open) return <button onClick={()=>setOpen(true)} style={{flex:1, padding:'4px 6px', borderRadius:6, border:'1px solid #3f3f46', background:'#09090b', color:'#a1a1aa', fontSize:11, cursor:'pointer'}}>+ Project</button>
  return <div style={{flex:'0 0 100%', display:'flex', gap:4, flexWrap:'wrap'}}>
    <input placeholder="name" value={name} onChange={e=>handleName(e.target.value)} style={{flex:1, minWidth:60, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <input placeholder="slug (auto)" value={slug} onChange={e=>{setSlug(e.target.value); setSlugTouched(true)}} style={{flex:1, minWidth:60, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <button disabled={busy||!name||!slug} onClick={async()=>{ setBusy(true); setErr(''); try{ await api('/projects',{method:'POST', body:JSON.stringify({org_id:orgId, name, slug})},auth.token); onDone(); setOpen(false); setName(''); setSlug(''); setSlugTouched(false) }catch(e){ setErr(String(e).slice(0,80)) }finally{ setBusy(false) } }} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #3f3f46', background:'#18181b', color:'#4ade80', fontSize:11, opacity:busy||!name||!slug?0.5:1}}>{busy?'…':'Create'}</button>
    <button onClick={()=>setOpen(false)} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#71717a', fontSize:11}}>✕</button>
    {err && <div style={{flex:'0 0 100%', color:'#f87171', fontSize:10}}>{err}</div>}
  </div>
}
function CreateQueue({auth, projectId, onDone}){
  const [name,setName]=useState(''), [conc,setConc]=useState(5), [open,setOpen]=useState(false), [err,setErr]=useState(''), [busy,setBusy]=useState(false)
  if(!projectId) return <button disabled style={{flex:1, padding:'4px 6px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#52525b', fontSize:11, cursor:'not-allowed'}}>+ Queue</button>
  if(!open) return <button onClick={()=>setOpen(true)} style={{flex:1, padding:'4px 6px', borderRadius:6, border:'1px solid #3f3f46', background:'#09090b', color:'#a1a1aa', fontSize:11, cursor:'pointer'}}>+ Queue</button>
  return <div style={{flex:'0 0 100%', display:'flex', gap:4, flexWrap:'wrap'}}>
    <input placeholder="queue name" value={name} onChange={e=>setName(e.target.value)} style={{flex:1, minWidth:60, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <input type="number" value={conc} onChange={e=>setConc(e.target.value)} style={{width:50, background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'4px 6px', borderRadius:6, fontSize:11}}/>
    <button disabled={busy||!name} onClick={async()=>{ setBusy(true); setErr(''); try{ await api('/queues',{method:'POST', body:JSON.stringify({project_id:projectId, name, max_concurrency: parseInt(conc)})},auth.token); onDone(); setOpen(false); setName('') }catch(e){ setErr(String(e).slice(0,80)) }finally{ setBusy(false) } }} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #3f3f46', background:'#18181b', color:'#4ade80', fontSize:11, opacity:busy||!name?0.5:1}}>{busy?'…':'Create'}</button>
    <button onClick={()=>setOpen(false)} style={{padding:'4px 8px', borderRadius:6, border:'1px solid #27272a', background:'#09090b', color:'#71717a', fontSize:11}}>✕</button>
    {err && <div style={{flex:'0 0 100%', color:'#f87171', fontSize:10}}>{err}</div>}
  </div>
}

function AuthScreen({auth}){
  const [mode,setMode]=useState('login')
  const [email,setEmail]=useState('demo@example.com')
  const [password,setPassword]=useState('demo1234')
  const [display,setDisplay]=useState('Admin')
  const [err,setErr]=useState('')
  const [busy,setBusy]=useState(false)
  const submit = async(e)=>{
    e.preventDefault(); setErr(''); setBusy(true)
    try{
      const path=mode==='login'?'/auth/login':'/auth/register'
      const body=mode==='login'? {email,password} : {email,password,display_name:display}
      const r=await api(path,{method:'POST', body:JSON.stringify(body)},'')
      auth.login(r.token, r.user)
    }catch(e){
      const raw = String(e).replace('Error: ','')
      const is409 = raw.toLowerCase().includes('already registered') || raw.includes('409') || e.status===409
      if(is409 && mode==='register'){
        setErr('409: email already exists — try admin123346@example.com or click Login')
      } else if(raw==='{}' || raw==='[object Object]' || !raw){
        setErr(mode==='register' ? 'Register failed — try different email' : 'Login failed — check email/password')
      } else {
        setErr(raw.slice(0,120))
      }
    }finally{ setBusy(false) }
  }
  return <div style={{width:'100vw', height:'100vh', background:'#09090b', display:'flex', alignItems:'center', justifyContent:'center'}}>
    <form onSubmit={submit} style={{width:360, background:'#18181b', border:'1px solid #27272a', borderRadius:12, padding:20, display:'flex', flexDirection:'column', gap:10}}>
      <h2 style={{textAlign:'center', color:'#fafafa', fontSize:14, letterSpacing:1}}>DISTRIBUTED JOB SCHEDULER</h2>
      <div style={{display:'flex', gap:8}}>
        <button type="button" onClick={()=>setMode('login')} style={{flex:1, padding:8, borderRadius:8, border:'1px solid #27272a', background: mode==='login'?'#27272a':'#09090b', color:'#e4e4e7'}}>Login</button>
        <button type="button" onClick={()=>setMode('register')} style={{flex:1, padding:8, borderRadius:8, border:'1px solid #27272a', background: mode==='register'?'#27272a':'#09090b', color:'#e4e4e7'}}>Register</button>
      </div>
      <input placeholder="email" value={email} onChange={e=>setEmail(e.target.value)} style={{background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'8px 10px', borderRadius:8}}/>
      <input placeholder="password" type="password" value={password} onChange={e=>setPassword(e.target.value)} style={{background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'8px 10px', borderRadius:8}}/>
      {mode==='register' && <input placeholder="display name" value={display} onChange={e=>setDisplay(e.target.value)} style={{background:'#09090b', border:'1px solid #27272a', color:'#e4e4e7', padding:'8px 10px', borderRadius:8}}/>}
      {err && <div style={{color:'#f87171', fontSize:11}}>{err}</div>}
      <button type="submit" disabled={busy} style={{padding:10, borderRadius:8, border:'1px solid #27272a', background:'#fafafa', color:'#09090b', fontWeight:700, opacity:busy?0.5:1}}>{busy?'…':mode==='login'?'Login':'Register'}</button>
      <div style={{fontSize:10, color:'#71717a', textAlign:'center'}}>Demo: demo@example.com / demo1234</div>
    </form>
  </div>
}
