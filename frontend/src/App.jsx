import React,{useCallback,useEffect,useRef,useState} from 'react'
import { api } from './lib/api'
import { slug } from './lib/format'
import { useAuth } from './hooks/useAuth'
import ErrorBoundary from './components/ErrorBoundary'
import { Metric, QueueCharts } from './components/Metrics'
import { ThroughputChart, StatusDonut, LifecycleStrip } from './components/Charts'
import { ActivityFeed } from './components/ActivityFeed'
import { relTime, fmtTimeIST } from './lib/format'
import { JobsPanel, PAGE_SIZE } from './components/JobsPanel'
import { JobDetails } from './components/JobDetails'
import { DlqPanel } from './components/DlqPanel'
import { WorkersPanel } from './components/WorkersPanel'
import { EntityForm, JobForm } from './components/Modals'
import { Auth } from './components/Auth'

export default function App(){const a=useAuth();return <ErrorBoundary>{a.signed?<Dashboard auth={a}/>:<Auth auth={a}/>}</ErrorBoundary>}

function Dashboard({auth}){
  const[orgs,setOrgs]=useState([]),[projects,setProjects]=useState([]),[queues,setQueues]=useState([])
  const[stats,setStats]=useState({}),[workers,setWorkers]=useState([]),[healthy,setHealthy]=useState(true)
  const[org,setOrg]=useState(''),[project,setProject]=useState(''),[queue,setQueue]=useState('')
  const[modal,setModal]=useState(null),[note,setNote]=useState(null)
  const[live,setLive]=useState(false),[recent,setRecent]=useState([])

  useEffect(()=>{const expire=()=>auth.logout();window.addEventListener('jobflow:session-expired',expire);return()=>window.removeEventListener('jobflow:session-expired',expire)},[auth])
  const loadOrgs=useCallback(async()=>{const x=await api('/organizations',{},auth.token);setOrgs(x);setOrg(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token])
  const loadProjects=useCallback(async()=>{if(!org){setProjects([]);setProject('');return}const x=await api(`/projects?org_id=${org}`,{},auth.token);setProjects(x);setProject(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token,org])
  const loadQueues=useCallback(async()=>{if(!project){setQueues([]);setQueue('');return}const x=await api(`/queues?project_id=${project}`,{},auth.token);setQueues(x);setQueue(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token,project])
  const refresh=async()=>{await Promise.all([loadOrgs(),loadProjects(),loadQueues()])}
  useEffect(()=>{loadOrgs().catch(e=>setNote({e:1,t:e.message}))},[loadOrgs])
  useEffect(()=>{loadProjects().catch(e=>setNote({e:1,t:e.message}))},[loadProjects])
  useEffect(()=>{loadQueues().catch(e=>setNote({e:1,t:e.message}))},[loadQueues])

  useEffect(()=>{const f=async()=>{setWorkers(await api('/workers',{},auth.token).catch(()=>[]))};f();const i=setInterval(f,8000);return()=>clearInterval(i)},[auth.token])
  useEffect(()=>{const f=async()=>{try{await api('/health');setHealthy(true)}catch{setHealthy(false)}};f();const i=setInterval(f,15000);return()=>clearInterval(i)},[])
  useEffect(()=>{if(!queues.length){setStats({});return}
    const f=async()=>{const ids=queues.map(q=>q.id).slice(0,100);if(!ids.length)return;const x=await api(`/queues/batch-stats?ids=${ids.join(',')}`,{},auth.token).catch(()=>[]);setStats(Object.fromEntries(x.map(i=>[i.queue_id,i])))};f();const i=setInterval(f,10000);return()=>clearInterval(i)},[auth.token,queues])

  // Live updates: the event stream pushes project snapshots every 2s. We use
  // it as a change trigger (refresh job list / stats immediately when any
  // count moves) instead of blind fast polling.
  const bumpRef = useRef(null)
  useEffect(()=>{
    if(!project) return
    const es = new EventSource(`/api/v1/events/stream?project_id=${project}&access_token=${encodeURIComponent(auth.token)}`)
    es.onopen = () => setLive(true)
    es.onerror = () => setLive(false)
    let last = ''
    es.onmessage = (ev) => {
      try{
        const snap = JSON.parse(ev.data)
        const sig = JSON.stringify(snap.counts || {})
        setLive(true)
        if(snap.recent) setRecent(snap.recent)
        if(sig !== last){
          last = sig
          clearTimeout(bumpRef.current)
          bumpRef.current = setTimeout(() => window.dispatchEvent(new Event('jobflow:bump')), 400)
        }
      }catch{/* ignore malformed frames */}
    }
    return () => { es.close(); setLive(false) }
  },[project, auth.token])

  const q = queues.find(x=>x.id===queue)
  return <div className="app">
    <header>
      <div className="brand"><b>↯</b> Jobflow <span>/ Scheduler</span></div>
      <div className={'system '+(healthy?'':'offline')}>
        <i/>{healthy?'System operational':'API unavailable'}{live&&q&&<span className="live-dot" title="Streaming live events"/>}
        <button className="avatar" aria-label="Sign out" title="Sign out" onClick={auth.logout}>{(auth.user?.display_name||auth.user?.email||'U')[0].toUpperCase()}</button>
      </div>
    </header>
    {note&&<div className={'notice '+(note.e?'error':'success')} role={note.e?'alert':'status'}>{note.t}<button aria-label="Dismiss notification" onClick={()=>setNote(null)}>×</button></div>}
    <div className="layout">
      <aside>
        <div className="side-title">Workspace <button aria-label="Add organization" onClick={()=>setModal('organization')}>+</button></div>
        <label>Organization</label>
        <select aria-label="Organization" value={org} onChange={e=>setOrg(e.target.value)}>
          <option value="">{orgs.length?'Choose organization':'No organizations yet'}</option>{orgs.map(x=><option key={x.id} value={x.id}>{x.name}</option>)}
        </select>
        <div className="side-title">Project <button aria-label="Add project" disabled={!org} onClick={()=>setModal('project')}>+</button></div>
        <select aria-label="Project" disabled={!org} value={project} onChange={e=>setProject(e.target.value)}>
          <option value="">{projects.length?'Choose project':'No projects yet'}</option>{projects.map(x=><option key={x.id} value={x.id}>{x.name}</option>)}
        </select>
        <div className="side-title">Queues <button aria-label="Add queue" disabled={!project} onClick={()=>setModal('queue')}>+</button></div>
        <div className="queue-list">{queues.map(x=>
          <button key={x.id} className={'queue '+(x.id===queue?'selected':'')} onClick={()=>setQueue(x.id)}>
            <strong>{x.name}</strong>
            <span><em className={x.is_paused?'paused':''}>{x.is_paused?'Paused':'Active'}</em><i className={'q-dot '+((stats[x.id]?.failed||0)+(stats[x.id]?.dlq||0)>0?'warn':'ok')} title={((stats[x.id]?.failed||0)+(stats[x.id]?.dlq||0))>0?'Needs attention':'Healthy'}/>{(stats[x.id]?.queued||0)+(stats[x.id]?.running||0)} pending</span>
          </button>)}
          {!queues.length&&<div className="empty-side"><b>Start a workspace</b><p>Create an organization, project and queue to begin.</p><button onClick={()=>setModal(project?'queue':org?'project':'organization')}>Create now →</button></div>}
        </div>
        <footer><i/>{workers.filter(x=>x.status==='ONLINE').length} active workers</footer>
      </aside>
      <main>{q?<QueueView q={q} stats={stats[queue]||{}} auth={auth} refresh={refresh} note={setNote} recent={recent}/>:<Welcome orgs={orgs} projects={projects} open={setModal}/>}</main>
    </div>
    {modal&&<EntityForm type={modal} auth={auth} org={org} project={project} close={()=>setModal(null)} done={async l=>{setModal(null);await refresh();setNote({t:l+' created successfully.'})}}/>}
  </div>
}

function QueueView({q,stats,auth,refresh,note,recent}){
  const[jobs,setJobs]=useState([]),[filter,setFilter]=useState(''),[search,setSearch]=useState(''),[job,setJob]=useState(null)
  const[logs,setLogs]=useState([]),[create,setCreate]=useState(false),[loading,setLoading]=useState(true)
  const[updated,setUpdated]=useState(null),[page,setPage]=useState(1),[total,setTotal]=useState(0),[totalPages,setTotalPages]=useState(1)
  const[tab,setTab]=useState('jobs')
  const[tp,setTp]=useState(null)
  const jobSeq=useRef(0), tpSeq=useRef(0)

  useEffect(()=>{setPage(1)},[filter,q.id])
  // Sequence guard: stale responses from a previous queue/page are discarded
  // instead of overwriting fresh data.
  const load=useCallback(async()=>{
    const my=++jobSeq.current
    setLoading(true)
    const p=new URLSearchParams({queue_id:q.id,page_size:String(PAGE_SIZE),page:String(page)})
    if(filter)p.set('status',filter)
    const x=await api(`/jobs?${p}`,{},auth.token).catch(e=>{if(my===jobSeq.current)note({e:1,t:e.message});return null})
    if(my!==jobSeq.current)return
    setJobs(x?.data||[]);setTotal(x?.total||0);setTotalPages(x?.total_pages||1);setUpdated(new Date());setLoading(false)
  },[auth.token,filter,page,q.id,note])
  useEffect(()=>{load();const i=setInterval(load,10000);return()=>clearInterval(i)},[load])

  const loadThroughput=useCallback(async()=>{
    const my=++tpSeq.current
    try{
      const x=await api(`/queues/${q.id}/throughput?minutes=30`,{},auth.token)
      if(my===tpSeq.current)setTp(x)
    }catch{/* chart simply stays on last good data */}
  },[auth.token,q.id])
  useEffect(()=>{loadThroughput();const i=setInterval(loadThroughput,10000);return()=>clearInterval(i)},[loadThroughput])
  // Event-stream change trigger: refresh immediately between intervals.
  useEffect(()=>{
    const h=()=>{load();loadThroughput()}
    window.addEventListener('jobflow:bump',h)
    return()=>window.removeEventListener('jobflow:bump',h)
  },[load,loadThroughput])

  const inspect=async x=>{setJob(x);setTab('jobs');setLogs(await api(`/jobs/${x.id}/logs`,{},auth.token).catch(()=>[]))}
  const retry=async x=>{try{await api(`/jobs/${x.id}/retry`,{method:'POST'},auth.token);note({t:'Job queued for another attempt.'});load()}catch(e){note({e:1,t:e.message})}}
  const toggle=async()=>{try{await api(`/queues/${q.id}`,{method:'PATCH',body:JSON.stringify({is_paused:!q.is_paused})},auth.token);await refresh();note({t:!q.is_paused?'Queue resumed.':'Queue paused. New jobs will wait safely.'})}catch(e){note({e:1,t:e.message})}}

  return <div className="content">
    <div className="page-head">
      <div>
        <div className="crumb">Projects / Queues</div>
        <h1>{q.name} <em className={q.is_paused?'paused':''}>{q.is_paused?'Paused':'Accepting work'}</em></h1>
        <p>Priority {q.default_priority} · Maximum concurrency {q.max_concurrency}</p>
      </div>
      <div>
        <button onClick={load}>Refresh</button>
        <button onClick={toggle}>{q.is_paused?'Resume queue':'Pause queue'}</button>
        <button className="primary" onClick={()=>setCreate(true)}>Create job</button>
      </div>
    </div>
    <div className="metrics">
      <Metric x="Queued" y={stats.queued||0} c="blue"/>
      <Metric x="Running" y={stats.running||0} d={`${stats.running||0} of ${q.max_concurrency} capacity`} c="purple"/>
      <Metric x="Completed" y={stats.completed||0} c="green"/>
      <Metric x="Needs attention" y={(stats.failed||0)+(stats.dlq||0)} d={`${stats.dlq||0} in DLQ`} c="red"/>
    </div>
    <LifecycleStrip s={stats} onStageClick={(st)=>{setTab('jobs');setSearch('');setFilter(st||'')}}/>
    <div className="metrics">
      <Metric x="Success rate · 24h" y={Number.isFinite(+tp?.success_rate_24h) ? `${tp.success_rate_24h}%` : '—'} c="green"/>
      <Metric x="Avg duration · 24h" y={Number.isFinite(+tp?.avg_duration_ms_24h) ? `${Math.round(tp.avg_duration_ms_24h)} ms` : '—'} c="blue"/>
      <Metric x="Scheduled" y={stats.scheduled||0} d="waiting for their moment" c="purple"/>
      <Metric x="In retry wait" y={stats.retry_wait||0} d="backing off before next attempt" c="purple"/>
    </div>
    <div className="charts-row">
      <ThroughputChart buckets={tp?.buckets||[]}/>
      <StatusDonut
        onSegmentClick={(k)=>{setTab('jobs');setSearch('');setFilter(k)}}
        counts={{
          RUNNING:stats.running,QUEUED:stats.queued,COMPLETED:stats.completed,
          RETRY_WAIT:stats.retry_wait,FAILED:stats.failed,SCHEDULED:stats.scheduled,CLAIMED:stats.claimed,
        }}
        total={(stats.queued||0)+(stats.running||0)+(stats.completed||0)+(stats.failed||0)+(stats.retry_wait||0)+(stats.scheduled||0)+(stats.claimed||0)}
      />
      <div style={{display:'flex',flexDirection:'column',gap:'14px'}}>
        <QueueCharts stats={stats} maxConcurrency={q.max_concurrency}/>
      </div>
    </div>
    <nav className="tabs" role="tablist" aria-label="Queue views">
      <button role="tab" aria-selected={tab==='jobs'} className={tab==='jobs'?'on':''} onClick={()=>setTab('jobs')}>Jobs</button>
      <button role="tab" aria-selected={tab==='dlq'} className={tab==='dlq'?'on':''} onClick={()=>setTab('dlq')}>Dead letters{stats.dlq?` (${stats.dlq})`:''}</button>
    </nav>
    <div className="grid">
      {tab==='jobs'?<>
        <JobsPanel jobs={jobs} loading={loading} total={total} totalPages={totalPages} page={page} setPage={setPage} pageSize={PAGE_SIZE}
                   onInspect={inspect} onRetry={retry} search={search} setSearch={setSearch} filter={filter} setFilter={setFilter} updated={updated}/>
        <JobDetails job={job} logs={logs} retry={()=>retry(job)}/>
        <ActivityFeed events={recent.filter(e=>e.queue_id===q.id)}/>
      </>:<>
        <DlqPanel q={q} auth={auth} note={note} onChanged={load}/>
        <WorkersPanel workers={[]} />
      </>}
    </div>
    {create&&<JobForm q={q} auth={auth} close={()=>setCreate(false)} done={()=>{setCreate(false);load();refresh();note({t:'Job accepted and added to the queue.'})}}/>}
  </div>
}

function Welcome({orgs,projects,open}){
  const i=!orgs.length?0:!projects.length?1:2,L=['Create an organization','Add a project','Configure a queue'],T=['organization','project','queue']
  return <div className="welcome"><div><small>Distributed job operations</small><h1>Run asynchronous work with confidence.</h1><p>Set up your first queue to submit jobs, monitor workers, inspect attempts, and recover from failures in one place.</p></div>
  <section className="setup"><div className="setup-head"><div><small>Getting started</small><h2>Set up your workspace</h2></div><span>Step {i+1} of 3</span></div>
  {L.map((x,n)=><div className={'step '+(n<i?'done':n===i?'current':'')} key={x}><b>{n<i?'✓':n+1}</b><div><strong>{x}</strong><p>{['Create a secure workspace for your team.','Group related queues and operational views.','Set concurrency, priority and retry behavior.'][n]}</p></div>{n===i&&<button className="primary" onClick={()=>open(T[n])}>Continue</button>}</div>)}</section></div>
}
