import React,{useCallback,useEffect,useState}from'react'
const S=['QUEUED','RUNNING','RETRY_WAIT','COMPLETED','FAILED','SCHEDULED','WAITING']
async function api(path,o={},token=''){const h={'Content-Type':'application/json',...(o.headers||{})};if(token)h.Authorization=`Bearer ${token}`;const r=await fetch(path,{...o,headers:h});if(!r.ok){const j=await r.json().catch(()=>({}));throw Error(j?.error?.message||j?.message||r.statusText)}return r.status===204?null:r.json()}
const cap=s=>String(s||'unknown').replaceAll('_',' ').toLowerCase().replace(/\b\w/g,x=>x.toUpperCase()),short=x=>x?x.slice(0,8)+'…':'—',dt=x=>x?new Intl.DateTimeFormat(undefined,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}).format(new Date(x)):'—',slug=x=>x.toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'')
function useAuth(){const[t,setT]=useState(()=>localStorage.getItem('token')||''),[u,setU]=useState(()=>{try{return JSON.parse(localStorage.getItem('user')||'null')}catch{return null}});return{token:t,user:u,signed:!!t,login:(a,b)=>{setT(a);setU(b);localStorage.setItem('token',a);localStorage.setItem('user',JSON.stringify(b))},logout:()=>{setT('');setU(null);localStorage.removeItem('token');localStorage.removeItem('user')}}}
export default function App(){const a=useAuth();return a.signed?<Dashboard auth={a}/>:<Auth auth={a}/>}
function Dashboard({auth}){const[orgs,setOrgs]=useState([]),[projects,setProjects]=useState([]),[queues,setQueues]=useState([]),[stats,setStats]=useState({}),[workers,setWorkers]=useState([]),[healthy,setHealthy]=useState(true),[org,setOrg]=useState(''),[project,setProject]=useState(''),[queue,setQueue]=useState(''),[modal,setModal]=useState(null),[note,setNote]=useState(null)
const loadOrgs=useCallback(async()=>{const x=await api('/organizations',{},auth.token);setOrgs(x);setOrg(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token]);const loadProjects=useCallback(async()=>{if(!org){setProjects([]);setProject('');return}const x=await api(`/projects?org_id=${org}`,{},auth.token);setProjects(x);setProject(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token,org]);const loadQueues=useCallback(async()=>{if(!project){setQueues([]);setQueue('');return}const x=await api(`/queues?project_id=${project}`,{},auth.token);setQueues(x);setQueue(v=>x.some(i=>i.id===v)?v:x[0]?.id||'')},[auth.token,project]);const refresh=async()=>{await loadOrgs();await loadProjects();await loadQueues()};useEffect(()=>{loadOrgs().catch(e=>setNote({e:1,t:e.message}))},[loadOrgs]);useEffect(()=>{loadProjects().catch(e=>setNote({e:1,t:e.message}))},[loadProjects]);useEffect(()=>{loadQueues().catch(e=>setNote({e:1,t:e.message}))},[loadQueues]);useEffect(()=>{const f=async()=>{setWorkers(await api('/workers',{},auth.token).catch(()=>[]))};f();const i=setInterval(f,8000);return()=>clearInterval(i)},[auth.token]);useEffect(()=>{const f=async()=>{try{await api('/health');setHealthy(true)}catch{setHealthy(false)}};f();const i=setInterval(f,5000);return()=>clearInterval(i)},[]);useEffect(()=>{if(!queues.length){setStats({});return}const f=async()=>{const x=await api(`/queues/batch-stats?ids=${queues.map(q=>q.id).join(',')}`,{},auth.token).catch(()=>[]);setStats(Object.fromEntries(x.map(i=>[i.queue_id,i])))};f();const i=setInterval(f,5000);return()=>clearInterval(i)},[auth.token,queues]);const q=queues.find(x=>x.id===queue);return <div className="app">
<header>
<div className="brand">
<b>↯</b> Jobflow <span>/ Scheduler</span>
</div>
<div className={'system '+(healthy?'':'offline')}>
<i/>{healthy?'System operational':'API unavailable'}<button className="avatar" onClick={auth.logout}>{(auth.user?.display_name||auth.user?.email||'U')[0].toUpperCase()}</button>
</div>
</header>{note&&<div className={'notice '+(note.e?'error':'success')}>{note.t}<button onClick={()=>setNote(null)}>×</button>
</div>}<div className="layout">
<aside>
<div className="side-title">Workspace <button onClick={()=>setModal('organization')}>+</button>
</div>
<label>Organization</label>
<select value={org} onChange={e=>setOrg(e.target.value)}>
<option value="">{orgs.length?'Choose organization':'No organizations yet'}</option>{orgs.map(x=>
<option key={x.id} value={x.id}>{x.name}</option>)}</select>
<div className="side-title">Project <button disabled={!org} onClick={()=>setModal('project')}>+</button>
</div>
<select disabled={!org} value={project} onChange={e=>setProject(e.target.value)}>
<option value="">{projects.length?'Choose project':'No projects yet'}</option>{projects.map(x=>
<option key={x.id} value={x.id}>{x.name}</option>)}</select>
<div className="side-title">Queues <button disabled={!project} onClick={()=>setModal('queue')}>+</button>
</div>
<div className="queue-list">{queues.map(x=>
<button key={x.id} className={'queue '+(x.id===queue?'selected':'')} onClick={()=>setQueue(x.id)}>
<strong>{x.name}</strong>
<span>
<em className={x.is_paused?'paused':''}>{x.is_paused?'Paused':'Active'}</em>{(stats[x.id]?.queued||0)+(stats[x.id]?.running||0)} pending</span>
</button>)}{!queues.length&&<div className="empty-side">
<b>Start a workspace</b>
<p>Create an organization, project and queue to begin.</p>
<button onClick={()=>setModal(project?'queue':org?'project':'organization')}>Create now →</button>
</div>}</div>
<footer>
<i/>{workers.filter(x=>x.status==='ONLINE').length} active workers</footer>
</aside>
<main>{q?<Queue q={q} stats={stats[queue]||{}} auth={auth} refresh={refresh} note={setNote}/>:<Welcome orgs={orgs} projects={projects} open={setModal}/>}</main>
</div>{modal&&<Entity type={modal} auth={auth} org={org} project={project} close={()=>setModal(null)} done={async x=>{setModal(null);await refresh();setNote({t:x+' created successfully.'})}}/>}</div>}
function Welcome({orgs,projects,open}){const i=!orgs.length?0:!projects.length?1:2,L=['Create an organization','Add a project','Configure a queue'],T=['organization','project','queue'];return <div className="welcome">
<div>
<small>Distributed job operations</small>
<h1>Run asynchronous work with confidence.</h1>
<p>Set up your first queue to submit jobs, monitor workers, inspect attempts, and recover from failures in one place.</p>
</div>
<section className="setup">
<div className="setup-head">
<div>
<small>Getting started</small>
<h2>Set up your workspace</h2>
</div>
<span>Step {i+1} of 3</span>
</div>{L.map((x,n)=>
<div className={'step '+(n<i?'done':n===i?'current':'')} key={x}>
<b>{n<i?'✓':n+1}</b>
<div>
<strong>{x}</strong>
<p>{['Create a secure workspace for your team.','Group related queues and operational views.','Set concurrency, priority and retry behavior.'][n]}</p>
</div>{n===i&&<button className="primary" onClick={()=>open(T[n])}>Continue</button>}</div>)}</section>
</div>}
function Queue({q,stats,auth,refresh,note}){const[jobs,setJobs]=useState([]),[filter,setFilter]=useState(''),[search,setSearch]=useState(''),[job,setJob]=useState(null),[logs,setLogs]=useState([]),[create,setCreate]=useState(false),[loading,setLoading]=useState(true),[updated,setUpdated]=useState(null);const load=useCallback(async()=>{setLoading(true);const p=new URLSearchParams({queue_id:q.id,page_size:'100'});if(filter)p.set('status',filter);const x=await api(`/jobs?${p}`,{},auth.token).catch(e=>{note({e:1,t:e.message});return{data:[]}});setJobs(x.data||[]);setUpdated(new Date());setLoading(false)},[auth.token,filter,note,q.id]);useEffect(()=>{load();const i=setInterval(load,5000);return()=>clearInterval(i)},[load]);const inspect=async x=>{setJob(x);setLogs(await api(`/jobs/${x.id}/logs`,{},auth.token).catch(()=>[]))},retry=async x=>{try{await api(`/jobs/${x.id}/retry`,{method:'POST'},auth.token);note({t:'Job queued for another attempt.'});load();refresh()}catch(e){note({e:1,t:e.message})}},toggle=async()=>{try{await api(`/queues/${q.id}/${q.is_paused?'resume':'pause'}`,{method:'POST'},auth.token);await refresh();note({t:q.is_paused?'Queue resumed.':'Queue paused. New jobs will wait safely.'})}catch(e){note({e:1,t:e.message})}},visible=jobs.filter(x=>`${x.id} ${x.idempotency_key||''}`.toLowerCase().includes(search.toLowerCase()));return <div className="content">
<div className="page-head">
<div>
<div className="crumb">Projects / Queues</div>
<h1>{q.name} <em className={q.is_paused?'paused':''}>{q.is_paused?'Paused':'Accepting work'}</em>
</h1>
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
<div className="grid">
<section className="panel jobs">
<div className="panel-head">
<div>
<h2>Jobs</h2>
<p>{updated?`Updated ${updated.toLocaleTimeString([], {hour:'numeric',minute:'2-digit',second:'2-digit'})}`:'Live list updates every five seconds.'}</p>
</div>
<input className="job-search" value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search job ID or key" aria-label="Search jobs"/>
<select value={filter} onChange={e=>setFilter(e.target.value)}>
<option value="">All states</option>{S.map(x=>
<option key={x} value={x}>{cap(x)}</option>)}</select>
</div>
<div className="table-wrap">
<table>
<thead>
<tr>
<th>Job</th>
<th>Status</th>
<th>Attempts</th>
<th>Created</th>
<th/>
</tr>
</thead>
<tbody>{loading&&!jobs.length?<tr>
<td colSpan="5" className="empty-cell">Loading jobs…</td>
</tr>:visible.map(x=>
<tr key={x.id}>
<td>
<button className="job" onClick={()=>inspect(x)}>
<b>{short(x.id)}</b>
<span>{x.idempotency_key||'No idempotency key'}</span>
</button>
</td>
<td>
<em className={String(x.status).toLowerCase()}>{cap(x.status)}</em>
</td>
<td>{x.attempt||0} <span className="muted">/ {x.max_attempts||1}</span>
</td>
<td className="muted">{dt(x.created_at)}</td>
<td>
<button className="text" onClick={()=>inspect(x)}>Inspect</button>{['FAILED','RETRY_WAIT','UNKNOWN_EXTERNAL_RESULT'].includes(String(x.status).toUpperCase())&&<button className="text" onClick={()=>retry(x)}>Retry</button>}</td>
</tr>)}{!loading&&!visible.length&&<tr>
<td colSpan="5" className="empty-cell">
<b>{jobs.length?'No matching jobs':'No jobs yet'}</b>
<span>{jobs.length?'Try clearing your search or status filter.':'Create a job to see queue activity here.'}</span>
<button onClick={()=>setCreate(true)}>Create job</button>
</td>
</tr>}</tbody>
</table>
</div>
</section>
<Details job={job} logs={logs} retry={()=>retry(job)}/>
</div>{create&&<Job q={q} auth={auth} close={()=>setCreate(false)} done={()=>{setCreate(false);load();refresh();note({t:'Job accepted and added to the queue.'})}}/>}</div>}
function Metric({x,y,d,c}){return <article className={c}>
<span>{x}</span>
<b>{Number(y).toLocaleString()}</b>{d&&<small>{d}</small>}</article>}
function Details({job,logs,retry}){if(!job)return <aside className="panel details blank">
<div>◎</div>
<h2>Job details</h2>
<p>Select a job to view attempts, timestamps, logs and retry history.</p>
</aside>;return <aside className="panel details">
<div className="panel-head">
<div>
<small>Job details</small>
<h2>{short(job.id)}</h2>
</div>
<em className={String(job.status).toLowerCase()}>{cap(job.status)}</em>
</div>
<section>
<h3>Execution</h3>
<p>
<span>Attempts</span>
<b>{job.attempt||0} of {job.max_attempts||1}</b>
</p>
<p>
<span>Worker</span>
<b>{short(job.lease_owner)}</b>
</p>
<p>
<span>Created</span>
<b>{dt(job.created_at)}</b>
</p>
</section>
<section>
<h3>Recent logs</h3>
<div className="logs">{logs.slice(-4).map(x=>
<p key={x.id}>
<time>{new Date(x.created_at).toLocaleTimeString()}</time> <b>{x.level}</b> {x.message}</p>)}{!logs.length&&<p className="muted">No logs recorded for this job.</p>}</div>
</section>{['FAILED','RETRY_WAIT','UNKNOWN_EXTERNAL_RESULT'].includes(String(job.status).toUpperCase())&&<button onClick={retry}>Retry job</button>}</aside>}
function Entity({type,auth,org,project,close,done}){const[name,setName]=useState(''),[s,setS]=useState(''),[n,setN]=useState(5),[err,setErr]=useState(''),[busy,setBusy]=useState(false),label=type==='organization'?'Create organization':type==='project'?'Create project':'Create queue';const submit=async e=>{e.preventDefault();setBusy(true);try{if(type==='organization')await api('/organizations',{method:'POST',body:JSON.stringify({name,slug:s||slug(name)})},auth.token);if(type==='project')await api('/projects',{method:'POST',body:JSON.stringify({org_id:org,name,slug:s||slug(name)})},auth.token);if(type==='queue')await api('/queues',{method:'POST',body:JSON.stringify({project_id:project,name,max_concurrency:Number(n)})},auth.token);done(type[0].toUpperCase()+type.slice(1))}catch(e){setErr(e.message)}finally{setBusy(false)}};return <Modal title={label} close={close}>
<form onSubmit={submit}>
<label>Name<input autoFocus required value={name} onChange={e=>setName(e.target.value)} placeholder={type==='queue'?'email-delivery':'Acme Inc.'}/>
</label>{type!=='queue'&&<label>URL slug<input required value={s} onChange={e=>setS(e.target.value)} placeholder="acme-inc"/>
</label>}{type==='queue'&&<label>Maximum concurrency<input type="number" min="1" max="1000" value={n} onChange={e=>setN(e.target.value)}/>
</label>}{err&&<p className="error">{err}</p>}<div className="modal-actions">
<button type="button" onClick={close}>Cancel</button>
<button className="primary" disabled={busy}>{busy?'Creating…':label}</button>
</div>
</form>
</Modal>}
function Job({q,auth,close,done}){const[p,setP]=useState('{\n  "handler": "echo",\n  "message": "Hello from Jobflow"\n}'),[priority,setPriority]=useState(q.default_priority||0),[err,setErr]=useState(''),[busy,setBusy]=useState(false);const submit=async e=>{e.preventDefault();let value;try{value=JSON.parse(p)}catch{setErr('Payload must be valid JSON.');return}setBusy(true);try{await api('/jobs',{method:'POST',body:JSON.stringify({queue_id:q.id,payload:value,priority:Number(priority)})},auth.token);done()}catch(e){setErr(e.message)}finally{setBusy(false)}};return <Modal title="Create job" close={close}>
<p className="description">Submit work to <b>{q.name}</b>. A worker will claim it when capacity is available.</p>
<form onSubmit={submit}>
<label>Priority<input type="number" value={priority} onChange={e=>setPriority(e.target.value)}/>
</label>
<label>Payload<textarea rows="8" value={p} onChange={e=>setP(e.target.value)}/>
</label>{err&&<p className="error">{err}</p>}<div className="modal-actions">
<button type="button" onClick={close}>Cancel</button>
<button className="primary" disabled={busy}>{busy?'Submitting…':'Submit job'}</button>
</div>
</form>
</Modal>}
function Modal({title,close,children}){return <div className="backdrop" onMouseDown={close}>
<div className="modal" role="dialog" onMouseDown={e=>e.stopPropagation()}>
<div className="modal-head">
<h2>{title}</h2>
<button onClick={close}>×</button>
</div>{children}</div>
</div>}
function Auth({auth}){const[mode,setMode]=useState('register'),[email,setEmail]=useState(''),[password,setPassword]=useState(''),[name,setName]=useState(''),[err,setErr]=useState(''),[busy,setBusy]=useState(false);const submit=async e=>{e.preventDefault();setBusy(true);setErr('');try{const x=await api(mode==='login'?'/auth/login':'/auth/register',{method:'POST',body:JSON.stringify(mode==='login'?{email,password}:{email,password,display_name:name})});auth.login(x.token,x.user)}catch(e){setErr(e.message)}finally{setBusy(false)}};return <main className="auth">
<section>
<div className="brand">
<b>↯</b> Jobflow <span>/ Scheduler</span>
</div>
<div>
<small>Reliable background execution</small>
<h1>Operate every job with a clear view.</h1>
<p>Queue work, follow attempts, and recover from failures without guessing what happened.</p>
</div>
<ul>
<li>✓ Atomic job claims and lease fencing</li>
<li>✓ Retries, schedules, and dead-letter recovery</li>
<li>✓ Project-scoped operational visibility</li>
</ul>
</section>
<div className="auth-card">
<nav>
<button className={mode==='register'?'on':''} onClick={()=>setMode('register')}>Create account</button>
<button className={mode==='login'?'on':''} onClick={()=>setMode('login')}>Sign in</button>
</nav>
<h2>{mode==='register'?'Get started':'Welcome back'}</h2>
<p>{mode==='register'?'Create an account to set up your first workspace.':'Sign in to continue to your workspace.'}</p>
<form onSubmit={submit}>{mode==='register'&&<label>Display name<input required value={name} onChange={e=>setName(e.target.value)} placeholder="Alex Morgan"/>
</label>}<label>Email<input type="email" required value={email} onChange={e=>setEmail(e.target.value)} placeholder="you@company.com"/>
</label>
<label>Password<input type="password" minLength="8" required value={password} onChange={e=>setPassword(e.target.value)} placeholder="At least 8 characters"/>
</label>{err&&<p className="error">{err}</p>}<button className="primary submit" disabled={busy}>{busy?'Please wait…':mode==='register'?'Create account':'Sign in'}</button>
</form>
</div>
</main>}
