import React, { useState } from 'react'
import { api } from '../lib/api'

export function Auth({ auth }) {
  const [mode, setMode] = useState('register'), [email, setEmail] = useState(''), [password, setPassword] = useState(''), [name, setName] = useState(''), [err, setErr] = useState(''), [busy, setBusy] = useState(false)
  const submit = async e => {
    e.preventDefault(); setBusy(true); setErr('')
    try {
      const x = await api(mode === 'login' ? '/auth/login' : '/auth/register', { method:'POST', body: JSON.stringify(mode === 'login' ? { email, password } : { email, password, display_name: name }) })
      auth.login(x.access_token || x.token, x.refresh_token, x.user)
    } catch (e) { setErr(e.message) } finally { setBusy(false) }
  }
  return <main className="auth">
    <section>
      <div className="brand"><b>↯</b> Jobflow <span>/ Scheduler</span></div>
      <div><small>Reliable background execution</small><h1>Operate every job with a clear view.</h1><p>Queue work, follow attempts, and recover from failures without guessing what happened.</p></div>
      <ul><li>✓ Atomic job claims and lease fencing</li><li>✓ Retries, schedules, and dead-letter recovery</li><li>✓ Project-scoped operational visibility</li></ul>
    </section>
    <div className="auth-card">
      <nav><button className={mode==='register'?'on':''} onClick={()=>setMode('register')}>Create account</button><button className={mode==='login'?'on':''} onClick={()=>setMode('login')}>Sign in</button></nav>
      <h2>{mode==='register'?'Get started':'Welcome back'}</h2>
      <p>{mode==='register'?'Create an account to set up your first workspace.':'Sign in to continue to your workspace.'}</p>
      <form onSubmit={submit}>
        {mode==='register' && <label>Display name<input required value={name} onChange={e=>setName(e.target.value)} placeholder="Alex Morgan"/></label>}
        <label>Email<input type="email" required value={email} onChange={e=>setEmail(e.target.value)} placeholder="you@company.com"/></label>
        <label>Password<input type="password" minLength="8" required value={password} onChange={e=>setPassword(e.target.value)} placeholder="At least 8 characters"/></label>
        {err && <p className="error">{err}</p>}
        <button className="primary submit" disabled={busy}>{busy?'Please wait…':mode==='register'?'Create account':'Sign in'}</button>
      </form>
    </div>
  </main>
}
