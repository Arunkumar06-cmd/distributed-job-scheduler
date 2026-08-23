const API_BASE = '/api/v1'

export async function api(path, o = {}, token = '', allowRefresh = true) {
  const h = { 'Content-Type': 'application/json', ...(o.headers || {}) }
  if (token) h.Authorization = `Bearer ${token}`
  const url = path.startsWith('/api/') ? path : API_BASE + path
  const r = await fetch(url, { ...o, headers: h })
  if (r.status === 401 && token && allowRefresh) {
    const rt = localStorage.getItem('refresh')
    if (rt) {
      const rr = await fetch(API_BASE + '/auth/refresh', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ refresh_token: rt }) })
      if (rr.ok) {
        const j = await rr.json()
        localStorage.setItem('token', j.access_token)
        localStorage.setItem('refresh', j.refresh_token)
        return api(path, { ...o, headers:{ ...o.headers, Authorization:`Bearer ${j.access_token}` } }, j.access_token, false)
      }
    }
    window.dispatchEvent(new Event('jobflow:session-expired'))
  }
  if (!r.ok) {
    const j = await r.json().catch(() => ({}))
    throw Error(j?.error?.message || j?.message || r.statusText)
  }
  return r.status === 204 ? null : r.json()
}
