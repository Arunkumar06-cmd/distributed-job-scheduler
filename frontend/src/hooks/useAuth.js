import { useState } from 'react'

export function useAuth() {
  const [t, setT] = useState(() => localStorage.getItem('token') || '')
  const [u, setU] = useState(() => { try { return JSON.parse(localStorage.getItem('user') || 'null') } catch { return null } })
  return {
    token: t,
    user: u,
    signed: !!t,
    login: (access, refresh, user) => {
      setT(access); setU(user)
      localStorage.setItem('token', access)
      if (refresh) localStorage.setItem('refresh', refresh)
      localStorage.setItem('user', JSON.stringify(user))
    },
    logout: () => { setT(''); setU(null); ['token','refresh','user'].forEach(k => localStorage.removeItem(k)) },
  }
}
