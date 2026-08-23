const safeDate = (x) => {
  const d = x instanceof Date ? x : new Date(x)
  return Number.isNaN(d.getTime()) ? null : d
}

export const cap = (s) =>
  String(s || 'unknown').replaceAll('_',' ').toLowerCase().replace(/\b\w/g, x => x.toUpperCase())
export const short = (x) => (x ? x.slice(0, 8) + '…' : '—')
const _dt = new Intl.DateTimeFormat('en-IN', { timeZone:'Asia/Kolkata', month:'short', day:'numeric', hour:'numeric', minute:'2-digit' })
export const dt = (x) => { const d = safeDate(x); return d ? _dt.format(d) : '—' }
export const slug = (x) => x.toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'')

export const relTime = (x) => {
  const d = safeDate(x)
  if (!d) return '—'
  const s = Math.max(0, (Date.now() - d.getTime()) / 1000)
  if (s < 45) return 'just now'
  if (s < 3600) return `${Math.round(s / 60)}m ago`
  if (s < 86400) return `${Math.round(s / 3600)}h ago`
  return `${Math.round(s / 86400)}d ago`
}

const IST = new Intl.DateTimeFormat('en-IN', {
  timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false,
})
// All times shown in IST per product requirement.
export const fmtTimeIST = (x) => { const d = safeDate(x); return d ? IST.format(d) : '—' }
