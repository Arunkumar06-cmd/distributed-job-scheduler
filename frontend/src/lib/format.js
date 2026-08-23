export const cap = (s) =>
  String(s || 'unknown').replaceAll('_',' ').toLowerCase().replace(/\b\w/g, x => x.toUpperCase())
export const short = (x) => (x ? x.slice(0, 8) + '…' : '—')
export const dt = (x) => (x
  ? new Intl.DateTimeFormat(undefined, { month:'short', day:'numeric', hour:'numeric', minute:'2-digit' }).format(new Date(x))
  : '—')
export const slug = (x) => x.toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'')

export const relTime = (x) => {
  if (!x) return '—'
  const s = Math.max(0, (Date.now() - new Date(x).getTime()) / 1000)
  if (s < 45) return 'just now'
  if (s < 3600) return `${Math.round(s / 60)}m ago`
  if (s < 86400) return `${Math.round(s / 3600)}h ago`
  return `${Math.round(s / 86400)}d ago`
}

const IST = new Intl.DateTimeFormat('en-IN', {
  timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false,
})
export const fmtTimeIST = (x) => (x ? IST.format(new Date(x)) : '—')
