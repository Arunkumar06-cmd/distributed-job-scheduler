export const cap = (s) =>
  String(s || 'unknown').replaceAll('_',' ').toLowerCase().replace(/\b\w/g, x => x.toUpperCase())
export const short = (x) => (x ? x.slice(0, 8) + '…' : '—')
export const dt = (x) => (x
  ? new Intl.DateTimeFormat(undefined, { month:'short', day:'numeric', hour:'numeric', minute:'2-digit' }).format(new Date(x))
  : '—')
export const slug = (x) => x.toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'')
