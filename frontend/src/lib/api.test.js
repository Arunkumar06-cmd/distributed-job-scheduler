// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest'

const fetchMock = vi.fn()
vi.stubGlobal('fetch', fetchMock)

describe('api() refresh wrapper', () => {
  // Node 22 exposes a native localStorage stub that is inert without
  // --localstorage-file; stub our own so the test is hermetic.
  const storage = (() => {
    const m = new Map()
    return {
      getItem: (k) => (m.has(k) ? m.get(k) : null),
      setItem: (k, v) => m.set(k, String(v)),
      removeItem: (k) => m.delete(k),
      clear: () => m.clear(),
    }
  })()

  beforeEach(() => {
    fetchMock.mockReset()
    vi.stubGlobal('localStorage', storage)
    storage.clear()
  })

  it('rotates on 401 then retries with the new access token', async () => {
    localStorage.setItem('refresh', 'rt-old')
    const { api } = await import('./api')
    fetchMock
      .mockResolvedValueOnce(new Response('{}', { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ access_token:'at-new', refresh_token:'rt-new' }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))

    const out = await api('/organizations', {}, 'at-stale')
    expect(out.ok).toBe(true)
    const [, retryCall] = fetchMock.mock.calls[2]
    expect(retryCall.headers.Authorization).toBe('Bearer at-new')
    expect(localStorage.getItem('refresh')).toBe('rt-new')
  })

  it('dispatches session-expired when refresh fails', async () => {
    localStorage.setItem('refresh', 'rt-dead')
    const { api } = await import('./api')
    fetchMock
      .mockResolvedValueOnce(new Response('{}', { status: 401 }))
      .mockResolvedValueOnce(new Response('{}', { status: 401 }))
    let expired = false
    window.addEventListener('jobflow:session-expired', () => { expired = true }, { once: true })
    await expect(api('/jobs', {}, 'at-stale')).rejects.toThrow()
    expect(expired).toBe(true)
  })
})
