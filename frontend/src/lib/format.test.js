// @vitest-environment node
import { describe, it, expect } from 'vitest'
import { cap, slug, short, dt } from './format'

describe('format helpers', () => {
  it('capitalizes status labels', () => {
    expect(cap('RETRY_WAIT')).toBe('Retry Wait')
    expect(cap(null)).toBe('Unknown')
  })
  it('slugs names url-safe', () => {
    expect(slug('My Cool Org')).toBe('my-cool-org')
    expect(slug('!!!')).toBe('')
  })
  it('shortens ids', () => {
    expect(short('abcdefgh-xyz')).toMatch(/^abcdefgh…$/)
    expect(short(null)).toBe('—')
  })
})
