// @vitest-environment jsdom
import React from 'react'
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import ErrorBoundary from './ErrorBoundary'

function Boom(){ throw new Error('kaboom') }

describe('ErrorBoundary', () => {
  it('renders fallback instead of crashing the app', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
    render(<ErrorBoundary><Boom/></ErrorBoundary>)
    // The fallback notice must exist; React splits the message across nodes.
    const notices = document.querySelectorAll('.notice.error')
    expect(notices.length).toBe(1)
    expect(notices[0].textContent).toContain('kaboom')
    spy.mockRestore()
  })
})
