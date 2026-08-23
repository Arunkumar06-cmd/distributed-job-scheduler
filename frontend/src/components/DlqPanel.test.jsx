// @vitest-environment jsdom
import React from 'react'
import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { DlqPanel } from './DlqPanel'

vi.mock('../lib/api', () => ({
  api: vi.fn(async () => ({
    data: [{
      id: 'dlq-1', job_id: 'job-1', queue_id: 'q1', org_id: 'o1',
      reason: 'max_attempts_exceeded', attempt: 3,
      payload: {}, final_error: 'boom', moved_at: '2026-01-01T00:00:00Z',
      replayed_to_job_id: null,
    }],
    page: 1, total: 1, total_pages: 1,
  })),
}))

describe('DlqPanel', () => {
  it('renders reason pills without ReferenceError (regression: cap import)', async () => {
    const auth = { token: 't' }
    render(<DlqPanel q={{ id: 'q1' }} auth={auth} note={() => {}} />)
    expect(await screen.findByText('Max Attempts Exceeded')).toBeTruthy()
  })
})
