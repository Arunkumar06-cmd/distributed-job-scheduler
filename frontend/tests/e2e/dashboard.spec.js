import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

const uniq = () => `e2e-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
let email

test.beforeEach(async () => {
  email = `${uniq()}@t.io`
})

async function register(page) {
  await page.goto('/')
  await page.getByRole('button', { name: 'Create account' }).first().click()
  // register is default tab
  await page.getByLabel('Display name').fill('E2E Bot')
  await page.getByLabel('Email').fill(email)
  await page.getByLabel('Password').fill('password123')
  await page.getByRole('button', { name: 'Create account' }).last().click()
  await expect(page.getByText('Set up your workspace')).toBeVisible({ timeout: 10_000 })
}

// Org/project/queue slugs are globally unique server-side; every run needs
// fresh names or repeat runs collide with 409s.
let ORG_NAME, QUEUE_NAME

async function createWorkspace(page) {
  ORG_NAME = `E2E Org ${uniq()}`
  QUEUE_NAME = `e2e-q-${Date.now()}`

  await page.getByRole('button', { name: 'Continue' }).click()
  const orgDialog = page.getByRole('dialog')
  await orgDialog.getByLabel('Name', { exact: true }).fill(ORG_NAME)
  await orgDialog.getByRole('button', { name: 'Create organization' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()

  await page.getByRole('button', { name: 'Add project' }).click()
  const projDialog = page.getByRole('dialog').last()
  await projDialog.getByLabel('Name', { exact: true }).fill('E2E Project')
  await projDialog.getByRole('button', { name: 'Create project' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()

  await page.getByRole('button', { name: 'Add queue' }).click()
  const qDialog = page.getByRole('dialog').last()
  await qDialog.getByLabel('Name', { exact: true }).fill(QUEUE_NAME)
  await qDialog.getByRole('button', { name: 'Create queue' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()
  page.on('console', c => { const t=c.text(); if(t.includes('401')||t.includes('session')) console.log('PAGE-NET:', t.slice(0,160)) })
  await page.waitForTimeout(1500)
  console.log('TOKEN-LEN:', ((await page.evaluate(() => localStorage.getItem('token'))) || '').length)
  const queuesState = await page.evaluate(async () => {
    const tok = localStorage.getItem('token')
    const r = await fetch('/api/v1/queues?project_id=' + document.querySelectorAll('select')[1]?.value, { headers: { authorization: 'Bearer ' + tok } })
    return { status: r.status, body: (await r.text()).slice(0, 150) }
  })
  console.log('DIRECT-FETCH:', JSON.stringify(queuesState))
  const cnt = await page.locator('.queue-list .queue').count()
  console.log('DBG-COUNT:', cnt)
  if (!cnt) {
    console.log('DBG-SIDEBAR:', (await page.locator('.queue-list').innerHTML()).slice(0,300))
    console.log('DBG-SELECTS:', await page.locator('select').count())
    const projNow = await page.evaluate(() => fetch('/api/v1/projects?org_id=' + document.querySelector('select')?.value, { headers: { authorization: 'Bearer ' + localStorage.getItem('token') } }).then(r => r.status + ':' + r.text().then(t => t.slice(0, 120)))).catch(e => String(e))
    console.log('DBG-PROJFETCH:', JSON.stringify(projNow))
  }
}

test.describe('dashboard e2e', () => {
  test('register → onboarding wizard renders', async ({ page }) => {
    await register(page)
    const steps = page.locator('.step')
    await expect(steps).toHaveCount(3)
    // Accessibility scan of the welcome surface.
    const results = await new AxeBuilder({ page }).analyze()
    const critical = results.violations.filter(v => v.impact === 'critical' || v.impact === 'serious')
    expect(critical, JSON.stringify(critical.map(v => ({ id: v.id, nodes: v.nodes.length })))).toEqual([])
  })

  test('create org → project → queue via modals; queue appears in sidebar', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    await expect(page.locator('.queue-list').getByText(QUEUE_NAME)).toBeVisible()
    await expect(page.locator('.queue.selected')).toBeVisible()
  })

  test('submit job from UI and see it listed with pagination footer', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    await page.getByRole('button', { name: 'Create job' }).click()
    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()
    // Payload must route to a registered handler.
    const payload = dialog.locator('textarea')
    await payload.fill('{ "type": "echo", "msg": "hi" }')
    await dialog.getByRole('button', { name: 'Submit job' }).click()
    await expect(dialog).toBeHidden()
    await expect
      .poll(async () => page.locator('.panel.jobs tbody tr').first().textContent(), { timeout: 10_000 })
      .toMatch(/queued|claimed|running|completed/i)
    await expect(page.getByText(/Page \d+ of \d+ ·/)).toBeVisible()
  })

  test('rejects non-object payloads with a validation message', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    await page.getByRole('button', { name: 'Create job' }).click()
    const dialog = page.getByRole('dialog')
    await dialog.locator('textarea').fill('[1,2,3]')
    await dialog.getByRole('button', { name: 'Submit job' }).click()
    await expect(page.getByText('Payload must be a JSON object.')).toBeVisible()
  })

  test('pause/resume queue updates its badge', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    await page.getByRole('button', { name: /Pause queue/ }).click()
    await expect(page.locator('.queue.selected')).toContainText('Paused')
    await page.getByRole('button', { name: /Resume queue/ }).click()
    await expect(page.locator('.queue.selected')).toContainText('Active')
  })

  test('DLQ tab shows empty state for a healthy queue', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    await page.getByRole('tab', { name: /Dead letters/ }).click()
    await expect(page.getByText('Nothing dead here.')).toBeVisible()
  })

  test('full a11y scan of the queue workspace', async ({ page }) => {
    await register(page)
    await createWorkspace(page)
    const results = await new AxeBuilder({ page }).analyze()
    const serious = results.violations.filter(v => v.impact === 'critical' || v.impact === 'serious')
    expect(serious, JSON.stringify(serious.map(v => ({ id: v.id, impact: v.impact, nodes: v.nodes.slice(0, 3).map(n => n.target) })))).toEqual([])
  })

  test('signing out returns to the auth screen', async ({ page }) => {
    await register(page)
    await page.getByRole('button', { name: 'Sign out' }).click()
    await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible()
  })
})

test('websocket stream delivers authenticated project snapshots', async ({ page }) => {
  await register(page)
  // Grab the access token the app stored, then open a raw WS like an external client would.
  const token = await page.evaluate(() => localStorage.getItem('token'))
  const orgResp = await page.request.post('/api/v1/organizations', {
    data: { name: `WS Org ${Date.now()}`, slug: `ws-${Date.now()}` },
    headers: { authorization: `Bearer ${token}` },
  })
  expect(orgResp.status()).toBe(201)
  const org = await orgResp.json()
  await page.request.post('/api/v1/projects', {
    data: { org_id: org.id, name: 'WSP', slug: `ws-p-${Date.now()}` },
    headers: { authorization: `Bearer ${token}` },
  })
  const projs = await (await page.request.get(`/api/v1/projects?org_id=${org.id}`, { headers: { authorization: `Bearer ${token}` } })).json()
  const projectId = projs[0].id

  const frame = await page.evaluate(({ pid, tok }) => new Promise((resolve, reject) => {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${proto}://${location.host}/api/v1/events/ws?project_id=${pid}&access_token=${tok}`)
    const timer = setTimeout(() => reject(new Error('no ws frame in 10s')), 10_000)
    ws.onmessage = ev => { clearTimeout(timer); resolve(JSON.parse(ev.data)); ws.close() }
    ws.onerror = () => { clearTimeout(timer); reject(new Error('ws error')) }
  }), { pid: projectId, tok: token })
  expect(frame.type).toBe('project.snapshot')
  expect(frame.counts).toBeTruthy()
})

test('metric cards populate with real values (no NaN, no silent zeros)', async ({ page }) => {
  await register(page)
  await createWorkspace(page)
  await page.getByRole('button', { name: 'Create job' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.locator('textarea').fill('{ "type": "echo" }')
  await dialog.getByRole('button', { name: 'Submit job' }).click()
  // Worker completes it; success-rate must become a number, never NaN.
  await expect(page.locator('.metrics article', { hasText: 'Success rate' }).locator('b')).toHaveText(/\d+%/, { timeout: 20_000 })
  const avg = await page.locator('.metrics article', { hasText: 'Avg duration' }).locator('b').textContent()
  expect(avg).not.toContain('NaN')
  // Lifecycle Done counter must match reality (>=1), proving stats fallback.
  await expect
    .poll(async () => Number(await page.locator('.lc-stage', { hasText: 'Done' }).locator('b').textContent()), { timeout: 20_000 })
    .toBeGreaterThanOrEqual(1)
})
