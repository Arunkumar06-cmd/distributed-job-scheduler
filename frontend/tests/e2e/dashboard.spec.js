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
