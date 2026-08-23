// Visual-regression baselines. Dynamic regions (refresh timestamp) are masked;
// every surface is captured against a freshly created, empty workspace so
// counters are deterministic across runs.
import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

const uniq = () => Date.now() + '-' + Math.floor(Math.random() * 1e6)
const email = () => `viz-${uniq()}@t.io`

const MASKS = (page) => [
  page.locator('.panel.jobs .panel-head p'), // "Updated <time>" stamp
]

async function register(page, mail) {
  await page.goto('/')
  await page.getByRole('button', { name: 'Create account' }).first().click()
  await page.getByLabel('Display name').fill('Viz Bot')
  await page.getByLabel('Email').fill(mail)
  await page.getByLabel('Password').fill('password123')
  await page.getByRole('button', { name: 'Create account' }).last().click()
  await expect(page.getByText('Set up your workspace')).toBeVisible()
}

async function createWorkspace(page) {
  const orgName = `Viz Org ${uniq()}`
  await page.getByRole('button', { name: 'Continue' }).click()
  const orgDialog = page.getByRole('dialog')
  await orgDialog.getByLabel('Name', { exact: true }).fill(orgName)
  await orgDialog.getByRole('button', { name: 'Create organization' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()

  await page.getByRole('button', { name: 'Add project' }).click()
  const projDialog = page.getByRole('dialog').last()
  await projDialog.getByLabel('Name', { exact: true }).fill('Viz Project')
  await projDialog.getByRole('button', { name: 'Create project' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()

  await page.getByRole('button', { name: 'Add queue' }).click()
  const qDialog = page.getByRole('dialog').last()
  await qDialog.getByLabel('Name', { exact: true }).fill(`viz-q-${uniq()}`)
  await qDialog.getByRole('button', { name: 'Create queue' }).click()
  await expect(page.getByText(/created successfully/)).toBeVisible()

  // Dismiss the toast so it never leaks into a baseline.
  const dismiss = page.getByRole('button', { name: 'Dismiss notification' })
  if (await dismiss.count()) await dismiss.click()
}

async function blur(page) {
  await page.evaluate(() => document.activeElement?.blur())
}

test('auth screen baseline', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByLabel('Email')).toBeVisible()
  await blur(page)
  await expect(page).toHaveScreenshot('auth.png', { maxDiffPixelRatio: 0.02 })
})

test('welcome wizard baseline', async ({ page }) => {
  await register(page, email())
  await blur(page)
  await expect(page).toHaveScreenshot('welcome.png', { maxDiffPixelRatio: 0.02, mask: MASKS(page).concat([page.locator('.avatar')]) })
})

test('workspace jobs tab baseline', async ({ page }) => {
  await register(page, email())
  await createWorkspace(page)
  await expect(page.getByText(/Page \d+ of \d+ ·/)).toBeVisible()
  await blur(page)
  await expect(page).toHaveScreenshot('workspace-jobs.png', { maxDiffPixelRatio: 0.02, mask: MASKS(page).concat([page.locator('.avatar')]) })

  // Contrast must hold on this densest surface too — no exclusions.
  const results = await new AxeBuilder({ page }).analyze()
  const serious = results.violations.filter(v => v.impact === 'critical' || v.impact === 'serious')
  expect(serious, JSON.stringify(serious.map(v => ({ id: v.id })))).toEqual([])
})

test('dlq tab baseline', async ({ page }) => {
  await register(page, email())
  await createWorkspace(page)
  await page.getByRole('tab', { name: /Dead letters/ }).click()
  await expect(page.getByText('Nothing dead here.')).toBeVisible()
  await blur(page)
  await expect(page).toHaveScreenshot('dlq.png', { maxDiffPixelRatio: 0.02, mask: MASKS(page).concat([page.locator('.avatar')]) })
})
