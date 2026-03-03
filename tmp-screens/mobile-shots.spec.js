const { test, expect } = require('@playwright/test');

test.use({ ...require('@playwright/test').devices['iPhone 13'] });

test('capture mobile screenshots', async ({ page }) => {
  const base = 'http://127.0.0.1:4173';
  const out = 'c:/Users/izzat/Desktop/f1-dual/tmp-screens';

  await page.goto(base, { waitUntil: 'networkidle' });
  await page.waitForTimeout(1500);

  await page.screenshot({ path: `${out}/mobile-fullpage-casual-round1.png`, fullPage: true });

  await page.getByRole('button', { name: 'Nerd Mode' }).click();
  await page.waitForTimeout(1600);
  await page.screenshot({ path: `${out}/mobile-nerd-mode.png`, fullPage: true });

  await page.getByRole('button', { name: 'Lap-by-Lap' }).click();
  await page.waitForTimeout(2200);
  await page.screenshot({ path: `${out}/mobile-lap-by-lap.png`, fullPage: true });

  await page.getByRole('button', { name: 'Casual Mode' }).click();
  await page.waitForTimeout(600);

  const roundSelect = page.locator('.mode-switch select').nth(1);
  await roundSelect.selectOption('2');
  await page.waitForTimeout(2200);
  await page.screenshot({ path: `${out}/mobile-sprint-weekend-round2.png`, fullPage: true });
});
