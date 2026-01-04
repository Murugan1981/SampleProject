import { test } from '@playwright/test';

test('Open intranet SPA correctly', async ({ page }) => {
  await page.goto('http://rdb/#/', {
    waitUntil: 'networkidle'
  });

  await page.waitForTimeout(5000);
});
