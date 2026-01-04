const { test } = require('@playwright/test');

test('Open intranet URL', async ({ page }) => {
    await page.goto('http://rdb/#/', {
        waitUntil: 'domcontentloaded'
    });

    await page.waitForTimeout(5000);
});
