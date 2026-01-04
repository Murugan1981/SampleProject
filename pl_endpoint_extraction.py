import asyncio
from playwright.async_api import async_playwright
from dotenv import set_key
import os

ENV_FILE = ".env"
BASE_URL = "http://rdb"   # INTRANET BASE URL

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--start-maximized"]
        )

        context = await browser.new_context()
        page = await context.new_page()

        print("Opening intranet application...")
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        # ===============================
        # CLICK ELEMENTS (NO XPATH NEEDED)
        # ===============================

        # Example 1: Click button by text
        await page.get_by_role("button", name="Dashboard").click()

        # Example 2: Click dropdown
        await page.get_by_role("combobox").click()

        # Example 3: Select option
        await page.get_by_text("JIL").click()

        await page.wait_for_timeout(2000)

        # ===============================
        # EXTRACT TEXT ENTITY
        # ===============================

        # Example: visible line of text
        entity_text = await page.get_by_text("Environment").locator("..").inner_text()

        print("Extracted Entity:", entity_text)

        # ===============================
        # SAVE TO .env FILE
        # ===============================

        if not os.path.exists(ENV_FILE):
            open(ENV_FILE, "w").close()

        set_key(ENV_FILE, "DASHBOARD_ENV", entity_text)

        print("Saved to .env")

        await browser.close()

asyncio.run(run())
