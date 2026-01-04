import asyncio
from playwright.async_api import async_playwright
from dotenv import set_key
import os

BASE_URL = "http://rdb"
ENV_FILE = ".env"

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # ---------------------------
        # OPEN APPLICATION
        # ---------------------------
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        # ---------------------------
        # TOP LEVEL PAGE CLICKS
        # ---------------------------
        await page.get_by_role("button", name="Select").click()
        await page.get_by_role("searchbox", name="Filter").click()
        await page.get_by_role("searchbox", name="Filter").fill("ares")
        await page.get_by_role("option", name="Ares").click()
        await page.get_by_role("button", name="Ares | PRD | Asia").click()

        await page.get_by_role("link", name="Data Service").click()
        await page.get_by_role("link", name="Data Service").press("NumLock")

        # ---------------------------
        # SWITCH TO IFRAME  (CRITICAL)
        # ---------------------------
        iframe = page.frame_locator("iframe")

        # Try it out
        await iframe.get_by_role("button", name="Try it out").click()

        # reportingDate
        await iframe.get_by_role("textbox", name="reportingDate").click()
        await iframe.get_by_role("textbox", name="reportingDate").fill("2025-11-23")

        # Execute
        await iframe.get_by_role("button", name="Execute").click()

        await page.wait_for_timeout(3000)

        # ---------------------------
        # EXTRACT RESULT TEXT (ENTITY)
        # ---------------------------
        result_text = await iframe.get_by_text(
            "http://apw-riskrem01.uk."
        ).nth(1).inner_text()

        print("Extracted URL:", result_text)

        # ---------------------------
        # SAVE TO .env
        # ---------------------------
        if not os.path.exists(ENV_FILE):
            open(ENV_FILE, "w").close()

        set_key(ENV_FILE, "JIL_DATASERVICE_URL", result_text)

        await browser.close()

asyncio.run(run())
