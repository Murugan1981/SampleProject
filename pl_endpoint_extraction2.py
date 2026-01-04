import os
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# CONFIG
load_dotenv()
BASE_URL = "http://rdb"
ENV_FILE = ".env"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # --------- Step 1: Open dashboard and automate navigation ---------
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.get_by_role("button", name="Select").wait_for(timeout=15000)
        await page.get_by_role("button", name="Select").click()
        await page.get_by_role("searchbox", name="Filter").fill("ares")
        await page.get_by_role("option", name="Ares").click()
        await page.get_by_role("button", name="Ares | PRD | Asia").click()
        await page.get_by_role("link", name="Data Service").click()
        await page.wait_for_selector("iframe", timeout=10000)

        # --------- Step 2: Get Swagger URL from iframe ---------
        iframe = page.frame_locator("iframe")
        swagger_url = await iframe.locator("code").filter(has_text="http").first.inner_text()
        if not swagger_url:
            raise Exception("Swagger URL not found")
        print(f"Resolved Swagger URL: {swagger_url}")

        # Optional: Save to .env
        if not os.path.exists(ENV_FILE):
            open(ENV_FILE, "w").close()
        set_key(ENV_FILE, "JIL_DATASERVICE_URL", swagger_url)

        await browser.close()  # Clean up dashboard session

    # --------- Step 3: Open Swagger UI and click first GET, Try it out ---------
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(swagger_url, wait_until="domcontentloaded")
        await page.wait_for_selector("div.opblock.opblock-get", timeout=10000)
        get_blocks = await page.query_selector_all("div.opblock.opblock-get")
        if not get_blocks:
            raise Exception("No GET endpoints found on Swagger page.")

        first_get = get_blocks[0]
        await first_get.click()
        await page.wait_for_timeout(500)  # Let the panel expand

        # Find "Try it out" within this block
        try_btn = await first_get.query_selector("button[title='Try it out'], button:has-text('Try it out')")
        if try_btn:
            await try_btn.click()
            print("Clicked 'Try it out' on first GET endpoint.")
        else:
            print("Try it out button not found in the first GET block.")

        await page.wait_for_timeout(5000)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
