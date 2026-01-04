import os
import asyncio
import pandas as pd
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -------------------- CONFIG --------------------
load_dotenv()

BASE_URL = "http://rdb"
ENV_FILE = ".env"
OUTPUT_FILE = os.path.join("API", "reports", "endpoints.xlsx")

GET_BLOCK_SELECTOR = "div.opblock.opblock-get"
PATH_SELECTOR = ".opblock-summary-path"
PARAM_ROW_SELECTOR = "tr"
PARAM_NAME_SELECTOR = ".parameter__name"
PARAM_ENUM_SELECTOR = ".parameter__enum"

# -------------------- SWAGGER EXTRACTION --------------------
async def extract_first_get_endpoint(page):
    await page.wait_for_selector("h4[id^='operations-tag'] span", timeout=15000)
    get_blocks = await page.query_selector_all(GET_BLOCK_SELECTOR)
    if not get_blocks:
        print("No GET endpoints found.")
        return []

    block = get_blocks[0]  # Only the first GET endpoint

    try:
        await block.click()
        await page.wait_for_timeout(300)

        # Endpoint path
        path_el = await block.query_selector(PATH_SELECTOR)
        endpoint = (await path_el.inner_text()).strip() if path_el else ""

        # TAG extraction
        tag_el = await block.query_selector(
            "xpath=ancestor::div[contains(@class,'opblock-tag-section')]"
            "//h4[contains(@id,'operations-tag')]//span"
        )
        tag = (await tag_el.inner_text()).strip() if tag_el else "UNKNOWN"

        # PARAMETER extraction
        parameters = {}
        rows = await block.query_selector_all(PARAM_ROW_SELECTOR)
        for row in rows:
            name_el = await row.query_selector(PARAM_NAME_SELECTOR)
            enum_el = await row.query_selector(PARAM_ENUM_SELECTOR)
            if not name_el:
                continue
            param_name = (await name_el.inner_text()).strip()
            if enum_el:
                values = (
                    await enum_el.inner_text()
                ).replace("Available values:", "").strip()
            else:
                values = ""
            parameters[param_name] = values

        row = {
            "tag": tag,
            "method": "GET",
            "endpoint": endpoint
        }
        row.update(parameters)
        return [row]

    except Exception as e:
        print(f"Skipped endpoint due to error: {e}")
        return []

# -------------------- PROCESS ENV --------------------
async def process_environment(env_name, swagger_url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(swagger_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        data = await extract_first_get_endpoint(page)
        await browser.close()
        return pd.DataFrame(data)

# -------------------- MAIN FLOW --------------------
async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION --------
        print("Opening dashboard:", BASE_URL)
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded")
        except PlaywrightTimeoutError:
            print("Timeout loading BASE_URL")
            return

        # Strong wait for SELECT button
        try:
            await page.get_by_role("button", name="Select").wait_for(timeout=15000)
        except PlaywrightTimeoutError:
            print("Could not find button 'Select'. Check if the label/case has changed.")
            return

        await page.get_by_role("button", name="Select").click()
        await page.get_by_role("searchbox", name="Filter").fill("ares")
        await page.get_by_role("option", name="Ares").click()
        await page.get_by_role("button", name="Ares | PRD | Asia").click()
        await page.get_by_role("link", name="Data Service").click()
        await page.wait_for_selector("iframe", timeout=10000)

        # -------- RESOLVE SWAGGER URL --------
        iframe = page.frame_locator("iframe")
        swagger_url = await iframe.locator("code").filter(has_text="http").first.inner_text()
        if not swagger_url:
            raise Exception("Swagger URL not found")

        print("Resolved Swagger URL:", swagger_url)
        if not os.path.exists(ENV_FILE):
            open(ENV_FILE, "w").close()
        set_key(ENV_FILE, "JIL_DATASERVICE_URL", swagger_url)
        await browser.close()

    # -------- EXTRACT ONLY FIRST ENDPOINT --------
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df = await process_environment("SOURCE", swagger_url)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ENDPOINT", index=False)

if __name__ == "__main__":
    asyncio.run(run())
