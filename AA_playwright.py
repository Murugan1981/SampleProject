import asyncio
import pandas as pd
from playwright.async_api import async_playwright

# ================= CONFIG =================
SAMPLE_URL = "https://petstore.swagger.io/"   # <-- replace with your real URL
OUTPUT_EXCEL = "get_endpoints_available_values.xlsx"

# XPath / CSS selectors
GET_BLOCK_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
GET_PATH_XPATH = './/span[contains(@class,"opblock-summary-path")]'
EXPAND_BTN_CSS = 'button.opblock-summary'
AVAILABLE_VALUES_P_XPATH = './/p[i[text()="Available values"]]'

# ==========================================

async def extract_get_available_values(url):
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        print(f"Opening URL: {url}")
        await page.goto(url)
        await page.wait_for_timeout(4000)

        # ---------------- FIND GET ENDPOINTS ----------------
        get_blocks = await page.locator(f'xpath={GET_BLOCK_XPATH}').all()
        print(f"\nTotal GET endpoints found: {len(get_blocks)}")

        for idx, get_block in enumerate(get_blocks, start=1):

            # -------- Endpoint path --------
            try:
                endpoint = await get_block.locator(f'xpath={GET_PATH_XPATH}').inner_text()
            except Exception:
                endpoint = "UNKNOWN"

            print(f"\n[{idx}] Endpoint: {endpoint}")

            # -------- Expand if collapsed --------
            try:
                expand_btn = get_block.locator(EXPAND_BTN_CSS)
                if await expand_btn.count() > 0 and await expand_btn.is_visible():
                    await expand_btn.scroll_into_view_if_needed()
                    await expand_btn.click(timeout=2000)
                    print("  Expanded endpoint")
            except Exception as e:
                print(f"  Expand skipped: {e}")

            await page.wait_for_timeout(300)

            # -------- Available Values Extraction --------
            available_values = []
            try:
                av_p = get_block.locator(f'xpath={AVAILABLE_VALUES_P_XPATH}')
                if await av_p.count() > 0:
                    full_text = await av_p.inner_text()

                    # Example text:
                    # Available values " : Undefined, MHI, MHEU, MSUKG, MHBK, MHSS, MBE"
                    if ":" in full_text:
                        values_part = full_text.split(":", 1)[1]
                        values_part = values_part.replace('"', '').strip()
                        available_values = [
                            v.strip() for v in values_part.split(",") if v.strip()
                        ]

                    print(f"  Available values: {available_values}")
                else:
                    print("  No available values")
            except Exception as e:
                print(f"  Error reading available values: {e}")

            results.append({
                "Endpoint": endpoint,
                "AvailableValues": ", ".join(available_values)
            })

        await browser.close()

    # ---------------- SAVE TO EXCEL ----------------
    df = pd.DataFrame(results)
    df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"\nExtraction completed. Output written to: {OUTPUT_EXCEL}")


# ---------------- RUN ----------------
if __name__ == "__main__":
    asyncio.run(extract_get_available_values(SAMPLE_URL))
