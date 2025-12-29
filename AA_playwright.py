import asyncio
from playwright.async_api import async_playwright
import pandas as pd

sample_url = "https://petstore.swagger.io/"  # Change as needed

EXCEL_FILE = "get_endpoints_values.xlsx"

async def extract_get_endpoints_and_values(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url)
        await page.wait_for_timeout(3000)

        GET_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
        get_blocks = await page.locator(f'xpath={GET_XPATH}').all()
        print(f"Found {len(get_blocks)} GET endpoints.")

        GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'
        GET_EXPAND_BTN_CSS = "button.opblock-summary"
        AVAILABLE_VALUES_XPATH = './/i[text()="Available Values"]/following-sibling::*'

        result_rows = []

        for i, get_block in enumerate(get_blocks):
            try:
                endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
            except Exception:
                try:
                    # fallback: print full HTML if missing
                    html = await get_block.inner_html()
                    print(f"[{i+1}] endpoint HTML:\n{html}")
                except Exception:
                    pass
                endpoint_path = "Unknown"

            print(f"\n[{i+1}] Endpoint: {endpoint_path}")

            try:
                expand_btn = get_block.locator(GET_EXPAND_BTN_CSS)
                await expand_btn.click()
                print("  Clicked to expand endpoint.")
            except Exception as ex:
                print(f"  Expand click failed: {ex}")

            await page.wait_for_timeout(700)

            available_values_elems = await get_block.locator(f'xpath={AVAILABLE_VALUES_XPATH}').all()
            if available_values_elems:
                available_values = [await elem.inner_text() for elem in available_values_elems]
                available_values = [v.strip() for v in available_values if v.strip()]
                print(f"  Found {len(available_values)} available value(s): {available_values}")
            else:
                available_values = []
                print("  No available values found.")

            result_rows.append({
                "Endpoint": endpoint_path,
                "AvailableValues": ", ".join(available_values) if available_values else ""
            })

        await browser.close()
        df = pd.DataFrame(result_rows)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nExtraction complete. Results saved to {EXCEL_FILE}")

if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(sample_url))
