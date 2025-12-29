import asyncio
from playwright.async_api import async_playwright
import pandas as pd

sample_url = "https://petstore.swagger.io/"   # Change to your real Swagger URL

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
        # This XPath selects a <p> containing <i>Available values</i>
        AVAILABLE_VALUES_PARENT_XPATH = './/p[i[translate(text(),"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")="available values"]]'
        
        result_rows = []

        for i, get_block in enumerate(get_blocks):
            # Get endpoint path
            try:
                endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
            except Exception:
                endpoint_path = "Unknown"

            print(f"\n[{i+1}] Endpoint: {endpoint_path}")

            # Try to expand (but skip if not present or already open)
            expand_btn = get_block.locator(GET_EXPAND_BTN_CSS)
            try:
                if await expand_btn.count() > 0 and await expand_btn.is_visible():
                    await expand_btn.scroll_into_view_if_needed()
                    await expand_btn.click(timeout=3000)
                    print("  Clicked to expand endpoint.")
                else:
                    print("  Expand button not present or not visible; maybe already expanded.")
            except Exception as ex:
                print(f"  Expand click failed: {ex}")

            await page.wait_for_timeout(500)

            # --- Extract available values ---
            available_values_p = get_block.locator(f'xpath={AVAILABLE_VALUES_PARENT_XPATH}')
            if await available_values_p.count() > 0:
                full_text = await available_values_p.inner_text()
                # Debug print: see the actual text
                print(f"  Raw available values text: '{full_text.strip()}'")
                # Remove label, get after ":"
                after_colon = full_text.split(":", 1)[-1].strip()
                after_colon = after_colon.strip('"').strip("'")
                available_values = [v.strip() for v in after_colon.split(",") if v.strip()]
                print(f"  Found {len(available_values)} available value(s): {available_values}")
            else:
                available_values = []
                print("  No available values found.")

            result_rows.append({
                "Endpoint": endpoint_path,
                "AvailableValues": ", ".join(available_values) if available_values else ""
            })

        await browser.close()
        # Save to Excel
        df = pd.DataFrame(result_rows)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nExtraction complete. Results saved to {EXCEL_FILE}")

if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(sample_url))
