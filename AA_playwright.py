import asyncio
from playwright.async_api import async_playwright
import pandas as pd

sample_url = "https://petstore.swagger.io/"  # Change to your real Swagger URL

EXCEL_FILE = "get_endpoints_values.xlsx"

async def extract_get_endpoints_and_values(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url)
        await page.wait_for_timeout(3000)

        # Check for iframes
        frames = page.frames
        print(f"Total frames: {len(frames)}")
        mainframe = page.main_frame
        target_frame = mainframe

        for i, f in enumerate(frames):
            print(f"[{i}] Frame name: {f.name}, url: {f.url}")

        # You may need to adjust the index if your endpoints are inside an iframe
        # For petstore.swagger.io, endpoints are in the main frame

        # Try multiple XPATHs
        GET_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
        get_blocks = await target_frame.locator(GET_XPATH).all()
        print(f"Found {len(get_blocks)} GET endpoints using '{GET_XPATH}'.")

        if len(get_blocks) == 0:
            # Try fallback
            fallback_xpath = '//div[contains(@class,"opblock-get")]'
            get_blocks = await target_frame.locator(fallback_xpath).all()
            print(f"Fallback: Found {len(get_blocks)} GET endpoints using '{fallback_xpath}'.")

        # Print class for each found block for debugging
        for i, block in enumerate(get_blocks):
            class_attr = await block.get_attribute("class")
            print(f"  [{i}] class={class_attr}")

        # Now extract endpoints and values
        GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'
        GET_EXPAND_BTN_XPATH = './/button[contains(@class,"opblock-summary")]'
        AVAILABLE_VALUES_XPATH = './/i[text()="Available Values"]/following-sibling::*'

        result_rows = []

        for i, get_block in enumerate(get_blocks):
            try:
                endpoint_path = await get_block.locator(GET_SUMMARY_XPATH).inner_text()
            except Exception:
                endpoint_path = "Unknown"

            print(f"\n[{i+1}] Endpoint: {endpoint_path}")

            # Expand the GET block
            try:
                expand_btn = get_block.locator(GET_EXPAND_BTN_XPATH)
                await expand_btn.click()
                print("  Clicked to expand endpoint.")
            except Exception as ex:
                print(f"  Expand click failed: {ex}")

            await page.wait_for_timeout(700)

            # Check for "Available Values" block
            available_values_elems = await get_block.locator(AVAILABLE_VALUES_XPATH).all()
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

        # Save to Excel
        df = pd.DataFrame(result_rows)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nExtraction complete. Results saved to {EXCEL_FILE}")

# Run it!
if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(sample_url))
