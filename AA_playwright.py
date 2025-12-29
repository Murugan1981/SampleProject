import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# ================= CONFIGURATION =================
# Replace with your actual URL
SAMPLE_URL = "https://petstore.swagger.io/" 
EXCEL_FILE = "get_endpoints_values.xlsx"

# Selectors
GET_BLOCK_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'
EXPAND_BTN_CSS = 'button.opblock-summary'

# Based on your HTML: <div class="parameter__enum renderedMarkdown">
ENUM_CONTAINER_CSS = '.parameter__enum' 
# =================================================

async def extract_get_endpoints_and_values(url):
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"Opening URL: {url}")
        await page.goto(url)
        # Give it ample time to load the initial UI
        await page.wait_for_timeout(4000)

        # Find all GET blocks
        get_blocks = await page.locator(f'xpath={GET_BLOCK_XPATH}').all()
        print(f"Found {len(get_blocks)} GET endpoints.")

        result_rows = []

        for i, get_block in enumerate(get_blocks):
            # 1. Extract Endpoint Name
            try:
                endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
            except Exception:
                endpoint_path = "Unknown"
            
            print(f"\n[{i+1}/{len(get_blocks)}] Processing: {endpoint_path}")

            # 2. Expand the Block
            # We check if expand button exists and is visible. 
            # If the block is already expanded (rare but possible), we skip clicking.
            expand_btn = get_block.locator(EXPAND_BTN_CSS)
            try:
                if await expand_btn.count() > 0 and await expand_btn.is_visible():
                    # Check if it has the class 'is-open' or similar logic if needed, 
                    # but usually clicking the summary expands/collapses.
                    # We assume it starts collapsed.
                    await expand_btn.scroll_into_view_if_needed()
                    await expand_btn.click(timeout=2000)
                    # print("  -> Expanded.")
                else:
                    print("  -> Expand button not visible (might be hidden or already open).")
            except Exception as ex:
                print(f"  -> Warning: Could not click expand: {ex}")

            # WAIT for the animation/rendering of parameters
            await page.wait_for_timeout(1000)

            # 3. Extract Available Values
            # Strategy: Find ALL elements with class 'parameter__enum' inside this block
            all_values_found = []
            
            try:
                # Locate all enum blocks within this GET endpoint
                enum_divs = await get_block.locator(ENUM_CONTAINER_CSS).all()
                
                if enum_divs:
                    for enum_div in enum_divs:
                        text_content = await enum_div.inner_text()
                        # Clean up text. Expected format: "Available values : Undefined, MHI, ..."
                        # Remove newlines and extra spaces
                        text_content = " ".join(text_content.split())
                        
                        if "Available values" in text_content:
                            # Split by 'Available values' to get the right side
                            # Handle cases like "Available values : " or "Available values"
                            parts = text_content.split("Available values")
                            if len(parts) > 1:
                                raw_vals = parts[1]
                                # Remove colon if present
                                raw_vals = raw_vals.replace(":", "").strip()
                                # Remove quotes if present
                                raw_vals = raw_vals.replace('"', "").replace("'", "")
                                
                                # Split by comma
                                items = [x.strip() for x in raw_vals.split(",") if x.strip()]
                                all_values_found.extend(items)
                
                # Deduplicate and store
                final_values = list(dict.fromkeys(all_values_found))
                
                if final_values:
                    print(f"  -> Found Values: {final_values}")
                else:
                    print("  -> No values found.")
                    # DEBUG: If 0 values, print a small part of HTML to see if we missed something
                    # inner_html = await get_block.inner_html()
                    # print(f"DEBUG HTML: {inner_html[:300]}...")

            except Exception as e:
                print(f"  -> Error extracting values: {e}")
                final_values = []

            # 4. Save to list
            result_rows.append({
                "Endpoint": endpoint_path,
                "AvailableValues": ", ".join(final_values) if final_values else ""
            })

        # Close browser
        await browser.close()

        # 5. Write to Excel
        df = pd.DataFrame(result_rows)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nSUCCESS: Data saved to {EXCEL_FILE}")

if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(SAMPLE_URL))
