import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# ================= CONFIGURATION =================
# Replace with your actual URL
SAMPLE_URL = "https://petstore.swagger.io/" 
EXCEL_FILE = "get_endpoints_values.xlsx"

# 1. Select the main block for GET requests
GET_BLOCK_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'

# 2. Select the Summary/Header (Changed to generic class to match your DIV)
EXPAND_BTN_CSS = '.opblock-summary'

# 3. Select the Path/Endpoint Name
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'

# 4. Select the Available Values container (from your earlier snippet)
ENUM_CONTAINER_CSS = '.parameter__enum' 
# =================================================

async def extract_get_endpoints_and_values(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"Opening URL: {url}")
        await page.goto(url)
        await page.wait_for_timeout(4000)

        # Find all GET blocks
        get_blocks = await page.locator(f'xpath={GET_BLOCK_XPATH}').all()
        print(f"Found {len(get_blocks)} GET endpoints.")

        result_rows = []

        for i, get_block in enumerate(get_blocks):
            # --- 1. Get Endpoint Name ---
            try:
                endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
            except Exception:
                endpoint_path = "Unknown"
            
            print(f"\n[{i+1}/{len(get_blocks)}] Processing: {endpoint_path}")

            # --- 2. Force Expand ---
            # We target the '.opblock-summary' div directly.
            expand_target = get_block.locator(EXPAND_BTN_CSS)
            
            # Check if already expanded by looking for the body section
            # (The body usually has class 'opblock-body')
            is_expanded = await get_block.locator('.opblock-body').count() > 0

            if not is_expanded:
                try:
                    if await expand_target.count() > 0:
                        # Scroll to it to ensure clicks work
                        await expand_target.scroll_into_view_if_needed()
                        await expand_target.click(timeout=2000)
                        # print("  -> Clicked expand.")
                        
                        # CRITICAL: Wait for the body to actually render
                        try:
                            await get_block.locator('.opblock-body').wait_for(state="visible", timeout=2000)
                        except:
                            pass # Proceed anyway, sometimes animation is slow
                    else:
                        print("  -> Expand target not found.")
                except Exception as ex:
                    print(f"  -> Click failed: {ex}")
            else:
                pass
                # print("  -> Already expanded.")

            # --- 3. Extract Values ---
            all_values_found = []
            
            try:
                # Find all parameter__enum divs inside this expanded block
                enum_divs = await get_block.locator(ENUM_CONTAINER_CSS).all()
                
                if enum_divs:
                    for enum_div in enum_divs:
                        text_content = await enum_div.inner_text()
                        # Cleanup text: "Available values : Undefined, MHI..."
                        text_content = " ".join(text_content.split())
                        
                        if "Available values" in text_content:
                            # Split and parse
                            parts = text_content.split("Available values")
                            if len(parts) > 1:
                                raw_vals = parts[1]
                                raw_vals = raw_vals.replace(":", "").strip()
                                raw_vals = raw_vals.replace('"', "").replace("'", "")
                                
                                items = [x.strip() for x in raw_vals.split(",") if x.strip()]
                                all_values_found.extend(items)
                
                final_values = list(dict.fromkeys(all_values_found))
                
                if final_values:
                    print(f"  -> Found: {final_values}")
                else:
                    print("  -> No values found.")

            except Exception as e:
                print(f"  -> Error: {e}")
                final_values = []

            result_rows.append({
                "Endpoint": endpoint_path,
                "AvailableValues": ", ".join(final_values) if final_values else ""
            })

        await browser.close()

        # Save to Excel
        df = pd.DataFrame(result_rows)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nSaved to {EXCEL_FILE}")

if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(SAMPLE_URL))
