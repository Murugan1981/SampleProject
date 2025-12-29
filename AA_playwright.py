import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# ================= CONFIGURATION =================
# Replace with your actual URL
SAMPLE_URL = "https://petstore.swagger.io/" 
EXCEL_FILE = "swagger_report_dynamic_columns.xlsx"

# Selectors
GET_BLOCK_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
EXPAND_BTN_CSS = '.opblock-summary'
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'

# New Selectors for Table Rows
PARAM_ROW_CSS = 'tr' 
PARAM_NAME_CSS = '.parameter__name'
PARAM_ENUM_CSS = '.parameter__enum'
# =================================================

async def extract_swagger_data(url):
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

        all_endpoints_data = []

        for i, get_block in enumerate(get_blocks):
            # --- 1. Get Endpoint Name ---
            try:
                endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
            except Exception:
                endpoint_path = "Unknown"
            
            print(f"\n[{i+1}/{len(get_blocks)}] Processing: {endpoint_path}")

            # Initialize the dictionary for this row with the Endpoint name
            # This dict will grow dynamically (e.g., adding 'tradingEntity', 'reportingDate' keys)
            current_row_data = {"Endpoint": endpoint_path}

            # --- 2. Force Expand ---
            expand_target = get_block.locator(EXPAND_BTN_CSS)
            is_expanded = await get_block.locator('.opblock-body').count() > 0

            if not is_expanded:
                try:
                    if await expand_target.count() > 0:
                        await expand_target.scroll_into_view_if_needed()
                        await expand_target.click(timeout=2000)
                        
                        # Wait for the parameters table to be visible
                        try:
                            await get_block.locator('.opblock-body').wait_for(state="visible", timeout=2000)
                        except:
                            pass 
                except Exception:
                    pass

            # --- 3. Extract Values Row by Row ---
            try:
                # Find all table rows (tr) inside this specific GET block
                rows = await get_block.locator(PARAM_ROW_CSS).all()
                
                for row in rows:
                    # check if this row has a parameter name
                    name_el = row.locator(PARAM_NAME_CSS)
                    
                    if await name_el.count() > 0:
                        # A. EXTRACT NAME
                        raw_name = await name_el.inner_text()
                        # Clean name: "tradingEntity *" -> "tradingEntity"
                        # We remove newlines, asterisks, and generic spaces
                        param_name = raw_name.replace('*', '').strip()
                        param_name = param_name.split('\n')[0].strip() 

                        # B. EXTRACT VALUES (if they exist in this row)
                        enum_el = row.locator(PARAM_ENUM_CSS)
                        
                        if await enum_el.count() > 0:
                            text_content = await enum_el.inner_text()
                            # Cleaning logic: "Available values : A, B, C"
                            text_content = " ".join(text_content.split()) # flatten whitespace
                            
                            if "Available values" in text_content:
                                parts = text_content.split("Available values")
                                if len(parts) > 1:
                                    raw_vals = parts[1]
                                    # Clean punctuation
                                    raw_vals = raw_vals.replace(":", "").replace('"', "").replace("'", "").strip()
                                    
                                    # Convert to clean comma-separated string
                                    # e.g. "MHI, MHEU, MBE"
                                    clean_values_list = [x.strip() for x in raw_vals.split(",") if x.strip()]
                                    final_val_str = ", ".join(clean_values_list)
                                    
                                    # ADD TO DICT: This creates the "column" logic
                                    if final_val_str:
                                        print(f"   -> Found {param_name}: {final_val_str}")
                                        current_row_data[param_name] = final_val_str

            except Exception as e:
                print(f"   -> Error processing parameters: {e}")

            # Add this endpoint's completed dictionary to our master list
            all_endpoints_data.append(current_row_data)

        await browser.close()

        # --- 4. Save to Excel ---
        # Pandas is smart! It will take our list of dictionaries (which might have different keys)
        # and align them perfectly into columns. Keys present in one dict but missing in others 
        # will simply be blank cells.
        df = pd.DataFrame(all_endpoints_data)
        
        # Optional: Reorder columns to put Endpoint first if needed (Pandas usually does this, but to be safe)
        cols = ['Endpoint'] + [c for c in df.columns if c != 'Endpoint']
        df = df[cols]
        
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nSUCCESS: Report saved to {EXCEL_FILE} with dynamic columns!")

if __name__ == "__main__":
    asyncio.run(extract_swagger_data(SAMPLE_URL))
