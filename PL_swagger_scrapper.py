import asyncio
import pandas as pd
from playwright.async_api import async_playwright

# ================= PLAYWRIGHT CONFIGURATION  =================
GET_BLOCK_XPATH = '//div[contains(@class,"opblock") and contains(@class,"opblock-get")]'
EXPAND_BTN_CSS = '.opblock-summary'
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'

PARAM_ROW_CSS = 'tr' 
PARAM_NAME_CSS = '.parameter__name'
PARAM_ENUM_CSS = '.parameter__enum'
# =========================================================================

async def scrape_single_env(url, context):
    # Opens a page and scrapes GET endpoints + Enum parameters
    page = await context.new_page()
    print(f"   -> Opening: {url}")
    
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(4000)
    except Exception as e:
        print(f"   -> Failed to load {url}: {e}")
        await page.close()
        return [] 

    # Find all GET blocks
    get_blocks = await page.locator(f'xpath={GET_BLOCK_XPATH}').all()
    print(f"   -> Found {len(get_blocks)} GET endpoints.")

    all_endpoints_data = []

    for i, get_block in enumerate(get_blocks):
        try:
            endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
        except Exception:
            endpoint_path = "Unknown"
        
        current_row_data = {"Endpoint": endpoint_path}

        # Force Expand Logic
        expand_target = get_block.locator(EXPAND_BTN_CSS)
        if await expand_target.count() > 0:
            is_expanded = await get_block.locator('.opblock-body').count() > 0
            if not is_expanded:
                try:
                    await expand_target.scroll_into_view_if_needed()
                    await expand_target.click(timeout=1000)
                    try:
                        await get_block.locator('.opblock-body').wait_for(state="visible", timeout=2000)
                    except:
                        pass
                except:
                    pass

        # Extract Values Logic
        try:
            rows = await get_block.locator(PARAM_ROW_CSS).all()
            for row in rows:
                name_el = row.locator(PARAM_NAME_CSS)
                if await name_el.count() > 0:
                    raw_name = await name_el.inner_text()
                    # Clean name
                    param_name = raw_name.replace('*', '').strip().split('\n')[0].strip()

                    # Extract Enum
                    enum_el = row.locator(PARAM_ENUM_CSS)
                    if await enum_el.count() > 0:
                        text_content = await enum_el.inner_text()
                        text_content = " ".join(text_content.split())
                        
                        if "Available values" in text_content:
                            parts = text_content.split("Available values")
                            if len(parts) > 1:
                                raw_vals = parts[1].replace(":", "").replace('"', "").replace("'", "").strip()
                                clean_values_list = [x.strip() for x in raw_vals.split(",") if x.strip()]
                                final_val_str = ", ".join(clean_values_list)
                                if final_val_str:
                                    current_row_data[param_name] = final_val_str
        except Exception as e:
            pass 

        all_endpoints_data.append(current_row_data)

    await page.close()
    return all_endpoints_data

async def run(dev_url, prod_url, output_file):
    # Orchestrator: Scrapes both environments and saves to multi-sheet Excel
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()

        # 1. Scrape Source (Dev)
        print(f"Processing SOURCE (Dev)...")
        dev_data = await scrape_single_env(dev_url, context)
        df_dev = pd.DataFrame(dev_data)

        # 2. Scrape Target (Prod)
        print(f"Processing TARGET (Prod)...")
        prod_data = await scrape_single_env(prod_url, context)
        df_prod = pd.DataFrame(prod_data)

        await browser.close()

    # 3. Save to Excel with separate sheets
    print(f"Saving to {output_file}...")
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if not df_dev.empty:
                # Reorder columns to ensure Endpoint is first
                cols = ['Endpoint'] + [c for c in df_dev.columns if c != 'Endpoint']
                df_dev[cols].to_excel(writer, sheet_name='SOURCE', index=False)
            
            if not df_prod.empty:
                cols = ['Endpoint'] + [c for c in df_prod.columns if c != 'Endpoint']
                df_prod[cols].to_excel(writer, sheet_name='TARGET', index=False)
        print("Extraction Complete.")
    except Exception as e:
        print(f"Failed to save Excel: {e}")
