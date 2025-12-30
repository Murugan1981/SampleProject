import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os

async def scrape_swagger_endpoints(page, url, label):
    """
    Visits the Swagger UI, waits for it to load, and extracts API definitions.
    """
    print(f"   [{label}] 🚀 Launching Browser for {label}...")
    print(f"   [{label}] 🌐 Navigating to: {url}")
    
    try:
        # 1. Navigate
        await page.goto(url, wait_until='networkidle', timeout=60000)
        print(f"   [{label}] ✅ Page Loaded. Title: {await page.title()}")
        
        # 2. Wait for Swagger UI container
        # Note: Swagger UI usually has a main container.
        print(f"   [{label}] ⏳ Waiting for Swagger UI elements to render...")
        await page.wait_for_selector('.opblock', timeout=10000)
        
        # 3. Extract Elements
        # We look for the Summary block which contains Method (GET/POST) and Path (/api/v1/...)
        ops = await page.query_selector_all('.opblock')
        total_ops = len(ops)
        print(f"   [{label}] 🧐 Found {total_ops} raw operation blocks. Parsing details...")

        data = []
        
        for i, op in enumerate(ops):
            # Extract Method (GET, POST, etc.)
            method_el = await op.query_selector('.opblock-summary-method')
            path_el = await op.query_selector('.opblock-summary-path')
            
            if method_el and path_el:
                method = await method_el.inner_text()
                path = await path_el.inner_text()
                
                # We only care about GET requests for now (Safe to test)
                if method.strip().upper() == 'GET':
                    # Optional: Extract Description
                    desc_el = await op.query_selector('.opblock-summary-description')
                    desc = await desc_el.inner_text() if desc_el else ""
                    
                    data.append({
                        "Endpoint": path.strip(),
                        "Method": method.strip(),
                        "Description": desc.strip()
                    })
                    # Log every 10th item so we know it's working but don't spam
                    if len(data) % 10 == 0:
                        print(f"   [{label}]    -> Extracted {len(data)} GET endpoints so far...")

        print(f"   [{label}] 🎉 Finished! Total GET Endpoints captured: {len(data)}")
        return data

    except Exception as e:
        print(f"   [{label}] ❌ Error scraping: {e}")
        return []

async def run(dev_url, prod_url, output_file):
    print(f"--- Starting Scraper ---")
    
    async with async_playwright() as p:
        # Headless=False so you can see it working (Change to True to hide it)
        browser = await p.chromium.launch(headless=False) 
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()

        # 1. Scrape DEV
        dev_data = await scrape_swagger_endpoints(page, dev_url, "DEV")
        
        # 2. Scrape PROD
        # (Optional: In real life, Prod Swagger might be same structure, 
        # but we check both just in case definitions differ)
        # prod_data = await scrape_swagger_endpoints(page, prod_url, "PROD")
        
        await browser.close()
        
    # 3. Save to Excel
    if dev_data:
        print(f"--- Saving {len(dev_data)} endpoints to Excel ---")
        df = pd.DataFrame(dev_data)
        
        # We explicitly name the sheet 'SOURCE' because test_generator expects it
        with pd.ExcelWriter(output_file) as writer:
            df.to_excel(writer, sheet_name='SOURCE', index=False)
            
        print(f"✅ Metadata saved to: {output_file}")
    else:
        print("⚠️ No data found. Excel not created.")
