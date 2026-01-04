import os
import asyncio
import pandas as pd
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

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
async def extract_endpoints(page):
    results = []

    # Ensure tags are rendered
    await page.wait_for_selector("h4[id^='operations-tag'] span", timeout=10000)

    get_blocks = await page.query_selector_all(GET_BLOCK_SELECTOR)
    print(f"Found {len(get_blocks)} GET endpoints")

    for block in get_blocks:
        try:
            # Expand GET block
            await block.click()
            await page.wait_for_timeout(300)

            # Endpoint path
            path_el = await block.query_selector(PATH_SELECTOR)
            endpoint = (await path_el.inner_text()).strip() if path_el else ""

            # -------- TAG EXTRACTION (FIXED) --------
            tag_el = await block.query_selector(
                "xpath=ancestor::div[contains(@class,'opblock-tag-section')]"
                "//h4[contains(@id,'operations-tag')]//span"
            )
            tag = (await tag_el.inner_text()).strip() if tag_el else "UNKNOWN"

            # -------- PARAMETER EXTRACTION --------
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

            # -------- ROW OUTPUT --------
            row = {
                "tag": tag,
                "method": "GET",
                "endpoint": endpoint
            }

            row.update(parameters)
            results.append(row)

        except Exception as e:
            print(f"Skipped endpoint due to error: {e}")

    return results


# -------------------- PROCESS ENV --------------------
async def process_environment(env_name, swagger_url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(swagger_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        data = await extract_endpoints(page)

        await browser.close()
        return pd.DataFrame(data)


# -------------------- MAIN FLOW --------------------
async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION --------
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        await page.get_by_text("Europe", exact=True).click()
        await page.get_by_text("Select system...").click()
        await page.get_by_role("searchbox", name="Filter").fill("jil")
        await page.get_by_role("option", name="JIL").click()
        await page.get_by_role("button", name="JIL | SIT2 | Europe").click()
        await page.get_by_role("link", name="Data Service").click()
        await page.get_by_role("link", name="Data Service").press("NumLock")
        
        # -------- WAIT FOR IFRAME (FIXED) --------
        try:
            # Wait for iframe element to be present in DOM
            await page.wait_for_selector("iframe", timeout=15000, state="attached")
            print("✓ Iframe found in DOM")
            
            # Additional wait for iframe content to load
            await page.wait_for_timeout(2000)
            
            # Get the iframe element
            iframe_element = await page.query_selector("iframe")
            
            if iframe_element:
                # Wait for iframe to have a valid src
                iframe_src = await iframe_element.get_attribute("src")
                print(f"\n{'='*60}")
                print(f"IFRAME SRC: {iframe_src}")
                print(f"{'='*60}\n")
                
                # Stop here as requested
                await browser.close()
                return
            
        except Exception as e:
            print(f"Error waiting for iframe: {e}")
            # Take screenshot for debugging
            await page.screenshot(path="debug_iframe_error.png")
            await browser.close()
            raise

        # -------- RESOLVE SWAGGER URL (FIXED) --------
        try:
            # Use frame_locator
            iframe = page.frame_locator("iframe")
            
            # Wait for the code element containing the URL
            code_locator = iframe.locator("code").filter(has_text="http").first
            await code_locator.wait_for(timeout=10000, state="visible")
            
            swagger_url = await code_locator.inner_text()
            
            if not swagger_url:
                raise Exception("Swagger URL not found in iframe")
            
            print(f"✓ Swagger URL extracted: {swagger_url}")
            
        except Exception as e:
            print(f"Error extracting Swagger URL: {e}")
            
            # Fallback: Try using frames() method
            try:
                print("Attempting fallback method...")
                frames = page.frames
                print(f"Total frames found: {len(frames)}")
                
                for idx, frame in enumerate(frames):
                    print(f"Frame {idx}: {frame.url}")
                    
                    # Skip main frame
                    if frame == page.main_frame:
                        continue
                    
                    # Try to find code element in this frame
                    try:
                        code_elements = await frame.query_selector_all("code")
                        for code in code_elements:
                            text = await code.inner_text()
                            if "http" in text:
                                swagger_url = text.strip()
                                print(f"✓ Found URL in frame {idx}: {swagger_url}")
                                break
                        
                        if swagger_url:
                            break
                    except:
                        continue
                
                if not swagger_url:
                    raise Exception("Could not find Swagger URL using fallback method")
                    
            except Exception as fallback_error:
                print(f"Fallback method failed: {fallback_error}")
                await page.screenshot(path="debug_final_error.png")
                raise

        # -------- SAVE TO .env --------
        if not os.path.exists(ENV_FILE):
            open(ENV_FILE, "w").close()

        set_key(ENV_FILE, "JIL_DATASERVICE_URL", swagger_url)
        print(f"✓ Saved to {ENV_FILE}")

        await browser.close()

    # -------- EXTRACT ENDPOINTS --------
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df = await process_environment("SOURCE", swagger_url)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ENDPOINTS", index=False)
    
    print(f"✓ Endpoints saved to {OUTPUT_FILE}")


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    asyncio.run(run())
