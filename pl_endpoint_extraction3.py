import os
import json
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# -------------------- CONFIG --------------------
load_dotenv()

BASE_URL = "http://rdb"
ENV_FILE = ".env"
CONFIG_FILE = os.path.join("shared", "input", "ApiTestData.json")


# -------------------- LOAD CONFIG --------------------
def load_config():
    """Load configuration from ApiTestData.json"""
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Config file not found: {CONFIG_FILE}")
    
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    print(f"✓ Loaded config from {CONFIG_FILE}")
    print(f"  System: {config['System']}")
    print(f"  Env_Target: {config['Env_Target']}")
    print(f"  Env_Source: {config['Env_Source']}")
    print(f"  Region: {config['Region']}")
    print(f"  URLTYPE: {config['URLTYPE']}")
    
    return config


# -------------------- MAIN FLOW --------------------
async def run():
    # Load configuration
    config = load_config()
    
    system = config['System']
    env_target = config['Env_Target']
    env_source = config['Env_Source']
    region = config['Region']
    url_type = config['URLTYPE']
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION --------
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        # Select Region
        await page.get_by_text(region, exact=True).click()
        
        # Select System
        await page.get_by_text("Select system...").click()
        await page.get_by_role("searchbox", name="Filter").fill(system.lower())
        await page.get_by_role("option", name=system).click()
        
        # Select Environment
        button_text = f"{system} | {env_target} | {region}"
        await page.get_by_role("button", name=button_text).click()
        
        # Navigate to Service based on URLTYPE
        service_name = "Data Service" if url_type == "DATASERVICE" else url_type
        await page.get_by_role("link", name=service_name).click()
        await page.get_by_role("link", name=service_name).press("NumLock")
        
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
                
                # -------- SAVE TO .env --------
                if not os.path.exists(ENV_FILE):
                    open(ENV_FILE, "w").close()

                # Save with dynamic key: {System}_{Region}_{Env_Target}_{Env_Source}
                env_key = f"{system}_{region}_{env_target}_{env_source}"
                set_key(ENV_FILE, env_key, iframe_src)
                
                print(f"\n{'='*60}")
                print(f"✓ Saved to {ENV_FILE}")
                print(f"  Variable: {env_key}")
                print(f"  Value: {iframe_src}")
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

        # Save with dynamic key: {System}_{Region}_{Env_Target}_{Env_Source}
        env_key = f"{system}_{region}_{env_target}_{env_source}"
        set_key(ENV_FILE, env_key, iframe_src)
        
        print(f"\n{'='*60}")
        print(f"✓ Saved to {ENV_FILE}")
        print(f"  Variable: {env_key}")
        print(f"  Value: {iframe_src}")
        print(f"{'='*60}\n")

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
