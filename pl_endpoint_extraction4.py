import os
import json
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# -------------------- CONFIG --------------------
load_dotenv()

# Base URLs
SOURCE_BASE_URL = "http://rdb"
TARGET_BASE_URL = "http://rdbi"

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


# -------------------- PROCESS SOURCE (PRD) --------------------
async def process_source(config):
    """
    Process SOURCE (Production) environment
    Uses navigation from first half of playwright script (lines 4-12)
    """
    system = config['System']
    env_source = config['Env_Source']
    region = config['Region']
    url_type = config['URLTYPE']
    
    print(f"\n{'='*60}")
    print(f"🔵 Processing SOURCE Environment: {env_source}")
    print(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # -------- SOURCE UI NAVIGATION (http://rdb) --------
            await page.goto(f"{SOURCE_BASE_URL}/#/", wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)

            # Select button "Select"
            await page.get_by_role("button", name="Select").click()
            
            # Fill filter with system name (ARES)
            await page.get_by_role("searchbox", name="Filter").fill("ARES")
            
            # Click on system (Ares)
            await page.get_by_text("Ares", exact=True).click()
            
            # Check radio button for Europe
            await page.get_by_role("radio", name="Europe").check()
            
            # Click environment button (Ares | PRD | Europe)
            button_text = f"Ares | {env_source} | {region}"
            await page.get_by_role("button", name=button_text).click()
            
            # Click Data Service link
            service_name = "Data Service" if url_type == "DATASERVICE" else url_type
            await page.get_by_role("link", name=service_name).click()
            
            # Press NumLock on Data Service link
            await page.get_by_role("link", name=service_name).press("NumLock")
            
            # -------- WAIT FOR IFRAME --------
            await page.wait_for_selector("iframe", timeout=15000, state="attached")
            print("✓ Iframe found in DOM")
            
            await page.wait_for_timeout(2000)
            
            iframe_element = await page.query_selector("iframe")
            
            if iframe_element:
                iframe_src = await iframe_element.get_attribute("src")
                print(f"\n{'='*60}")
                print(f"IFRAME SRC: {iframe_src}")
                print(f"{'='*60}\n")
                
                # -------- SAVE TO .env --------
                if not os.path.exists(ENV_FILE):
                    open(ENV_FILE, "w").close()

                # Save with key: {System}_{Region}_{Env_Source}_SOURCE
                env_key = f"{system}_{region}_{env_source}_SOURCE"
                set_key(ENV_FILE, env_key, iframe_src)
                
                print(f"\n{'='*60}")
                print(f"✓ Saved to {ENV_FILE}")
                print(f"  Variable: {env_key}")
                print(f"  Value: {iframe_src}")
                print(f"{'='*60}\n")
                
                await browser.close()
                return iframe_src
            
        except Exception as e:
            print(f"❌ Error in SOURCE processing: {e}")
            await page.screenshot(path="debug_source_error.png")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------- PROCESS TARGET (SIT5) --------------------
async def process_target(config):
    """
    Process TARGET (Testing) environment
    Uses navigation from second half of playwright script (lines 23-35)
    """
    system = config['System']
    env_target = config['Env_Target']
    region = config['Region']
    url_type = config['URLTYPE']
    
    print(f"\n{'='*60}")
    print(f"🟢 Processing TARGET Environment: {env_target}")
    print(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # -------- TARGET UI NAVIGATION (http://rdbi) --------
            await page.goto(f"{TARGET_BASE_URL}/#/", wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)

            # Select button "Select"
            await page.get_by_role("button", name="Select").click()
            
            # Fill filter searchbox with "Are"
            await page.get_by_role("searchbox", name="Filter").fill("Are")
            
            # Press NumLock on searchbox
            await page.get_by_role("searchbox", name="Filter").press("NumLock")
            
            # Fill searchbox again with "Ares"
            await page.get_by_role("searchbox", name="Filter").fill("Ares")
            
            # Click on Ares text
            await page.get_by_text("Ares", exact=True).click()
            
            # Check radio button for Europe
            await page.get_by_role("radio", name="Europe").check()
            
            # Click environment button (Ares | SIT5 | Europe)
            button_text = f"Ares | {env_target} | {region}"
            await page.get_by_role("button", name=button_text).click()
            
            # Press NumLock on body
            await page.locator("body").press("NumLock")
            
            # Click Data Service link
            service_name = "Data Service" if url_type == "DATASERVICE" else url_type
            await page.get_by_role("link", name=service_name).click()
            
            # Click on iframe sub-frame-error-details
            await page.locator("iframe").content_frame().locator("#sub-frame-error-details").click()
            
            # -------- WAIT FOR IFRAME --------
            await page.wait_for_selector("iframe", timeout=15000, state="attached")
            print("✓ Iframe found in DOM")
            
            await page.wait_for_timeout(2000)
            
            iframe_element = await page.query_selector("iframe")
            
            if iframe_element:
                iframe_src = await iframe_element.get_attribute("src")
                print(f"\n{'='*60}")
                print(f"IFRAME SRC: {iframe_src}")
                print(f"{'='*60}\n")
                
                # -------- SAVE TO .env --------
                if not os.path.exists(ENV_FILE):
                    open(ENV_FILE, "w").close()

                # Save with key: {System}_{Region}_{Env_Target}_TARGET
                env_key = f"{system}_{region}_{env_target}_TARGET"
                set_key(ENV_FILE, env_key, iframe_src)
                
                print(f"\n{'='*60}")
                print(f"✓ Saved to {ENV_FILE}")
                print(f"  Variable: {env_key}")
                print(f"  Value: {iframe_src}")
                print(f"{'='*60}\n")
                
                await browser.close()
                return iframe_src
            
        except Exception as e:
            print(f"❌ Error in TARGET processing: {e}")
            await page.screenshot(path="debug_target_error.png")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------- MAIN --------------------
async def main():
    """Main execution flow"""
    # Load configuration
    config = load_config()
    
    print(f"\n{'='*60}")
    print(f"SOURCE_BASE_URL: {SOURCE_BASE_URL}")
    print(f"TARGET_BASE_URL: {TARGET_BASE_URL}")
    print(f"{'='*60}\n")
    
    # Process SOURCE environment
    print("\n🔵 Starting SOURCE environment processing...")
    await process_source(config)
    
    # Process TARGET environment
    print("\n🟢 Starting TARGET environment processing...")
    await process_target(config)
    
    print("\n✅ All environments processed successfully!")


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    asyncio.run(main())
