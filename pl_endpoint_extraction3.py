import os
import json
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# -------------------- CONFIG --------------------
load_dotenv()

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


# -------------------- EXTRACT IFRAME SRC --------------------
async def extract_iframe_src(base_url, config, env_type):
    """
    Extract iframe src from the RDB UI
    
    Args:
        base_url: Base URL for navigation
        config: Configuration dictionary from JSON
        env_type: Either 'source' or 'target'
    """
    system = config['System']
    env = config['Env_Source'] if env_type == 'source' else config['Env_Target']
    region = config['Region']
    url_type = config['URLTYPE']
    
    print(f"\n{'='*60}")
    print(f"Processing {env_type.upper()} Environment: {env}")
    print(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION --------
        await page.goto(base_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        # Select Region
        await page.get_by_text(region, exact=True).click()
        
        # Select System
        await page.get_by_text("Select system...").click()
        await page.get_by_role("searchbox", name="Filter").fill(system.lower())
        await page.get_by_role("option", name=system).click()
        
        # Select Environment
        button_text = f"{system} | {env} | {region}"
        await page.get_by_role("button", name=button_text).click()
        
        # Navigate to Service based on URLTYPE
        service_name = "Data Service" if url_type == "DATASERVICE" else url_type
        await page.get_by_role("link", name=service_name).click()
        await page.get_by_role("link", name=service_name).press("NumLock")
        
        # -------- WAIT FOR IFRAME --------
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

                # Save with dynamic key: {System}_{Region}_{Env}_{EnvType}
                env_key = f"{system}_{region}_{env}_{env_type.upper()}"
                set_key(ENV_FILE, env_key, iframe_src)
                
                print(f"\n{'='*60}")
                print(f"✓ Saved to {ENV_FILE}")
                print(f"  Variable: {env_key}")
                print(f"  Value: {iframe_src}")
                print(f"{'='*60}\n")
                
                await browser.close()
                return iframe_src
            
        except Exception as e:
            print(f"Error waiting for iframe: {e}")
            # Take screenshot for debugging
            await page.screenshot(path=f"debug_iframe_error_{env_type}.png")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------- PROCESS SOURCE --------------------
async def process_source(base_url, config):
    """
    Process SOURCE (Production) environment
    
    Args:
        base_url: Base URL from SOURCE_BASE_URL in .env
        config: Configuration dictionary
    """
    system = config['System']
    env_source = config['Env_Source']
    region = config['Region']
    url_type = config['URLTYPE']
    
    print(f"\n{'='*60}")
    print(f"Processing SOURCE Environment: {env_source}")
    print(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION FOR SOURCE --------
        await page.goto(base_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        # Select Region
        await page.get_by_text(region, exact=True).click()
        
        # Select System
        await page.get_by_text("Select system...").click()
        await page.get_by_role("searchbox", name="Filter").fill(system.lower())
        await page.get_by_role("option", name=system).click()
        
        # Select Environment
        button_text = f"{system} | {env_source} | {region}"
        await page.get_by_role("button", name=button_text).click()
        
        # Navigate to Service based on URLTYPE
        service_name = "Data Service" if url_type == "DATASERVICE" else url_type
        await page.get_by_role("link", name=service_name).click()
        await page.get_by_role("link", name=service_name).press("NumLock")
        
        # -------- WAIT FOR IFRAME --------
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

                # Save with dynamic key: {System}_{Region}_{Env_Source}_SOURCE
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
            print(f"Error waiting for iframe: {e}")
            # Take screenshot for debugging
            await page.screenshot(path="debug_iframe_error_source.png")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------- PROCESS TARGET --------------------
async def process_target(base_url, config):
    """
    Process TARGET (Testing) environment
    
    Args:
        base_url: Base URL from TARGET_BASE_URL in .env
        config: Configuration dictionary
    """
    system = config['System']
    env_target = config['Env_Target']
    region = config['Region']
    url_type = config['URLTYPE']
    
    print(f"\n{'='*60}")
    print(f"Processing TARGET Environment: {env_target}")
    print(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # -------- UI NAVIGATION FOR TARGET --------
        await page.goto(base_url, wait_until="domcontentloaded")
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
        
        # -------- WAIT FOR IFRAME --------
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

                # Save with dynamic key: {System}_{Region}_{Env_Target}_TARGET
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
            print(f"Error waiting for iframe: {e}")
            # Take screenshot for debugging
            await page.screenshot(path="debug_iframe_error_target.png")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------- MAIN --------------------
async def main():
    """Main execution flow"""
    # Load configuration
    config = load_config()
    
    # Load base URLs from .env
    load_dotenv()
    source_base_url = os.getenv("SOURCE_BASE_URL")
    target_base_url = os.getenv("TARGET_BASE_URL")
    
    if not source_base_url:
        raise ValueError("SOURCE_BASE_URL not found in .env file")
    if not target_base_url:
        raise ValueError("TARGET_BASE_URL not found in .env file")
    
    print(f"\n{'='*60}")
    print(f"SOURCE_BASE_URL: {source_base_url}")
    print(f"TARGET_BASE_URL: {target_base_url}")
    print(f"{'='*60}\n")
    
    # Process SOURCE environment
    print("\n🔵 Starting SOURCE environment processing...")
    await process_source(source_base_url, config)
    
    # Process TARGET environment
    print("\n🟢 Starting TARGET environment processing...")
    await process_target(target_base_url, config)
    
    print("\n✅ All environments processed successfully!")


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    asyncio.run(main())
