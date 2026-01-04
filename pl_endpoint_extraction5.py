import os
import json
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# ----------------------- Load env -----------------------
load_dotenv()

SOURCE_BASE_URL = os.getenv("SOURCE_BASE_URL")
TARGET_BASE_URL = os.getenv("TARGET_BASE_URL")

ENV_FILE = ".env"
TESTDATA_FILE = os.path.join("shared", "input", "ApiTestData.json")


# ----------------------- Load config -----------------------
def load_config():
    if not os.path.exists(TESTDATA_FILE):
        raise FileNotFoundError(f"Config file not found: {TESTDATA_FILE}")
    with open(TESTDATA_FILE, "r") as f:
        config = json.load(f)
    return config


# ----------------------- Extract env URL -----------------------
async def extract_env_url(page, base_url, system, region, env, urltype, is_source):
    await page.goto(f"{base_url}/#/", wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    print(f"\nNavigating to {env} > {system} > {region} > {urltype}")

    try:
        # Select System Dropdown
        await page.get_by_role("button", name="Select").click()
        await page.get_by_role("searchbox", name="Filter").fill(system.lower())
        await page.get_by_text(system, exact=True).click()

        # Select Region
        await page.get_by_role("radio", name=region).check()

        # Click Env Button
        button_text = f"{system} | {env} | {region}"
        await page.get_by_role("button", name=button_text).click()

        # Click on DataService / URLType
        service_name = "Data Service" if urltype.upper() == "DATASERVICE" else urltype

        # 🧠 Wait for the link to appear, timeout after 10s
        await page.wait_for_selector(f'a[role="link"]:has-text("{service_name}")', timeout=10000)
        await page.get_by_role("link", name=service_name).click()

        # Wait for iframe and extract
        await page.wait_for_selector("iframe", timeout=15000)
        iframe_element = await page.query_selector("iframe")
        if iframe_element:
            iframe_src = await iframe_element.get_attribute("src")
            print(f"✅ Extracted iframe URL → {iframe_src}")

            # Save to .env (remove quotes manually)
            env_key = f"{system}_{region}_{env}"
            clean_url = iframe_src.replace("'", "").strip()
            set_key(ENV_FILE, env_key, clean_url)
            print(f"🔐 Saved in .env: {env_key} = {clean_url}")
            return iframe_src
        else:
            print("❌ iframe not found")
    except Exception as e:
        print(f"🔥 Error extracting env URL: {e}")
        return None


# ----------------------- Source + Target -----------------------
async def process_source(config):
    print("\n🌍 Processing SOURCE")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await extract_env_url(
            page,
            SOURCE_BASE_URL,
            config["System"],
            config["Region"],
            config["Env_Source"],
            config["URLTYPE"],
            is_source=True
        )
        await browser.close()


async def process_target(config):
    print("\n🌍 Processing TARGET")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await extract_env_url(
            page,
            TARGET_BASE_URL,
            config["System"],
            config["Region"],
            config["Env_Target"],
            config["URLTYPE"],
            is_source=False
        )
        await browser.close()


# ----------------------- MAIN -----------------------
async def main():
    config = load_config()
    print(f"\nLoaded config → System: {config['System']} | Region: {config['Region']} | Source: {config['Env_Source']} | Target: {config['Env_Target']}")

    await process_source(config)
    await process_target(config)

    print("\n✅ All environments processed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
