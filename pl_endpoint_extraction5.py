# pl_endpoint_extraction5.py

import os
import json
import asyncio
from dotenv import load_dotenv, set_key
from playwright.async_api import async_playwright

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

SOURCE_BASE_URL = os.getenv("SOURCE_BASE_URL")
TARGET_BASE_URL = os.getenv("TARGET_BASE_URL")

ENV_FILE = ".env"
TESTDATA_FILE = os.path.join("shared", "input", "ApiTestData.json")


# -------------------------------------------------
# Load configuration
# -------------------------------------------------
def load_config():
    """Load configuration from ApiTestData.json"""
    if not os.path.exists(TESTDATA_FILE):
        raise FileNotFoundError(f"Config file not found: {TESTDATA_FILE}")

    with open(TESTDATA_FILE, "r") as f:
        config = json.load(f)

    print(f"Loaded config from {TESTDATA_FILE}")
    print(f" System      : {config['System']}")
    print(f" Env_Target : {config['Env_Target']}")
    print(f" Env_Source : {config['Env_Source']}")
    print(f" Region     : {config['Region']}")
    print(f" URLTYPE    : {config['URLTYPE']}")

    return config


# -------------------------------------------------
# PROCESS SOURCE (PRD)
# -------------------------------------------------
async def process_source(config):
    system = config["System"]
    env_source = config["Env_Source"]
    region = config["Region"]
    url_type = config["URLTYPE"]

    print(f"Processing SOURCE Environment: {env_source}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto(f"{SOURCE_BASE_URL}/#/", wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)

            await page.get_by_role("button", name="Select").click()
            await page.get_by_role("searchbox", name="Filter").fill(system.lower())
            await page.get_by_text(system, exact=True).click()
            await page.get_by_role("radio", name=region).check()

            button_text = f"{system} | {env_source} | {region}"
            await page.get_by_role("button", name=button_text).click()

            service_name = "Data Service" if url_type == "DATASERVICE" else url_type
            await page.get_by_role("link", name=service_name).click()
            await page.get_by_role("link", name=service_name).press("NumLock")

            await page.wait_for_selector("iframe", timeout=15000, state="attached")
            print("Iframe found in DOM")
            await page.wait_for_timeout(2000)

            iframe_element = await page.query_selector("iframe")
            if iframe_element:
                iframe_src = await iframe_element.get_attribute("src")
                print(f"Source Swagger URL: {iframe_src}")

                if not os.path.exists(ENV_FILE):
                    open(ENV_FILE, "w").close()

                env_key = f"{system}_{region}_{env_source}"
                set_key(ENV_FILE, env_key, iframe_src)

                print(f"Saved to {ENV_FILE}")
                print(f" Variable: {env_key}")
                print(f" Value   : {iframe_src}")

                await browser.close()
                return iframe_src

        except Exception as e:
            print(f"Error in SOURCE processing: {e}")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------------------------------------
# PROCESS TARGET
# -------------------------------------------------
async def process_target(config):
    system = config["System"]
    env_target = config["Env_Target"]
    region = config["Region"]
    url_type = config["URLTYPE"]

    print(f"Processing TARGET Environment: {env_target}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto(f"{TARGET_BASE_URL}/#/", wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)

            await page.get_by_text("Select system...").click()
            await page.get_by_role("searchbox", name="Filter").fill(system.lower())
            await page.get_by_role("option", name=system).click()

            button_text = f"{system} | {env_target} | {region}"
            await page.get_by_role("button", name=button_text).click()

            service_name = "Data Service" if url_type == "DATASERVICE" else url_type
            await page.get_by_role("link", name=service_name).click()
            await page.get_by_role("link", name=service_name).press("NumLock")

            await page.wait_for_selector("iframe", timeout=15000, state="attached")
            print("Iframe found in DOM")
            await page.wait_for_timeout(2000)

            iframe_element = await page.query_selector("iframe")
            if iframe_element:
                iframe_src = await iframe_element.get_attribute("src")
                print(f"Target Swagger URL: {iframe_src}")

                if not os.path.exists(ENV_FILE):
                    open(ENV_FILE, "w").close()

                env_key = f"{system}_{region}_{env_target}"
                set_key(ENV_FILE, env_key, iframe_src)

                print(f"Saved to {ENV_FILE}")
                print(f" Variable: {env_key}")
                print(f" Value   : {iframe_src}")

                await browser.close()
                return iframe_src

        except Exception as e:
            print(f"Error in TARGET processing: {e}")
            await browser.close()
            raise

        await browser.close()
        return None


# -------------------------------------------------
# MAIN
# -------------------------------------------------
async def main():
    config = load_config()

    print(f"SOURCE_BASE_URL : {SOURCE_BASE_URL}")
    print(f"TARGET_BASE_URL : {TARGET_BASE_URL}")

    print("\nStarting SOURCE environment processing...")
    await process_source(config)

    print("\nStarting TARGET environment processing...")
    await process_target(config)

    print("\nAll environments processed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
