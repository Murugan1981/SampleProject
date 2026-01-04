import asyncio
import json
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Load environment variables
load_dotenv()

API_TESTDATA_FILE = "shared/input/ApiTestData.json"
ENV_FILE = ".env"

# Utility to cleanly set env key-value pairs
def set_env_key_clean(key, value, env_path=".env"):
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    key_found = False
    with open(env_path, "w", encoding="utf-8") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}={value}\n")
                key_found = True
            else:
                f.write(line)
        if not key_found:
            f.write(f"{key}={value}\n")

# Load API test data
with open(API_TESTDATA_FILE, "r") as f:
    apidata = json.load(f)

system = apidata.get("System")
region = apidata.get("Region")
urltype = apidata.get("URLTYPE")
env_source = apidata.get("Env_Source")
env_target = apidata.get("Env_Target")

source_dashboard = os.getenv("SOURCE_DASHBOARD")
target_dashboard = os.getenv("TARGET_DASHBOARD")

# Main scraping function
async def extract_env_url(dashboard_url, system):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(dashboard_url)
        await page.wait_for_timeout(2000)

        # Select region
        await page.get_by_role("radio", name=region).check()
        await page.wait_for_timeout(1000)

        # Select system from dropdown
        await page.get_by_role("combobox").click()
        await page.get_by_role("option", name=system).click()
        await page.wait_for_timeout(1000)

        # Click DataService link
        await page.get_by_role("link", name=urltype).click()
        await page.wait_for_timeout(2000)

        # Click first GET button and "Try it out"
        await page.locator(".opblock-get").first.click()
        await page.get_by_role("button", name="Try it out").click()
        await page.wait_for_timeout(1000)

        # Fill inputs if needed (optional, based on UI behavior)

        # Execute the call
        await page.get_by_role("button", name="Execute").click()
        await page.wait_for_timeout(3000)

        # Extract the request URL
        try:
            request_url = await page.locator(".request-url").text_content()
            return request_url
        except:
            return None

# Extract base URL
async def process_source():
    url = await extract_env_url(source_dashboard, system)
    if url:
        if "?" in url:
            base = url.split("?")[0].rsplit("/", 1)[0]
        else:
            base = url.rsplit("/", 1)[0]
        set_env_key_clean(f"{system}_{region}_{env_source}", base)
        print(f"✅ Source env saved: {base}")

async def process_target():
    url = await extract_env_url(target_dashboard, system)
    if url:
        if "?" in url:
            base = url.split("?")[0].rsplit("/", 1)[0]
        else:
            base = url.rsplit("/", 1)[0]
        set_env_key_clean(f"{system}_{region}_{env_target}", base)
        print(f"✅ Target env saved: {base}")

async def main():
    await process_source()
    await process_target()

if __name__ == "__main__":
    asyncio.run(main())
