import os
import json
from dotenv import load_dotenv, set_key
from playwright.sync_api import sync_playwright

# Load existing .env
load_dotenv()
ENV_PATH = ".env"

# ========== Input Files ==========
APITESTDATA_FILE = "shared/input/ApiTestData.json"

# ========== Read Configurations ==========
with open(APITESTDATA_FILE, "r") as f:
    config = json.load(f)

system = config.get("System", "UNKNOWN").upper()
region = config.get("Region", "").capitalize()
urltype = config.get("URLTYPE", "")
env_source = config.get("Env_Source", "")
env_target = config.get("Env_Target", "")

source_dashboard = os.getenv("SOURCE_DASHBOARD")
target_dashboard = os.getenv("TARGET_DASHBOARD")

if not source_dashboard or not target_dashboard:
    raise Exception("SOURCE_DASHBOARD or TARGET_DASHBOARD not set in .env")

# ========== Helper Function to Extract BaseURL ==========
def extract_baseurl_from_request(request_url):
    # Example: https://apw-lite01:21100/jupiter/bdm/... --> https://apw-lite01:21100
    parts = request_url.split("/")
    return f"{parts[0]}//{parts[2]}"

# ========== Main Function ==========
def extract_env_details(dashboard_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(dashboard_url)
        page.wait_for_timeout(4000)

        # Click on dataservice module
        page.click("text=Dataservice")
        page.wait_for_timeout(2000)

        # Expand first GET method and click "Try it out"
        first_try_btn = page.locator(".opblock-get .try-out__btn").first
        first_try_btn.click()

        # Auto-fill parameters (if any exist)
        param_inputs = page.locator("input[type='text']")
        for i in range(param_inputs.count()):
            param_inputs.nth(i).fill("dummy")

        # Execute
        page.locator("button.execute.opblock-control__btn").click()
        page.wait_for_timeout(3000)

        # Extract request URL
        req_url_element = page.locator(".request-url").first
        request_url = req_url_element.inner_text()
        baseurl = extract_baseurl_from_request(request_url)
        dsurl = page.url  # Swagger UI full url

        browser.close()
        return baseurl, dsurl

# ========== Run for Source & Target ==========
source_baseurl, source_dsurl = extract_env_details(source_dashboard)
target_baseurl, target_dsurl = extract_env_details(target_dashboard)

# ========== Save to .env ==========
set_key(ENV_PATH, f"{system}_SOURCE_DS", source_dsurl)
set_key(ENV_PATH, f"{system}_TARGET_DS", target_dsurl)
set_key(ENV_PATH, f"{system}_SOURCE_BASEURL", source_baseurl)
set_key(ENV_PATH, f"{system}_TARGET_BASEURL", target_baseurl)

print(f"✅ Environment details saved to .env for system: {system}")
