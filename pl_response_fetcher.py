import os
import sys
import json
import time
import threading
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth
from auth import get_password
from concurrent.futures import ThreadPoolExecutor, as_completed
from unicodedata import normalize
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------------------------- LOAD ENV -------------------------
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()
if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")
AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

# ------------------------- PATHS ----------------------------
INPUT_TESTCASE_FILE = "shared/input/TestInclusionCriteria.xlsx"
APITESTDATA_FILE = "shared/input/ApiTestData.json"
REPORT_BASE = "shared/reports"
REQUEST_URL_JSON = os.path.join(REPORT_BASE, "request_urls.json")
ERROR_LOG_EXCEL = os.path.join(REPORT_BASE, "RESPONSE_ERROR.xlsx")
REQUEST_URL_EXCEL = os.path.join(REPORT_BASE, "REQUEST_URL_REFERENCE.xlsx")

# ------------------------- SYSTEM NAME ----------------------
with open(APITESTDATA_FILE, "r") as f:
    api_data = json.load(f)
SYSTEM = api_data.get("System", "UNKNOWN")
reporting_date = api_data.get("TestData", {}).get("default", {}).get("reportingDate", "")

# ------------------------- HELPERS --------------------------
def clean(val):
    return normalize("NFKC", str(val)).replace("\u00A0", "").replace("\u200B", "").strip()

def build_url(template, param_dict):
    url = template
    for key, val in param_dict.items():
        url = url.replace(f"{{{key}}}", val)
    return url

def send_request(tag, url, env):
    try:
        headers = {"Accept": "application/json"}
        response = requests.get(url, auth=AUTH, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        return {"tag": tag, "env": env, "status": "success", "url": url, "error": None}
    except Exception as e:
        return {"tag": tag, "env": env, "status": "failed", "url": url, "error": str(e)}

# ------------------------- MAIN EXECUTION -------------------
def main():
    df = pd.read_excel(INPUT_TESTCASE_FILE)
    url_records = {}
    error_records = []
    excel_rows = []

    futures = []
    executor = ThreadPoolExecutor()

    for _, row in df.iterrows():
        tag = str(row.get("tag")).strip()
        method = str(row.get("method")).strip().upper()
        endpoint_template = clean(row.get("endpoint"))

        if method != "GET" or not endpoint_template:
            continue

        # Extract parameters
        param_cols = [col for col in row.index if col not in ["tag", "method", "endpoint"]]
        param_dict = {col: clean(row[col]) for col in param_cols if pd.notna(row[col]) and clean(row[col])}
        if "reportingDate" in endpoint_template:
            param_dict["reportingDate"] = reporting_date

        try:
            source_base = clean(row.get("SourceBaseURL", "https://apw-lite01:21100"))
            target_base = clean(row.get("TargetBaseURL", "https://aiw-riskrem02.uk.mizuho-sc.com:21100"))
            source_url = f"{source_base}{build_url(endpoint_template, param_dict)}"
            target_url = f"{target_base}{build_url(endpoint_template, param_dict)}"

            url_records[tag] = {
                "SOURCE_FinalURL": source_url,
                "TARGET_FinalURL": target_url
            }

            excel_rows.append({"TagName": tag, "SOURCE_FinalURL": source_url, "TARGET_FinalURL": target_url})

            # Launch requests in parallel
            futures.append(executor.submit(send_request, tag, source_url, "SOURCE"))
            futures.append(executor.submit(send_request, tag, target_url, "TARGET"))

        except Exception as ex:
            error_records.append({"TagName": tag, "Endpoint": endpoint_template, "Error": f"URL_BUILD_FAILED: {str(ex)}"})

    # Wait for responses
    for future in as_completed(futures):
        result = future.result()
        if result["status"] == "failed":
            error_records.append({
                "TagName": result["tag"],
                "Endpoint": result["url"],
                "Error": result["error"]
            })

    # Write outputs
    with open(REQUEST_URL_JSON, "w", encoding="utf-8") as f:
        json.dump(url_records, f, indent=2)

    pd.DataFrame(excel_rows).to_excel(REQUEST_URL_EXCEL, index=False)
    if error_records:
        pd.DataFrame(error_records).to_excel(ERROR_LOG_EXCEL, index=False)

    print(f"✅ Completed. URLs saved to: {REQUEST_URL_JSON}")
    print(f"📘 Reference Excel saved to: {REQUEST_URL_EXCEL}")
    print(f"❗ Errors logged to: {ERROR_LOG_EXCEL if error_records else 'No errors'}")

if __name__ == "__main__":
    main()
