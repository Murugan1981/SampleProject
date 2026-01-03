import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth
from auth import get_password
from unicodedata import normalize
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------- CONFIG --------------------
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()
if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")
AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

INPUT_TESTCASE_FILE = "shared/reports/pl_testcases.xlsx"
API_TESTDATA_FILE = "shared/input/ApiTestData.json"

with open(API_TESTDATA_FILE, "r") as f:
    api_data = json.load(f)
SYSTEM = api_data.get("System", "UNKNOWN")
REPORT_BASE = os.path.join("shared", "reports", SYSTEM)
os.makedirs(REPORT_BASE, exist_ok=True)

# -------------------- HELPERS --------------------
def clean(val):
    return normalize("NFKC", str(val)).replace("\u00A0", "").replace("\u200B", "").strip()

def fetch_and_save(tag_id, url, env):
    try:
        response = requests.get(url, auth=AUTH, headers={"Accept": "application/json"}, timeout=30, verify=False)
        response.raise_for_status()
        out_path = os.path.join(REPORT_BASE, f"{tag_id}_{env}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        return True, None
    except Exception as e:
        return False, str(e)

# -------------------- MAIN --------------------
def main():
    df = pd.read_excel(INPUT_TESTCASE_FILE)
    error_logs = []

    for _, row in df.iterrows():
        tag_id = str(row.get("TestCaseID")).strip()
        tag = str(row.get("TagName")).strip()

        source_url = clean(row.get("SourceRequestURL", ""))
        target_url = clean(row.get("TargetRequestURL", ""))

        if source_url:
            success, error = fetch_and_save(tag_id, source_url, "Source")
            if not success:
                error_logs.append({"TestCaseID": tag_id, "TagName": tag, "Endpoint": source_url, "Error": error})

        if target_url:
            success, error = fetch_and_save(tag_id, target_url, "Target")
            if not success:
                error_logs.append({"TestCaseID": tag_id, "TagName": tag, "Endpoint": target_url, "Error": error})

    if error_logs:
        pd.DataFrame(error_logs).to_excel(os.path.join("shared", "reports", "RESPONSE_ERROR.xlsx"), index=False)
        print("❗ Errors encountered. Logged to RESPONSE_ERROR.xlsx")
    else:
        print("✅ All responses fetched successfully.")

if __name__ == "__main__":
    main()
