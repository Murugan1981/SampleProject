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

# Load SYSTEM from ApiTestData.json
with open(API_TESTDATA_FILE, "r") as f:
    api_data = json.load(f)
SYSTEM = api_data.get("System", "UNKNOWN")
REPORT_BASE = os.path.join("shared", "reports", SYSTEM)
os.makedirs(REPORT_BASE, exist_ok=True)

# Output Excel File
OUTPUT_XLSX = os.path.join("shared", "reports", "pl_responseComparison.xlsx")
ERROR_LOG_XLSX = os.path.join("shared", "reports", "RESPONSE_ERROR.xlsx")

# -------------------- HELPERS --------------------
def clean(val):
    return normalize("NFKC", str(val)).replace("\u00A0", "").replace("\u200B", "").strip()

def fetch_and_save(tag_id, url, env):
    """Fetch API and save response as JSON"""
    try:
        response = requests.get(url, auth=AUTH, headers={"Accept": "application/json"}, timeout=30, verify=False)
        response.raise_for_status()
        file_name = f"{tag_id}_{env}.json"
        out_path = os.path.join(REPORT_BASE, file_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        return True, None, file_name
    except Exception as e:
        return False, str(e), None

# -------------------- MAIN --------------------
def main():
    df = pd.read_excel(INPUT_TESTCASE_FILE)
    output_rows = []
    error_logs = []

    for _, row in df.iterrows():
        tag_id = str(row.get("TestCaseID")).strip()
        tag = str(row.get("TagName")).strip()
        src_base = clean(row.get("SourceBaseURL", ""))
        tgt_base = clean(row.get("TargetBaseURL", ""))
        src_url = clean(row.get("SourceRequestURL", ""))
        tgt_url = clean(row.get("TargetRequestURL", ""))

        row_result = {
            "TestCaseID": tag_id,
            "TagName": tag,
            "SourceBaseURL": src_base,
            "TargetBaseURL": tgt_base,
            "SourceRequestURL": src_url,
            "TargetRequestURL": tgt_url,
            "Response": "",
            "SourceResponse": "",
            "TargetResponse": "",
            "ComparisonResult": "",
            "Comments": ""
        }

        # ----- Source -----
        src_success, src_err, src_file = fetch_and_save(tag_id, src_url, "Source") if src_url else (False, "Missing URL", None)
        if src_success:
            row_result["SourceResponse"] = src_file
        else:
            error_logs.append({"TestCaseID": tag_id, "TagName": tag, "Endpoint": src_url, "Error": src_err})

        # ----- Target -----
        tgt_success, tgt_err, tgt_file = fetch_and_save(tag_id, tgt_url, "Target") if tgt_url else (False, "Missing URL", None)
        if tgt_success:
            row_result["TargetResponse"] = tgt_file
        else:
            error_logs.append({"TestCaseID": tag_id, "TagName": tag, "Endpoint": tgt_url, "Error": tgt_err})

        # ----- Summary Status -----
        if src_success and tgt_success:
            row_result["Response"] = "FETCHED"
        else:
            row_result["Response"] = f"SOURCE ERROR: {src_err}" if not src_success else f"TARGET ERROR: {tgt_err}"

        output_rows.append(row_result)

    # ----- Write Response Summary -----
    pd.DataFrame(output_rows).to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ Response summary saved → {OUTPUT_XLSX}")

    # ----- Write Error Logs -----
    if error_logs:
        pd.DataFrame(error_logs).to_excel(ERROR_LOG_XLSX, index=False)
        print(f"❌ Errors logged to → {ERROR_LOG_XLSX}")
    else:
        print("✅ No API errors encountered.")

if __name__ == "__main__":
    main()
