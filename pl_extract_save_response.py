import os
import sys
import json
import time
from auth import get_password

import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()
if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")
AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

INPUT_TESTCASE_FILE = "shared/reports/pl_testcases.xlsx"
APITESTDATA_FILE = "shared/input/ApiTestData.json"
REPORT_BASE = "shared/reports"
REPORT_EXTRACT_RESPONSES = os.path.join(REPORT_BASE, "pl_extract_save_responses.xlsx")  # <--- OUTPUT REPORT

# Manually set this for limiting requests (set to None for no limit)
REQUEST_LIMIT = 10

# =========== BLOCK: Load System Name ===========
with open(APITESTDATA_FILE, "r") as f:
    api_testdata = json.load(f)
SYSTEM = api_testdata.get("System", "UNKNOWN_SYSTEM")

SOURCE_JSON_FOLDER = os.path.join(REPORT_BASE, SYSTEM, "Source_json")
TARGET_JSON_FOLDER = os.path.join(REPORT_BASE, SYSTEM, "Target_json")
os.makedirs(SOURCE_JSON_FOLDER, exist_ok=True)
os.makedirs(TARGET_JSON_FOLDER, exist_ok=True)

# =========== BLOCK: Read Test Case Excel ===========
df = pd.read_excel(INPUT_TESTCASE_FILE)
if "SourceRequestURL" not in df.columns or "TargetRequestURL" not in df.columns:
    raise Exception("Missing required columns in test case file.")

# =========== BLOCK: Response Extraction ===========
def save_json_response(url, auth, out_path):
    """Fetch the API response and save JSON content to out_path"""
    try:
        resp = requests.get(url, auth=auth, timeout=30, verify=False)
        resp.raise_for_status()
        try:
            data = resp.json()
        except Exception:
            data = resp.text
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True, None
    except Exception as ex:
        return False, str(ex)

def extract_param_string(row, param_cols):
    """Extract parameter string for file naming from row, e.g., tradingEntity_reportingDate"""
    param_values = [str(row.get(col, "")).strip() for col in param_cols]
    filtered_params = [v for v in param_values if v]
    return "_".join(filtered_params) if filtered_params else "no_params"

def get_param_columns(df):
    static_cols = {"TestCaseID", "TagName", "SourceBaseURL", "TargetBaseURL", "SourceRequestURL", "TargetRequestURL"}
    return [col for col in df.columns if col not in static_cols]

# =========== BLOCK: Main Extraction Loop ===========
def main():
    total_rows = len(df)
    param_cols = get_param_columns(df)
    processed = 0
    t0 = time.time()

    # --------- Collect rows for reporting ---------
    report_rows = []

    for idx, row in df.iterrows():
        if REQUEST_LIMIT and processed >= REQUEST_LIMIT:
            break

        tagname = str(row["TagName"]).strip() if "TagName" in row else "no_tag"
        param_str = extract_param_string(row, param_cols)

        # --- Source API ---
        src_url = str(row["SourceRequestURL"]).strip()
        src_err = ""
        if src_url:
            src_filename = f"{tagname}_{param_str}.json"
            src_path = os.path.join(SOURCE_JSON_FOLDER, src_filename)
            success, err = save_json_response(src_url, AUTH, src_path)
            if not success:
                print(f"[SOURCE] Error fetching {src_url}: {err}")
                src_err = err

        # --- Target API ---
        tgt_url = str(row["TargetRequestURL"]).strip()
        tgt_err = ""
        if tgt_url:
            tgt_filename = f"{tagname}_{param_str}.json"
            tgt_path = os.path.join(TARGET_JSON_FOLDER, tgt_filename)
            success, err = save_json_response(tgt_url, AUTH, tgt_path)
            if not success:
                print(f"[TARGET] Error fetching {tgt_url}: {err}")
                tgt_err = err

        # Append to report (combine errors for easy debug)
        if src_err or tgt_err:
            error_text = f"SOURCE: {src_err}; TARGET: {tgt_err}".strip("; ")
        else:
            error_text = ""

        report_rows.append({
            "sourcerequestapi": src_url,
            "targetrequestapi": tgt_url,
            "error": error_text
        })

        processed += 1
        print(f"Processed {processed}/{total_rows} test cases...", end="\r", flush=True)

    t1 = time.time()
    print(f"\nTotal time taken: {t1-t0:.2f} seconds. Processed {processed} testcases.")

    # --------- Write report to Excel ---------
    report_df = pd.DataFrame(report_rows)
    report_df.to_excel(REPORT_EXTRACT_RESPONSES, index=False)
    print(f"Extract/Save responses report written to: {REPORT_EXTRACT_RESPONSES}")

if __name__ == "__main__":
    main()
