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
REPORT_EXTRACT_RESPONSES = os.path.join(REPORT_BASE, "pl_extract_save_responses.xlsx")

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

# =========== BLOCK: Helpers ===========
def save_json_response(url, auth, out_path):
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
    param_values = [str(row.get(col, "")).strip() for col in param_cols]
    filtered_params = [v for v in param_values if v]
    return "_".join(filtered_params) if filtered_params else "no_params"

def get_param_columns(df):
    static_cols = {
        "TestCaseID", "TagName",
        "SourceBaseURL", "TargetBaseURL",
        "SourceRequestURL", "TargetRequestURL"
    }
    return [col for col in df.columns if col not in static_cols]

def expand_urls(raw_url):
    """
    FIX: If URL contains comma-separated values, expand them correctly.
    exurl/a,b  =>  [exurl/a, exurl/b]
    """
    if "," not in raw_url:
        return [raw_url]

    base, tail = raw_url.rsplit("/", 1)
    values = [v.strip() for v in tail.split(",") if v.strip()]
    return [f"{base}/{v}" for v in values]

# =========== BLOCK: Main ===========
def main():
    total_rows = len(df)
    param_cols = get_param_columns(df)
    processed = 0
    t0 = time.time()

    report_rows = []

    for idx, row in df.iterrows():
        if REQUEST_LIMIT and processed >= REQUEST_LIMIT:
            break

        tagname = str(row["TagName"]).strip() if "TagName" in row else "no_tag"
        param_str = extract_param_string(row, param_cols)

        src_err = ""
        tgt_err = ""

        # -------- SOURCE --------
        src_url_raw = str(row["SourceRequestURL"]).strip()
        if src_url_raw:
            for i, src_url in enumerate(expand_urls(src_url_raw), start=1):
                src_filename = f"{tagname}_{param_str}_{i}.json"
                src_path = os.path.join(SOURCE_JSON_FOLDER, src_filename)
                success, err = save_json_response(src_url, AUTH, src_path)
                if not success:
                    src_err += f"[{src_url}] {err} | "

        # -------- TARGET --------
        tgt_url_raw = str(row["TargetRequestURL"]).strip()
        if tgt_url_raw:
            for i, tgt_url in enumerate(expand_urls(tgt_url_raw), start=1):
                tgt_filename = f"{tagname}_{param_str}_{i}.json"
                tgt_path = os.path.join(TARGET_JSON_FOLDER, tgt_filename)
                success, err = save_json_response(tgt_url, AUTH, tgt_path)
                if not success:
                    tgt_err += f"[{tgt_url}] {err} | "

        error_text = ""
        if src_err or tgt_err:
            error_text = f"SOURCE: {src_err.strip()} TARGET: {tgt_err.strip()}"

        report_rows.append({
            "sourcerequestapi": src_url_raw,
            "targetrequestapi": tgt_url_raw,
            "error": error_text
        })

        processed += 1
        print(f"Processed {processed}/{total_rows}", end="\r", flush=True)

    t1 = time.time()
    print(f"\nTotal time taken: {t1 - t0:.2f}s. Processed {processed} testcases.")

    report_df = pd.DataFrame(report_rows)
    report_df.to_excel(REPORT_EXTRACT_RESPONSES, index=False)
    print(f"Report written to: {REPORT_EXTRACT_RESPONSES}")

if __name__ == "__main__":
    main()
