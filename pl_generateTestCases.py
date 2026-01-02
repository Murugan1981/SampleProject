import os
import sys
import json
import time
import subprocess
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth
from auth import get_password
import urllib3
from unicodedata import normalize

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------------------------- LOAD ENV -------------------------
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()
if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")
AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

# ------------------------- PATHS ----------------------------
INPUT_TESTCASE_FILE = "shared/reports/pl_testcases.xlsx"
APITESTDATA_FILE = "shared/input/ApiTestData.json"
REPORT_BASE = "shared/reports"
REPORT_EXTRACT_RESPONSES = os.path.join(REPORT_BASE, "pl_extract_save_responses.xlsx")

with open(APITESTDATA_FILE, "r") as f:
    SYSTEM = json.load(f).get("System", "UNKNOWN")

SOURCE_JSON_FOLDER = os.path.join(REPORT_BASE, SYSTEM, "Source_json")
TARGET_JSON_FOLDER = os.path.join(REPORT_BASE, SYSTEM, "Target_json")
os.makedirs(SOURCE_JSON_FOLDER, exist_ok=True)
os.makedirs(TARGET_JSON_FOLDER, exist_ok=True)

# -------------------- CLEAN URL -----------------------------
def clean_url(url):
    return normalize("NFKC", str(url)).replace("\u00A0", "").replace("\u200B", "").strip()

# -------------------- TRY REQUESTS --------------------------
def try_requests_first(url, out_file):
    try:
        headers = {"Accept": "application/json"}
        r = requests.get(url, auth=AUTH, headers=headers, timeout=30, verify=False)
        r.raise_for_status()
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(r.json(), f, indent=2)
        return "requests_success", None
    except Exception as ex:
        return "fallback_to_curl", str(ex)

# -------------------- TRY CURL ------------------------------
def try_curl(url, out_file):
    try:
        cmd = f'curl -s --ntlm --negotiate -u : "{url}"'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(result.stdout)
            return "curl_success", None
        else:
            return "curl_failed", result.stderr.strip()
    except Exception as e:
        return "curl_exception", str(e)

# -------------------- MAIN LOOP -----------------------------
def main():
    df = pd.read_excel(INPUT_TESTCASE_FILE)
    if "SourceRequestURL" not in df.columns or "TargetRequestURL" not in df.columns:
        raise Exception("Missing required columns in test case file.")

    report_rows = []
    for _, row in df.iterrows():
        tagname = str(row.get("TagName", "no_tag")).strip()

        source_url = clean_url(row["SourceRequestURL"])
        target_url = clean_url(row["TargetRequestURL"])

        param_str = "_".join([str(row.get(col, "")).strip() for col in df.columns if col not in {"TestCaseID", "TagName", "SourceRequestURL", "TargetRequestURL"}]) or "no_params"

        src_outfile = os.path.join(SOURCE_JSON_FOLDER, f"{tagname}_{param_str}.json")
        tgt_outfile = os.path.join(TARGET_JSON_FOLDER, f"{tagname}_{param_str}.json")

        src_method, src_error = try_requests_first(source_url, src_outfile)
        if src_method == "fallback_to_curl":
            src_method, src_error = try_curl(source_url, src_outfile)

        tgt_method, tgt_error = try_requests_first(target_url, tgt_outfile)
        if tgt_method == "fallback_to_curl":
            tgt_method, tgt_error = try_curl(target_url, tgt_outfile)

        result = "FETCHED"
        if src_method not in ["requests_success", "curl_success"] or tgt_method not in ["requests_success", "curl_success"]:
            result = f"SRC: {src_error or src_method}; TGT: {tgt_error or tgt_method}"

        report_rows.append({
            "TagName": tagname,
            "sourcerequestapi": source_url,
            "targetrequestapi": target_url,
            "result": result
        })

    df_out = pd.DataFrame(report_rows)
    df_out.to_excel(REPORT_EXTRACT_RESPONSES, index=False)
    print(f"✅ Extraction complete. Report saved to: {REPORT_EXTRACT_RESPONSES}")

if __name__ == "__main__":
    main()
