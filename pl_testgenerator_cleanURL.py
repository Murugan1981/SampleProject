import os
import json
import itertools
import pandas as pd
from dotenv import load_dotenv
from unicodedata import normalize

# -------------------- CONFIG --------------------
load_dotenv()

ENDPOINTS_FILE = os.path.join("API", "reports", "endpoints.xlsx")
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")

SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

SOURCE_BASEURL = os.getenv("SOURCE_BASEURL")
TARGET_BASEURL = os.getenv("TARGET_BASEURL")

if not SOURCE_BASEURL or not TARGET_BASEURL:
    raise Exception("SOURCE_BASEURL / TARGET_BASEURL missing in .env")


# -------------------- HELPERS --------------------
def load_reporting_date():
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)
    return data["TestData"]["default"]["reportingDate"]

def parse_values(value):
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [v.strip() for v in str(value).split(",") if v.strip()]

def extract_path_params(endpoint):
    return [
        p.strip("{}")
        for p in endpoint.split("/")
        if p.startswith("{") and p.endswith("}")
    ]

def resolve_endpoint(endpoint, param_map):
    for k, v in param_map.items():
        endpoint = endpoint.replace(f"{{{k}}}", v)
    return endpoint

def clean_url(url):
    return normalize("NFKC", str(url)).replace("\u00A0", "").replace("\u200B", "").strip()


# -------------------- CORE LOGIC --------------------
def main():
    reporting_date = load_reporting_date()

    source_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    target_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)

    merged = pd.merge(
        source_df,
        target_df,
        on=["endpoint", "tag", "method"],
        suffixes=("_SOURCE", "_TARGET")
    )

    test_rows = []
    test_counter = {}

    for _, row in merged.iterrows():
        endpoint_template = row["endpoint"]
        tag = row["tag"]
        method = row["method"]

        if method != "GET":
            continue

        path_params = extract_path_params(endpoint_template)
        param_values = {}

        for p in path_params:
            if p == "reportingDate":
                param_values[p] = [reporting_date]
            else:
                values = parse_values(row.get(p))
                if not values:
                    param_values = {}
                    break
                param_values[p] = values

        if not param_values:
            continue

        keys = list(param_values.keys())
        combinations = itertools.product(*param_values.values())

        for combo in combinations:
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            source_url = clean_url(f"{SOURCE_BASEURL}{resolved_endpoint}")
            target_url = clean_url(f"{TARGET_BASEURL}{resolved_endpoint}")

            tag_key = tag
            test_counter[tag_key] = test_counter.get(tag_key, 0) + 1
            test_case_id = f"{tag}_{test_counter[tag_key]:03d}"

            test_rows.append({
                "TestCaseID": test_case_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": source_url,
                "TargetRequestURL": target_url,
            })

    result_df = pd.DataFrame(test_rows)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    result_df.to_excel(OUTPUT_FILE, index=False)

    print(f"Baselined test cases generated → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
