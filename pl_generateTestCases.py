import os
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# =====================================================
# LOAD ENVIRONMENT VARIABLES
# =====================================================
load_dotenv()

SOURCE_BASE_URL = os.getenv("SourceBaseURL")
TARGET_BASE_URL = os.getenv("TargetBaseURL")

if not SOURCE_BASE_URL or not TARGET_BASE_URL:
    raise Exception("SourceBaseURL / TargetBaseURL missing in .env")

# =====================================================
# LOAD reportingDate FROM ApiTestData.json
# =====================================================
API_TESTDATA_FILE = "ApiTestData.json"

with open(API_TESTDATA_FILE, "r") as f:
    api_data = json.load(f)

reporting_date = api_data["TestData"]["default"]["reportingDate"]

# =====================================================
# INPUT / OUTPUT FILES
# =====================================================
INPUT_EXCEL = "TestCaseInclusion.xlsx"
INPUT_SHEET = "Sheet1"
OUTPUT_EXCEL = "pl_testcases.xlsx"

# =====================================================
# READ INPUT EXCEL
# =====================================================
df = pd.read_excel(INPUT_EXCEL, sheet_name=INPUT_SHEET)

# Expected base columns
BASE_COLS = {"tag", "method", "endpoint"}

missing = BASE_COLS - set(df.columns)
if missing:
    raise Exception(f"Missing mandatory columns in TestCaseInclusion.xlsx: {missing}")

# Parameter columns = everything except base columns
PARAM_COLS = [c for c in df.columns if c not in BASE_COLS]

# =====================================================
# GENERATE TEST CASES (STRICT ROW BY ROW)
# =====================================================
testcases = []
tag_counter = {}

for _, row in df.iterrows():

    tag = str(row["tag"]).strip()
    method = str(row["method"]).strip()
    endpoint_template = str(row["endpoint"]).strip()

    # Prepare parameter values ONLY FROM THIS ROW
    param_values = {}

    for col in PARAM_COLS:
        cell = row[col]

        if pd.isna(cell):
            continue

        values = [v.strip() for v in str(cell).split(",") if v.strip()]
        if values:
            param_values[col] = values

    # Inject reportingDate from JSON (NOT Excel)
    if "{reportingDate}" in endpoint_template:
        param_values["reportingDate"] = [reporting_date]

    # If no params, still generate one testcase
    if not param_values:
        param_combinations = [()]
        param_keys = []
    else:
        param_keys = list(param_values.keys())
        param_combinations = itertools.product(*[param_values[k] for k in param_keys])

    # =================================================
    # COMPLETE ALL COMBINATIONS FOR THIS ROW
    # =================================================
    for combo in param_combinations:

        resolved_endpoint = endpoint_template

        for k, v in zip(param_keys, combo):
            resolved_endpoint = resolved_endpoint.replace(f"{{{k}}}", v)

        tag_counter[tag] = tag_counter.get(tag, 0) + 1
        test_case_id = f"{tag}_{tag_counter[tag]:03d}"

        testcases.append({
            "TestCaseID": test_case_id,
            "TagName": tag,
            "SourceBaseURL": SOURCE_BASE_URL,
            "TargetBaseURL": TARGET_BASE_URL,
            "SourceRequestURL": SOURCE_BASE_URL.rstrip("/") + resolved_endpoint,
            "TargetRequestURL": TARGET_BASE_URL.rstrip("/") + resolved_endpoint
        })

# =====================================================
# WRITE OUTPUT
# =====================================================
output_df = pd.DataFrame(testcases)
output_df.to_excel(OUTPUT_EXCEL, index=False)

print(f"Generated {len(output_df)} test cases")
print(f"Output file: {OUTPUT_EXCEL}")
