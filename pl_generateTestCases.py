import os
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION SECTION
# ============================================================

# Load environment variables from .env file
load_dotenv()

# Input files
ENDPOINTS_FILE = os.path.join("API", "reports", "endpoints.xlsx")
INCLUSION_FILE = os.path.join("API", "shared", "input", "TestInclusionCriteria.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")

# Output file
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")

# Sheet names in endpoints.xlsx
SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

# Base URLs from .env
SOURCE_BASEURL = os.getenv("SOURCE_BASEURL")
TARGET_BASEURL = os.getenv("TARGET_BASEURL")

# Validate mandatory environment variables
if not SOURCE_BASEURL or not TARGET_BASEURL:
    raise Exception("Missing SOURCE_BASEURL or TARGET_BASEURL in .env")

# Final output column order (baseline contract)
OUT_COLUMNS = [
    "TestCaseID",
    "TagName",
    "SourceBaseURL",
    "TargetBaseURL",
    "SourceRequestURL",
    "TargetRequestURL",
    "Comments"
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_reporting_date():
    """
    Load reportingDate from ApiTestData.json.
    reportingDate is treated as a single-value parameter
    and participates in permutation as 1 element.
    """
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)
    return data["TestData"]["default"]["reportingDate"]


def parse_csv_values(cell):
    """
    Convert comma-separated values from Excel cell into list.
    Example: 'MHI, MHBK' -> ['MHI', 'MHBK']
    """
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    return [v.strip() for v in str(cell).split(",") if v.strip()]


def extract_path_params(endpoint):
    """
    Extract path parameters from endpoint template.
    Example:
    /jupiter/bdm/{bdmType}/{reportingDate}/{tradingEntity}
    -> ['bdmType', 'reportingDate', 'tradingEntity']
    """
    params = []
    for part in str(endpoint).split("/"):
        if part.startswith("{") and part.endswith("}"):
            params.append(part.strip("{}").strip())
    return params


def resolve_endpoint(endpoint_template, param_map):
    """
    Replace placeholders in endpoint template using actual values.
    """
    resolved = endpoint_template
    for key, value in param_map.items():
        resolved = resolved.replace("{" + key + "}", value)
    return resolved


def get_case_insensitive_value(row, column_name):
    """
    Fetch column value from pandas row ignoring case.
    Needed because Excel column casing may differ from path param.
    """
    column_lookup = {c.lower(): c for c in row.index}
    actual_col = column_lookup.get(column_name.lower())
    if not actual_col:
        return None
    return row.get(actual_col)


# ============================================================
# MAIN LOGIC
# ============================================================

def main():
    # ------------------------
    # Basic input validation
    # ------------------------
    if not os.path.exists(ENDPOINTS_FILE):
        raise FileNotFoundError(ENDPOINTS_FILE)

    if not os.path.exists(INCLUSION_FILE):
        raise FileNotFoundError(INCLUSION_FILE)

    if not os.path.exists(API_TESTDATA_FILE):
        raise FileNotFoundError(API_TESTDATA_FILE)

    # Load reporting date once
    reporting_date = load_reporting_date()

    # ------------------------
    # Load endpoints.xlsx
    # ------------------------
    source_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    target_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)

    # Mandatory metadata columns
    required_cols = {"tag", "method", "endpoint"}

    if not required_cols.issubset(source_df.columns):
        raise Exception("SOURCE sheet missing required columns")

    if not required_cols.issubset(target_df.columns):
        raise Exception("TARGET sheet missing required columns")

    # ------------------------
    # Identify endpoints present in BOTH SOURCE and TARGET
    # ------------------------
    common_endpoints = pd.merge(
        source_df[["tag", "method", "endpoint"]],
        target_df[["tag", "method", "endpoint"]],
        on=["tag", "method", "endpoint"],
        how="inner"
    )

    # ------------------------
    # Load Test Inclusion Criteria
    # ------------------------
    inclusion_df = pd.read_excel(INCLUSION_FILE)

    if not required_cols.issubset(inclusion_df.columns):
        raise Exception("TestInclusionCriteria.xlsx missing required columns")

    # Filter inclusion rules only to valid SOURCE/TARGET endpoints
    inclusion_df = pd.merge(
        inclusion_df,
        common_endpoints,
        on=["tag", "method", "endpoint"],
        how="inner"
    )

    # ------------------------
    # Generate test cases
    # ------------------------
    test_cases = []

    # Per-tag counter for TestCaseID sequencing
    tag_counters = {}

    for _, incl_row in inclusion_df.iterrows():
        tag = str(incl_row["tag"]).strip()
        method = str(incl_row["method"]).strip().upper()
        endpoint_template = str(incl_row["endpoint"]).strip()

        # Current baseline supports GET only
        if method != "GET":
            continue

        # Extract path parameters from endpoint
        path_params = extract_path_params(endpoint_template)

        # Map parameter -> list of values
        param_values = {}

        for param in path_params:
            # reportingDate comes from ApiTestData.json
            if param.lower() == "reportingdate":
                param_values[param] = [reporting_date]
                continue

            # Other params come from inclusion criteria sheet
            cell_value = get_case_insensitive_value(incl_row, param)
            values = parse_csv_values(cell_value)

            # If no values provided -> endpoint not runnable
            if not values:
                param_values = {}
                break

            param_values[param] = values

        # Skip endpoints without complete parameter data
        if not param_values:
            continue

        # ------------------------
        # Cartesian product of all parameter values
        # ------------------------
        param_keys = list(param_values.keys())

        for combination in itertools.product(*[param_values[k] for k in param_keys]):
            param_map = dict(zip(param_keys, combination))

            # Resolve endpoint with concrete values
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            # Increment per-tag test case counter
            tag_counters[tag] = tag_counters.get(tag, 0) + 1

            test_case_id = f"{tag}_{tag_counters[tag]:03d}"

            # Construct final URLs
            source_request_url = f"{SOURCE_BASEURL}{resolved_endpoint}"
            target_request_url = f"{TARGET_BASEURL}{resolved_endpoint}"

            # Append final test case row
            test_cases.append({
                "TestCaseID": test_case_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": source_request_url,
                "TargetRequestURL": target_request_url,
                "Comments": ""
            })

    # ------------------------
    # Write output Excel
    # ------------------------
    output_df = pd.DataFrame(test_cases, columns=OUT_COLUMNS)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    output_df.to_excel(OUTPUT_FILE, index=False)

    print(f"pl_testcases.xlsx generated successfully → {OUTPUT_FILE}")
    print(f"Total test cases generated: {len(output_df)}")


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
