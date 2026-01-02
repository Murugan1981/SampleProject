import os
import re
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

ENDPOINTS_FILE = os.path.join("API", "reports", "endpoints.xlsx")
INCLUSION_FILE = os.path.join("API", "shared", "input", "TestInclusionCriteria.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")

SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

SOURCE_BASEURL = os.getenv("SOURCE_BASEURL")
TARGET_BASEURL = os.getenv("TARGET_BASEURL")

if not SOURCE_BASEURL or not TARGET_BASEURL:
    raise Exception("SOURCE_BASEURL / TARGET_BASEURL missing in .env")

OUT_COLUMNS = [
    "TestCaseID",
    "TagName",
    "SourceBaseURL",
    "TargetBaseURL",
    "SourceRequestURL",
    "TargetRequestURL",
    "Comments"
]

REQUIRED_META_COLS = {"tag", "method", "endpoint"}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_reporting_date():
    """Load reportingDate from ApiTestData.json (single fixed value)."""
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)
    return data["TestData"]["default"]["reportingDate"]


def parse_csv_values(cell):
    """Parse comma / newline separated Excel values into list."""
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []
    s = s.replace("\n", ",").replace("\r", ",")
    return [v.strip() for v in s.split(",") if v.strip()]


def extract_path_params(endpoint):
    """Extract {param} placeholders using regex (robust)."""
    return [p.strip() for p in re.findall(r"\{([^{}]+)\}", str(endpoint))]


def resolve_endpoint(endpoint_template, param_map):
    """Replace placeholders with concrete values."""
    resolved = endpoint_template
    for k, v in param_map.items():
        resolved = resolved.replace("{" + k + "}", str(v))
    return resolved


def get_case_insensitive_value(row, column_name):
    """Get value from row ignoring case and minor formatting differences."""
    want = re.sub(r"[\s_\-]", "", column_name.lower())
    for col in row.index:
        col_norm = re.sub(r"[\s_\-]", "", str(col).lower())
        if col_norm == want:
            return row[col]
    return None


# ============================================================
# MAIN LOGIC
# ============================================================

def main():
    # --------------------------------------------------------
    # Validate inputs
    # --------------------------------------------------------
    for f in [ENDPOINTS_FILE, INCLUSION_FILE, API_TESTDATA_FILE]:
        if not os.path.exists(f):
            raise FileNotFoundError(f)

    reporting_date = load_reporting_date()

    # --------------------------------------------------------
    # Load endpoints.xlsx
    # --------------------------------------------------------
    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)

    if not REQUIRED_META_COLS.issubset(src_df.columns):
        raise Exception("SOURCE sheet missing required columns")

    if not REQUIRED_META_COLS.issubset(tgt_df.columns):
        raise Exception("TARGET sheet missing required columns")

    # Build lookup set of endpoints present in BOTH environments
    valid_endpoints = set(
        tuple(x) for x in
        pd.merge(
            src_df[["tag", "method", "endpoint"]],
            tgt_df[["tag", "method", "endpoint"]],
            on=["tag", "method", "endpoint"],
            how="inner"
        ).values
    )

    # --------------------------------------------------------
    # Load Test Inclusion Criteria
    # --------------------------------------------------------
    incl_df = pd.read_excel(INCLUSION_FILE)

    if not REQUIRED_META_COLS.issubset(incl_df.columns):
        raise Exception("TestInclusionCriteria.xlsx missing required columns")

    test_cases = []
    tag_counters = {}

    # ========================================================
    # STRICT ROW-BY-ROW PROCESSING (NO MERGES, NO LEAKAGE)
    # ========================================================
    for row_idx, row in incl_df.iterrows():

        tag = str(row["tag"]).strip()
        method = str(row["method"]).strip().upper()
        endpoint_template = str(row["endpoint"]).strip()

        # ---- Row-level validation ----
        if (tag, method, endpoint_template) not in valid_endpoints:
            continue

        if method != "GET":
            continue

        # ---- Extract placeholders ONLY from this row ----
        path_params = extract_path_params(endpoint_template)

        param_values = {}

        for p in path_params:
            if p.lower() == "reportingdate":
                param_values[p] = [reporting_date]
                continue

            cell_value = get_case_insensitive_value(row, p)
            values = parse_csv_values(cell_value)

            if not values:
                param_values = {}
                break

            param_values[p] = values

        if not param_values:
            continue

        # ====================================================
        # COMPLETE ALL COMBINATIONS FOR THIS ROW
        # ====================================================
        keys = list(param_values.keys())

        for combo in itertools.product(*[param_values[k] for k in keys]):
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            # Safety check
            if re.search(r"\{[^{}]+\}", resolved_endpoint):
                continue

            tag_counters[tag] = tag_counters.get(tag, 0) + 1
            tc_id = f"{tag}_{tag_counters[tag]:03d}"

            test_cases.append({
                "TestCaseID": tc_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                "Comments": f"Generated from inclusion row {row_idx + 2}"
            })

        # ---- ONLY AFTER ALL COMBOS ARE DONE → NEXT ROW ----

    # --------------------------------------------------------
    # Write output
    # --------------------------------------------------------
    out_df = pd.DataFrame(test_cases, columns=OUT_COLUMNS)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print(f"pl_testcases.xlsx generated → {OUTPUT_FILE}")
    print(f"Total testcases: {len(out_df)}")


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
