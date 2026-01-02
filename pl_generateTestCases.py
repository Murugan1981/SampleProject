import os
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# -------------------- CONFIG --------------------
load_dotenv()

ENDPOINTS_FILE = os.path.join("API", "reports", "endpoints.xlsx")
INCLUSION_FILE = os.path.join("API", "shared", "input", "TestInclusionCriteria.xlsx")
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")

SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

SOURCE_BASEURL = os.getenv("SOURCE_BASEURL")
TARGET_BASEURL = os.getenv("TARGET_BASEURL")

if not SOURCE_BASEURL or not TARGET_BASEURL:
    raise Exception("Missing SOURCE_BASEURL or TARGET_BASEURL in .env")

# Output columns (match your expected baseline)
OUT_COLS = [
    "TestCaseID",
    "TagName",
    "SourceBaseURL",
    "TargetBaseURL",
    "SourceRequestURL",
    "TargetRequestURL",
    "Comments",
]


# -------------------- HELPERS --------------------
def load_reporting_date() -> str:
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)
    # Your existing structure
    return data["TestData"]["default"]["reportingDate"]


def parse_csv_values(cell) -> list[str]:
    """Parse 'A,B,C' into ['A','B','C']"""
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    return [v.strip() for v in str(cell).split(",") if v.strip()]


def extract_path_params(endpoint: str) -> list[str]:
    """Extract {param} names from path"""
    params = []
    for part in str(endpoint).split("/"):
        if part.startswith("{") and part.endswith("}"):
            params.append(part.strip("{}").strip())
    return params


def resolve_endpoint(endpoint_template: str, param_map: dict[str, str]) -> str:
    resolved = endpoint_template
    for k, v in param_map.items():
        resolved = resolved.replace("{" + k + "}", v)
    return resolved


def get_case_insensitive_value(row: pd.Series, colname: str):
    """Return row[colname] ignoring case; None if not found."""
    lower_map = {c.lower(): c for c in row.index}
    key = lower_map.get(colname.lower())
    if not key:
        return None
    return row.get(key)


# -------------------- CORE LOGIC --------------------
def main():
    # --- basic file checks ---
    if not os.path.exists(ENDPOINTS_FILE):
        raise FileNotFoundError(ENDPOINTS_FILE)
    if not os.path.exists(INCLUSION_FILE):
        raise FileNotFoundError(INCLUSION_FILE)
    if not os.path.exists(API_TESTDATA_FILE):
        raise FileNotFoundError(API_TESTDATA_FILE)

    reporting_date = load_reporting_date()

    # --- read endpoints.xlsx ---
    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)

    required_cols = {"tag", "method", "endpoint"}
    if not required_cols.issubset(set(src_df.columns)) or not required_cols.issubset(set(tgt_df.columns)):
        raise Exception("endpoints.xlsx must contain columns: tag, method, endpoint (in SOURCE and TARGET sheets)")

    # Endpoints that exist in BOTH environments (same tag/method/endpoint)
    both_df = pd.merge(
        src_df[["tag", "method", "endpoint"]],
        tgt_df[["tag", "method", "endpoint"]],
        on=["tag", "method", "endpoint"],
        how="inner",
    )

    # --- read inclusion criteria ---
    # Use first sheet by default (user’s screenshots indicate single sheet)
    incl_df = pd.read_excel(INCLUSION_FILE)

    if not required_cols.issubset(set(incl_df.columns)):
        raise Exception("TestInclusionCriteria.xlsx must contain columns: tag, method, endpoint")

    # Filter inclusion to only endpoints present in BOTH SOURCE and TARGET
    incl_df = pd.merge(
        incl_df,
        both_df,
        on=["tag", "method", "endpoint"],
        how="inner"
    )

    test_rows = []
    counters_by_tag = {}

    for _, incl_row in incl_df.iterrows():
        tag = str(incl_row["tag"]).strip()
        method = str(incl_row["method"]).strip().upper()
        endpoint_template = str(incl_row["endpoint"]).strip()

        # baseline: only GET (as per your current scope)
        if method != "GET":
            continue

        path_params = extract_path_params(endpoint_template)

        # Build param -> list(values) using inclusion criteria
        param_values: dict[str, list[str]] = {}

        for p in path_params:
            if p.lower() == "reportingdate":
                param_values[p] = [reporting_date]
                continue

            cell = get_case_insensitive_value(incl_row, p)
            values = parse_csv_values(cell)

            # If inclusion criteria does not provide values -> cannot generate testcases
            # (This is intentional: inclusion file is the driver)
            if not values:
                param_values = {}
                break

            param_values[p] = values

        if not param_values:
            # Skip silently: endpoint not runnable per inclusion criteria
            continue

        # Cartesian product of all param lists
        keys = list(param_values.keys())
        for combo in itertools.product(*[param_values[k] for k in keys]):
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            # per-tag running counter for TestCaseID
            counters_by_tag[tag] = counters_by_tag.get(tag, 0) + 1
            tc_id = f"{tag}_{counters_by_tag[tag]:03d}"

            source_url = f"{SOURCE_BASEURL}{resolved_endpoint}"
            target_url = f"{TARGET_BASEURL}{resolved_endpoint}"

            test_rows.append({
                "TestCaseID": tc_id,
                "TagName": tag,  # copy-paste from inclusion tag
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": source_url,
                "TargetRequestURL": target_url,
                "Comments": ""
            })

    out_df = pd.DataFrame(test_rows, columns=OUT_COLS)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print(f"pl_testcases.xlsx generated → {OUTPUT_FILE}")
    print(f"Total testcases: {len(out_df)}")


if __name__ == "__main__":
    main()
