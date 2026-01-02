import os
import re
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION SECTION
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
    raise Exception("Missing SOURCE_BASEURL or TARGET_BASEURL in .env")

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
    """
    PURPOSE:
        Load reportingDate from ApiTestData.json (single fixed value).
    RETURNS:
        str
    """
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)
    return data["TestData"]["default"]["reportingDate"]


def parse_csv_values(cell):
    """
    PURPOSE:
        Convert cell containing comma-separated values into list.
    NOTE:
        Also supports newline-separated values (common in Excel wrapped cells).
    RETURNS:
        list[str]
    """
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []

    # Normalize common separators (commas + newlines)
    s = s.replace("\n", ",").replace("\r", ",")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts


def normalize_param_name(name: str) -> str:
    """
    PURPOSE:
        Make extracted placeholder names robust against hidden characters.
    RETURNS:
        cleaned string
    """
    if name is None:
        return ""
    # Strip whitespace and remove non-printable characters
    cleaned = "".join(ch for ch in str(name).strip() if ch.isprintable())
    return cleaned.strip()


def extract_path_params(endpoint: str) -> list[str]:
    """
    PURPOSE:
        Robustly extract placeholders like {bdmDataType} using regex.
    RETURNS:
        list of parameter names without braces
    """
    raw = re.findall(r"\{([^{}]+)\}", str(endpoint))
    return [normalize_param_name(x) for x in raw if normalize_param_name(x)]


def resolve_endpoint(endpoint_template: str, param_map: dict[str, str]) -> str:
    """
    PURPOSE:
        Replace placeholders in template with concrete values.
    RETURNS:
        resolved endpoint string
    """
    resolved = str(endpoint_template)

    # Replace using exact placeholder tokens {param}
    for k, v in param_map.items():
        resolved = resolved.replace("{" + k + "}", str(v))

    return resolved


def get_case_insensitive_value(row: pd.Series, column_name: str):
    """
    PURPOSE:
        Fetch value for a column ignoring case.
        Also tries a normalized compare (remove underscores/spaces) to handle minor naming variance.
    RETURNS:
        cell value or None
    """
    if column_name is None:
        return None

    want = normalize_param_name(column_name)
    want_l = want.lower()
    want_norm = re.sub(r"[\s_\-]", "", want_l)

    for c in row.index:
        c_str = str(c)
        c_l = c_str.lower()
        c_norm = re.sub(r"[\s_\-]", "", c_l)
        if c_l == want_l or c_norm == want_norm:
            return row.get(c)

    return None


# ============================================================
# MAIN PROCESS
# ============================================================

def main():
    """
    PURPOSE:
        Generate pl_testcases.xlsx by applying TestInclusionCriteria permutations
        and resolving endpoint placeholders into executable URLs for SOURCE and TARGET.
    """

    # ------------------------
    # Validate required inputs
    # ------------------------
    if not os.path.exists(ENDPOINTS_FILE):
        raise FileNotFoundError(ENDPOINTS_FILE)
    if not os.path.exists(INCLUSION_FILE):
        raise FileNotFoundError(INCLUSION_FILE)
    if not os.path.exists(API_TESTDATA_FILE):
        raise FileNotFoundError(API_TESTDATA_FILE)

    reporting_date = load_reporting_date()

    # ------------------------
    # Load endpoints.xlsx
    # ------------------------
    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)

    if not REQUIRED_META_COLS.issubset(set(src_df.columns)):
        raise Exception("endpoints.xlsx SOURCE sheet must contain: tag, method, endpoint")
    if not REQUIRED_META_COLS.issubset(set(tgt_df.columns)):
        raise Exception("endpoints.xlsx TARGET sheet must contain: tag, method, endpoint")

    # Only generate testcases for endpoints that exist in BOTH environments
    common_endpoints = pd.merge(
        src_df[["tag", "method", "endpoint"]],
        tgt_df[["tag", "method", "endpoint"]],
        on=["tag", "method", "endpoint"],
        how="inner"
    )

    # ------------------------
    # Load inclusion criteria
    # ------------------------
    incl_df = pd.read_excel(INCLUSION_FILE)

    if not REQUIRED_META_COLS.issubset(set(incl_df.columns)):
        raise Exception("TestInclusionCriteria.xlsx must contain: tag, method, endpoint")

    # Keep only endpoints allowed by inclusion AND present in both SOURCE+TARGET
    incl_df = pd.merge(
        incl_df,
        common_endpoints,
        on=["tag", "method", "endpoint"],
        how="inner"
    )

    # ------------------------
    # Generate test cases
    # ------------------------
    test_cases = []
    counters_by_tag = {}

    for _, incl_row in incl_df.iterrows():
        tag = str(incl_row["tag"]).strip()
        method = str(incl_row["method"]).strip().upper()
        endpoint_template = str(incl_row["endpoint"]).strip()

        # Baseline: GET only
        if method != "GET":
            continue

        # Robust placeholder extraction (fix for your issue)
        path_params = extract_path_params(endpoint_template)

        # Build param -> list(values)
        param_values = {}
        comments = []

        for p in path_params:
            p_clean = normalize_param_name(p)

            # reportingDate from ApiTestData.json
            if p_clean.lower() == "reportingdate":
                param_values[p_clean] = [reporting_date]
                continue

            # other params from inclusion criteria
            cell = get_case_insensitive_value(incl_row, p_clean)
            values = parse_csv_values(cell)

            if not values:
                # If a placeholder exists but we have no values in inclusion criteria, we skip
                comments.append(f"Missing inclusion values for '{p_clean}'")
                param_values = {}
                break

            param_values[p_clean] = values

        # If any required placeholder is not resolvable, skip this endpoint
        if not param_values:
            continue

        # Cartesian product for permutations (e.g., 1*2*40 = 80)
        keys = list(param_values.keys())
        for combo in itertools.product(*[param_values[k] for k in keys]):
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            # Safety: ensure no placeholders remain
            if re.search(r"\{[^{}]+\}", resolved_endpoint):
                # If placeholders remain, write a row with comment for debugging
                counters_by_tag[tag] = counters_by_tag.get(tag, 0) + 1
                tc_id = f"{tag}_{counters_by_tag[tag]:03d}"
                test_cases.append({
                    "TestCaseID": tc_id,
                    "TagName": tag,
                    "SourceBaseURL": SOURCE_BASEURL,
                    "TargetBaseURL": TARGET_BASEURL,
                    "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                    "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                    "Comments": "Unresolved placeholders remain in endpoint"
                })
                continue

            # Generate deterministic TestCaseID per tag
            counters_by_tag[tag] = counters_by_tag.get(tag, 0) + 1
            tc_id = f"{tag}_{counters_by_tag[tag]:03d}"

            test_cases.append({
                "TestCaseID": tc_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                "Comments": ""  # Baseline: manual debug column
            })

    # ------------------------
    # Write output
    # ------------------------
    out_df = pd.DataFrame(test_cases, columns=OUT_COLUMNS)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print(f"pl_testcases.xlsx generated → {OUTPUT_FILE}")
    print(f"Total test cases generated: {len(out_df)}")


if __name__ == "__main__":
    main()
