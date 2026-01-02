import os
import re
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# ================= CONFIG =================
load_dotenv()

INCLUSION_FILE = os.path.join("API", "shared", "input", "TestInclusionCriteria.xlsx")
ENDPOINTS_FILE = os.path.join("API", "reports", "endpoints.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")

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


# ================= NORMALIZATION HELPERS =================

def norm_text(s: str) -> str:
    """
    Normalize for reliable matching across Excel sources:
    - convert to string
    - strip
    - collapse all whitespace (space/newline/tab) to single space
    - normalize slashes (// -> /)
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)         # collapse whitespace
    s = re.sub(r"/{2,}", "/", s)       # collapse multiple slashes
    return s


def norm_method(m: str) -> str:
    return norm_text(m).upper()


def load_reporting_date() -> str:
    with open(API_TESTDATA_FILE, "r") as f:
        return json.load(f)["TestData"]["default"]["reportingDate"]


def extract_path_params(endpoint: str) -> list[str]:
    # Regex placeholder extraction
    return [p.strip() for p in re.findall(r"\{([^{}]+)\}", str(endpoint))]


def parse_values(cell) -> list[str]:
    # Support comma + newline wrapped values
    if pd.isna(cell):
        return []
    text = str(cell).replace("\n", ",").replace("\r", ",").strip()
    return [v.strip() for v in text.split(",") if v.strip()]


def resolve_endpoint(template: str, param_map: dict[str, str]) -> str:
    resolved = str(template)
    for k, v in param_map.items():
        resolved = resolved.replace(f"{{{k}}}", str(v))
    return resolved


def get_value_case_insensitive(row: pd.Series, param: str):
    """
    Find param column in the SAME ROW ignoring case and separators.
    Example: bdmType vs bdmType vs bdm_type
    """
    want = re.sub(r"[\s_\-]", "", param.lower())
    for col in row.index:
        col_norm = re.sub(r"[\s_\-]", "", str(col).lower())
        if col_norm == want:
            return row[col]
    return None


# ================= MAIN =================

def main():
    reporting_date = load_reporting_date()

    # Load inclusion criteria
    incl_df = pd.read_excel(INCLUSION_FILE)
    if not REQUIRED_META_COLS.issubset(set(incl_df.columns)):
        raise Exception("TestInclusionCriteria.xlsx must contain tag, method, endpoint")

    # Load endpoints.xlsx for validation
    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name="SOURCE")
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name="TARGET")
    if not REQUIRED_META_COLS.issubset(set(src_df.columns)) or not REQUIRED_META_COLS.issubset(set(tgt_df.columns)):
        raise Exception("endpoints.xlsx SOURCE/TARGET must contain tag, method, endpoint")

    # Build normalized lookup set of endpoints existing in BOTH envs
    merged = pd.merge(
        src_df[list(REQUIRED_META_COLS)],
        tgt_df[list(REQUIRED_META_COLS)],
        on=list(REQUIRED_META_COLS),
        how="inner"
    )

    valid_endpoints = set()
    for _, r in merged.iterrows():
        key = (
            norm_text(r["tag"]),
            norm_method(r["method"]),
            norm_text(r["endpoint"])
        )
        valid_endpoints.add(key)

    testcases = []
    tag_counter = {}

    # Diagnostics counters
    skipped_not_in_endpoints = 0
    skipped_method = 0
    skipped_missing_param_values = 0
    generated_rows = 0

    # ===== STRICT ROW LOOP =====
    for row_idx, row in incl_df.iterrows():
        tag_raw = row.get("tag", "")
        method_raw = row.get("method", "")
        endpoint_raw = row.get("endpoint", "")

        tag = norm_text(tag_raw)
        method = norm_method(method_raw)
        endpoint = norm_text(endpoint_raw)

        # Gate 1: exists in both SOURCE & TARGET
        if (tag, method, endpoint) not in valid_endpoints:
            skipped_not_in_endpoints += 1
            continue

        # Gate 2: only GET for now
        if method != "GET":
            skipped_method += 1
            continue

        # Build param map FROM THIS ROW ONLY
        path_params = extract_path_params(endpoint)
        param_values = {}

        missing_param = None

        for p in path_params:
            if p.lower() == "reportingdate":
                param_values[p] = [reporting_date]
                continue

            cell = get_value_case_insensitive(row, p)
            values = parse_values(cell)

            if not values:
                missing_param = p
                param_values = None
                break

            param_values[p] = values

        if not param_values:
            skipped_missing_param_values += 1
            continue

        # Cartesian product per row
        keys = list(param_values.keys())
        combos = itertools.product(*[param_values[k] for k in keys])

        for combo in combos:
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint, param_map)

            # If still unresolved placeholders remain, skip this particular case
            if re.search(r"\{[^{}]+\}", resolved_endpoint):
                continue

            tag_counter[tag] = tag_counter.get(tag, 0) + 1
            tc_id = f"{tag}_{tag_counter[tag]:03d}"

            testcases.append({
                "TestCaseID": tc_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                "Comments": f"Row {row_idx + 2} (endpoint validated)"
            })

        generated_rows += 1

    # Write output
    df = pd.DataFrame(testcases, columns=OUT_COLUMNS)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    # Print diagnostics (critical for your case)
    print("==== pl_generateTestCase Diagnostics ====")
    print(f"Valid endpoints (SOURCE ∩ TARGET): {len(valid_endpoints)}")
    print(f"Inclusion rows total: {len(incl_df)}")
    print(f"Rows generated (at least one testcase): {generated_rows}")
    print(f"Total testcases generated: {len(df)}")
    print("--- skipped reasons ---")
    print(f"Skipped: not found in endpoints.xlsx (after normalization): {skipped_not_in_endpoints}")
    print(f"Skipped: method not GET: {skipped_method}")
    print(f"Skipped: missing param values in that row: {skipped_missing_param_values}")
    print(f"Output → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
