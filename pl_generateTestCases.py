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


# ================= HELPERS =================

def load_reporting_date():
    with open(API_TESTDATA_FILE, "r") as f:
        return json.load(f)["TestData"]["default"]["reportingDate"]


def extract_path_params(endpoint):
    return re.findall(r"\{([^{}]+)\}", str(endpoint))


def parse_values(cell):
    if pd.isna(cell):
        return []
    text = str(cell).replace("\n", ",").strip()
    return [v.strip() for v in text.split(",") if v.strip()]


def resolve_endpoint(template, param_map):
    for k, v in param_map.items():
        template = template.replace(f"{{{k}}}", str(v))
    return template


def get_value_case_insensitive(row, param):
    wanted = re.sub(r"[\s_\-]", "", param.lower())
    for col in row.index:
        col_norm = re.sub(r"[\s_\-]", "", str(col).lower())
        if col_norm == wanted:
            return row[col]
    return None


# ================= MAIN =================

def main():

    reporting_date = load_reporting_date()

    # Load inclusion criteria
    incl_df = pd.read_excel(INCLUSION_FILE)

    # Load endpoints for validation
    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name="SOURCE")
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name="TARGET")

    valid_endpoints = set(
        tuple(x) for x in
        pd.merge(
            src_df[list(REQUIRED_META_COLS)],
            tgt_df[list(REQUIRED_META_COLS)],
            on=list(REQUIRED_META_COLS),
            how="inner"
        ).values
    )

    testcases = []
    tag_counter = {}

    # ===== STRICT ROW LOOP =====
    for row_idx, row in incl_df.iterrows():

        tag = str(row["tag"]).strip()
        method = str(row["method"]).strip().upper()
        endpoint = str(row["endpoint"]).strip()

        # Validate endpoint exists
        if (tag, method, endpoint) not in valid_endpoints:
            continue

        if method != "GET":
            continue

        path_params = extract_path_params(endpoint)

        param_values = {}

        # ----- collect values FOR THIS ROW ONLY -----
        for p in path_params:

            if p.lower() == "reportingdate":
                param_values[p] = [reporting_date]
                continue

            cell = get_value_case_insensitive(row, p)
            values = parse_values(cell)

            # IMPORTANT: empty list means THIS ROW cannot generate cases
            if not values:
                param_values = None
                break

            param_values[p] = values

        # Skip only THIS ROW if invalid
        if not param_values:
            continue

        # ----- generate ALL combinations FOR THIS ROW -----
        keys = list(param_values.keys())
        combinations = itertools.product(*[param_values[k] for k in keys])

        for combo in combinations:
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint, param_map)

            tag_counter[tag] = tag_counter.get(tag, 0) + 1
            tc_id = f"{tag}_{tag_counter[tag]:03d}"

            testcases.append({
                "TestCaseID": tc_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                "Comments": f"Generated from inclusion row {row_idx + 2}"
            })

        # 🔑 ROW COMPLETES HERE — NEXT ROW STARTS

    df = pd.DataFrame(testcases, columns=OUT_COLUMNS)
    df.to_excel(OUTPUT_FILE, index=False)

    print(f"Generated {len(df)} testcases")
    print(f"Output → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
