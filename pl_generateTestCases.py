import os
import json
import itertools
import pandas as pd
from dotenv import load_dotenv

# -------------------- CONFIG --------------------
load_dotenv()

INPUT_FILE = os.path.join("API", "reports", "endpoints.xlsx")
OUTPUT_FILE = os.path.join("API", "reports", "pl_testcases.xlsx")
API_TESTDATA_FILE = os.path.join("API", "ApiTestData.json")

SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

SOURCE_BASEURL = os.getenv("SOURCE_BASEURL")
TARGET_BASEURL = os.getenv("TARGET_BASEURL")

if not SOURCE_BASEURL or not TARGET_BASEURL:
    raise Exception("SOURCE_BASEURL or TARGET_BASEURL missing in .env")


# -------------------- HELPERS --------------------
def load_reporting_date():
    with open(API_TESTDATA_FILE, "r") as f:
        data = json.load(f)

    return data["TestData"]["default"]["reportingDate"]


def parse_values(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    return [v.strip() for v in str(cell).split(",") if v.strip()]


def replace_path_params(endpoint, param_map):
    for param, value in param_map.items():
        endpoint = endpoint.replace(f"{{{param}}}", value)
    return endpoint


# -------------------- CORE LOGIC --------------------
def generate_cases(df, baseurl, env_name, reporting_date):
    rows = []

    for _, r in df.iterrows():
        endpoint = r["endpoint"]

        # Identify parameters present in endpoint
        path_params = [
            p.strip("{}")
            for p in endpoint.split("/")
            if p.startswith("{") and p.endswith("}")
        ]

        param_values = {}

        for p in path_params:
            if p == "reportingDate":
                param_values[p] = [reporting_date]
            else:
                param_values[p] = parse_values(r.get(p, ""))

        # Skip endpoints with missing values
        if any(len(v) == 0 for v in param_values.values()):
            continue

        # Cartesian product
        keys = list(param_values.keys())
        for combo in itertools.product(*param_values.values()):
            param_map = dict(zip(keys, combo))
            resolved_endpoint = replace_path_params(endpoint, param_map)

            rows.append({
                "ENV": env_name,
                "endpoint_template": endpoint,
                "resolved_endpoint": resolved_endpoint,
                "final_url": f"{baseurl}{resolved_endpoint}"
            })

    return rows


def main():
    reporting_date = load_reporting_date()

    source_df = pd.read_excel(INPUT_FILE, sheet_name=SOURCE_SHEET)
    target_df = pd.read_excel(INPUT_FILE, sheet_name=TARGET_SHEET)

    source_rows = generate_cases(
        source_df, SOURCE_BASEURL, "SOURCE", reporting_date
    )

    target_rows = generate_cases(
        target_df, TARGET_BASEURL, "TARGET", reporting_date
    )

    out_df = pd.DataFrame(source_rows + target_rows)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print(f"Test cases generated → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
