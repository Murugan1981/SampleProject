import os
import json
import pandas as pd
from deepdiff import DeepDiff
from dotenv import load_dotenv

# -------- Load env vars --------
load_dotenv()
INPUT_XLSX = "shared/input/pl_responseFiles.xlsx"
APITESTDATA_FILE = "shared/input/ApiTestData.json"

# -------- Output File --------
OUTPUT_XLSX = "shared/reports/pl_comparison_result.xlsx"

# -------- Load System Name --------
with open(APITESTDATA_FILE, "r") as f:
    SYSTEM = json.load(f).get("System", "UNKNOWN")

RESPONSE_FOLDER = os.path.join("shared", "reports", SYSTEM)

# -------- Helper to Load JSON File --------
def load_json(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return None

# -------- Main Comparison --------
def compare_jsons(source_data, target_data):
    if source_data is None or target_data is None:
        return "NotMatch", "Missing or invalid JSON"
    diff = DeepDiff(source_data, target_data, ignore_order=True)
    if not diff:
        return "Match", ""
    return "NotMatch", str(diff)

def main():
    df = pd.read_excel(INPUT_XLSX)
    output_rows = []

    for _, row in df.iterrows():
        testcase_id = str(row.get("TestCaseID", "")).strip()
        tag = str(row.get("TagName", "")).strip()
        src_base = str(row.get("SourceBaseURL", "")).strip()
        tgt_base = str(row.get("TargetBaseURL", "")).strip()
        src_url = str(row.get("SourceRequestURL", "")).strip()
        tgt_url = str(row.get("TargetRequestURL", "")).strip()
        src_file = str(row.get("SourceResponse", "")).strip()
        tgt_file = str(row.get("TargetResponse", "")).strip()

        src_path = os.path.join(RESPONSE_FOLDER, src_file)
        tgt_path = os.path.join(RESPONSE_FOLDER, tgt_file)

        src_json = load_json(src_path)
        tgt_json = load_json(tgt_path)

        result, comment = compare_jsons(src_json, tgt_json)

        output_rows.append({
            "TestCaseID": testcase_id,
            "TagName": tag,
            "SourceBaseURL": src_base,
            "TargetBaseURL": tgt_base,
            "SourceRequestURL": src_url,
            "TargetRequestURL": tgt_url,
            "SourceResponse": src_file,
            "TargetResponse": tgt_file,
            "ComparisonResult": result,
            "Comments": comment if result == "NotMatch" else ""
        })

    pd.DataFrame(output_rows).to_excel(OUTPUT_XLSX, index=False)
    print(f"✅ Comparison complete. Output written to → {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
