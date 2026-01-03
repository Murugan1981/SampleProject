import os
import json
import pandas as pd
from deepdiff import DeepDiff
from dotenv import load_dotenv
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

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
    return "NotMatch", json.dumps(diff, indent=2)

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

        # Truncate and stringify JSON for preview
        src_preview = json.dumps(src_json)[:100] if result == "NotMatch" and src_json else ""
        tgt_preview = json.dumps(tgt_json)[:100] if result == "NotMatch" and tgt_json else ""

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
            "Comments": comment if result == "NotMatch" else "",
            "SourceSnapshot": src_preview,
            "TargetSnapshot": tgt_preview
        })

    df_out = pd.DataFrame(output_rows)
    df_out.to_excel(OUTPUT_XLSX, index=False)

    # Apply highlight
    wb = load_workbook(OUTPUT_XLSX)
    ws = wb.active

    yellow_fill = PatternFill(start_color="FFFF99", end_color="FFFF99", fill_type="solid")
    blue_fill = PatternFill(start_color="CCFFFF", end_color="CCFFFF", fill_type="solid")

    for row in ws.iter_rows(min_row=2):
        comparison_cell = row[8]  # ComparisonResult
        source_snapshot_cell = row[10]  # SourceSnapshot
        target_snapshot_cell = row[11]  # TargetSnapshot

        if comparison_cell.value == "NotMatch":
            source_snapshot_cell.fill = yellow_fill
            target_snapshot_cell.fill = blue_fill

    wb.save(OUTPUT_XLSX)
    print(f"✅ Comparison complete. Output written to → {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
