import os
import pandas as pd
from datetime import datetime

# Define file inputs
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/csv_metadata_report.xlsx"

# Read Input Excel
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

# Ensure expected columns exist
required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Input file must contain columns: {required_cols}")

# Output Containers
columns_data = []     # For Sheet1: column names
rowcount_data = []    # For Sheet2: row counts

# Iterate and validate
for _, row in df_input.iterrows():
    base_file = str(row["BaseFileName"]).strip()
    full_file = str(row["FullFileName"]).strip()
    validate_flag = str(row["Validate?"]).strip().upper()
    dest_folder = str(row["DestinationFolder"]).strip()

    if validate_flag != "YES":
        continue

    if not os.path.exists(dest_folder):
        continue

    files_in_folder = os.listdir(dest_folder)

    # Step 1: Find exact or similar match
    matched_file = None
    if full_file in files_in_folder:
        matched_file = full_file
    else:
        similar_files = [f for f in files_in_folder if base_file in f]
        if similar_files:
            matched_file = sorted(
                similar_files,
                key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)),
                reverse=True
            )[0]

    if not matched_file:
        continue

    file_path = os.path.join(dest_folder, matched_file)

    try:
        # ✅ Read column names (only header)
        df = pd.read_csv(file_path, nrows=0)
        for col in df.columns:
            columns_data.append({
                "FileName": matched_file,
                "ColumnName": col
            })

        # ✅ Read last line for reported count (e.g., "#1234")
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        last_line = lines[-1].strip()
        if last_line.startswith("#"):
            reported_count = int(last_line[1:])
            actual_count = len(lines) - 2  # exclude header + last line
        else:
            reported_count = "NotFound"
            actual_count = len(lines) - 1  # just exclude header

        match_status = "Match" if str(reported_count) == str(actual_count) else "Mismatch"

        rowcount_data.append({
            "FileName": matched_file,
            "ReportedRowCount": reported_count,
            "ActualRowCount": actual_count,
            "MatchStatus": match_status
        })

    except Exception as e:
        rowcount_data.append({
            "FileName": matched_file,
            "ReportedRowCount": "ERROR",
            "ActualRowCount": "ERROR",
            "MatchStatus": str(e)
        })

# ✅ Save to Excel
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    pd.DataFrame(columns_data).to_excel(writer, index=False, sheet_name="Sheet1_Columns")
    pd.DataFrame(rowcount_data).to_excel(writer, index=False, sheet_name="Sheet2_RowCounts")

print(f"✅ CSV metadata report generated at: {output_file}")
