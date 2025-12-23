import os 
import pandas as pd
from datetime import datetime
import csv

# Inputs
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/csv_column_and_row_report.xlsx"

# Load input sheet
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

# Validate required columns
required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Missing required columns: {required_cols}")

# Output holders
column_rows = []   # for Sheet1
rowcount_rows = [] # for Sheet2

def detect_delimiter(filepath, sample_size=5):
    with open(filepath, 'r', encoding='utf-8') as f:
        sample = ''.join([next(f) for _ in range(sample_size)])
        sniffer = csv.Sniffer()
        try:
            return sniffer.sniff(sample).delimiter
        except:
            return ','  # default fallback

# Loop through input rows
for _, row in df_input.iterrows():
    base_file = str(row["BaseFileName"]).strip()
    full_file = str(row["FullFileName"]).strip()
    validate_flag = str(row["Validate?"]).strip().upper()
    dest_folder = str(row["DestinationFolder"]).strip()

    if validate_flag != "YES" or not os.path.exists(dest_folder):
        continue

    files_in_folder = os.listdir(dest_folder)
    matched_files = [f for f in files_in_folder if f.startswith(full_file)]

    matched_file = None
    comment = ""

    if matched_files:
        # Select most recent match
        matched_file = sorted(
            matched_files,
            key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)),
            reverse=True
        )[0]
        comment = ", ".join(matched_files)
    else:
        partial_matches = [f for f in files_in_folder if base_file in f and not f.startswith(full_file)]
        if partial_matches:
            matched_file = sorted(
                partial_matches,
                key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)),
                reverse=True
            )[0]
            comment = f"Partial match: {', '.join(partial_matches)}"

    if not matched_file:
        continue

    file_path = os.path.join(dest_folder, matched_file)

    try:
        # Detect delimiter
        delimiter = detect_delimiter(file_path)

        # Get column names
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader)

        for col in header:
            column_rows.append({
                "FileName": matched_file,
                "ColumnName": col
            })

        # Count actual rows (excluding header and comment)
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        last_line = lines[-1].strip()
        if last_line.startswith("#"):
            reported = int(last_line[1:])
            actual = len(lines) - 2  # minus header and comment
        else:
            reported = "NotFound"
            actual = len(lines) - 1  # minus header only

        match = "Match" if str(reported) == str(actual) else "Mismatch"

        rowcount_rows.append({
            "FileName": matched_file,
            "ReportedRowCount": reported,
            "ActualRowCount": actual,
            "MatchStatus": match,
            "Comment": comment
        })

    except Exception as e:
        rowcount_rows.append({
            "FileName": matched_file,
            "ReportedRowCount": "ERROR",
            "ActualRowCount": "ERROR",
            "MatchStatus": "Error",
            "Comment": str(e)
        })

# Export to Excel with two sheets
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    pd.DataFrame(column_rows).to_excel(writer, index=False, sheet_name="SensiFile_columns")
    pd.DataFrame(rowcount_rows).to_excel(writer, index=False, sheet_name="SensiFile_RowCount")

print(f"✅ Done. Output saved to: {output_file}")
