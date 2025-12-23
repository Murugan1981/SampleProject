import os
import pandas as pd
import shutil
from datetime import datetime

# Input and output paths
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/sensitivity_file_report.xlsx"

# Load Excel input
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder", "LocalFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Missing columns in input: {required_cols}")

columns_report = []
rowcount_report = []

for _, row in df_input.iterrows():
    validate_flag = str(row["Validate?"]).strip().upper()
    full_file = str(row["FullFileName"]).strip()
    base_file = str(row["BaseFileName"]).strip()
    shared_folder = str(row["DestinationFolder"]).strip()
    local_folder = str(row["LocalFolder"]).strip()

    result = "MISSING"
    comment = ""
    matched_filename = None

    if validate_flag != "YES" or not os.path.exists(shared_folder):
        comment = "Folder missing or validation skipped"
    else:
        all_files = os.listdir(shared_folder)

        # Step 1: Look for exact match (file startswith full_file)
        exact_matches = [f for f in all_files if f.startswith(full_file)]

        if exact_matches:
            # Copy the most recent file
            most_recent = sorted(exact_matches, key=lambda x: os.path.getmtime(os.path.join(shared_folder, x)), reverse=True)[0]
            src_path = os.path.join(shared_folder, most_recent)
            dst_path = os.path.join(local_folder, most_recent)
            try:
                os.makedirs(local_folder, exist_ok=True)
                shutil.copy2(src_path, dst_path)
                result = "FOUND"
                matched_filename = most_recent
                comment = ", ".join(exact_matches)
            except Exception as e:
                result = "COPY_FAILED"
                comment = str(e)
        else:
            # Step 2: Try partial match on base file
            partial_matches = [f for f in all_files if base_file in f and f.startswith(base_file)]
            if partial_matches:
                result = "MISSING"
                comment = f"Partial match: {', '.join(partial_matches)}"
            else:
                result = "MISSING"
                comment = "No matching file found"

    # Sheet 1: Column names
    if result == "FOUND" and matched_filename:
        try:
            full_path = os.path.join(local_folder, matched_filename)
            delimiter = "|" if full_path.endswith(".csv") else ","
            df = pd.read_csv(full_path, delimiter=delimiter, dtype=str, engine='python', nrows=1)
            for col in df.columns:
                columns_report.append({
                    "FileName": matched_filename,
                    "ColumnName": col,
                    "SourceFile": full_path
                })
        except Exception as e:
            columns_report.append({
                "FileName": matched_filename or full_file,
                "ColumnName": "ERROR",
                "SourceFile": f"Read error: {str(e)}"
            })

    # Sheet 2: Row count
    row_count = 0
    try:
        if result == "FOUND" and matched_filename:
            df_data = pd.read_csv(full_path, delimiter=delimiter, dtype=str, engine='python')
            row_count = len(df_data)
    except Exception as e:
        comment += f" | Row read error: {str(e)}"

    rowcount_report.append({
        "FullFileName": full_file,
        "Result": result,
        "CopiedFile": matched_filename or "-",
        "Comment": comment,
        "RowCount": row_count
    })

# Write to Excel
with pd.ExcelWriter(output_file) as writer:
    pd.DataFrame(columns_report).to_excel(writer, sheet_name="SensiFile_columns", index=False)
    pd.DataFrame(rowcount_report).to_excel(writer, sheet_name="SensiFile_RowCount", index=False)

print(f"✅ Output generated: {output_file}")
