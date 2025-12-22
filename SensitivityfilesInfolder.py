import os
import pandas as pd
from datetime import datetime

# Define input/output paths
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/file_validation_report.xlsx"

# Load input Excel
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

# Ensure all required columns exist
required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Missing required columns. Found: {df_input.columns}")

results = []

# Process each row
for _, row in df_input.iterrows():
    base_file = str(row["BaseFileName"]).strip()
    full_file = str(row["FullFileName"]).strip()
    dest_folder = str(row["DestinationFolder"]).strip()
    validate = str(row["Validate?"]).strip().upper()

    if validate != "YES":
        continue

    result = "MISSING"
    comment = ""
    created_time = ""
    modified_time = ""

    if os.path.exists(dest_folder):
        all_files = os.listdir(dest_folder)

        # Check for exact match (ignoring timestamp)
        exact_matches = [f for f in all_files if f.startswith(full_file)]
        similar_matches = [f for f in all_files if base_file in f]

        if exact_matches:
            result = "FOUND"
            matched_names = ", ".join(exact_matches)
            comment = f"Exact match found: {matched_names}"

            # Use first matched file for timestamps
            full_path = os.path.join(dest_folder, exact_matches[0])
            created_time = datetime.fromtimestamp(os.path.getctime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
            modified_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
        elif similar_matches:
            result = "MISSING"
            matched_names = ", ".join(similar_matches)
            comment = f"Similar file(s) found: {matched_names}"

            # Use latest modified file for timestamp
            similar_matches.sort(key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)), reverse=True)
            latest_file_path = os.path.join(dest_folder, similar_matches[0])
            created_time = datetime.fromtimestamp(os.path.getctime(latest_file_path)).strftime("%Y-%m-%d %H:%M:%S")
            modified_time = datetime.fromtimestamp(os.path.getmtime(latest_file_path)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            comment = "No matching or similar file found"
    else:
        comment = f"Destination folder not found: {dest_folder}"

    results.append({
        "Trade_Position": row.get("Trade_Position", ""),
        "Sensitivity_Type": row.get("Sensitivity_Type", ""),
        "FullFileName": full_file,
        "BaseFileName": base_file,
        "DestinationFolder": dest_folder,
        "Result": result,
        "CreatedTime": created_time,
        "ModifiedTime": modified_time,
        "Comments": comment
    })

# Output result to Excel
df_result = pd.DataFrame(results)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
df_result.to_excel(output_file, index=False)

print(f"✅ File validation completed. Report saved to: {output_file}")
