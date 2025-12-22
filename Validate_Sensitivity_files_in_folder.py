import os
import pandas as pd
from datetime import datetime

input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/file_validation_report.xlsx"

df_input = pd.read_excel(input_file, sheet_name=sheet_name)

required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Input file must contain columns: {required_cols}")

results = []

for _, row in df_input.iterrows():
    base_file = str(row["BaseFileName"]).strip()
    full_file = str(row["FullFileName"]).strip()
    validate_flag = str(row["Validate?"]).strip().upper()
    dest_folder = str(row["DestinationFolder"]).strip()

    if validate_flag != "YES":
        continue

    result = "Missing"
    comment = ""
    created_time = ""
    modified_time = ""

    if os.path.exists(dest_folder):
        files_in_folder = os.listdir(dest_folder)

        # ---------- EXACT MATCH ----------
        if full_file in files_in_folder:
            result = "Found"
            comment = "Exact file found"

            full_path = os.path.join(dest_folder, full_file)
            created_time = datetime.fromtimestamp(
                os.path.getctime(full_path)
            ).strftime("%Y-%m-%d %H:%M:%S")
            modified_time = datetime.fromtimestamp(
                os.path.getmtime(full_path)
            ).strftime("%Y-%m-%d %H:%M:%S")

        # ---------- SIMILAR MATCH (ONLY IF EXACT NOT FOUND) ----------
        else:
            similar_files = [
                f for f in files_in_folder
                if base_file in f and f != full_file
            ]

            if similar_files:
                result = "Similar_Found"
                comment = f"Found similar file(s): {', '.join(similar_files)}"

                similar_files.sort(
                    key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)),
                    reverse=True
                )
                latest_file = similar_files[0]
                latest_path = os.path.join(dest_folder, latest_file)

                created_time = datetime.fromtimestamp(
                    os.path.getctime(latest_path)
                ).strftime("%Y-%m-%d %H:%M:%S")
                modified_time = datetime.fromtimestamp(
                    os.path.getmtime(latest_path)
                ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                comment = "No exact or similar file found"
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

df_result = pd.DataFrame(results)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
df_result.to_excel(output_file, index=False)

print(f"Validation Completed. Report generated: {output_file}")
