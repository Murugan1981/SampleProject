import os
import pandas as pd
from datetime import datetime

# -----------------------------
# Configuration
# -----------------------------
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/file_validation_report.xlsx"

# -----------------------------
# Read Input
# -----------------------------
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

required_cols = {"Validate?", "FullFileName", "BaseFileName", "DestinationFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Missing required columns: {required_cols}")

results = []

# -----------------------------
# Validation Logic
# -----------------------------
for _, row in df_input.iterrows():
    validate_flag = str(row["Validate?"]).strip().upper()
    if validate_flag != "YES":
        continue

    full_file = str(row["FullFileName"]).strip()
    base_file = str(row["BaseFileName"]).strip()
    dest_folder = str(row["DestinationFolder"]).strip()

    result = "Missing"
    comment = ""
    created_time = ""
    modified_time = ""

    # -----------------------------
    # Folder existence check
    # -----------------------------
    if not os.path.exists(dest_folder):
        result = "FolderNotFound"
        comment = f"Destination folder not found: {dest_folder}"

    else:
        files_in_folder = os.listdir(dest_folder)

        # -----------------------------
        # 1️⃣ Exact Match (Authoritative)
        # -----------------------------
        if full_file in files_in_folder:
            result = "Found"
            exact_path = os.path.join(dest_folder, full_file)

            created_time = datetime.fromtimestamp(
                os.path.getctime(exact_path)
            ).strftime("%Y-%m-%d %H:%M:%S")

            modified_time = datetime.fromtimestamp(
                os.path.getmtime(exact_path)
            ).strftime("%Y-%m-%d %H:%M:%S")

            # Optional diagnostic only
            similar_files = [
                f for f in files_in_folder
                if base_file in f and f != full_file
            ]
            if similar_files:
                comment = f"Exact file found. Additional related files exist: {', '.join(similar_files)}"
            else:
                comment = "Exact file found"

        # -----------------------------
        # 2️⃣ Similar Match (Fallback)
        # -----------------------------
        else:
            similar_files = [f for f in files_in_folder if base_file in f]

            if similar_files:
                result = "SimilarFound"
                comment = f"Exact file missing. Found similar file(s): {', '.join(similar_files)}"

                # Choose most recently modified similar file
                similar_files.sort(
                    key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)),
                    reverse=True
                )

                chosen_file = similar_files[0]
                chosen_path = os.path.join(dest_folder, chosen_file)

                created_time = datetime.fromtimestamp(
                    os.path.getctime(chosen_path)
                ).strftime("%Y-%m-%d %H:%M:%S")

                modified_time = datetime.fromtimestamp(
                    os.path.getmtime(chosen_path)
                ).strftime("%Y-%m-%d %H:%M:%S")

            else:
                result = "Missing"
                comment = "Exact file and similar files not found"

    # -----------------------------
    # Collect Result
    # -----------------------------
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

# -----------------------------
# Write Output
# -----------------------------
df_result = pd.DataFrame(results)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
df_result.to_excel(output_file, index=False)

print(f"Validation completed successfully. Report generated at: {output_file}")
