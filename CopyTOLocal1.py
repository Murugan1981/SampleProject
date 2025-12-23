import os
import shutil
import pandas as pd
from datetime import datetime

# ==============================
# INPUT / OUTPUT CONFIG
# ==============================
input_file = "shared/input/input_filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/files_copy_report.xlsx"

# ==============================
# READ INPUT EXCEL
# ==============================
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

required_cols = {"FullFileName", "DestinationFolder", "LocalFolder"}
missing_cols = required_cols - set(df_input.columns)
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

results = []

# ==============================
# PROCESS EACH ROW
# ==============================
for _, row in df_input.iterrows():

    full_file = str(row["FullFileName"]).strip()
    source_folder = str(row["DestinationFolder"]).strip()
    local_folder = str(row["LocalFolder"]).strip()

    result = "MISSING"
    comment = ""
    copied_file = ""
    created_time = ""
    modified_time = ""

    if not os.path.exists(source_folder):
        comment = f"Source folder not found: {source_folder}"
        results.append({
            "FullFileName": full_file,
            "DestinationFolder": source_folder,
            "LocalFolder": local_folder,
            "Result": result,
            "CopiedFile": copied_file,
            "CreatedTime": created_time,
            "ModifiedTime": modified_time,
            "Comments": comment
        })
        continue

    files_in_folder = os.listdir(source_folder)

    # ==============================
    # STEP 1: EXACT MATCH (IGNORE TIMESTAMP)
    # ==============================
    exact_matches = [
        f for f in files_in_folder
        if f.startswith(full_file)
    ]

    if exact_matches:
        exact_with_time = [
            (f, os.path.getmtime(os.path.join(source_folder, f)))
            for f in exact_matches
        ]
        exact_with_time.sort(key=lambda x: x[1], reverse=True)

        latest_file = exact_with_time[0][0]
        full_path = os.path.join(source_folder, latest_file)

        os.makedirs(local_folder, exist_ok=True)
        shutil.copy2(full_path, os.path.join(local_folder, latest_file))

        result = "FOUND"
        copied_file = latest_file

        created_time = datetime.fromtimestamp(
            os.path.getctime(full_path)
        ).strftime("%Y-%m-%d %H:%M:%S")

        modified_time = datetime.fromtimestamp(
            os.path.getmtime(full_path)
        ).strftime("%Y-%m-%d %H:%M:%S")

        comment = (
            f"Multiple matches found. "
            f"Copied latest file: {latest_file}. "
            f"All matches: {', '.join(exact_matches)}"
        )

    else:
        # ==============================
        # STEP 2: BASE NAME (PARTIAL MATCH)
        # ==============================
        base_matches = [
            f for f in files_in_folder
            if full_file.split("_")[0] in f
        ]

        if base_matches:
            base_with_time = [
                (f, os.path.getmtime(os.path.join(source_folder, f)))
                for f in base_matches
            ]
            base_with_time.sort(key=lambda x: x[1], reverse=True)

            latest_file = base_with_time[0][0]
            full_path = os.path.join(source_folder, latest_file)

            os.makedirs(local_folder, exist_ok=True)
            shutil.copy2(full_path, os.path.join(local_folder, latest_file))

            copied_file = latest_file

            created_time = datetime.fromtimestamp(
                os.path.getctime(full_path)
            ).strftime("%Y-%m-%d %H:%M:%S")

            modified_time = datetime.fromtimestamp(
                os.path.getmtime(full_path)
            ).strftime("%Y-%m-%d %H:%M:%S")

            comment = (
                f"No exact match. "
                f"Copied latest partial match: {latest_file}. "
                f"All partial matches: {', '.join(base_matches)}"
            )

        else:
            comment = "No matching files found"

    # ==============================
    # APPEND RESULT
    # ==============================
    results.append({
        "FullFileName": full_file,
        "DestinationFolder": source_folder,
        "LocalFolder": local_folder,
        "Result": result,
        "CopiedFile": copied_file,
        "CreatedTime": created_time,
        "ModifiedTime": modified_time,
        "Comments": comment
    })

# ==============================
# WRITE REPORT
# ==============================
df_result = pd.DataFrame(results)
df_result.to_excel(output_file, index=False)

print(f"Report generated: {output_file}")
