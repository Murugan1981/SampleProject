import os
import shutil
import pandas as pd
from datetime import datetime

# Define file inputs
input_file = "shared/input/SensitivityInput_Filenames.xlsx"
sheet_name = "FilesInFolder"
output_file = "shared/reports/file_copy_report.xlsx"

# Read Input Excel
df_input = pd.read_excel(input_file, sheet_name=sheet_name)

# Ensure expected columns exist
required_cols = {"FullFileName", "BaseFileName", "DestinationFolder", "LocalFolder"}
if not required_cols.issubset(df_input.columns):
    raise ValueError(f"Input file must contain columns: {required_cols}")

# Process each row
results = []

for _, row in df_input.iterrows():
    full_file = str(row["FullFileName"]).strip()
    base_file = str(row["BaseFileName"]).strip()
    source_folder = str(row["DestinationFolder"]).strip()
    local_folder = str(row["LocalFolder"]).strip()

    result = "Missing"
    comment = ""
    created_time = ""
    modified_time = ""

    if os.path.exists(source_folder):
        files_in_folder = os.listdir(source_folder)

        # Exact match check
        if full_file in files_in_folder:
            result = "Found"
            full_path = os.path.join(source_folder, full_file)

            # Copy to local folder
            os.makedirs(local_folder, exist_ok=True)
            dest_path = os.path.join(local_folder, full_file)
            shutil.copy2(full_path, dest_path)

            created_time = datetime.fromtimestamp(os.path.getctime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
            modified_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
            comment = f"Exact file copied: {full_file}"

        else:
            # Similar match
            similar_files = [f for f in files_in_folder if base_file in f]

            if similar_files:
                comment = f"Similar files found: {', '.join(similar_files)}"

                for sim_file in similar_files:
                    src_path = os.path.join(source_folder, sim_file)
                    dest_path = os.path.join(local_folder, sim_file)
                    os.makedirs(local_folder, exist_ok=True)
                    shutil.copy2(src_path, dest_path)

                result = "PartialMatch-Copied"
                created_time = datetime.fromtimestamp(os.path.getctime(os.path.join(source_folder, similar_files[0]))).strftime("%Y-%m-%d %H:%M:%S")
                modified_time = datetime.fromtimestamp(os.path.getmtime(os.path.join(source_folder, similar_files[0]))).strftime("%Y-%m-%d %H:%M:%S")
            else:
                comment = "No similar files found"
    else:
        comment = f"Source folder not found: {source_folder}"

    results.append({
        "FullFileName": full_file,
        "BaseFileName": base_file,
        "DestinationFolder": source_folder,
        "LocalFolder": local_folder,
        "Result": result,
        "CreatedTime": created_time,
        "ModifiedTime": modified_time,
        "Comments": comment
    })

# Write result to Excel
df_result = pd.DataFrame(results)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
df_result.to_excel(output_file, index=False)

print(f"✅ Copy Report generated: {output_file}")
