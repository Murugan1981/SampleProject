import os
import pandas as pd
from datetime import datetime

def get_all_files_metadata(folder_path):
    file_data = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            full_path = os.path.join(root, file)
            try:
                stat = os.stat(full_path)
                created = datetime.fromtimestamp(stat.st_ctime)
                modified = datetime.fromtimestamp(stat.st_mtime)

                file_data.append({
                    "FolderPath": root,
                    "FilenameWithExtn": file,
                    "DateCreated": created.strftime('%Y-%m-%d %H:%M:%S'),
                    "DateModified": modified.strftime('%Y-%m-%d %H:%M:%S')
                })
            except Exception as e:
                print(f"Skipping file due to error: {full_path} - {e}")

    return file_data

# === USAGE ===
input_folder = r"C:\Your\Folder\Path"   # 🔁 Replace with your actual folder path
output_excel = "files_list.xlsx"        # 📄 Output file

# Extract and convert to DataFrame
file_metadata = get_all_files_metadata(input_folder)
df = pd.DataFrame(file_metadata)

# Write to Excel
df.to_excel(output_excel, index=False)
print(f"✅ File list written to {output_excel} with {len(df)} rows.")
