import pandas as pd
from pathlib import Path

# ✅ Setup paths
base_path = Path(__file__).resolve().parent.parent
input_folder = base_path / "shared" / "input" / "ExcelCompare"
output_file = input_folder / "RowLevelDifferences.xlsx"

file1 = input_folder / "PRD.xlsm"
file2 = input_folder / "UAT.xlsm"

# ✅ Check file existence
if not file1.exists():
    raise FileNotFoundError(f"Missing file: {file1}")
if not file2.exists():
    raise FileNotFoundError(f"Missing file: {file2}")

# ✅ Load .xlsm files
df_prd = pd.read_excel(file1, engine="openpyxl")
df_uat = pd.read_excel(file2, engine="openpyxl")

# ✅ Strip column whitespace
df_prd.columns = df_prd.columns.str.strip()
df_uat.columns = df_uat.columns.str.strip()

# ✅ Check columns match
if set(df_prd.columns) != set(df_uat.columns):
    raise ValueError("❌ Column mismatch between PRD and UAT files")

# ✅ Align column order
df_uat = df_uat[df_prd.columns]

# ✅ Sort columns to normalize (column order doesn't affect comparison)
df_prd_sorted = df_prd[df_prd.columns.sort_values()]
df_uat_sorted = df_uat[df_uat.columns.sort_values()]

# ✅ Add SOURCE TAG for diff tracking
df_prd_sorted["DIFFERENCE_TYPE"] = "ONLY_IN_PRD"
df_uat_sorted["DIFFERENCE_TYPE"] = "ONLY_IN_UAT"

# ✅ Combine and drop duplicate rows (those that are equal)
combined_df = pd.concat([df_prd_sorted, df_uat_sorted], ignore_index=True)
difference_df = combined_df.drop_duplicates(subset=df_prd.columns.tolist(), keep=False)

# ✅ Save result
difference_df.to_excel(output_file, index=False)
print(f"✅ Row-level differences saved to: {output_file}")
