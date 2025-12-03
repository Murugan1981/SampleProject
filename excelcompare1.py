import pandas as pd
from pathlib import Path

# ✅ Step 1: Resolve correct project root (go 2 levels up from this script)
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent  # Go up from /api/

# ✅ Step 2: Set input and output paths
input_folder = project_root / "shared" / "input" / "ExcelCompare"
output_file = input_folder / "Differences.xlsx"

file1 = input_folder / "PRD.xlsm"  # .xlsm macro file
file2 = input_folder / "UAT.xlsm"  # .xlsm macro file

# ✅ Step 3: Validate file existence
if not file1.exists():
    raise FileNotFoundError(f"Missing file: {file1}")
if not file2.exists():
    raise FileNotFoundError(f"Missing file: {file2}")

# ✅ Step 4: Read .xlsm files using openpyxl engine
df1 = pd.read_excel(file1, engine='openpyxl')
df2 = pd.read_excel(file2, engine='openpyxl')

# ✅ Step 5: Strip columns
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# ✅ Step 6: Column validation and alignment
if set(df1.columns) != set(df2.columns):
    raise ValueError("❌ Column mismatch between files")
df2 = df2[df1.columns]

# ✅ Step 7: Sort and compare
df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

if df1_sorted.equals(df2_sorted):
    print("✅ Files are identical (ignoring row order).")
else:
    print("❌ Files have differences.")
    diff_df = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
    diff_df.to_excel(output_file, index=False)
    print(f"🔍 Differences saved to: {output_file}")
