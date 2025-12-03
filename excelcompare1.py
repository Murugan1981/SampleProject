import pandas as pd
from pathlib import Path

# ✅ STEP 1: Define base path relative to this script
base_path = Path(__file__).resolve().parent
input_folder = base_path / "shared" / "input" / "ExcelCompare"
output_file = input_folder / "Differences.xlsx"

# ✅ STEP 2: Read the Excel files
file1 = input_folder / "PRD.xlsx"
file2 = input_folder / "UAT.xlsx"

# Check file existence
if not file1.exists():
    raise FileNotFoundError(f"Missing file: {file1}")
if not file2.exists():
    raise FileNotFoundError(f"Missing file: {file2}")

# ✅ STEP 3: Load Excel files
df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)

# ✅ STEP 4: Clean column names
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# ✅ STEP 5: Validate column structure
if set(df1.columns) != set(df2.columns):
    raise ValueError("❌ Column mismatch between files")

# ✅ STEP 6: Align column order
df2 = df2[df1.columns]

# ✅ STEP 7: Sort both DataFrames by all columns
df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

# ✅ STEP 8: Compare content
if df1_sorted.equals(df2_sorted):
    print("✅ Files are identical (ignoring row order).")
else:
    print("❌ Files have differences.")
    
    # ✅ STEP 9: Export differences
    diff_df = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
    diff_df.to_excel(output_file, index=False)
    print(f"🔍 Differences saved to: {output_file}")
