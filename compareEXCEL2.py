import pandas as pd
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# === STEP 1: Setup paths ===
base_path = Path(__file__).resolve().parent.parent
input_folder = base_path / "shared" / "input" / "ExcelCompare"
output_file = input_folder / "PRD_UAT_PositionID_Comparison.xlsx"

file_prd = input_folder / "PRD.xlsm"
file_uat = input_folder / "UAT.xlsm"

# === STEP 2: Load data ===
df_prd = pd.read_excel(file_prd, engine="openpyxl")
df_uat = pd.read_excel(file_uat, engine="openpyxl")

df_prd.columns = df_prd.columns.str.strip()
df_uat.columns = df_uat.columns.str.strip()

# === STEP 3: Check for PositionID key ===
if "PositionID" not in df_prd.columns or "PositionID" not in df_uat.columns:
    raise KeyError("❌ 'PositionID' column is missing in one of the files.")

# === STEP 4: Set PositionID as index and align both ===
df_prd = df_prd.set_index("PositionID").sort_index()
df_uat = df_uat.set_index("PositionID").sort_index()

# Align both DataFrames on PositionID (inner join)
common_ids = df_prd.index.intersection(df_uat.index)
df_prd_common = df_prd.loc[common_ids]
df_uat_common = df_uat.loc[common_ids]

# === STEP 5: Create Excel with highlighting ===
wb = Workbook()
ws_prd = wb.active
ws_prd.title = "PRD"
ws_uat = wb.create_sheet("UAT")

# Reset index for writing to Excel
df_prd_common_reset = df_prd_common.reset_index()
df_uat_common_reset = df_uat_common.reset_index()

# Write PRD sheet
for row in dataframe_to_rows(df_prd_common_reset, index=False, header=True):
    ws_prd.append(row)

# Write UAT sheet
for row in dataframe_to_rows(df_uat_common_reset, index=False, header=True):
    ws_uat.append(row)

# === STEP 6: Highlight mismatches ===
highlight_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

num_rows = len(df_prd_common_reset)
num_cols = len(df_prd_common_reset.columns)

# Compare each cell (skip header = row 1)
for row in range(2, num_rows + 2):  # Excel rows start at 1
    for col in range(1, num_cols + 1):
        val_prd = ws_prd.cell(row=row, column=col).value
        val_uat = ws_uat.cell(row=row, column=col).value

        if val_prd != val_uat:
            ws_prd.cell(row=row, column=col).fill = highlight_fill
            ws_uat.cell(row=row, column=col).fill = highlight_fill

# === STEP 7: Save result ===
wb.save(output_file)
print(f"✅ Highlighted comparison saved: {output_file}")
