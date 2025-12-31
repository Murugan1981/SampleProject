import pandas as pd
import os
import sys

# ================= CONFIGURATION =================
# Hardcoded paths based on your project structure
BASE_DIR = os.getcwd()
INPUT_FILE = os.path.join(BASE_DIR, "shared", "raw", "Extraction.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "shared", "reports", "Pl_SOURCEvsTARGET_Metadata_Comparison.xlsx")

def compare_endpoints():
    print(f"Starting Metadata Comparison...")
    print(f"Reading Input: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Input file not found at {INPUT_FILE}")
        return

    # 1. Load Data
    try:
        df_source = pd.read_excel(INPUT_FILE, sheet_name='SOURCE')
        df_target = pd.read_excel(INPUT_FILE, sheet_name='TARGET')
    except ValueError as e:
        print(f"❌ Error reading sheets: {e}")
        print("Ensure 'SOURCE' and 'TARGET' sheets exist.")
        return

    # 2. Normalize Data (Trim whitespace for accurate comparison)
    # We assume 'Endpoint' is the unique key
    key_col = 'Endpoint'
    
    if key_col not in df_source.columns or key_col not in df_target.columns:
        print(f"❌ Error: Column '{key_col}' missing in one of the sheets.")
        return

    # Create sets for quick lookup
    source_endpoints = set(df_source[key_col].astype(str).str.strip())
    target_endpoints = set(df_target[key_col].astype(str).str.strip())

    # 3. Perform Comparison
    all_endpoints = source_endpoints.union(target_endpoints)
    comparison_rows = []

    print(f"Analysing {len(all_endpoints)} total unique endpoints...")

    for endpoint in sorted(all_endpoints):
        in_source = endpoint in source_endpoints
        in_target = endpoint in target_endpoints

        status = "MATCHING"
        if in_source and not in_target:
            status = "MISSING IN TARGET"
        elif not in_source and in_target:
            status = "MISSING IN SOURCE"

        # Extract details to show in the report
        # We try to pull the row data from Source first, then Target if missing
        row_data = {}
        
        if in_source:
            # Get original row data from Source
            original_row = df_source[df_source[key_col] == endpoint].iloc[0].to_dict()
            row_data.update(original_row)
        elif in_target:
            # Get original row data from Target
            original_row = df_target[df_target[key_col] == endpoint].iloc[0].to_dict()
            row_data.update(original_row)

        # Append Status Column
        row_data['COMPARISON_STATUS'] = status
        
        # Ensure Endpoint is first in the dict
        # (Dictionary insertion order is preserved in modern Python)
        final_row = {'Endpoint': endpoint}
        final_row.update({k: v for k, v in row_data.items() if k != 'Endpoint'})
        
        comparison_rows.append(final_row)

    # 4. Create DataFrame & Save
    df_result = pd.DataFrame(comparison_rows)

    # Ensure Output Directory Exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print(f"Saving Report to: {OUTPUT_FILE}")
    
    # Formatting the Excel for readability
    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        df_result.to_excel(writer, sheet_name='Comparison_Result', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['Comparison_Result']
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}) # Missing
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'}) # Matching

        # Apply Header Format
        for col_num, value in enumerate(df_result.columns.values):
            worksheet.write(0, col_num, value, header_fmt)
            # Set approximate column width
            worksheet.set_column(col_num, col_num, 20)

        # Conditional Formatting on Status Column
        status_col_idx = df_result.columns.get_loc('COMPARISON_STATUS')
        # Apply to all rows
        worksheet.conditional_format(1, status_col_idx, len(df_result), status_col_idx,
                                     {'type': 'text',
                                      'criteria': 'containing',
                                      'value': 'MISSING',
                                      'format': red_fmt})
        
        worksheet.conditional_format(1, status_col_idx, len(df_result), status_col_idx,
                                     {'type': 'text',
                                      'criteria': 'containing',
                                      'value': 'MATCHING',
                                      'format': green_fmt})

    print("✅ Comparison Complete.")

if __name__ == "__main__":
    compare_endpoints()
