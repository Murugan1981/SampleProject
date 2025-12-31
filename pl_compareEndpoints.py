import os
import pandas as pd
import numpy as np

# ================= CONFIGURATION =================
BASE_DIR = os.getcwd()
RAW_DIR = os.path.join(BASE_DIR, "shared", "raw")
REPORT_DIR = os.path.join(BASE_DIR, "shared", "reports")

# Input File (User specified name: Endpoints.xlsx)
INPUT_FILE = os.path.join(RAW_DIR, "Endpoints.xlsx") 

# Output File
OUTPUT_FILE = os.path.join(REPORT_DIR, "Pl_SOURCEvsTARGET_Metadata_Comparison.xlsx")

def compare_endpoints():
    print("Starting Endpoint Comparison Script...")
    print(f"Reading Input: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Input file not found at {INPUT_FILE}")
        return

    # 1. Load Data
    try:
        df_source = pd.read_excel(INPUT_FILE, sheet_name="SOURCE")
        df_target = pd.read_excel(INPUT_FILE, sheet_name="TARGET")
    except Exception as e:
        print(f"❌ Error reading Excel sheets: {e}")
        return

    # 2. Normalize Data (Fill NaNs with empty string for clean comparison)
    df_source = df_source.fillna("").astype(str)
    df_target = df_target.fillna("").astype(str)

    print(f"Comparing {len(df_source)} Source endpoints vs {len(df_target)} Target endpoints.")

    # 3. Merge Dataframes (Preserving ALL Columns)
    # This automatically adds _SOURCE and _TARGET suffixes to colliding column names
    merged = pd.merge(
        df_source,
        df_target,
        on="Endpoint",
        how="outer",
        suffixes=('_SOURCE', '_TARGET'),
        indicator=True
    )

    # 4. Calculate Logic Columns
    
    # A. Presence Status
    status_map = {
        "left_only": "Only in SOURCE",
        "right_only": "Only in TARGET",
        "both": "Present in Both"
    }
    merged["Presence_Status"] = merged["_merge"].map(status_map)

    # B. Final Availability (MISSING / FOUND)
    # If it is in Target (both or right_only), it is FOUND. If only in Source, it is MISSING.
    merged["Final_Availability"] = merged["_merge"].apply(
        lambda x: "MISSING" if x == "left_only" else "FOUND"
    )

    # C. Content Mismatch (Check all other columns for equality)
    # Identify dynamic parameter columns (excluding the control columns we just added)
    control_cols = ["Endpoint", "_merge", "Presence_Status", "Final_Availability"]
    data_cols = [c for c in merged.columns if c not in control_cols]
    
    def check_content_mismatch(row):
        if row["_merge"] != "both":
            return "" # No content comparison possible if missing
        
        mismatches = []
        # We need to find matching pairs (e.g. 'param1_SOURCE' vs 'param1_TARGET')
        # We scan for _SOURCE columns and find their _TARGET counterpart
        for col in row.index:
            if col.endswith("_SOURCE"):
                base_name = col[:-7] # remove _SOURCE
                target_col = f"{base_name}_TARGET"
                
                if target_col in row.index:
                    val_s = str(row[col])
                    val_t = str(row[target_col])
                    if val_s != val_t:
                        mismatches.append(base_name)
        
        if mismatches:
            return f"MISMATCH: {', '.join(mismatches)}"
        return "MATCH"

    merged["Content_Check"] = merged.apply(check_content_mismatch, axis=1)

    # 5. Reorder Columns for Report
    # Layout: [Endpoint, Statuses...] + [All SOURCE Columns] + [All TARGET Columns]
    
    # Identify Source and Target columns specifically
    source_cols = [c for c in merged.columns if c.endswith("_SOURCE")]
    target_cols = [c for c in merged.columns if c.endswith("_TARGET")]
    
    # Define Report Order
    report_cols = (
        ["Endpoint", "Final_Availability", "Presence_Status", "Content_Check"] + 
        sorted(source_cols) + 
        sorted(target_cols)
    )
    
    # Select only columns that exist (safety check)
    final_cols = [c for c in report_cols if c in merged.columns]
    final_df = merged[final_cols]

    # 6. Save to Excel
    os.makedirs(REPORT_DIR, exist_ok=True)
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode="w") as writer:
            final_df.to_excel(writer, sheet_name="Comparison_Result", index=False)
            
            # Auto-adjust column widths (Optional Visual Polish)
            worksheet = writer.sheets["Comparison_Result"]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = min(adjusted_width, 50) # Cap width at 50

        print(f"✅ Comparison Complete. Report saved to:\n   {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"❌ Failed to save report: {e}")

if __name__ == "__main__":
    compare_endpoints()
