import os
import pandas as pd
from datetime import datetime

def run_files_in_folder(datafile: str, rows: str, output_file: str = "./shared/reports/file_validation_report.csv"):
    print(f"\n📄 Validating files from: {datafile}")
    
    # Read CSV input
    try:
        df_input = pd.read_csv(datafile)
    except Exception as e:
        print(f"❌ Failed to read input CSV: {e}")
        return

    # Ensure required columns exist
    required_cols = {"Validate", "FullFileName", "BaseFileName", "DestinationFolder"}
    if not required_cols.issubset(df_input.columns):
        raise ValueError(f"Input file must contain columns: {required_cols}")

    # Slice the dataframe if specific row index is given
    if rows.upper() != "ALL":
        try:
            row_index = int(rows)
            if row_index < len(df_input):
                df_input = df_input.iloc[[row_index]]
            else:
                print(f"⚠️ Row {row_index} out of range in input file.")
                return
        except ValueError:
            print(f"⚠️ Invalid row index: {rows}")
            return

    results = []

    # Validate each row
    for _, row in df_input.iterrows():
        base_file = str(row["BaseFileName"]).strip()
        full_file = str(row["FullFileName"]).strip()
        validate_flag = str(row["Validate"]).strip().upper()
        dest_folder = str(row["DestinationFolder"]).strip()

        if validate_flag != "YES":
            continue

        result = "Missing"
        comment = ""
        created_time = "-"
        modified_time = "-"

        if os.path.exists(dest_folder):
            files_in_folder = os.listdir(dest_folder)

            # Exact match
            if full_file in files_in_folder:
                result = "Found"
                full_path = os.path.join(dest_folder, full_file)
                created_time = datetime.fromtimestamp(os.path.getctime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
                modified_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Check for similar files
                similar_files = [f for f in files_in_folder if base_file in f]
                if similar_files:
                    comment = f"Found similar file(s): {', '.join(similar_files)}"
                    similar_files.sort(key=lambda x: os.path.getmtime(os.path.join(dest_folder, x)), reverse=True)
                    latest_file = similar_files[0]
                    latest_path = os.path.join(dest_folder, latest_file)
                    created_time = datetime.fromtimestamp(os.path.getctime(latest_path)).strftime("%Y-%m-%d %H:%M:%S")
                    modified_time = datetime.fromtimestamp(os.path.getmtime(latest_path)).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    comment = "No similar file found"
        else:
            comment = f"Destination folder not found: {dest_folder}"

        results.append({
            "FileType": row.get("FileType", ""),
            "FullFileName": full_file,
            "BaseFileName": base_file,
            "DestinationFolder": dest_folder,
            "Result": result,
            "CreatedTime": created_time,
            "ModifiedTime": modified_time,
            "Comments": comment
        })

    # Write output to CSV
    df_result = pd.DataFrame(results)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df_result.to_csv(output_file, index=False)
    print(f"✅ Validation completed. Report generated: {output_file}")
