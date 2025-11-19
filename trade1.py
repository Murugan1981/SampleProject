import os
import subprocess
import xml.etree.ElementTree as ET
import pandas as pd
from openpyxl import Workbook
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Function: Fetch response from CDW URL using curl
def fetch_cdw_response(full_url):
    try:
        cmd = [
            "curl",
            "-u", f"{USERNAME}:{PASSWORD}",
            "-s", full_url
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        print(f"[ERROR] Failed to fetch: {e}")
        return None

# Function: Flatten XML nodes recursively
def flatten_xml(element, parent_path="", flattened=None):
    if flattened is None:
        flattened = {}
    current_path = f"{parent_path}.{element.tag}" if parent_path else element.tag
    if element.text and element.text.strip():
        flattened[current_path] = element.text.strip()
    for child in element:
        flatten_xml(child, current_path, flattened)
    return flattened

# Function: Main validation logic
def validate_cdw_trade_status():
    input_path = "./shared/input/cdw_input_file.xlsx"
    output_path = "./shared/reports/cdw_validation_report.xlsx"

    df = pd.read_excel(input_path, sheet_name=0)
    results = []

    for idx, row in df.iterrows():
        trade_id = row.get("TRADEID", "").strip()
        full_url = row.get("CDWURL", "").strip()
        field1 = row.get("FIELD1", "").strip()
        field2 = row.get("FIELD2", "").strip()

        result_row = {
            "TRADEID": trade_id,
            "CDWURL": full_url,
            "FIELD1": field1,
            "FIELD2": field2,
            "TRADESTATUS": "",
            "TRADESETTLEMENTSTATUS": "",
            "Comments": ""
        }

        xml_text = fetch_cdw_response(full_url)

        if not xml_text:
            result_row["Comments"] = "CDWURL not found"
        else:
            try:
                root = ET.fromstring(xml_text)
                flat_xml = flatten_xml(root)
                result_row["Comments"] = "CDWURL works"

                # Field1: TRADESTATUS
                result_row["TRADESTATUS"] = next(
                    (v for k, v in flat_xml.items() if field1.lower() in k.lower()), ""
                )

                # Field2: TRADESETTLEMENTSTATUS
                result_row["TRADESETTLEMENTSTATUS"] = next(
                    (v for k, v in flat_xml.items() if field2.lower() in k.lower()), ""
                )

            except ET.ParseError:
                result_row["Comments"] = "CDWURL not found (Parse Error)"

        results.append(result_row)

    # Export to Excel
    output_df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    output_df.to_excel(output_path, index=False)
    print(f"✅ Validation completed. Report saved to: {output_path}")

# Run the script
if __name__ == "__main__":
    validate_cdw_trade_status()
