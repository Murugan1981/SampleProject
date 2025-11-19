import pandas as pd
import xml.etree.ElementTree as ET
import subprocess
import os
from dotenv import load_dotenv

load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Function to fetch CDW response
def fetch_cdw_response(cdwurl):
    try:
        command = ['curl', '-u', f'{USERNAME}:{PASSWORD}', '-H', 'Accept: application/xml',
                   '-H', 'Content-Type:application/xml', cdwurl]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode == 0:
            if "<FpML" in result.stdout:
                print(f"[DEBUG] CDWURL worked: {cdwurl}")
                return result.stdout
            else:
                print(f"[WARN] CDWURL responded but invalid XML: {cdwurl}")
                return None
        else:
            print(f"[ERROR] curl failed with return code {result.returncode}")
            return None
    except Exception as e:
        print(f"Exception during curl: {e}")
        return None

# Function to flatten XML
def flatten_xml(element, parent_path="", flattened=None):
    if flattened is None:
        flattened = {}
    tag = element.tag.split("}")[-1]  # Remove namespace
    path = f"{parent_path}.{tag}" if parent_path else tag
    text = element.text.strip() if element.text and element.text.strip() else None
    if text:
        flattened[path] = text
    for child in element:
        flatten_xml(child, path, flattened)
    return flattened

# Main function
def validate_trade_status():
    input_path = "./shared/input/CDW_Validation_Input.xlsx"
    output_path = "./shared/reports/CDW_Validation_Report.xlsx"
    
    df_input = pd.read_excel(input_path, sheet_name=0)
    results = []

    for idx, row in df_input.iterrows():
        base_url = row["CDWBASEURL"]
        trade_ids = str(row["TRADEIDS"]).split(",")
        batch_date = str(row["BATCHDATE"])
        field1 = row["FIELD1"]
        field2 = row["FIELD2"]

        for trade_id in trade_ids:
            trade_id = trade_id.strip()
            cdw_url = f"{base_url}/{trade_id}/{batch_date}"
            xml_response = fetch_cdw_response(cdw_url)

            result_row = {
                "TRADEID": trade_id,
                "CDWURL": cdw_url,
                "TRADESTATUS": "",
                "TRADESETTLEMENTSTATUS": "",
                "Comments": ""
            }

            if xml_response is None:
                result_row["Comments"] = "CDWURL not found"
            else:
                try:
                    root = ET.fromstring(xml_response)
                    flat_xml = flatten_xml(root)
                    result_row["Comments"] = "CDWURL works"
                    result_row["TRADESTATUS"] = flat_xml.get(field1, "")
                    result_row["TRADESETTLEMENTSTATUS"] = flat_xml.get(field2, "")
                except Exception as e:
                    result_row["Comments"] = f"Parse error: {e}"
            results.append(result_row)

    df_result = pd.DataFrame(results)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_result.to_excel(output_path, index=False)
    print(f"\n✅ CDW Trade Status validation completed. Report saved at: {output_path}")

# Run the script
if __name__ == "__main__":
    validate_trade_status()
