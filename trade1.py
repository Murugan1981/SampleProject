import os
import pandas as pd
import subprocess
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Input/output file paths
INPUT_FILE = "./shared/input/CDW_Trade_Input.xlsx"
OUTPUT_FILE = "./shared/reports/cdw_trade_validation_report.xlsx"

# Function to fetch CDW response using curl
def fetch_cdw_response(cdwurl):
    try:
        command = [
            'curl', '-u', f'{USERNAME}:{PASSWORD}',
            '-H', 'Accept: application/xml',
            '-H', 'Content-Type:application/xml',
            cdwurl
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode == 0:
            print(f"[DEBUG] curl output fetched successfully")
            return result.stdout
        else:
            print(f"[DEBUG] curl failed with return code {result.returncode}")
            return None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

# Function to extract tag value using namespace
def extract_field_value(xml_root, tag, ns_prefix, ns_map):
    if xml_root is None:
        return "N/A"
    try:
        element = xml_root.find(f".//{ns_prefix}:{tag}", namespaces=ns_map)
        return element.text if element is not None else "NODENOTFOUND"
    except Exception as e:
        return f"ERROR: {e}"

# Read input data
df_input = pd.read_excel(INPUT_FILE, sheet_name=0)
base_url = df_input.loc[0, "CDWBASEURL"]
batch_date = df_input.loc[0, "BATCHDATE"]
trade_ids = str(df_input.loc[0, "TRADEIDS"]).split(",")
field1 = df_input.loc[0, "FIELD1"]
field2 = df_input.loc[0, "FIELD2"]
namespace_str = df_input.loc[0, "namespace"]

# Parse namespace
ns_prefix, ns_uri = namespace_str.split(":", 1)
ns_map = {ns_prefix: ns_uri}

# Final output rows
results = []

# Loop over each trade
for trade_id in trade_ids:
    trade_id = trade_id.strip()
    cdwurl = f"{base_url}/{trade_id}?on={batch_date}"
    xml_text = fetch_cdw_response(cdwurl)

    row = {
        "TRADEID": trade_id,
        "CDWURL": cdwurl,
        "URLRESPONSE": "FOUND" if xml_text else "MISSING"
    }

    if xml_text:
        try:
            xml_root = ET.fromstring(xml_text)
            row["TRADE_STATUS"] = extract_field_value(xml_root, field1, ns_prefix, ns_map)
            row["TRADE_SETTLEMENT_STATUS"] = extract_field_value(xml_root, field2, ns_prefix, ns_map)
            row["COMMENTS"] = ""
        except Exception as e:
            row["TRADE_STATUS"] = "PARSE_ERROR"
            row["TRADE_SETTLEMENT_STATUS"] = "PARSE_ERROR"
            row["COMMENTS"] = str(e)
    else:
        row["TRADE_STATUS"] = "N/A"
        row["TRADE_SETTLEMENT_STATUS"] = "N/A"
        row["COMMENTS"] = "No response or curl error"

    results.append(row)

# Save output
df_output = pd.DataFrame(results)
df_output.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Report saved to {OUTPUT_FILE}")
