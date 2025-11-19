import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load .env values
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Input/output file paths
INPUT_FILE = "./shared/input/CDW_Trade_Input.xlsx"
OUTPUT_FILE = "./shared/reports/cdw_trade_validation_report.xlsx"

# Read input
df_input = pd.read_excel(INPUT_FILE, sheet_name=0)
base_url = df_input.loc[0, "CDWBASEURL"]
batch_date = df_input.loc[0, "BATCHDATE"]
trade_ids = str(df_input.loc[0, "TRADEIDS"]).split(",")
field1 = df_input.loc[0, "FIELD1"]
field2 = df_input.loc[0, "FIELD2"]
namespace_str = df_input.loc[0, "namespace"]

# Parse namespace (e.g., 'fpml:http://www.fpml.org/FpML-5/reporting')
prefix, uri = namespace_str.split(":", 1)
ns = {prefix: uri}

# Helper to fetch and parse XML
def fetch_and_parse_xml(url):
    try:
        response = requests.get(url, auth=(USERNAME, PASSWORD), timeout=10)
        if response.status_code == 200:
            return "FOUND", ET.fromstring(response.text), ""
        else:
            return "MISSING", None, f"HTTP {response.status_code}"
    except Exception as e:
        return "MISSING", None, str(e)

# Helper to extract value from XML
def extract_field_value(root, tag):
    if root is None:
        return "N/A"
    element = root.find(f".//{prefix}:{tag}", ns)
    return element.text if element is not None else "NODENOTFOUND"

# Process all trades
results = []
for trade_id in trade_ids:
    trade_id = trade_id.strip()
    full_url = f"{base_url}/{trade_id}?on={batch_date}"
    url_status, xml_root, error = fetch_and_parse_xml(full_url)
    
    row = {
        "TRADEID": trade_id,
        "CDWURL": full_url,
        "URLRESPONSE": url_status
    }

    if url_status == "FOUND":
        row["TRADE_STATUS"] = extract_field_value(xml_root, field1)
        row["TRADE_SETTLEMENT_STATUS"] = extract_field_value(xml_root, field2)
        row["COMMENTS"] = ""
    else:
        row["TRADE_STATUS"] = "N/A"
        row["TRADE_SETTLEMENT_STATUS"] = "N/A"
        row["COMMENTS"] = error
    
    results.append(row)

# Save results
df_output = pd.DataFrame(results)
df_output.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Validation complete. Output saved to: {OUTPUT_FILE}")
