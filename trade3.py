import os
import pandas as pd
import subprocess
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from auth import get_password
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load .env credentials
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

# Input/Output
input_excel = "shared/input/ExtractTradeStatusFromCDW.xlsx"
output_excel = "shared/reports/ExtractTradeStatusFromCDW_report.xlsx"
sheet_name = "CDWInput"
output_data = []

# Fetch CDW XML using curl
def fetch_cdw_response(cdwurl):
    try:
        command = [
            'curl', '-u', f'{USERNAME}:{PASSWORD}',
            '-H', 'Accept: application/xml',
            '-H', 'Content-Type: application/xml',
            cdwurl
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode == 0:
            return result.stdout
        else:
            return None
    except Exception as e:
        return None

# Extract exact node value from XML
def extract_node_value(xml_data, tag_name, namespace):
    try:
        root = ET.fromstring(xml_data)
        ns = {'fpml': namespace}
        node = root.find(f".//fpml:{tag_name}", ns)
        if node is not None and node.text:
            return node.text.strip()
        return None
    except ET.ParseError:
        return "XML Parse Error"
    except Exception:
        return "Extraction Error"

# Main processing logic
def process_excel():
    df = pd.read_excel(input_excel, sheet_name=sheet_name)

    for idx, row in df.iterrows():
        base_url = str(row.get("CDWBASEURL", "")).strip()
        trades = str(row.get("TRADEIDS", "")).strip()
        batch_date = str(row.get("BATCHDATE", "")).strip()
        field1 = str(row.get("FIELD1", "")).strip()
        field2 = str(row.get("FIELD2", "")).strip()
        ns_raw = str(row.get("namespace", "")).strip()

        if ":" not in ns_raw:
            print(f"❌ Invalid namespace format in row {idx+2}. Expected: prefix!:url")
            continue

        prefix, ns = ns_raw.split("!:", 1)
        trade_ids = [tid.strip() for tid in trades.split(",") if tid.strip()]

        for trade_id in trade_ids:
            full_url = f"{base_url.strip()}/{trade_id}?on={batch_date}"
            xml_data = fetch_cdw_response(full_url)

            if xml_data is None:
                output_data.append({
                    "TradeID": trade_id,
                    "CDWURL": full_url,
                    "CDWURL_Status": "CDWURL not found",
                    field1: "",
                    field2: "",
                    "Comments": "No response from CDW"
                })
                continue

            val1 = extract_node_value(xml_data, field1, ns)
            val2 = extract_node_value(xml_data, field2, ns)

            output_data.append({
                "TradeID": trade_id,
                "CDWURL": full_url,
                "CDWURL_Status": "CDWURL works",
                field1: val1,
                field2: val2,
                "Comments": "Trade info extracted" if val1 or val2 else "Fields not found"
            })

    pd.DataFrame(output_data).to_excel(output_excel, index=False)
    print(f"✅ Report generated: {output_excel}")

# Run
if __name__ == "__main__":
    process_excel()
