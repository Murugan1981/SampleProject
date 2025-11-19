import os
import pandas as pd
import xml.etree.ElementTree as ET
import subprocess
from dotenv import load_dotenv

# Load env vars
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Input/output
INPUT_FILE = 'shared/input/CDW_TradeFields_Input.xlsx'
OUTPUT_FILE = 'shared/reports/CDW_TradeFields_Report.xlsx'

def fetch_cdw_response(cdwurl):
    try:
        command = [
            'curl', '-u', f'{USERNAME}:{PASSWORD}',
            '-H', 'Accept: application/xml',
            '-H', 'Content-Type: application/xml',
            cdwurl
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        return result.stdout if result.returncode == 0 else None
    except Exception:
        return None

def extract_fields_from_xml(xml_text, fields, ns_prefix, ns_uri):
    try:
        ns = {ns_prefix: ns_uri}
        root = ET.fromstring(xml_text)
        values = []
        for field in fields:
            tag = root.find(f".//{ns_prefix}:{field}", ns)
            values.append(tag.text.strip() if tag is not None and tag.text else "")
        return values, None
    except ET.ParseError as e:
        return ["" for _ in fields], f"XML Parse Error: {str(e)}"
    except Exception as e:
        return ["" for _ in fields], f"Unknown XML Error: {str(e)}"

def main():
    df = pd.read_excel(INPUT_FILE, sheet_name=0)
    results = []

    # Expecting single row with baseurl, tradeids, batchdate, field1, field2, namespace
    row = df.iloc[0]
    baseurl = str(row['CDWBASEURL']).strip()
    trade_ids = [x.strip() for x in str(row['TRADEIDS']).split(",") if x.strip()]
    batchdate = str(row['BATCHDATE']).strip()
    fields = [str(row['FIELD1']).strip(), str(row['FIELD2']).strip()]
    ns_cell = str(row['namespace']).strip()  # e.g., "fpml!:http://www.fpml.org/FpML-5/reporting"

    if "!" not in ns_cell:
        print("❌ Invalid namespace format. Expected: prefix!:url")
        return

    ns_prefix, ns_uri = ns_cell.split("!")

    for tid in trade_ids:
        full_url = f"{baseurl}/{tid}?on={batchdate}"
        print(f"🔍 Fetching: {full_url}")
        xml_data = fetch_cdw_response(full_url)

        if not xml_data:
            results.append({
                "TRADEID": tid,
                "CDWURL": full_url,
                "CDWURL_STATUS": "CDWURL not found",
                fields[0]: "",
                fields[1]: "",
                "COMMENTS": "CDWURL fetch failed"
            })
            continue

        extracted_values, error = extract_fields_from_xml(xml_data, fields, ns_prefix, ns_uri)

        if error:
            results.append({
                "TRADEID": tid,
                "CDWURL": full_url,
                "CDWURL_STATUS": "CDWURL opened but XML error",
                fields[0]: "",
                fields[1]: "",
                "COMMENTS": error
            })
        else:
            results.append({
                "TRADEID": tid,
                "CDWURL": full_url,
                "CDWURL_STATUS": "CDWURL works",
                fields[0]: extracted_values[0],
                fields[1]: extracted_values[1],
                "COMMENTS": "Success"
            })

    pd.DataFrame(results).to_excel(OUTPUT_FILE, index=False)
    print(f"\n✅ Output generated: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
