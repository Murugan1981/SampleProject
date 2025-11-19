import os
import subprocess
import pandas as pd
import xml.etree.ElementTree as ET


def fetch_cdw_response(url, username, password):
    try:
        command = [
            "curl",
            "-u", f"{username}:{password}",
            "-H", "Accept: application/xml",
            "-H", "Content-Type: application/xml",
            url
        ]

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60
        )

        xml_data = result.stdout.strip()
        return xml_data if xml_data else None

    except Exception:
        return None


def extract_trade_field(root, field, ns):
    """
    Extracts any field such as:
    trade-status → look for <m:tradeNarrative informationItemTypeScheme="mhi:trade-status">
    """
    xpath = f".//m:tradeNarrative[@informationItemTypeScheme='mhi:{field}']"
    node = root.find(xpath, namespaces=ns)
    if node is not None and node.text:
        return node.text.strip()
    return ""


def process_cdw_file(input_path, output_path):
    df = pd.read_excel(input_path)

    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")

    results = []

    # Known namespaces in your XML
    ns = {
        "fpml": "http://www.fpml.org/FpML-5/reporting",
        "m": "urn:com.mizuho.bdm"
    }

    # Detect FIELD columns dynamically
    field_columns = [col for col in df.columns if col.startswith("FIELD")]

    for _, row in df.iterrows():

        base_url = str(row["CDWBASEURL"]).strip()
        trade_ids_raw = str(row["TRADEIDS"]).strip()
        batch_date = str(row["BATCHDATE"]).strip()

        trade_ids = [t.strip() for t in trade_ids_raw.split(",") if t.strip()]

        for trade_id in trade_ids:

            cdw_url = f"{base_url}/{trade_id}?on={batch_date}"

            xml_response = fetch_cdw_response(cdw_url, USERNAME, PASSWORD)

            if xml_response is None:
                result = {
                    "TRADEID": trade_id,
                    "CDWURL": cdw_url,
                    "CDWSTATUS": "NOT WORKING",
                    "COMMENTS": "URL returned no content"
                }
                # Add FIELD columns empty
                for f in field_columns:
                    result[f] = ""
                results.append(result)
                continue

            try:
                root = ET.fromstring(xml_response)

                result = {
                    "TRADEID": trade_id,
                    "CDWURL": cdw_url,
                    "CDWSTATUS": "WORKING",
                    "COMMENTS": ""
                }

                # Extract each field generically
                for f in field_columns:
                    field_name = str(row[f]).strip()
                    value = extract_trade_field(root, field_name, ns)
                    result[f] = value

                results.append(result)

            except Exception:
                result = {
                    "TRADEID": trade_id,
                    "CDWURL": cdw_url,
                    "CDWSTATUS": "NOT WORKING",
                    "COMMENTS": "XML parse failed"
                }
                for f in field_columns:
                    result[f] = ""
                results.append(result)

    df_out = pd.DataFrame(results)
    df_out.to_excel(output_path, index=False)
    print("Report generated:", output_path)


if __name__ == "__main__":
    process_cdw_file(
        input_path="shared/input/CDW_Input.xlsx",
        output_path="shared/reports/CDW_Report.xlsx"
    )
