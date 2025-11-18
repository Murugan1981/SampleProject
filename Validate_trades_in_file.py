import os
import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from auth import get_password
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")

AUTH = HttpNtlmAuth(USERNAME, PASSWORD)


def fetch_cde_response(cdw_url):
    try:
        response = requests.get(cdw_url, auth=AUTH, verify=False, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"❌ Failed to fetch CDW data from {cdw_url} — {e}")
        return None


def extract_bookref_from_cdw_xml(xml_content):
    try:
        root = ET.fromstring(xml_content)
        namespaces = {'fpml': 'http://www.fpml.org/FpML-5/confirmation'}
        account_nodes = root.findall(".//fpml:account", namespaces)

        for account in account_nodes:
            if account.attrib.get("id") == "HOUSE-ACCOUNT":
                book_ref = account.find("fpml:accountId", namespaces).text.strip()
                book_type = account.find("fpml:accountType", namespaces).text.strip()
                return f"{book_ref} {book_type}"

        return "BOOKREF NOT FOUND"

    except Exception as e:
        return f"XML PARSE ERROR: {e}"


def validate_trades_in_file(input_file, sheet_name, output_file):
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    all_results = []

    for idx, row in df.iterrows():
        print(f"Processing row {idx + 1}/{len(df)}: {row['Filename']}")
        try:
            filetype = row["FileType"]
            filename = row["Filename"]
            trades = str(row["Trades"]).split(",")
            cdw_url_template = str(row["CDWURL"])
            batch_date = str(row["BatchDate"]).strip()
            db_instance = row["DatabaseInstanceName"]
            db_name = row["Databasename"]
            table_name = row["Tablename"]
            column_name = row["ColumnNameForBookRef"]

            for trade in trades:
                trade = trade.strip()
                row_result = {
                    "FileType": filetype,
                    "Filename": filename,
                    "TradeID": trade,
                    "CDWURL": "",
                    "BOOKREF_INCDW": "",
                    "BookRefFoundINDB": "",
                    "TradeFound": "",
                    "query_used": "",
                    "comments": ""
                }

                if not trade:
                    row_result["comments"] = "Empty trade ID"
                    all_results.append(row_result)
                    continue

                cdw_url = cdw_url_template.replace("{trade}", trade).replace("{BatchDate}", batch_date)
                row_result["CDWURL"] = cdw_url

                xml_data = fetch_cde_response(cdw_url)
                if xml_data is None:
                    row_result["comments"] = "Failed to fetch CDW data"
                    all_results.append(row_result)
                    continue

                book_ref = extract_bookref_from_cdw_xml(xml_data)
                row_result["BOOKREF_INCDW"] = book_ref

                # Optional DB logic to be added
                # row_result["BookRefFoundINDB"] = ...
                # row_result["TradeFound"] = ...
                # row_result["query_used"] = ...
                # row_result["comments"] = ...

                all_results.append(row_result)

        except Exception as e:
            all_results.append({
                "FileType": row.get("FileType"),
                "Filename": row.get("Filename"),
                "TradeID": "",
                "CDWURL": "",
                "BOOKREF_INCDW": "",
                "BookRefFoundINDB": "",
                "TradeFound": "",
                "query_used": "",
                "comments": f"Row processing error: {e}"
            })

    output_df = pd.DataFrame(all_results)
    output_df.to_excel(output_file, index=False)
    print(f"\n✅ Validation completed. Results saved to: {output_file}")
