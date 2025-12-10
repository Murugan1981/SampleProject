import os
import pyodbc
import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from auth import get_password
import subprocess
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------------------------------------
# Load environment variables
# -------------------------------------------------------
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

# -------------------------------------------------------
# Input / Output files
# -------------------------------------------------------
input_file = "shared/input/ExtractBookReffromCDWurl_ValidateInDB_Input.xlsx"
sheet_name = "TradesInFile_BookRefInDB"
output_file = "shared/reports/ExtractBookReffromCDWurl_ValidateInDB_report.xlsx"

# -------------------------------------------------------
# Fetch XML from CDW
# -------------------------------------------------------
def fetch_cdw_response(cdwurl):
    try:
        command = [
            'curl', '-u', f'{USERNAME}:{PASSWORD}',
            '-H', 'Accept: application/xml',
            '-H', 'Content-Type:application/xml',
            cdwurl
        ]
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            print("[DEBUG] curl output fetched successfully")
            return result.stdout
        else:
            print(f"[DEBUG] curl failed with return code {result.returncode}")
            return None

    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

# -------------------------------------------------------
# Validate BookRef in SQL Server
# -------------------------------------------------------
def validate_book_in_db(server, db, table, column, book_ref):
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()

        # Final SQL from your screenshot
        query = f"SELECT DISTINCT {column} FROM {db}.{table} WHERE {column} IN ('{book_ref}')"

        cursor.execute(query)
        rows = cursor.fetchall()

        return "FOUND" if rows else "MISSING"

    except Exception as e:
        return f"Exception:{e}"

# -------------------------------------------------------
# Extract BookRef + BookType from CDW XML
# -------------------------------------------------------
def extract_book_from_cdw(xml_data):
    try:
        root = ET.fromstring(xml_data)
        ns = {'fpml': 'http://www.fpml.org/FpML-5/reporting'}

        accounts = root.findall(".//fpml:account[@id='HOUSE-ACCOUNT']", namespaces=ns)

        if accounts:
            account_node = accounts[-1]

            book_ref_node = account_node.find(
                "fpml:accountId[@accountIdScheme='mhi:book-ref']",
                namespaces=ns
            )
            book_type_node = account_node.find(
                "fpml:accountId[@accountIdScheme='mhi:book-type']",
                namespaces=ns
            )

            if book_ref_node is not None and book_type_node is not None:
                return book_ref_node.text.strip(), book_type_node.text.strip(), None
            else:
                return None, None, "Missing book-ref or book-type in account node"

        else:
            return None, None, "HOUSE-ACCOUNT node not found"

    except ET.ParseError as e:
        return None, None, f"XML ParseError: {str(e)}"
    except Exception as e:
        return None, None, f"XML Error: {str(e)}"

# -------------------------------------------------------
# Main processing function
# -------------------------------------------------------
def process_trades():

    df = pd.read_excel(input_file, sheet_name=sheet_name)
    results = []

    for _, row in df.iterrows():

        dest_folder = str(row.get("DESTINATIONFOLDER", "")).strip()
        file_type = str(row.get("FILETYPE", "")).strip().upper()
        filename = str(row.get("FILENAME", "")).strip()
        filepath = os.path.join(dest_folder, filename)

        trades_str = str(row.get("TRADES", "")).strip()
        trade_ids = [t.strip() for t in trades_str.split(",") if t.strip()]

        cdw_base_url = str(row.get("CDWURL_BASEURL", "")).strip()
        cdwurl_filler = str(row.get("CDWURL_FILLER", "")).strip()
        reporting_date = str(row.get("CDWURL_BATCHDATE", "")).strip()

        server = str(row.get("DatabaseServerName", "")).strip()
        db = str(row.get("DatabaseName", "")).strip()
        table = str(row.get("TableName", "")).strip()
        column = str(row.get("ColumnNameForBookRef", "")).strip()

        for trade_id in trade_ids:

            trade_found = "MISSING"
            book_ref_in_cdw = "-"
            book_found_flag = "MISSING"
            query_used = f"-- CDW URL: {cdwurl_filler}"
            comments = ""

            cdw_url = f"{cdw_base_url}/{trade_id}{cdwurl_filler}{reporting_date}"

            try:
                # STEP 1: Search in MBE_POSITION_SET file
                file_df = pd.read_csv(filepath, delimiter="|", dtype=str, engine="python")
                file_df = file_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                file_df["M_ORIGIN_RE"] = file_df["M_ORIGIN_RE"].astype(str).str.strip()
                file_df["BOOK_ID"] = file_df["BOOK_ID"].astype(str).str.strip()

                match_row = file_df[file_df["M_ORIGIN_RE"] == trade_id]

                if not match_row.empty:
                    trade_found = "FOUND"
                    file_book_ref = match_row["BOOK_ID"].values[0].strip()

                    # STEP 2: Fetch from CDW
                    xml_data = fetch_cdw_response(cdw_url)

                    if xml_data:
                        book_ref, book_type, err = extract_book_from_cdw(xml_data)

                        if book_ref and book_type:

                            book_ref_in_cdw = f"{book_ref} {book_type}"
                            book_ref_in_db = f"{book_ref} {book_type}"

                            query_used = f"SELECT DISTINCT {column} FROM {db}.{table} WHERE {column} IN ('{book_ref_in_db}')"
                            comments = book_ref_in_cdw

                            # STEP 3: DB Validation
                            db_result = validate_book_in_db(server, db, table, column, book_ref_in_db)
                            book_found_flag = db_result

                            comments += f" | BookRef DB Check: {db_result}"

                        else:
                            comments = err or "BookRef not extracted"
                            query_used += " -- Missing book-ref/book-type"

                    else:
                        comments = "CDW fetch failed or empty response"
                        query_used += " -- CDW fetch failed"

                else:
                    comments = "TradeID not found in MBE_POSITION_SET file"
                    query_used += " -- M_ORIGIN_RE not found"

            except Exception as e:
                comments = f"Exception occurred: {str(e)}"
                query_used += " -- Exception during validation"

            # Append result
            results.append({
                "FileType": file_type,
                "Filename": filepath,
                "TradeID": trade_id,
                "TradeFound": trade_found,
                "CDWURL": cdw_url,
                "BOOKREF_INCDW": book_ref_in_cdw,
                "BOOKFOUND": book_found_flag,   # NEW COLUMN
                "Query_Used": query_used,
                "Comments": comments
            })

    pd.DataFrame(results).to_excel(output_file, index=False)
    print(f"Report generated: {output_file}")

# -------------------------------------------------------
# Main entry
# -------------------------------------------------------
if __name__ == "__main__":
    process_trades()
