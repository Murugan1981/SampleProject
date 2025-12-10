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

# Load environment variables
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

# Input/Output files
input_file = "shared/input/ExtractBookReffFromCDWurl_ValidateBookInDB_Input.xlsx"
sheet_name = "TradesInFile_BookRefInDB"
output_file = "shared/reports/ExtractBookReffFromCDWurl_ValidateInDB_report.xlsx"

def fetch_cdw_response(cdwurl):
    try:
        # Note: Using subprocess with curl as per original image, though requests library is imported
        command = ['curl', '-u', f'{USERNAME}:{PASSWORD}', '-H', 'Accept: application/xml', '-H', 'Content-Type:application/xml', cdwurl]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode == 0:
            print(f"[DEBUG] curl output fetched successfully")
            return result.stdout
        else:
            print(f"[DEBUG] curl failed with return code {result.returncode}")
            return None
    except Exception as e:
        print(f"Exception occured: {e}")
        return None

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
        
        # FIX: Trim whitespace from the DB column to ensure accurate matching
        # Old query: SELECT DISTINCT {column} FROM {db}.{table} WHERE {column} = ?
        query = f"SELECT DISTINCT {column} FROM {db}.{table} WHERE LTRIM(RTRIM({column})) = ?"
        
        cursor.execute(query, (book_ref,))
        
        # Check if any row is returned
        return "FOUND" if cursor.fetchone() else "MISSING"
        
    except Exception as e:
        return f"Exception: {e}"

def extract_book_from_cdw(xml_data):
    try:
        root = ET.fromstring(xml_data)
        ns = {'fpml': 'http://www.fpml.org/FpML-5/reporting'}
        accounts = root.findall(".//fpml:account[@id='HOUSE-ACCOUNT']", namespaces=ns)
        if accounts:
            account_node = accounts[-1]
            book_ref_node = account_node.find(".//fpml:accountId[@accountIdScheme='mhi:book-ref']", namespaces=ns)
            book_type_node = account_node.find(".//fpml:accountId[@accountIdScheme='mhi:book-type']", namespaces=ns)
            
            if book_ref_node is not None and book_type_node is not None:
                # Strip text immediately to avoid whitespace issues
                return book_ref_node.text.strip(), book_type_node.text.strip(), None
            else:
                return None, None, "Missing book-ref or book-type in account node"
        else:
            return None, None, "HOUSE-ACCOUNT node not found"
    except ET.ParseError as e:
        return None, None, f"XML ParseError: {str(e)}"
    except Exception as e:
        return None, None, f"XML Error: {str(e)}"

def process_trades():
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    results = []

    for _, row in df.iterrows():
        dest_folder = str(row.get("DESTINATIONFOLDER", "")).strip()
        print(f"dest_folder={dest_folder}")
        file_type = str(row.get("FILETYPE", "")).strip().upper()
        filename = str(row.get("FILENAME", "")).strip()
        print(f"filename = {filename}")
        filepath = os.path.join(dest_folder, filename)
        print(f"filepath={filepath}")
        
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
            book_db_result = "Not Checked"  # Default value for new column
            
            cdw_url = f"{cdw_base_url}/{trade_id}{cdwurl_filler}{reporting_date}"
            query_used = f"-- CDW URL: {cdw_url}"
            comments = ""

            try:
                # STEP 1: Search in MBE_POSITION_SET file
                # Using engine='python' for more robust parsing as per original code
                file_df = pd.read_csv(filepath, delimiter="|", dtype=str, engine="python")
                
                # Clean up dataframe columns (strip whitespace)
                file_df = file_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                # Ensure specific columns are treated as strings and stripped
                if "M_ORIGIN_RE" in file_df.columns:
                    file_df["M_ORIGIN_RE"] = file_df["M_ORIGIN_RE"].astype(str).str.strip()
                if "BOOK_ID" in file_df.columns:
                    file_df["BOOK_ID"] = file_df["BOOK_ID"].astype(str).str.strip()

                match_row = file_df[file_df["M_ORIGIN_RE"] == trade_id]
                
                if not match_row.empty:
                    trade_found = "FOUND"
                    # file_book_ref logic existed in original, but wasn't used further in logic shown
                    file_book_ref = match_row["BOOK_ID"].values[0].strip()

                    # STEP 2: Fetch from CDW
                    xml_data = fetch_cdw_response(cdw_url)
                    if xml_data:
                        book_ref, book_type, err = extract_book_from_cdw(xml_data)
                        
                        if book_ref and book_type:
                            book_ref_in_cdw = f"{book_ref} {book_type}"
                            book_ref_in_db = f"{book_ref} {book_type}"
                            
                            # Logging the query string for the report (visual only)
                            query_used = f"SELECT DISTINCT {column} FROM {db}.{table} WHERE {column} IN ('{book_ref_in_db}')"
                            
                            # STEP 3: DB Validation
                            book_db_result = validate_book_in_db(server, db, table, column, book_ref_in_db)
                            comments = f"{book_ref_in_cdw} | BookRef DB Check: {book_db_result}"
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

            # Append results including the NEW COLUMN
            results.append({
                "FileType": file_type,
                "Filename": filepath,
                "TradeID": trade_id,
                "TradeFound": trade_found,
                "CDWURL": cdw_url,
                "BOOKREF_INCDW": book_ref_in_cdw,
                "BOOKFOUND": book_db_result,  # <--- NEW COLUMN ADDED HERE
                "Query_Used": query_used,
                "Comments": comments
            })

    # Save output
    pd.DataFrame(results).to_excel(output_file, index=False)
    print(f"Report generated: {output_file}")

if __name__ == "__main__":
    process_trades()
