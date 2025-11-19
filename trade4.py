import requests
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv
from auth import get_password
import subprocess

# Load credentials
load_dotenv()
USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

def fetch_cdw_response(url):
    """Uses curl to fetch CDW XML content"""
    try:
        command = [
            'curl', '-u', f'{USERNAME}:{PASSWORD}',
            '-H', 'Accept: application/xml',
            '-H', 'Content-Type:application/xml',
            url
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode == 0:
            return result.stdout
        else:
            print(f"[ERROR] Curl failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"[EXCEPTION] {e}")
        return None

def extract_trade_status(xml_text):
    try:
        root = ET.fromstring(xml_text)

        # Extract <trade-status>
        trade_status_node = root.find('.//trade-status')
        trade_settlement_node = root.find('.//trade-settlement-status')

        trade_status = trade_status_node.text.strip() if trade_status_node is not None else "NOT_FOUND"
        trade_settlement_status = trade_settlement_node.text.strip() if trade_settlement_node is not None else "NOT_FOUND"

        return trade_status, trade_settlement_status, "Success"
    except Exception as e:
        return None, None, f"XML Parse Error: {e}"

# Main function to test
if __name__ == "__main__":
    # SAMPLE input (from your screenshot)
    trade_id = "58707184"
    reporting_date = "2025-07-01"
    cdw_base_url = "https://svc-sit2-cdw/mbe/fpml/eodTrades"
    full_url = f"{cdw_base_url}/{trade_id}?on={reporting_date}"

    print(f"Fetching CDW URL: {full_url}")
    xml_content = fetch_cdw_response(full_url)

    if xml_content:
        status, settlement_status, result = extract_trade_status(xml_content)
        print("✅ Trade Status:", status)
        print("✅ Trade Settlement Status:", settlement_status)
        print("✅ Result:", result)
    else:
        print("❌ Failed to load CDW URL.")
