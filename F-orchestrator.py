import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os

def flatten_xml(element, parent_path="", flattened=None):
    """
    Recursively flattens XML nodes into key-path: value format.
    """
    if flattened is None:
        flattened = {}

    tag = element.tag.split("}")[-1]  # Remove namespace
    path = f"{parent_path}.{tag}" if parent_path else tag

    # If the element has text, save it
    text = element.text.strip() if element.text and element.text.strip() else None
    if text:
        flattened[path] = text

    # Recurse into children
    for child in element:
        flatten_xml(child, path, flattened)

    return flattened

def CDWscraper(base_url, trade_id, batch_date, output_excel):
    full_url = f"{base_url.rstrip('/')}/{trade_id}/{batch_date}"
    print(f"🔗 Fetching URL: {full_url}")

    try:
        # OPTIONAL: Add auth=(username, password)
        response = requests.get(full_url, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        return

    try:
        root = ET.fromstring(response.text)
        flat_data = flatten_xml(root)

        # Add trade identifiers
        flat_data["TRADE_ID"] = trade_id
        flat_data["BATCH_DATE"] = batch_date
        flat_data["CDW_URL"] = full_url
        flat_data["CDW_STATUS"] = "SUCCESS"

    except Exception as e:
        flat_data = {
            "TRADE_ID": trade_id,
            "BATCH_DATE": batch_date,
            "CDW_URL": full_url,
            "CDW_STATUS": f"XML_PARSE_FAIL: {e}"
        }

    # Write to Excel
    df_new = pd.DataFrame([flat_data])
    if os.path.exists(output_excel):
        df_existing = pd.read_excel(output_excel)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    os.makedirs(os.path.dirname(output_excel), exist_ok=True)
    df_combined.to_excel(output_excel, index=False)
    print(f"✅ Trade {trade_id} CDW data saved to: {output_excel}")
