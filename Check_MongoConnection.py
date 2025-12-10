from urllib.parse import quote_plus
import ast
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os
import urllib.parse
from bson import json_util
import copy  # <--- Added this import for the fix

# Load environment variables
load_dotenv()
MONGO_USERNAME = os.getenv("MONGOUSERNAME")
MONGO_PASSWORD = os.getenv("MONGOPASSWORD")
encoded_user = urllib.parse.quote_plus(MONGO_USERNAME)
encoded_pass = urllib.parse.quote_plus(MONGO_PASSWORD)

# Input and Output Paths
input_file = r"./shared/input/pegasus_mongo_validation_input.xlsx"
sheet_name = "MongoValidationInput"
output_file = "./shared/reports/mongo_dynamic_validation_report.xlsx"

# Read Excel
df = pd.read_excel(input_file, sheet_name=sheet_name)
validation_results = []

# Process Each Row
for index, row in df.iterrows():
    mongo_host = row.get("MongoHost")
    mongo_port = row.get("MongoPort")
    db_name = row.get("DatabaseName")
    collection_name = row.get("CollectionName")

    print(f"Processing row {index+1}: {db_name}.{collection_name} on {mongo_host}:{mongo_port}")

    # Connect to MongoDB
    try:
        # Use +srv if required, or standard connection string
        mongo_uri = f"mongodb+srv://{encoded_user}:{encoded_pass}@{mongo_host}"
        # Alternative: mongo_uri = f"mongodb://{encoded_user}:{encoded_pass}@{mongo_host}:{mongo_port}/"
        
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        print(f"Connection error: {e}")
        continue

    # Build query dynamically
    query_conditions = []
    col_names = df.columns.tolist()

    for i in range(1, 50):
        field_col = f"Field{i}"
        op_col = f"Operator{i}"
        val_col = f"Value{i}"

        if field_col in col_names and op_col in col_names and val_col in col_names:
            field = str(row.get(field_col)).strip()
            operator = str(row.get(op_col)).strip().lower()
            val_raw = row.get(val_col)
            
            if pd.isna(field) or pd.isna(operator) or pd.isna(val_raw):
                continue

            # Parse ISODate or normal value
            try:
                if isinstance(val_raw, str) and val_raw.startswith("ISODate"):
                    iso_str = val_raw.replace("ISODate(", "").rstrip(")").strip("'").strip('"')
                    val = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
                else:
                    val = ast.literal_eval(str(val_raw))
            except:
                val = str(val_raw)
            
            # Build condition
            condition = {}
            if operator == "eq":
                condition = {field: val}
            elif operator == "ne":
                condition = {field: {"$ne": val}}
            elif operator == "in":
                if isinstance(val, str):
                    val = [v.strip() for v in val.split(",")]
                condition = {field: {"$in": val}}
            elif operator == "nin":
                if isinstance(val, str):
                    val = [v.strip() for v in val.split(",")]
                condition = {field: {"$nin": val}}
            elif operator == "gt":
                condition = {field: {"$gt": val}}
            elif operator == "gte":
                condition = {field: {"$gte": val}}
            elif operator == "lt":
                condition = {field: {"$lt": val}}
            elif operator == "lte":
                condition = {field: {"$lte": val}}
            else:
                print(f"Unsupported operator '{operator}' - skipping.")
                continue

            query_conditions.append(condition)

    if not query_conditions:
        print("No valid conditions found - skipping row.")
        continue

    # Initial Combined Query (before splitting $in)
    mongo_query = {"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
    print(f"Initial mongo_query : {mongo_query}")

    # Check for $in operator to split validation per item
    in_fields = [q for q in query_conditions if isinstance(q, dict) and list(q.values())[0] and isinstance(list(q.values())[0], dict) and "$in" in list(q.values())[0]]

    if in_fields:
        in_field = list(in_fields[0].keys())[0]
        trade_list = list(in_fields[0].values())[0]["$in"]

        # Build base query excluding the $in field
        base_conditions = [q for q in query_conditions if in_field not in q]
        base_query = {"$and": base_conditions} if base_conditions else {}

        for trade in trade_list:
            # FIX: Use deepcopy to prevent list mutation across iterations
            per_trade_query = copy.deepcopy(base_query)

            if "$and" in per_trade_query:
                per_trade_query["$and"].append({in_field: trade})
            else:
                # If base_query was empty or simple, we need to handle it carefully
                if not per_trade_query:
                     per_trade_query = {in_field: trade}
                else:
                    per_trade_query[in_field] = trade

            try:
                found = collection.count_documents(per_trade_query) > 0
                print(f"trade : {trade} - Status : {found}")
                
                validation_results.append({
                    "Database": db_name,
                    "Collection": collection_name,
                    "QueryField": in_field,
                    "TradeValue": trade,
                    "Query": str(per_trade_query),
                    "Status": "FOUND" if found else "MISSING"
                })
            except Exception as e:
                validation_results.append({
                    "Database": db_name,
                    "Collection": collection_name,
                    "QueryField": in_field,
                    "TradeValue": trade,
                    "Query": str(per_trade_query),
                    "Status": f"ERROR: {e}"
                })
    else:
        # Standard query without splitting $in
        try:
            found = collection.count_documents(mongo_query) > 0
            validation_results.append({
                "Database": db_name,
                "Collection": collection_name,
                "QueryField": "N/A",
                "TradeValue": "N/A",
                "Query": str(mongo_query),
                "Status": "FOUND" if found else "MISSING"
            })
        except Exception as e:
            validation_results.append({
                "Database": db_name,
                "Collection": collection_name,
                "QueryField": "N/A",
                "TradeValue": "N/A",
                "Query": str(mongo_query),
                "Status": f"ERROR: {e}"
            })

# Final Export
df_out = pd.DataFrame(validation_results)
df_out["RunTimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_out.to_excel(output_file, index=False)
print(f"Report generated -> {output_file}")
