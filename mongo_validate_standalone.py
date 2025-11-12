import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv
import os
import urllib.parse
from pymongo.errors import ConnectionFailure, ConfigurationError
import json

# Load .env file
load_dotenv()

# Load credentials
MONGO_USERNAME = os.getenv("MONGOUSERNAME")
MONGO_PASSWORD = os.getenv("MONGOPASSWORD")
encoded_user = urllib.parse.quote_plus(MONGO_USERNAME)
encoded_pass = urllib.parse.quote_plus(MONGO_PASSWORD)

# STEP 1: Read input Excel
input_file = r"./shared/input/pegasus_mongo_validation_input.xlsx"
sheet_name = "MongoValidationInput"
df = pd.read_excel(input_file, sheet_name=sheet_name)

validation_results = []

def smart_cast(value_str):
    """
    Intelligently cast a string value to the appropriate type.
    Returns the value in priority order: int -> float -> datetime -> string
    """
    value_str = str(value_str).strip()
    
    # Check for NaN/empty
    if not value_str or value_str.lower() in ['nan', 'none', 'null', '']:
        return None
    
    # Try integer
    if value_str.isdigit() or (value_str.startswith('-') and value_str[1:].isdigit()):
        return int(value_str)
    
    # Try float
    try:
        return float(value_str)
    except ValueError:
        pass
    
    # Try ISO datetime
    if 'T' in value_str or '-' in value_str:
        try:
            return parser.isoparse(value_str)
        except (ValueError, parser.ParserError):
            pass
    
    # Return as string
    return value_str

# STEP 2: Loop through each row of Excel
for index, row in df.iterrows():
    mongo_host = str(row.get("mongoHost")).strip()
    mongo_port = str(row.get("MongoPort")).strip()
    db_name = str(row.get("DatabaseName")).strip()
    collection_name = str(row.get("CollectionName")).strip()

    print(f"\n🔍 Processing row {index+1}: {db_name}.{collection_name} on {mongo_host}")

    # STEP 3: Build dynamic query dictionary
    query_conditions = []
    col_names = df.columns.tolist()

    for i in range(1, 50):  # Support up to 50 dynamic conditions
        field_col = f"Field{i}"
        op_col = f"Operator{i}"
        val_col = f"Value{i}"

        if field_col in col_names and op_col in col_names and val_col in col_names:
            field = str(row.get(field_col)).strip()
            op = str(row.get(op_col)).strip().lower()
            val_raw = str(row.get(val_col)).strip()

            # Skip if field is empty/nan
            if not field or field.lower() in ['nan', 'none']:
                continue
            
            # Skip if value is empty/nan
            if not val_raw or val_raw.lower() in ['nan', 'none']:
                print(f"⚠️ Skipping {field} - value is empty/NaN")
                continue

            # Parse value(s)
            if op in ["in", "nin"]:
                values = []
                for v in val_raw.split(","):
                    casted = smart_cast(v)
                    if casted is not None:
                        values.append(casted)
                
                if not values:
                    print(f"⚠️ No valid values for {field} with operator {op}")
                    continue
            else:
                values = smart_cast(val_raw)
                if values is None:
                    print(f"⚠️ Skipping {field} - could not parse value: {val_raw}")
                    continue

            # Build condition
            if op == "eq":
                condition = {field: values}
            elif op == "ne":
                condition = {field: {"$ne": values}}
            elif op == "in":
                condition = {field: {"$in": values}}
            elif op == "nin":
                condition = {field: {"$nin": values}}
            elif op == "gt":
                condition = {field: {"$gt": values}}
            elif op == "gte":
                condition = {field: {"$gte": values}}
            elif op == "lt":
                condition = {field: {"$lt": values}}
            elif op == "lte":
                condition = {field: {"$lte": values}}
            else:
                print(f"⚠️ Unsupported operator '{op}' for field '{field}' — skipping.")
                continue

            print(f"✓ Condition: {field} {op} {values} → {condition}")
            query_conditions.append(condition)

    if not query_conditions:
        print(f"⚠️ No valid query conditions found in row {index+1}, skipping.")
        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port,
            "DatabaseName": db_name,
            "CollectionName": collection_name,
            "Query": "N/A",
            "DocumentsFound": "N/A",
            "Status": "Error: No valid query conditions"
        })
        continue

    # STEP 4: Build mongo_query
    flattened_query = {}
    has_duplicate_keys = False
    for cond in query_conditions:
        for key in cond:
            if key in flattened_query:
                has_duplicate_keys = True
                break
            flattened_query[key] = cond[key]

    mongo_query = {"$and": query_conditions} if has_duplicate_keys else flattened_query
    print(f"📋 Final Query: {mongo_query}")

    try:
        # STEP 5: Connect to MongoDB
        if ".mongodb.net" in mongo_host:
            mongo_uri = f"mongodb+srv://{encoded_user}:{encoded_pass}@{mongo_host}/?retryWrites=true&w=majority"
        else:
            mongo_uri = f"mongodb://{encoded_user}:{encoded_pass}@{mongo_host}:{mongo_port}/"

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        db = client[db_name]
        collection = db[collection_name]

        # STEP 6: Execute query
        print(f"🔎 Executing query on {db_name}.{collection_name}...")
        results = list(collection.find(mongo_query, {"_id": 0}).limit(100))  # Limit for safety
        found_count = collection.count_documents(mongo_query)  # Accurate count

        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port,
            "DatabaseName": db_name,
            "CollectionName": collection_name,
            "Query": json.dumps(mongo_query, ensure_ascii=False, default=str),
            "DocumentsFound": found_count,
            "Status": "Found" if found_count > 0 else "Missing"
        })

        print(f"✅ Found {found_count} document(s).")
        
        # Debug: Show first result if any
        if results:
            print(f"📄 Sample document: {json.dumps(results[0], indent=2, default=str)[:500]}...")

    except Exception as e:
        print(f"❌ Error processing row {index+1}: {e}")
        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port,
            "DatabaseName": db_name,
            "CollectionName": collection_name,
            "Query": json.dumps(mongo_query, ensure_ascii=False, default=str),
            "DocumentsFound": "N/A",
            "Status": f"Error: {e}"
        })
        continue
    finally:
        try:
            client.close()
        except:
            pass

# STEP 7: Export to Excel
output_file = r"./shared/reports/mongo_dynamic_validation_report.xlsx"
df_results = pd.DataFrame(validation_results)
df_results["RunTimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_results.to_excel(output_file, index=False)
print(f"\n📊 Report generated successfully → {output_file}")
