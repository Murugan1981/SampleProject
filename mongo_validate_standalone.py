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

# Convert all column names to string and strip whitespace
df.columns = df.columns.astype(str).str.strip()

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

def get_excel_value(row, column_name):
    """Safely get value from Excel, handling NaN and converting to proper type"""
    val = row.get(column_name)
    if pd.isna(val):
        return None
    return str(val).strip()

# STEP 2: Loop through each row of Excel
for index, row in df.iterrows():
    # Use safe getter for connection details
    mongo_host = get_excel_value(row, "mongoHost")
    mongo_port = get_excel_value(row, "MongoPort")
    db_name = get_excel_value(row, "DatabaseName")
    collection_name = get_excel_value(row, "CollectionName")

    # Validate required fields
    if not mongo_host or not db_name or not collection_name:
        print(f"\n❌ Row {index+1}: Missing required fields (mongoHost, DatabaseName, or CollectionName)")
        validation_results.append({
            "mongoHost": mongo_host or "N/A",
            "MongoPort": mongo_port or "N/A",
            "DatabaseName": db_name or "N/A",
            "CollectionName": collection_name or "N/A",
            "Query": "N/A",
            "DocumentsFound": "N/A",
            "Status": "Error: Missing required connection details"
        })
        continue

    print(f"\n🔍 Processing row {index+1}: {db_name}.{collection_name} on {mongo_host}")

    # STEP 3: Build dynamic query dictionary
    query_conditions = []
    col_names = df.columns.tolist()

    for i in range(1, 50):  # Support up to 50 dynamic conditions
        field_col = f"Field{i}"
        op_col = f"Operator{i}"
        val_col = f"Value{i}"

        if field_col in col_names and op_col in col_names and val_col in col_names:
            # IMPORTANT: Preserve original case from Excel!
            field = get_excel_value(row, field_col)
            op = get_excel_value(row, op_col)
            val_raw = get_excel_value(row, val_col)

            # Skip if field is empty/nan
            if not field:
                continue
            
            # Skip if operator is empty/nan
            if not op:
                continue
            
            # Skip if value is empty/nan
            if not val_raw:
                print(f"⚠️ Skipping {field} - value is empty/NaN")
                continue

            op = op.lower()  # Only lowercase the operator

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

            print(f"✓ Condition: {field} {op} {values} (type: {type(values).__name__})")
            print(f"  → {condition}")
            query_conditions.append(condition)

    if not query_conditions:
        print(f"⚠️ No valid query conditions found in row {index+1}, skipping.")
        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port or "N/A",
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
        if has_duplicate_keys:
            break

    mongo_query = {"$and": query_conditions} if has_duplicate_keys else flattened_query
    print(f"📋 Final Query: {mongo_query}")

    try:
        # STEP 5: Connect to MongoDB
        if ".mongodb.net" in mongo_host:
            mongo_uri = f"mongodb+srv://{encoded_user}:{encoded_pass}@{mongo_host}/?retryWrites=true&w=majority"
            print(f"🔗 Connecting to Atlas: {mongo_host}")
        else:
            if not mongo_port:
                mongo_port = "27017"  # Default port
            mongo_uri = f"mongodb://{encoded_user}:{encoded_pass}@{mongo_host}:{mongo_port}/"
            print(f"🔗 Connecting to: {mongo_host}:{mongo_port}")

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        print(f"✅ Connected successfully")
        
        db = client[db_name]
        collection = db[collection_name]

        # STEP 6: Execute query
        print(f"🔎 Executing query on {db_name}.{collection_name}...")
        
        # Get accurate count first
        found_count = collection.count_documents(mongo_query)
        print(f"✅ Found {found_count} document(s).")
        
        # Get sample documents if found
        if found_count > 0:
            results = list(collection.find(mongo_query, {"_id": 0}).limit(3))
            print(f"📄 Sample document fields: {list(results[0].keys())}")
            print(f"📄 Sample document (first 500 chars): {json.dumps(results[0], indent=2, default=str)[:500]}...")

        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port or "N/A",
            "DatabaseName": db_name,
            "CollectionName": collection_name,
            "Query": json.dumps(mongo_query, ensure_ascii=False, default=str),
            "DocumentsFound": found_count,
            "Status": "Found" if found_count > 0 else "Missing"
        })

    except Exception as e:
        print(f"❌ Error processing row {index+1}: {e}")
        import traceback
        print(traceback.format_exc())
        
        validation_results.append({
            "mongoHost": mongo_host,
            "MongoPort": mongo_port or "N/A",
            "DatabaseName": db_name,
            "CollectionName": collection_name,
            "Query": json.dumps(mongo_query, ensure_ascii=False, default=str) if query_conditions else "N/A",
            "DocumentsFound": "N/A",
            "Status": f"Error: {str(e)[:200]}"
        })
    finally:
        try:
            client.close()
        except:
            pass

# STEP 7: Export to Excel
output_file = r"./shared/reports/mongo_dynamic_validation_report.xlsx"
df_results = pd.DataFrame(validation_results)
df_results["RunTimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Ensure output directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

df_results.to_excel(output_file, index=False)
print(f"\n📊 Report generated successfully → {output_file}")
print(f"\n📈 Summary:")
print(f"  Total rows processed: {len(df_results)}")
print(f"  Found: {len(df_results[df_results['Status'] == 'Found'])}")
print(f"  Missing: {len(df_results[df_results['Status'] == 'Missing'])}")
print(f"  Errors: {len(df_results[df_results['Status'].str.startswith('Error', na=False)])}")
