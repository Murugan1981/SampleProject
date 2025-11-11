
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from auth import MONGO_USERNAME, MONGO_PASSWORD
from urllib.parse import quote_plus


# STEP 1: Read Input Excel File

input_file = r"./shared/input/input_trades_to_validate.xlsx"
sheet_name = "Sheet1"

print("Reading input Excel file...")
df = pd.read_excel(input_file, sheet_name=sheet_name)
print(f"Found {len(df)} rows to process\n")


# STEP 2: Prepare Result Container

validation_results = []


# STEP 3: Loop Through Each Row

for index, row in df.iterrows():
    # MongoDB connection details
    mongo_host = str(row["MongoHost"]).strip()  # e.g., localhost or cluster.mongodb.net
    mongo_port = str(row["MongoPort"]).strip() if pd.notna(row.get("MongoPort")) else "27017"
    database_name = str(row["DatabaseName"]).strip()
    collection_name = str(row["CollectionName"]).strip()
    field_name = str(row["FieldName"]).strip()
    trade_ids_str = str(row["TradeIDs"]).strip()
    
    # Optional: Auth database (default is usually 'admin')
    auth_db = str(row.get("AuthDatabase", "admin")).strip()

    # Validate required fields
    if not all([mongo_host, database_name, collection_name, field_name, trade_ids_str]):
        print(f"Skipping incomplete row at index {index}")
        continue

    # Parse trade IDs
    trade_ids = [t.strip() for t in trade_ids_str.split(",") if t.strip()]

    print(f"\n{'='*60}")
    print(f"Validating: {database_name}.{collection_name}")
    print(f"   Host: {mongo_host}:{mongo_port}")
    print(f"   Field: {field_name}")
    print(f"   Trade IDs: {len(trade_ids)}")
    print(f"{'='*60}")

    
    # STEP 4: Create MongoDB Connection String
    
    # URL-encode username and password to handle special characters
    username_encoded = quote_plus(MONGO_USERNAME)
    password_encoded = quote_plus(MONGO_PASSWORD)
    
    # Build connection string based on whether it's a local or Atlas cluster
    if "mongodb.net" in mongo_host or "mongodb+srv" in mongo_host:
        # MongoDB Atlas (SRV connection)
        connection_string = f"mongodb+srv://{username_encoded}:{password_encoded}@{mongo_host}/{auth_db}?retryWrites=true&w=majority"
    else:
        # Local MongoDB or standard connection
        connection_string = f"mongodb://{username_encoded}:{password_encoded}@{mongo_host}:{mongo_port}/{auth_db}?authSource={auth_db}"

    client = None
    try:
        # Connect to MongoDB
        print(f"🔌 Connecting to MongoDB...")
        client = MongoClient(
            connection_string,
            serverSelectionTimeoutMS=10000,  # 10 second timeout
            connectTimeoutMS=10000
        )
        
        # Test connection
        client.admin.command('ping')
        
        # Access database and collection
        db = client[database_name]
        collection = db[collection_name]
        
        print(f"Connected successfully!")

    except ConnectionFailure as e:
        print(f"Connection failed: {e}")
        for tid in trade_ids:
            validation_results.append({
                "MongoHost": mongo_host,
                "MongoPort": mongo_port,
                "DatabaseName": database_name,
                "CollectionName": collection_name,
                "FieldName": field_name,
                "TradeID": tid,
                "Status": f"CONNECTION ERROR: {e}"
            })
        continue
    except Exception as e:
        print(f"Unexpected error: {e}")
        for tid in trade_ids:
            validation_results.append({
                "MongoHost": mongo_host,
                "MongoPort": mongo_port,
                "DatabaseName": database_name,
                "CollectionName": collection_name,
                "FieldName": field_name,
                "TradeID": tid,
                "Status": f"ERROR: {e}"
            })
        continue

    
    # STEP 5: Validate Each Trade ID
    
    print(f"\n Validating trade IDs...")
    for trade_id in trade_ids:
        try:
            # Try to convert to int if it's numeric
            search_value = trade_id
            if trade_id.isdigit():
                search_value = int(trade_id)
            
            # Query MongoDB - check if document exists with this field value
            query = {field_name: search_value}
            document = collection.find_one(query, {"_id": 1})
            
            if document:
                status = "FOUND"
                
            else:
                status = "MISSING"
            
                
            print(f"  {icon} TradeID {trade_id}: {status}")
            
        except OperationFailure as e:
            status = f"QUERY ERROR: {e}"
            print(f"TradeID {trade_id}: {status}")
        except Exception as e:
            status = f"ERROR: {e}"
            print(f"TradeID {trade_id}: {status}")

        validation_results.append({
            "MongoHost": mongo_host,
            "MongoPort": mongo_port,
            "DatabaseName": database_name,
            "CollectionName": collection_name,
            "FieldName": field_name,
            "TradeID": trade_id,
            "Status": status
        })

    # Close connection
    if client:
        client.close()
        print(f"Connection closed")


# STEP 6: Export Validation Results

print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")

output_path = r"./shared/reports/trade_validation_report_mongo.xlsx"
result_df = pd.DataFrame(validation_results)

# Calculate statistics
total_trades = len(validation_results)
found_trades = len(result_df[result_df['Status'] == 'FOUND'])
missing_trades = len(result_df[result_df['Status'] == 'MISSING'])
error_trades = total_trades - found_trades - missing_trades

print(f"Total Trades Validated: {total_trades}")
print(f"  FOUND: {found_trades}")
print(f"  MISSING: {missing_trades}")
print(f"  ERRORS: {error_trades}")

result_df.to_excel(output_path, index=False)

print(f"\n Validation completed!")
print(f" Results saved to: {output_path}")
print(f"{'='*60}\n")
