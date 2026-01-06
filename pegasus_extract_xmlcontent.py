import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# -------------------------------------------------
# Helper function to flatten nested dictionaries
# -------------------------------------------------
def flatten_dict(d, parent_key='', sep='_'):
    """
    Flattens a nested dictionary.
    Example: {"a": {"b": 1}} becomes {"a_b": 1}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to string representation
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")

# -------------------------------------------------
# MongoDB Connection String
# -------------------------------------------------
if MONGO_USERNAME and MONGO_PASSWORD:
    MONGO_URI = (
        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}"
        f"@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_DB}"
    )
else:
    # For local / no-auth environments
    MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

# -------------------------------------------------
# Database & Collection
# -------------------------------------------------
DATABASE_NAME = "Pegasus"
COLLECTION_NAME = "Pegasus.Matsuri.PositionMessage"

# -------------------------------------------------
# Output File
# -------------------------------------------------
OUTPUT_DIR = os.path.join("shared", "reports")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "AllData.xlsx")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------
# Fetch Data from MongoDB
# -------------------------------------------------
print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)

# List all available databases
print("\n" + "="*50)
print("Available Databases:")
print("="*50)
db_list = client.list_database_names()
for db_name in db_list:
    print(f"  - {db_name}")

# Check if our target database exists
if DATABASE_NAME not in db_list:
    print(f"\n⚠️  Warning: Database '{DATABASE_NAME}' not found!")
else:
    print(f"\n✓ Database '{DATABASE_NAME}' exists")

# List all collections in the target database
db = client[DATABASE_NAME]
print("\n" + "="*50)
print(f"Available Collections in '{DATABASE_NAME}':")
print("="*50)
collection_list = db.list_collection_names()
if collection_list:
    for coll_name in collection_list:
        coll_count = db[coll_name].count_documents({})
        print(f"  - {coll_name} ({coll_count} documents)")
else:
    print("  No collections found in this database")

# Check if our target collection exists
if COLLECTION_NAME not in collection_list:
    print(f"\n⚠️  Warning: Collection '{COLLECTION_NAME}' not found!")
    print(f"\nDid you mean one of these collections?")
    for coll_name in collection_list:
        print(f"  - {coll_name}")
else:
    print(f"\n✓ Collection '{COLLECTION_NAME}' exists")

print("\n" + "="*50)
print(f"Attempting to fetch from: {DATABASE_NAME}.{COLLECTION_NAME}")
print("="*50)

collection = db[COLLECTION_NAME]

print("Fetching documents...")
cursor = collection.find({})

rows = []
for doc in cursor:
    # Convert ObjectId to string for _id field
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    
    # Flatten the document (handles nested fields)
    flattened_doc = flatten_dict(doc)
    rows.append(flattened_doc)

client.close()

# -------------------------------------------------
# Write to Excel
# -------------------------------------------------
if rows:
    df = pd.DataFrame(rows)
    print(f"\n✓ Writing {len(df)} records to Excel...")
    print(f"✓ Columns found: {list(df.columns)}")
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"\n✓ Extraction completed successfully.")
    print(f"✓ Output file: {OUTPUT_FILE}")
else:
    print("\n❌ No documents found in the collection.")
    print("\nPossible reasons:")
    print("  1. Collection name is incorrect")
    print("  2. Collection is empty")
    print("  3. Database name is incorrect")
    print("  4. Connection authentication issue")
