import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import urllib

# Helper function to flatten nested dictionaries
def flatten_dict(d, parent_key='', sep='_'):
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

# Load environment variables
load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")



# Database & Collection
DATABASE_NAME = "Pegasus"
COLLECTION_NAME = "Matsuri.PositionMessage"

# Output File
OUTPUT_DIR = os.path.join("shared", "reports")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "XmlContent.ndjson")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fetch Data from MongoDB
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
    print(f"\n Warning: Database '{DATABASE_NAME}' not found!")
else:
    print(f"\n✓ Database '{DATABASE_NAME}' exists")

# List all collections in the target database
db = client[DATABASE_NAME]
print("\n" + "="*50)
print(f"Available Collections in '{DATABASE_NAME}':")
print("="*50)
collection_list = db.list_collection_names()
# if collection_list:
#     for coll_name in collection_list:
#         coll_count = db[coll_name].count_documents({})
#         print(f"  - {coll_name} ({coll_count} documents)")
# else:
#     print("  No collections found in this database")

# Check if our target collection exists
if COLLECTION_NAME not in collection_list:
    print(f"\n⚠️  Warning: Collection '{COLLECTION_NAME}' not found!")
else:
    print(f"\n✓ Collection '{COLLECTION_NAME}' exists")

print(f"Attempting to fetch from: {DATABASE_NAME}.{COLLECTION_NAME}")
collection = db[COLLECTION_NAME]

print("Fetching documents (XmlContent only)...")
print("This may take a while for large collections...")

# Fetch only the XmlContent field
cursor = collection.find({}, {"_id": 1, "XmlContent": 1})

# Write to NDJSON file
document_count = 0

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    for doc in cursor:
        # Convert ObjectId to string
        doc["_id"] = str(doc["_id"])
        
        # Write each document as a JSON line
        f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        document_count += 1
        
        # Progress indicator every 10000 documents
        if document_count % 10000 == 0:
            print(f"  Processed {document_count} documents...")

client.close()

# Summary
if document_count > 0:
    print(f"\n✓ Extraction completed successfully.")
    print(f"✓ Total documents extracted: {document_count}")
    print(f"✓ Output file: {OUTPUT_FILE}")
    
    # Show file size
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"✓ File size: {file_size_mb:.2f} MB")
else:
    print("\n No documents found in the collection.")
