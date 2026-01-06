import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

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
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

print("Fetching documents...")
# Remove the projection to get ALL fields
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
# Write to Excel
# -------------------------------------------------
if rows:
    df = pd.DataFrame(rows)
    print(f"Writing {len(df)} records to Excel...")
    print(f"Columns found: {list(df.columns)}")
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"Extraction completed successfully.")
    print(f"Output file: {OUTPUT_FILE}")
else:
    print("No documents found in the collection.")
