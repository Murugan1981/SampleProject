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
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "XmlContent.xlsx")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------------------------
# Fetch Data from MongoDB
# -------------------------------------------------
print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)

db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

print("Fetching documents...")
cursor = collection.find({}, {"XmlContent": 1})

rows = []
for doc in cursor:
    rows.append({
        "_id": str(doc.get("_id")),
        "XmlContent": doc.get("XmlContent")
    })

client.close()

# -------------------------------------------------
# Write to Excel
# -------------------------------------------------
df = pd.DataFrame(rows)

print(f"Writing {len(df)} records to Excel...")
df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

print(f"Extraction completed successfully.")
print(f"Output file: {OUTPUT_FILE}")
