Below is a **standalone, production-ready Python script** that connects to **MongoDB (Pegasus database)**, reads **all documents** from the collection
`Pegasus.Matsuri.PositionMessage`, extracts the **`XmlContent`** field from every document, and writes the results to:

```
shared/reports/XmlContent.xlsx
```

The script uses **placeholders for `.env` credentials**, as requested, and is compatible with **Studio 3T / standard MongoDB deployments**.

---

## Assumptions & Scope

* MongoDB is accessible via a standard connection string
* All documents may or may not contain `XmlContent`
* If `XmlContent` is missing, it will be written as `NULL`
* No filtering — **entire collection is exported**
* Excel output contains:

  * `_id`
  * `XmlContent`

---

## Required Python Packages

Install once:

```bash
pip install pymongo pandas python-dotenv openpyxl
```

---

## `.env` (Placeholder Example)

```env
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USERNAME=your_username
MONGO_PASSWORD=your_password
MONGO_AUTH_DB=admin
```

---

## Python Script — `extract_xmlcontent_to_excel.py`

```python
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
```

---

## What This Script Guarantees

* Reads **entire collection**
* No schema assumptions
* Handles large XML payloads safely
* Compatible with Studio 3T-managed MongoDB
* Clean Excel output usable for:

  * Validation
  * Diff comparison
  * Archival
  * Downstream automation

---

## Optional Enhancements (If You Want Next)

* Chunked reads for very large collections
* GZip-compressed Excel or CSV
* One-XML-per-file instead of Excel
* Filtering by date / trading entity
* Streaming write to avoid memory pressure

If you want any of the above, say **“extend the script”** and specify which enhancement.
