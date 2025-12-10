import urllib.parse
from pymongo import MongoClient
import os

# 1. ENTER YOUR RAW CREDENTIALS HERE (Exactly as they are in Studio 3T)
#    (Do not manually replace @ with %40 here. Put the REAL password)
RAW_USERNAME = "myUser"       # Replace this
RAW_PASSWORD = "my@Password!" # Replace this (e.g., if it has @ or :)
HOST = "192.168.1.50"         # Replace this
PORT = 27017                  # Replace this
AUTH_DB = "admin"             # Usually 'admin'

print("--- Step 1: Escaping Credentials ---")

# 2. ESCAPE CREDENTIALS (The RFC 3986 Fix)
# This converts 'apple@123' -> 'apple%40123' automatically
username_safe = urllib.parse.quote_plus(RAW_USERNAME)
password_safe = urllib.parse.quote_plus(RAW_PASSWORD)

print(f"Original Password: {RAW_PASSWORD}")
print(f"Escaped Password:  {password_safe}")  # You should see % symbols here now

# 3. CONSTRUCT URI
# Notice we use the *_safe versions here
uri = f"mongodb://{username_safe}:{password_safe}@{HOST}:{PORT}/?authSource={AUTH_DB}"

print(f"\n--- Step 2: Connecting to {HOST} ---")
print(f"URI (masked): mongodb://{username_safe}:****@{HOST}:{PORT}/...")

try:
    # 4. ATTEMPT CONNECTION
    # tls=True is usually required for Enterprise Mongo
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, tls=True, tlsAllowInvalidCertificates=True)
    
    # Force a network call
    info = client.server_info()
    print("\n✅ SUCCESS! Connected successfully.")
    print(f"Server Version: {info.get('version')}")

except Exception as e:
    print("\n❌ FAILURE")
    print("Error Details:")
    print(e)
    
    print("\n--- Troubleshooting Tips ---")
    if "SSL" in str(e):
        print("1. Try setting tls=False in the MongoClient line above.")
    elif "Authentication failed" in str(e):
        print("1. Check if AUTH_DB is 'admin' or something else (like your specific database name).")
        print("2. Verify the username/password again in Studio 3T.")
