from pymongo import MongoClient
import ssl

# HARDCODE YOUR DETAILS HERE FOR TESTING
TEST_HOST = "192.168.1.50"  # Replace with your actual Mongo Host IP
TEST_PORT = 27017           # Replace with actual Port
TEST_USER = "myUser"        # Replace with actual Username
TEST_PASS = "myPassword"    # Replace with actual Password
TEST_DB   = "admin"         # The DB where the user was created (usually 'admin')

print(f"--- Attempting Connection to {TEST_HOST} ---")

# 1. Construct URI
uri = f"mongodb://{TEST_USER}:{TEST_PASS}@{TEST_HOST}:{TEST_PORT}/?authSource={TEST_DB}"

try:
    # 2. Try Basic Connection
    print("Attempt 1: Standard Connection...")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    print(client.server_info())
    print("SUCCESS: Standard Connection worked!")

except Exception as e:
    print(f"FAILED: {e}")
    
    # 3. Try Enterprise SSL Connection (Most likely the fix)
    print("\nAttempt 2: Trying with SSL/TLS...")
    try:
        # allowInvalidCertificates=True is often needed for internal banking certificates
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, tls=True, tlsAllowInvalidCertificates=True)
        print(client.server_info())
        print("SUCCESS: SSL Connection worked!")
    except Exception as e2:
        print(f"FAILED SSL Attempt: {e2}")
