import logging
from pymongo import MongoClient
from urllib.parse import quote_plus

# 1. SETUP DEBUG LOGGING (This will show us WHY the handshake fails)
#    This prints the internal "conversation" between Python and Mongo
logging.basicConfig()
logger = logging.getLogger('pymongo')
logger.setLevel(logging.DEBUG)

# 2. CONFIGURATION
HOST_IP = "192.168.1.50"   # Use the IP that replied to your Ping
PORT = 27017               # Usually 27017

# Creds
RAW_USER = "myUser"
RAW_PASS = "myPassword"

# Safe Encode
user_safe = quote_plus(RAW_USER)
pass_safe = quote_plus(RAW_PASS)

# 3. URI CONSTRUCTION
# We force the 'directConnection=true' flag here to stop it looking for other nodes
uri = f"mongodb://{user_safe}:{pass_safe}@{HOST_IP}:{PORT}/?authSource=admin&directConnection=true"

print(f"Connecting to: {HOST_IP} with SSL...")

try:
    # 4. THE CRITICAL FIX: SSL SETTINGS
    # tls=True                     -> "Speak the encrypted language"
    # tlsAllowInvalidCertificates  -> "Don't check if the ID card is official" (Fixes self-signed cert errors)
    
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5000,
        tls=True,    
        tlsAllowInvalidCertificates=True  
    )
    
    # 5. TEST
    info = client.server_info()
    print("\n✅ SUCCESS! Connected with SSL.")
    print(f"Version: {info.get('version')}")

except Exception as e:
    print("\n❌ FAILURE DETAILS")
    print(e)
