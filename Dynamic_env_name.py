import os
import json
from dotenv import load_dotenv

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

TESTDATA_FILE = os.path.join("shared", "input", "ApiTestData.json")

# -------------------------------------------------
# Load configuration from ApiTestData.json
# -------------------------------------------------
if not os.path.exists(TESTDATA_FILE):
    raise FileNotFoundError(f"Config file not found: {TESTDATA_FILE}")

with open(TESTDATA_FILE, "r") as f:
    config = json.load(f)

print(f"Loaded config from {TESTDATA_FILE}")
print(f" System      : {config['System']}")
print(f" Env_Target  : {config['Env_Target']}")
print(f" Env_Source  : {config['Env_Source']}")
print(f" Region      : {config['Region']}")
print(f" URLTYPE     : {config['URLTYPE']}")

# -------------------------------------------------
# Build dynamic environment variable names
# -------------------------------------------------
source_var_name = f"{config['System']}_{config['Region']}_{config['Env_Source']}"
target_var_name = f"{config['System']}_{config['Region']}_{config['Env_Target']}"

SOURCE_DS = os.getenv(source_var_name)
TARGET_DS = os.getenv(target_var_name)

if SOURCE_DS is None:
    raise ValueError(f"Environment variable '{source_var_name}' not found in .env")
if TARGET_DS is None:
    raise ValueError(f"Environment variable '{target_var_name}' not found in .env")

print(f"\nLoaded datasources:")
print(f" {source_var_name} = {SOURCE_DS}")
print(f" {target_var_name} = {TARGET_DS}")
