import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# ================= PORTS & ADAPTERS (Settings) =================
# Base Directories
BASE_DIR = os.getcwd()

# Shared Directory Structure
SHARED_DIR = os.path.join(BASE_DIR, "shared")
INPUT_DIR = os.path.join(SHARED_DIR, "input")       # For manual inputs (Inclusion/Exclusion)
RAW_DIR = os.path.join(SHARED_DIR, "raw")           # For fetched extraction data
RESPONSE_DIR = os.path.join(SHARED_DIR, "response") # For raw API JSON
REPORT_DIR = os.path.join(SHARED_DIR, "reports")    # For final Excel reports

# Input File Paths (Updated to point to shared/input)
INCLUSION_FILE = os.path.join(INPUT_DIR, "TestConditionInclusion.xlsx")
EXCLUSION_FILE = os.path.join(INPUT_DIR, "TestConditionExclusion.xlsx")
# Note: Ensure EndPoint_TestCondition.xlsx (manual plan) is also looked for in INPUT_DIR if used.
MANUAL_TEST_PLAN = os.path.join(INPUT_DIR, "EndPoint_TestCondition.xlsx")

# Output File Paths
EXTRACTED_ENDPOINTS = os.path.join(RAW_DIR, "Extraction.xlsx")
MASTER_JSON_REPORT = os.path.join(RESPONSE_DIR, "DataService.json")
FINAL_EXCEL_REPORT = os.path.join(REPORT_DIR, "Final_Comparison_Report.xlsx")

# URLs
DEV_URL = os.getenv("SOURCE_URL") 
PROD_URL = os.getenv("TARGET_URL")

def init_environment():
    """Creates the shared directory and its subfolders."""
    # Ensure all specific subdirectories exist before writing
    # Note: We create INPUT_DIR too, in case it's missing, though files should be there.
    for p in [INPUT_DIR, RAW_DIR, RESPONSE_DIR, REPORT_DIR]:
        os.makedirs(p, exist_ok=True)
    print(f"Environment initialized in: {SHARED_DIR}")
