# ============================================================
# IMPORTS
# ============================================================

import os
# Used for file path handling and directory creation

import json
# Used to read CDW environment configuration

import pandas as pd
# Used for reading, filtering, and writing tabular data

import requests
# Used for making HTTP calls to CDW services

from dotenv import load_dotenv
# Used to load environment variables from .env file

from requests_ntlm import HttpNtlmAuth
# Used to enable NTLM authentication for CDW calls

from auth import get_password
# Used to securely retrieve password (company standard)


# ============================================================
# LOAD ENVIRONMENT VARIABLES & AUTHENTICATION
# ============================================================

load_dotenv()
# Loads variables from .env into runtime

USERNAME = os.getenv("USERNAME")
# Reads USERNAME from .env file

PASSWORD = get_password()
# Fetches password securely using auth.py

if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")
# Stops execution if credentials are missing

AUTH = HttpNtlmAuth(USERNAME, PASSWORD)
# Creates NTLM authentication object for CDW requests


# ============================================================
# STATIC CONFIGURATION
# ============================================================

BASE_DIR = r"H:\LDNM1\SIMM\Final"
# Root directory containing all SIMM input/output files

RUN_DATE = "20251128"
# Business date used to resolve filenames

SIMM_FILE = f"MHBK_Sensitivities_{RUN_DATE}.txt"
# SIMM sensitivities input file

PV_FILE = f"MHBK_MX_PV_{RUN_DATE}.csv"
# PV reference file for FILECOMBINE logic

MAPPING_FILE = "CRIF_LDN_IRD_Mapping.xlsx"
# Mapping definition file

CDW_INPUT_FILE = "CRIF_LDN_IRD_Input.json"
# CDW environment configuration file

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
# Directory where output file will be written

OUTPUT_FILE = f"CRIF_LDN_IRD_{RUN_DATE}.csv"
# Final CRIF output file name

os.makedirs(OUTPUT_DIR, exist_ok=True)
# Ensures output directory exists before writing file


# ============================================================
# LOAD CDW ENVIRONMENT CONFIGURATION
# ============================================================

with open(os.path.join(BASE_DIR, CDW_INPUT_FILE), "r") as f:
    cfg = json.load(f)
# Loads CDW config JSON into dictionary

ENV = cfg["env"]
# Current environment (e.g., SIT6)

TRADING_ENTITY = cfg["tradingEntity"]
# Trading entity (e.g., mhbk)

CDW_BASE_URL = cfg[ENV]
# Resolves environment-specific CDW base URL


# ============================================================
# LOAD AND FILTER SIMM SENSITIVITIES FILE
# ============================================================

simm_df = pd.read_csv(
    os.path.join(BASE_DIR, SIMM_FILE),
    sep="|",
    low_memory=False
)
# Loads SIMM sensitivities file into DataFrame

simm_df = simm_df[
    (simm_df["MUREXPRODUCTFAMILY"] == "IRD") &
    (simm_df["SENSITIVITY"].isin([
        "Risk_IRVol",
        "Risk_Inflation",
        "Risk_FX",
        "Risk_IRCurve",
        "Risk_XCcyBasis"
    ]))
].reset_index(drop=True)
# Filters SIMM rows exactly as per algorithm conditions


# ============================================================
# LOAD MAPPING FILE AND PREPARE OUTPUT DATAFRAME
# ============================================================

mapping_df = pd.read_excel(os.path.join(BASE_DIR, MAPPING_FILE))
# Loads CRIF mapping rules

output_df = pd.DataFrame(index=simm_df.index)
# Initializes output DataFrame with same row count as SIMM data


# ============================================================
# CDW CALL HELPER (SINGLE SOURCE OF TRUTH)
# ============================================================

def call_cdw(url: str) -> str:
    """
    Makes a NTLM-authenticated call to a CDW endpoint
    """
    response = requests.get(
        url,
        auth=AUTH,
        verify=False,
        timeout=30
    )
    # Performs authenticated GET request

    response.raise_for_status()
    # Raises exception if CDW returns HTTP error

    return response.text
    # Returns raw response payload


# ============================================================
# SELECT-CASE HANDLER FUNCTIONS
# ============================================================

def handle_cdw(row):
    """Handles CDW mapping type"""
    values = []
    # Stores one value per SIMM row

    for trade_id in simm_df["TRADE_ID"]:
        # Iterates through all SIMM trade IDs

        url = f"{CDW_BASE_URL}{row['CDWURL'].format(trade_id=trade_id)}"
        # Builds CDW URL dynamically

        values.append(call_cdw(url))
        # Calls CDW and appends response

    return values
    # Returns populated column values


def handle_constant(row):
    """Handles CONSTANT mapping type"""
    return [row["ConstantValue"]] * len(simm_df)
    # Repeats constant value for all rows


def handle_blank(row):
    """Handles BLANK mapping type"""
    return [""] * len(simm_df)
    # Returns empty string for all rows


def handle_file(row):
    """Handles FILE mapping type"""
    return simm_df[row["SourceColumn"]]
    # Copies column directly from SIMM file


def handle_cdwcombine(row):
    """Handles CDWCOMBINE mapping type"""
    results = []
    # Stores combined CDW values

    for trade_id in simm_df["TRADE_ID"]:
        # Processes each trade separately

        trade_url = f"{CDW_BASE_URL}/{TRADING_ENTITY}/fpml/IntradayTrades/{trade_id}"
        # Builds IntradayTrades endpoint

        trade_xml = call_cdw(trade_url)
        # Fetches trade XML

        party_id = trade_xml.split("<partyId>")[1].split("</partyId>")[0]
        # Extracts COUNTERPARTY partyId

        le_url = f"{CDW_BASE_URL}/{TRADING_ENTITY}/common/legalEntityClients/{party_id}"
        # Builds legal entity lookup URL

        le_xml = call_cdw(le_url)
        # Fetches legal entity XML

        ccif = le_xml.split("<MIZUHO_CCIF_NO>")[1].split("</MIZUHO_CCIF_NO>")[0]
        # Extracts MIZUHO_CCIF_NO

        results.append(ccif)
        # Appends final combined value

    return results
    # Returns combined column data


def handle_filecombine(row):
    """Handles FILECOMBINE mapping type"""
    pv_df = pd.read_csv(os.path.join(BASE_DIR, PV_FILE))
    # Loads PV reference file

    pv_map = pv_df.set_index("MUREXROOTCONTRACTID")["LEGNPVUSD"].to_dict()
    # Converts PV data to lookup dictionary

    return simm_df["MUREXROOTCONTRACTID"].map(pv_map)
    # Maps PV values to SIMM rows


# ============================================================
# SELECT-CASE DISPATCH TABLE
# ============================================================

MAPPING_DISPATCH = {
    "CDW": handle_cdw,
    "CONSTANT": handle_constant,
    "BLANK": handle_blank,
    "FILE": handle_file,
    "CDWCOMBINE": handle_cdwcombine,
    "FILECOMBINE": handle_filecombine
}
# Maps mapping type → corresponding handler function


# ============================================================
# MAIN EXECUTION LOOP
# ============================================================

for _, row in mapping_df.iterrows():
    # Iterates through each mapping rule

    target_column = row["TargetColumn"]
    # Determines output column name

    mapping_type = row["MappingType"]
    # Determines which logic to apply

    if mapping_type not in MAPPING_DISPATCH:
        raise Exception(f"Unsupported MappingType: {mapping_type}")
    # Prevents silent failures

    output_df[target_column] = MAPPING_DISPATCH[mapping_type](row)
    # Executes selected mapping logic


# ============================================================
# WRITE FINAL OUTPUT FILE
# ============================================================

output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
# Builds final output path

output_df.to_csv(output_path, index=False)
# Writes CRIF output CSV file

print(f"CRIF_LDN_IRD generated successfully: {output_path}")
# Prints success message
