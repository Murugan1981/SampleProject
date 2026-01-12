import os
import json
import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = r"H:\LDNM1\SIMM\Final"
RUN_DATE = "20251128"

SIMM_FILE = f"MHBK_Sensitivities_{RUN_DATE}.txt"
PV_FILE = f"MHBK_MX_PV_{RUN_DATE}.csv"
MAPPING_FILE = "CRIF_LDN_IRD_Mapping.xlsx"
CDW_INPUT_FILE = "CRIF_LDN_IRD_Input.json"

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_FILE = f"CRIF_LDN_IRD_{RUN_DATE}.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)

AUTH = HttpNtlmAuth("USERNAME", "PASSWORD")  # placeholder

# =========================================================
# LOAD CDW CONFIG
# =========================================================

with open(os.path.join(BASE_DIR, CDW_INPUT_FILE)) as f:
    cfg = json.load(f)

ENV = cfg["env"]
TRADING_ENTITY = cfg["tradingEntity"]
CDW_BASE_URL = cfg[ENV]

# =========================================================
# LOAD SIMM FILE
# =========================================================

simm_df = pd.read_csv(
    os.path.join(BASE_DIR, SIMM_FILE),
    sep="|",
    low_memory=False
)

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

# =========================================================
# LOAD MAPPING
# =========================================================

mapping_df = pd.read_excel(os.path.join(BASE_DIR, MAPPING_FILE))

output_df = pd.DataFrame(index=simm_df.index)

# =========================================================
# CDW HELPER
# =========================================================

def call_cdw(url: str) -> str:
    r = requests.get(url, auth=AUTH, verify=False, timeout=30)
    r.raise_for_status()
    return r.text

# =========================================================
# SELECT-CASE HANDLERS
# =========================================================

def handle_cdw(row):
    results = []
    for trade_id in simm_df["TRADE_ID"]:
        url = f"{CDW_BASE_URL}{row['CDWURL'].format(trade_id=trade_id)}"
        results.append(call_cdw(url))
    return results


def handle_constant(row):
    return [row["ConstantValue"]] * len(simm_df)


def handle_blank(row):
    return [""] * len(simm_df)


def handle_file(row):
    return simm_df[row["SourceColumn"]]


def handle_cdwcombine(row):
    values = []

    for trade_id in simm_df["TRADE_ID"]:
        trade_url = (
            f"{CDW_BASE_URL}/{TRADING_ENTITY}/fpml/IntradayTrades/{trade_id}"
        )
        trade_xml = call_cdw(trade_url)

        party_id = trade_xml.split("<partyId>")[1].split("</partyId>")[0]

        le_url = (
            f"{CDW_BASE_URL}/{TRADING_ENTITY}/common/legalEntityClients/{party_id}"
        )
        le_xml = call_cdw(le_url)

        ccif = le_xml.split("<MIZUHO_CCIF_NO>")[1].split("</MIZUHO_CCIF_NO>")[0]
        values.append(ccif)

    return values


def handle_filecombine(row):
    pv_df = pd.read_csv(os.path.join(BASE_DIR, PV_FILE))
    pv_map = pv_df.set_index("MUREXROOTCONTRACTID")["LEGNPVUSD"].to_dict()
    return simm_df["MUREXROOTCONTRACTID"].map(pv_map)


# =========================================================
# SELECT-CASE DISPATCHER
# =========================================================

MAPPING_DISPATCH = {
    "CDW": handle_cdw,
    "CONSTANT": handle_constant,
    "BLANK": handle_blank,
    "FILE": handle_file,
    "CDWCOMBINE": handle_cdwcombine,
    "FILECOMBINE": handle_filecombine
}

# =========================================================
# MAIN LOOP
# =========================================================

for _, row in mapping_df.iterrows():

    target_col = row["TargetColumn"]
    mapping_type = row["MappingType"]

    if mapping_type not in MAPPING_DISPATCH:
        raise Exception(f"Unsupported MappingType: {mapping_type}")

    handler = MAPPING_DISPATCH[mapping_type]
    output_df[target_col] = handler(row)

# =========================================================
# WRITE OUTPUT
# =========================================================

output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
output_df.to_csv(output_path, index=False)

print(f"CRIF_LDN_IRD generated successfully: {output_path}")
