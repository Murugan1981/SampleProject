# script2_recreate_crif_ird.py
# -----------------------------------------
# Re-create CRIF_LDN_IRD_YYYYMMDD.csv
# -----------------------------------------

import os
import json
import time
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth
import urllib3
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# HARD-CODED PATHS (NO AUTO-DETECTION)
# ============================================================
BASE_DIR = Path(r"H:\LDNM1\SIMM\Final")

SIMM_FILE = BASE_DIR / "MHBK_Sensitivities_YYYYMMDD.txt"
PV_FILE   = BASE_DIR / "MHBK_MX_PV_YYYYMMDD.csv"
CSA_FILE  = BASE_DIR / "CSA_COUNTERPARTY_INFO_ALL_YYYYMMDD.csv"

OUTPUT_FILE = BASE_DIR / "CRIF_LDN_IRD_YYYYMMDD_QA.csv"
ERROR_FILE  = BASE_DIR / "Script2_Errors.csv"
CDW_CACHE_FILE = BASE_DIR / "cdw_cache.json"

# ============================================================
# CONSTANTS & RULES
# ============================================================
ALLOWED_IRD_SENSITIVITIES = {
    "Risk_IRVol",
    "Risk_Inflation",
    "Risk_FX",
    "Risk_IRCurve",
    "Risk_XCcyBasis"
}

CONST_PARTY_ID = "MIZBK"
CONST_IM_MODEL = "SIMM"
CONST_PRODUCT_CLASS = "RatesFX"

DATE_OUTPUT_FORMAT = "%d/%m/%Y"

# ============================================================
# SIMM COLUMN NAMES (CHANGE IF REQUIRED)
# ============================================================
SIMM_COL_TRADE_ID = "TRADE_ID"
SIMM_COL_PRODUCT_FAMILY = "MUREXPRODUCTFAMILY"
SIMM_COL_SENSITIVITY = "Sensitivity"
SIMM_COL_QUALIFIER = "Qualifier"
SIMM_COL_BUCKET = "Bucket"
SIMM_COL_LABEL1 = "Label1"
SIMM_COL_LABEL2 = "Label2"
SIMM_COL_VALUE = "Value"
SIMM_COL_CURRENCY = "Currency"
SIMM_COL_VALUE_USD = "ValueUSD"
SIMM_COL_PV_JOIN_KEY = "MUREXROOTCONTRACTID"
SIMM_COL_CP_JOIN_KEY = "CP_ID"

# ============================================================
# PV FILE COLUMN NAMES
# ============================================================
PV_JOIN_KEY = "MUREXROOTCONTRACTID"
PV_VALUE_COL = "PV"

# ============================================================
# CSA FILE COLUMN NAMES
# ============================================================
CSA_JOIN_KEY = "CP_ID"
CSA_CP_ID_COL = "CP_ID"

# ============================================================
# CDW CONFIG
# ============================================================
CDW_BASE = "https://svc-sit6-cdw.uk.mizuho-sc.com/mhbk"
INTRADAY_URL = CDW_BASE + "/fpml/intradayTrades/{trade_id}"
LEGAL_ENTITY_URL = CDW_BASE + "/common/legalEntityClients/{party_id}/"

# ============================================================
# OUTPUT COLUMNS (NEW LAYOUT)
# ============================================================
OUTPUT_COLUMNS = [
    "TRADE_ID", "PARTY_ID", "CP_ID", "IM_MODEL", "PRODUCT_CLASS",
    "TRADE_DATE", "END_DATE",
    "PRODUCT_TYPE", "NOTIONAL", "TRADE_CURRENCY",
    "NOTIONAL2", "TRADE_CURRENCY2", "VALUATION_DATE",
    "PV", "RISK_TYPE", "QUALIFIER", "BUCKET",
    "LABEL1", "LABEL2", "AMOUNT", "AMOUNT_CURRENCY",
    "AMOUNT_USD", "MASTER_CURRENCY", "MASTER_AMOUNT", "C_CIF"
]

MANDATORY_COLUMNS = [
    "TRADE_ID", "PARTY_ID", "CP_ID", "IM_MODEL", "PRODUCT_CLASS",
    "TRADE_DATE", "END_DATE", "PV",
    "RISK_TYPE", "QUALIFIER", "BUCKET", "LABEL1", "LABEL2",
    "AMOUNT", "AMOUNT_CURRENCY", "AMOUNT_USD", "C_CIF"
]

# ============================================================
# UTILS
# ============================================================
def norm(x):
    if pd.isna(x) or x is None:
        return ""
    return str(x).strip()

def normalize_date(x):
    try:
        return pd.to_datetime(x).strftime(DATE_OUTPUT_FORMAT)
    except Exception:
        return ""

# ============================================================
# AUTH
# ============================================================
def get_auth():
    load_dotenv()
    user = os.getenv("USERNAME")
    pwd = os.getenv("PASSWORD")
    if not user or not pwd:
        raise RuntimeError("USERNAME / PASSWORD not found in environment")
    return HttpNtlmAuth(user, pwd)

# ============================================================
# CDW CACHE
# ============================================================
def load_cache():
    if CDW_CACHE_FILE.exists():
        return json.loads(CDW_CACHE_FILE.read_text())
    return {"intraday": {}, "ccif": {}}

def save_cache(cache):
    CDW_CACHE_FILE.write_text(json.dumps(cache, indent=2))

# ============================================================
# CDW PARSERS
# ============================================================
def parse_intraday(xml):
    trade_date = re.search(r"<tradeDate>(.*?)</tradeDate>", xml)
    adj_date = re.search(r"<adjustedDate>(.*?)</adjustedDate>", xml)
    party = re.search(r'<party id="COUNTERPARTY">.*?<partyId.*?>(.*?)</partyId>', xml, re.S)

    return (
        trade_date.group(1) if trade_date else "",
        adj_date.group(1) if adj_date else "",
        party.group(1) if party else ""
    )

def parse_ccif(xml):
    m = re.search(r'<identifier name="MIZUHO_CCIF_NO">(.*?)</identifier>', xml)
    return m.group(1) if m else ""

# ============================================================
# MAIN
# ============================================================
def main():
    print("Starting Script 2 – Recreate CRIF_LDN_IRD")

    # ---------- Load files ----------
    simm = pd.read_csv(SIMM_FILE, sep="|", dtype=str)
    pv = pd.read_csv(PV_FILE, dtype=str)
    csa = pd.read_csv(CSA_FILE, dtype=str)

    # ---------- Filter IRD ----------
    simm = simm[
        (simm[SIMM_COL_PRODUCT_FAMILY] == "IRD") &
        (simm[SIMM_COL_SENSITIVITY].isin(ALLOWED_IRD_SENSITIVITIES))
    ].copy()

    # ---------- Build base output ----------
    out = pd.DataFrame()
    out["TRADE_ID"] = simm[SIMM_COL_TRADE_ID].map(norm)
    out["PARTY_ID"] = CONST_PARTY_ID
    out["CP_ID"] = simm[SIMM_COL_CP_JOIN_KEY].map(norm)
    out["IM_MODEL"] = CONST_IM_MODEL
    out["PRODUCT_CLASS"] = CONST_PRODUCT_CLASS

    out["TRADE_DATE"] = ""
    out["END_DATE"] = ""

    out["PRODUCT_TYPE"] = ""
    out["NOTIONAL"] = ""
    out["TRADE_CURRENCY"] = ""
    out["NOTIONAL2"] = ""
    out["TRADE_CURRENCY2"] = ""
    out["VALUATION_DATE"] = ""

    out["PV"] = ""
    out["RISK_TYPE"] = simm[SIMM_COL_SENSITIVITY].map(norm)
    out["QUALIFIER"] = simm[SIMM_COL_QUALIFIER].map(norm)
    out["BUCKET"] = simm[SIMM_COL_BUCKET].map(norm)
    out["LABEL1"] = simm[SIMM_COL_LABEL1].map(norm)
    out["LABEL2"] = simm[SIMM_COL_LABEL2].map(norm)
    out["AMOUNT"] = simm[SIMM_COL_VALUE].map(norm)
    out["AMOUNT_CURRENCY"] = simm[SIMM_COL_CURRENCY].map(norm)
    out["AMOUNT_USD"] = simm[SIMM_COL_VALUE_USD].map(norm)

    out["MASTER_CURRENCY"] = ""
    out["MASTER_AMOUNT"] = ""
    out["C_CIF"] = ""

    # ---------- Join PV ----------
    pv_map = dict(zip(pv[PV_JOIN_KEY].map(norm), pv[PV_VALUE_COL].map(norm)))
    out["PV"] = simm[SIMM_COL_PV_JOIN_KEY].map(norm).map(lambda x: pv_map.get(x, ""))

    # ---------- CDW enrichment ----------
    auth = get_auth()
    cache = load_cache()

    for trade_id in out["TRADE_ID"].unique():
        if trade_id not in cache["intraday"]:
            xml = requests.get(INTRADAY_URL.format(trade_id=trade_id),
                               auth=auth, verify=False).text
            td, ed, party_id = parse_intraday(xml)
            cache["intraday"][trade_id] = (td, ed, party_id)

            ccif = ""
            if party_id:
                xml2 = requests.get(LEGAL_ENTITY_URL.format(party_id=party_id),
                                    auth=auth, verify=False).text
                ccif = parse_ccif(xml2)
            cache["ccif"][party_id] = ccif
            time.sleep(0.05)

        td, ed, party_id = cache["intraday"][trade_id]
        out.loc[out["TRADE_ID"] == trade_id, "TRADE_DATE"] = normalize_date(td)
        out.loc[out["TRADE_ID"] == trade_id, "END_DATE"] = normalize_date(ed)
        out.loc[out["TRADE_ID"] == trade_id, "C_CIF"] = cache["ccif"].get(party_id, "")

    save_cache(cache)

    # ---------- Validation ----------
    errors = []
    for idx, row in out.iterrows():
        missing = [c for c in MANDATORY_COLUMNS if not norm(row[c])]
        if missing:
            errors.append({"ROW": idx, "TRADE_ID": row["TRADE_ID"], "MISSING": ",".join(missing)})

    # ---------- Write output ----------
    out = out[OUTPUT_COLUMNS]
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"Generated file: {OUTPUT_FILE}")

    if errors:
        pd.DataFrame(errors).to_csv(ERROR_FILE, index=False)
        print(f"Errors written to: {ERROR_FILE}")
    else:
        print("No validation errors found")

    print("Script 2 completed successfully")

if __name__ == "__main__":
    main()
