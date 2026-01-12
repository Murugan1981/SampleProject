# script2_recreate_crif_ird.py
# -------------------------------------------------------
# Re-create CRIF_LDN_IRD_YYYYMMDD.csv from SIMM + PV + CSA
# -------------------------------------------------------

import os
import json
import time
import re
from pathlib import Path

import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
import urllib3

from auth import get_password   # <-- as requested

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =======================================================
# HARD-CODED FILE PATHS (NO AUTO-DETECTION)
# =======================================================
BASE_DIR = Path(r"H:\LDNM1\SIMM\Final")

SIMM_FILE = BASE_DIR / "MHBK_Sensitivities_20251128.txt"
PV_FILE   = BASE_DIR / "MHBK_MX_PV_20251128.csv"
CSA_FILE  = BASE_DIR / "CSA_COUNTERPARTY_INFO_ALL_20251128.csv"

OUTPUT_FILE = BASE_DIR / "CRIF_LDN_IRD_20251128_QA.csv"
CDW_CACHE_FILE = BASE_DIR / "cdw_cache.json"

# =======================================================
# IRD RULES
# =======================================================
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

# =======================================================
# SIMM COLUMN NAMES (CONFIRMED SET)
# =======================================================
SIMM_COL_TRADE_ID = "TRADE_ID"
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

# =======================================================
# PV FILE
# =======================================================
PV_JOIN_KEY = "MUREXROOTCONTRACTID"
PV_VALUE_COL = "PV"

# =======================================================
# CDW CONFIG
# =======================================================
CDW_BASE = "https://svc-sit6-cdw.uk.mizuho-sc.com/mhbk"
INTRADAY_URL = CDW_BASE + "/fpml/intradayTrades/{trade_id}"
LEGAL_ENTITY_URL = CDW_BASE + "/common/legalEntityClients/{party_id}/"

# =======================================================
# OUTPUT LAYOUT (NEW CRIF IRD)
# =======================================================
OUTPUT_COLUMNS = [
    "TRADE_ID", "PARTY_ID", "CP_ID", "IM_MODEL", "PRODUCT_CLASS",
    "TRADE_DATE", "END_DATE",
    "PRODUCT_TYPE", "NOTIONAL", "TRADE_CURRENCY",
    "NOTIONAL2", "TRADE_CURRENCY2", "VALUATION_DATE",
    "PV", "RISK_TYPE", "QUALIFIER", "BUCKET",
    "LABEL1", "LABEL2", "AMOUNT", "AMOUNT_CURRENCY",
    "AMOUNT_USD", "MASTER_CURRENCY", "MASTER_AMOUNT", "C_CIF"
]

# =======================================================
# UTILS
# =======================================================
def norm(x):
    if x is None or pd.isna(x):
        return ""
    return str(x).strip()

def normalize_date(x):
    try:
        return pd.to_datetime(x).strftime("%d/%m/%Y")
    except Exception:
        return ""

# =======================================================
# AUTH (MATCHES YOUR FRAMEWORK)
# =======================================================
def get_auth():
    username = os.getenv("USERNAME")
    password = get_password()
    if not username or not password:
        raise RuntimeError("USERNAME or PASSWORD missing")
    return HttpNtlmAuth(username, password)

# =======================================================
# CDW CACHE
# =======================================================
def load_cache():
    if CDW_CACHE_FILE.exists():
        return json.loads(CDW_CACHE_FILE.read_text())
    return {"intraday": {}, "ccif": {}}

def save_cache(cache):
    CDW_CACHE_FILE.write_text(json.dumps(cache, indent=2))

# =======================================================
# CDW XML PARSERS
# =======================================================
def parse_intraday(xml):
    trade_date = re.search(r"<tradeDate>(.*?)</tradeDate>", xml)
    end_date = re.search(r"<adjustedDate>(.*?)</adjustedDate>", xml)
    party = re.search(
        r'<party id="COUNTERPARTY">.*?<partyId.*?>(.*?)</partyId>',
        xml,
        re.S
    )

    return (
        trade_date.group(1) if trade_date else "",
        end_date.group(1) if end_date else "",
        party.group(1) if party else ""
    )

def parse_ccif(xml):
    m = re.search(r'<identifier name="MIZUHO_CCIF_NO">(.*?)</identifier>', xml)
    return m.group(1) if m else ""

# =======================================================
# MAIN
# =======================================================
def main():
    print("Starting Script 2 – Recreate CRIF_LDN_IRD")

    # ---------- LOAD FILES (SAFE FOR OLD PANDAS) ----------
    with open(SIMM_FILE, "r", encoding="cp1252", errors="replace") as f:
        simm = pd.read_csv(f, sep="|", dtype=str)

    with open(PV_FILE, "r", encoding="cp1252", errors="replace") as f:
        pv = pd.read_csv(f, dtype=str)

    with open(CSA_FILE, "r", encoding="cp1252", errors="replace") as f:
        csa = pd.read_csv(f, dtype=str)

    # ---------- SANITY CHECK ----------
    if SIMM_COL_SENSITIVITY not in simm.columns:
        raise RuntimeError(
            f"Sensitivity column '{SIMM_COL_SENSITIVITY}' not found.\n"
            f"Available columns: {simm.columns.tolist()}"
        )

    # ---------- FILTER IRD (CORRECT LOGIC) ----------
    simm = simm[
        simm[SIMM_COL_SENSITIVITY].map(norm).isin(ALLOWED_IRD_SENSITIVITIES)
    ].copy()

    print(f"IRD rows after filter: {len(simm)}")

    # ---------- BUILD OUTPUT ----------
    out = pd.DataFrame()
    out["TRADE_ID"] = simm[SIMM_COL_TRADE_ID].map(norm)
    out["PARTY_ID"] = CONST_PARTY_ID
    out["CP_ID"] = simm[SIMM_COL_CP_JOIN_KEY].map(norm)
    out["IM_MODEL"] = CONST_IM_MODEL
    out["PRODUCT_CLASS"] = CONST_PRODUCT_CLASS
    out["TRADE_DATE"] = ""
    out["END_DATE"] = ""

    # Blank-by-design columns
    out["PRODUCT_TYPE"] = ""
    out["NOTIONAL"] = ""
    out["TRADE_CURRENCY"] = ""
    out["NOTIONAL2"] = ""
    out["TRADE_CURRENCY2"] = ""
    out["VALUATION_DATE"] = ""

    # Risk columns
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

    # ---------- PV JOIN ----------
    pv_map = dict(zip(
        pv[PV_JOIN_KEY].map(norm),
        pv[PV_VALUE_COL].map(norm)
    ))
    out["PV"] = simm[SIMM_COL_PV_JOIN_KEY].map(norm).map(lambda x: pv_map.get(x, ""))

    # ---------- CDW ENRICH ----------
    auth = get_auth()
    cache = load_cache()

    for trade_id in out["TRADE_ID"].unique():
        if trade_id not in cache["intraday"]:
            xml = requests.get(
                INTRADAY_URL.format(trade_id=trade_id),
                auth=auth,
                verify=False
            ).text

            td, ed, party_id = parse_intraday(xml)
            cache["intraday"][trade_id] = (td, ed, party_id)

            ccif = ""
            if party_id:
                xml2 = requests.get(
                    LEGAL_ENTITY_URL.format(party_id=party_id),
                    auth=auth,
                    verify=False
                ).text
                ccif = parse_ccif(xml2)

            cache["ccif"][party_id] = ccif
            time.sleep(0.05)

        td, ed, party_id = cache["intraday"][trade_id]
        out.loc[out["TRADE_ID"] == trade_id, "TRADE_DATE"] = normalize_date(td)
        out.loc[out["TRADE_ID"] == trade_id, "END_DATE"] = normalize_date(ed)
        out.loc[out["TRADE_ID"] == trade_id, "C_CIF"] = cache["ccif"].get(party_id, "")

    save_cache(cache)

    # ---------- WRITE OUTPUT ----------
    out = out[OUTPUT_COLUMNS]
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Generated file: {OUTPUT_FILE}")
    print("Script 2 completed successfully")

if __name__ == "__main__":
    main()
