import os
import pandas as pd
import requests
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv
from API.auth import get_password
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= LOAD ENV =================
load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

RAW_PATH = os.path.join("shared", "raw")
REPORT_PATH = os.path.join("shared", "reports")
INPUT_PATH = os.path.join("shared", "input")
SWAGGER_FILE = os.path.join(REPORT_PATH, "Swagger.xlsx")
JSON_FILE = os.path.join(INPUT_PATH, "ApiTestData.json")

os.makedirs(REPORT_PATH, exist_ok=True)

# ================= LOAD FILTERS =================
with open(JSON_FILE, "r") as f:
    config = json.load(f)

system_filter = config.get("System")
region_filter = config.get("Region")
urltype_filter = config.get("URLTYPE")

AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

# ================= REF RESOLUTION =================
def resolve_ref(openapi, ref):
    if not ref or not ref.startswith("#/"):
        return {}
    node = openapi
    for part in ref[2:].split("/"):
        node = node.get(part, {})
    return node if isinstance(node, dict) else {}

def extract_enum(schema, openapi):
    if not schema:
        return []

    if "$ref" in schema:
        schema = resolve_ref(openapi, schema["$ref"])

    if "enum" in schema:
        return schema["enum"]

    for k in ("allOf", "oneOf", "anyOf"):
        if k in schema:
            enums = []
            for s in schema[k]:
                enums.extend(extract_enum(s, openapi))
            return list(dict.fromkeys(enums))

    return []

# ================= CORE EXTRACTION =================
def extract_endpoints(baseurl, swagger_url, system, region, env, urltype):
    rows = []
    errors = []

    try:
        r = requests.get(swagger_url, auth=AUTH, verify=False)
        if r.status_code != 200:
            return [], [{
                "System": system, "Region": region, "Env": env,
                "SwaggerURL": swagger_url, "URLTYPE": urltype,
                "Error": f"HTTP {r.status_code}"
            }]

        openapi = r.json()
        paths = openapi.get("paths", {})

        for path, methods in paths.items():
            for method, spec in methods.items():
                if not isinstance(spec, dict):
                    continue

                params = spec.get("parameters", [])
                responses = spec.get("responses", {})
                response_code = next(iter(responses.keys()), "")
                response_desc = responses.get(response_code, {}).get("description", "")

                for p in params:
                    schema = p.get("schema", {})
                    schema_ref = schema.get("$ref")
                    enum_vals = extract_enum(schema, openapi)

                    rows.append({
                        "System": system,
                        "Region": region,
                        "Env": env,
                        "BASEURL": baseurl,
                        "SwaggerURL": swagger_url,
                        "URLTYPE": urltype,
                        "Method": method.upper(),
                        "Endpoint": path,
                        "Tags": ",".join(spec.get("tags", [])),
                        "Response_Code": response_code,
                        "Response_Description": response_desc,
                        "Param_Name": p.get("name"),
                        "Param_In": p.get("in"),
                        "Required": p.get("required", False),
                        "Schema_Ref": schema_ref,
                        "Enum_Values": "|".join(enum_vals),
                        "Enum_Count": len(enum_vals)
                    })

        return rows, None

    except Exception as e:
        errors.append({
            "System": system, "Region": region, "Env": env,
            "SwaggerURL": swagger_url, "URLTYPE": urltype,
            "Error": str(e)
        })
        return [], errors

# ================= PROCESS SHEET =================
def process_swagger_sheet(sheet):
    df = pd.read_excel(SWAGGER_FILE, sheet_name=sheet)

    df = df[
        (df["SYSTEM"] == system_filter) &
        (df["REGION"] == region_filter) &
        (df["URLTYPE"] == urltype_filter)
    ]

    all_rows, err_rows = [], []

    for _, r in df.iterrows():
        baseurl = str(r["BASEURL"]).strip()
        if not baseurl or baseurl.lower() == "nan":
            continue

        swagger_url = f"{baseurl}/swagger/v1/swagger.json"
        rows, err = extract_endpoints(
            baseurl, swagger_url,
            r["SYSTEM"], r["REGION"],
            sheet, r["URLTYPE"]
        )

        all_rows.extend(rows)
        if err:
            err_rows.extend(err)

    return pd.DataFrame(all_rows), pd.DataFrame(err_rows)

# ================= MAIN =================
def fetch_and_store_swagger_metadata():
    print("Executing fetch_swagger_metadata with schema resolution")

    src, src_err = process_swagger_sheet("SOURCE")
    tgt, tgt_err = process_swagger_sheet("TARGET")

    with pd.ExcelWriter(SWAGGER_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        src.to_excel(w, sheet_name="SOURCE_Metadata", index=False)
        tgt.to_excel(w, sheet_name="TARGET_Metadata", index=False)
        if not src_err.empty:
            src_err.to_excel(w, sheet_name="SOURCE_Metadata_Error", index=False)
        if not tgt_err.empty:
            tgt_err.to_excel(w, sheet_name="TARGET_Metadata_Error", index=False)

    print("Swagger metadata + schema enums extracted successfully")

if __name__ == "__main__":
    fetch_and_store_swagger_metadata()
