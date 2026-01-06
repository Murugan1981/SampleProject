import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth
from API.auth import get_password
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================= LOAD ENV =================
load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()
AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

REPORT_PATH = os.path.join("shared", "reports")
INPUT_PATH = os.path.join("shared", "input")
SWAGGER_FILE = os.path.join(REPORT_PATH, "Swagger.xlsx")
JSON_FILE = os.path.join(INPUT_PATH, "ApiTestData.json")

os.makedirs(REPORT_PATH, exist_ok=True)

# ================= LOAD FILTERS =================
with open(JSON_FILE, "r") as f:
    config = json.load(f)

SYSTEM_FILTER = config.get("System")
REGION_FILTER = config.get("Region")
URLTYPE_FILTER = config.get("URLTYPE")

# ================= REF / ENUM RESOLUTION =================
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

    for key in ("allOf", "oneOf", "anyOf"):
        if key in schema:
            enums = []
            for s in schema[key]:
                enums.extend(extract_enum(s, openapi))
            return list(dict.fromkeys(enums))

    return []

# ================= CORE EXTRACTION =================
def extract_endpoints(baseurl, swagger_url, system, region, env, urltype):
    rows = []
    errors = []

    try:
        resp = requests.get(swagger_url, auth=AUTH, verify=False)
        if resp.status_code != 200:
            return [], [{
                "System": system,
                "Region": region,
                "Env": env,
                "SwaggerURL": swagger_url,
                "URLTYPE": urltype,
                "Error": f"HTTP {resp.status_code}"
            }]

        openapi = resp.json()
        paths = openapi.get("paths", {})

        for path, methods in paths.items():
            for method, spec in methods.items():
                if not isinstance(spec, dict):
                    continue

                params = spec.get("parameters", [])
                responses = spec.get("responses", {})
                response_code = next(iter(responses.keys()), "")
                response_desc = responses.get(response_code, {}).get("description", "")

                # ---- aggregate parameter info (ONE ROW) ----
                param_names = []
                param_in = []
                param_required = []
                schema_refs = []
                enum_map = []

                for p in params:
                    name = p.get("name")
                    location = p.get("in")
                    required = p.get("required", False)
                    schema = p.get("schema", {})
                    schema_ref = schema.get("$ref", schema.get("type", ""))

                    enums = extract_enum(schema, openapi)

                    param_names.append(name)
                    param_in.append(location)
                    param_required.append(str(required))
                    schema_refs.append(schema_ref)

                    if enums:
                        enum_map.append(f"{name}={','.join(enums)}")

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

                    # ---- aggregated columns ----
                    "Param_Names": "|".join(param_names),
                    "Param_In": "|".join(param_in),
                    "Param_Required": "|".join(param_required),
                    "Schema_Refs": "|".join(schema_refs),
                    "Enum_Map": "; ".join(enum_map),
                })

        return rows, None

    except Exception as e:
        errors.append({
            "System": system,
            "Region": region,
            "Env": env,
            "SwaggerURL": swagger_url,
            "URLTYPE": urltype,
            "Error": str(e)
        })
        return [], errors

# ================= PROCESS SHEET =================
def process_swagger_sheet(sheet):
    df = pd.read_excel(SWAGGER_FILE, sheet_name=sheet)

    df = df[
        (df["SYSTEM"] == SYSTEM_FILTER) &
        (df["REGION"] == REGION_FILTER) &
        (df["URLTYPE"] == URLTYPE_FILTER)
    ]

    all_rows, err_rows = [], []

    for _, row in df.iterrows():
        baseurl = str(row.get("BASEURL", "")).strip()
        if not baseurl or baseurl.lower() == "nan":
            continue

        swagger_url = f"{baseurl}/swagger/v1/swagger.json"

        rows, err = extract_endpoints(
            baseurl,
            swagger_url,
            row["SYSTEM"],
            row["REGION"],
            sheet,
            row["URLTYPE"]
        )

        all_rows.extend(rows)
        if err:
            err_rows.extend(err)

    return pd.DataFrame(all_rows), pd.DataFrame(err_rows)

# ================= MAIN =================
def fetch_and_store_swagger_metadata():
    print("Executing fetch_swagger_metadata (ONE ROW PER ENDPOINT)")

    src, src_err = process_swagger_sheet("SOURCE")
    tgt, tgt_err = process_swagger_sheet("TARGET")

    with pd.ExcelWriter(SWAGGER_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        src.to_excel(writer, sheet_name="SOURCE_Metadata", index=False)
        tgt.to_excel(writer, sheet_name="TARGET_Metadata", index=False)

        if not src_err.empty:
            src_err.to_excel(writer, sheet_name="SOURCE_Metadata_Error", index=False)
        if not tgt_err.empty:
            tgt_err.to_excel(writer, sheet_name="TARGET_Metadata_Error", index=False)

    print("Swagger metadata extraction completed successfully")

if __name__ == "__main__":
    fetch_and_store_swagger_metadata()
