import os
import pandas as pd
import json

RAW_PATH = os.path.join("shared", "raw")
REPORT_PATH = os.path.join("shared", "reports")
INPUT_PATH = os.path.join("shared", "input")

os.makedirs(REPORT_PATH, exist_ok=True)

TENANT_FILE = os.path.join(RAW_PATH, "tenant_data.xlsx")
SWAGGER_FILE = os.path.join(REPORT_PATH, "Swagger.xlsx")
TESTDATAFILE = os.path.join(INPUT_PATH, "ApiTestData.json")

# -------------------- LOAD TEST CONFIG --------------------
with open(TESTDATAFILE, "r") as f:
    testdata = json.load(f)

urltype_filter = testdata.get("URLTYPE")
system_filter = testdata.get("System")
region_filter = testdata.get("Region")
env_target_filter = testdata.get("Env_Target")
env_source_filter = testdata.get("Env_Source")

url_columns = {
    "addOnLinks_dataService_url": "DATASERVICE",
    "orchestrationApiUrl": "ORCHESTRATION",
    "addOnLinks_configuration_url": "CONFIGURATION",
    "addOnLinks_legacyDashboardApi_url": "LEGACY_DASHBOARD",
    "addOnLinks_files_url": "FILES",
    "addOnLinks_jilamendments_url": "JILAMENDMENTS",
    "addOnLinks_amendments_url": "AMENDMENTS",
    "addOnLinks_feedSummary_url": "FEEDSUMMARY",
    "addOnLinks_asOfDate_url": "ASOFDATE",
    "addOnLinks_override_url": "OVERRIDE"
}

# -------------------- CORE EXTRACTION --------------------
def extract_base_url_from_tenant(df, env_label, deployenv_filter):
    result = []

    for _, row in df.iterrows():
        system = str(row.get("system", "")).strip()
        region = str(row.get("region", "")).strip()
        deployenv = str(row.get("env", "")).strip()

        # SILENT FILTER (vomit non-matching rows)
        if (
            system != system_filter
            or region != region_filter
            or deployenv != deployenv_filter
        ):
            continue

        for url_col, url_type in url_columns.items():
            if url_type != urltype_filter:
                continue

            baseurl = str(row.get(url_col, "")).strip()

            if baseurl == "" or pd.isna(baseurl):
                continue

            swagger_url = baseurl.rstrip("/") + "/swagger/v1/swagger.json"

            result.append({
                "SYSTEM": system,
                "REGION": region,
                "ENV": env_label,
                "BASEURL": baseurl,
                "SWAGGERURL": swagger_url,
                "URLTYPE": url_type
            })

    return pd.DataFrame(result)

# -------------------- MAIN --------------------
def main():
    df_SOURCE = pd.read_excel(TENANT_FILE, sheet_name="SOURCE")
    df_TARGET = pd.read_excel(TENANT_FILE, sheet_name="TARGET")

    df_swagger_SOURCE = extract_base_url_from_tenant(
        df_SOURCE, "SOURCE", env_source_filter
    )

    df_swagger_TARGET = extract_base_url_from_tenant(
        df_TARGET, "TARGET", env_target_filter
    )

    with pd.ExcelWriter(
        SWAGGER_FILE,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df_swagger_SOURCE.to_excel(writer, sheet_name="SOURCE", index=False)
        df_swagger_TARGET.to_excel(writer, sheet_name="TARGET", index=False)

if __name__ == "__main__":
    main()
