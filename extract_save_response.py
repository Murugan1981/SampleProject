import os
import json
import pandas as pd
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv
from API.auth import get_password
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------- LOAD ENV --------------------
load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = get_password()

if not USERNAME or not PASSWORD:
    raise Exception("Missing USERNAME or PASSWORD")

AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

INPUT_PATH = os.path.join("Shared", "input")
REPORTS_PATH = os.path.join("Shared", "reports")
JSON_FILE = os.path.join(INPUT_PATH, "ApiTestData.json")

# -------------------- LOAD TEST DATA --------------------
with open(JSON_FILE, "r") as f:
    apidata = json.load(f)

system = apidata.get("System")
region = apidata.get("Region")
urltype = apidata.get("URLTYPE")
testdata = apidata.get("TestData")

# -------------------- URL BUILDER --------------------
def build_url(base_url, endpoint_template, tag):
    tag_data = testdata.get(tag, testdata.get("default", {}))

    missing_vars = []
    for part in endpoint_template.split("/"):
        if "{" in part and "}" in part:
            var = part.strip("{}")
            if var not in tag_data or not str(tag_data[var]).strip():
                missing_vars.append(var)

    if missing_vars:
        return None, missing_vars, "INPUT TEST DATA MISSING"

    try:
        return base_url + endpoint_template.format(**tag_data), [], None
    except Exception as e:
        return None, [], f"REQUEST FORMAT ERROR: {str(e)}"

# -------------------- WORKER FUNCTION --------------------
def execute_single_request(row, env, output_folder, error_log, lock):
    tag = str(row["Tags"]).strip()
    endpoint_template = row["Endpoint"]
    base_url = str(row[f"BASEURL_{env}"]).strip("/")

    full_url, missing, error = build_url(base_url, endpoint_template, tag)

    if error:
        with lock:
            error_log.append({
                "System": system,
                "Region": region,
                "Env": env,
                "Tag": tag,
                "Endpoint": endpoint_template,
                "Missing_variables": ", ".join(missing),
                "Error": error
            })
        return

    try:
        response = requests.get(
            full_url,
            auth=AUTH,
            timeout=30,
            verify=False
        )

        if response.status_code == 200:
            suffix = "_".join(testdata.get(tag, testdata.get("default", {})).keys())
            filename = f"{tag}_{suffix}_{env}.json".replace(" ", "")
            filepath = os.path.join(output_folder, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(response.text)
        else:
            with lock:
                error_log.append({
                    "System": system,
                    "Region": region,
                    "Env": env,
                    "Tag": tag,
                    "Endpoint": full_url,
                    "Missing_variables": "",
                    "Error": f"HTTP {response.status_code}:{response.reason}"
                })

    except Exception as e:
        with lock:
            error_log.append({
                "System": system,
                "Region": region,
                "Env": env,
                "Tag": tag,
                "Endpoint": full_url,
                "Missing_variables": "",
                "Error": f"EXCEPTION: {str(e)}"
            })

# -------------------- PUBLIC API FUNCTION --------------------
def fetch_and_save_response():
    """
    Public entry point.
    Call this from external code.
    """

    excel_file = os.path.join(REPORTS_PATH, "PRDvsUAT_Metadata_Comparison.xlsx")
    df = pd.read_excel(excel_file, sheet_name="PRD_vs_UAT_Metadata")

    filtered_df = df[
        (df["System"] == system) &
        (df["Region"] == region) &
        (df["URLTYPE"] == urltype) &
        (df["_merge"] == "Present in both") &
        (df["Method"].str.upper() == "GET") &
        (df["Response_Code Match?"] == True) &
        (df["Response_Description Match?"] == True) &
        (df["Parameters Match?"] == True) &
        (df["Overall Match"] == True)
    ]

    output_folder = os.path.join(REPORTS_PATH, f"{system}_OUTPUT")
    os.makedirs(output_folder, exist_ok=True)

    for f in os.listdir(output_folder):
        if f.endswith(".json"):
            os.remove(os.path.join(output_folder, f))

    error_file = os.path.join(REPORTS_PATH, "Response_Error.xlsx")
    if os.path.exists(error_file):
        os.remove(error_file)

    error_log = []
    lock = threading.Lock()
    tasks = []

    max_workers = min(32, (os.cpu_count() or 1) * 5)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, row in filtered_df.iterrows():
            for env in ["PRD", "UAT"]:
                tasks.append(
                    executor.submit(
                        execute_single_request,
                        row,
                        env,
                        output_folder,
                        error_log,
                        lock
                    )
                )

        for _ in as_completed(tasks):
            pass

    if error_log:
        pd.DataFrame(error_log).to_excel(error_file, index=False)
