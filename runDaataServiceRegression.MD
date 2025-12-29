Below is a **single, standalone Python script** that you can run independently.

It does **all of the following in one run**:

* Resolves enum parameters (STATIC or API-based)
* Expands each endpoint into **N executions (one per enum value)**
* Calls the API
* Writes **all responses to `DataService.json`**
* Writes a **fully traceable Excel report** (`DataService.xlsx`)
* Does **not depend on any of your existing modules**
* Uses **ApiTestData.json + ParameterMatrix.json only**

No Swagger modification required.

---

# 📁 Expected Input Files

```
config/
 ├── ApiTestData.json
 ├── ParameterMatrix.json
```

---

# 📄 Output Files

```
output/
 ├── DataService.json
 └── DataService.xlsx
```

---

# ✅ Standalone Script

### **Filename: `run_dataservice_regression.py`**

```python
import json
import itertools
import requests
import pandas as pd
from pathlib import Path
from functools import lru_cache
from requests_ntlm import HttpNtlmAuth
import os

# ---------------- CONFIG ----------------
CONFIG_DIR = Path("config")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

API_TESTDATA_FILE = CONFIG_DIR / "ApiTestData.json"
PARAM_MATRIX_FILE = CONFIG_DIR / "ParameterMatrix.json"

JSON_OUTPUT_FILE = OUTPUT_DIR / "DataService.json"
EXCEL_OUTPUT_FILE = OUTPUT_DIR / "DataService.xlsx"

VERIFY_SSL = False

# -------------- AUTH --------------------
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

if not USERNAME or not PASSWORD:
    raise RuntimeError("USERNAME / PASSWORD env variables not set")

AUTH = HttpNtlmAuth(USERNAME, PASSWORD)

# ------------ LOAD INPUTS ----------------
with open(API_TESTDATA_FILE) as f:
    API_DATA = json.load(f)

with open(PARAM_MATRIX_FILE) as f:
    PARAM_MATRIX = json.load(f)

BASE_URL_SOURCE = API_DATA["BaseURL_Source"]
BASE_URL_TARGET = API_DATA["BaseURL_Target"]

ENDPOINT = API_DATA["Endpoint"]
METHOD = API_DATA.get("Method", "GET")

DEFAULT_PARAMS = API_DATA.get("TestData", {}).get("default", {})

# ---------- PARAMETER RESOLVER ----------
class ParameterResolver:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")

    @lru_cache(maxsize=128)
    def fetch_from_api(self, endpoint, value_field):
        url = f"{self.base_url}{endpoint}"
        r = requests.get(url, auth=AUTH, verify=VERIFY_SSL)
        r.raise_for_status()
        data = r.json()
        return [item[value_field] for item in data]

    def resolve(self, param, cfg):
        if cfg["source"] == "STATIC":
            return cfg["values"]

        if cfg["source"] == "API":
            return self.fetch_from_api(
                cfg["endpoint"],
                cfg["valueField"]
            )

        raise ValueError(f"Unknown source for {param}")

# ---------- PARAM EXTRACTION -------------
def extract_path_params(endpoint):
    return [p.strip("{}") for p in endpoint.split("/") if p.startswith("{")]

# ---------- EXPANSION LOGIC --------------
def expand_endpoint(endpoint, resolver):
    params = extract_path_params(endpoint)
    param_values = {}

    for p in params:
        if p in PARAM_MATRIX:
            param_values[p] = resolver.resolve(p, PARAM_MATRIX[p])
        elif p in DEFAULT_PARAMS:
            param_values[p] = [DEFAULT_PARAMS[p]]
        else:
            raise ValueError(f"No value defined for parameter: {p}")

    combinations = itertools.product(*param_values.values())

    expanded = []
    for combo in combinations:
        concrete = endpoint
        param_map = {}
        for p, v in zip(param_values.keys(), combo):
            concrete = concrete.replace(f"{{{p}}}", str(v))
            param_map[p] = v

        expanded.append((concrete, param_map))

    return expanded

# ---------- EXECUTION --------------------
def execute(base_url, concrete_endpoint):
    url = f"{base_url}{concrete_endpoint}"
    r = requests.get(url, auth=AUTH, verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()

# ---------- MAIN -------------------------
def main():
    resolver_source = ParameterResolver(BASE_URL_SOURCE)
    resolver_target = ParameterResolver(BASE_URL_TARGET)

    executions = expand_endpoint(ENDPOINT, resolver_source)

    json_results = []
    excel_rows = []

    for concrete_ep, params in executions:
        for env, base_url in [("SOURCE", BASE_URL_SOURCE), ("TARGET", BASE_URL_TARGET)]:
            try:
                response = execute(base_url, concrete_ep)

                json_results.append({
                    "Env": env,
                    "Endpoint": concrete_ep,
                    "Parameters": params,
                    "Response": response
                })

                excel_rows.append({
                    "Env": env,
                    "Endpoint": concrete_ep,
                    "Parameters": json.dumps(params),
                    "Status": "SUCCESS",
                    "RecordCount": len(response) if isinstance(response, list) else 1
                })

            except Exception as e:
                excel_rows.append({
                    "Env": env,
                    "Endpoint": concrete_ep,
                    "Parameters": json.dumps(params),
                    "Status": "ERROR",
                    "Error": str(e)
                })

    # -------- WRITE JSON ----------
    with open(JSON_OUTPUT_FILE, "w") as f:
        json.dump(json_results, f, indent=2)

    # -------- WRITE EXCEL ---------
    df = pd.DataFrame(excel_rows)
    df.to_excel(EXCEL_OUTPUT_FILE, index=False)

    print(f"✔ JSON written to: {JSON_OUTPUT_FILE}")
    print(f"✔ Excel written to: {EXCEL_OUTPUT_FILE}")

# ---------- ENTRY ------------------------
if __name__ == "__main__":
    main()
```

---

# 🧪 Example `ApiTestData.json`

```json
{
  "BaseURL_Source": "https://prd.api.company.com",
  "BaseURL_Target": "https://sit.api.company.com",
  "Endpoint": "/jupiter/bdm/{bdmDataType}/{reportingDate}/{tradingEntity}",
  "Method": "GET",
  "TestData": {
    "default": {
      "reportingDate": "2025-10-23",
      "tradingEntity": "MHI"
    }
  }
}
```

---

# 🧪 Example `ParameterMatrix.json`

```json
{
  "bdmDataType": {
    "source": "STATIC",
    "values": [
      "AssetAgeing",
      "BreachAudit",
      "BreachStatus"
    ]
  }
}
```

---

# ✅ What This Gives You

* One script → one command
* Automatically runs **N×SOURCE + N×TARGET**
* Works even if enums are **not in Swagger**
* Produces **machine-readable JSON** and **human-readable Excel**
* CI-ready
* No dependency on your existing framework

---

## Next Enhancements (Optional)

If you want, next I can add:

* Parallel execution
* Source vs Target JSON diff sheet
* Ignore fields (timestamps, IDs)
* Resume-from-failure
* Bamboo/Jenkins wrapper

Just tell me.
