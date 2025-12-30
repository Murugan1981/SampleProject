That clarifies it perfectly. **Column 2 is the "Ignore List" (Blacklist).**

This is a much safer approach than the previous one because we are cherry-picking **one valid scenario** per endpoint instead of guessing thousands of combinations.

Here is the implementation plan for **Phase 2 ONLY**.

---

### Step 1: Create the Config Files 🛠️

You need to create these two files in your `Config/` folder so the new code has data to work with.

#### File 1: `Config/APITestData.json`

*Copy-paste the content you provided.*

```json
{
  "URLTYPE": "DATASERVICE",
  "System": "JIL",
  "Env_Target" : "SIT2",
  "Env_Source" : "PRD",
  "Region": "Europe",
  "TestData": {
    "default": {
      "tradingEntity": "MBE",
      "reportingDate": "2025-10-22"
    }
  }
}

```

#### File 2: `Config/TestValueExclusion.xlsx`

*Create this Excel file with 2 columns: `Parameter` and `Values`.*

| Parameter | Values |
| --- | --- |
| bdmDataType | BrokenType, LegacyType |
| creditType | InvalidCredit |
| aggregationLevel | All |

*(Note: "All" is just an example of a value you want to skip).*

---

### Step 2: The New Logic (`Modules/test_generator.py`) 🧠

I have rewritten this module completely.
**What it does now:**

1. **Loads Defaults:** Reads `tradingEntity` and `reportingDate` from the JSON.
2. **Loads Exclusions:** Reads the "Ignore List" from Excel.
3. **Scans Endpoints:** It looks at every endpoint found in Phase 1.
4. **The "Smart Picker":**
* If the parameter is `tradingEntity`, it forces `MBE`.
* If the parameter has options (e.g., `[Delta, Gamma, Vega]`), it subtracts the Exclusions and picks the **first remaining valid option**.


5. **Saves:** Writes `Config/EndPoint_TestCondition.xlsx`.

**Replace the code in `Modules/test_generator.py` with this:**

```python
import pandas as pd
import json
import os
from Modules import config

def load_json_defaults(json_path):
    """Loads the 'TestData' > 'default' section from APITestData.json."""
    if not os.path.exists(json_path):
        print(f"⚠️ Warning: JSON Config not found at {json_path}")
        return {}
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
            return data.get("TestData", {}).get("default", {})
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return {}

def load_exclusions(excel_path):
    """
    Loads exclusion rules into a dictionary.
    Returns: {'bdmDataType': ['BrokenType', 'LegacyType'], ...}
    """
    if not os.path.exists(excel_path):
        return {}
    
    df = pd.read_excel(excel_path)
    exclusions = {}
    
    for _, row in df.iterrows():
        param = str(row['Parameter']).strip()
        # Handle comma-separated values in the Excel cell
        raw_val = str(row['Values'])
        vals = [x.strip() for x in raw_val.split(',') if x.strip()]
        
        if param in exclusions:
            exclusions[param].extend(vals)
        else:
            exclusions[param] = vals
            
    return exclusions

def parse_swagger_options(cell_value):
    """
    Simulates extracting options from the Phase 1 Scraping output.
    Assumption: Phase 1 saves options in a format we can parse, 
    or we just assume the cell contains a single example value.
    
    For this 'Golden Path' approach, if Phase 1 gave us a list, 
    we pick from it. If Phase 1 gave us just a text placeholder, we return it.
    """
    val_str = str(cell_value)
    # Check if it looks like a list "['A', 'B']"
    if val_str.startswith("[") and val_str.endswith("]"):
        # Simple clean up to get a python list
        val_str = val_str.replace("'", "").replace("[", "").replace("]", "")
        return [x.strip() for x in val_str.split(',')]
    return [val_str]

def execute_planning_phase():
    """
    The Main Logic for Phase 2:
    Reads Scraped Data -> Applies JSON Defaults -> Applies Exclusions -> Generates Golden Set.
    """
    print(f"\n[Phase 2] Generating 'Golden Path' Test Conditions...")

    # 1. Load Inputs
    defaults = load_json_defaults(os.path.join(config.CONF_DIR, "APITestData.json"))
    exclusions = load_exclusions(config.EXCLUSION_FILE) # Using the new Exclusion logic
    
    # Load Phase 1 Output
    if not os.path.exists(config.EXTRACTED_ENDPOINTS):
        print("❌ Scraped data not found. Run Phase 1 first.")
        return None

    df_source = pd.read_excel(config.EXTRACTED_ENDPOINTS)
    
    # 2. Prepare the Output Table
    # We want 1 row per endpoint.
    final_rows = []

    print(f"   -> Processing {len(df_source)} endpoints...")

    for _, row in df_source.iterrows():
        endpoint = row.get('Endpoint')
        if not endpoint: continue

        # Start building the test case for this endpoint
        test_case = {'Endpoint': endpoint}
        
        # Iterate over all columns in the Scraped file (except 'Endpoint')
        # These columns represent the parameters found in Swagger
        for param_name in df_source.columns:
            if param_name == 'Endpoint': continue
            
            # The cell value from Phase 1 (could be an Enum list or a placeholder)
            scraped_val = row[param_name]
            
            # Skip empty columns for this row
            if pd.isna(scraped_val) or str(scraped_val).strip() == "":
                continue

            # LOGIC ENGINE -----------------------------------------
            
            # RULE 1: Global Defaults (JSON)
            if param_name in defaults:
                selected_value = defaults[param_name]
            
            else:
                # Get possible options from Swagger (Phase 1)
                options = parse_swagger_options(scraped_val)
                
                # Get Blacklist for this parameter
                blacklist = exclusions.get(param_name, [])
                
                # Filter: Options - Blacklist
                valid_options = [opt for opt in options if opt not in blacklist]
                
                # RULE 2 & 3: Pick the First Valid Option
                if valid_options:
                    selected_value = valid_options[0]
                else:
                    # Fallback: If we excluded everything, note it
                    selected_value = "NO_VALID_OPTIONS"

            # ------------------------------------------------------
            
            test_case[param_name] = selected_value

        final_rows.append(test_case)

    # 3. Save to "EndPoint_TestCondition.xlsx"
    # This is the file your TL requested
    output_path = os.path.join(config.CONF_DIR, "EndPoint_TestCondition.xlsx")
    
    df_result = pd.DataFrame(final_rows)
    df_result.to_excel(output_path, index=False)
    
    print(f"✅ Generated Golden Test Plan: {output_path}")
    print(f"   -> Contains {len(df_result)} Unique Test Cases.")
    
    return df_result

```

---

### Step 3: Verify (How to check if it works) 🔍

We don't want to run the whole `Main.py` yet. We just want to see if this new logic creates the Excel file correctly.

Create a temporary file named `VerifyPhase2.py` in your root folder:

```python
# VerifyPhase2.py
from Modules import config, test_generator

# Just run the planner
config.init_environment()
test_generator.execute_planning_phase()

```

**Run it:**

```bash
python VerifyPhase2.py

```

**Check the result:**

1. Go to `Config/`.
2. Open `EndPoint_TestCondition.xlsx`.
3. **Success Criteria:**
* Do you see `tradingEntity` set to `MBE` everywhere?
* Do you see `reportingDate` set to `2025-10-22` everywhere?
* Did it pick valid values for the other columns without the "Excluded" ones?



If this file looks correct, **Phase 2 is Conquered.** 🏆
