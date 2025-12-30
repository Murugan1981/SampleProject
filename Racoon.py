import pandas as pd
import json
import os
from Modules import config

def load_json_defaults(json_path):
    """Loads default overrides (tradingEntity, reportingDate) from JSON."""
    if not os.path.exists(json_path):
        return {}
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
            return data.get("TestData", {}).get("default", {})
    except:
        return {}

def load_exclusions(excel_path):
    """Loads the blacklist: {'bdmDataType': ['BrokenType'], ...}"""
    if not os.path.exists(excel_path):
        return {}
    
    df = pd.read_excel(excel_path)
    exclusions = {}
    for _, row in df.iterrows():
        param = str(row['Parameter']).strip()
        vals = [x.strip() for x in str(row['Values']).split(',') if x.strip()]
        if param in exclusions:
            exclusions[param].extend(vals)
        else:
            exclusions[param] = vals
    return exclusions

def execute_planning_phase():
    print(f"\n[Phase 2] Loading & Validating Test Plan...")

    # 1. Define Paths
    manual_input_file = os.path.join(config.CONF_DIR, "EndPoint_TestCondition.xlsx")
    json_file = os.path.join(config.CONF_DIR, "APITestData.json")
    exclusion_file = config.EXCLUSION_FILE # This is TestValueExclusion.xlsx

    # 2. Check if Manual File Exists
    if not os.path.exists(manual_input_file):
        print(f"❌ Error: Input file not found: {manual_input_file}")
        print("   Please create this file manually as per TL instructions.")
        return []

    # 3. Load All Inputs
    df_manual = pd.read_excel(manual_input_file)
    defaults = load_json_defaults(json_file)
    exclusions = load_exclusions(exclusion_file)

    test_cases = []
    skipped_count = 0

    print(f"   -> Found {len(df_manual)} rows in manual plan.")

    # 4. Iterate through your manual rows
    for index, row in df_manual.iterrows():
        # Basic Test Case Structure
        # We assume column 1 is 'Endpoint' and others are parameters
        if 'Endpoint' not in row:
            print(f"   ⚠️ Row {index} skipped: Missing 'Endpoint' column.")
            continue

        endpoint = row['Endpoint']
        current_params = {}
        is_unsafe = False

        # Loop through columns (Parameter Names)
        for param_name in df_manual.columns:
            if param_name == 'Endpoint': continue
            
            # A. Get Value from Manual Sheet
            user_value = str(row[param_name]).strip()
            if pd.isna(row[param_name]) or user_value == "":
                continue # Skip empty cells

            # B. Apply JSON Overrides (Rule #1)
            # If JSON has a value for this param, we FORCE it.
            if param_name in defaults:
                final_val = defaults[param_name]
            else:
                final_val = user_value

            # C. Check Exclusion (Rule #2)
            # If the final value is in the blacklist, we mark this test as UNSAFE
            blacklist = exclusions.get(param_name, [])
            if final_val in blacklist:
                print(f"   ⚠️ Skipping {endpoint}: Found excluded value '{final_val}' for '{param_name}'")
                is_unsafe = True
                break
            
            # Add to params map
            current_params[param_name] = final_val

        if is_unsafe:
            skipped_count += 1
            continue

        # 5. Construct Final Test Object (Compatible with Phase 3)
        # We need to distinguish Path vs Query params.
        # Simple heuristic: If {param} is in URL -> Path, else -> Query
        url_suffix = endpoint
        query_params = {}

        for p_key, p_val in current_params.items():
            token = f"{{{p_key}}}"
            if token in url_suffix:
                url_suffix = url_suffix.replace(token, str(p_val))
            else:
                query_params[p_key] = str(p_val)

        test_cases.append({
            "test_id": f"TEST_{len(test_cases)+1:04d}",
            "endpoint_original": endpoint,
            "url_suffix": url_suffix,
            "query_params": query_params
        })

    print(f"✅ Loaded {len(test_cases)} valid tests. (Skipped {skipped_count} excluded).")
    return test_cases
