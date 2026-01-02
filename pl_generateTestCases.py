import os
import itertools
import pandas as pd
from dotenv import load_dotenv

# ----------------------- BLOCK: Load Config -----------------------
# Load environment variables from .env file
load_dotenv()
SOURCE_BASE_URL = os.getenv('SourceBaseURL', '').strip()
TARGET_BASE_URL = os.getenv('TargetBaseURL', '').strip()

# Check if base URLs are loaded
if not SOURCE_BASE_URL or not TARGET_BASE_URL:
    raise Exception("SourceBaseURL and TargetBaseURL must be set in the .env file")

# ----------------------- BLOCK: Read Input Excel -----------------------
# Read the inclusion criteria Excel file
INPUT_FILE = 'TestCaseInclusion.xlsx'
OUTPUT_FILE = 'pl_testcases.xlsx'
SHEET_NAME = 'Sheet1'

# Load the Excel sheet
df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME)

# ----------------------- BLOCK: Helper Function -----------------------
def expand_row(row):
    """
    For a given row, generate all test case permutations based on 
    all possible combinations of parameter values (comma-separated).
    Returns a list of dicts with the testcase info.
    """
    # Static fields: tag, method, endpoint
    tag = str(row['tag']).strip()
    method = str(row['method']).strip()
    endpoint_template = str(row['endpoint']).strip()

    # Find parameter columns (everything after 'endpoint')
    param_cols = [col for col in row.index if col not in ['tag', 'method', 'endpoint']]

    # Parse possible values for each parameter (split by ',')
    param_values_lists = []
    for col in param_cols:
        # If cell is nan or empty, treat as empty list
        cell_val = str(row[col]).strip()
        if cell_val and cell_val.lower() != 'nan':
            # Split by comma, remove extra spaces
            values = [v.strip() for v in cell_val.split(',') if v.strip()]
        else:
            values = []
        param_values_lists.append(values)

    # Cartesian product of all parameters
    all_param_combinations = list(itertools.product(*param_values_lists)) if param_values_lists else [[]]

    testcases = []
    # For each combination, substitute in the endpoint and build test case row
    for idx, param_combo in enumerate(all_param_combinations, start=1):
        param_dict = dict(zip(param_cols, param_combo))

        # Fill endpoint placeholders with parameter values
        endpoint_final = endpoint_template
        for param, val in param_dict.items():
            endpoint_final = endpoint_final.replace('{' + param + '}', val)

        # Construct request URLs
        source_url = SOURCE_BASE_URL.rstrip('/') + endpoint_final
        target_url = TARGET_BASE_URL.rstrip('/') + endpoint_final

        # Construct test case row
        testcase = {
            'TestCaseID': f"{tag}_{idx:03d}",
            'TagName': tag,
            'SourceBaseURL': SOURCE_BASE_URL,
            'TargetBaseURL': TARGET_BASE_URL,
            'SourceRequestURL': source_url,
            'TargetRequestURL': target_url,
        }
        # Add all parameter columns (for reference)
        for param in param_cols:
            testcase[param] = param_dict.get(param, '')

        testcases.append(testcase)

    return testcases

# ----------------------- BLOCK: Generate All Test Cases -----------------------
all_testcases = []
for _, row in df.iterrows():
    # For each row, expand to multiple testcases
    all_testcases.extend(expand_row(row))

# Convert to DataFrame
df_out = pd.DataFrame(all_testcases)

# ----------------------- BLOCK: Write Output Excel -----------------------
# Write to Excel (index=False)
df_out.to_excel(OUTPUT_FILE, index=False)
print(f"Generated {len(df_out)} test cases to {OUTPUT_FILE}")

