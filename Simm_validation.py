import os
import pandas as pd

# ==========================
# PATH CONFIGURATION
# ==========================
BASE_PATH = os.path.join("shared", "reports", "SIMM")

TARGET_FILE = os.path.join(BASE_PATH, "target.csv")
OUTPUT_FILE = os.path.join("shared", "reports", "SIMM Validation.xlsx")

# ==========================
# VALIDATION RULES
# ==========================
VALIDATION_RULES = [
    {
        "rule_name": "SingleColumnCheck",
        "target_column": "exam",
        "values_to_check": ["EXAM001", "EXAM005", "EXAM999"]
    }
]

# ==========================
# LOAD TARGET CSV
# ==========================
target_df = pd.read_csv(TARGET_FILE)
target_df.columns = target_df.columns.str.strip()

# ==========================
# VALIDATION ENGINE
# ==========================
results = []

for rule in VALIDATION_RULES:
    rule_name = rule["rule_name"]
    target_column = rule["target_column"]
    values_to_check = rule["values_to_check"]

    # Column existence check
    if target_column not in target_df.columns:
        raise Exception(f"Target column missing: {target_column}")

    # Normalize target values (string-safe)
    target_values = set(
        target_df[target_column]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    for value in values_to_check:
        exists = str(value).strip() in target_values

        results.append({
            "Rule Name": rule_name,
            "Target Column": target_column,
            "Value Checked": value,
            "Validation Status": "PASS" if exists else "FAIL"
        })

# ==========================
# WRITE OUTPUT
# ==========================
output_df = pd.DataFrame(results)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    output_df.to_excel(writer, sheet_name="Validation Result", index=False)

print("SIMM validation completed successfully.")
print(f"Output written to: {OUTPUT_FILE}")
