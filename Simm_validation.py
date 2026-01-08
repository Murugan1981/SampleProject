
import os
import pandas as pd

# ==========================
# PATH CONFIGURATION
# ==========================
BASE_PATH = os.path.join("shared", "reports", "SIMM")

SOURCE_FILE = os.path.join(BASE_PATH, "source.xlsx")
TARGET_FILE = os.path.join(BASE_PATH, "target.xlsx")
OUTPUT_FILE = os.path.join("shared", "reports", "SIMM Validation.xlsx")

# ==========================
# VALIDATION RULES
# ==========================
# You can add / remove rules freely
VALIDATION_RULES = [
    {
        "rule_name": "SingleColumnCheck",
        "source_columns": ["exam"],
        "target_columns": ["exam"]
    },
    {
        "rule_name": "MultiColumnCheck",
        "source_columns": ["exam", "subject"],
        "target_columns": ["exam", "subject"]
    }
]

# ==========================
# LOAD EXCEL FILES
# ==========================
source_df = pd.read_excel(SOURCE_FILE)
target_df = pd.read_excel(TARGET_FILE)

# Normalize column names (safe practice)
source_df.columns = source_df.columns.str.strip()
target_df.columns = target_df.columns.str.strip()

# ==========================
# VALIDATION ENGINE
# ==========================
results = []

for rule in VALIDATION_RULES:
    rule_name = rule["rule_name"]
    src_cols = rule["source_columns"]
    tgt_cols = rule["target_columns"]

    # Safety checks
    for col in src_cols:
        if col not in source_df.columns:
            raise Exception(f"Source column missing: {col}")

    for col in tgt_cols:
        if col not in target_df.columns:
            raise Exception(f"Target column missing: {col}")

    # Iterate source rows
    for idx, src_row in source_df.iterrows():
        src_values = tuple(src_row[col] for col in src_cols)

        # Drop NA rows before comparison
        tgt_subset = target_df[tgt_cols].dropna()

        # Create tuple set for fast lookup
        target_value_set = set(
            tuple(row[col] for col in tgt_cols)
            for _, row in tgt_subset.iterrows()
        )

        exists = src_values in target_value_set

        results.append({
            "Rule Name": rule_name,
            "Source Row": idx + 1,
            "Source Columns": ", ".join(src_cols),
            "Source Values": ", ".join(map(str, src_values)),
            "Validation Status": "PASS" if exists else "FAIL"
        })

# ==========================
# WRITE OUTPUT EXCEL
# ==========================
output_df = pd.DataFrame(results)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode="w") as writer:
    output_df.to_excel(writer, sheet_name="Validation Result", index=False)

print(f"SIMM validation completed successfully.")
print(f"Output written to: {OUTPUT_FILE}")
