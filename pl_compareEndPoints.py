import os
import pandas as pd

# -------------------- CONFIG --------------------
INPUT_FILE = os.path.join("API", "reports", "endpoints.xlsx")
OUTPUT_FILE = os.path.join(
    "API", "reports", "Pl_SOURCEvsTARGET_Metadata_Comparison.xlsx"
)

SOURCE_SHEET = "SOURCE"
TARGET_SHEET = "TARGET"

ENDPOINT_COL = "endpoint"


# -------------------- HELPERS --------------------
def normalize_values(value):
    if pd.isna(value) or str(value).strip() == "":
        return set()
    return {v.strip() for v in str(value).split(",") if v.strip()}


def diff_target_extra(source_val, target_val):
    src_set = normalize_values(source_val)
    tgt_set = normalize_values(target_val)
    extra = tgt_set - src_set
    return ", ".join(sorted(extra))


# -------------------- CORE LOGIC --------------------
def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(INPUT_FILE)

    source_df = pd.read_excel(INPUT_FILE, sheet_name=SOURCE_SHEET)
    target_df = pd.read_excel(INPUT_FILE, sheet_name=TARGET_SHEET)

    if ENDPOINT_COL not in source_df.columns or ENDPOINT_COL not in target_df.columns:
        raise Exception("endpoint column missing in SOURCE or TARGET")

    # -------- META + PARAM COLUMNS --------
    meta_cols = {"tag", "method", "endpoint"}
    all_columns = set(source_df.columns) | set(target_df.columns)
    param_cols = sorted(all_columns - meta_cols)

    # -------- MERGE --------
    merged = pd.merge(
        source_df,
        target_df,
        on=ENDPOINT_COL,
        how="outer",
        suffixes=("_SOURCE", "_TARGET"),
        indicator=True,
    )

    output_rows = []

    for _, row in merged.iterrows():
        merge_flag = row["_merge"]

        # -------- RESULT --------
        if merge_flag == "both":
            result = "MATCH in Both"
        elif merge_flag == "left_only":
            result = "Endpoint only in SOURCE"
        else:
            result = "Endpoint only in TARGET"

        out = {
            "SOURCE_endpoint": row[ENDPOINT_COL] if merge_flag != "right_only" else "",
            "TARGET_endpoint": row[ENDPOINT_COL] if merge_flag != "left_only" else "",
            "RESULT": result,
        }

        # -------- TAG & METHOD --------
        out["Tag_SOURCE"] = row.get("tag_SOURCE", "") if merge_flag != "right_only" else ""
        out["Tag_TARGET"] = row.get("tag_TARGET", "") if merge_flag != "left_only" else ""

        out["Method_SOURCE"] = row.get("method_SOURCE", "") if merge_flag != "right_only" else ""
        out["Method_TARGET"] = row.get("method_TARGET", "") if merge_flag != "left_only" else ""

        mismatch_comments = []

        # -------- PARAMETER COMPARISON --------
        for param in param_cols:
            src_col = f"{param}_SOURCE"
            tgt_col = f"{param}_TARGET"

            src_val = row.get(src_col, "")
            tgt_val = row.get(tgt_col, "")

            out[src_col] = "" if pd.isna(src_val) else src_val
            out[tgt_col] = "" if pd.isna(tgt_val) else tgt_val

            if merge_flag == "both":
                extra_target = diff_target_extra(src_val, tgt_val)
                if extra_target:
                    out[tgt_col] = extra_target
                    mismatch_comments.append(
                        f"{param}: extra in TARGET -> {extra_target}"
                    )

        out["ParameterMisMatchComments"] = (
            " | ".join(mismatch_comments) if mismatch_comments else ""
        )

        output_rows.append(out)

    result_df = pd.DataFrame(output_rows)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    result_df.to_excel(OUTPUT_FILE, index=False)

    print(f"Comparison completed → {OUTPUT_FILE}")
