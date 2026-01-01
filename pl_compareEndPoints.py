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
    """
    Convert comma-separated string to normalized set
    """
    if pd.isna(value) or str(value).strip() == "":
        return set()
    return {v.strip() for v in str(value).split(",") if v.strip()}


def diff_target_extra(source_val, target_val):
    """
    Return only extra values present in TARGET compared to SOURCE
    """
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

    # Ensure endpoint exists
    if ENDPOINT_COL not in source_df.columns or ENDPOINT_COL not in target_df.columns:
        raise Exception("endpoint column missing in SOURCE or TARGET sheet")

    # Identify parameter columns (exclude meta columns)
    meta_cols = {"tag", "method", "endpoint"}
    param_cols = sorted(
        (set(source_df.columns) | set(target_df.columns)) - meta_cols
    )

    # Merge on endpoint (FULL OUTER)
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
        endpoint = row[ENDPOINT_COL]

        # -------- RESULT CLASSIFICATION --------
        if row["_merge"] == "both":
            result = "MATCH in Both"
        elif row["_merge"] == "left_only":
            result = "Endpoint only in SOURCE"
        else:
            result = "Endpoint only in TARGET"

        out = {
            "SOURCE_endpoint": row[ENDPOINT_COL]
            if row["_merge"] != "right_only"
            else "",
            "TARGET_endpoint": row[ENDPOINT_COL]
            if row["_merge"] != "left_only"
            else "",
            "RESULT": result,
        }

        mismatch_comments = []

        # -------- PARAMETER COMPARISON --------
        for param in param_cols:
            src_col = f"{param}_SOURCE"
            tgt_col = f"{param}_TARGET"

            src_val = row[src_col] if src_col in row else ""
            tgt_val = row[tgt_col] if tgt_col in row else ""

            # Store raw values
            out[src_col] = src_val if not pd.isna(src_val) else ""
            out[tgt_col] = tgt_val if not pd.isna(tgt_val) else ""

            # Compare only when endpoint exists in both
            if row["_merge"] == "both":
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


if __name__ == "__main__":
    main()
