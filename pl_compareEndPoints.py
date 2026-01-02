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
    source_df = pd.read_excel(INPUT_FILE, sheet_name=SOURCE_SHEET)
    target_df = pd.read_excel(INPUT_FILE, sheet_name=TARGET_SHEET)

    # Identify parameter columns
    meta_cols = {"tag", "method", "endpoint"}
    all_cols = set(source_df.columns) | set(target_df.columns)
    param_cols = sorted(all_cols - meta_cols)

    # Merge on endpoint
    merged = pd.merge(
        source_df,
        target_df,
        on=ENDPOINT_COL,
        how="outer",
        suffixes=("_SOURCE", "_TARGET"),
        indicator=True
    )

    rows = []

    for _, r in merged.iterrows():
        merge_flag = r["_merge"]

        if merge_flag == "both":
            result = "MATCH in Both"
        elif merge_flag == "left_only":
            result = "Endpoint only in SOURCE"
        else:
            result = "Endpoint only in TARGET"

        row = {
            "Tag_SOURCE": r.get("tag_SOURCE", "") if merge_flag != "right_only" else "",
            "Tag_TARGET": r.get("tag_TARGET", "") if merge_flag != "left_only" else "",

            "SOURCE_endpoint": r[ENDPOINT_COL] if merge_flag != "right_only" else "",
            "TARGET_endpoint": r[ENDPOINT_COL] if merge_flag != "left_only" else "",

            "RESULT": result,

            "Method_SOURCE": r.get("method_SOURCE", "") if merge_flag != "right_only" else "",
            "Method_TARGET": r.get("method_TARGET", "") if merge_flag != "left_only" else ""
        }

        mismatch_comments = []

        # Parameter comparison
        for p in param_cols:
            src_col = f"{p}_SOURCE"
            tgt_col = f"{p}_TARGET"

            src_val = r.get(src_col, "")
            tgt_val = r.get(tgt_col, "")

            row[src_col] = "" if pd.isna(src_val) else src_val
            row[tgt_col] = "" if pd.isna(tgt_val) else tgt_val

            if merge_flag == "both":
                extra_target = diff_target_extra(src_val, tgt_val)
                if extra_target:
                    row[tgt_col] = extra_target
                    mismatch_comments.append(
                        f"{p}: extra in TARGET -> {extra_target}"
                    )

        row["ParameterMisMatchComments"] = (
            " | ".join(mismatch_comments) if mismatch_comments else ""
        )

        rows.append(row)

    df = pd.DataFrame(rows)

    # -------------------- COLUMN ORDER ENFORCEMENT --------------------
    fixed_columns = [
        "Tag_SOURCE",
        "Tag_TARGET",
        "SOURCE_endpoint",
        "TARGET_endpoint",
        "RESULT",
        "Method_SOURCE",
        "Method_TARGET"
    ]

    param_columns = []
    for p in param_cols:
        param_columns.extend([f"{p}_SOURCE", f"{p}_TARGET"])

    final_columns = fixed_columns + param_columns + ["ParameterMisMatchComments"]
    df = df[final_columns]

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    print(f"Comparison completed → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
