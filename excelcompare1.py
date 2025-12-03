import pandas as pd

# Load both Excel files
df1 = pd.read_excel("file1.xlsx")  # Replace with your actual file path
df2 = pd.read_excel("file2.xlsx")

# Optional: Strip whitespace from column names
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# Check columns match
if set(df1.columns) != set(df2.columns):
    raise ValueError("Column mismatch between files")

# Reorder columns in same order (just in case)
df2 = df2[df1.columns]

# Sort both DataFrames by all columns
df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

# Compare
comparison_result = df1_sorted.equals(df2_sorted)

# Print result
if comparison_result:
    print("✅ Files are identical (ignoring row order).")
else:
    print("❌ Files have differences.")

    # Optional: Show diffs side-by-side
    diff_df = pd.concat([df1_sorted, df2_sorted]).drop_duplicates(keep=False)
    diff_df.to_excel("differences.xlsx", index=False)
    print("🔍 Differences saved to differences.xlsx")
