def main():
    # ------------------------
    # Load inputs
    # ------------------------
    reporting_date = load_reporting_date()

    src_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=SOURCE_SHEET)
    tgt_df = pd.read_excel(ENDPOINTS_FILE, sheet_name=TARGET_SHEET)
    incl_df = pd.read_excel(INCLUSION_FILE)

    # Build a SET of valid endpoints present in BOTH SOURCE and TARGET
    valid_endpoints = set(
        tuple(x) for x in
        pd.merge(
            src_df[["tag", "method", "endpoint"]],
            tgt_df[["tag", "method", "endpoint"]],
            on=["tag", "method", "endpoint"],
            how="inner"
        ).values
    )

    test_cases = []
    tag_counters = {}

    # ------------------------
    # STRICT ROW-BY-ROW PROCESSING
    # ------------------------
    for row_idx, row in incl_df.iterrows():

        tag = str(row["tag"]).strip()
        method = str(row["method"]).strip().upper()
        endpoint_template = str(row["endpoint"]).strip()

        # Validate endpoint exists in both SOURCE and TARGET
        if (tag, method, endpoint_template) not in valid_endpoints:
            continue

        if method != "GET":
            continue

        # Extract placeholders ONLY from THIS ROW
        path_params = extract_path_params(endpoint_template)

        param_values = {}

        for p in path_params:
            if p.lower() == "reportingdate":
                param_values[p] = [reporting_date]
                continue

            cell_value = get_case_insensitive_value(row, p)
            values = parse_csv_values(cell_value)

            if not values:
                param_values = {}
                break

            param_values[p] = values

        if not param_values:
            continue

        # Cartesian expansion ONLY within this row
        keys = list(param_values.keys())

        for combo in itertools.product(*[param_values[k] for k in keys]):
            param_map = dict(zip(keys, combo))
            resolved_endpoint = resolve_endpoint(endpoint_template, param_map)

            tag_counters[tag] = tag_counters.get(tag, 0) + 1
            tc_id = f"{tag}_{tag_counters[tag]:03d}"

            test_cases.append({
                "TestCaseID": tc_id,
                "TagName": tag,
                "SourceBaseURL": SOURCE_BASEURL,
                "TargetBaseURL": TARGET_BASEURL,
                "SourceRequestURL": f"{SOURCE_BASEURL}{resolved_endpoint}",
                "TargetRequestURL": f"{TARGET_BASEURL}{resolved_endpoint}",
                "Comments": f"Generated from inclusion row {row_idx + 2}"
            })

    # ------------------------
    # Write output
    # ------------------------
    out_df = pd.DataFrame(test_cases, columns=OUT_COLUMNS)
    out_df.to_excel(OUTPUT_FILE, index=False)

    print(f"pl_testcases.xlsx generated → {OUTPUT_FILE}")
    print(f"Total testcases: {len(out_df)}")
