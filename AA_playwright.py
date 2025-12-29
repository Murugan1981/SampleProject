for i, get_block in enumerate(get_blocks):
    try:
        endpoint_path = await get_block.locator(f'xpath={GET_SUMMARY_XPATH}').inner_text()
    except Exception:
        endpoint_path = "Unknown"

    print(f"\n[{i+1}] Endpoint: {endpoint_path}")

    expand_btn = get_block.locator(GET_EXPAND_BTN_CSS)
    # Only click if present and visible (Swagger UI may already have it open)
    try:
        if await expand_btn.count() > 0 and await expand_btn.is_visible():
            await expand_btn.scroll_into_view_if_needed()
            await expand_btn.click(timeout=3000)
            print("  Clicked to expand endpoint.")
        else:
            print("  Expand button not present or not visible; maybe already expanded.")
    except Exception as ex:
        print(f"  Expand click failed: {ex}")

    await page.wait_for_timeout(700)

    available_values_elems = await get_block.locator(f'xpath={AVAILABLE_VALUES_XPATH}').all()
    if available_values_elems:
        available_values = [await elem.inner_text() for elem in available_values_elems]
        available_values = [v.strip() for v in available_values if v.strip()]
        print(f"  Found {len(available_values)} available value(s): {available_values}")
    else:
        available_values = []
        print("  No available values found.")

    result_rows.append({
        "Endpoint": endpoint_path,
        "AvailableValues": ", ".join(available_values) if available_values else ""
    })
