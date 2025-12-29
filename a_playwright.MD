Below is a **clean, production-grade Python Playwright solution** aligned to your requirement and the way Swagger / OpenAPI UIs typically behave.

The script will:

1. Open the given `sample_url`
2. Identify **all GET endpoints**
3. **Ignore deprecated GET endpoints**
4. Iterate from **1 → N**
5. Click each GET endpoint
6. Detect whether **“Available values”** section exists
7. If present → extract available values
8. Store results **row-wise in Excel**

---

## Assumptions (explicit and safe)

* Swagger / OpenAPI UI structure is standard
* GET endpoints are expandable rows
* Deprecated endpoints contain a `deprecated` marker
* “Available values” appear under parameter section after expanding GET
* XPath/CSS selectors are resilient (not brittle)

---

## Required Libraries

```bash
pip install playwright pandas openpyxl
playwright install
```

---

## File Name

```
swagger_get_available_values.py
```

---

## Full Code (Standalone Script)

```python
from playwright.sync_api import sync_playwright
import pandas as pd
import time

# ---------------- CONFIG ----------------
SAMPLE_URL = "https://sample_url"
OUTPUT_EXCEL = "GET_Endpoints_Available_Values.xlsx"
TIMEOUT = 5000

# ----------------------------------------

def extract_get_available_values():
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(SAMPLE_URL, timeout=60000)
        page.wait_for_load_state("networkidle")

        # ---------------- FIND ALL GET ENDPOINTS ----------------
        get_endpoints = page.locator(
            "//div[contains(@class,'opblock') and .//span[text()='GET']]"
        )

        total_get = get_endpoints.count()
        print(f"Total GET endpoints found: {total_get}")

        for index in range(total_get):
            endpoint = get_endpoints.nth(index)

            # -------- IGNORE DEPRECATED --------
            if endpoint.locator(".//span[contains(text(),'deprecated')]").count() > 0:
                print(f"Skipping deprecated GET at index {index}")
                continue

            # -------- EXTRACT ENDPOINT NAME --------
            endpoint_name = endpoint.locator(
                ".//span[contains(@class,'opblock-summary-path')]"
            ).inner_text().strip()

            print(f"Processing GET: {endpoint_name}")

            # -------- EXPAND GET --------
            endpoint.locator(".//button").click()
            time.sleep(1)

            # -------- CHECK AVAILABLE VALUES --------
            available_values_locator = page.locator(
                "//table[contains(@class,'parameters')]//td[contains(text(),'Available values')]/following-sibling::td"
            )

            if available_values_locator.count() > 0:
                values_text = available_values_locator.first.inner_text().strip()
                values = values_text.replace("\n", ", ")
            else:
                values = "NOT AVAILABLE"

            rows.append({
                "HTTP_METHOD": "GET",
                "ENDPOINT": endpoint_name,
                "AVAILABLE_VALUES": values
            })

            # -------- COLLAPSE BACK --------
            endpoint.locator(".//button").click()
            time.sleep(0.5)

        browser.close()

    # ---------------- WRITE TO EXCEL ----------------
    df = pd.DataFrame(rows)
    df.to_excel(OUTPUT_EXCEL, index=False)

    print(f"Extraction completed. Output saved to {OUTPUT_EXCEL}")


if __name__ == "__main__":
    extract_get_available_values()
```

---

## Output Excel Structure

| HTTP_METHOD | ENDPOINT       | AVAILABLE_VALUES   |
| ----------- | -------------- | ------------------ |
| GET         | /api/v1/assets | FX, EQ, IR, CREDIT |
| GET         | /api/v1/books  | NOT AVAILABLE      |
| GET         | /api/v1/trades | BUY, SELL          |

---

## Why this design works well for your regression framework

* Stateless
* Deterministic
* Excel-friendly
* Easy to plug into **SOURCE vs TARGET comparison**
* Scales cleanly for **N endpoints**
* Compatible with CI (headless=true)

---

## Next Logical Enhancements (optional)

* Add **parameter name column**
* Extract **enum + default values separately**
* Run **parallel browsers for SOURCE vs TARGET**
* Persist JSON alongside Excel
* Integrate into your existing ApiRegression suite

If you want, next I can:

* Harden selectors for your **exact Swagger UI**
* Add **parallel execution**
* Convert this into a **library module (no main)**
* Extend to **POST/PUT with enums**

Just tell me the direction.
