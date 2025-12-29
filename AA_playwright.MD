Sure, let's write a Playwright script (Python) to:

* Open the given Swagger/OpenAPI UI page (`sample_url`)
* Find all `GET` endpoints (excluding those marked as deprecated)
* For each endpoint:

  * Click to expand it
  * Check if an **"Available values"** section (by XPath) is present
  * If found, extract those available values
  * Write each endpoint and its available values into Excel (row-wise)
* Output console logs for each major step

This code is written **as a standalone script** (no main function) so you can import and call it from anywhere.

---

### Prerequisites

* `playwright` (`pip install playwright`)
* `pandas` (`pip install pandas`)
* `openpyxl` (`pip install openpyxl`)
* Install browser: `playwright install`

---

## Code: playwright_swagger_get_values.py

```python
import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# Change this to your actual URL
sample_url = "https://petstore.swagger.io/"

# XPath for GET endpoints (ignoring deprecated)
GET_XPATH = '//div[contains(@class,"opblock") and contains(@class,"get") and not(contains(@class,"is-deprecated"))]'
# XPath for GET endpoint summary/name
GET_SUMMARY_XPATH = './/span[contains(@class,"opblock-summary-path")]'
# XPath to expand/collapse operation
GET_EXPAND_BTN_XPATH = './/button[contains(@class,"opblock-summary-control")]'
# XPath for "Available values" (varies by Swagger version, this is a common one)
AVAILABLE_VALUES_XPATH = './/div[contains(@class, "parameters-container")]//div[contains(text(),"Available values")]/following-sibling::div//div'

# Output Excel filename
EXCEL_FILE = "get_endpoints_values.xlsx"

async def extract_get_endpoints_and_values(url):
    result_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # set to True to hide browser
        context = await browser.new_context()
        page = await context.new_page()

        print(f"Opening URL: {url}")
        await page.goto(url)
        await page.wait_for_timeout(3000)  # Wait for Swagger UI to fully load

        # Find all GET endpoint blocks
        get_blocks = await page.locator(GET_XPATH).all()
        N = len(get_blocks)
        print(f"Found {N} GET endpoints (excluding deprecated).")

        for i, get_block in enumerate(get_blocks):
            try:
                endpoint_path = await get_block.locator(GET_SUMMARY_XPATH).inner_text()
            except Exception:
                endpoint_path = "Unknown"

            print(f"\n[{i+1}/{N}] Endpoint: {endpoint_path}")

            # Expand the GET block (if not already expanded)
            expand_btn = get_block.locator(GET_EXPAND_BTN_XPATH)
            try:
                await expand_btn.click()
                print("  Clicked to expand endpoint.")
            except Exception:
                print("  Already expanded or cannot click.")

            await page.wait_for_timeout(700)  # Give UI time to update

            # Check for "Available values" block
            available_values_elems = await get_block.locator(AVAILABLE_VALUES_XPATH).all()
            if available_values_elems:
                available_values = [await elem.inner_text() for elem in available_values_elems]
                available_values = [v.strip() for v in available_values if v.strip()]
                print(f"  Found {len(available_values)} available value(s): {available_values}")
            else:
                available_values = []
                print("  No available values found.")

            # Store in result
            result_rows.append({
                "Endpoint": endpoint_path,
                "AvailableValues": ", ".join(available_values) if available_values else ""
            })

        await browser.close()

    # Save to Excel
    df = pd.DataFrame(result_rows)
    df.to_excel(EXCEL_FILE, index=False)
    print(f"\nExtraction complete. Results saved to {EXCEL_FILE}")

# Usage:
# asyncio.run(extract_get_endpoints_and_values(sample_url))
```

---

### **How to Run**

```bash
playwright install  # Only needed once
pip install playwright pandas openpyxl
```

You can either **import and call** `extract_get_endpoints_and_values(url)` from another script, or just run this one by adding:

```python
if __name__ == "__main__":
    asyncio.run(extract_get_endpoints_and_values(sample_url))
```

---

## **Notes**

* Adjust the XPaths if your Swagger UI structure differs. This should work for typical Swagger 3.0/OpenAPI UI.
* Logs are printed to the console for each key action (opening endpoint, found values, saving Excel).
* Each endpoint and its available values are output as a row in the Excel file.

---

Let me know your Swagger/OpenAPI URL (if different), or if you want this in **sync** mode (not async), or if you need custom XPath for your version!
