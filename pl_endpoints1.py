import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# -------------------- CONFIG --------------------
load_dotenv()

SOURCE_DS = os.getenv("SOURCE_DS")
TARGET_DS = os.getenv("TARGET_DS")

if not SOURCE_DS or not TARGET_DS:
    raise Exception("SOURCE_DS or TARGET_DS missing in .env")

OUTPUT_FILE = os.path.join("API", "reports", "endpoints.xlsx")

# Swagger UI selectors
GET_BLOCK_SELECTOR = "div.opblock.opblock-get"
SUMMARY_SELECTOR = ".opblock-summary"
PATH_SELECTOR = ".opblock-summary-path"

PARAM_ROW_SELECTOR = "tr"
PARAM_NAME_SELECTOR = ".parameter__name"
PARAM_ENUM_SELECTOR = ".parameter__enum"

# -------------------- CORE EXTRACTION --------------------
async def extract_endpoints(page):
    results = []

    # Ensure tags are rendered
    await page.wait_for_selector("h4[id^='operations-tag'] span", timeout=10000)

    get_blocks = await page.query_selector_all(GET_BLOCK_SELECTOR)
    print(f"Found {len(get_blocks)} GET endpoints")

    for block in get_blocks:
        try:
            # Expand GET block
            await block.click()
            await page.wait_for_timeout(300)

            # Endpoint path
            path_el = await block.query_selector(PATH_SELECTOR)
            endpoint = (await path_el.inner_text()).strip() if path_el else ""

            # -------- TAG EXTRACTION (FIXED) --------
            tag_el = await block.query_selector(
                "xpath=ancestor::div[contains(@class,'opblock-tag-section')]"
                "//h4[contains(@id,'operations-tag')]//span"
            )
            tag = (await tag_el.inner_text()).strip() if tag_el else "UNKNOWN"

            # -------- PARAMETER EXTRACTION --------
            parameters = {}

            rows = await block.query_selector_all(PARAM_ROW_SELECTOR)
            for row in rows:
                name_el = await row.query_selector(PARAM_NAME_SELECTOR)
                enum_el = await row.query_selector(PARAM_ENUM_SELECTOR)

                if not name_el:
                    continue

                param_name = (await name_el.inner_text()).strip()

                if enum_el:
                    values = (
                        await enum_el.inner_text()
                    ).replace("Available values:", "").strip()
                else:
                    values = ""

                parameters[param_name] = values

            # -------- ROW OUTPUT --------
            row = {
                "tag": tag,
                "method": "GET",
                "endpoint": endpoint
            }

            row.update(parameters)
            results.append(row)

        except Exception as e:
            print(f"Skipped endpoint due to error: {e}")

    return results


async def process_environment(env_name, url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"Opening {env_name}: {url}")
        await page.goto(url)
        await page.wait_for_timeout(5000)

        data = await extract_endpoints(page)

        await browser.close()
        return pd.DataFrame(data)


# -------------------- ENTRY POINT --------------------
async def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    source_df = await process_environment("SOURCE", SOURCE_DS)
    target_df = await process_environment("TARGET", TARGET_DS)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        source_df.to_excel(writer, sheet_name="SOURCE", index=False)
        target_df.to_excel(writer, sheet_name="TARGET", index=False)

    print(f"Endpoint extraction completed → {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
