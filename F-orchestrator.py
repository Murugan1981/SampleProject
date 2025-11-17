from libs.cdw_helpers.CDWExtractor import CDWscraper

# SAMPLE CALL — Replace with real trade ID and batch date
CDWscraper(
    base_url="https://your-cdw-url/api/trade",   # ✅ Replace this with your actual base URL
    trade_id="TR12345",                          # ✅ Replace this with any valid trade ID
    batch_date="2025-07-01",                     # ✅ Replace with real batch date
    output_excel="./output/cdw_extract_test.xlsx"
)
