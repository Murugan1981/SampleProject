from libs.cdw_helpers.CDWExtractor import CDWscraper

# Replace with a real trade ID and batch date if needed
CDWscraper(
    base_url="https://cdw-uat/api/trade",
    trade_id="TR12345",
    batch_date="2025-07-01",
    output_excel="./output/cdw_extract.xlsx"
)
