import asyncio
import sys
import json
from Modules import config, swagger_scraper, test_generator, api_executor, comparator

async def main():
    print("Starting RiskRecon Framework...")
    
    # 0. Setup & Safety Check
    config.init_environment()
    
    if not config.DEV_URL or not config.PROD_URL:
        print("ERROR: SOURCE_URL or TARGET_URL is missing in .env file.")
        print("Please check your .env configuration.")
        return

    # --- PHASE 1: DATA ACQUISITION ---
    print("\n[Phase 1] Acquiring Data...")
    try:
        await swagger_scraper.run(
            dev_url=config.DEV_URL, 
            prod_url=config.PROD_URL, 
            output_file=config.EXTRACTED_ENDPOINTS
        )
    except Exception as e:
        print(f"Scraper Failed: {e}")
        return

    # --- PHASE 2: TEST GENERATION (UPDATED) ---
    print("\n[Phase 2] Loading & Validating Test Plan...")
    try:
        # UPDATED: Calling the new function name, with no arguments
        test_batch = test_generator.execute_planning_phase()
    except Exception as e:
        print(f"Test Generation Failed: {e}")
        # Print the full error for debugging
        import traceback
        traceback.print_exc()
        return

    if not test_batch:
        print("No valid tests found. Please check Config/EndPoint_TestCondition.xlsx")
        return

    # --- PHASE 3: EXECUTION ---
    print(f"\n[Phase 3] Firing {len(test_batch)} Parallel Requests...")
    results = await api_executor.run_parallel_tests(
        test_cases=test_batch,
        dev_base_url=config.DEV_URL,
        prod_base_url=config.PROD_URL
    )
    
    # Dump raw data (Safety Net)
    try:
        with open(config.MASTER_JSON_REPORT, 'w') as f:
            json.dump(results, f, indent=4, default=str)
    except Exception as e:
        print(f"Warning: Could not save raw JSON dump: {e}")

    # --- PHASE 4: COMPARISON & REPORTING ---
    print("\n[Phase 4] Comparing & Reporting...")
    try:
        comparator.generate_excel_report(
            results_data=results,
            output_path=config.FINAL_EXCEL_REPORT
        )
        print(f"\nDone. Report: {config.FINAL_EXCEL_REPORT}")
    except Exception as e:
        print(f"Reporting Failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
