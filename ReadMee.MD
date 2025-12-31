

FOLDER STRUCTURE

RiskFramework/
├── pl_main.py                     # Entry point ( it should only call module from API , no other logic should be presented here)
├── .env                           # Environment Configuration ( SOURCE_DS , TARGET_DS , username (password is in auth.py get_password() ))
├── shared/                        ( Inside shared folder there is raw , report and reponse , 3 folders  ,   Generated Reports , fetched datas ) 
├── API/
│   ├── pl_config.py               # Path and Environment settings 
│   ├── pl_endpoints.py            # Playwright logic for Swagger UI
│   ├── pl_generateTestCase.py     # Permutation logic
│   ├── pl_comparedEndPoints.py
│   └── pl_comparator.py           # DeepDiff logic and Reporting
│   └── pl_extract_save_response.py
│   └── auth.py


System Flow 
1. pl_endpoints.py 
2. pl_comparedEndPoints.py
3. pl_generateTestCase.py
4. pl_extract_save_response.py




1. pl_endpoints.py  
{
    URLs are defined as SOURCE_DS,TARGET_DS in the .env file. Open the urls using playwright.
    Extract all the endpoints of SOURCE and TARGET, store it in endpoints.xlsx with the sheetname as SOURCE and TARGET.
    Extract column names:tag,API METHOD[GET,POST],endpoint,parameter with available values
}


2. pl_comparedEndPoints.py
    {
        Compare SOURCE and TARGET sheets of endpoints.xlsx file. Do the comparison row by row. Consider the endpoint as primary / matching condition.
        Write the matching records, SOURCE endpoint , TARGET endpoint , Found in Both , Records[endpoint] only in SOURCE , Records [endpoint] found only in TARGET. 
        PL_SOURCEvsTARGET_Metadata_Comparison.
    }

3. pl_generateTestCase.py
    {
        If the endpoint has reportingdate and trading entity. .................
    }

4. pl_extract_save_response.py
    {
        When the records match between SOURCE and TARGET, fetch response and write into json file 
        [ Follow the same structure as extract_save_response.py ]
    }



Automated Discovery: Scrapes Swagger/OpenAPI endpoints dynamically using Playwright.
Data-Driven Testing: Generates test cases using Permutation logic (Cartesian Product) based on an Excel input matrix.
Parallel Execution: Uses asynchronous networking (Playwright APIRequestContext) to fire 50+ concurrent requests.
Smart Reconciliation: Utilizes `DeepDiff` for intelligent JSON comparison, supporting floating-point tolerance and exclusion of specific JSON paths (e.g., timestamps).


