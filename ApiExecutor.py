import asyncio
import os
from playwright.async_api import async_playwright
from Modules import config
from auth import get_password  # Integrated custom auth module

MAX_CONCURRENT_REQUESTS = 50 

def get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json"
    }

async def fetch_single_pair(context, test_case, dev_base, prod_base):
    suffix = test_case['url_suffix']
    if not suffix.startswith('/'):
        suffix = '/' + suffix
        
    q_params = test_case.get('query_params', {})
    
    dev_full_url = dev_base.rstrip('/') + suffix
    prod_full_url = prod_base.rstrip('/') + suffix

    # Logging URL for visibility (Phase 3 requirement)
    print(f"   --> Firing: {dev_full_url} | Params: {q_params}")

    result = {
        "test_id": test_case['test_id'],
        "endpoint": test_case['endpoint_original'],
        "params": q_params,
        "dev_status": None,
        "prod_status": None,
        "dev_response": None,
        "prod_response": None,
        "error": None
    }

    try:
        future_dev = context.get(dev_full_url, params=q_params)
        future_prod = context.get(prod_full_url, params=q_params)
        
        resp_dev, resp_prod = await asyncio.gather(future_dev, future_prod)

        result['dev_status'] = resp_dev.status
        result['prod_status'] = resp_prod.status

        try: result['dev_response'] = await resp_dev.json()
        except: result['dev_response'] = await resp_dev.text()
            
        try: result['prod_response'] = await resp_prod.json()
        except: result['prod_response'] = await resp_prod.text()

    except Exception as e:
        result['error'] = str(e)
        
    return result

async def run_parallel_tests(test_cases, dev_base_url, prod_base_url):
    results = []
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # Credential Logic
    username = os.getenv("USERNAME")
    try:
        password = get_password() # Retrieving from auth module
    except ImportError:
        print("❌ Critical: 'auth.py' not found or get_password() missing.")
        password = None
    except Exception as e:
        print(f"❌ Critical: Password retrieval failed: {e}")
        password = None
    
    creds = None
    if username and password:
        creds = {"username": username, "password": password}
    else:
        print("   ⚠️ Missing credentials. Running unauthenticated.")
    
    async with async_playwright() as p:
        api_context = await p.request.new_context(
            ignore_https_errors=True,
            extra_http_headers=get_headers(),
            http_credentials=creds 
        )
        
        async def sem_task(tc):
            async with sem:
                return await fetch_single_pair(api_context, tc, dev_base_url, prod_base_url)

        tasks = [sem_task(tc) for tc in test_cases]
        
        for future in asyncio.as_completed(tasks):
            results.append(await future)

    return results
