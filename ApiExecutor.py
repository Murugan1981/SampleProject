import asyncio
import os
from playwright.async_api import async_playwright
from Modules import config

MAX_CONCURRENT_REQUESTS = 50 

def get_headers():
    """
    Standard headers to mimic a browser. 
    Note: We do NOT need to manually add Authorization headers here
    because 'http_credentials' below handles the login.
    """
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json"
    }

async def fetch_single_pair(context, test_case, dev_base, prod_base):
    # Ensure URL formatting
    suffix = test_case['url_suffix']
    if not suffix.startswith('/'):
        suffix = '/' + suffix
        
    q_params = test_case.get('query_params', {})
    
    # Clean URLs
    dev_full_url = dev_base.rstrip('/') + suffix
    prod_full_url = prod_base.rstrip('/') + suffix

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
        # Fire requests using the Authenticated Context
        future_dev = context.get(dev_full_url, params=q_params)
        future_prod = context.get(prod_full_url, params=q_params)
        
        resp_dev, resp_prod = await asyncio.gather(future_dev, future_prod)

        result['dev_status'] = resp_dev.status
        result['prod_status'] = resp_prod.status

        # Parse Responses
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

    # 1. Load Credentials from .env
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    
    creds = None
    if username and password:
        creds = {"username": username, "password": password}
        print(f"   🔒 Authenticating as user: {username}")
    else:
        print("   ⚠️ No credentials found in .env (USERNAME/PASSWORD). Sending anonymous requests.")

    async with async_playwright() as p:
        # 2. Create Context with HTTP Credentials
        # This handles Basic Auth, NTLM, and Digest automatically.
        api_context = await p.request.new_context(
            ignore_https_errors=True,
            extra_http_headers=get_headers(),
            http_credentials=creds  # <--- THE FIX
        )
        
        async def sem_task(tc):
            async with sem:
                return await fetch_single_pair(api_context, tc, dev_base_url, prod_base_url)

        tasks = [sem_task(tc) for tc in test_cases]
        
        for future in asyncio.as_completed(tasks):
            results.append(await future)

    return results
