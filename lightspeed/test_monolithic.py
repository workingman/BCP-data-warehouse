#!/usr/bin/env python3
"""Test monolithic (non-paginated) API requests"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing different approaches to get all data without pagination...")
print("=" * 70)

endpoints_to_test = ['customers', 'products', 'sales']

for endpoint in endpoints_to_test:
    print(f"\n🔍 Testing {endpoint}:")
    
    # Test 1: No pagination params at all
    try:
        print("  1. No pagination parameters:")
        response1 = client._make_request(endpoint)
        data1 = response1.get('data', response1.get(endpoint, []))
        print(f"     Got {len(data1)} records")
    except Exception as e:
        print(f"     Failed: {e}")
    
    # Test 2: Very large page_size
    try:
        print("  2. Large page_size (10000):")
        response2 = client._make_request(endpoint, {'page_size': 10000})
        data2 = response2.get('data', response2.get(endpoint, []))
        print(f"     Got {len(data2)} records")
    except Exception as e:
        print(f"     Failed: {e}")
    
    # Test 3: page_size without page param
    try:
        print("  3. page_size=1000, no page param:")
        response3 = client._make_request(endpoint, {'page_size': 1000})
        data3 = response3.get('data', response3.get(endpoint, []))
        print(f"     Got {len(data3)} records")
    except Exception as e:
        print(f"     Failed: {e}")
    
    # Test 4: Check if we can specify page=0
    try:
        print("  4. page=0, page_size=5000:")
        response4 = client._make_request(endpoint, {'page': 0, 'page_size': 5000})
        data4 = response4.get('data', response4.get(endpoint, []))
        print(f"     Got {len(data4)} records")
    except Exception as e:
        print(f"     Failed: {e}")

print(f"\n" + "=" * 70)
print("Summary: Looking for approaches that return more than 200 records")