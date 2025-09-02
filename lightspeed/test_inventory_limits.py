#!/usr/bin/env python3
"""Test inventory endpoint with different page sizes"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing inventory endpoint with different page sizes...")
print("=" * 60)

page_sizes_to_test = [1000, 5000, 10000, 25000, 50000]

for page_size in page_sizes_to_test:
    try:
        print(f"\n📦 Testing inventory with page_size={page_size}:")
        response = client._make_request('inventory', {'page_size': page_size})
        data = response.get('data', response.get('inventory', []))
        
        print(f"   Got {len(data)} records")
        
        # Check if response has pagination metadata
        if 'pagination' in response:
            pagination = response['pagination']
            print(f"   Pagination info: {pagination}")
        
        # Flag if we hit the page_size limit exactly
        if len(data) == page_size:
            print(f"   🚨 WARNING: Got exactly page_size records - likely more data available!")
        elif len(data) < page_size:
            print(f"   ✅ Got fewer than page_size - likely complete dataset")
            
    except Exception as e:
        print(f"   ❌ Failed: {e}")

print(f"\n" + "=" * 60)
print("Looking for the page_size that returns fewer records than requested")
print("That will tell us the true inventory count.")