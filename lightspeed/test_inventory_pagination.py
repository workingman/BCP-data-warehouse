#!/usr/bin/env python3
"""Test inventory endpoint pagination approaches"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing inventory endpoint pagination strategies...")
print("=" * 60)

# Test 1: Traditional pagination with page_size=5000
print("\n📦 Test 1: Traditional pagination (page_size=5000):")
try:
    page1_response = client._make_request('inventory', {'page': 1, 'page_size': 5000})
    page1_data = page1_response.get('data', [])
    print(f"   Page 1: {len(page1_data)} records")
    
    if 'pagination' in page1_response:
        print(f"   Pagination metadata: {page1_response['pagination']}")
    
    # Try page 2
    page2_response = client._make_request('inventory', {'page': 2, 'page_size': 5000})
    page2_data = page2_response.get('data', [])
    print(f"   Page 2: {len(page2_data)} records")
    
    # Check for duplicates between pages
    if page1_data and page2_data:
        page1_ids = set(item['id'] for item in page1_data if 'id' in item)
        page2_ids = set(item['id'] for item in page2_data if 'id' in item)
        overlap = page1_ids.intersection(page2_ids)
        print(f"   Duplicate records between pages: {len(overlap)}")
        if len(overlap) == 0:
            print("   ✅ Pages appear to have unique data - pagination might work!")
        else:
            print("   🚨 Found duplicate records - pagination is broken here too")
            
except Exception as e:
    print(f"   ❌ Failed: {e}")

# Test 2: Check if there are filtering options
print(f"\n📦 Test 2: Check API response structure:")
try:
    response = client._make_request('inventory', {'page_size': 100})
    print(f"   Response keys: {list(response.keys())}")
    
    if 'data' in response and response['data']:
        first_record = response['data'][0]
        print(f"   Sample record keys: {list(first_record.keys())}")
        
except Exception as e:
    print(f"   ❌ Failed: {e}")

print(f"\n" + "=" * 60)
print("Summary: Checking if inventory pagination actually works unlike other endpoints")