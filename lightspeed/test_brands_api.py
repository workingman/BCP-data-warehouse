#!/usr/bin/env python3
"""
Test script to investigate the brands API pagination issue
"""
import os
import json
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()

domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')

if not domain or not token:
    print("Missing environment variables")
    exit(1)

client = LightspeedClient(domain, token)

# Test direct API calls to understand the response structure
print("Testing brands API pagination...")
print("=" * 50)

# Test page 1 with small page size to see pagination structure
print("\n1. Testing page 1 with page_size=10:")
response1 = client._make_request('brands', {'page': 1, 'page_size': 10})
print(f"Response keys: {list(response1.keys())}")
if 'pagination' in response1:
    print(f"Pagination metadata: {json.dumps(response1['pagination'], indent=2)}")
else:
    print("No pagination metadata found")

if 'data' in response1:
    data1 = response1['data']
    print(f"Data count: {len(data1)}")
    print(f"First record ID: {data1[0]['id'] if data1 else 'None'}")
    print(f"Last record ID: {data1[-1]['id'] if data1 else 'None'}")
elif 'brands' in response1:
    data1 = response1['brands']
    print(f"Data count: {len(data1)}")
    print(f"First record ID: {data1[0]['id'] if data1 else 'None'}")
    print(f"Last record ID: {data1[-1]['id'] if data1 else 'None'}")

# Test page 2 with same params
print("\n2. Testing page 2 with page_size=10:")
response2 = client._make_request('brands', {'page': 2, 'page_size': 10})
if 'pagination' in response2:
    print(f"Pagination metadata: {json.dumps(response2['pagination'], indent=2)}")

if 'data' in response2:
    data2 = response2['data']
elif 'brands' in response2:
    data2 = response2['brands']
else:
    data2 = response2 if isinstance(response2, list) else []

print(f"Data count: {len(data2)}")
if data2:
    print(f"First record ID: {data2[0]['id']}")
    print(f"Last record ID: {data2[-1]['id']}")

# Check if page 1 and page 2 have identical data
if data1 and data2:
    page1_ids = [record['id'] for record in data1]
    page2_ids = [record['id'] for record in data2]
    
    print(f"\n3. Comparing pages:")
    print(f"Page 1 IDs: {page1_ids[:3]}..." if len(page1_ids) > 3 else f"Page 1 IDs: {page1_ids}")
    print(f"Page 2 IDs: {page2_ids[:3]}..." if len(page2_ids) > 3 else f"Page 2 IDs: {page2_ids}")
    print(f"Are pages identical? {page1_ids == page2_ids}")

# Test with different page sizes
print("\n4. Testing different page sizes:")
for page_size in [5, 20, 50]:
    response = client._make_request('brands', {'page': 1, 'page_size': page_size})
    if 'data' in response:
        data = response['data']
    elif 'brands' in response:
        data = response['brands']
    else:
        data = response if isinstance(response, list) else []
    
    print(f"Page size {page_size}: got {len(data)} records")