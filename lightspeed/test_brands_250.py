#!/usr/bin/env python3
"""Test brands export with page_size 250"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')

client = LightspeedClient(domain, token)

print("Testing brands with page_size=250...")
total_brands = 0
page_count = 0
unique_brands = set()

for page_data in client.stream_paginated_data('brands'):
    page_count += 1
    page_brand_count = len(page_data)
    total_brands += page_brand_count
    
    # Track unique brand IDs
    for brand in page_data:
        unique_brands.add(brand['id'])
    
    print(f"Page {page_count}: {page_brand_count} brands (total: {total_brands}, unique: {len(unique_brands)})")
    
    if page_count > 3:  # Safety break
        break

print(f"\nFinal results:")
print(f"- Total records: {total_brands}")
print(f"- Unique brands: {len(unique_brands)}")
print(f"- Pages fetched: {page_count}")

if len(unique_brands) > 200:
    print(f"\n✅ Found {len(unique_brands) - 200} additional brands!")
else:
    print(f"\n📝 Still only {len(unique_brands)} unique brands (same as before)")