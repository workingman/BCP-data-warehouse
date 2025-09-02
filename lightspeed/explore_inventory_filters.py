#!/usr/bin/env python3
"""Explore filtering options for inventory endpoint"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Exploring inventory endpoint filtering options...")
print("=" * 60)

# First, let's look at the structure of inventory records
print("\n📦 Analyzing inventory record structure:")
try:
    response = client._make_request('inventory', {'page_size': 10})
    sample_records = response.get('data', [])
    
    if sample_records:
        print(f"Sample inventory record:")
        first_record = sample_records[0]
        for key, value in first_record.items():
            print(f"   {key}: {value} ({type(value).__name__})")
            
        print(f"\n🔍 Checking product_id distribution in first 100 records:")
        response_100 = client._make_request('inventory', {'page_size': 100})
        records_100 = response_100.get('data', [])
        
        # Analyze product_id patterns for alphabetical chunking potential
        product_ids = [r.get('product_id', '') for r in records_100 if r.get('product_id')]
        print(f"   Found {len(product_ids)} product IDs")
        print(f"   First 5 product IDs: {product_ids[:5]}")
        print(f"   Last 5 product IDs: {product_ids[-5:]}")
        
        # Check if product_ids are UUIDs or have alphabetical patterns
        first_chars = [pid[0].lower() if pid else '' for pid in product_ids[:20]]
        print(f"   First characters: {sorted(set(first_chars))}")
        
except Exception as e:
    print(f"   ❌ Failed: {e}")

# Test potential filtering parameters
print(f"\n📦 Testing potential filter parameters:")

filter_tests = [
    # Test outlet-based filtering (you mentioned few outlets)
    {'outlet_id': ''},  # We'll get a real outlet_id from our data
    
    # Test if we can filter by product_id ranges (alphabetical chunking)
    {'product_id': 'a'},  # Test if partial product_id matching works
    
    # Test other potential filters
    {'since': '2020-01-01'},  # Date-based filtering
    {'deleted_at': 'null'},  # Active records only
]

# First get a real outlet_id from our export
try:
    print("   Getting real outlet_id for testing...")
    outlets_response = client._make_request('outlets', {'page_size': 10})
    outlets = outlets_response.get('data', [])
    real_outlet_id = outlets[0]['id'] if outlets else None
    print(f"   Found outlet_id: {real_outlet_id}")
    
    if real_outlet_id:
        print(f"\n   Testing outlet_id filter:")
        outlet_response = client._make_request('inventory', {
            'page_size': 1000, 
            'outlet_id': real_outlet_id
        })
        outlet_data = outlet_response.get('data', [])
        print(f"   Outlet filter returned {len(outlet_data)} records")

except Exception as e:
    print(f"   ❌ Outlet filter test failed: {e}")

# Test product_id filtering (alphabetical approach)
try:
    print(f"\n   Testing product_id prefix filtering:")
    # Try filtering by product_ids starting with specific characters
    for prefix in ['0', '1', '2', 'a', 'A']:
        try:
            prefix_response = client._make_request('inventory', {
                'page_size': 1000,
                'product_id': prefix  # This might not work, but worth testing
            })
            prefix_data = prefix_response.get('data', [])
            print(f"   product_id='{prefix}': {len(prefix_data)} records")
        except Exception as e:
            print(f"   product_id='{prefix}': Failed ({str(e)[:50]}...)")

except Exception as e:
    print(f"   ❌ Product ID filter tests failed: {e}")

print(f"\n" + "=" * 60)
print("Analysis: Looking for viable chunking strategies based on available filters")