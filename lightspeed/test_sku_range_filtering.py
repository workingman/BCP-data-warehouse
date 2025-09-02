#!/usr/bin/env python3
"""Test inventory filtering by SKU/product ranges"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing inventory filtering by numeric ranges...")
print("=" * 60)

print("💡 Strategy: Since products have sequential numeric SKUs (1,006 to 18,271),")
print("   maybe we can filter inventory by product ID ranges or SKU ranges")

# Test various range-based filtering approaches
test_approaches = [
    # SKU-based filtering
    {'sku__gte': '1000', 'sku__lt': '5000'},  # SKU range
    {'sku_min': '1000', 'sku_max': '5000'},   # Alternative SKU range
    {'sku': '1006'},                          # Exact SKU match
    
    # Product ID based (though these are UUIDs, less promising)  
    {'product_id__gte': '0'},                 # Product ID range (alphabetical)
    {'product_id__lt': '5'},                  # Product ID range
    
    # General range parameters
    {'min_sku': '1000', 'max_sku': '5000'},   # Range params
    {'from': '1000', 'to': '5000'},           # Generic range
    {'range': '1000-5000'},                   # Range string
    
    # Date/version based chunking
    {'updated_since': '2020-01-01'},          # Time-based
    {'version__gte': '1000000000'},           # Version-based
]

print(f"\n🧪 Testing {len(test_approaches)} different filtering approaches:")

successful_filters = []

for i, filter_params in enumerate(test_approaches):
    try:
        print(f"\n   Test {i+1}: {filter_params}")
        
        # Add page_size to avoid hitting limits
        params = {**filter_params, 'page_size': 100}
        response = client._make_request('inventory', params)
        data = response.get('data', [])
        
        print(f"     Result: {len(data)} records")
        
        if len(data) > 0:
            # Check if filtering actually worked by examining the data
            first_record = data[0]
            product_id = first_record.get('product_id', '')
            
            # We'd need to cross-reference with products to get SKU
            # For now, just check if we got a reasonable response
            if len(data) != 100:  # If we got fewer than page_size, it might be working
                print(f"     ✅ Promising: Got {len(data)} records (not hitting page_size limit)")
                successful_filters.append((filter_params, len(data)))
            elif i == 0:  # First test, check baseline
                print(f"     📝 Baseline: Got full page_size")
            else:
                print(f"     ❓ Got full page_size - filter might be ignored")
                
        else:
            print(f"     ❌ No data returned")
            
    except Exception as e:
        print(f"     ❌ Failed: {str(e)[:50]}...")

# Summary
print(f"\n" + "=" * 60)
print("RESULTS:")

if successful_filters:
    print("✅ Successful filtering approaches found:")
    for filter_params, count in successful_filters:
        print(f"   {filter_params}: {count} records")
    print("\n🎯 We can use these to chunk inventory data by ranges!")
else:
    print("❌ No working filter parameters found")
    print("📝 Recommendation: Accept the 5,000 inventory records we can get")
    print("   OR contact Lightspeed support about accessing complete inventory data")

print(f"\n💡 Alternative: Since we have product data with SKUs,")
print(f"   we could cross-reference the 5,000 inventory records with product SKUs")
print(f"   to see which SKU ranges are missing from inventory tracking.")