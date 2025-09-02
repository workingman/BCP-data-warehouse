#!/usr/bin/env python3
"""Check if products endpoint has inventory information"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Checking if products contain inventory data...")
print("=" * 60)

try:
    # Get a few products to see their structure
    response = client._make_request('products', {'page_size': 5})
    products = response.get('data', [])
    
    if products:
        print("📦 Sample product structure:")
        sample_product = products[0]
        
        for key, value in sample_product.items():
            print(f"   {key}: {str(value)[:100]}{'...' if len(str(value)) > 100 else ''} ({type(value).__name__})")
        
        # Look for inventory-related fields
        inventory_fields = []
        for key in sample_product.keys():
            if any(word in key.lower() for word in ['inventory', 'stock', 'quantity', 'level', 'amount']):
                inventory_fields.append(key)
        
        if inventory_fields:
            print(f"\n✅ Found inventory-related fields in products:")
            for field in inventory_fields:
                print(f"   - {field}: {sample_product[field]}")
        else:
            print(f"\n❌ No inventory-related fields found in products")
            
        # Check if we have more products than inventory records
        print(f"\n📦 Checking total product count vs inventory records:")
        print(f"   Products available: 8,301 (from our previous export)")
        print(f"   Inventory records: 5,000 (limited by API)")
        print(f"   Missing inventory for ~3,301 products")
            
except Exception as e:
    print(f"❌ Failed to check products: {e}")

print(f"\n" + "=" * 60)
print("Summary: Evaluating if products can supplement inventory data")