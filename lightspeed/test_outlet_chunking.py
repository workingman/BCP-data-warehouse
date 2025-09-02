#!/usr/bin/env python3
"""Test outlet-based chunking for inventory data"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing outlet-based inventory chunking...")
print("=" * 60)

# Get all outlets first
print("📦 Step 1: Getting all outlets...")
try:
    outlets_response = client._make_request('outlets', {'page_size': 100})
    outlets = outlets_response.get('data', [])
    print(f"   Found {len(outlets)} outlets:")
    for outlet in outlets:
        print(f"   - {outlet['id']}: {outlet.get('name', 'Unnamed')}")
        
except Exception as e:
    print(f"   ❌ Failed to get outlets: {e}")
    exit(1)

# Test inventory for each outlet
print(f"\n📦 Step 2: Testing inventory by outlet...")
total_inventory_records = 0
outlet_inventory_counts = {}

for i, outlet in enumerate(outlets):
    outlet_id = outlet['id']
    outlet_name = outlet.get('name', f'Outlet_{i}')
    
    try:
        print(f"\n   Testing outlet: {outlet_name} ({outlet_id})")
        
        # Try different page sizes to find the limit
        for page_size in [1000, 5000, 10000]:
            response = client._make_request('inventory', {
                'page_size': page_size,
                'outlet_id': outlet_id
            })
            data = response.get('data', [])
            
            print(f"     page_size={page_size}: {len(data)} records")
            
            # If we get fewer records than page_size, we've found the true count
            if len(data) < page_size:
                outlet_inventory_counts[outlet_id] = len(data)
                total_inventory_records += len(data)
                print(f"     ✅ Final count: {len(data)} records")
                break
            elif page_size == 10000:
                # Hit the limit even at 10k
                outlet_inventory_counts[outlet_id] = len(data)
                total_inventory_records += len(data)
                print(f"     🚨 Hit limit at {len(data)} records (may be more)")
                break
                
    except Exception as e:
        print(f"     ❌ Failed: {e}")
        outlet_inventory_counts[outlet_id] = 0

# Summary
print(f"\n" + "=" * 60)
print("SUMMARY:")
print(f"Total outlets: {len(outlets)}")
print(f"Estimated total inventory records: {total_inventory_records}")

print(f"\nPer-outlet breakdown:")
for outlet_id, count in outlet_inventory_counts.items():
    outlet_name = next((o.get('name', 'Unnamed') for o in outlets if o['id'] == outlet_id), 'Unknown')
    print(f"  {outlet_name}: {count:,} records")

if total_inventory_records > 5000:
    print(f"\n✅ SUCCESS: Outlet-based chunking can get {total_inventory_records:,} records!")
    print(f"   This is {total_inventory_records - 5000:,} more than the monolithic approach.")
else:
    print(f"\n❌ Outlet chunking didn't help - still only {total_inventory_records} records total.")