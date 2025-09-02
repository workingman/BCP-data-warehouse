#!/usr/bin/env python3
"""Clarify the difference between products and inventory data"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Clarifying: Products vs Inventory data")
print("=" * 60)

print("📦 PRODUCTS (8,301 records) - Master catalog of items you sell:")
try:
    products_response = client._make_request('products', {'page_size': 3})
    products = products_response.get('data', [])
    
    if products:
        print("Sample product record:")
        sample_product = products[0]
        print(f"   ID: {sample_product.get('id', 'N/A')}")
        print(f"   Name: {sample_product.get('name', 'N/A')}")
        print(f"   SKU: {sample_product.get('sku', 'N/A')}")
        print(f"   Price: ${sample_product.get('price_including_tax', 0)}")
        print(f"   Has Inventory Tracking: {sample_product.get('has_inventory', 'N/A')}")
        print(f"   Active: {sample_product.get('active', 'N/A')}")

except Exception as e:
    print(f"   ❌ Failed: {e}")

print(f"\n📦 INVENTORY (5,000 records) - Stock levels for each product at each location:")
try:
    inventory_response = client._make_request('inventory', {'page_size': 3})
    inventory = inventory_response.get('data', [])
    
    if inventory:
        print("Sample inventory record:")
        sample_inventory = inventory[0]
        print(f"   ID: {sample_inventory.get('id', 'N/A')}")
        print(f"   Product ID: {sample_inventory.get('product_id', 'N/A')}")
        print(f"   Outlet ID: {sample_inventory.get('outlet_id', 'N/A')}")
        print(f"   Current Stock Level: {sample_inventory.get('inventory_level', 'N/A')}")
        print(f"   Current Amount: {sample_inventory.get('current_amount', 'N/A')}")
        print(f"   Average Cost: ${sample_inventory.get('average_cost', 'N/A')}")
        print(f"   Reorder Point: {sample_inventory.get('reorder_point', 'N/A')}")

except Exception as e:
    print(f"   ❌ Failed: {e}")

print(f"\n🔍 ANALYSIS:")
print(f"   Products (8,301): Master catalog of ALL items you can sell")
print(f"   Inventory (5,000): Stock tracking records for products that have inventory")
print(f"   ")
print(f"   The discrepancy makes sense because:")
print(f"   1. Some products don't have inventory tracking (has_inventory=False)")
print(f"   2. Some products might be services, gift cards, etc.")
print(f"   3. The 5,000 inventory records could be the complete set")

print(f"\n🧮 Let's verify this theory:")
try:
    # Count products with has_inventory=True from our export
    print("   Checking products.jsonl for has_inventory field distribution...")
    
    # We'll need to read from our export file
    import json
    from pathlib import Path
    
    products_file = Path('exports/20250901_201804/products.jsonl')
    if products_file.exists():
        has_inventory_count = 0
        no_inventory_count = 0
        
        with open(products_file) as f:
            for line_num, line in enumerate(f):
                if line_num >= 1000:  # Sample first 1000 to avoid processing all 8k
                    break
                try:
                    product = json.loads(line)
                    if product.get('has_inventory', False):
                        has_inventory_count += 1
                    else:
                        no_inventory_count += 1
                except:
                    continue
        
        total_sampled = has_inventory_count + no_inventory_count
        if total_sampled > 0:
            has_inventory_pct = (has_inventory_count / total_sampled) * 100
            estimated_inventory_products = int((8301 * has_inventory_pct) / 100)
            
            print(f"   Sample of {total_sampled} products:")
            print(f"   - With inventory tracking: {has_inventory_count} ({has_inventory_pct:.1f}%)")
            print(f"   - Without inventory tracking: {no_inventory_count} ({100-has_inventory_pct:.1f}%)")
            print(f"   - Estimated products with inventory: ~{estimated_inventory_products:,}")
            
            if estimated_inventory_products <= 5500:  # Close to our 5,000 inventory records
                print(f"   ✅ This explains the difference! We likely have most/all inventory records.")
            else:
                print(f"   🚨 We're still missing inventory records (expected ~{estimated_inventory_products}, got 5,000)")
    
except Exception as e:
    print(f"   ❌ Analysis failed: {e}")

print(f"\n" + "=" * 60)
print("Conclusion: Understanding the relationship between products and inventory")