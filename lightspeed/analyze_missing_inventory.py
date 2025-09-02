#!/usr/bin/env python3
"""Cross-reference products and inventory to find missing inventory data"""
import json
from pathlib import Path

print("Analyzing missing inventory by cross-referencing with products...")
print("=" * 60)

# Read products data
products_file = Path('exports/20250901_201804/products.jsonl')
inventory_file = Path('exports/20250901_201804/inventory.jsonl')

if not products_file.exists() or not inventory_file.exists():
    print(f"❌ Required files not found")
    exit(1)

print("📦 Step 1: Loading product data...")
products_by_id = {}
product_skus = []

with open(products_file) as f:
    for line in f:
        try:
            product = json.loads(line)
            product_id = product.get('id')
            sku = product.get('sku', '')
            has_inventory = product.get('has_inventory', False)
            
            if product_id:
                products_by_id[product_id] = {
                    'sku': sku,
                    'name': product.get('name', 'Unnamed'),
                    'has_inventory': has_inventory
                }
                
                if has_inventory and sku and sku.isdigit():
                    product_skus.append(int(sku))
                    
        except json.JSONDecodeError:
            continue

print(f"   Loaded {len(products_by_id):,} products")
print(f"   Products with numeric SKUs & inventory tracking: {len(product_skus):,}")

print("📦 Step 2: Loading inventory data...")
inventory_product_ids = set()
inventory_skus = []

with open(inventory_file) as f:
    for line in f:
        try:
            inventory = json.loads(line)
            product_id = inventory.get('product_id')
            
            if product_id:
                inventory_product_ids.add(product_id)
                
                # Look up the SKU for this product
                product = products_by_id.get(product_id, {})
                sku = product.get('sku', '')
                
                if sku and sku.isdigit():
                    inventory_skus.append(int(sku))
                    
        except json.JSONDecodeError:
            continue

print(f"   Loaded {len(inventory_product_ids):,} inventory records")
print(f"   Inventory records with numeric SKUs: {len(inventory_skus):,}")

print("📦 Step 3: Finding missing inventory...")

# Find products that should have inventory but don't
missing_inventory_products = []
for product_id, product_data in products_by_id.items():
    if product_data['has_inventory'] and product_id not in inventory_product_ids:
        missing_inventory_products.append(product_data)

print(f"   Products missing inventory records: {len(missing_inventory_products):,}")

print("📦 Step 4: SKU range analysis...")

if product_skus and inventory_skus:
    product_skus.sort()
    inventory_skus.sort()
    
    print(f"\n📊 SKU Range Comparison:")
    print(f"   Products with inventory tracking:")
    print(f"     Count: {len(product_skus):,}")
    print(f"     SKU Range: {min(product_skus):,} to {max(product_skus):,}")
    
    print(f"   Inventory records found:")
    print(f"     Count: {len(inventory_skus):,}")
    print(f"     SKU Range: {min(inventory_skus):,} to {max(inventory_skus):,}")
    
    # Find missing SKU ranges
    product_sku_set = set(product_skus)
    inventory_sku_set = set(inventory_skus)
    missing_skus = product_sku_set - inventory_sku_set
    
    if missing_skus:
        missing_skus_list = sorted(list(missing_skus))
        print(f"\n🚨 Missing inventory for {len(missing_skus):,} products:")
        print(f"   Missing SKU range: {min(missing_skus_list):,} to {max(missing_skus_list):,}")
        print(f"   Sample missing SKUs: {missing_skus_list[:10]}")
        
        # Analyze if there's a pattern to missing SKUs
        if len(missing_skus_list) > 10:
            # Check if missing SKUs are clustered in ranges
            ranges = []
            start = missing_skus_list[0]
            prev = start
            
            for sku in missing_skus_list[1:] + [float('inf')]:
                if sku > prev + 1:  # Gap found
                    ranges.append((start, prev))
                    start = sku
                prev = sku
            
            print(f"\n📍 Missing SKU ranges:")
            for i, (range_start, range_end) in enumerate(ranges[:10]):
                if range_start == range_end:
                    print(f"     SKU {range_start:,}")
                else:
                    print(f"     SKUs {range_start:,} to {range_end:,} ({range_end-range_start+1:,} products)")
                if i == 9 and len(ranges) > 10:
                    print(f"     ... and {len(ranges)-10} more ranges")
    else:
        print(f"\n✅ All products with inventory tracking have inventory records!")

print(f"\n" + "=" * 60)
print("Summary: Understanding which inventory data is missing")