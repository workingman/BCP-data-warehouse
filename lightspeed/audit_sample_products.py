#!/usr/bin/env python3
"""Sample random products from export for web UI audit"""
import json
import random
from pathlib import Path

print("Selecting random products for web UI audit...")
print("=" * 60)

products_file = Path('exports/20250901_201804/products.jsonl')
if not products_file.exists():
    print(f"❌ Products file not found")
    exit(1)

# Load all products
products = []
with open(products_file) as f:
    for line in f:
        try:
            product = json.loads(line)
            products.append(product)
        except json.JSONDecodeError:
            continue

print(f"📦 Loaded {len(products):,} products from export")

# Select random sample for audit
sample_size = 5
random_products = random.sample(products, min(sample_size, len(products)))

print(f"\n🎲 Selected {len(random_products)} random products for audit:")
print()

for i, product in enumerate(random_products, 1):
    print(f"PRODUCT {i}:")
    print(f"   ID: {product.get('id', 'N/A')}")
    print(f"   SKU: {product.get('sku', 'N/A')}")
    print(f"   Name: {product.get('name', 'N/A')}")
    print(f"   Price: ${product.get('price_including_tax', 0)}")
    print(f"   Active: {product.get('active', 'N/A')}")
    print(f"   Has Inventory: {product.get('has_inventory', 'N/A')}")
    print(f"   Created: {product.get('created_at', 'N/A')}")
    print()

# Also get some high/low SKU products for range verification
print("📊 Additional samples for range verification:")

# Get products with lowest and highest numeric SKUs
numeric_sku_products = [p for p in products if p.get('sku', '').isdigit()]
if numeric_sku_products:
    numeric_sku_products.sort(key=lambda x: int(x.get('sku', '0')))
    
    lowest_sku = numeric_sku_products[0]
    highest_sku = numeric_sku_products[-1]
    
    print(f"\nLOWEST SKU PRODUCT:")
    print(f"   SKU: {lowest_sku.get('sku')} - {lowest_sku.get('name')}")
    
    print(f"\nHIGHEST SKU PRODUCT:")
    print(f"   SKU: {highest_sku.get('sku')} - {highest_sku.get('name')}")

print(f"\n" + "=" * 60)
print("Ready for web UI verification of these samples")