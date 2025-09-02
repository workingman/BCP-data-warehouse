#!/usr/bin/env python3
"""Analyze product SKUs to understand data patterns"""
import json
from pathlib import Path
import re

print("Analyzing product SKUs from export data...")
print("=" * 60)

products_file = Path('exports/20250901_201804/products.jsonl')

if not products_file.exists():
    print(f"❌ Products file not found: {products_file}")
    exit(1)

print(f"📦 Reading products from {products_file}...")

# Collect SKU data
skus = []
sku_lengths = []
numeric_skus = []
max_sku = ""
min_sku = ""
sku_patterns = {}

print("Processing products...")

with open(products_file) as f:
    for line_num, line in enumerate(f):
        try:
            product = json.loads(line)
            sku = product.get('sku', '')
            
            if sku:
                skus.append(sku)
                sku_lengths.append(len(sku))
                
                # Track longest and shortest
                if not max_sku or len(sku) > len(max_sku):
                    max_sku = sku
                if not min_sku or len(sku) < len(min_sku):
                    min_sku = sku
                
                # Check if numeric
                if sku.isdigit():
                    numeric_skus.append(int(sku))
                
                # Analyze patterns
                if sku.isdigit():
                    pattern = "all_numeric"
                elif re.match(r'^[A-Za-z]+\d+$', sku):
                    pattern = "letters_then_numbers"
                elif re.match(r'^\d+[A-Za-z]+$', sku):
                    pattern = "numbers_then_letters"
                elif re.match(r'^[A-Za-z]+$', sku):
                    pattern = "all_letters"
                elif '-' in sku:
                    pattern = "contains_dash"
                elif '_' in sku:
                    pattern = "contains_underscore"
                else:
                    pattern = "mixed_other"
                
                sku_patterns[pattern] = sku_patterns.get(pattern, 0) + 1
                
        except json.JSONDecodeError:
            continue

print(f"✅ Processed {len(skus):,} products with SKUs")

print(f"\n📊 SKU Analysis:")
print(f"   Total SKUs: {len(skus):,}")
print(f"   Average SKU length: {sum(sku_lengths)/len(sku_lengths):.1f} characters")
print(f"   Shortest SKU: '{min_sku}' ({len(min_sku)} chars)")
print(f"   Longest SKU: '{max_sku}' ({len(max_sku)} chars)")

print(f"\n🔢 Numeric SKUs:")
if numeric_skus:
    numeric_skus.sort()
    print(f"   Count: {len(numeric_skus):,} ({len(numeric_skus)/len(skus)*100:.1f}%)")
    print(f"   Smallest: {min(numeric_skus):,}")
    print(f"   Largest: {max(numeric_skus):,}")
    print(f"   Sample: {numeric_skus[:5]}")
else:
    print(f"   No purely numeric SKUs found")

print(f"\n📝 SKU Patterns:")
for pattern, count in sorted(sku_patterns.items(), key=lambda x: x[1], reverse=True):
    percentage = (count / len(skus)) * 100
    print(f"   {pattern}: {count:,} ({percentage:.1f}%)")

# Show some sample SKUs of different types
print(f"\n📋 Sample SKUs by category:")
sample_by_pattern = {}
with open(products_file) as f:
    for line in f:
        try:
            product = json.loads(line)
            sku = product.get('sku', '')
            name = product.get('name', 'Unnamed')
            
            if sku:
                if sku.isdigit():
                    pattern = "all_numeric"
                elif re.match(r'^[A-Za-z]+\d+$', sku):
                    pattern = "letters_then_numbers"
                elif '-' in sku:
                    pattern = "contains_dash"
                else:
                    pattern = "other"
                
                if pattern not in sample_by_pattern:
                    sample_by_pattern[pattern] = []
                
                if len(sample_by_pattern[pattern]) < 3:
                    sample_by_pattern[pattern].append(f"'{sku}' ({name})")
                    
        except:
            continue

for pattern, samples in sample_by_pattern.items():
    print(f"\n   {pattern}:")
    for sample in samples:
        print(f"     {sample}")

print(f"\n" + "=" * 60)
print("Analysis complete - looking for insights about data organization")