#!/usr/bin/env python3
"""Compare consignments data with inventory data to understand the difference"""
import json
from pathlib import Path

print("Analyzing Consignments vs Inventory data...")
print("=" * 60)

# Read both datasets
consignments_file = Path('exports/20250901_201804/consignments.jsonl')
inventory_file = Path('exports/20250901_201804/inventory.jsonl')

if not consignments_file.exists() or not inventory_file.exists():
    print(f"❌ Required files not found")
    exit(1)

print("📦 CONSIGNMENTS DATA (433 records):")
print("Looking at sample consignment records...")

consignment_data = []
with open(consignments_file) as f:
    for i, line in enumerate(f):
        if i >= 5:  # Just get first 5 for analysis
            break
        try:
            consignment = json.loads(line)
            consignment_data.append(consignment)
        except json.JSONDecodeError:
            continue

if consignment_data:
    print("\nSample consignment record structure:")
    sample = consignment_data[0]
    for key, value in sample.items():
        print(f"   {key}: {str(value)[:100]}{'...' if len(str(value)) > 100 else ''} ({type(value).__name__})")

print(f"\n📦 INVENTORY DATA (5,000 records) - for comparison:")
print("Looking at sample inventory records...")

with open(inventory_file) as f:
    for line in f:
        try:
            inventory = json.loads(line)
            print("\nSample inventory record structure:")
            for key, value in list(inventory.items())[:10]:  # First 10 fields
                print(f"   {key}: {str(value)[:100]}{'...' if len(str(value)) > 100 else ''} ({type(value).__name__})")
            break
        except json.JSONDecodeError:
            continue

print(f"\n🔍 COMPARATIVE ANALYSIS:")

# Load all consignments for analysis
all_consignments = []
with open(consignments_file) as f:
    for line in f:
        try:
            consignment = json.loads(line)
            all_consignments.append(consignment)
        except json.JSONDecodeError:
            continue

print(f"\nConsignments count: {len(all_consignments)}")

# Analyze what consignments contain
if all_consignments:
    # Look for common fields that might indicate purpose
    sample_consignment = all_consignments[0]
    
    # Check for product-related fields
    product_fields = [k for k in sample_consignment.keys() if 'product' in k.lower()]
    quantity_fields = [k for k in sample_consignment.keys() if any(word in k.lower() for word in ['qty', 'quantity', 'count', 'amount'])]
    cost_fields = [k for k in sample_consignment.keys() if any(word in k.lower() for word in ['cost', 'price', 'value', 'total'])]
    date_fields = [k for k in sample_consignment.keys() if any(word in k.lower() for word in ['date', 'time', 'created', 'updated'])]
    supplier_fields = [k for k in sample_consignment.keys() if any(word in k.lower() for word in ['supplier', 'vendor', 'consignor'])]
    
    print(f"\nKey field categories found in consignments:")
    print(f"   Product-related fields: {product_fields}")
    print(f"   Quantity fields: {quantity_fields}")  
    print(f"   Cost/Price fields: {cost_fields}")
    print(f"   Date fields: {date_fields}")
    print(f"   Supplier fields: {supplier_fields}")
    
    # Sample values for key fields
    print(f"\nSample values from first consignment:")
    for field in ['id', 'name', 'outlet_id', 'supplier_id']:
        if field in sample_consignment:
            print(f"   {field}: {sample_consignment[field]}")
    
    # Check if consignments have status or state information
    status_fields = [k for k in sample_consignment.keys() if any(word in k.lower() for word in ['status', 'state', 'type', 'kind'])]
    if status_fields:
        print(f"   Status fields: {status_fields}")
        for field in status_fields:
            print(f"   {field}: {sample_consignment[field]}")

# Try to understand the business purpose
print(f"\n💡 HYPOTHESIS - What consignments likely represent:")
print(f"   Consignments are typically inventory received from suppliers")
print(f"   that you haven't paid for yet (goods on consignment).")
print(f"   ")
print(f"   INVENTORY (5,000 records): Current stock levels per product/location")
print(f"   CONSIGNMENTS (433 records): Shipments/receipts of inventory from suppliers")
print(f"   ")
print(f"   Think of it as:")
print(f"   - INVENTORY = \"How much do I have in stock?\"")
print(f"   - CONSIGNMENTS = \"What shipments did I receive?\"")

# Check for overlapping product IDs
print(f"\n🔗 Checking for product overlap:")
consignment_product_ids = set()
for consignment in all_consignments:
    # Look for product-related IDs in consignments
    for key, value in consignment.items():
        if 'product' in key.lower() and isinstance(value, str) and len(value) > 10:
            consignment_product_ids.add(value)

if consignment_product_ids:
    print(f"   Found {len(consignment_product_ids)} unique product references in consignments")
else:
    print(f"   No obvious product ID references found in consignments")

print(f"\n" + "=" * 60)
print("Summary: Understanding the business purpose of consignments vs inventory")