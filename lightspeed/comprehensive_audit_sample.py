#!/usr/bin/env python3
"""Sample records from all endpoints for comprehensive audit"""
import json
import random
from pathlib import Path

print("COMPREHENSIVE AUDIT: Sampling records from all endpoints")
print("=" * 70)

export_dir = Path('exports/20250901_201804')
if not export_dir.exists():
    print(f"❌ Export directory not found")
    exit(1)

# Define endpoints that support individual record fetching via API
auditable_endpoints = {
    'products': {'id_field': 'id', 'api_endpoint': 'products'},
    'customers': {'id_field': 'id', 'api_endpoint': 'customers'},
    'brands': {'id_field': 'id', 'api_endpoint': 'brands'},
    'suppliers': {'id_field': 'id', 'api_endpoint': 'suppliers'},
    'users': {'id_field': 'id', 'api_endpoint': 'users'},
    'outlets': {'id_field': 'id', 'api_endpoint': 'outlets'},
    'registers': {'id_field': 'id', 'api_endpoint': 'registers'},
    'taxes': {'id_field': 'id', 'api_endpoint': 'taxes'},
    'payment_types': {'id_field': 'id', 'api_endpoint': 'payment_types'},
    'customer_groups': {'id_field': 'id', 'api_endpoint': 'customer_groups'},
    'product_types': {'id_field': 'id', 'api_endpoint': 'product_types'},
    'inventory': {'id_field': 'id', 'api_endpoint': 'inventory'},
    'consignments': {'id_field': 'id', 'api_endpoint': 'consignments'},
}

audit_samples = {}

print("🎲 Sampling records from each endpoint:")
print()

for endpoint, config in auditable_endpoints.items():
    jsonl_file = export_dir / f"{endpoint}.jsonl"
    
    if not jsonl_file.exists():
        print(f"❌ {endpoint}: File not found")
        continue
    
    # Load all records from this endpoint
    records = []
    with open(jsonl_file) as f:
        for line in f:
            try:
                record = json.loads(line)
                if record.get(config['id_field']):  # Must have ID for API verification
                    records.append(record)
            except json.JSONDecodeError:
                continue
    
    if not records:
        print(f"❌ {endpoint}: No valid records found")
        continue
    
    # Sample 1 record (or all if fewer than 1)
    sample_size = min(1, len(records))
    sample_records = random.sample(records, sample_size)
    
    audit_samples[endpoint] = {
        'config': config,
        'total_records': len(records),
        'samples': sample_records
    }
    
    print(f"✅ {endpoint}: Sampled {sample_size} of {len(records):,} records")
    for i, record in enumerate(sample_records):
        record_id = record.get(config['id_field'])
        name_field = record.get('name', record.get('sku', record.get('handle', str(record_id)[:12])))
        print(f"   Sample: {record_id} ({name_field})")

# Save samples for API verification
samples_file = export_dir / 'audit_samples.json'
with open(samples_file, 'w') as f:
    json.dump(audit_samples, f, indent=2, default=str)

print(f"\n📋 AUDIT SAMPLE SUMMARY:")
print(f"   Total endpoints sampled: {len(audit_samples)}")
print(f"   Total records to verify: {sum(len(data['samples']) for data in audit_samples.values())}")
print(f"   Samples saved to: {samples_file}")

print(f"\n📊 Sample distribution:")
for endpoint, data in audit_samples.items():
    sample_count = len(data['samples'])
    total_count = data['total_records']
    print(f"   {endpoint}: {sample_count} sample(s) from {total_count:,} records")

print(f"\n" + "=" * 70)
print("Ready for API verification of samples")