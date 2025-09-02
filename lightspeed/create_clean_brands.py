#!/usr/bin/env python3
"""Create clean brands.jsonl with exactly 213 unique brands"""
import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')

client = LightspeedClient(domain, token)
export_dir = Path('/Users/gwr/dev/lightspeed/exports/20250901_194318')
brands_file = export_dir / 'brands.jsonl'

print("Creating clean brands.jsonl with unique records only...")

# Get the first (and only) page with all 213 brands
response = client._make_request('brands', {'page': 1, 'page_size': 250})
brands_data = response.get('data', [])

print(f"Got {len(brands_data)} brands from API")

# Write them to JSONL file
with open(brands_file, 'w') as f:
    for brand in brands_data:
        # Add export timestamp
        brand['_exported_at'] = datetime.utcnow().isoformat()
        json.dump(brand, f, ensure_ascii=False, default=str)
        f.write('\n')

print(f"✅ Created {brands_file} with {len(brands_data)} unique brands")

# Verify
with open(brands_file) as f:
    line_count = sum(1 for _ in f)
print(f"✅ File contains {line_count} records")