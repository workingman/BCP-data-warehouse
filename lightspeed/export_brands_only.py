#!/usr/bin/env python3
"""Export just the brands data with correct page size"""
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

print("Exporting all brands with page_size=250...")

with open(brands_file, 'w') as f:
    total_brands = 0
    for page_data in client.stream_paginated_data('brands'):
        for brand in page_data:
            # Add export timestamp
            brand['_exported_at'] = datetime.utcnow().isoformat()
            json.dump(brand, f, ensure_ascii=False, default=str)
            f.write('\n')
            total_brands += 1

print(f"✅ Exported {total_brands} brands to {brands_file}")

# Verify uniqueness
unique_ids = set()
with open(brands_file) as f:
    for line in f:
        brand = json.loads(line)
        unique_ids.add(brand['id'])

print(f"✅ Verified {len(unique_ids)} unique brands in file")