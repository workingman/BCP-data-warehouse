#!/usr/bin/env python3
"""Test the brands pagination fix"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')

client = LightspeedClient(domain, token)

print("Testing fixed brands pagination...")
total = 0
page_count = 0

for page_data in client.stream_paginated_data('brands'):
    page_count += 1
    total += len(page_data)
    print(f"Page {page_count}: {len(page_data)} records (total: {total})")
    
    if page_count > 5:  # Safety break
        break

print(f"Total: {total} unique brands in {page_count} page(s)")