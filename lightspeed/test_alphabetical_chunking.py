#!/usr/bin/env python3
"""Test alphabetical chunking by product_id for inventory data"""
import os
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("Testing alphabetical chunking for inventory...")
print("=" * 60)

# The challenge: Lightspeed API might not support partial product_id matching
# Let's try different approaches

print("📦 Approach 1: Direct filtering (unlikely to work but worth testing)")

# Test filtering by first character of product_id
hex_chars = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
total_found = 0

for char in hex_chars[:4]:  # Test first few characters
    try:
        print(f"\n   Testing product_id prefix '{char}':")
        
        # Different ways the API might interpret filtering
        test_params = [
            {'product_id__startswith': char},  # Django-style
            {'product_id__prefix': char},      # Custom prefix filter
            {'product_id_prefix': char},       # Alternative naming
            {'q': char},                       # General search
        ]
        
        for params in test_params:
            try:
                response = client._make_request('inventory', {**params, 'page_size': 100})
                data = response.get('data', [])
                param_name = list(params.keys())[0]
                print(f"     {param_name}: {len(data)} records")
                
                if len(data) > 0:
                    # Check if the filtering actually worked
                    first_product_id = data[0].get('product_id', '')
                    if first_product_id.lower().startswith(char.lower()):
                        print(f"     ✅ Filtering worked! First product_id: {first_product_id[:12]}...")
                    else:
                        print(f"     ❌ Filter ignored. First product_id: {first_product_id[:12]}...")
                        
            except Exception as e:
                print(f"     {param_name}: Failed ({str(e)[:30]}...)")
                
    except Exception as e:
        print(f"   ❌ Failed for '{char}': {e}")

print(f"\n📦 Approach 2: Post-processing chunking")
print("If direct filtering doesn't work, we can:")
print("1. Get the 5,000 records we can access")
print("2. Analyze their product_id distribution") 
print("3. Identify gaps that suggest missing data")

# Get sample data to analyze
try:
    print(f"\n   Analyzing current 5,000 records...")
    response = client._make_request('inventory', {'page_size': 5000})
    data = response.get('data', [])
    
    if data:
        product_ids = [record.get('product_id', '') for record in data]
        
        # Analyze first character distribution
        first_chars = {}
        for pid in product_ids:
            if pid:
                first_char = pid[0].lower()
                first_chars[first_char] = first_chars.get(first_char, 0) + 1
        
        print(f"   First character distribution in 5,000 records:")
        for char in sorted(first_chars.keys()):
            print(f"     '{char}': {first_chars[char]:,} records")
        
        # Check for suspicious patterns
        expected_per_char = len(product_ids) / 16  # 16 hex characters
        print(f"\n   Expected per character if evenly distributed: ~{expected_per_char:.0f}")
        
        suspicious_gaps = []
        for char in hex_chars:
            if char not in first_chars:
                suspicious_gaps.append(char)
        
        if suspicious_gaps:
            print(f"   🚨 Missing characters entirely: {suspicious_gaps}")
            print(f"   This suggests we're missing inventory records starting with: {', '.join(suspicious_gaps)}")

except Exception as e:
    print(f"   ❌ Analysis failed: {e}")

print(f"\n" + "=" * 60)
print("Conclusion: Checking if alphabetical filtering is supported")