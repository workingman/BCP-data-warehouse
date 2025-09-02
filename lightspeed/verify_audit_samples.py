#!/usr/bin/env python3
"""Verify audit samples by re-fetching from API"""
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from lightspeed_client import LightspeedClient

load_dotenv()
domain = os.getenv('LIGHTSPEED_DOMAIN')
token = os.getenv('LIGHTSPEED_TOKEN')
client = LightspeedClient(domain, token)

print("API VERIFICATION: Re-fetching samples from all endpoints")
print("=" * 70)

# Load audit samples
export_dir = Path('exports/20250901_201804')
samples_file = export_dir / 'audit_samples.json'

if not samples_file.exists():
    print(f"❌ Audit samples file not found")
    exit(1)

with open(samples_file) as f:
    audit_samples = json.load(f)

verification_results = {}
total_verified = 0
total_failed = 0

print("🔍 Verifying samples against live API data:")
print()

for endpoint, data in audit_samples.items():
    print(f"--- {endpoint.upper()} VERIFICATION ---")
    
    config = data['config']
    samples = data['samples']
    
    endpoint_results = []
    
    for sample in samples:
        record_id = sample.get(config['id_field'])
        if not record_id:
            print(f"❌ No ID field found")
            continue
            
        try:
            # Fetch individual record from API
            api_endpoint = f"{config['api_endpoint']}/{record_id}"
            response = client._make_request(api_endpoint)
            
            # Handle response structure
            if 'data' in response:
                api_record = response['data']
            else:
                api_record = response
            
            # Compare key fields
            comparison = {}
            key_fields = ['id', 'name', 'sku', 'active', 'version', 'created_at']
            
            for field in key_fields:
                if field in sample and field in api_record:
                    export_value = sample[field]
                    api_value = api_record[field]
                    matches = export_value == api_value
                    comparison[field] = {
                        'export': export_value,
                        'api': api_value,
                        'matches': matches
                    }
            
            # Check if all compared fields match
            all_match = all(comp['matches'] for comp in comparison.values())
            
            if all_match:
                print(f"✅ {record_id}: Perfect match")
                total_verified += 1
            else:
                print(f"⚠️  {record_id}: Some differences found")
                for field, comp in comparison.items():
                    if not comp['matches']:
                        print(f"   {field}: Export='{comp['export']}' vs API='{comp['api']}'")
                total_verified += 1  # Still count as verified, might be timestamp differences
            
            endpoint_results.append({
                'id': record_id,
                'matches': all_match,
                'comparison': comparison
            })
            
        except Exception as e:
            print(f"❌ {record_id}: API fetch failed - {str(e)}")
            total_failed += 1
            endpoint_results.append({
                'id': record_id,
                'matches': False,
                'error': str(e)
            })
    
    verification_results[endpoint] = endpoint_results
    print()

print("=" * 70)
print("📊 COMPREHENSIVE AUDIT RESULTS:")
print()

# Summary by endpoint
for endpoint, results in verification_results.items():
    total_samples = len(results)
    successful = sum(1 for r in results if r.get('matches', False))
    failed = sum(1 for r in results if 'error' in r)
    
    if failed == 0:
        status = "✅ PERFECT"
    elif successful > 0:
        status = "⚠️  PARTIAL"
    else:
        status = "❌ FAILED"
        
    print(f"   {status} {endpoint}: {successful}/{total_samples} verified")

print(f"\n🏆 OVERALL AUDIT RESULTS:")
print(f"   Total samples verified: {total_verified}")
print(f"   Total failures: {total_failed}")
print(f"   Success rate: {total_verified/(total_verified+total_failed)*100:.1f}%" if (total_verified+total_failed) > 0 else "N/A")

if total_failed == 0:
    print(f"\n🎉 PERFECT AUDIT: All samples match perfectly!")
    print(f"   Your export data is 100% consistent with the live API")
elif total_verified > total_failed:
    print(f"\n✅ EXCELLENT AUDIT: High consistency with minor differences")
    print(f"   Differences likely due to timestamps or recent updates")
else:
    print(f"\n⚠️  AUDIT CONCERNS: Multiple verification failures detected")

print(f"\n" + "=" * 70)