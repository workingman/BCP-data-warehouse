#!/usr/bin/env python3
"""
Convert JSONL export files to CSV format
Extracts nested data (variants, line items, payments) into separate CSV files
"""

import json
import csv
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JSONLToCSVConverter:
    """Convert JSONL files to normalized CSV tables"""
    
    def __init__(self, jsonl_dir: str, csv_dir: str = None):
        """
        Initialize converter
        
        Args:
            jsonl_dir: Directory containing JSONL files
            csv_dir: Output directory for CSV files (defaults to jsonl_dir/csv)
        """
        self.jsonl_dir = Path(jsonl_dir)
        if not self.jsonl_dir.exists():
            raise ValueError(f"JSONL directory does not exist: {jsonl_dir}")
        
        if csv_dir:
            self.csv_dir = Path(csv_dir)
        else:
            self.csv_dir = self.jsonl_dir / 'csv'
        
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Converting JSONL from {self.jsonl_dir} to CSV in {self.csv_dir}")
    
    def read_jsonl(self, filename: str):
        """Generator to read JSONL file"""
        filepath = self.jsonl_dir / filename
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    
    def write_csv(self, filename: str, records: List[Dict], fieldnames: List[str]):
        """Write records to CSV file"""
        if not records:
            logger.warning(f"No records to write for {filename}")
            return
        
        filepath = self.csv_dir / filename
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)
        
        logger.info(f"Wrote {len(records):,} records to {filename}")
    
    def convert_customers(self):
        """Convert customers.jsonl to customers.csv"""
        records = []
        fieldnames = [
            'id', 'customer_code', 'first_name', 'last_name', 'email',
            'phone', 'mobile', 'company_name', 'customer_group_id',
            'enable_loyalty', 'loyalty_balance', 'note', 'gender',
            'date_of_birth', 'created_at', 'updated_at'
        ]
        
        for record in self.read_jsonl('customers.jsonl'):
            # Remove metadata field
            record.pop('_exported_at', None)
            records.append(record)
        
        self.write_csv('customers.csv', records, fieldnames)
    
    def convert_products(self):
        """Convert products.jsonl to products.csv and product_variants.csv"""
        products = []
        variants = []
        
        product_fields = [
            'id', 'source_id', 'handle', 'sku', 'name', 'description',
            'brand_id', 'supplier_id', 'product_type_id', 'supply_price',
            'retail_price', 'tag_string', 'is_active', 'track_inventory',
            'created_at', 'updated_at'
        ]
        
        variant_fields = [
            'id', 'product_id', 'name', 'sku', 'barcode',
            'retail_price', 'supply_price', 'is_active'
        ]
        
        for product in self.read_jsonl('products.jsonl'):
            product.pop('_exported_at', None)
            
            # Extract variants if present
            if 'variant_options' in product and product['variant_options']:
                for variant in product['variant_options']:
                    variant_data = {
                        'id': variant.get('id'),
                        'product_id': product.get('id'),
                        'name': variant.get('name'),
                        'sku': variant.get('sku'),
                        'barcode': variant.get('barcode'),
                        'retail_price': variant.get('retail_price'),
                        'supply_price': variant.get('supply_price'),
                        'is_active': variant.get('is_active')
                    }
                    variants.append(variant_data)
            
            # Remove nested data from product
            product.pop('variant_options', None)
            product.pop('inventory', None)  # Remove inventory array if present
            products.append(product)
        
        self.write_csv('products.csv', products, product_fields)
        self.write_csv('product_variants.csv', variants, variant_fields)
    
    def convert_sales(self):
        """Convert sales.jsonl to sales.csv, sale_items.csv, and sale_payments.csv"""
        sales = []
        items = []
        payments = []
        
        sale_fields = [
            'id', 'source_id', 'outlet_id', 'register_id', 'user_id',
            'customer_id', 'invoice_number', 'receipt_number', 'status',
            'note', 'total_price', 'total_tax', 'total_discount',
            'total_loyalty', 'created_at', 'updated_at', 'sale_date'
        ]
        
        item_fields = [
            'id', 'sale_id', 'product_id', 'variant_id', 'quantity',
            'price', 'cost', 'price_total', 'discount', 'discount_total',
            'tax', 'tax_total', 'status'
        ]
        
        payment_fields = [
            'id', 'sale_id', 'register_id', 'payment_type_id',
            'amount', 'payment_date'
        ]
        
        for sale in self.read_jsonl('sales.jsonl'):
            sale.pop('_exported_at', None)
            
            # Extract line items
            if 'line_items' in sale and sale['line_items']:
                for item in sale['line_items']:
                    item_data = {
                        'id': item.get('id'),
                        'sale_id': sale.get('id'),
                        'product_id': item.get('product_id'),
                        'variant_id': item.get('variant_id'),
                        'quantity': item.get('quantity'),
                        'price': item.get('price'),
                        'cost': item.get('cost'),
                        'price_total': item.get('price_total'),
                        'discount': item.get('discount'),
                        'discount_total': item.get('discount_total'),
                        'tax': item.get('tax'),
                        'tax_total': item.get('tax_total'),
                        'status': item.get('status')
                    }
                    items.append(item_data)
            
            # Extract payments
            if 'payments' in sale and sale['payments']:
                for payment in sale['payments']:
                    payment_data = {
                        'id': payment.get('id'),
                        'sale_id': sale.get('id'),
                        'register_id': payment.get('register_id'),
                        'payment_type_id': payment.get('payment_type_id'),
                        'amount': payment.get('amount'),
                        'payment_date': payment.get('payment_date')
                    }
                    payments.append(payment_data)
            
            # Remove nested data from sale
            sale.pop('line_items', None)
            sale.pop('payments', None)
            sale.pop('taxes', None)  # Remove tax array if present
            sales.append(sale)
        
        self.write_csv('sales.csv', sales, sale_fields)
        self.write_csv('sale_items.csv', items, item_fields)
        self.write_csv('sale_payments.csv', payments, payment_fields)
    
    def convert_simple_endpoint(self, endpoint: str, fieldnames: List[str]):
        """Convert a simple endpoint (no nested data) to CSV"""
        records = []
        for record in self.read_jsonl(f'{endpoint}.jsonl'):
            record.pop('_exported_at', None)
            records.append(record)
        
        self.write_csv(f'{endpoint}.csv', records, fieldnames)
    
    def convert_all(self):
        """Convert all JSONL files to CSV"""
        logger.info("Starting JSONL to CSV conversion...")
        
        # Convert simple endpoints
        simple_conversions = {
            'outlets': ['id', 'name', 'physical_address_1', 'physical_address_2',
                       'physical_city', 'physical_state', 'physical_postcode',
                       'phone', 'email', 'timezone', 'currency'],
            'registers': ['id', 'name', 'outlet_id', 'receipt_prefix',
                         'receipt_suffix', 'receipt_number', 'is_open'],
            'users': ['id', 'username', 'display_name', 'email',
                     'outlet_id', 'is_active', 'created_at'],
            'customer_groups': ['id', 'name', 'discount_percentage'],
            'brands': ['id', 'name', 'description'],
            'product_types': ['id', 'name', 'parent_id'],
            'suppliers': ['id', 'name', 'contact_name', 'email', 'phone',
                         'address_1', 'address_2', 'city', 'state', 'postcode'],
            'taxes': ['id', 'name', 'rate', 'outlet_id'],
            'payment_types': ['id', 'name', 'outlet_id'],
            'inventory': ['id', 'product_id', 'outlet_id', 'quantity_available',
                         'reorder_point', 'reorder_amount', 'updated_at'],
        }
        
        for endpoint, fields in simple_conversions.items():
            if (self.jsonl_dir / f'{endpoint}.jsonl').exists():
                logger.info(f"Converting {endpoint}...")
                self.convert_simple_endpoint(endpoint, fields)
        
        # Convert complex endpoints with nested data
        if (self.jsonl_dir / 'customers.jsonl').exists():
            logger.info("Converting customers...")
            self.convert_customers()
        
        if (self.jsonl_dir / 'products.jsonl').exists():
            logger.info("Converting products and variants...")
            self.convert_products()
        
        if (self.jsonl_dir / 'sales.jsonl').exists():
            logger.info("Converting sales, items, and payments...")
            self.convert_sales()
        
        logger.info(f"\n✅ Conversion complete! CSV files saved to: {self.csv_dir}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python jsonl_to_csv.py <jsonl_directory> [csv_directory]")
        print("\nExample:")
        print("  python jsonl_to_csv.py exports/20240831_120000")
        print("  python jsonl_to_csv.py exports/20240831_120000 output/csv")
        sys.exit(1)
    
    jsonl_dir = sys.argv[1]
    csv_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        converter = JSONLToCSVConverter(jsonl_dir, csv_dir)
        converter.convert_all()
    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()