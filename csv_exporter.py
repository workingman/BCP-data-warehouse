import csv
import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class CSVExporter:
    """Export Lightspeed data to CSV files"""
    
    def __init__(self, output_dir: str = "./exports"):
        """
        Initialize CSV exporter
        
        Args:
            output_dir: Directory to save CSV files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Add timestamp to prevent overwriting
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.export_dir = self.output_dir / self.timestamp
        self.export_dir.mkdir(parents=True, exist_ok=True)
    
    def _write_csv(self, filename: str, data: List[Dict], fieldnames: List[str]):
        """
        Write data to CSV file
        
        Args:
            filename: Name of CSV file
            data: List of dictionaries to write
            fieldnames: List of field names for CSV header
        """
        filepath = self.export_dir / filename
        
        if not data:
            logger.warning(f"No data to export for {filename}")
            return
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Exported {len(data)} records to {filepath}")
    
    def export_customers(self, customers: List[Dict]):
        """Export customer data"""
        fieldnames = [
            'id', 'customer_code', 'first_name', 'last_name', 'email', 
            'phone', 'mobile', 'company_name', 'customer_group_id',
            'enable_loyalty', 'loyalty_balance', 'note', 'gender',
            'date_of_birth', 'created_at', 'updated_at'
        ]
        self._write_csv('customers.csv', customers, fieldnames)
    
    def export_customer_groups(self, groups: List[Dict]):
        """Export customer group data"""
        fieldnames = ['id', 'name', 'discount_percentage']
        self._write_csv('customer_groups.csv', groups, fieldnames)
    
    def export_products(self, products: List[Dict]):
        """Export product data"""
        fieldnames = [
            'id', 'source_id', 'handle', 'sku', 'name', 'description',
            'brand_id', 'supplier_id', 'product_type_id', 'supply_price',
            'retail_price', 'tag_string', 'is_active', 'track_inventory',
            'created_at', 'updated_at'
        ]
        self._write_csv('products.csv', products, fieldnames)
    
    def export_product_variants(self, products: List[Dict]):
        """Export product variants from product data"""
        variants = []
        for product in products:
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
        
        fieldnames = [
            'id', 'product_id', 'name', 'sku', 'barcode',
            'retail_price', 'supply_price', 'is_active'
        ]
        self._write_csv('product_variants.csv', variants, fieldnames)
    
    def export_inventory(self, inventory: List[Dict]):
        """Export inventory data"""
        fieldnames = [
            'id', 'product_id', 'outlet_id', 'quantity_available',
            'reorder_point', 'reorder_amount', 'updated_at'
        ]
        self._write_csv('inventory.csv', inventory, fieldnames)
    
    def export_sales(self, sales: List[Dict]):
        """Export sales data"""
        fieldnames = [
            'id', 'source_id', 'outlet_id', 'register_id', 'user_id',
            'customer_id', 'invoice_number', 'receipt_number', 'status',
            'note', 'total_price', 'total_tax', 'total_discount',
            'total_loyalty', 'created_at', 'updated_at', 'sale_date'
        ]
        self._write_csv('sales.csv', sales, fieldnames)
    
    def export_sale_items(self, sales: List[Dict]):
        """Export sale line items from sales data"""
        items = []
        for sale in sales:
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
        
        fieldnames = [
            'id', 'sale_id', 'product_id', 'variant_id', 'quantity',
            'price', 'cost', 'price_total', 'discount', 'discount_total',
            'tax', 'tax_total', 'status'
        ]
        self._write_csv('sale_items.csv', items, fieldnames)
    
    def export_sale_payments(self, sales: List[Dict]):
        """Export payment records from sales data"""
        payments = []
        for sale in sales:
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
        
        fieldnames = [
            'id', 'sale_id', 'register_id', 'payment_type_id',
            'amount', 'payment_date'
        ]
        self._write_csv('sale_payments.csv', payments, fieldnames)
    
    def export_outlets(self, outlets: List[Dict]):
        """Export outlet data"""
        fieldnames = [
            'id', 'name', 'physical_address_1', 'physical_address_2',
            'physical_city', 'physical_state', 'physical_postcode',
            'physical_country_id', 'phone', 'email', 'timezone',
            'default_tax_id', 'currency', 'currency_symbol'
        ]
        self._write_csv('outlets.csv', outlets, fieldnames)
    
    def export_registers(self, registers: List[Dict]):
        """Export register data"""
        fieldnames = [
            'id', 'name', 'outlet_id', 'receipt_prefix',
            'receipt_suffix', 'receipt_number', 'is_open'
        ]
        self._write_csv('registers.csv', registers, fieldnames)
    
    def export_register_closures(self, closures: List[Dict]):
        """Export register closure data"""
        fieldnames = [
            'id', 'register_id', 'opened_at', 'closed_at',
            'opening_float', 'closing_float', 'total_counted',
            'cash_counted', 'cash_expected', 'cash_difference'
        ]
        self._write_csv('register_closures.csv', closures, fieldnames)
    
    def export_users(self, users: List[Dict]):
        """Export user data"""
        fieldnames = [
            'id', 'username', 'display_name', 'email',
            'outlet_id', 'is_active', 'created_at'
        ]
        self._write_csv('users.csv', users, fieldnames)
    
    def export_suppliers(self, suppliers: List[Dict]):
        """Export supplier data"""
        fieldnames = [
            'id', 'name', 'contact_name', 'email', 'phone',
            'address_1', 'address_2', 'city', 'state',
            'postcode', 'country_id'
        ]
        self._write_csv('suppliers.csv', suppliers, fieldnames)
    
    def export_taxes(self, taxes: List[Dict]):
        """Export tax data"""
        fieldnames = ['id', 'name', 'rate', 'outlet_id']
        self._write_csv('taxes.csv', taxes, fieldnames)
    
    def export_payment_types(self, payment_types: List[Dict]):
        """Export payment type data"""
        fieldnames = ['id', 'name', 'outlet_id']
        self._write_csv('payment_types.csv', payment_types, fieldnames)
    
    def export_brands(self, brands: List[Dict]):
        """Export brand data"""
        fieldnames = ['id', 'name', 'description']
        self._write_csv('brands.csv', brands, fieldnames)
    
    def export_product_types(self, types: List[Dict]):
        """Export product type data"""
        fieldnames = ['id', 'name', 'parent_id']
        self._write_csv('product_types.csv', types, fieldnames)
    
    def export_price_books(self, price_books: List[Dict]):
        """Export price book data"""
        fieldnames = [
            'id', 'name', 'outlet_id', 'customer_group_id',
            'valid_from', 'valid_to', 'is_active'
        ]
        self._write_csv('price_books.csv', price_books, fieldnames)
    
    def export_promotions(self, promotions: List[Dict]):
        """Export promotion data"""
        fieldnames = [
            'id', 'name', 'type', 'value', 'start_date',
            'end_date', 'is_active'
        ]
        self._write_csv('promotions.csv', promotions, fieldnames)
    
    def export_consignments(self, consignments: List[Dict]):
        """Export consignment data"""
        fieldnames = [
            'id', 'outlet_id', 'supplier_id', 'invoice_number',
            'consignment_date', 'received_at', 'total_cost',
            'status', 'type'
        ]
        self._write_csv('consignments.csv', consignments, fieldnames)
    
    def export_gift_cards(self, gift_cards: List[Dict]):
        """Export gift card data"""
        fieldnames = [
            'id', 'number', 'balance', 'customer_id',
            'expires_at', 'created_at', 'status'
        ]
        self._write_csv('gift_cards.csv', gift_cards, fieldnames)
    
    def get_export_summary(self) -> str:
        """Get summary of exported files"""
        files = list(self.export_dir.glob('*.csv'))
        summary = f"\nExport completed to: {self.export_dir}\n"
        summary += "Files created:\n"
        for file in files:
            size = file.stat().st_size / 1024  # KB
            summary += f"  - {file.name} ({size:.1f} KB)\n"
        return summary