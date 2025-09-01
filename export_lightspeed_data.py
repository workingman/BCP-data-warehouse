#!/usr/bin/env python3
"""
Lightspeed X-Series Data Export Tool
Exports POS data from Lightspeed to CSV files for database import
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

from lightspeed_client import LightspeedClient
from csv_exporter import CSVExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def export_all_data():
    """Main function to export all Lightspeed data"""
    
    # Load environment variables
    load_dotenv()
    
    domain = os.getenv('LIGHTSPEED_DOMAIN')
    token = os.getenv('LIGHTSPEED_TOKEN')
    output_dir = os.getenv('OUTPUT_DIR', './exports')
    
    if not domain or not token:
        logger.error("Missing required environment variables. Please check .env file.")
        logger.error("Required: LIGHTSPEED_DOMAIN and LIGHTSPEED_TOKEN")
        sys.exit(1)
    
    logger.info(f"Starting Lightspeed data export for domain: {domain}")
    
    # Initialize clients
    try:
        client = LightspeedClient(domain, token)
        exporter = CSVExporter(output_dir)
    except Exception as e:
        logger.error(f"Failed to initialize clients: {str(e)}")
        sys.exit(1)
    
    # Define export tasks with progress tracking
    export_tasks = [
        ("Fetching outlets", client.get_outlets, exporter.export_outlets),
        ("Fetching registers", client.get_registers, exporter.export_registers),
        ("Fetching users", client.get_users, exporter.export_users),
        ("Fetching customer groups", client.get_customer_groups, exporter.export_customer_groups),
        ("Fetching customers", client.get_customers, exporter.export_customers),
        ("Fetching brands", client.get_brands, exporter.export_brands),
        ("Fetching product types", client.get_product_types, exporter.export_product_types),
        ("Fetching suppliers", client.get_suppliers, exporter.export_suppliers),
        ("Fetching taxes", client.get_taxes, exporter.export_taxes),
        ("Fetching payment types", client.get_payment_types, exporter.export_payment_types),
    ]
    
    # Execute basic exports
    with tqdm(total=len(export_tasks), desc="Exporting data") as pbar:
        for description, fetch_func, export_func in export_tasks:
            try:
                pbar.set_description(description)
                data = fetch_func()
                export_func(data)
                pbar.update(1)
            except Exception as e:
                logger.error(f"Failed to export {description}: {str(e)}")
                continue
    
    # Handle products (includes variants)
    try:
        logger.info("Fetching products (this may take a while)...")
        products = client.get_products()
        exporter.export_products(products)
        exporter.export_product_variants(products)
        logger.info(f"Exported {len(products)} products")
    except Exception as e:
        logger.error(f"Failed to export products: {str(e)}")
    
    # Handle inventory
    try:
        logger.info("Fetching inventory...")
        inventory = client.get_inventory()
        exporter.export_inventory(inventory)
        logger.info(f"Exported {len(inventory)} inventory records")
    except Exception as e:
        logger.error(f"Failed to export inventory: {str(e)}")
    
    # Handle sales (includes line items and payments)
    try:
        logger.info("Fetching sales (this may take a while)...")
        sales = client.get_sales()
        exporter.export_sales(sales)
        exporter.export_sale_items(sales)
        exporter.export_sale_payments(sales)
        logger.info(f"Exported {len(sales)} sales")
    except Exception as e:
        logger.error(f"Failed to export sales: {str(e)}")
    
    # Handle additional endpoints if they exist
    optional_exports = [
        ("register closures", client.get_register_closures, exporter.export_register_closures),
        ("price books", client.get_price_books, exporter.export_price_books),
        ("promotions", client.get_promotions, exporter.export_promotions),
        ("consignments", client.get_consignments, exporter.export_consignments),
        ("gift cards", client.get_gift_cards, exporter.export_gift_cards),
    ]
    
    for description, fetch_func, export_func in optional_exports:
        try:
            logger.info(f"Attempting to fetch {description}...")
            data = fetch_func()
            export_func(data)
            logger.info(f"Exported {len(data)} {description}")
        except Exception as e:
            logger.warning(f"Could not export {description} (may not be available): {str(e)}")
    
    # Print summary
    summary = exporter.get_export_summary()
    logger.info(summary)
    print(summary)
    
    logger.info("Export completed successfully!")


if __name__ == "__main__":
    try:
        export_all_data()
    except KeyboardInterrupt:
        logger.info("Export cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)