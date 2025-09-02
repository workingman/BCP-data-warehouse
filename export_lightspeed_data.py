#!/usr/bin/env python3
"""
Lightspeed X-Series Data Export Tool
Exports POS data from Lightspeed to CSV files for database import
Supports interruption and resuming of exports
"""

import os
import sys
import signal
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

from lightspeed_client import LightspeedClient
from csv_exporter import CSVExporter
from checkpoint_manager import CheckpointManager

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

# Global variables for graceful shutdown
checkpoint_manager = None
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown"""
    global shutdown_requested
    logger.info("Interrupt signal received. Saving progress and shutting down...")
    shutdown_requested = True


def find_existing_export_dirs(output_dir: str) -> list:
    """Find existing export directories that can be resumed"""
    export_dirs = []
    output_path = Path(output_dir)
    
    if output_path.exists():
        for item in output_path.iterdir():
            if item.is_dir() and (item / "checkpoint.json").exists():
                checkpoint_file = item / "checkpoint.json"
                try:
                    import json
                    with open(checkpoint_file) as f:
                        checkpoint_data = json.load(f)
                    if not checkpoint_data.get("export_complete", False):
                        export_dirs.append(item)
                except:
                    continue
    
    return sorted(export_dirs, key=lambda x: x.name, reverse=True)


def prompt_for_resume(existing_dirs: list) -> str:
    """Prompt user to resume an existing export or start new one"""
    print("\nFound incomplete exports:")
    for i, dir_path in enumerate(existing_dirs[:5], 1):  # Show max 5
        checkpoint_file = dir_path / "checkpoint.json"
        try:
            import json
            with open(checkpoint_file) as f:
                data = json.load(f)
            completed = len(data.get("completed_endpoints", []))
            started = data.get("started_at", "Unknown")
            print(f"  {i}. {dir_path.name} (started: {started}, completed: {completed} endpoints)")
        except:
            print(f"  {i}. {dir_path.name} (corrupted checkpoint)")
    
    print(f"  {len(existing_dirs) + 1}. Start new export")
    
    while True:
        try:
            choice = input(f"\nChoose option (1-{len(existing_dirs) + 1}): ").strip()
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(existing_dirs):
                return str(existing_dirs[choice_num - 1])
            elif choice_num == len(existing_dirs) + 1:
                return None  # Start new export
            else:
                print("Invalid choice. Please try again.")
        except (ValueError, KeyboardInterrupt):
            print("Invalid input. Please enter a number.")


def create_checkpoint_callback(checkpoint_mgr: CheckpointManager):
    """Create a callback function for saving checkpoint progress"""
    def callback(endpoint: str, page: int, total_records: int):
        if shutdown_requested:
            return
        
        checkpoint_mgr.save_partial_progress(endpoint, {
            "last_page": page,
            "total_records": total_records,
            "timestamp": datetime.now().isoformat()
        })
    
    return callback


def export_endpoint_with_resume(client: LightspeedClient, exporter: CSVExporter, 
                               checkpoint_mgr: CheckpointManager, endpoint_name: str,
                               fetch_func, export_func):
    """Export a single endpoint with resume capability"""
    global shutdown_requested
    
    if shutdown_requested:
        return False
    
    if checkpoint_mgr.is_endpoint_complete(endpoint_name):
        logger.info(f"Skipping {endpoint_name} (already complete)")
        return True
    
    try:
        # Check for partial progress
        progress = checkpoint_mgr.get_partial_progress(endpoint_name)
        start_page = 1
        append_mode = False
        
        if progress:
            start_page = progress.get("last_page", 1) + 1
            append_mode = True
            logger.info(f"Resuming {endpoint_name} from page {start_page}")
        
        # Create checkpoint callback
        checkpoint_callback = create_checkpoint_callback(checkpoint_mgr)
        
        # Fetch data with resume capability
        if hasattr(fetch_func, '__code__') and 'start_page' in fetch_func.__code__.co_varnames:
            # Function supports pagination resume
            data = fetch_func(start_page=start_page, checkpoint_callback=checkpoint_callback)
        else:
            # Simple function, call normally
            data = fetch_func()
        
        if shutdown_requested:
            logger.info(f"Export interrupted during {endpoint_name}")
            return False
        
        # Export data
        if hasattr(export_func, '__code__') and 'append' in export_func.__code__.co_varnames:
            export_func(data, append=append_mode)
        else:
            export_func(data)
        
        # Mark as complete
        checkpoint_mgr.mark_endpoint_complete(endpoint_name)
        checkpoint_mgr.clear_partial_progress(endpoint_name)
        
        logger.info(f"Completed {endpoint_name}: {len(data)} records")
        return True
        
    except KeyboardInterrupt:
        logger.info(f"Export interrupted during {endpoint_name}")
        shutdown_requested = True
        return False
    except Exception as e:
        logger.error(f"Failed to export {endpoint_name}: {str(e)}")
        return False


def export_all_data():
    """Main function to export all Lightspeed data with resume capability"""
    global checkpoint_manager, shutdown_requested
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Load environment variables
    load_dotenv()
    
    domain = os.getenv('LIGHTSPEED_DOMAIN')
    token = os.getenv('LIGHTSPEED_TOKEN')
    output_dir = os.getenv('OUTPUT_DIR', './exports')
    
    if not domain or not token:
        logger.error("Missing required environment variables. Please check .env file.")
        logger.error("Required: LIGHTSPEED_DOMAIN and LIGHTSPEED_TOKEN")
        sys.exit(1)
    
    # Check for existing exports to resume
    existing_exports = find_existing_export_dirs(output_dir)
    resume_dir = None
    
    if existing_exports:
        resume_dir = prompt_for_resume(existing_exports)
    
    logger.info(f"Starting Lightspeed data export for domain: {domain}")
    
    # Initialize clients
    try:
        client = LightspeedClient(domain, token)
        
        if resume_dir:
            exporter = CSVExporter(output_dir, resume_export_dir=resume_dir)
            checkpoint_manager = CheckpointManager(Path(resume_dir))
            logger.info("Resuming existing export")
            print(checkpoint_manager.get_summary())
        else:
            exporter = CSVExporter(output_dir)
            checkpoint_manager = CheckpointManager(exporter.export_dir)
            logger.info("Starting new export")
            
    except Exception as e:
        logger.error(f"Failed to initialize clients: {str(e)}")
        sys.exit(1)
    
    # Define export tasks
    export_tasks = [
        ("outlets", lambda: client.get_paginated_data('outlets'), exporter.export_outlets),
        ("registers", lambda: client.get_paginated_data('registers'), exporter.export_registers),
        ("users", lambda: client.get_paginated_data('users'), exporter.export_users),
        ("customer_groups", lambda: client.get_paginated_data('customer_groups'), exporter.export_customer_groups),
        ("customers", None, None),  # Handle customers separately with streaming
        ("brands", lambda: client.get_paginated_data('brands'), exporter.export_brands),
        ("product_types", lambda: client.get_paginated_data('product_types'), exporter.export_product_types),
        ("suppliers", lambda: client.get_paginated_data('suppliers'), exporter.export_suppliers),
        ("taxes", lambda: client.get_paginated_data('taxes'), exporter.export_taxes),
        ("payment_types", lambda: client.get_paginated_data('payment_types'), exporter.export_payment_types),
    ]
    
    # Execute basic exports with progress tracking
    successful_exports = 0
    with tqdm(total=len(export_tasks), desc="Basic exports") as pbar:
        for endpoint_name, fetch_func, export_func in export_tasks:
            if shutdown_requested:
                break
            
            # Handle customers with streaming
            if endpoint_name == "customers":
                pbar.set_description("Streaming customers")
                try:
                    if not checkpoint_manager.is_endpoint_complete("customers"):
                        logger.info("Streaming customers export...")
                        
                        progress = checkpoint_manager.get_partial_progress("customers")
                        start_page = 1 if not progress else progress.get("last_page", 1) + 1
                        checkpoint_callback = create_checkpoint_callback(checkpoint_manager)
                        
                        # Stream customers data
                        customer_stream = client.stream_paginated_data('customers', start_page=start_page,
                                                                     checkpoint_callback=checkpoint_callback)
                        
                        # Check if we're resuming (start_page > 1 means we have partial progress)
                        is_resuming = start_page > 1
                        total_customers = exporter.stream_export_customers(customer_stream, is_resuming)
                        
                        checkpoint_manager.mark_endpoint_complete("customers")
                        checkpoint_manager.clear_partial_progress("customers")
                        logger.info(f"Streamed {total_customers} customers")
                        successful_exports += 1
                    else:
                        logger.info("Skipping customers (already complete)")
                        successful_exports += 1
                        
                except Exception as e:
                    logger.error(f"Failed to export customers: {str(e)}")
            else:
                # Handle other endpoints normally
                pbar.set_description(f"Exporting {endpoint_name}")
                success = export_endpoint_with_resume(client, exporter, checkpoint_manager, 
                                                    endpoint_name, fetch_func, export_func)
                if success:
                    successful_exports += 1
            
            pbar.update(1)
    
    if shutdown_requested:
        logger.info("Export interrupted by user. Progress saved.")
        print(f"\nExport interrupted. You can resume later by running the script again.")
        print(f"Export directory: {exporter.export_dir}")
        return
    
    # Handle complex exports (products with variants) - STREAMING
    if not checkpoint_manager.is_endpoint_complete("products"):
        try:
            logger.info("Streaming products export...")
            
            progress = checkpoint_manager.get_partial_progress("products")
            start_page = 1 if not progress else progress.get("last_page", 1) + 1
            checkpoint_callback = create_checkpoint_callback(checkpoint_manager)
            
            # Stream products data
            product_stream = client.stream_paginated_data('products', start_page=start_page, 
                                                        checkpoint_callback=checkpoint_callback)
            
            if not shutdown_requested:
                # Use streaming export for products
                is_resuming = start_page > 1
                total_products = exporter.stream_export_products(product_stream, is_resuming)
                
                # For variants, we still need to collect all products since variants are extracted from product data
                # TODO: This could be optimized to stream variants as well in future versions
                logger.info("Fetching products again for variant extraction...")
                all_products = client.get_paginated_data('products', start_page=start_page)
                exporter.export_product_variants(all_products)
                
                checkpoint_manager.mark_endpoint_complete("products")
                checkpoint_manager.clear_partial_progress("products")
                logger.info(f"Streamed {total_products} products")
            
        except KeyboardInterrupt:
            shutdown_requested = True
        except Exception as e:
            logger.error(f"Failed to export products: {str(e)}")
    
    if shutdown_requested:
        logger.info("Export interrupted during products. Progress saved.")
        return
    
    # Handle sales (large dataset) - STREAMING
    if not checkpoint_manager.is_endpoint_complete("sales"):
        try:
            logger.info("Streaming sales export...")
            
            progress = checkpoint_manager.get_partial_progress("sales")
            start_page = 1 if not progress else progress.get("last_page", 1) + 1
            checkpoint_callback = create_checkpoint_callback(checkpoint_manager)
            
            # Stream sales data
            sales_stream = client.stream_paginated_data('sales', start_page=start_page,
                                                      checkpoint_callback=checkpoint_callback)
            
            if not shutdown_requested:
                # Use streaming export for sales
                is_resuming = start_page > 1
                total_sales = exporter.stream_export_sales(sales_stream, is_resuming)
                
                # For line items and payments, we still need all sales data
                # TODO: This could be optimized to stream these as well in future versions
                logger.info("Fetching sales again for line items and payments extraction...")
                all_sales = client.get_paginated_data('sales', start_page=start_page)
                exporter.export_sale_items(all_sales)
                exporter.export_sale_payments(all_sales)
                
                checkpoint_manager.mark_endpoint_complete("sales")
                checkpoint_manager.clear_partial_progress("sales")
                logger.info(f"Streamed {total_sales} sales")
            
        except KeyboardInterrupt:
            shutdown_requested = True
        except Exception as e:
            logger.error(f"Failed to export sales: {str(e)}")
    
    if shutdown_requested:
        logger.info("Export interrupted during sales. Progress saved.")
        return
    
    # Handle additional optional endpoints
    optional_exports = [
        ("inventory", lambda: client.get_paginated_data('inventory'), exporter.export_inventory),
        ("register_closures", lambda: client.get_paginated_data('register_sales'), exporter.export_register_closures),
        ("price_books", lambda: client.get_paginated_data('price_books'), exporter.export_price_books),
        ("promotions", lambda: client.get_paginated_data('promotions'), exporter.export_promotions),
        ("consignments", lambda: client.get_paginated_data('consignments'), exporter.export_consignments),
        ("gift_cards", lambda: client.get_paginated_data('gift_cards'), exporter.export_gift_cards),
    ]
    
    for endpoint_name, fetch_func, export_func in optional_exports:
        if shutdown_requested:
            break
            
        try:
            success = export_endpoint_with_resume(client, exporter, checkpoint_manager,
                                                endpoint_name, fetch_func, export_func)
            if success:
                successful_exports += 1
        except Exception as e:
            logger.warning(f"Could not export {endpoint_name} (may not be available): {str(e)}")
    
    if not shutdown_requested:
        # Mark export as complete
        checkpoint_manager.mark_export_complete()
        
        # Print summary
        summary = exporter.get_export_summary()
        logger.info(summary)
        print(summary)
        
        logger.info("Export completed successfully!")
        print(f"\n✅ Export completed successfully!")
        print(f"📁 Files saved to: {exporter.export_dir}")
    else:
        print(f"\n⏸️  Export paused. Resume anytime by running the script again.")
        print(f"📁 Partial export saved to: {exporter.export_dir}")


if __name__ == "__main__":
    try:
        export_all_data()
    except KeyboardInterrupt:
        logger.info("Export cancelled by user")
        if checkpoint_manager:
            logger.info("Progress saved")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)