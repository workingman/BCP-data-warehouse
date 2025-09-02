#!/usr/bin/env python3
"""
Lightspeed X-Series Data Export Tool - JSONL Format
Exports POS data from Lightspeed to JSONL files for flexible post-processing
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
from jsonl_exporter import JSONLExporter
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


def export_endpoint_streaming(client: LightspeedClient, exporter: JSONLExporter,
                            checkpoint_mgr: CheckpointManager, endpoint: str):
    """
    Export a single endpoint using streaming to JSONL
    
    Args:
        client: Lightspeed API client
        exporter: JSONL exporter
        checkpoint_mgr: Checkpoint manager
        endpoint: API endpoint name
    
    Returns:
        True if successful, False if interrupted or failed
    """
    global shutdown_requested
    
    if shutdown_requested:
        return False
    
    if checkpoint_mgr.is_endpoint_complete(endpoint):
        logger.info(f"Skipping {endpoint} (already complete)")
        return True
    
    try:
        # Check for partial progress
        progress = checkpoint_mgr.get_partial_progress(endpoint)
        start_page = 1 if not progress else progress.get("last_page", 1) + 1
        is_resuming = start_page > 1
        
        if is_resuming:
            logger.info(f"Resuming {endpoint} from page {start_page}")
        
        # Create checkpoint callback
        checkpoint_callback = create_checkpoint_callback(checkpoint_mgr)
        
        # Stream data directly to JSONL
        data_stream = client.stream_paginated_data(
            endpoint, 
            start_page=start_page,
            checkpoint_callback=checkpoint_callback
        )
        
        total_records = exporter.stream_export_data(endpoint, data_stream, is_resuming)
        
        if not shutdown_requested:
            checkpoint_mgr.mark_endpoint_complete(endpoint)
            checkpoint_mgr.clear_partial_progress(endpoint)
            logger.info(f"Completed {endpoint}: {total_records} new records")
            return True
        else:
            logger.info(f"Export interrupted during {endpoint}")
            return False
            
    except KeyboardInterrupt:
        logger.info(f"Export interrupted during {endpoint}")
        shutdown_requested = True
        return False
    except Exception as e:
        logger.error(f"Failed to export {endpoint}: {str(e)}")
        return False


def export_all_data():
    """Main function to export all Lightspeed data to JSONL format"""
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
            exporter = JSONLExporter(output_dir, resume_export_dir=resume_dir)
            checkpoint_manager = CheckpointManager(Path(resume_dir))
            logger.info("Resuming existing export")
            print(checkpoint_manager.get_summary())
        else:
            exporter = JSONLExporter(output_dir)
            checkpoint_manager = CheckpointManager(exporter.export_dir)
            logger.info("Starting new export")
            
    except Exception as e:
        logger.error(f"Failed to initialize clients: {str(e)}")
        sys.exit(1)
    
    # Define all endpoints to export
    # Order matters: export reference data first, then transactional data
    endpoints = [
        # Reference data (small, rarely changes)
        'outlets',
        'registers', 
        'users',
        'customer_groups',
        'brands',
        'product_types',
        'suppliers',
        'taxes',
        'payment_types',
        
        # Master data (medium size)
        'customers',
        'products',
        
        # Transactional data (large, grows over time)
        'inventory',
        'sales',
        
        # Optional/specialized endpoints
        'register_sales',  # Register closures
        'price_books',
        'promotions',
        'consignments',
        'gift_cards',
    ]
    
    # Execute exports with progress tracking
    successful_exports = 0
    failed_endpoints = []
    
    with tqdm(total=len(endpoints), desc="Exporting endpoints") as pbar:
        for endpoint in endpoints:
            if shutdown_requested:
                break
            
            pbar.set_description(f"Exporting {endpoint}")
            
            # Main endpoints that should always work
            if endpoint in ['outlets', 'registers', 'users', 'customer_groups', 
                          'customers', 'products', 'sales', 'inventory',
                          'brands', 'product_types', 'suppliers', 'taxes', 'payment_types']:
                success = export_endpoint_streaming(client, exporter, checkpoint_manager, endpoint)
                if success:
                    successful_exports += 1
                else:
                    failed_endpoints.append(endpoint)
            
            # Optional endpoints that might not be available
            else:
                try:
                    success = export_endpoint_streaming(client, exporter, checkpoint_manager, endpoint)
                    if success:
                        successful_exports += 1
                except Exception as e:
                    logger.warning(f"Could not export {endpoint} (may not be available): {str(e)}")
            
            pbar.update(1)
    
    # Print summary
    if not shutdown_requested:
        checkpoint_manager.mark_export_complete()
        summary = exporter.get_export_summary()
        logger.info(summary)
        print(summary)
        
        if failed_endpoints:
            print(f"\n⚠️  Failed endpoints: {', '.join(failed_endpoints)}")
        
        print(f"\n✅ Export completed successfully!")
        print(f"📁 JSONL files saved to: {exporter.export_dir}")
        print(f"\nNext steps:")
        print(f"1. Process JSONL files to CSV: python jsonl_to_csv.py {exporter.export_dir}")
        print(f"2. Import to MongoDB: mongoimport --db lightspeed --collection [name] < [file].jsonl")
        print(f"3. Query with jq: jq '.customer_id' < sales.jsonl | sort | uniq -c")
    else:
        print(f"\n⏸️  Export paused. Resume anytime by running the script again.")
        print(f"📁 Partial export saved to: {exporter.export_dir}")
        
        if failed_endpoints:
            print(f"\n⚠️  Failed endpoints: {', '.join(failed_endpoints)}")


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