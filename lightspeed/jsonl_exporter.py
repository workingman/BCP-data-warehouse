import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class JSONLExporter:
    """Export Lightspeed data to JSONL (JSON Lines) format"""
    
    def __init__(self, output_dir: str = "./exports", resume_export_dir: str = None):
        """
        Initialize JSONL exporter
        
        Args:
            output_dir: Directory to save JSONL files
            resume_export_dir: Existing export directory to resume from
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        if resume_export_dir:
            # Resume existing export
            self.export_dir = Path(resume_export_dir)
            self.timestamp = self.export_dir.name
            logger.info(f"Resuming export in: {self.export_dir}")
        else:
            # Create new export directory
            self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.export_dir = self.output_dir / self.timestamp
            self.export_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Starting new export in: {self.export_dir}")
    
    def _append_jsonl(self, filename: str, records: List[Dict[str, Any]]):
        """
        Append records to a JSONL file
        
        Args:
            filename: Name of JSONL file (e.g., 'customers.jsonl')
            records: List of records to append
        """
        if not records:
            return
            
        filepath = self.export_dir / filename
        
        with open(filepath, 'a', encoding='utf-8') as f:
            for record in records:
                # Add metadata to each record
                record['_exported_at'] = datetime.utcnow().isoformat()
                json.dump(record, f, ensure_ascii=False, default=str)
                f.write('\n')
        
        logger.debug(f"Appended {len(records)} records to {filepath}")
    
    def stream_export_data(self, endpoint: str, data_stream, is_resuming: bool = False):
        """
        Stream export data to JSONL format
        
        Args:
            endpoint: API endpoint name (used as filename)
            data_stream: Iterator/generator that yields chunks of data
            is_resuming: Whether we're resuming an interrupted export
        
        Returns:
            Total number of records written
        """
        filename = f"{endpoint}.jsonl"
        filepath = self.export_dir / filename
        
        # Log whether we're creating new or appending
        if is_resuming and filepath.exists():
            # Count existing records for accurate reporting
            existing_count = sum(1 for _ in open(filepath, 'r'))
            logger.info(f"Resuming {filename} with {existing_count} existing records")
        else:
            existing_count = 0
            logger.info(f"Creating new JSONL file: {filepath}")
        
        total_records = 0
        for chunk in data_stream:
            if chunk:
                self._append_jsonl(filename, chunk)
                total_records += len(chunk)
        
        logger.info(f"Exported {total_records} new records to {filename} (total: {existing_count + total_records})")
        return total_records
    
    def get_export_summary(self) -> str:
        """Get summary of exported JSONL files"""
        files = list(self.export_dir.glob('*.jsonl'))
        summary = f"\nExport completed to: {self.export_dir}\n"
        summary += "JSONL files created:\n"
        
        for file in sorted(files):
            # Count records in each file
            record_count = sum(1 for _ in open(file, 'r'))
            size = file.stat().st_size / 1024  # KB
            summary += f"  - {file.name}: {record_count:,} records ({size:.1f} KB)\n"
        
        return summary
    
    def count_records(self, endpoint: str) -> int:
        """
        Count records in a JSONL file
        
        Args:
            endpoint: API endpoint name
            
        Returns:
            Number of records in the file, or 0 if file doesn't exist
        """
        filename = f"{endpoint}.jsonl"
        filepath = self.export_dir / filename
        
        if not filepath.exists():
            return 0
            
        return sum(1 for _ in open(filepath, 'r'))