import json
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages checkpoints for resumable data exports"""
    
    def __init__(self, export_dir: Path):
        """
        Initialize checkpoint manager
        
        Args:
            export_dir: Directory where export is being saved
        """
        self.export_dir = export_dir
        self.checkpoint_file = export_dir / "checkpoint.json"
        self.checkpoint_data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load existing checkpoint or create new one"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
                    return data
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
                return self._create_new_checkpoint()
        else:
            return self._create_new_checkpoint()
    
    def _create_new_checkpoint(self) -> Dict[str, Any]:
        """Create a new checkpoint structure"""
        return {
            "started_at": datetime.now().isoformat(),
            "completed_endpoints": [],
            "partial_progress": {},
            "last_updated": datetime.now().isoformat(),
            "export_complete": False
        }
    
    def save(self):
        """Save current checkpoint to disk"""
        self.checkpoint_data["last_updated"] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
            logger.debug(f"Checkpoint saved to {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def mark_endpoint_complete(self, endpoint: str):
        """Mark an endpoint as completely exported"""
        if endpoint not in self.checkpoint_data["completed_endpoints"]:
            self.checkpoint_data["completed_endpoints"].append(endpoint)
            self.save()
            logger.info(f"Marked {endpoint} as complete")
    
    def is_endpoint_complete(self, endpoint: str) -> bool:
        """Check if an endpoint has been completely exported"""
        return endpoint in self.checkpoint_data["completed_endpoints"]
    
    def save_partial_progress(self, endpoint: str, progress: Dict[str, Any]):
        """Save partial progress for an endpoint (e.g., last page fetched)"""
        self.checkpoint_data["partial_progress"][endpoint] = progress
        self.save()
    
    def get_partial_progress(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Get partial progress for an endpoint"""
        return self.checkpoint_data["partial_progress"].get(endpoint)
    
    def clear_partial_progress(self, endpoint: str):
        """Clear partial progress for an endpoint"""
        if endpoint in self.checkpoint_data["partial_progress"]:
            del self.checkpoint_data["partial_progress"][endpoint]
            self.save()
    
    def mark_export_complete(self):
        """Mark the entire export as complete"""
        self.checkpoint_data["export_complete"] = True
        self.checkpoint_data["completed_at"] = datetime.now().isoformat()
        self.save()
    
    def is_export_complete(self) -> bool:
        """Check if the export is complete"""
        return self.checkpoint_data.get("export_complete", False)
    
    def get_summary(self) -> str:
        """Get a summary of the checkpoint status"""
        completed = len(self.checkpoint_data["completed_endpoints"])
        partial = len(self.checkpoint_data["partial_progress"])
        
        summary = f"Checkpoint Status:\n"
        summary += f"  Started: {self.checkpoint_data['started_at']}\n"
        summary += f"  Completed endpoints: {completed}\n"
        summary += f"  Partial progress: {partial}\n"
        summary += f"  Export complete: {self.is_export_complete()}\n"
        
        if completed > 0:
            summary += f"\nCompleted:\n"
            for endpoint in self.checkpoint_data["completed_endpoints"][:5]:
                summary += f"  - {endpoint}\n"
            if completed > 5:
                summary += f"  ... and {completed - 5} more\n"
        
        return summary