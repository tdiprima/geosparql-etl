"""Checkpoint and state management utilities for resumable ETL operations."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


class CheckpointManager:
    """Manages checkpoint state for resumable ETL operations.

    Example usage:
        checkpoint = CheckpointManager("etl_checkpoint.json")
        checkpoint.mark_processed("exec123", "img456")

        if checkpoint.is_processed("exec123", "img456"):
            print("Already processed!")

        checkpoint.save()
    """

    def __init__(self, checkpoint_file: str):
        """Initialize checkpoint manager.

        Args:
            checkpoint_file: Path to checkpoint file
        """
        self.checkpoint_file = Path(checkpoint_file)
        self.data: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        """Load checkpoint data from file.

        Returns:
            Dict containing checkpoint data
        """
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return self._default_checkpoint()
        return self._default_checkpoint()

    def _default_checkpoint(self) -> Dict[str, Any]:
        """Create default checkpoint structure.

        Returns:
            Default checkpoint dictionary
        """
        return {
            "processed": [],
            "failed": [],
            "last_updated": None,
            "metadata": {},
        }

    def save(self) -> None:
        """Save checkpoint data to file."""
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def mark_processed(self, *identifiers: str) -> None:
        """Mark items as processed.

        Args:
            *identifiers: One or more identifiers to mark as processed

        Example:
            checkpoint.mark_processed("exec123_img456")
            checkpoint.mark_processed("exec123", "img456")
        """
        key = "_".join(identifiers)
        if key not in self.data["processed"]:
            self.data["processed"].append(key)

    def mark_failed(self, *identifiers: str, error: Optional[str] = None) -> None:
        """Mark items as failed.

        Args:
            *identifiers: One or more identifiers to mark as failed
            error: Optional error message
        """
        key = "_".join(identifiers)
        failed_entry = {"key": key, "timestamp": datetime.now().isoformat()}
        if error:
            failed_entry["error"] = error
        self.data["failed"].append(failed_entry)

    def is_processed(self, *identifiers: str) -> bool:
        """Check if items have been processed.

        Args:
            *identifiers: One or more identifiers to check

        Returns:
            bool: True if already processed
        """
        key = "_".join(identifiers)
        return key in self.data["processed"]

    def get_processed_keys(self) -> Set[str]:
        """Get set of all processed keys.

        Returns:
            Set of processed keys
        """
        return set(self.data["processed"])

    def get_failed_keys(self) -> List[Dict[str, Any]]:
        """Get list of all failed items with metadata.

        Returns:
            List of failed items
        """
        return self.data["failed"]

    def set_metadata(self, key: str, value: Any) -> None:
        """Store metadata in checkpoint.

        Args:
            key: Metadata key
            value: Metadata value
        """
        self.data["metadata"][key] = value

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Retrieve metadata from checkpoint.

        Args:
            key: Metadata key
            default: Default value if key not found

        Returns:
            Metadata value or default
        """
        return self.data["metadata"].get(key, default)

    def clear(self) -> None:
        """Clear all checkpoint data."""
        self.data = self._default_checkpoint()
        self.save()

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics.

        Returns:
            Dictionary with processing stats
        """
        return {
            "processed_count": len(self.data["processed"]),
            "failed_count": len(self.data["failed"]),
            "last_updated": self.data.get("last_updated"),
        }


class SimpleCheckpoint:
    """Simplified checkpoint manager for basic use cases.

    Example usage:
        checkpoint = SimpleCheckpoint("checkpoint.json")

        for item in items:
            if item['id'] in checkpoint:
                continue
            process(item)
            checkpoint.add(item['id'])
            checkpoint.save()
    """

    def __init__(self, checkpoint_file: str):
        """Initialize simple checkpoint manager.

        Args:
            checkpoint_file: Path to checkpoint file
        """
        self.checkpoint_file = Path(checkpoint_file)
        self.processed: Set[str] = self._load()

    def _load(self) -> Set[str]:
        """Load processed items from file.

        Returns:
            Set of processed item IDs
        """
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return set(data)
                    elif isinstance(data, dict) and "processed" in data:
                        return set(data["processed"])
            except json.JSONDecodeError:
                pass
        return set()

    def save(self) -> None:
        """Save checkpoint to file."""
        with open(self.checkpoint_file, "w") as f:
            json.dump(list(self.processed), f, indent=2)

    def add(self, item_id: str) -> None:
        """Add item to checkpoint.

        Args:
            item_id: Item identifier
        """
        self.processed.add(item_id)

    def __contains__(self, item_id: str) -> bool:
        """Check if item is in checkpoint.

        Args:
            item_id: Item identifier

        Returns:
            bool: True if item is processed
        """
        return item_id in self.processed

    def __len__(self) -> int:
        """Get number of processed items.

        Returns:
            int: Count of processed items
        """
        return len(self.processed)

    def clear(self) -> None:
        """Clear all processed items."""
        self.processed.clear()
        self.save()
