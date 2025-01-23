"""Core functionality for MongoDB restore operations."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

def get_collections_info(backup_folder: Path) -> Dict[str, List[Tuple[str, int, int]]]:
    """Get information about all collections in the backup.
    Returns a dict of db_name -> list of (collection_name, doc_count, size)"""
    collections_info = {}
    
    try:
        for db_dir in backup_folder.iterdir():
            if not db_dir.is_dir():
                continue
                
            collections_info[db_dir.name] = []
            
            for collection_file in db_dir.glob("*.json"):
                try:
                    # Count documents and get file size
                    with open(collection_file) as f:
                        data = json.load(f)
                        doc_count = len(data)
                        size = collection_file.stat().st_size
                        
                    collections_info[db_dir.name].append(
                        (collection_file.stem, doc_count, size)
                    )
                except Exception as e:
                    logger.error(f"Error reading collection file {collection_file}: {str(e)}")
                    continue
        
        # Remove empty databases
        collections_info = {k: v for k, v in collections_info.items() if v}
        return collections_info
        
    except Exception as e:
        logger.error(f"Error reading backup folder: {str(e)}")
        return {}

def restore_collection(client, backup_path: Path, db_name: str, collection_name: str) -> bool:
    """Execute the restore operation with the given parameters."""
    try:
        # Get database and collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Read backup file
        backup_file = backup_path / db_name / f"{collection_name}.json"
        if not backup_file.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        # Load documents
        with open(backup_file) as f:
            documents = json.load(f)
        
        # Process in batches
        batch_size = 1000
        total_docs = len(documents)
        processed = 0
        
        while processed < total_docs:
            batch = documents[processed:processed + batch_size]
            if batch:
                collection.insert_many(batch)
                processed += len(batch)
                logger.info(f"Restored {processed}/{total_docs} documents")
        
        logger.info(f"Successfully restored {processed} documents to {db_name}.{collection_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error restoring collection: {str(e)}")
        return False
