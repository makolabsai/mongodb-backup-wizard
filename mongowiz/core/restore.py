"""Core functionality for MongoDB restore operations."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from bson import ObjectId
from datetime import datetime

logger = logging.getLogger(__name__)

class RestoreError(Exception):
    """Custom exception for restore operations."""
    pass

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

def restore_collection(client, backup_dir: Path, db_name: str, collection_name: str, force: bool = False) -> bool:
    """Restore a MongoDB collection from a backup file.
    
    Args:
        client: MongoDB client instance
        backup_dir: Directory containing backup
        db_name: Database name
        collection_name: Collection name
        force: If True, overwrite existing collection
        
    Returns:
        bool: True if restore was successful, False otherwise
        
    Raises:
        RestoreError: If collection exists and force is False
    """
    try:
        db = client[db_name]
        
        # Check if collection exists
        if collection_name in db.list_collection_names():
            if not force:
                raise RestoreError(f"Collection {collection_name} already exists in database {db_name}")
            db[collection_name].drop()
        
        # Read backup file
        backup_file = backup_dir / db_name / f"{collection_name}.json"
        if not backup_file.exists():
            return False
        
        with open(backup_file) as f:
            documents = json.load(f)
        
        # Convert string format back to ObjectId and datetime
        def restore_types(value):
            if isinstance(value, dict):
                if "$type" in value and "$value" in value:
                    type_name = value["$type"]
                    type_value = value["$value"]
                    if type_name == "ObjectId":
                        return ObjectId(type_value)
                    elif type_name == "datetime":
                        return datetime.fromisoformat(type_value)
                return {k: restore_types(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [restore_types(v) for v in value]
            return value
        
        documents = [restore_types(doc) for doc in documents]
        
        # Insert documents
        if documents:
            db[collection_name].insert_many(documents)
        else:
            # Create empty collection
            db.create_collection(collection_name)
        
        return True
        
    except RestoreError:
        raise
    except Exception as e:
        print(f"Error restoring collection: {e}")
        return False
