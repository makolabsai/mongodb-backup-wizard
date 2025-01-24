"""Core functionality for MongoDB backup operations."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from tqdm import tqdm

from pymongo import MongoClient
from bson import ObjectId

logger = logging.getLogger(__name__)

def json_serialize(obj):
    """Custom JSON serializer for handling MongoDB specific types."""
    try:
        if obj is None:
            return None
        if isinstance(obj, datetime):
            return {"$type": "datetime", "$value": obj.isoformat()}
        if isinstance(obj, ObjectId):
            return {"$type": "ObjectId", "$value": str(obj)}
        if isinstance(obj, (bytes, bytearray)):
            return str(obj)
        return obj
    except Exception as e:
        logger.error(f"Error serializing object of type {type(obj)}: {str(e)}")
        return None

def process_document(doc):
    """Process a single document, handling None values and serialization."""
    if doc is None:
        return {}
    
    try:
        processed = {}
        if not hasattr(doc, 'items'):
            logger.error(f"Document is not a dictionary: {type(doc)}")
            return {}
            
        for k, v in doc.items():
            try:
                if isinstance(v, dict):
                    processed[k] = process_document(v)
                elif isinstance(v, list):
                    processed[k] = [
                        process_document(item) if isinstance(item, dict)
                        else json_serialize(item)
                        for item in v
                    ]
                else:
                    processed[k] = json_serialize(v)
            except Exception as e:
                logger.error(f"Error processing field {k}: {str(e)}")
                processed[k] = None
        return processed
    except Exception as e:
        logger.error(f"Error in process_document: {str(e)}")
        return {}

def get_collections_info(client) -> Dict[str, List[Tuple[str, int, int]]]:
    """Get information about all collections in the MongoDB instance.
    Returns a dict of db_name -> list of (collection_name, doc_count, size)"""
    collections_info = {}
    
    for db_name in client.list_database_names():
        # Skip system databases
        if db_name in ['admin', 'local', 'config']:
            continue
            
        db = client[db_name]
        collections_info[db_name] = []
        
        for coll_name in db.list_collection_names():
            # Skip system collections silently
            if coll_name.startswith('system.'):
                continue
                
            try:
                # Get collection stats
                stats = db.command('collStats', coll_name)
                doc_count = stats.get('count', 0)
                size = stats.get('size', 0)
                collections_info[db_name].append((coll_name, doc_count, size))
            except Exception as e:
                # Skip collections we can't access without logging warnings
                continue
    
    # Remove empty databases
    collections_info = {k: v for k, v in collections_info.items() if v}
    return collections_info

def backup_collection(client, db_name: str, collection_name: str, backup_dir: Path) -> bool:
    """Backup a MongoDB collection to a JSON file.
    
    Args:
        client: MongoDB client instance
        db_name: Database name
        collection_name: Collection name
        backup_dir: Directory to store backup
        
    Returns:
        bool: True if backup was successful, False otherwise
    """
    try:
        db = client[db_name]
        
        # Check if collection exists using list_collection_names (requires less privileges)
        if collection_name not in db.list_collection_names():
            logger.error(f"Collection {collection_name} does not exist in database {db_name}")
            return False
        
        collection = db[collection_name]
        
        # Create backup directory
        try:
            backup_path = backup_dir / db_name
            backup_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create backup directory: {str(e)}")
            return False

        # Get an estimate of total documents using collStats
        try:
            stats = db.command('collStats', collection_name)
            estimated_docs = stats.get('count', 0)
        except Exception as e:
            logger.warning(f"Could not get collection stats, progress may be inaccurate: {e}")
            estimated_docs = 0
        
        # Process and write documents in batches
        backup_file = backup_path / f"{collection_name}.json"
        processed = 0
        documents = []  # Collect all documents first for atomic write
        
        try:
            # Use tqdm with estimated total, will adjust if estimate was off
            with tqdm(total=estimated_docs, desc=f"Backing up {db_name}.{collection_name}", 
                     unit="docs", dynamic_ncols=True) as pbar:
                
                cursor = collection.find(batch_size=1000)
                
                for doc in cursor:
                    # Convert types inline
                    processed_doc = process_document(doc)
                    documents.append(processed_doc)
                    
                    processed += 1
                    pbar.update(1)
                    
                    # If our estimate was low, update total
                    if processed > estimated_docs:
                        pbar.total = processed + 1000
            
            # Write all documents at once for atomicity
            with open(backup_file, 'w') as f:
                json.dump(documents, f, indent=2)
                
            logger.info(f"Successfully backed up {processed} documents to {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed during backup: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error during backup: {str(e)}")
        return False
