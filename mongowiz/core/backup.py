"""Core functionality for MongoDB backup operations."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from pymongo import MongoClient

logger = logging.getLogger(__name__)

def json_serialize(obj):
    """Custom JSON serializer for handling MongoDB specific types."""
    try:
        if obj is None:
            return ""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, (bytes, bytearray)):
            return str(obj)
        return str(obj)
    except Exception as e:
        logger.error(f"Error serializing object of type {type(obj)}: {str(e)}")
        return ""

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
                processed[k] = ""
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
    """Backup a single collection to the specified directory."""
    try:
        # Create backup directory structure
        db_dir = backup_dir / db_name
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Get collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Get total document count
        total_docs = collection.count_documents({})
        
        # Create backup file
        backup_file = db_dir / f"{collection_name}.json"
        
        # Process documents in batches
        batch_size = 1000
        processed = 0
        
        with open(backup_file, 'w') as f:
            f.write('[\n')
            
            cursor = collection.find({})
            first = True
            
            for doc in cursor:
                if not first:
                    f.write(',\n')
                first = False
                
                # Process document
                processed_doc = process_document(doc)
                json.dump(processed_doc, f, indent=2)
                processed += 1
                
                # Log progress
                if processed % batch_size == 0:
                    logger.info(f"Processed {processed}/{total_docs} documents")
            
            f.write('\n]')
        
        logger.info(f"Successfully backed up {processed} documents to {backup_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error backing up collection: {str(e)}")
        return False
