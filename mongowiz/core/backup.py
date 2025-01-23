"""Core functionality for MongoDB backup operations."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from pymongo import MongoClient
from bson import ObjectId

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
        
        # Get all documents
        try:
            documents = list(collection.find())
            logger.info(f"Retrieved {len(documents)} documents from {db_name}.{collection_name}")
        except Exception as e:
            logger.error(f"Failed to retrieve documents: {str(e)}")
            return False
        
        # Convert ObjectId and datetime to string format
        def convert_types(doc):
            if isinstance(doc, dict):
                return {k: convert_types(v) for k, v in doc.items()}
            elif isinstance(doc, list):
                return [convert_types(v) for v in doc]
            elif isinstance(doc, (ObjectId, datetime)):
                return {"$type": doc.__class__.__name__, "$value": str(doc)}
            return doc
        
        try:
            documents = [convert_types(doc) for doc in documents]
            logger.info("Successfully converted document types")
        except Exception as e:
            logger.error(f"Failed to convert document types: {str(e)}")
            return False
        
        # Write to file
        try:
            backup_file = backup_path / f"{collection_name}.json"
            with open(backup_file, "w") as f:
                json.dump(documents, f, indent=2)
            logger.info(f"Successfully wrote backup to {backup_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to write backup file: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error during backup: {str(e)}")
        return False
