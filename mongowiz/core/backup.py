"""Core functionality for MongoDB backup operations."""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from tqdm import tqdm

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
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

def backup_collection(client, db_name: str, collection_name: str, backup_dir: Path, batch_size: int = 1000, 
                     max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Backup a MongoDB collection to a JSON file.
    
    Args:
        client: MongoDB client instance
        db_name: Database name
        collection_name: Collection name
        backup_dir: Directory to store backup
        batch_size: Number of documents to process in each batch (default: 1000)
        max_retries: Maximum number of retry attempts for failed operations (default: 3)
        retry_delay: Delay in seconds between retry attempts (default: 5)
        
    Returns:
        bool: True if backup was successful, False otherwise
    """
    retry_count = 0
    last_processed_id = None
    processed_total = 0
    
    while retry_count <= max_retries:
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
            
            try:
                # Open file in append mode if resuming, otherwise write mode
                file_mode = 'a' if last_processed_id else 'w'
                with open(backup_file, file_mode) as f:
                    # Write opening bracket if starting fresh
                    if not last_processed_id:
                        f.write('[\n')
                    
                    # Use tqdm with estimated total, will adjust if estimate was off
                    with tqdm(total=estimated_docs, desc=f"Backing up {db_name}.{collection_name}", 
                             unit="docs", dynamic_ncols=True, initial=processed_total) as pbar:
                        
                        # Build query to resume from last processed document if applicable
                        query = {'_id': {'$gt': last_processed_id}} if last_processed_id else {}
                        cursor = collection.find(query, batch_size=batch_size)
                        batch = []
                        
                        for doc in cursor:
                            # Convert types inline
                            processed_doc = process_document(doc)
                            batch.append(processed_doc)
                            last_processed_id = doc['_id']
                            
                            processed_total += 1
                            
                            # Write batch when it reaches batch_size
                            if len(batch) >= batch_size:
                                # Write batch with comma if not first batch
                                batch_json = json.dumps(batch, indent=2)[1:-1]  # Remove [ and ]
                                if processed_total > batch_size:
                                    f.write(',\n')
                                f.write(batch_json)
                                
                                # Clear batch
                                batch = []
                                
                                # Flush to disk periodically
                                f.flush()
                            
                            pbar.update(1)
                            
                            # If our estimate was low, update total
                            if processed_total > estimated_docs:
                                pbar.total = processed_total + batch_size
                        
                        # Write any remaining documents
                        if batch:
                            if processed_total > batch_size:
                                f.write(',\n')
                            batch_json = json.dumps(batch, indent=2)[1:-1]
                            f.write(batch_json)
                        
                        # Write closing bracket
                        f.write('\n]')
                    
                logger.info(f"Successfully backed up {processed_total} documents to {backup_file}")
                return True
                
            except (ConnectionFailure, OperationFailure) as e:
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(f"Connection failed during backup (attempt {retry_count}/{max_retries}): {str(e)}")
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    # Exponential backoff for retry delay
                    retry_delay *= 2
                    continue
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded. Backup failed.")
                    return False
                    
            except Exception as e:
                logger.error(f"Failed during backup: {str(e)}")
                # Try to clean up partial backup file
                if backup_file.exists():
                    backup_file.unlink()
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error during backup: {str(e)}")
            return False
            
    return False
