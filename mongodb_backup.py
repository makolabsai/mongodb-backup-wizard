import os
import json
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
from tqdm import tqdm
import humanize
import questionary
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from typing import Dict, List, Tuple

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set up rich console
console = Console()

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

def inspect_document(doc, path=""):
    """Recursively inspect a document to identify problematic fields."""
    if doc is None:
        return f"None value at {path}"
    
    issues = []
    
    try:
        for key, value in doc.items():
            current_path = f"{path}.{key}" if path else key
            
            if value is None:
                issues.append(f"None value at {current_path}")
            elif isinstance(value, dict):
                sub_issues = inspect_document(value, current_path)
                if sub_issues:
                    issues.extend(sub_issues)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        sub_issues = inspect_document(item, f"{current_path}[{i}]")
                        if sub_issues:
                            issues.extend(sub_issues)
    except Exception as e:
        issues.append(f"Error inspecting {path}: {str(e)}")
    
    return issues

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

def select_collection(collections_info: Dict[str, List[Tuple[str, int, int]]]) -> Tuple[str, str]:
    """Let user select which collection to backup."""
    # Prepare all collections as choices
    choices = []
    for db_name, collections in collections_info.items():
        for coll_name, doc_count, size in collections:
            choice_text = f"{db_name}.{coll_name} ({doc_count:,} docs, {humanize.naturalsize(size)})"
            choices.append({"name": choice_text, "value": f"{db_name}.{coll_name}"})
    
    # Ask user to select a collection
    selected = questionary.select(
        "Select a collection to backup",
        choices=choices
    ).ask()
    
    if not selected:
        return None, None
    
    return selected.split('.')

def get_backup_location() -> Path:
    """Get the backup location from user or use default."""
    default_location = Path.cwd() / f"mongodb_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Ask for backup location
    location = questionary.text(
        "Enter backup location (press Enter for default)",
        default=str(default_location)
    ).ask()
    
    return Path(location)

def backup_collection(client, db_name: str, collection_name: str, backup_dir: Path) -> bool:
    """Backup a single collection to the specified directory."""
    try:
        # Create backup directory structure
        db_dir = backup_dir / db_name
        db_dir.mkdir(parents=True, exist_ok=True)
        
        # Get collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Get total document count for progress bar
        total_docs = collection.count_documents({})
        
        # Create backup file
        backup_file = db_dir / f"{collection_name}.json"
        
        # First, let's examine the first document
        first_doc = collection.find_one()
        if first_doc:
            console.print("\n[bold]Collection structure:[/bold]")
            for key, value in first_doc.items():
                console.print(f"  • {key}: {type(value).__name__}")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task(
                f"[cyan]Backing up {db_name}.{collection_name}...",
                total=total_docs
            )
            
            try:
                # Write the opening bracket
                with open(backup_file, 'w') as f:
                    f.write('[\n')
                
                # Process all documents
                cursor = collection.find({})
                first = True
                processed_count = 0
                
                for doc in cursor:
                    try:
                        # Convert ObjectId to string
                        if '_id' in doc:
                            doc['_id'] = str(doc['_id'])
                        
                        # Convert all values to strings if they're not basic types
                        processed_doc = {}
                        for key, value in doc.items():
                            if isinstance(value, (str, int, float, bool)) or value is None:
                                processed_doc[key] = value
                            else:
                                processed_doc[key] = str(value)
                        
                        # Write to file
                        with open(backup_file, 'a') as f:
                            if not first:
                                f.write(',\n')
                            json.dump(processed_doc, f, indent=2)
                            first = False
                        
                        processed_count += 1
                        progress.update(task, advance=1)
                        
                    except Exception as e:
                        console.print(f"[yellow]Warning: Error processing document: {str(e)}[/yellow]")
                        continue
                
                # Write the closing bracket
                with open(backup_file, 'a') as f:
                    f.write('\n]')
                
                # Get final file size
                file_size = backup_file.stat().st_size
                console.print(f"\n[green]✓ Backup completed:[/green]")
                console.print(f"  • Documents processed: {processed_count}")
                console.print(f"  • Backup size: {humanize.naturalsize(file_size)}")
                return True
                
            except Exception as e:
                console.print(f"[red]Error during backup: {str(e)}[/red]")
                return False
        
    except Exception as e:
        console.print(f"[red]Error backing up {db_name}.{collection_name}: {str(e)}[/red]")
        return False

def backup_mongodb():
    """Main function to backup MongoDB data."""
    try:
        # Get MongoDB connection string
        mongodb_url = os.getenv('MONGODB_URL')
        if not mongodb_url:
            console.print("[red]Error: MONGODB_URL environment variable not set[/red]")
            return
            
        # Connect to MongoDB
        client = MongoClient(mongodb_url)
        
        # Get collection information
        collections_info = get_collections_info(client)
        if not collections_info:
            console.print("[red]Error: No collections found[/red]")
            return
            
        # Let user select collection
        db_name, collection_name = select_collection(collections_info)
        if not db_name or not collection_name:
            console.print("[red]Error: No collection selected[/red]")
            return
            
        # Get backup location
        backup_dir = get_backup_location()
        
        # Perform backup
        backup_collection(client, db_name, collection_name, backup_dir)
        
    except Exception as e:
        console.print(f"[red]Error during backup: {str(e)}[/red]")

if __name__ == "__main__":
    backup_mongodb()
